use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use colored::Colorize;
use regex::Regex;
use statsdcat::{
    encode_statsd_metric, format_aggregation_table, format_metric_ndjson, format_metric_stream,
    parse_statsd_packet, MetricFilter, MetricType, StatsdAggregator,
};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliMetricType {
    Counter,
    Gauge,
    Timer,
    Histogram,
    Distribution,
    Set,
}

impl From<CliMetricType> for MetricType {
    fn from(c: CliMetricType) -> Self {
        match c {
            CliMetricType::Counter => MetricType::Counter,
            CliMetricType::Gauge => MetricType::Gauge,
            CliMetricType::Timer => MetricType::Timer,
            CliMetricType::Histogram => MetricType::Histogram,
            CliMetricType::Distribution => MetricType::Distribution,
            CliMetricType::Set => MetricType::Set,
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "statsdcat",
    about = "Fast, standalone StatsD & DogStatsD metrics inspector, listener, aggregator, and emitter",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    listen_defaults: ListenArgs,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Listen for incoming StatsD/DogStatsD UDP packets (default)
    Listen(ListenArgs),
    /// Emit synthetic StatsD/DogStatsD packets to a UDP target
    Emit(EmitArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct ListenArgs {
    /// UDP address to bind listener to (e.g. "127.0.0.1:8125" or ":8125")
    #[arg(short, long, default_value = "127.0.0.1:8125")]
    bind: String,

    /// Rolling aggregation window in seconds (e.g. 1.0 or 5.0). Enables aggregated dashboard mode.
    #[arg(short = 'a', long)]
    aggregate: Option<f64>,

    /// Group aggregated metrics by distinct tag combinations
    #[arg(long)]
    group_by_tags: bool,

    /// Output stream or aggregation summaries as NDJSON lines
    #[arg(short = 'j', long)]
    ndjson: bool,

    /// Filter metric names by regex pattern
    #[arg(short = 'm', long)]
    metric: Option<String>,

    /// Filter by metric type (counter, gauge, timer, histogram, distribution, set)
    #[arg(short = 't', long, value_enum)]
    r#type: Option<CliMetricType>,

    /// Filter by tag key or key:value (e.g. "env:prod" or "region")
    #[arg(long)]
    tag: Option<String>,

    /// Exit after receiving N matching metrics
    #[arg(short = 'n', long)]
    count: Option<u64>,

    /// Disable ANSI color formatting
    #[arg(long)]
    no_color: bool,
}

impl Default for ListenArgs {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8125".to_string(),
            aggregate: None,
            group_by_tags: false,
            ndjson: false,
            metric: None,
            r#type: None,
            tag: None,
            count: None,
            no_color: false,
        }
    }
}

#[derive(Parser, Debug)]
pub struct EmitArgs {
    /// Target StatsD UDP address (e.g. "127.0.0.1:8125")
    #[arg(short = 't', long = "target", default_value = "127.0.0.1:8125")]
    target: String,

    /// Metric name (e.g. "api.request.latency")
    #[arg(short = 'm', long)]
    metric: String,

    /// Metric value string or float
    #[arg(short = 'v', long, default_value = "1")]
    value: String,

    /// Metric type
    #[arg(short = 'y', long = "type", value_enum, default_value_t = CliMetricType::Counter)]
    metric_type: CliMetricType,

    /// Sample rate between 0.0 and 1.0 (e.g. 0.1)
    #[arg(long)]
    sample_rate: Option<f64>,

    /// DogStatsD tags in key:value or key format (can be specified multiple times)
    #[arg(long = "tag")]
    tags: Vec<String>,

    /// Number of packets to emit
    #[arg(short = 'n', long, default_value_t = 1)]
    count: u64,

    /// Emission rate limit in packets per second (0 for as fast as possible)
    #[arg(long, default_value_t = 0)]
    rate: u64,
}

fn resolve_bind_addr(addr_str: &str) -> Result<SocketAddr> {
    let normalized = if addr_str.starts_with(':') {
        format!("0.0.0.0{}", addr_str)
    } else {
        addr_str.to_string()
    };

    use std::net::ToSocketAddrs;
    let mut addrs = normalized
        .to_socket_addrs()
        .with_context(|| format!("Failed to resolve bind address '{}'", addr_str))?;

    addrs
        .next()
        .context(format!("No IP address found for '{}'", addr_str))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Listen(args)) => run_listen(args).await,
        Some(Commands::Emit(args)) => run_emit(args).await,
        None => run_listen(cli.listen_defaults).await,
    }
}

async fn run_listen(args: ListenArgs) -> Result<()> {
    if args.no_color {
        colored::control::set_override(false);
    }

    let bind_addr = resolve_bind_addr(&args.bind)?;
    let socket = UdpSocket::bind(bind_addr)
        .await
        .with_context(|| format!("Failed to bind UDP socket on {}", bind_addr))?;

    let name_regex = match args.metric {
        Some(ref pat) => Some(Regex::new(pat).context("Invalid --metric regex pattern")?),
        None => None,
    };

    let tag_match = args.tag.as_deref().map(|t| {
        if let Some((k, v)) = t.split_once(':') {
            (k.trim().to_string(), Some(v.trim().to_string()))
        } else {
            (t.trim().to_string(), None)
        }
    });

    let filter = MetricFilter {
        name_regex,
        type_filter: args.r#type.map(Into::into),
        tag_match,
    };

    eprintln!(
        "{} Listening for StatsD datagrams on {}...",
        "●".cyan(),
        bind_addr.to_string().bold()
    );

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        r.store(false, Ordering::SeqCst);
    });

    let is_aggregate = args.aggregate.is_some();
    let agg_interval_sec = args.aggregate.unwrap_or(1.0);
    let mut aggregator = StatsdAggregator::new(args.group_by_tags);
    let mut last_agg_flush = tokio::time::Instant::now();
    let agg_duration = Duration::from_secs_f64(agg_interval_sec);

    let mut buf = vec![0u8; 65536];
    let mut received_count = 0u64;
    let mut stdout = io::stdout().lock();

    while running.load(Ordering::SeqCst) {
        let recv_future = socket.recv_from(&mut buf);
        let timeout_duration = if is_aggregate {
            let elapsed = last_agg_flush.elapsed();
            if elapsed >= agg_duration {
                Duration::from_millis(10)
            } else {
                agg_duration - elapsed
            }
        } else {
            Duration::from_millis(500)
        };

        match tokio::time::timeout(timeout_duration, recv_future).await {
            Ok(Ok((n, from_addr))) => {
                let packet_data = &buf[..n];
                let from_str = from_addr.to_string();
                let metrics = parse_statsd_packet(packet_data, Some(&from_str));

                for m in metrics {
                    if !filter.matches(&m) {
                        continue;
                    }

                    received_count += 1;

                    if is_aggregate {
                        aggregator.record(&m);
                    } else if args.ndjson {
                        let json = format_metric_ndjson(&m)?;
                        writeln!(stdout, "{}", json)?;
                        stdout.flush()?;
                    } else {
                        let line = format_metric_stream(&m, !args.no_color);
                        writeln!(stdout, "{}", line)?;
                        stdout.flush()?;
                    }

                    if let Some(limit) = args.count {
                        if received_count >= limit {
                            running.store(false, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                eprintln!("{}: {}", "Error reading UDP socket".red(), e);
                break;
            }
            Err(_) => {
                // Timeout fired
            }
        }

        if is_aggregate && last_agg_flush.elapsed() >= agg_duration {
            let summary = aggregator.flush();
            if args.ndjson {
                for item in summary {
                    let json = serde_json::to_string(&item)?;
                    writeln!(stdout, "{}", json)?;
                }
            } else {
                let table = format_aggregation_table(&summary, !args.no_color);
                write!(stdout, "{}", table)?;
            }
            stdout.flush()?;
            last_agg_flush = tokio::time::Instant::now();
        }
    }

    if is_aggregate {
        let final_summary = aggregator.flush();
        if !final_summary.is_empty() {
            if args.ndjson {
                for item in final_summary {
                    let json = serde_json::to_string(&item)?;
                    writeln!(stdout, "{}", json)?;
                }
            } else {
                let table = format_aggregation_table(&final_summary, !args.no_color);
                write!(stdout, "{}", table)?;
            }
            stdout.flush()?;
        }
    }

    Ok(())
}

async fn run_emit(args: EmitArgs) -> Result<()> {
    let target_addr: SocketAddr = args
        .target
        .parse()
        .with_context(|| format!("Invalid target socket address '{}'", args.target))?;

    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("Failed to bind ephemeral UDP client socket")?;

    let mut tags = Vec::new();
    for t in &args.tags {
        if let Some((k, v)) = t.split_once(':') {
            tags.push((k.trim(), Some(v.trim())));
        } else {
            tags.push((t.trim(), None));
        }
    }

    let packet_str = encode_statsd_metric(
        &args.metric,
        &args.value,
        args.metric_type.into(),
        args.sample_rate,
        &tags,
    );

    let packet_bytes = packet_str.as_bytes();
    let delay = if args.rate > 0 {
        Some(Duration::from_secs_f64(1.0 / (args.rate as f64)))
    } else {
        None
    };

    eprintln!(
        "{} Emitting {} packet(s) to {}: \"{}\"",
        "●".cyan(),
        args.count,
        target_addr,
        packet_str.bold()
    );

    for i in 0..args.count {
        socket
            .send_to(packet_bytes, target_addr)
            .await
            .with_context(|| format!("Failed to send datagram #{}", i + 1))?;

        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
    }

    eprintln!(
        "{} Successfully sent {} datagram(s)",
        "✔".green(),
        args.count
    );
    Ok(())
}
