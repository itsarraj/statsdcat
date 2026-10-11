use anyhow::{bail, Context, Result};
use colored::Colorize;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as FmtWrite;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    Counter,
    Gauge,
    Timer,
    Histogram,
    Distribution,
    Set,
}

impl MetricType {
    pub fn badge(&self, color: bool) -> String {
        match self {
            MetricType::Counter => {
                if color {
                    "[CTR]".green().bold().to_string()
                } else {
                    "[CTR]".to_string()
                }
            }
            MetricType::Gauge => {
                if color {
                    "[GAG]".blue().bold().to_string()
                } else {
                    "[GAG]".to_string()
                }
            }
            MetricType::Timer => {
                if color {
                    "[TMR]".yellow().bold().to_string()
                } else {
                    "[TMR]".to_string()
                }
            }
            MetricType::Histogram => {
                if color {
                    "[HST]".magenta().bold().to_string()
                } else {
                    "[HST]".to_string()
                }
            }
            MetricType::Distribution => {
                if color {
                    "[DST]".purple().bold().to_string()
                } else {
                    "[DST]".to_string()
                }
            }
            MetricType::Set => {
                if color {
                    "[SET]".cyan().bold().to_string()
                } else {
                    "[SET]".to_string()
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatsdMetric {
    pub name: String,
    pub raw_value: String,
    pub value_f64: Option<f64>,
    pub metric_type: MetricType,
    pub sample_rate: f64,
    pub tags: Vec<(String, Option<String>)>,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_addr: Option<String>,
}

pub fn parse_statsd_line(line: &str, source_addr: Option<&str>) -> Result<StatsdMetric> {
    let line = line.trim();
    if line.is_empty() {
        bail!("Empty StatsD line");
    }

    let colon_idx = line
        .find(':')
        .context("Missing ':' separator between metric name and value")?;
    let name = line[..colon_idx].trim().to_string();
    if name.is_empty() {
        bail!("Metric name cannot be empty");
    }

    let rest = &line[colon_idx + 1..];
    let parts: Vec<&str> = rest.split('|').collect();
    if parts.len() < 2 {
        bail!("Invalid StatsD format: expected at least <value>|<type>");
    }

    let raw_value = parts[0].trim().to_string();
    let value_f64 = raw_value.parse::<f64>().ok();

    let type_str = parts[1].trim();
    let metric_type = match type_str {
        "c" => MetricType::Counter,
        "g" => MetricType::Gauge,
        "ms" => MetricType::Timer,
        "h" => MetricType::Histogram,
        "d" => MetricType::Distribution,
        "s" => MetricType::Set,
        other => bail!("Unknown metric type '{}'", other),
    };

    let mut sample_rate = 1.0;
    let mut tags = Vec::new();

    for &part in &parts[2..] {
        let trimmed = part.trim();
        if let Some(rate_str) = trimmed.strip_prefix('@') {
            if let Ok(rate) = rate_str.parse::<f64>() {
                if rate > 0.0 {
                    sample_rate = rate;
                }
            }
        } else if let Some(tag_str) = trimmed.strip_prefix('#') {
            for tag in tag_str.split(',') {
                let tag = tag.trim();
                if tag.is_empty() {
                    continue;
                }
                if let Some((k, v)) = tag.split_once(':') {
                    tags.push((k.trim().to_string(), Some(v.trim().to_string())));
                } else {
                    tags.push((tag.to_string(), None));
                }
            }
        }
    }

    let timestamp = chrono::Utc::now().to_rfc3339();

    Ok(StatsdMetric {
        name,
        raw_value,
        value_f64,
        metric_type,
        sample_rate,
        tags,
        timestamp,
        source_addr: source_addr.map(|s| s.to_string()),
    })
}

pub fn parse_statsd_packet(payload: &[u8], source_addr: Option<&str>) -> Vec<StatsdMetric> {
    let text = match std::str::from_utf8(payload) {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };

    let mut metrics = Vec::new();
    for line in text.lines() {
        if let Ok(metric) = parse_statsd_line(line, source_addr) {
            metrics.push(metric);
        }
    }
    metrics
}

pub fn format_tags(tags: &[(String, Option<String>)], color: bool) -> String {
    if tags.is_empty() {
        return String::new();
    }
    let formatted: Vec<String> = tags
        .iter()
        .map(|(k, v)| {
            if let Some(val) = v {
                format!("{}:{}", k, val)
            } else {
                k.clone()
            }
        })
        .collect();

    let tag_str = format!("[#{}]", formatted.join(","));
    if color {
        tag_str.cyan().to_string()
    } else {
        tag_str
    }
}

pub fn format_metric_stream(m: &StatsdMetric, color: bool) -> String {
    let ts_str = if color {
        format!("[{}]", m.timestamp).dimmed().to_string()
    } else {
        format!("[{}]", m.timestamp)
    };

    let badge = m.metric_type.badge(color);
    let name_str = if color {
        m.name.bold().to_string()
    } else {
        m.name.clone()
    };

    let rate_str = if (m.sample_rate - 1.0).abs() > f64::EPSILON {
        format!(" (@{:.2})", m.sample_rate)
    } else {
        String::new()
    };

    let tags_str = format_tags(&m.tags, color);
    let tags_formatted = if !tags_str.is_empty() {
        format!(" {}", tags_str)
    } else {
        String::new()
    };

    let src_str = if let Some(ref src) = m.source_addr {
        if color {
            format!(" <{}>", src).dimmed().to_string()
        } else {
            format!(" <{}>", src)
        }
    } else {
        String::new()
    };

    format!(
        "{} {} {} = {}{}{}{}",
        ts_str, badge, name_str, m.raw_value, rate_str, tags_formatted, src_str
    )
}

pub fn format_metric_ndjson(m: &StatsdMetric) -> Result<String> {
    Ok(serde_json::to_string(m)?)
}

#[derive(Debug, Clone)]
pub struct MetricFilter {
    pub name_regex: Option<Regex>,
    pub type_filter: Option<MetricType>,
    pub tag_match: Option<(String, Option<String>)>,
}

impl MetricFilter {
    pub fn matches(&self, m: &StatsdMetric) -> bool {
        if let Some(ref re) = self.name_regex {
            if !re.is_match(&m.name) {
                return false;
            }
        }
        if let Some(ref t) = self.type_filter {
            if &m.metric_type != t {
                return false;
            }
        }
        if let Some((ref req_k, ref req_v)) = self.tag_match {
            let found = m.tags.iter().any(|(k, v)| {
                if k != req_k {
                    return false;
                }
                if let Some(rv) = req_v {
                    v.as_ref() == Some(rv)
                } else {
                    true
                }
            });
            if !found {
                return false;
            }
        }
        true
    }
}

// ======================== Aggregator ========================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedCounter {
    pub count: u64,
    pub sum: f64,
    pub rate_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedGauge {
    pub last: f64,
    pub min: f64,
    pub max: f64,
    pub updates: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedTimer {
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub rate_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedSet {
    pub cardinality: usize,
    pub total_items: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AggregatedValue {
    Counter(AggregatedCounter),
    Gauge(AggregatedGauge),
    Timer(AggregatedTimer),
    Histogram(AggregatedTimer),
    Distribution(AggregatedTimer),
    Set(AggregatedSet),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMetric {
    pub name: String,
    pub tags: Vec<(String, Option<String>)>,
    #[serde(flatten)]
    pub value: AggregatedValue,
}

enum LiveState {
    Counter { count: u64, sum: f64 },
    Gauge { last: f64, min: f64, max: f64, updates: u64 },
    Timer { values: Vec<f64> },
    Set { items: HashSet<String>, total_items: u64 },
}

pub struct StatsdAggregator {
    group_by_tags: bool,
    state: HashMap<(String, Vec<(String, Option<String>)>), LiveState>,
    window_start: Instant,
}

impl StatsdAggregator {
    pub fn new(group_by_tags: bool) -> Self {
        Self {
            group_by_tags,
            state: HashMap::new(),
            window_start: Instant::now(),
        }
    }

    pub fn record(&mut self, m: &StatsdMetric) {
        let key_tags = if self.group_by_tags {
            let mut t = m.tags.clone();
            t.sort();
            t
        } else {
            Vec::new()
        };

        let key = (m.name.clone(), key_tags);

        match m.metric_type {
            MetricType::Counter => {
                let val = m.value_f64.unwrap_or(1.0) / m.sample_rate;
                self.state
                    .entry(key)
                    .and_modify(|s| {
                        if let LiveState::Counter { count, sum } = s {
                            *count += 1;
                            *sum += val;
                        }
                    })
                    .or_insert(LiveState::Counter { count: 1, sum: val });
            }
            MetricType::Gauge => {
                if let Some(val) = m.value_f64 {
                    self.state
                        .entry(key)
                        .and_modify(|s| {
                            if let LiveState::Gauge { last, min, max, updates } = s {
                                *last = val;
                                *min = min.min(val);
                                *max = max.max(val);
                                *updates += 1;
                            }
                        })
                        .or_insert(LiveState::Gauge {
                            last: val,
                            min: val,
                            max: val,
                            updates: 1,
                        });
                }
            }
            MetricType::Timer | MetricType::Histogram | MetricType::Distribution => {
                if let Some(val) = m.value_f64 {
                    self.state
                        .entry(key)
                        .and_modify(|s| {
                            if let LiveState::Timer { values } = s {
                                values.push(val);
                            }
                        })
                        .or_insert(LiveState::Timer { values: vec![val] });
                }
            }
            MetricType::Set => {
                let item = m.raw_value.clone();
                self.state
                    .entry(key)
                    .and_modify(|s| {
                        if let LiveState::Set { items, total_items } = s {
                            items.insert(item.clone());
                            *total_items += 1;
                        }
                    })
                    .or_insert_with(|| {
                        let mut set = HashSet::new();
                        set.insert(item);
                        LiveState::Set {
                            items: set,
                            total_items: 1,
                        }
                    });
            }
        }
    }

    pub fn flush(&mut self) -> Vec<AggregatedMetric> {
        let elapsed_sec = self.window_start.elapsed().as_secs_f64().max(0.001);
        let mut results = Vec::new();

        for ((name, tags), live) in self.state.drain() {
            let aggregated_value = match live {
                LiveState::Counter { count, sum } => {
                    AggregatedValue::Counter(AggregatedCounter {
                        count,
                        sum,
                        rate_per_sec: sum / elapsed_sec,
                    })
                }
                LiveState::Gauge { last, min, max, updates } => {
                    AggregatedValue::Gauge(AggregatedGauge {
                        last,
                        min,
                        max,
                        updates,
                    })
                }
                LiveState::Timer { mut values } => {
                    if values.is_empty() {
                        continue;
                    }
                    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let count = values.len() as u64;
                    let min = *values.first().unwrap();
                    let max = *values.last().unwrap();
                    let sum: f64 = values.iter().sum();
                    let mean = sum / (count as f64);

                    let calc_p = |p: f64| -> f64 {
                        let idx = ((values.len() - 1) as f64 * p).round() as usize;
                        values[idx.min(values.len() - 1)]
                    };

                    let p50 = calc_p(0.50);
                    let p95 = calc_p(0.95);
                    let p99 = calc_p(0.99);

                    AggregatedValue::Timer(AggregatedTimer {
                        count,
                        min,
                        max,
                        mean,
                        p50,
                        p95,
                        p99,
                        rate_per_sec: (count as f64) / elapsed_sec,
                    })
                }
                LiveState::Set { items, total_items } => {
                    AggregatedValue::Set(AggregatedSet {
                        cardinality: items.len(),
                        total_items,
                    })
                }
            };

            results.push(AggregatedMetric {
                name,
                tags,
                value: aggregated_value,
            });
        }

        self.window_start = Instant::now();
        results.sort_by(|a, b| a.name.cmp(&b.name));
        results
    }
}

pub fn format_aggregation_table(metrics: &[AggregatedMetric], color: bool) -> String {
    if metrics.is_empty() {
        return "No metrics recorded in this window.\n".to_string();
    }

    let mut out = String::new();
    let header_line = format!(
        "{:<32} {:<6} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
        "Metric", "Type", "Count", "Rate/s", "Min", "Avg/Last", "p95", "Max"
    );
    let sep = "-".repeat(102);

    if color {
        let _ = writeln!(out, "{}", header_line.bold());
        let _ = writeln!(out, "{}", sep.dimmed());
    } else {
        out.push_str(&header_line);
        out.push_str(&sep);
        out.push('\n');
    }

    for m in metrics {
        let name_display = if !m.tags.is_empty() {
            format!("{} {}", m.name, format_tags(&m.tags, color))
        } else {
            m.name.clone()
        };

        match &m.value {
            AggregatedValue::Counter(c) => {
                let badge = if color { "[CTR]".green().to_string() } else { "[CTR]".to_string() };
                let _ = writeln!(
                    out,
                    "{:<32} {:<6} {:>8} {:>10.1} {:>10} {:>10.1} {:>10} {:>10}",
                    name_display, badge, c.count, c.rate_per_sec, "-", c.sum, "-", "-"
                );
            }
            AggregatedValue::Gauge(g) => {
                let badge = if color { "[GAG]".blue().to_string() } else { "[GAG]".to_string() };
                let _ = writeln!(
                    out,
                    "{:<32} {:<6} {:>8} {:>10} {:>10.2} {:>10.2} {:>10} {:>10.2}",
                    name_display, badge, g.updates, "-", g.min, g.last, "-", g.max
                );
            }
            AggregatedValue::Timer(t)
            | AggregatedValue::Histogram(t)
            | AggregatedValue::Distribution(t) => {
                let badge = if color { "[TMR]".yellow().to_string() } else { "[TMR]".to_string() };
                let _ = writeln!(
                    out,
                    "{:<32} {:<6} {:>8} {:>10.1} {:>10.2} {:>10.2} {:>10.2} {:>10.2}",
                    name_display, badge, t.count, t.rate_per_sec, t.min, t.mean, t.p95, t.max
                );
            }
            AggregatedValue::Set(s) => {
                let badge = if color { "[SET]".cyan().to_string() } else { "[SET]".to_string() };
                let _ = writeln!(
                    out,
                    "{:<32} {:<6} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10}",
                    name_display, badge, s.total_items, "-", "-", format!("{} unq", s.cardinality), "-", "-"
                );
            }
        }
    }
    out
}

pub fn encode_statsd_metric(
    name: &str,
    value: &str,
    metric_type: MetricType,
    sample_rate: Option<f64>,
    tags: &[(&str, Option<&str>)],
) -> String {
    let type_char = match metric_type {
        MetricType::Counter => "c",
        MetricType::Gauge => "g",
        MetricType::Timer => "ms",
        MetricType::Histogram => "h",
        MetricType::Distribution => "d",
        MetricType::Set => "s",
    };

    let mut s = format!("{}:{}|{}", name, value, type_char);
    if let Some(r) = sample_rate {
        if (r - 1.0).abs() > f64::EPSILON && r > 0.0 {
            let _ = write!(s, "|@{:.2}", r);
        }
    }

    if !tags.is_empty() {
        let tag_strs: Vec<String> = tags
            .iter()
            .map(|(k, v)| {
                if let Some(val) = v {
                    format!("{}:{}", k, val)
                } else {
                    k.to_string()
                }
            })
            .collect();
        let _ = write!(s, "|#{}", tag_strs.join(","));
    }

    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_counter_simple() {
        let line = "users.signup:1|c";
        let m = parse_statsd_line(line, None).unwrap();
        assert_eq!(m.name, "users.signup");
        assert_eq!(m.value_f64, Some(1.0));
        assert_eq!(m.metric_type, MetricType::Counter);
        assert_eq!(m.sample_rate, 1.0);
        assert!(m.tags.is_empty());
    }

    #[test]
    fn test_parse_timer_with_sample_rate_and_tags() {
        let line = "db.query:42.5|ms|@0.1|#env:prod,cluster:us-east";
        let m = parse_statsd_line(line, Some("127.0.0.1:45123")).unwrap();
        assert_eq!(m.name, "db.query");
        assert_eq!(m.value_f64, Some(42.5));
        assert_eq!(m.metric_type, MetricType::Timer);
        assert_eq!(m.sample_rate, 0.1);
        assert_eq!(
            m.tags,
            vec![
                ("env".to_string(), Some("prod".to_string())),
                ("cluster".to_string(), Some("us-east".to_string()))
            ]
        );
        assert_eq!(m.source_addr, Some("127.0.0.1:45123".to_string()));
    }

    #[test]
    fn test_aggregator_counter_and_timer() {
        let mut agg = StatsdAggregator::new(false);

        let m1 = parse_statsd_line("api.req:1|c", None).unwrap();
        let m2 = parse_statsd_line("api.req:1|c", None).unwrap();
        let m3 = parse_statsd_line("api.latency:10|ms", None).unwrap();
        let m4 = parse_statsd_line("api.latency:20|ms", None).unwrap();

        agg.record(&m1);
        agg.record(&m2);
        agg.record(&m3);
        agg.record(&m4);

        let summary = agg.flush();
        assert_eq!(summary.len(), 2);

        let req = summary.iter().find(|m| m.name == "api.req").unwrap();
        match &req.value {
            AggregatedValue::Counter(c) => {
                assert_eq!(c.count, 2);
                assert_eq!(c.sum, 2.0);
            }
            _ => panic!("Expected Counter"),
        }

        let lat = summary.iter().find(|m| m.name == "api.latency").unwrap();
        match &lat.value {
            AggregatedValue::Timer(t) => {
                assert_eq!(t.count, 2);
                assert_eq!(t.min, 10.0);
                assert_eq!(t.max, 20.0);
                assert_eq!(t.mean, 15.0);
            }
            _ => panic!("Expected Timer"),
        }
    }

    #[test]
    fn test_encode_statsd() {
        let encoded = encode_statsd_metric(
            "cache.hit",
            "1",
            MetricType::Counter,
            Some(0.5),
            &[("tier", Some("l1")), ("active", None)],
        );
        assert_eq!(encoded, "cache.hit:1|c|@0.50|#tier:l1,active");
    }
}
