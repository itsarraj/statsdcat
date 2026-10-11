use regex::Regex;
use statsdcat::{
    format_aggregation_table, format_metric_ndjson, parse_statsd_packet, AggregatedValue,
    MetricFilter, MetricType, StatsdAggregator,
};
use std::time::Duration;
use tokio::net::UdpSocket;

#[tokio::test]
async fn test_live_udp_packet_ingestion() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let payload = b"web.request:1|c\napi.latency:42.5|ms|#route:auth\nmemory.usage:512|g\nusers.active:user123|s\n";
    client.send_to(payload, server_addr).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let (n, from) = tokio::time::timeout(Duration::from_secs(2), server.recv_from(&mut buf))
        .await
        .expect("Timeout receiving packet")
        .expect("recv error");

    let from_str = from.to_string();
    let metrics = parse_statsd_packet(&buf[..n], Some(&from_str));

    assert_eq!(metrics.len(), 4);

    assert_eq!(metrics[0].name, "web.request");
    assert_eq!(metrics[0].metric_type, MetricType::Counter);
    assert_eq!(metrics[0].value_f64, Some(1.0));

    assert_eq!(metrics[1].name, "api.latency");
    assert_eq!(metrics[1].metric_type, MetricType::Timer);
    assert_eq!(metrics[1].value_f64, Some(42.5));
    assert_eq!(
        metrics[1].tags,
        vec![("route".to_string(), Some("auth".to_string()))]
    );

    assert_eq!(metrics[2].name, "memory.usage");
    assert_eq!(metrics[2].metric_type, MetricType::Gauge);
    assert_eq!(metrics[2].value_f64, Some(512.0));

    assert_eq!(metrics[3].name, "users.active");
    assert_eq!(metrics[3].metric_type, MetricType::Set);
    assert_eq!(metrics[3].raw_value, "user123");

    // Verify NDJSON formatting
    let ndjson = format_metric_ndjson(&metrics[1]).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&ndjson).unwrap();
    assert_eq!(parsed["name"], "api.latency");
    assert_eq!(parsed["value_f64"], 42.5);
}

#[test]
fn test_aggregation_full_lifecycle() {
    let mut agg = StatsdAggregator::new(false);

    // Feed timers
    for val in [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0] {
        let line = format!("rpc.duration:{}|ms", val);
        let m = statsdcat::parse_statsd_line(&line, None).unwrap();
        agg.record(&m);
    }

    // Feed gauge
    let g1 = statsdcat::parse_statsd_line("queue.size:15|g", None).unwrap();
    let g2 = statsdcat::parse_statsd_line("queue.size:5|g", None).unwrap();
    let g3 = statsdcat::parse_statsd_line("queue.size:25|g", None).unwrap();
    agg.record(&g1);
    agg.record(&g2);
    agg.record(&g3);

    // Feed set
    let s1 = statsdcat::parse_statsd_line("visitors:u1|s", None).unwrap();
    let s2 = statsdcat::parse_statsd_line("visitors:u2|s", None).unwrap();
    let s3 = statsdcat::parse_statsd_line("visitors:u1|s", None).unwrap();
    agg.record(&s1);
    agg.record(&s2);
    agg.record(&s3);

    let summary = agg.flush();
    assert_eq!(summary.len(), 3);

    // Check timer metrics
    let rpc = summary.iter().find(|m| m.name == "rpc.duration").unwrap();
    match &rpc.value {
        AggregatedValue::Timer(t) => {
            assert_eq!(t.count, 10);
            assert_eq!(t.min, 10.0);
            assert_eq!(t.max, 100.0);
            assert_eq!(t.mean, 55.0);
            assert_eq!(t.p50, 60.0);
            assert_eq!(t.p95, 100.0);
        }
        _ => panic!("Expected timer"),
    }

    // Check gauge
    let q = summary.iter().find(|m| m.name == "queue.size").unwrap();
    match &q.value {
        AggregatedValue::Gauge(g) => {
            assert_eq!(g.updates, 3);
            assert_eq!(g.min, 5.0);
            assert_eq!(g.max, 25.0);
            assert_eq!(g.last, 25.0);
        }
        _ => panic!("Expected gauge"),
    }

    // Check set
    let visitors = summary.iter().find(|m| m.name == "visitors").unwrap();
    match &visitors.value {
        AggregatedValue::Set(s) => {
            assert_eq!(s.cardinality, 2);
            assert_eq!(s.total_items, 3);
        }
        _ => panic!("Expected set"),
    }

    // Check table output
    let table = format_aggregation_table(&summary, false);
    assert!(table.contains("rpc.duration"));
    assert!(table.contains("queue.size"));
    assert!(table.contains("visitors"));
}

#[test]
fn test_metric_filter() {
    let filter = MetricFilter {
        name_regex: Some(Regex::new(r"^api\..*").unwrap()),
        type_filter: Some(MetricType::Counter),
        tag_match: Some(("env".to_string(), Some("prod".to_string()))),
    };

    let match_metric =
        statsdcat::parse_statsd_line("api.calls:1|c|#env:prod,region:us", None).unwrap();
    assert!(filter.matches(&match_metric));

    let wrong_name =
        statsdcat::parse_statsd_line("db.calls:1|c|#env:prod", None).unwrap();
    assert!(!filter.matches(&wrong_name));

    let wrong_type =
        statsdcat::parse_statsd_line("api.calls:10|ms|#env:prod", None).unwrap();
    assert!(!filter.matches(&wrong_type));

    let wrong_tag =
        statsdcat::parse_statsd_line("api.calls:1|c|#env:staging", None).unwrap();
    assert!(!filter.matches(&wrong_tag));
}
