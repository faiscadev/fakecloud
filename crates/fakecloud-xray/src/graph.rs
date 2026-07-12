//! Deterministic X-Ray service-graph derivation from stored trace segments.
//!
//! A service graph is a set of nodes (services) and directed edges (downstream
//! calls) with aggregate statistics -- `OkCount`, `ErrorStatistics`,
//! `FaultStatistics`, `TotalCount`, `TotalResponseTime`. We compute it in-memory
//! from the ingested [`StoredSegment`]s, exactly as X-Ray does over real trace
//! data, rather than returning a canned graph:
//!
//! * Every top-level segment contributes a **node** named by the segment's
//!   `name`, typed by its `origin`. The node's statistics accumulate over all
//!   its segments (fault = 5xx-style, error = 4xx-style, throttle).
//! * Every `namespace: "remote"` subsegment contributes a downstream **node**
//!   (named by the subsegment) and an **edge** from the caller node to it, with
//!   statistics accumulated over the calls.
//! * A node is a graph **root** (entry point) when nothing calls it.
//!
//! Node `ReferenceId`s are assigned in first-seen order so the output is stable.

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use crate::segment::StoredSegment;

/// Ok / Error / Fault / total counters accumulated for a node or an edge.
#[derive(Debug, Default, Clone)]
struct Stats {
    ok: i64,
    error_throttle: i64,
    error_other: i64,
    fault_other: i64,
    total: i64,
    total_response_time: f64,
}

impl Stats {
    fn record(&mut self, fault: bool, error: bool, throttle: bool, response_time: Option<f64>) {
        self.total += 1;
        if let Some(rt) = response_time {
            self.total_response_time += rt;
        }
        if fault {
            self.fault_other += 1;
        } else if throttle {
            self.error_throttle += 1;
        } else if error {
            self.error_other += 1;
        } else {
            self.ok += 1;
        }
    }

    /// The `ErrorStatistics` count (4xx-style, includes throttles).
    fn error_total(&self) -> i64 {
        self.error_throttle + self.error_other
    }

    fn to_service_statistics(&self) -> Value {
        json!({
            "OkCount": self.ok,
            "ErrorStatistics": {
                "ThrottleCount": self.error_throttle,
                "OtherCount": self.error_other,
                "TotalCount": self.error_total(),
            },
            "FaultStatistics": {
                "OtherCount": self.fault_other,
                "TotalCount": self.fault_other,
            },
            "TotalCount": self.total,
            "TotalResponseTime": round6(self.total_response_time),
        })
    }
}

fn round6(v: f64) -> f64 {
    (v * 1_000_000.0).round() / 1_000_000.0
}

struct Node {
    reference_id: i64,
    name: String,
    node_type: String,
    start_time: f64,
    end_time: f64,
    stats: Stats,
    /// Downstream edges keyed by destination node name.
    edges: BTreeMap<String, Stats>,
    is_referenced: bool,
}

/// Build the service graph for a set of segments and render it as the wire
/// `ServiceList` (a JSON array of `Service` objects).
pub fn build_service_graph(segments: &[&StoredSegment]) -> Vec<Value> {
    let mut nodes: BTreeMap<String, Node> = BTreeMap::new();
    let mut next_ref: i64 = 0;

    // First pass: create a node for every segment and every downstream target,
    // so reference ids are assigned deterministically (segments first in trace
    // order, then any downstream-only nodes).
    let ensure_node =
        |nodes: &mut BTreeMap<String, Node>, next_ref: &mut i64, name: &str, node_type: &str| {
            if !nodes.contains_key(name) {
                let reference_id = *next_ref;
                *next_ref += 1;
                nodes.insert(
                    name.to_string(),
                    Node {
                        reference_id,
                        name: name.to_string(),
                        node_type: node_type.to_string(),
                        start_time: f64::MAX,
                        end_time: f64::MIN,
                        stats: Stats::default(),
                        edges: BTreeMap::new(),
                        is_referenced: false,
                    },
                );
            }
        };

    for seg in segments {
        let node_type = seg
            .origin
            .clone()
            .unwrap_or_else(|| "AWS::EC2::Instance".to_string());
        ensure_node(&mut nodes, &mut next_ref, &seg.name, &node_type);
        for d in &seg.downstream {
            let dtype = d.kind.clone().unwrap_or_else(|| "remote".to_string());
            ensure_node(&mut nodes, &mut next_ref, &d.name, &dtype);
        }
    }

    // Second pass: accumulate statistics and edges.
    for seg in segments {
        if let Some(node) = nodes.get_mut(&seg.name) {
            node.start_time = node.start_time.min(seg.start_time);
            node.end_time = node.end_time.max(seg.end_time.unwrap_or(seg.start_time));
            node.stats
                .record(seg.fault, seg.error, seg.throttle, seg.duration());
        }
        for d in &seg.downstream {
            let rt = d.end_time.map(|e| (e - d.start_time).max(0.0));
            if let Some(node) = nodes.get_mut(&seg.name) {
                node.edges
                    .entry(d.name.clone())
                    .or_default()
                    .record(d.fault, d.error, d.throttle, rt);
            }
            if let Some(dest) = nodes.get_mut(&d.name) {
                dest.is_referenced = true;
            }
        }
    }

    // Resolve destination reference ids (borrow the map immutably after mutation).
    let ref_by_name: BTreeMap<String, i64> = nodes
        .iter()
        .map(|(n, node)| (n.clone(), node.reference_id))
        .collect();

    let mut services: Vec<Value> = nodes
        .values()
        .map(|node| render_node(node, &ref_by_name))
        .collect();
    // Emit in reference-id order for a stable, readable graph.
    services.sort_by_key(|s| s.get("ReferenceId").and_then(Value::as_i64).unwrap_or(0));
    services
}

fn render_node(node: &Node, ref_by_name: &BTreeMap<String, i64>) -> Value {
    let start = if node.start_time == f64::MAX {
        0.0
    } else {
        node.start_time
    };
    let end = if node.end_time == f64::MIN {
        start
    } else {
        node.end_time
    };
    let edges: Vec<Value> = node
        .edges
        .iter()
        .map(|(dest_name, stats)| {
            let reference_id = ref_by_name.get(dest_name).copied().unwrap_or(-1);
            json!({
                "ReferenceId": reference_id,
                "StartTime": start,
                "EndTime": end,
                "SummaryStatistics": {
                    "OkCount": stats.ok,
                    "ErrorStatistics": {
                        "ThrottleCount": stats.error_throttle,
                        "OtherCount": stats.error_other,
                        "TotalCount": stats.error_total(),
                    },
                    "FaultStatistics": {
                        "OtherCount": stats.fault_other,
                        "TotalCount": stats.fault_other,
                    },
                    "TotalCount": stats.total,
                    "TotalResponseTime": round6(stats.total_response_time),
                },
                "ResponseTimeHistogram": [],
                "Aliases": [],
            })
        })
        .collect();

    let mut svc = Map::new();
    svc.insert("ReferenceId".into(), json!(node.reference_id));
    svc.insert("Name".into(), json!(node.name));
    svc.insert("Names".into(), json!([node.name]));
    svc.insert("Root".into(), json!(!node.is_referenced));
    svc.insert("Type".into(), json!(node.node_type));
    svc.insert("State".into(), json!("active"));
    svc.insert("StartTime".into(), json!(start));
    svc.insert("EndTime".into(), json!(end));
    svc.insert("Edges".into(), Value::Array(edges));
    svc.insert(
        "SummaryStatistics".into(),
        node.stats.to_service_statistics(),
    );
    svc.insert("DurationHistogram".into(), json!([]));
    svc.insert("ResponseTimeHistogram".into(), json!([]));
    Value::Object(svc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::parse_segment;

    fn seg(doc: &str) -> StoredSegment {
        parse_segment(doc).unwrap()
    }

    #[test]
    fn two_tier_graph_has_nodes_and_edge() {
        let web = seg(
            r#"{"trace_id":"1-a","id":"1111","name":"web","start_time":1.0,"end_time":3.0,
                "subsegments":[{"id":"2222","name":"backend","namespace":"remote","start_time":1.5,"end_time":2.5}]}"#,
        );
        let backend = seg(
            r#"{"trace_id":"1-a","id":"2222","name":"backend","start_time":1.5,"end_time":2.5,"parent_id":"1111"}"#,
        );
        let segs = vec![&web, &backend];
        let graph = build_service_graph(&segs);
        assert_eq!(graph.len(), 2);
        let web_node = graph.iter().find(|s| s["Name"] == json!("web")).unwrap();
        assert_eq!(web_node["Root"], json!(true));
        assert_eq!(web_node["Edges"].as_array().unwrap().len(), 1);
        let backend_node = graph
            .iter()
            .find(|s| s["Name"] == json!("backend"))
            .unwrap();
        assert_eq!(backend_node["Root"], json!(false));
    }

    #[test]
    fn fault_is_counted_in_statistics() {
        let s = seg(
            r#"{"trace_id":"1-b","id":"1","name":"svc","start_time":1.0,"end_time":2.0,"fault":true}"#,
        );
        let segs = vec![&s];
        let graph = build_service_graph(&segs);
        let stats = &graph[0]["SummaryStatistics"];
        assert_eq!(stats["FaultStatistics"]["TotalCount"], json!(1));
        assert_eq!(stats["OkCount"], json!(0));
        assert_eq!(stats["TotalCount"], json!(1));
    }
}
