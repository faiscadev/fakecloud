//! Time-ordered index over stored metric datapoints, so alarm evaluation
//! reads only the datapoints in an alarm's evaluation window instead of
//! scanning every datapoint ever published to the namespace.
//!
//! Datapoints are stored append-only per (region, namespace) in
//! `CloudWatchState::metrics`, and several writers push to them
//! (`PutMetricData`, the cross-service metrics delivery, snapshot load). The
//! index therefore catches up lazily: each lookup first indexes whatever was
//! appended since the last one (each datapoint is indexed once), then answers
//! with a range query on the series' timestamp-ordered map.
//!
//! The index is a cache derived from the stored datapoints: it isn't
//! persisted, and cloning a state (for a snapshot) yields an empty index that
//! rebuilds on first use.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};

use crate::state::MetricDatum;

/// A metric's identity within a namespace: name plus exact dimension set.
pub(crate) type SeriesKey = (String, BTreeMap<String, String>);

#[derive(Default)]
struct NamespaceIndex {
    /// How many of the namespace's datapoints are indexed.
    indexed: usize,
    /// Series -> timestamp (ms) -> positions in the namespace's datapoints.
    series: HashMap<SeriesKey, BTreeMap<i64, Vec<usize>>>,
}

/// Per (region, namespace) series indexes.
#[derive(Default)]
pub struct SeriesIndex {
    namespaces: HashMap<(String, String), NamespaceIndex>,
}

impl Clone for SeriesIndex {
    /// A derived cache: a clone starts empty and rebuilds on first use.
    fn clone(&self) -> Self {
        SeriesIndex::default()
    }
}

impl std::fmt::Debug for SeriesIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeriesIndex")
            .field("namespaces", &self.namespaces.len())
            .finish()
    }
}

impl SeriesIndex {
    /// The datapoints of series `(metric_name, dims)` in `namespace` (of
    /// `region`) whose timestamps fall in `[start, end)`, oldest first. `data`
    /// is the namespace's stored datapoints.
    pub(crate) fn window<'a>(
        &mut self,
        (region, namespace): (&str, &str),
        data: &'a [MetricDatum],
        metric_name: &str,
        dims: &BTreeMap<String, String>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<&'a MetricDatum> {
        let ns = self
            .namespaces
            .entry((region.to_string(), namespace.to_string()))
            .or_default();
        // Datapoints are append-only; a shorter list means it was replaced.
        if data.len() < ns.indexed {
            *ns = NamespaceIndex::default();
        }
        for (i, d) in data.iter().enumerate().skip(ns.indexed) {
            ns.series
                .entry((d.metric_name.clone(), d.dimensions.clone()))
                .or_default()
                .entry(d.timestamp.timestamp_millis())
                .or_default()
                .push(i);
        }
        ns.indexed = data.len();
        let key = (metric_name.to_string(), dims.clone());
        let Some(series) = ns.series.get(&key) else {
            return Vec::new();
        };
        series
            .range(start.timestamp_millis()..end.timestamp_millis())
            .flat_map(|(_, positions)| positions.iter().map(|&i| &data[i]))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn datum(name: &str, secs: i64) -> MetricDatum {
        MetricDatum {
            metric_name: name.into(),
            dimensions: BTreeMap::new(),
            timestamp: DateTime::<Utc>::from_timestamp(secs, 0).unwrap(),
            value: Some(secs as f64),
            statistic_values: None,
            unit: None,
            storage_resolution: None,
        }
    }

    #[test]
    fn window_returns_only_the_series_in_range_and_catches_up() {
        let mut idx = SeriesIndex::default();
        let mut data: Vec<MetricDatum> = (0..1000).map(|s| datum("A", s)).collect();
        data.push(datum("B", 500));
        let at = |s| DateTime::<Utc>::from_timestamp(s, 0).unwrap();
        let got = idx.window(("r", "ns"), &data, "A", &BTreeMap::new(), at(10), at(13));
        assert_eq!(
            got.iter().map(|d| d.value.unwrap()).collect::<Vec<_>>(),
            [10.0, 11.0, 12.0]
        );
        // Appended later, out of order: still found.
        data.push(datum("A", 11));
        let got = idx.window(("r", "ns"), &data, "A", &BTreeMap::new(), at(11), at(12));
        assert_eq!(got.len(), 2);
        // A replaced (shorter) list rebuilds.
        data.truncate(5);
        let got = idx.window(("r", "ns"), &data, "A", &BTreeMap::new(), at(0), at(100));
        assert_eq!(got.len(), 5);
    }
}
