//! Rendering metrics under a recorder of the test's own, and reading series
//! back out of the Prometheus text exposition.

/// Run `f` under a recorder local to the calling thread, and return its
/// Prometheus text exposition with the production histogram buckets.
///
/// Metric handles must be built inside `f`. Gauge ownership stays
/// process-wide (INV-10), so name each pipeline or component with
/// [`unique_name`](crate::unique_name).
pub fn render_metrics(f: impl FnOnce()) -> String {
    spate_core::metrics::render_local(f)
}

/// One sample line of a text exposition.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricSample {
    /// Label pairs in the order the line carries them.
    pub labels: Vec<(String, String)>,
    /// The sample's value.
    pub value: f64,
}

impl MetricSample {
    /// The value of label `key`, if the sample carries it.
    #[must_use]
    pub fn label(&self, key: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// Every sample named exactly `name` that carries all of `labels`, in the
/// order they appear.
///
/// `name` is a sample name, so a histogram is read through `<family>_bucket`,
/// `<family>_sum` and `<family>_count`. Label values compare against the
/// rendered text, such as `le="0.5"`.
///
/// # Panics
///
/// Panics on a sample line named `name` that does not parse.
#[must_use]
pub fn metric_series(rendered: &str, name: &str, labels: &[(&str, &str)]) -> Vec<MetricSample> {
    rendered
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix(name)?;
            if !(rest.starts_with('{') || rest.starts_with(' ')) {
                return None;
            }
            let sample =
                parse_sample(rest).unwrap_or_else(|| panic!("unparseable sample line: {line}"));
            labels
                .iter()
                .all(|(k, v)| sample.label(k) == Some(*v))
                .then_some(sample)
        })
        .collect()
}

/// The value of the one sample [`metric_series`] selects, or `None` if it
/// selects none.
///
/// # Panics
///
/// Panics if more than one sample matches, listing them.
#[must_use]
pub fn metric_value(rendered: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let mut matched = metric_series(rendered, name, labels);
    assert!(
        matched.len() <= 1,
        "{} samples of {name} match {labels:?}: {matched:?}",
        matched.len()
    );
    matched.pop().map(|s| s.value)
}

/// The sum of every sample [`metric_series`] selects, or `None` if it selects
/// none.
#[must_use]
pub fn metric_sum(rendered: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let matched = metric_series(rendered, name, labels);
    (!matched.is_empty()).then(|| matched.iter().map(|s| s.value).sum())
}

/// Parse what follows the sample name: an optional label set, then the value.
fn parse_sample(rest: &str) -> Option<MetricSample> {
    let mut labels = Vec::new();
    let mut rest = rest;
    if let Some(mut body) = rest.strip_prefix('{') {
        loop {
            body = body.strip_prefix(',').unwrap_or(body);
            if let Some(after) = body.strip_prefix('}') {
                rest = after;
                break;
            }
            let (key, quoted) = body.split_once("=\"")?;
            let (value, after) = label_value(quoted)?;
            labels.push((key.to_owned(), value));
            body = after;
        }
    }
    let value = rest.split_whitespace().next()?.parse().ok()?;
    Some(MetricSample { labels, value })
}

/// Unescape a label value up to its closing quote, returning it and the text
/// after the quote.
fn label_value(quoted: &str) -> Option<(String, &str)> {
    let mut value = String::new();
    let mut chars = quoted.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Some((value, &quoted[i + 1..])),
            '\\' => match chars.next()?.1 {
                'n' => value.push('\n'),
                escaped => value.push(escaped),
            },
            c => value.push(c),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const RENDERED: &str = r#"# TYPE spate_rows_total counter
spate_rows_total{pipeline="p",component="c",partition="0"} 3
spate_rows_total{pipeline="p",component="c",partition="1"} 4
spate_rows_total_rate{pipeline="p",component="c"} 9
spate_rows{pipeline="p",component="c"} 0
spate_up 1
spate_note{pipeline="p",reason="a \"quoted\", comma"} 2
spate_wait_seconds_bucket{pipeline="p",le="0.5"} 1
spate_wait_seconds_bucket{pipeline="p",le="+Inf"} 2
spate_wait_seconds_count{pipeline="p"} 2
"#;

    /// A sample name matches exactly, so a longer name sharing its prefix is
    /// a different series.
    #[test]
    fn the_name_matches_exactly() {
        let rows = metric_series(RENDERED, "spate_rows", &[]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 0.0);
        assert_eq!(metric_series(RENDERED, "spate_rows_total", &[]).len(), 2);
    }

    #[test]
    fn labels_select_a_subset_in_any_order() {
        let one = [("partition", "1"), ("pipeline", "p")];
        assert_eq!(metric_value(RENDERED, "spate_rows_total", &one), Some(4.0));
    }

    #[test]
    fn an_absent_series_differs_from_a_zero_one() {
        assert_eq!(metric_value(RENDERED, "spate_rows", &[]), Some(0.0));
        assert_eq!(metric_value(RENDERED, "spate_absent", &[]), None);
        assert_eq!(
            metric_value(RENDERED, "spate_rows", &[("pipeline", "q")]),
            None
        );
    }

    #[test]
    fn a_sum_covers_every_match_and_none_when_absent() {
        assert_eq!(metric_sum(RENDERED, "spate_rows_total", &[]), Some(7.0));
        assert_eq!(metric_sum(RENDERED, "spate_rows", &[]), Some(0.0));
        assert_eq!(metric_sum(RENDERED, "spate_absent", &[]), None);
    }

    #[test]
    fn an_unlabeled_sample_parses() {
        assert_eq!(metric_value(RENDERED, "spate_up", &[]), Some(1.0));
    }

    #[test]
    fn an_escaped_label_value_is_unescaped() {
        let note = metric_series(RENDERED, "spate_note", &[]);
        assert_eq!(note[0].label("reason"), Some(r#"a "quoted", comma"#));
        assert_eq!(note[0].value, 2.0);
    }

    #[test]
    fn histogram_samples_read_by_suffix_and_le() {
        let inf = [("le", "+Inf")];
        assert_eq!(
            metric_value(RENDERED, "spate_wait_seconds_bucket", &inf),
            Some(2.0)
        );
        assert_eq!(
            metric_value(RENDERED, "spate_wait_seconds_count", &[]),
            Some(2.0)
        );
    }

    #[test]
    #[should_panic(expected = "2 samples of spate_rows_total")]
    fn an_ambiguous_selector_panics() {
        let _ = metric_value(RENDERED, "spate_rows_total", &[("pipeline", "p")]);
    }

    /// Histograms render as buckets, the way an installed exporter renders
    /// them.
    #[test]
    fn render_metrics_uses_the_production_buckets() {
        let rendered = render_metrics(|| {
            metrics::histogram!("t_render_duration_seconds").record(0.2);
        });
        assert!(
            !metric_series(&rendered, "t_render_duration_seconds_bucket", &[]).is_empty(),
            "{rendered}"
        );
    }
}
