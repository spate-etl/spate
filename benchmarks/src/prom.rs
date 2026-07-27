//! Minimal Prometheus text-exposition parsing for scraping our own
//! `/metrics` endpoint: counter/gauge sums and histogram quantiles.

/// Sum of every series of `name` whose label block contains `filter`
/// (pass `""` to match all series). Returns `None` when no series matched.
pub fn value(text: &str, name: &str, filter: &str) -> Option<f64> {
    let mut sum = 0.0;
    let mut matched = false;
    for line in text.lines() {
        if !line.starts_with(name) {
            continue;
        }
        let rest = &line[name.len()..];
        // Exact-name guard: next char must be `{` or a space.
        let ok = rest.starts_with('{') || rest.starts_with(' ');
        if !ok || !line.contains(filter) {
            continue;
        }
        if let Some(v) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
            sum += v;
            matched = true;
        }
    }
    matched.then_some(sum)
}

/// Parse the cumulative `_bucket` series of `name` into `(le, cumulative)`
/// pairs, merging the same `le` boundary across the labeled series whose
/// label block contains `filter` (pass `""` to merge all of them). Malformed
/// bucket lines are skipped rather than aborting the parse.
fn buckets(text: &str, name: &str, filter: &str) -> Vec<(f64, f64)> {
    let bucket_prefix = format!("{name}_bucket");
    let mut buckets: Vec<(f64, f64)> = Vec::new();
    for line in text.lines() {
        if !line.starts_with(&bucket_prefix) || !line.contains(filter) {
            continue;
        }
        let Some(le) = line.split("le=\"").nth(1).and_then(|s| s.split('"').next()) else {
            continue;
        };
        let le = if le == "+Inf" {
            f64::INFINITY
        } else {
            match le.parse::<f64>() {
                Ok(v) => v,
                Err(_) => continue,
            }
        };
        let Some(count) = line.rsplit(' ').next().and_then(|c| c.parse::<f64>().ok()) else {
            continue;
        };
        match buckets.iter_mut().find(|(b, _)| *b == le) {
            Some((_, c)) => *c += count,
            None => buckets.push((le, count)),
        }
    }
    buckets
}

/// Interpolate the `q`-quantile (0.0..=1.0) from `(le, cumulative)` buckets,
/// linearly inside the winning bucket. `None` when empty or with a zero total.
fn quantile_from_buckets(mut buckets: Vec<(f64, f64)>, q: f64) -> Option<f64> {
    if buckets.is_empty() {
        return None;
    }
    buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).expect("le ordering"));
    let total = buckets.last()?.1;
    if total == 0.0 {
        return None;
    }
    let target = total * q;
    let mut prev_le = 0.0;
    let mut prev_count = 0.0;
    for (le, count) in &buckets {
        if *count >= target {
            if le.is_infinite() {
                return Some(prev_le);
            }
            let span = count - prev_count;
            let frac = if span > 0.0 {
                (target - prev_count) / span
            } else {
                1.0
            };
            return Some(prev_le + (le - prev_le) * frac);
        }
        prev_le = *le;
        prev_count = *count;
    }
    Some(prev_le)
}

/// Approximate `q`-quantile (0.0..=1.0) of a classic Prometheus histogram
/// from its cumulative `_bucket` series, with linear interpolation inside
/// the winning bucket. Buckets across labeled series are merged.
pub fn histogram_quantile(text: &str, name: &str, q: f64) -> Option<f64> {
    quantile_from_buckets(buckets(text, name, ""), q)
}

/// Like [`histogram_quantile`] but over only the series whose label block
/// contains `filter` — for a histogram whose labels partition it into
/// distinct populations that must not be merged into one distribution.
pub fn histogram_quantile_labeled(text: &str, name: &str, filter: &str, q: f64) -> Option<f64> {
    quantile_from_buckets(buckets(text, name, filter), q)
}

/// Like [`histogram_quantile`] but over the change in each cumulative `le`
/// bucket between two renders (`after` − `before`), so the quantile reflects
/// only the observations inside the window. An `le` missing from `before`
/// counts as 0; a negative delta (counter reset) clamps to 0.
pub fn histogram_quantile_delta(before: &str, after: &str, name: &str, q: f64) -> Option<f64> {
    let base = buckets(before, name, "");
    let mut delta = buckets(after, name, "");
    for (le, count) in &mut delta {
        let before_count = base.iter().find(|(b, _)| b == le).map_or(0.0, |(_, c)| *c);
        *count = (*count - before_count).max(0.0);
    }
    quantile_from_buckets(delta, q)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
spate_source_records_total{pipeline=\"p\",component=\"a\"} 10\n\
spate_source_records_total{pipeline=\"p\",component=\"b\"} 5\n\
spate_source_records_totally_not{pipeline=\"p\"} 100\n\
lat_bucket{le=\"0.1\"} 5\n\
lat_bucket{le=\"1\"} 9\n\
lat_bucket{le=\"+Inf\"} 10\n\
lat_sum 4.2\n\
lat_count 10\n";

    #[test]
    fn sums_exact_name_matches_only() {
        assert_eq!(value(SAMPLE, "spate_source_records_total", ""), Some(15.0));
        assert_eq!(
            value(SAMPLE, "spate_source_records_total", "component=\"a\""),
            Some(10.0)
        );
        assert_eq!(value(SAMPLE, "missing_metric", ""), None);
    }

    #[test]
    fn histogram_quantile_interpolates() {
        let p50 = histogram_quantile(SAMPLE, "lat", 0.5).unwrap();
        assert!(p50 <= 0.1, "median falls in the first bucket: {p50}");
        let p90 = histogram_quantile(SAMPLE, "lat", 0.9).unwrap();
        assert!((0.1..=1.0).contains(&p90), "p90 in second bucket: {p90}");
        // The +Inf bucket clamps to the last finite boundary.
        let p999 = histogram_quantile(SAMPLE, "lat", 0.999).unwrap();
        assert!(p999 <= 1.0);
    }

    #[test]
    fn histogram_quantile_labeled_keeps_populations_apart() {
        // Two phases of one family: `fast` observations all land in the first
        // bucket, `slow` ones all in the second. Merging them would put the
        // median of either population in the wrong bucket.
        let text = "\
h_bucket{phase=\"fast\",le=\"0.1\"} 10\n\
h_bucket{phase=\"fast\",le=\"1\"} 10\n\
h_bucket{phase=\"fast\",le=\"+Inf\"} 10\n\
h_bucket{phase=\"slow\",le=\"0.1\"} 0\n\
h_bucket{phase=\"slow\",le=\"1\"} 10\n\
h_bucket{phase=\"slow\",le=\"+Inf\"} 10\n";
        let fast = histogram_quantile_labeled(text, "h", "phase=\"fast\"", 0.5).unwrap();
        assert!(fast <= 0.1, "fast median in the first bucket: {fast}");
        let slow = histogram_quantile_labeled(text, "h", "phase=\"slow\"", 0.5).unwrap();
        assert!((0.1..=1.0).contains(&slow), "slow median second: {slow}");
        // Unmatched filter behaves like an absent family, not a panic.
        assert_eq!(
            histogram_quantile_labeled(text, "h", "phase=\"none\"", 0.5),
            None
        );
    }

    #[test]
    fn histogram_quantile_delta_windows_observations() {
        // `before` has all ten observations in the first bucket; `after` adds
        // ten more, all in the second bucket. The windowed p50 must fall in the
        // second bucket, unlike the cumulative p50.
        let before =
            "lat_bucket{le=\"0.1\"} 10\nlat_bucket{le=\"1\"} 10\nlat_bucket{le=\"+Inf\"} 10\n";
        let after =
            "lat_bucket{le=\"0.1\"} 10\nlat_bucket{le=\"1\"} 20\nlat_bucket{le=\"+Inf\"} 20\n";
        let p50 = histogram_quantile_delta(before, after, "lat", 0.5).unwrap();
        assert!(
            (0.1..=1.0).contains(&p50),
            "windowed p50 in second bucket: {p50}"
        );
        // No new observations → no quantile.
        assert_eq!(histogram_quantile_delta(after, after, "lat", 0.5), None);
    }
}
