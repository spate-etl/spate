//! Shared load-time guard for the opt-in TLS/SASL transport.
//!
//! TLS/mTLS and SASL are configured entirely through each connector's raw
//! `rdkafka` property passthrough (`security.protocol`, `ssl.*`, `sasl.*`) —
//! there is no typed security section, keeping `rdkafka` types out of the
//! public API. Those properties only do anything when librdkafka was compiled
//! with SSL/SASL support, which is the crate's off-by-default `tls` feature
//! (see the securing-connections guide).
//!
//! Without the feature, librdkafka rejects a security request only when the
//! client is created — the sink eagerly in `build()`, the source in `open()`.
//! Both call this guard from `validate()` before that point (the source also
//! at config-load via `from_component_config`), so a misconfiguration fails
//! identically with an actionable message instead of late and asymmetrically.

use etl_core::config::ConfigError;
use std::collections::BTreeMap;

/// Reject a passthrough that requests TLS/SASL when this build lacks the
/// transport. A no-op when the `tls` feature is enabled.
///
/// `scope` is the connector's config path prefix (`"source.kafka"` /
/// `"sink.kafka"`) so the message points at the offending section.
pub(crate) fn check_tls_feature(
    rdkafka: &BTreeMap<String, String>,
    scope: &str,
) -> Result<(), ConfigError> {
    // The feature compiles the SSL/SASL transport into librdkafka; with it on,
    // every security property is valid and handled by librdkafka directly.
    if cfg!(feature = "tls") {
        return Ok(());
    }

    // `security.protocol` defaults to `plaintext`; anything else (ssl,
    // sasl_ssl, sasl_plaintext) needs the transport, as does any explicit
    // `ssl.*` / `sasl.*` property — or the TLS-gated `enable.ssl.*` family
    // (e.g. `enable.ssl.certificate.verification`), which sits outside the
    // `ssl.` prefix — even without `security.protocol` set.
    let wants_security = rdkafka
        .get("security.protocol")
        .is_some_and(|v| !v.eq_ignore_ascii_case("plaintext"))
        || rdkafka.keys().any(|k| {
            k.starts_with("ssl.") || k.starts_with("sasl.") || k.starts_with("enable.ssl.")
        });

    if wants_security {
        return Err(ConfigError::Validation(format!(
            "{scope}.rdkafka requests TLS/SASL, but this build was compiled \
             without it; rebuild with the `kafka-tls` feature \
             (etl = {{ features = [\"kafka-tls\"] }})"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn plaintext_is_always_allowed() {
        // No security request: accepted regardless of the `tls` feature.
        assert!(check_tls_feature(&map(&[]), "source.kafka").is_ok());
        assert!(
            check_tls_feature(&map(&[("security.protocol", "plaintext")]), "sink.kafka").is_ok()
        );
        // A benign, non-security passthrough is not mistaken for one.
        assert!(check_tls_feature(&map(&[("linger.ms", "20")]), "sink.kafka").is_ok());
    }

    #[test]
    fn security_request_tracks_the_feature() {
        // These configs all request the secured transport; whether they are
        // accepted depends on whether the `tls` feature compiled it in.
        let secured = [
            map(&[("security.protocol", "ssl")]),
            map(&[
                ("security.protocol", "sasl_ssl"),
                ("sasl.mechanism", "SCRAM-SHA-256"),
            ]),
            // `ssl.*` / `sasl.*` alone (no `security.protocol`) still counts.
            map(&[("ssl.ca.location", "/etc/kafka/ca.pem")]),
            map(&[("sasl.username", "svc")]),
            // TLS-gated but outside the `ssl.` prefix.
            map(&[("enable.ssl.certificate.verification", "false")]),
        ];
        for cfg in secured {
            let result = check_tls_feature(&cfg, "source.kafka");
            if cfg!(feature = "tls") {
                assert!(result.is_ok(), "tls on: {cfg:?} should be accepted");
            } else {
                let err = result.expect_err("tls off: security config must be rejected");
                assert!(err.to_string().contains("kafka-tls"), "actionable: {err}");
            }
        }
    }
}
