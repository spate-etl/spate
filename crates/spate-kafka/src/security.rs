//! Shared load-time guard for the opt-in TLS/SASL transport.
//!
//! TLS/mTLS and SASL are configured entirely through each connector's raw
//! `rdkafka` property passthrough (`security.protocol`, `ssl.*`, `sasl.*`).
//! There is no typed security section, which keeps `rdkafka` types out of
//! the public API. Those properties only do anything when librdkafka was compiled
//! with SSL/SASL support, which is the crate's off-by-default `tls` feature.
//!
//! Without the feature, librdkafka rejects a security request only when the
//! client is created: the sink eagerly in `build()`, the source in `open()`.
//! Both call this guard from `validate()` before that point (the source also
//! at config-load via `from_component_config`), so a misconfiguration fails
//! identically with an actionable message instead of late and asymmetrically.

use spate_core::config::ConfigError;
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
    check(rdkafka, scope, cfg!(feature = "tls"))
}

fn check(rdkafka: &BTreeMap<String, String>, scope: &str, tls: bool) -> Result<(), ConfigError> {
    if tls {
        return Ok(());
    }

    // `enable.ssl.*` and `enable.sasl.*` configure TLS and SASL from outside
    // the `ssl.` / `sasl.` prefixes.
    let wants_security = rdkafka
        .get("security.protocol")
        .is_some_and(|v| !v.eq_ignore_ascii_case("plaintext"))
        || rdkafka.keys().any(|k| {
            ["ssl.", "sasl.", "enable.ssl.", "enable.sasl."]
                .iter()
                .any(|p| k.starts_with(p))
        });

    if wants_security {
        return Err(ConfigError::Validation(format!(
            "{scope}.rdkafka requests TLS/SASL, but this build was compiled \
             without it; rebuild with the `kafka-tls` feature \
             (spate = {{ features = [\"kafka-tls\"] }})"
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
        let plain = [
            map(&[]),
            map(&[("security.protocol", "plaintext")]),
            map(&[("security.protocol", "PLAINTEXT")]),
            // Non-security keys that share the `enable.` prefix.
            map(&[("enable.idempotence", "true")]),
            map(&[("enable.auto.commit", "false")]),
            map(&[("linger.ms", "20")]),
        ];
        for cfg in plain {
            assert!(check(&cfg, "sink.kafka", false).is_ok(), "{cfg:?}");
        }
    }

    /// Every TLS/SASL request is rejected with the actionable message without the
    /// feature, and accepted with it. Regression for #610.
    #[test]
    fn security_request_tracks_the_feature() {
        let secured = [
            map(&[("security.protocol", "ssl")]),
            map(&[
                ("security.protocol", "sasl_ssl"),
                ("sasl.mechanism", "SCRAM-SHA-256"),
            ]),
            // `ssl.*` / `sasl.*` alone (no `security.protocol`) still counts.
            map(&[("ssl.ca.location", "/etc/kafka/ca.pem")]),
            map(&[("sasl.username", "svc")]),
            // Gated on OpenSSL but outside the `ssl.` / `sasl.` prefixes.
            map(&[("enable.ssl.certificate.verification", "false")]),
            map(&[("enable.sasl.oauthbearer.unsecure.jwt", "true")]),
        ];
        for cfg in secured {
            let err = check(&cfg, "source.kafka", false).expect_err("tls off must reject");
            assert!(err.to_string().contains("kafka-tls"), "{cfg:?}: {err}");
            assert!(check(&cfg, "source.kafka", true).is_ok(), "tls on: {cfg:?}");
        }
    }
}
