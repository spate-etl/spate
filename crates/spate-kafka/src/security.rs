//! TLS/SASL handling shared by the source and sink: the load-time guard for
//! the opt-in transport, and the CA default applied to the client config.
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

    // `builtin.features`, `enable.ssl.*` and `enable.sasl.*` need OpenSSL from
    // outside the `ssl.` / `sasl.` prefixes.
    let wants_security = rdkafka
        .get("security.protocol")
        .is_some_and(|v| !v.eq_ignore_ascii_case("plaintext"))
        || rdkafka
            .get("builtin.features")
            .is_some_and(|v| names_openssl_feature(v))
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

/// Whether a `builtin.features` list names a flag librdkafka supports only with OpenSSL.
fn names_openssl_feature(features: &str) -> bool {
    features.split(',').any(|flag| {
        // librdkafka trims only leading whitespace, and rejects an unsupported
        // flag under `-` as well as `+`.
        let flag = flag.trim_ascii_start();
        let flag = flag.strip_prefix(['+', '-']).unwrap_or(flag);
        ["ssl", "sasl_scram", "sasl_oauthbearer"]
            .iter()
            .any(|f| flag.eq_ignore_ascii_case(f))
    })
}

/// Set `ssl.ca.location` to `probe` when the passthrough names no CA and
/// `openssl_env` is false, so the client trusts the system store on every
/// platform. A no-op without the `tls` feature.
pub(crate) fn apply_ca_default(
    cc: &mut rdkafka::ClientConfig,
    rdkafka: &BTreeMap<String, String>,
    openssl_env: bool,
) {
    if let Some(location) = ca_location_default(rdkafka, cfg!(feature = "tls"), openssl_env) {
        cc.set("ssl.ca.location", location);
    }
}

/// Whether `SSL_CERT_FILE` or `SSL_CERT_DIR` is set and non-empty.
///
/// `probe` stops at the first system bundle it loads and never reads these,
/// so the default stands aside for them.
pub(crate) fn openssl_env_overrides() -> bool {
    ["SSL_CERT_FILE", "SSL_CERT_DIR"]
        .iter()
        .any(|var| std::env::var_os(var).is_some_and(|v| !v.is_empty()))
}

fn ca_location_default(
    rdkafka: &BTreeMap<String, String>,
    tls: bool,
    openssl_env: bool,
) -> Option<&'static str> {
    let names_ca = ["ssl.ca.location", "ssl.ca.pem"]
        .iter()
        .any(|k| rdkafka.contains_key(*k));
    (tls && !names_ca && !openssl_env).then_some("probe")
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
            // Flags librdkafka builds without OpenSSL, or that `kafka-tls` does not add.
            map(&[("builtin.features", "sasl,sasl_plain,gzip,lz4")]),
            map(&[("builtin.features", "sasl_gssapi,oidc,http")]),
            // librdkafka keeps trailing whitespace, so `ssl ` is not the `ssl` flag.
            map(&[("builtin.features", "ssl ,gzip")]),
        ];
        for cfg in plain {
            assert!(check(&cfg, "sink.kafka", false).is_ok(), "{cfg:?}");
        }
    }

    /// Every TLS/SASL request is rejected with the actionable message without the
    /// feature, and accepted with it. Regression for #610 and #615.
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
            map(&[("builtin.features", "ssl")]),
            map(&[("builtin.features", "sasl_scram")]),
            map(&[("builtin.features", "sasl_oauthbearer")]),
            map(&[("builtin.features", "gzip, SSL")]),
            map(&[("builtin.features", "+sasl_scram")]),
            map(&[("builtin.features", "-ssl")]),
        ];
        for cfg in secured {
            let err = check(&cfg, "source.kafka", false).expect_err("tls off must reject");
            assert!(err.to_string().contains("kafka-tls"), "{cfg:?}: {err}");
            assert!(check(&cfg, "source.kafka", true).is_ok(), "tls on: {cfg:?}");
        }
    }

    /// `probe` applies only in a `tls` build whose passthrough names no CA and
    /// whose environment has no OpenSSL override. Regression for #609.
    #[test]
    fn ca_default_yields_to_a_named_ca_and_the_openssl_env() {
        let none = map(&[]);
        assert_eq!(ca_location_default(&none, true, false), Some("probe"));
        assert_eq!(ca_location_default(&none, false, false), None);
        assert_eq!(ca_location_default(&none, true, true), None);

        for named in [
            map(&[("ssl.ca.location", "/etc/kafka/ca.pem")]),
            map(&[("ssl.ca.pem", "-----BEGIN CERTIFICATE-----")]),
        ] {
            assert_eq!(ca_location_default(&named, true, false), None, "{named:?}");
        }
    }

    /// `openssl_env_overrides` reads both variables and treats an empty value
    /// as unset. Each case runs in a child test process with a controlled
    /// environment, because cargo sets both variables on Linux.
    #[test]
    fn openssl_env_overrides_reads_both_vars_and_ignores_empty() {
        const NAME: &str =
            "security::tests::openssl_env_overrides_reads_both_vars_and_ignores_empty";
        const EXPECT: &str = "SPATE_TEST_OPENSSL_ENV_EXPECT";
        if let Some(expected) = std::env::var_os(EXPECT) {
            assert_eq!(openssl_env_overrides(), expected == "1");
            return;
        }
        for (vars, expected) in [
            (&[][..], "0"),
            (&[("SSL_CERT_FILE", ""), ("SSL_CERT_DIR", "")][..], "0"),
            (&[("SSL_CERT_FILE", "/etc/kafka/ca.pem")][..], "1"),
            (&[("SSL_CERT_DIR", "/etc/kafka/certs")][..], "1"),
        ] {
            let out = std::process::Command::new(std::env::current_exe().expect("test binary"))
                .args(["--exact", NAME])
                .env_remove("SSL_CERT_FILE")
                .env_remove("SSL_CERT_DIR")
                .envs(vars.iter().copied())
                .env(EXPECT, expected)
                .output()
                .expect("spawn the test binary");
            let stdout = String::from_utf8_lossy(&out.stdout);
            // A filter that matches nothing also exits 0.
            assert!(
                out.status.success() && stdout.contains("1 passed"),
                "{vars:?}: {stdout}{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
}
