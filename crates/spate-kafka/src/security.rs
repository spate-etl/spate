//! TLS/SASL handling shared by every Kafka client: the load-time guard for
//! the opt-in transport, the CA bundle a section's `tls.root_ca` builds, and
//! the CA default the client factory applies.
//!
//! TLS/mTLS and SASL are configured through each connector's raw `rdkafka`
//! property passthrough (`security.protocol`, `ssl.*`, `sasl.*`). The one typed
//! key is `tls.root_ca`, core's [`TlsSection`], which adds a private CA to the
//! system roots. Those settings only do anything when librdkafka was compiled
//! with SSL/SASL support, which is the crate's off-by-default `tls` feature.
//!
//! Without the feature, librdkafka rejects a security request only when the
//! client is created: the sink eagerly in its constructors, the source in
//! `open()`. Both call this guard from `validate()` before that point, and
//! both again at config-load via their config's `from_component_config`, so a
//! misconfiguration fails identically with an actionable message instead of
//! late and asymmetrically.

use spate_core::config::{ConfigError, SectionPath};
use spate_core::tls::TlsSection;
use std::collections::BTreeMap;

/// What a build without the `tls` feature says to a request for TLS or SASL.
const REBUILD: &str = "requests TLS/SASL, but this build was compiled without it; rebuild \
                       with the `kafka-tls` feature (spate = { features = [\"kafka-tls\"] })";

/// Reject a passthrough or a `tls.root_ca` that requests TLS/SASL when this
/// build lacks the transport, and a `tls.root_ca` under a protocol that does
/// not verify the broker.
///
/// Each error starts at `at`, the path of the section that holds `rdkafka`
/// and `tls`.
pub(crate) fn check_tls_feature(
    rdkafka: &BTreeMap<String, String>,
    tls_section: &TlsSection,
    at: &SectionPath,
) -> Result<(), ConfigError> {
    check(rdkafka, tls_section, at, cfg!(feature = "tls"))
}

fn check(
    rdkafka: &BTreeMap<String, String>,
    tls_section: &TlsSection,
    at: &SectionPath,
    tls: bool,
) -> Result<(), ConfigError> {
    if tls_section.root_ca.is_some() {
        if !tls {
            return Err(at.invalid("tls.root_ca", REBUILD));
        }
        // librdkafka builds its SSL context only for these two protocols.
        if !protocol(rdkafka)
            .is_some_and(|p| p.eq_ignore_ascii_case("ssl") || p.eq_ignore_ascii_case("sasl_ssl"))
        {
            return Err(at.invalid(
                "tls.root_ca",
                "needs rdkafka.\"security.protocol\" set to ssl or sasl_ssl, the \
                 protocols under which librdkafka verifies the broker",
            ));
        }
    }
    if tls {
        return Ok(());
    }

    // `builtin.features`, `enable.ssl.*` and `enable.sasl.*` need OpenSSL from
    // outside the `ssl.` / `sasl.` prefixes.
    let wants_security = protocol(rdkafka).is_some_and(|v| !v.eq_ignore_ascii_case("plaintext"))
        || rdkafka
            .get("builtin.features")
            .is_some_and(|v| names_openssl_feature(v))
        || rdkafka.keys().any(|k| {
            ["ssl.", "sasl.", "enable.ssl.", "enable.sasl."]
                .iter()
                .any(|p| k.starts_with(p))
        });

    if wants_security {
        return Err(at.invalid("rdkafka", REBUILD));
    }
    Ok(())
}

/// `security.protocol` as librdkafka reads it, without leading whitespace,
/// for a match that ignores ASCII case. `None` when the passthrough does not
/// set it.
fn protocol(rdkafka: &BTreeMap<String, String>) -> Option<&str> {
    rdkafka
        .get("security.protocol")
        .map(|v| v.trim_ascii_start())
}

/// Logs one warning when `rdkafka` sends a non-empty `sasl.password` over
/// `sasl_plaintext`, which carries the SASL login without TLS. Call it after
/// the section's checks pass and before the client is created.
pub(crate) fn warn_sasl_plaintext(rdkafka: &BTreeMap<String, String>, at: &SectionPath) {
    if sends_password_without_tls(rdkafka) {
        at.warn_cleartext_credential(
            r#"rdkafka."sasl.password""#,
            r#"rdkafka."security.protocol" is sasl_plaintext"#,
        );
    }
}

/// Whether `sasl.password` is non-empty and `security.protocol` is
/// `sasl_plaintext`. librdkafka matches the protocol in any case.
fn sends_password_without_tls(rdkafka: &BTreeMap<String, String>) -> bool {
    protocol(rdkafka).is_some_and(|v| v.eq_ignore_ascii_case("sasl_plaintext"))
        && rdkafka.get("sasl.password").is_some_and(|p| !p.is_empty())
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

/// The PEM bundle librdkafka verifies brokers against when `tls.root_ca` is
/// set: the system roots, then the certificates in `root_ca`. `None` when
/// `root_ca` is unset.
///
/// Reading the system trust store blocks, as [`SystemRoots::load`] does.
///
/// [`SystemRoots::load`]: spate_core::tls::SystemRoots::load
///
/// # Errors
///
/// Why `root_ca` could not be read, for the caller to anchor at
/// `tls.root_ca`.
#[cfg(feature = "tls")]
pub(crate) fn ca_bundle(tls_section: &TlsSection) -> Result<Option<String>, String> {
    ca_bundle_from(tls_section, spate_core::tls::SystemRoots::load)
}

/// Without the `tls` feature validation refuses `tls.root_ca`, so there is
/// no bundle to build.
#[cfg(not(feature = "tls"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the same signature as the `tls` build's"
)]
pub(crate) fn ca_bundle(_tls_section: &TlsSection) -> Result<Option<String>, String> {
    Ok(None)
}

/// [`ca_bundle`] over the system roots `system` reads.
#[cfg(feature = "tls")]
fn ca_bundle_from(
    tls_section: &TlsSection,
    system: impl FnOnce() -> spate_core::tls::SystemRoots,
) -> Result<Option<String>, String> {
    let Some(root_ca) = tls_section.root_ca.as_deref() else {
        return Ok(None);
    };
    spate_core::tls::TrustRoots::load(system(), Some(root_ca))
        .map(|roots| Some(roots.to_pem()))
        .map_err(|e| e.to_string())
}

/// Set `ssl.ca.location` to `probe` in a `tls` build that links vendored
/// OpenSSL, when the passthrough names no CA, `tls.root_ca` is unset and
/// `openssl_env` is false.
///
/// A build linked against the system OpenSSL keeps librdkafka's native CA
/// default.
pub(crate) fn apply_ca_default(
    cc: &mut rdkafka::ClientConfig,
    rdkafka: &BTreeMap<String, String>,
    root_ca: bool,
    openssl_env: bool,
) {
    if let Some(location) = ca_location_default(
        rdkafka,
        root_ca,
        cfg!(feature = "tls"),
        cfg!(spate_openssl_vendored),
        openssl_env,
    ) {
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
    root_ca: bool,
    tls: bool,
    vendored: bool,
    openssl_env: bool,
) -> Option<&'static str> {
    let names_ca = root_ca
        || ["ssl.ca.location", "ssl.ca.pem"]
            .iter()
            .any(|k| rdkafka.contains_key(*k));
    (tls && vendored && !names_ca && !openssl_env).then_some("probe")
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const CA_CASE: &str = "SPATE_TEST_CA_CASE";

    /// An OpenSSL environment and passthrough, and whether the connector's CA
    /// default applies to them.
    struct CaCase {
        env: &'static [(&'static str, &'static str)],
        rdkafka: &'static [(&'static str, &'static str)],
        defaulted: bool,
    }

    const CA_CASES: &[CaCase] = &[
        CaCase {
            env: &[],
            rdkafka: &[],
            defaulted: true,
        },
        CaCase {
            env: &[("SSL_CERT_FILE", ""), ("SSL_CERT_DIR", "")],
            rdkafka: &[],
            defaulted: true,
        },
        CaCase {
            env: &[("SSL_CERT_FILE", "/etc/kafka/ca.pem")],
            rdkafka: &[],
            defaulted: false,
        },
        CaCase {
            env: &[("SSL_CERT_DIR", "/etc/kafka/certs")],
            rdkafka: &[],
            defaulted: false,
        },
        CaCase {
            env: &[],
            rdkafka: &[("ssl.ca.location", "/etc/kafka/ca.pem")],
            defaulted: false,
        },
        CaCase {
            env: &[],
            rdkafka: &[("ssl.ca.pem", "-----BEGIN CERTIFICATE-----")],
            defaulted: false,
        },
        CaCase {
            env: &[],
            rdkafka: &[("ssl.ca.location", "")],
            defaulted: false,
        },
        CaCase {
            env: &[],
            rdkafka: &[("ssl.ca.pem", "")],
            defaulted: false,
        },
    ];

    /// Runs each case in a child process of the test `name` and checks every
    /// configuration `configs` builds from the case's passthrough: its CA keys
    /// are kept, and `probe` appears exactly where the default applies in this
    /// build.
    pub(crate) fn assert_ca_cases(
        name: &str,
        configs: impl Fn(BTreeMap<String, String>) -> Vec<(&'static str, rdkafka::ClientConfig)>,
    ) {
        if let Some(index) = std::env::var_os(CA_CASE) {
            let index: usize = index
                .to_str()
                .and_then(|i| i.parse().ok())
                .expect("case index");
            let case = &CA_CASES[index];
            let probe = cfg!(all(feature = "tls", spate_openssl_vendored)) && case.defaulted;
            for (label, cc) in configs(map(case.rdkafka)) {
                for key in ["ssl.ca.location", "ssl.ca.pem"] {
                    let given = case
                        .rdkafka
                        .iter()
                        .find(|(k, _)| *k == key)
                        .map(|(_, v)| *v);
                    let expected = given.or((probe && key == "ssl.ca.location").then_some("probe"));
                    assert_eq!(cc.get(key), expected, "{label} {key}, env {:?}", case.env);
                }
            }
            return;
        }
        for (index, case) in CA_CASES.iter().enumerate() {
            spate_test_support::run_in_child(name, |child| {
                child
                    .env_remove("SSL_CERT_FILE")
                    .env_remove("SSL_CERT_DIR")
                    .envs(case.env.iter().copied())
                    .env(CA_CASE, index.to_string())
            });
        }
    }

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
            // librdkafka drops leading whitespace from the protocol.
            map(&[("security.protocol", " plaintext")]),
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
            assert!(
                check(
                    &cfg,
                    &TlsSection::default(),
                    &SectionPath::sink("kafka"),
                    false
                )
                .is_ok(),
                "{cfg:?}"
            );
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
            let at = SectionPath::source("kafka");
            let err =
                check(&cfg, &TlsSection::default(), &at, false).expect_err("tls off must reject");
            let msg = err.to_string();
            assert!(
                msg.starts_with("source.kafka.rdkafka: requests TLS/SASL"),
                "{cfg:?}: {msg}"
            );
            assert!(msg.contains("kafka-tls"), "{cfg:?}: {msg}");
            assert!(
                check(&cfg, &TlsSection::default(), &at, true).is_ok(),
                "tls on: {cfg:?}"
            );
        }
    }

    /// `probe` applies only in a `tls` build that links vendored OpenSSL, whose
    /// passthrough names no CA, whose section sets no `tls.root_ca`, and whose
    /// environment has no OpenSSL override. Regression for #609 and #825.
    #[test]
    fn ca_default_tracks_openssl_provenance() {
        for tls in [false, true] {
            for vendored in [false, true] {
                for openssl_env in [false, true] {
                    for root_ca in [false, true] {
                        for named in [
                            map(&[]),
                            map(&[("ssl.ca.location", "/etc/kafka/ca.pem")]),
                            map(&[("ssl.ca.pem", "-----BEGIN CERTIFICATE-----")]),
                        ] {
                            let expected =
                                (tls && vendored && !openssl_env && !root_ca && named.is_empty())
                                    .then_some("probe");
                            assert_eq!(
                                ca_location_default(&named, root_ca, tls, vendored, openssl_env),
                                expected,
                                "tls {tls}, vendored {vendored}, env {openssl_env}, \
                                 root_ca {root_ca}, {named:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Without the `tls` feature there is no bundle, whatever the section
    /// names, and no file is read.
    #[cfg(not(feature = "tls"))]
    #[test]
    fn no_bundle_without_the_tls_feature() {
        let mut section = TlsSection::default();
        assert_eq!(ca_bundle(&section), Ok(None));
        section.root_ca = Some("/nonexistent/private-ca.pem".into());
        assert_eq!(ca_bundle(&section), Ok(None));
    }

    /// The bundle holds the system roots, then the certificates in
    /// `root_ca`. Without `root_ca` there is none, and the system store is
    /// left unread. An unreadable `root_ca` is refused with core's reason.
    #[cfg(feature = "tls")]
    #[test]
    fn the_bundle_keeps_the_system_roots_and_adds_root_ca() {
        use spate_core::tls::{SystemRoots, read_root_ca};
        use spate_test_support::TestCa;

        let dir = tempfile::tempdir().unwrap();
        let (system, private) = (TestCa::new("system"), TestCa::new("private"));
        let mut section = TlsSection::default();
        assert_eq!(
            ca_bundle_from(&section, || panic!("no root_ca reads no system root")),
            Ok(None)
        );
        section.root_ca = Some(private.write(dir.path()));
        let pem = ca_bundle_from(&section, || SystemRoots::from_certs(vec![system.der()]))
            .unwrap()
            .expect("a bundle");
        let written = dir.path().join("bundle.pem");
        std::fs::write(&written, pem).unwrap();
        assert_eq!(
            read_root_ca(&written).unwrap(),
            vec![system.der(), private.der()]
        );
        section.root_ca = Some(dir.path().join("missing.pem"));
        let why = ca_bundle_from(&section, SystemRoots::default).unwrap_err();
        assert!(why.contains("missing.pem"), "{why}");
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
            spate_test_support::run_in_child(NAME, |child| {
                child
                    .env_remove("SSL_CERT_FILE")
                    .env_remove("SSL_CERT_DIR")
                    .envs(vars.iter().copied())
                    .env(EXPECT, expected)
            });
        }
    }

    /// A non-empty `sasl.password` over `sasl_plaintext`, in any case, is
    /// sent without TLS. Every other protocol, no protocol, and an empty or
    /// absent password are not.
    #[test]
    fn only_a_password_over_sasl_plaintext_is_sent_without_tls() {
        for (case, (protocol, secret, cleartext)) in [
            (Some("sasl_plaintext"), Some("p"), true),
            (Some("SASL_PLAINTEXT"), Some("p"), true),
            (Some(" sasl_plaintext"), Some("p"), true),
            (Some("sasl_plaintext"), Some(""), false),
            (Some("sasl_plaintext"), None, false),
            (Some("sasl_ssl"), Some("p"), false),
            (Some("plaintext"), Some("p"), false),
            (Some("ssl"), Some("p"), false),
            (None, Some("p"), false),
        ]
        .into_iter()
        .enumerate()
        {
            let rdkafka: BTreeMap<String, String> = [
                protocol.map(|v| ("security.protocol", v)),
                secret.map(|v| ("sasl.password", v)),
            ]
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
            assert_eq!(
                sends_password_without_tls(&rdkafka),
                cleartext,
                "case {case}"
            );
        }
    }
}
