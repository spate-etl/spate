//! The trust roots a connector's TLS client verifies a server against: the
//! system trust store and the certificates in the connector's `root_ca`.
//!
//! [`TrustRoots::load`] keeps the system certificates that rustls accepts as
//! trust anchors, then adds every certificate in `root_ca`. There is no
//! fallback bundle. A connector with an `https://` endpoint calls
//! [`TrustRoots::require_any`], so an empty set fails at load with the fixes
//! named; one with no `https://` endpoint keeps the set, empty or not
//! (ADR-0105).
//!
//! The roots are DER certificates, which every rustls-based client takes: a
//! `rustls::RootCertStore` through `add_parsable_certificates`, a reqwest or
//! `object_store` certificate through `from_der`, or one PEM bundle through
//! [`TrustRoots::to_pem`].
//!
//! Enabled by the `tls-roots` feature.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
pub use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use serde::Deserialize;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// A connector's `tls:` section.
///
/// ```yaml
/// tls:
///   root_ca: /etc/spate/private-ca.pem
/// ```
///
/// Construct with [`TlsSection::default`] and set the fields. The struct is
/// `#[non_exhaustive]` so new knobs can be added without breaking callers.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TlsSection {
    /// PEM bundle of root CAs trusted in addition to the system trust store.
    /// Requires an `https://` endpoint.
    #[serde(default)]
    pub root_ca: Option<PathBuf>,
}

/// What the system trust store yielded.
#[derive(Clone, Debug)]
pub struct SystemRoots {
    certs: Vec<CertificateDer<'static>>,
    errors: Vec<String>,
}

impl SystemRoots {
    /// Reads the system trust store. `SSL_CERT_FILE` and `SSL_CERT_DIR`
    /// replace it when set.
    ///
    /// Reading blocks on the file system, or on the keychain on macOS, with no
    /// bound of its own; an async caller runs it on a blocking thread.
    #[must_use]
    pub fn load() -> Self {
        Self::from_native(rustls_native_certs::load_native_certs())
    }

    fn from_native(loaded: rustls_native_certs::CertificateResult) -> Self {
        SystemRoots {
            certs: loaded.certs,
            errors: loaded.errors.iter().map(ToString::to_string).collect(),
        }
    }

    /// A store that yielded `certs` and no errors.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn from_certs(certs: Vec<CertificateDer<'static>>) -> Self {
        SystemRoots {
            certs,
            errors: Vec::new(),
        }
    }
}

/// The certificates a client verifies servers against.
#[derive(Clone, Debug)]
pub struct TrustRoots {
    certs: Vec<CertificateDer<'static>>,
    system_found: usize,
    system_errors: Vec<String>,
}

impl TrustRoots {
    /// The certificates in `system` that are valid trust anchors, then every
    /// certificate in `root_ca`.
    ///
    /// The set may be empty; [`require_any`](Self::require_any) refuses that.
    ///
    /// # Errors
    ///
    /// [`TrustError::RootCa`] when `root_ca` cannot be used, as
    /// [`read_root_ca`] says.
    pub fn load(system: SystemRoots, root_ca: Option<&Path>) -> Result<Self, TrustError> {
        let found = system.certs.len();
        let mut certs: Vec<_> = system.certs.into_iter().filter(usable).collect();
        let refused = found - certs.len();
        if !certs.is_empty() && (refused > 0 || !system.errors.is_empty()) {
            tracing::debug!(
                refused,
                errors = ?system.errors,
                "part of the system trust store did not load"
            );
        }
        if let Some(path) = root_ca {
            certs.extend(read_root_ca(path)?);
        }
        Ok(TrustRoots {
            certs,
            system_found: found,
            system_errors: system.errors,
        })
    }

    /// `self` when it holds a certificate.
    ///
    /// # Errors
    ///
    /// [`TrustError::NoRoots`] when it holds none.
    pub fn require_any(self) -> Result<Self, TrustError> {
        if self.certs.is_empty() {
            return Err(TrustError::NoRoots {
                found: self.system_found,
                errors: self.system_errors,
            });
        }
        Ok(self)
    }

    /// The certificates, system ones first.
    #[must_use]
    pub fn certs(&self) -> &[CertificateDer<'static>] {
        &self.certs
    }

    /// The certificates, system ones first.
    #[must_use]
    pub fn into_certs(self) -> Vec<CertificateDer<'static>> {
        self.certs
    }

    /// The certificates as one PEM bundle, in order.
    #[must_use]
    pub fn to_pem(&self) -> String {
        let mut pem = String::new();
        for cert in &self.certs {
            let body = STANDARD.encode(cert);
            pem.push_str("-----BEGIN CERTIFICATE-----\n");
            for line in body.as_bytes().chunks(64) {
                // Base64 output is ASCII, so each chunk is UTF-8.
                pem.push_str(std::str::from_utf8(line).unwrap_or_default());
                pem.push('\n');
            }
            pem.push_str("-----END CERTIFICATE-----\n");
        }
        pem
    }
}

/// Every certificate in the PEM file at `path`.
///
/// # Errors
///
/// [`TrustError::RootCa`] when the file cannot be read, holds no certificate,
/// or holds one that is not a valid trust anchor.
pub fn read_root_ca(path: &Path) -> Result<Vec<CertificateDer<'static>>, TrustError> {
    let fail = |reason: String| TrustError::RootCa {
        path: path.to_path_buf(),
        reason,
    };
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| fail(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| fail(e.to_string()))?;
    if certs.is_empty() {
        return Err(fail("no PEM certificates found".into()));
    }
    for (n, cert) in certs.iter().enumerate() {
        webpki::anchor_from_trusted_cert(cert).map_err(|e| {
            fail(format!(
                "certificate {} is not a valid trust anchor: {e}",
                n + 1
            ))
        })?;
    }
    Ok(certs)
}

/// The check rustls's `RootCertStore` runs on each root it is given.
fn usable(cert: &CertificateDer<'_>) -> bool {
    webpki::anchor_from_trusted_cert(cert).is_ok()
}

/// Why trust roots could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TrustError {
    /// The connector's `root_ca` file cannot be used. The caller anchors it at
    /// that key.
    #[error("`{}`: {reason}", path.display())]
    #[non_exhaustive]
    RootCa {
        /// The file.
        path: PathBuf,
        /// Why it cannot be used.
        reason: String,
    },
    /// No certificate is left to verify a server against. The caller anchors
    /// it at its `root_ca` key, which is one of the fixes.
    #[error("{}", no_roots(*found, errors))]
    #[non_exhaustive]
    NoRoots {
        /// How many certificates the system trust store yielded.
        found: usize,
        /// The errors reading the store.
        errors: Vec<String>,
    },
}

fn no_roots(found: usize, errors: &[String]) -> String {
    let mut text = if found == 0 {
        "the system trust store has no certificates".to_owned()
    } else {
        format!("none of the {found} certificates in the system trust store parsed")
    };
    if !errors.is_empty() {
        let _ = write!(text, " (errors reading it: {})", errors.join("; "));
    }
    text.push_str(
        "; install a CA bundle, point SSL_CERT_FILE at one, or set this key to the issuing CA",
    );
    text
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use spate_test::capture_logs;
    use spate_test_support::TestCa;
    use tracing::Level;

    fn unparsable() -> CertificateDer<'static> {
        CertificateDer::from(vec![0u8; 8])
    }

    fn read_error(path: &str) -> rustls_native_certs::Error {
        rustls_native_certs::Error {
            context: "failed to read PEM from file",
            kind: rustls_native_certs::ErrorKind::Io {
                inner: std::io::Error::other("permission denied"),
                path: path.into(),
            },
        }
    }

    /// A store that yielded `certs` and one read error naming `path`.
    fn with_read_error(certs: Vec<CertificateDer<'static>>, path: &str) -> SystemRoots {
        let mut loaded = spate_test_support::native_certs(certs);
        loaded.errors.push(read_error(path));
        SystemRoots::from_native(loaded)
    }

    /// A store with no usable certificate loads as an empty set, and
    /// `require_any` refuses it with the count found and each read error.
    #[test]
    fn no_usable_system_certificate_is_refused_by_require_any() {
        for (store, expect) in [
            (
                SystemRoots::from_certs(Vec::new()),
                "the system trust store has no certificates; install a CA bundle",
            ),
            (
                SystemRoots::from_certs(vec![unparsable(), unparsable()]),
                "none of the 2 certificates in the system trust store parsed; install",
            ),
            (
                with_read_error(vec![unparsable()], "/etc/ssl/certs/a.pem"),
                "none of the 1 certificates in the system trust store parsed (errors \
                 reading it: failed to read PEM from file: permission denied at \
                 '/etc/ssl/certs/a.pem'); install",
            ),
        ] {
            let roots = TrustRoots::load(store, None).unwrap();
            assert!(roots.certs().is_empty());
            let err = roots.require_any().unwrap_err();
            assert!(matches!(err, TrustError::NoRoots { .. }), "{err:?}");
            let text = err.to_string();
            assert!(text.starts_with(expect), "{text}");
            assert!(
                text.ends_with("or set this key to the issuing CA"),
                "{text}"
            );
        }
    }

    /// A `root_ca` fills a set the system store left empty.
    #[test]
    fn root_ca_alone_satisfies_require_any() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("private");
        let path = ca.write(dir.path());
        for store in [
            SystemRoots::from_certs(Vec::new()),
            SystemRoots::from_certs(vec![unparsable()]),
        ] {
            let roots = TrustRoots::load(store, Some(&path))
                .unwrap()
                .require_any()
                .unwrap();
            assert_eq!(roots.into_certs(), vec![ca.der()]);
        }
    }

    /// An unusable system certificate is dropped, so none reaches a client
    /// that would refuse or panic on it.
    #[test]
    fn unusable_system_certificates_are_dropped() {
        let ca = TestCa::new("system");
        let roots =
            TrustRoots::load(SystemRoots::from_certs(vec![unparsable(), ca.der()]), None).unwrap();
        assert_eq!(roots.certs(), [ca.der()]);
        let parsed = CertificateDer::pem_slice_iter(roots.to_pem().as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(parsed, vec![ca.der()]);
    }

    /// A store that yielded usable certificates loads, and a partial read is
    /// logged at debug and nowhere above it; a clean read logs nothing.
    #[test]
    fn a_partial_system_store_loads_and_logs_at_debug() {
        let ca = TestCa::new("system");
        let partial = || {
            [
                with_read_error(vec![ca.der()], "/etc/ssl/certs/a.pem"),
                SystemRoots::from_certs(vec![ca.der(), unparsable()]),
            ]
        };
        let line = "part of the system trust store did not load";
        for store in partial() {
            let mut loaded = None;
            let debug = capture_logs(Level::DEBUG, || {
                loaded = Some(TrustRoots::load(store, None).unwrap());
            });
            assert_eq!(loaded.unwrap().certs(), [ca.der()]);
            assert!(debug.iter().any(|l| l.contains(line)), "{debug:?}");
        }
        for store in partial() {
            let info = capture_logs(Level::INFO, || {
                TrustRoots::load(store, None).unwrap();
            });
            assert!(!info.iter().any(|l| l.contains(line)), "{info:?}");
        }
        let clean = capture_logs(Level::DEBUG, || {
            TrustRoots::load(SystemRoots::from_certs(vec![ca.der()]), None).unwrap();
        });
        assert!(!clean.iter().any(|l| l.contains(line)), "{clean:?}");
    }

    /// A `root_ca` that is missing, holds no certificate, or holds any
    /// certificate that is not a trust anchor fails with its path and why.
    #[test]
    fn an_unusable_root_ca_fails_with_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("private");
        let write = |name: &str, body: String| {
            let path = dir.path().join(name);
            std::fs::write(&path, body).unwrap();
            path
        };
        let bad = spate_test_support::pem("CERTIFICATE", &unparsable());
        let good = spate_test_support::pem("CERTIFICATE", &ca.der());
        for (path, why) in [
            (dir.path().join("missing.pem"), ""),
            (
                write("empty.pem", "no pem here\n".into()),
                "no PEM certificates found",
            ),
            (
                write("bad.pem", bad.clone()),
                "certificate 1 is not a valid trust anchor",
            ),
            (
                write("mixed.pem", format!("{good}{bad}")),
                "certificate 2 is not a valid trust anchor",
            ),
        ] {
            let err = read_root_ca(&path).unwrap_err();
            assert!(matches!(err, TrustError::RootCa { .. }), "{err:?}");
            let text = err.to_string();
            assert!(
                text.starts_with(&format!("`{}`: ", path.display())),
                "{text}"
            );
            assert!(text.contains(why), "{text}");
        }
        assert_eq!(
            read_root_ca(&write("good.pem", good)).unwrap(),
            vec![ca.der()]
        );
    }

    /// `root_ca` adds to the system roots, after them.
    #[test]
    fn root_ca_is_appended_after_the_system_roots() {
        let dir = tempfile::tempdir().unwrap();
        let system = TestCa::new("system");
        let private = TestCa::new("private");
        let roots = TrustRoots::load(
            SystemRoots::from_certs(vec![system.der()]),
            Some(&private.write(dir.path())),
        )
        .unwrap();
        assert_eq!(roots.into_certs(), vec![system.der(), private.der()]);
    }

    /// The PEM bundle holds each certificate in order, in 64-column lines.
    #[test]
    fn to_pem_round_trips_in_order() {
        let (a, b) = (TestCa::new("a"), TestCa::new("b"));
        let roots =
            TrustRoots::load(SystemRoots::from_certs(vec![a.der(), b.der()]), None).unwrap();
        let pem = roots.to_pem();
        assert!(pem.lines().all(|l| l.len() <= 64), "{pem}");
        assert_eq!(
            pem,
            format!(
                "{}{}",
                spate_test_support::pem("CERTIFICATE", &a.der()),
                spate_test_support::pem("CERTIFICATE", &b.der())
            )
        );
        let parsed = CertificateDer::pem_slice_iter(pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(parsed, vec![a.der(), b.der()]);
    }

    /// A native-certs result keeps its certificates and its errors.
    #[test]
    fn from_native_keeps_certificates_and_errors() {
        let ca = TestCa::new("system");
        let system = with_read_error(vec![ca.der()], "/etc/ssl/certs/a.pem");
        assert_eq!(system.certs, vec![ca.der()]);
        assert_eq!(
            system.errors,
            vec!["failed to read PEM from file: permission denied at '/etc/ssl/certs/a.pem'"]
        );
    }

    /// The section parses `root_ca`, defaults to none, and refuses an
    /// unknown key.
    #[test]
    fn tls_section_parses_root_ca_and_refuses_unknown_keys() {
        let parsed: TlsSection = serde_yaml::from_str("root_ca: /etc/ca.pem").unwrap();
        assert_eq!(parsed.root_ca, Some(PathBuf::from("/etc/ca.pem")));
        let empty: TlsSection = serde_yaml::from_str("{}").unwrap();
        assert_eq!(empty, TlsSection::default());
        let err = serde_yaml::from_str::<TlsSection>("root_cert: /etc/ca.pem").unwrap_err();
        assert!(
            err.to_string().contains("unknown field `root_cert`"),
            "{err}"
        );
    }
}
