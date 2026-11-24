//! A connector's `tls:` section, and the trust roots its TLS client verifies
//! a server against: the system trust store and the certificates in the
//! section's `root_ca`.
//!
//! [`TlsSection`] is always available. The loader needs the `tls-roots`
//! feature.
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
//! A reqwest-based client takes its roots from [`client_roots`]. Where
//! [`PLATFORM_VERIFIER`] is set it keeps reqwest's platform verifier and adds
//! the `root_ca` certificates. Elsewhere it verifies against the loaded roots
//! alone. [`is_https`] tells a connector whether an endpoint is called over
//! TLS.

use serde::Deserialize;
use std::path::PathBuf;

#[cfg(feature = "tls-roots")]
mod roots;
#[cfg(feature = "tls-roots")]
pub use roots::*;

/// A connector's `tls:` section.
///
/// ```yaml
/// tls:
///   root_ca: /etc/spate/private-ca.pem
/// ```
///
/// Construct with [`TlsSection::default`] and set the fields. The struct is
/// `#[non_exhaustive]` so new knobs can be added without breaking callers.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TlsSection {
    /// PEM bundle of root CAs trusted in addition to the system trust store.
    /// Each connector's docs say what the key requires, such as an
    /// `https://` endpoint.
    #[serde(default)]
    pub root_ca: Option<PathBuf>,
}
