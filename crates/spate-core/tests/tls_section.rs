//! `TlsSection` parses in every build, with or without `tls-roots`, so a
//! connector can take the `tls:` key before it builds the loader.

use spate_core::tls::TlsSection;
use std::path::PathBuf;

/// The section parses `root_ca`, defaults to none, and refuses an unknown key.
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
