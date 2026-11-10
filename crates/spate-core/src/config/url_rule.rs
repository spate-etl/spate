//! A load-time check that a URL in a config parses, uses a scheme the
//! connector reads and carries no userinfo.
//!
//! A [`UrlRule`] holds the accepted schemes and where credentials go instead
//! of the URL. [`UrlRule::check`] returns the parsed [`Url`], or a
//! [`UrlRuleError`] whose message states the problem without the key or the
//! value. The caller puts the key in front with
//! [`SectionPath::invalid`](super::SectionPath::invalid), so every connector
//! refuses a URL in one wording.

use std::fmt;
use url::Url;

/// The schemes a URL key accepts, and where its credentials go.
///
/// Built in a `const` item. The scheme comparison ignores ASCII case, as the
/// URL parser does.
#[derive(Clone, Copy, Debug)]
pub struct UrlRule<'a> {
    schemes: &'a [&'a str],
    credentials: &'a str,
}

impl<'a> UrlRule<'a> {
    /// A rule that accepts any scheme and refuses userinfo.
    ///
    /// `credentials` tells the user where credentials go instead, such as
    /// `set them in user and password`. The refusal of a URL carrying
    /// userinfo ends with it.
    #[must_use]
    pub const fn new(credentials: &'a str) -> Self {
        Self {
            schemes: &[],
            credentials,
        }
    }

    /// This rule, accepting only `schemes`.
    ///
    /// # Panics
    ///
    /// If `schemes` is empty. In a `const` item this fails to compile.
    #[must_use]
    pub const fn schemes(self, schemes: &'a [&'a str]) -> Self {
        assert!(
            !schemes.is_empty(),
            "a URL rule needs at least one scheme; UrlRule::new accepts any"
        );
        Self { schemes, ..self }
    }

    /// Parses `value` and checks it against this rule.
    ///
    /// # Errors
    ///
    /// [`UrlRuleError`] when `value` is not a URL, when its scheme is not
    /// one of the rule's, or when it names a user or a password. The checks
    /// run in that order.
    ///
    /// # Examples
    ///
    /// ```
    /// use spate_core::config::{SectionPath, UrlRule};
    ///
    /// const ENDPOINT: UrlRule<'static> =
    ///     UrlRule::new("set them in user and password").schemes(&["http", "https"]);
    ///
    /// let url = ENDPOINT.check("https://ch-0:8443").unwrap();
    /// assert_eq!(url.host_str(), Some("ch-0"));
    ///
    /// let err = ENDPOINT.check("https://svc:hunter2@ch-0:8443").unwrap_err();
    /// assert_eq!(
    ///     SectionPath::sink("clickhouse").invalid("distributed_check.endpoint", err.to_string()).to_string(),
    ///     "sink.clickhouse.distributed_check.endpoint: carries userinfo; set them in user and password"
    /// );
    /// ```
    pub fn check(&self, value: &str) -> Result<Url, UrlRuleError> {
        let url = Url::parse(value).map_err(|e| UrlRuleError(Refusal::NotAUrl(e)))?;
        if !self.schemes.is_empty()
            && !self
                .schemes
                .iter()
                .any(|scheme| scheme.eq_ignore_ascii_case(url.scheme()))
        {
            return Err(UrlRuleError(Refusal::Scheme {
                found: url.scheme().to_owned(),
                accepted: self.schemes.iter().map(|&s| s.to_owned()).collect(),
            }));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(UrlRuleError(Refusal::Userinfo {
                credentials: self.credentials.to_owned(),
            }));
        }
        Ok(url)
    }
}

/// A URL that [`UrlRule::check`] refused.
///
/// Its message starts where the key ends and never holds the URL, such as
/// `must be a URL with scheme http or https; found ftp`. The caller puts the
/// key in front of it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct UrlRuleError(Refusal);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Refusal {
    NotAUrl(url::ParseError),
    Scheme {
        found: String,
        accepted: Vec<String>,
    },
    Userinfo {
        credentials: String,
    },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // A parse error names its kind only, never the input.
            Refusal::NotAUrl(e) => write!(f, "is not a URL: {e}"),
            Refusal::Scheme { found, accepted } => {
                f.write_str("must be a URL with scheme ")?;
                for (i, scheme) in accepted.iter().enumerate() {
                    let sep = match i {
                        0 => "",
                        _ if i + 1 == accepted.len() => " or ",
                        _ => ", ",
                    };
                    write!(f, "{sep}{scheme}")?;
                }
                write!(f, "; found {found}")
            }
            Refusal::Userinfo { credentials } => write!(f, "carries userinfo; {credentials}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HINT: &str = "set them in user and password";
    const ANY: UrlRule<'static> = UrlRule::new(HINT);
    const HTTP: UrlRule<'static> = ANY.schemes(&["http", "https"]);

    fn refused(rule: UrlRule<'_>, value: &str) -> String {
        rule.check(value)
            .expect_err(&format!("{value} should be refused"))
            .to_string()
    }

    #[test]
    fn an_accepted_url_is_returned_parsed() {
        let url = HTTP.check("http://ch-0:8123/?database=orders").unwrap();
        assert_eq!(url.as_str(), "http://ch-0:8123/?database=orders");
    }

    #[test]
    fn a_value_that_does_not_parse_is_refused() {
        assert_eq!(
            refused(HTTP, "/orders/ingest"),
            "is not a URL: relative URL without a base"
        );
        assert_eq!(
            refused(ANY, "http://u:hunter2@exa mple:8123"),
            "is not a URL: invalid international domain name"
        );
    }

    #[test]
    fn a_scheme_off_the_list_is_refused_naming_the_list() {
        assert_eq!(
            refused(HTTP, "ftp://ch-0"),
            "must be a URL with scheme http or https; found ftp"
        );
        assert_eq!(
            refused(UrlRule::new(HINT).schemes(&["s3"]), "file:///tmp"),
            "must be a URL with scheme s3; found file"
        );
        assert_eq!(
            refused(UrlRule::new(HINT).schemes(&["s3", "s3a", "file"]), "gs://b"),
            "must be a URL with scheme s3, s3a or file; found gs"
        );
    }

    /// The parser lowercases the scheme, so the comparison does too.
    #[test]
    fn the_scheme_matches_in_any_case() {
        assert_eq!(HTTP.check("HTTPS://ch-0").unwrap().scheme(), "https");
        let upper = UrlRule::new(HINT).schemes(&["HTTPS"]);
        assert_eq!(upper.check("https://ch-0").unwrap().scheme(), "https");
    }

    #[test]
    fn a_rule_without_schemes_accepts_any() {
        assert_eq!(ANY.check("s3://bucket/data/").unwrap().scheme(), "s3");
    }

    #[test]
    fn a_user_a_password_or_both_are_refused() {
        for value in [
            "https://svc@ch-0",
            "https://:hunter2@ch-0",
            "https://svc:hunter2@ch-0",
        ] {
            assert_eq!(
                refused(HTTP, value),
                "carries userinfo; set them in user and password",
                "{value}"
            );
        }
        assert_eq!(
            refused(ANY, "s3://svc:hunter2@bucket/data/"),
            "carries userinfo; set them in user and password"
        );
    }

    /// The scheme is reported before the userinfo.
    #[test]
    fn a_foreign_scheme_with_userinfo_reports_the_scheme() {
        assert_eq!(
            refused(HTTP, "ftp://svc:hunter2@ch-0"),
            "must be a URL with scheme http or https; found ftp"
        );
    }

    #[test]
    fn no_refusal_echoes_the_password() {
        for value in [
            "https://svc:hunter2@ch-0",
            "ftp://svc:hunter2@ch-0",
            "http://svc:hunter2@exa mple",
        ] {
            let err = HTTP.check(value).unwrap_err();
            assert!(!err.to_string().contains("hunter2"), "{err}");
            assert!(!format!("{err:?}").contains("hunter2"), "{err:?}");
        }
    }
}
