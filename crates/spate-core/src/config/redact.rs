//! `Debug` and `Display` adapters that keep credential values out of
//! formatted output, for hand-written `Debug` impls on config types.
//!
//! Every adapter fails closed: it redacts each value it cannot prove is
//! structure, rather than matching a list of sensitive names.

use serde_yaml::Value;
use std::fmt;

/// Formats as `<redacted>` under both `Debug` and `Display`.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Display for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// A map's `Debug` form, from its keys, with every value shown as
/// `<redacted>`.
pub fn map<I>(keys: I) -> impl fmt::Debug
where
    I: IntoIterator + Clone,
    I::Item: fmt::Debug,
{
    Map(keys)
}

struct Map<I>(I);

impl<I> fmt::Debug for Map<I>
where
    I: IntoIterator + Clone,
    I::Item: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.clone().into_iter().map(|k| (k, Redacted)))
            .finish()
    }
}

/// `None`, or `Some(<redacted>)` whatever the value.
pub fn option<T>(value: &Option<T>) -> impl fmt::Debug + '_ {
    value.as_ref().map(|_| Redacted)
}

/// A URL with its userinfo and everything from the first `?` or `#` after
/// the host replaced by `<redacted>`.
///
/// The scheme, host, port and path stay visible. Userinfo runs to the last
/// `@` after the scheme, so an `@` later in the URL redacts the host as well.
/// A string without `://` is read the same way from its start, so
/// `user:pass@host:4222` renders as `<redacted>@host:4222`.
pub fn url(url: &str) -> impl fmt::Debug + fmt::Display + '_ {
    Url(url)
}

struct Url<'a>(&'a str);

impl fmt::Display for Url<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rest = match self.0.split_once("://") {
            Some((scheme, rest)) => {
                write!(f, "{scheme}://")?;
                rest
            }
            None => self.0,
        };
        let rest = match rest.rsplit_once('@') {
            Some((_, host)) => {
                f.write_str("<redacted>@")?;
                host
            }
            None => rest,
        };
        match rest.find(['?', '#']) {
            Some(at) => write!(f, "{}{}<redacted>", &rest[..at], &rest[at..=at]),
            None => f.write_str(rest),
        }
    }
}

impl fmt::Debug for Url<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.to_string(), f)
    }
}

/// A YAML tree with its mapping keys and shape kept and every scalar leaf
/// shown as `<redacted>`.
pub(crate) fn yaml(value: &Value) -> impl fmt::Debug + '_ {
    Yaml(value)
}

struct Yaml<'a>(&'a Value);

impl fmt::Debug for Yaml<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Value::Null => f.write_str("null"),
            Value::Bool(_) | Value::Number(_) | Value::String(_) => Redacted.fmt(f),
            Value::Sequence(items) => f.debug_list().entries(items.iter().map(Yaml)).finish(),
            Value::Mapping(map) => f
                .debug_map()
                .entries(map.iter().map(|(k, v)| (YamlKey(k), Yaml(v))))
                .finish(),
            Value::Tagged(tagged) => write!(f, "{} {:?}", tagged.tag, Yaml(&tagged.value)),
        }
    }
}

/// A mapping key: a string key prints as written, any other key as a leaf.
struct YamlKey<'a>(&'a Value);

impl fmt::Debug for YamlKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Value::String(key) => key.fmt(f),
            other => Yaml(other).fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shown(value: impl fmt::Debug) -> String {
        format!("{value:?}")
    }

    #[test]
    fn url_redacts_userinfo_query_and_fragment() {
        let cases = [
            ("https://sr:8081/x", "https://sr:8081/x"),
            (
                "https://svc:hunter2@sr:8081/x",
                "https://<redacted>@sr:8081/x",
            ),
            ("https://hunter2@sr", "https://<redacted>@sr"),
            (
                "http://ch:8123/?password=hunter2",
                "http://ch:8123/?<redacted>",
            ),
            ("http://ch:8123#hunter2", "http://ch:8123#<redacted>"),
            // A raw `/`, `?` or `#` inside the password still ends at the last `@`.
            ("https://u:hun/te?r2@sr/x", "https://<redacted>@sr/x"),
            ("nats://a:hunter2@n1:4222", "nats://<redacted>@n1:4222"),
        ];
        for (raw, want) in cases {
            assert_eq!(url(raw).to_string(), want, "{raw}");
            assert_eq!(shown(url(raw)), format!("{want:?}"), "{raw}");
        }
    }

    #[test]
    fn url_without_a_scheme_redacts_the_same_parts() {
        let cases = [
            ("n1:4222", "n1:4222"),
            ("svc:hunter2@n1:4222", "<redacted>@n1:4222"),
            ("n1:4222?token=hunter2", "n1:4222?<redacted>"),
        ];
        for (raw, want) in cases {
            assert_eq!(url(raw).to_string(), want, "{raw}");
        }
    }

    #[test]
    fn map_keeps_keys() {
        let pairs =
            std::collections::BTreeMap::from([("sasl.password", "hunter2"), ("acks", "all")]);
        assert_eq!(
            shown(map(pairs.keys())),
            r#"{"acks": <redacted>, "sasl.password": <redacted>}"#
        );
    }

    #[test]
    fn option_shows_presence() {
        assert_eq!(shown(option(&Some("hunter2"))), "Some(<redacted>)");
        assert_eq!(shown(option::<String>(&None)), "None");
    }

    #[test]
    fn yaml_keeps_keys_and_shape() {
        let value: Value = serde_yaml::from_str(
            "{url: https://sr, password: hunter2, port: 8081, tls: ~, list: [a, true], 7: x}",
        )
        .unwrap();
        assert_eq!(
            shown(yaml(&value)),
            "{\"url\": <redacted>, \"password\": <redacted>, \"port\": <redacted>, \
             \"tls\": null, \"list\": [<redacted>, <redacted>], <redacted>: <redacted>}"
        );
        let tagged: Value = serde_yaml::from_str("!secret hunter2").unwrap();
        assert_eq!(shown(yaml(&tagged)), "!secret <redacted>");
    }
}
