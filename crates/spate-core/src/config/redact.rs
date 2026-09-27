//! `Debug` and `Display` adapters that keep credential values out of
//! formatted output, for hand-written `Debug` impls on config types, and the
//! same guarantee for config error messages.
//!
//! Every adapter redacts all values it prints, whatever their key or name.

use serde_yaml::Value;
use std::borrow::Cow;
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
/// `@` after the scheme, so an `@` later in the path redacts the host as well.
/// When a `?` or `#` comes before that `@`, everything after the scheme is
/// redacted. A string without `://` is read the same way from its start, so
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
        let at = rest.rfind('@');
        if let (Some(at), Some(tail)) = (at, rest.find(['?', '#']))
            && tail < at
        {
            return f.write_str("<redacted>");
        }
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

/// A serde error message with the value removed from its `invalid type`,
/// `invalid value` or `unknown variant` clause.
///
/// The path, the expected type and any position stay. A message without one
/// of those clauses is returned unchanged.
pub(crate) fn error_message(message: &str) -> Cow<'_, str> {
    const TYPE: &str = "invalid type: ";
    const VALUE: &str = "invalid value: ";
    const VARIANT: &str = "unknown variant `";

    // The clause opens before the value, so the earliest marker is the real one.
    let Some((at, marker)) = [TYPE, VALUE, VARIANT]
        .into_iter()
        .filter_map(|marker| message.find(marker).map(|at| (at, marker)))
        .min()
    else {
        return Cow::Borrowed(message);
    };
    let start = at + marker.len();

    // The expected half is built from type and variant names, so the last
    // separator is the one that closes the value.
    let separators: &[&str] = if marker == VARIANT {
        &["`, expected ", "`, there are no variants"]
    } else {
        &[", expected "]
    };
    let Some(end) = separators
        .iter()
        .filter_map(|sep| message.rfind(sep))
        .filter(|&end| end >= start)
        .max()
    else {
        return Cow::Borrowed(message);
    };

    if marker == VARIANT {
        return Cow::Owned(format!(
            "{}unknown variant{}",
            &message[..at],
            &message[end + 1..]
        ));
    }
    let unexpected = &message[start..end];
    let kind = unexpected_kind(unexpected);
    if kind == unexpected {
        return Cow::Borrowed(message);
    }
    Cow::Owned(format!("{}{kind}{}", &message[..start], &message[end..]))
}

/// The kind named by a rendered `serde::de::Unexpected`, without its value.
fn unexpected_kind(unexpected: &str) -> &str {
    const VALUE_FREE: [&str; 11] = [
        "unit value",
        "Option value",
        "byte array",
        "newtype struct",
        "sequence",
        "map",
        "enum",
        "unit variant",
        "newtype variant",
        "tuple variant",
        "struct variant",
    ];
    if VALUE_FREE.contains(&unexpected) {
        return unexpected;
    }
    [
        "boolean",
        "integer",
        "floating point",
        "character",
        "string",
    ]
    .into_iter()
    .find(|kind| {
        unexpected
            .strip_prefix(kind)
            .is_some_and(|rest| rest.starts_with(' '))
    })
    .unwrap_or("value")
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
            // A raw `/` inside the password still ends at the last `@`.
            ("https://u:hun/ter2@sr/x", "https://<redacted>@sr/x"),
            // A `?` or `#` before the last `@` cannot be placed, so all of it goes.
            ("https://u:hun?ter2@sr/x", "https://<redacted>"),
            (
                "http://ch:8123/?user=svc@corp&password=hunter2",
                "http://<redacted>",
            ),
            ("http://ch:8123/#a@hunter2", "http://<redacted>"),
            (
                "http://ch:8123/p@x?password=hunter2",
                "http://<redacted>@x?<redacted>",
            ),
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

    #[test]
    fn error_message_drops_the_value_and_keeps_the_rest() {
        let cases = [
            (
                "invalid type: integer `918273645`, expected a string",
                "invalid type: integer, expected a string",
            ),
            (
                "invalid type: boolean `true`, expected a string",
                "invalid type: boolean, expected a string",
            ),
            (
                "invalid type: floating point `1500.0`, expected u8",
                "invalid type: floating point, expected u8",
            ),
            (
                "invalid type: character `h`, expected u8",
                "invalid type: character, expected u8",
            ),
            (
                "invalid value: string \"hunter2\", expected a duration",
                "invalid value: string, expected a duration",
            ),
            (
                "invalid value: integer `-5` as i128, expected u16",
                "invalid value: integer, expected u16",
            ),
            (
                "invalid type: i128, expected a string",
                "invalid type: value, expected a string",
            ),
            (
                "invalid value: string \"a, expected b\", expected u16",
                "invalid value: string, expected u16",
            ),
            (
                "unknown variant `hunter2`, expected `plain` or `scram`",
                "unknown variant, expected `plain` or `scram`",
            ),
            (
                "unknown variant `a`, expected b`, expected `plain`",
                "unknown variant, expected `plain`",
            ),
            (
                "unknown variant `hunter2`, there are no variants",
                "unknown variant, there are no variants",
            ),
            (
                "a.port: invalid type: string \"invalid value: x\", expected u16 at line 1 column 7",
                "a.port: invalid type: string, expected u16 at line 1 column 7",
            ),
        ];
        for (raw, want) in cases {
            assert_eq!(error_message(raw), want, "{raw}");
        }
    }

    #[test]
    fn error_message_leaves_value_free_messages_unchanged() {
        for raw in [
            "invalid type: map, expected a string",
            "invalid type: unit value, expected a string",
            "invalid length 2, expected a tuple of size 3",
            "missing field `topic`",
            "unknown field `bogus`, expected `brokers` or `topic`",
            "mapping values are not allowed in this context at line 2 column 6",
        ] {
            assert!(matches!(error_message(raw), Cow::Borrowed(_)), "{raw}");
        }
    }
}
