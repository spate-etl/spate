//! A minimal ClickHouse type-string parser for schema validation.
//!
//! This is a validator's parser, not a type system: it understands just
//! enough structure (`Nullable`, `LowCardinality`, `Array`, `Map`,
//! `Tuple`, `FixedString`, date/time/decimal parameters, enums) to
//! compare a column against a row struct's serde shape. Anything it does
//! not recognize collapses to [`ChType::Other`], which the comparison
//! treats as always-compatible; unknown server types must never fail
//! validation.

/// A parsed ClickHouse column type, shallow enough for class-based
/// compatibility checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChType {
    /// A parameterless type: `UInt32`, `String`, `Bool`, `UUID`, `IPv4`,
    /// `Date`, `Int256`, `JSON`, `Point`, ...
    Named(String),
    /// `Nullable(T)`.
    Nullable(Box<ChType>),
    /// `LowCardinality(T)`, transparent for compatibility.
    LowCardinality(Box<ChType>),
    /// `Array(T)`.
    Array(Box<ChType>),
    /// `Map(K, V)`.
    Map(Box<ChType>, Box<ChType>),
    /// `Tuple(T1, T2, ...)`; element names of named tuples are dropped.
    Tuple(Vec<ChType>),
    /// `FixedString(N)`.
    FixedString(u32),
    /// `DateTime` / `DateTime('tz')`.
    DateTime {
        /// The column's timezone parameter, if any.
        tz: Option<String>,
    },
    /// `DateTime64(p)` / `DateTime64(p, 'tz')`.
    DateTime64 {
        /// Sub-second precision (0..=9).
        precision: u8,
        /// The column's timezone parameter, if any.
        tz: Option<String>,
    },
    /// `Time64(p)`.
    Time64 {
        /// Sub-second precision (0..=9).
        precision: u8,
    },
    /// `Decimal(P, S)` and the `Decimal32/64/128/256(S)` sugar.
    Decimal {
        /// Total significant digits.
        precision: u8,
        /// Fractional digits.
        scale: u8,
    },
    /// `Enum8('a' = 1, ...)`, whose value list is parsed for tokenizer
    /// correctness then dropped.
    Enum8,
    /// `Enum16(...)`.
    Enum16,
    /// Anything unrecognized, always compatible.
    Other(String),
}

/// Parse a ClickHouse type string. Total: never fails, degrading to
/// [`ChType::Other`] instead.
pub(crate) fn parse(s: &str) -> ChType {
    let s = s.trim();
    try_parse(s).unwrap_or_else(|| ChType::Other(s.to_string()))
}

fn try_parse(s: &str) -> Option<ChType> {
    let (name, args) = match s.find('(') {
        None => (s.trim(), None),
        Some(i) if s.ends_with(')') => (s[..i].trim(), Some(&s[i + 1..s.len() - 1])),
        Some(_) => return None,
    };
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    let args = match args {
        None => Vec::new(),
        Some(a) => split_top_level(a)?,
    };

    Some(match (name, args.as_slice()) {
        ("Nullable", [t]) => ChType::Nullable(Box::new(parse(t))),
        ("LowCardinality", [t]) => ChType::LowCardinality(Box::new(parse(t))),
        ("Array", [t]) => ChType::Array(Box::new(parse(t))),
        ("Map", [k, v]) => ChType::Map(Box::new(parse(k)), Box::new(parse(v))),
        ("Tuple", elems) if !elems.is_empty() => {
            ChType::Tuple(elems.iter().map(|e| parse_tuple_element(e)).collect())
        }
        ("FixedString", [n]) => ChType::FixedString(n.parse().ok()?),
        ("DateTime", []) => ChType::DateTime { tz: None },
        ("DateTime", [tz]) => ChType::DateTime {
            tz: Some(unquote(tz)?),
        },
        ("DateTime64", [p]) => ChType::DateTime64 {
            precision: p.parse().ok()?,
            tz: None,
        },
        ("DateTime64", [p, tz]) => ChType::DateTime64 {
            precision: p.parse().ok()?,
            tz: Some(unquote(tz)?),
        },
        ("Time64", [p]) => ChType::Time64 {
            precision: p.parse().ok()?,
        },
        ("Decimal", [p, s]) => ChType::Decimal {
            precision: p.parse().ok()?,
            scale: s.parse().ok()?,
        },
        ("Decimal32", [s]) => ChType::Decimal {
            precision: 9,
            scale: s.parse().ok()?,
        },
        ("Decimal64", [s]) => ChType::Decimal {
            precision: 18,
            scale: s.parse().ok()?,
        },
        ("Decimal128", [s]) => ChType::Decimal {
            precision: 38,
            scale: s.parse().ok()?,
        },
        ("Decimal256", [s]) => ChType::Decimal {
            precision: 76,
            scale: s.parse().ok()?,
        },
        ("Enum8", _) => ChType::Enum8,
        ("Enum16", _) => ChType::Enum16,
        (_, []) => ChType::Named(name.to_string()),
        // A recognized-looking constructor we don't model
        // (AggregateFunction, Variant, JSON(...), Nested, ...).
        _ => return None,
    })
}

/// A named-tuple element is `name Type`; an unnamed one is just `Type`.
/// A top-level space only ever separates a name from its type (spaces
/// inside parameters sit at depth ≥ 1 or inside quotes).
fn parse_tuple_element(e: &str) -> ChType {
    let e = e.trim();
    match top_level_space(e) {
        Some(i) => parse(&e[i + 1..]),
        None => parse(e),
    }
}

fn top_level_space(s: &str) -> Option<usize> {
    let mut depth = 0u32;
    let mut in_quote = false;
    let mut escape = false;
    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match c {
            '\\' if in_quote => escape = true,
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => depth += 1,
            ')' if !in_quote => depth = depth.checked_sub(1)?,
            ' ' if !in_quote && depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Split a parameter list at top-level commas, respecting nesting and
/// single-quoted literals (with backslash escapes).
fn split_top_level(s: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut depth = 0u32;
    let mut in_quote = false;
    let mut escape = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match c {
            '\\' if in_quote => escape = true,
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => depth += 1,
            ')' if !in_quote => depth = depth.checked_sub(1)?,
            ',' if !in_quote && depth == 0 => {
                parts.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 || in_quote {
        return None;
    }
    let last = s[start..].trim();
    if !last.is_empty() || !parts.is_empty() {
        parts.push(last);
    }
    Some(parts)
}

fn unquote(s: &str) -> Option<String> {
    let s = s.trim();
    let inner = s.strip_prefix('\'')?.strip_suffix('\'')?;
    Some(inner.replace("\\'", "'").replace("\\\\", "\\"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(s: &str) -> ChType {
        ChType::Named(s.to_string())
    }

    #[test]
    fn parses_plain_and_nested_types() {
        assert_eq!(parse("UInt32"), named("UInt32"));
        assert_eq!(parse(" String "), named("String"));
        assert_eq!(
            parse("Nullable(LowCardinality(String))"),
            ChType::Nullable(Box::new(ChType::LowCardinality(Box::new(named("String")))))
        );
        assert_eq!(
            parse("Array(Tuple(UInt32, String))"),
            ChType::Array(Box::new(ChType::Tuple(vec![
                named("UInt32"),
                named("String")
            ])))
        );
        assert_eq!(
            parse("Map(String, Array(Nullable(Int64)))"),
            ChType::Map(
                Box::new(named("String")),
                Box::new(ChType::Array(Box::new(ChType::Nullable(Box::new(named(
                    "Int64"
                ))))))
            )
        );
    }

    #[test]
    fn parses_named_tuples_dropping_element_names() {
        assert_eq!(
            parse("Tuple(id UInt64, name String)"),
            ChType::Tuple(vec![named("UInt64"), named("String")])
        );
        // Parameters containing spaces are not mistaken for names.
        assert_eq!(
            parse("Tuple(Decimal(18, 4), price Decimal(9, 2))"),
            ChType::Tuple(vec![
                ChType::Decimal {
                    precision: 18,
                    scale: 4
                },
                ChType::Decimal {
                    precision: 9,
                    scale: 2
                },
            ])
        );
    }

    #[test]
    fn parses_datetime_and_decimal_parameters() {
        assert_eq!(parse("DateTime"), ChType::DateTime { tz: None });
        assert_eq!(
            parse("DateTime('Europe/London')"),
            ChType::DateTime {
                tz: Some("Europe/London".into())
            }
        );
        assert_eq!(
            parse("DateTime64(3, 'Europe/London')"),
            ChType::DateTime64 {
                precision: 3,
                tz: Some("Europe/London".into())
            }
        );
        assert_eq!(
            parse("DateTime64(6)"),
            ChType::DateTime64 {
                precision: 6,
                tz: None
            }
        );
        assert_eq!(parse("Time64(9)"), ChType::Time64 { precision: 9 });
        assert_eq!(
            parse("Decimal(18, 4)"),
            ChType::Decimal {
                precision: 18,
                scale: 4
            }
        );
        assert_eq!(
            parse("Decimal64(4)"),
            ChType::Decimal {
                precision: 18,
                scale: 4
            }
        );
        assert_eq!(parse("FixedString(16)"), ChType::FixedString(16));
    }

    #[test]
    fn enum_value_lists_survive_hostile_quoting() {
        assert_eq!(parse("Enum8('a' = 1, 'b' = 2)"), ChType::Enum8);
        // A quoted value containing commas, parens, and an escaped quote.
        assert_eq!(parse("Enum8('a,b)' = 1, 'c\\'d' = 2)"), ChType::Enum8);
        assert_eq!(parse("Enum16('x' = 300)"), ChType::Enum16);
    }

    #[test]
    fn unknown_types_collapse_to_other() {
        for s in [
            "AggregateFunction(sum, UInt64)",
            "SimpleAggregateFunction(max, DateTime)",
            "Variant(Int64, String)",
            "Nested(a UInt32, b String)",
            "Dynamic",
            "SomeFutureType(1, 2)",
        ] {
            match parse(s) {
                ChType::Other(o) => assert_eq!(o, s),
                ChType::Named(n) => assert_eq!(n, s, "parameterless unknowns stay Named"),
                other => panic!("{s} parsed to {other:?}"),
            }
        }
        // Unbalanced input degrades instead of panicking.
        assert!(matches!(parse("Array(UInt32"), ChType::Other(_)));
        assert!(matches!(parse("Tuple)("), ChType::Other(_)));
        assert!(matches!(parse(""), ChType::Other(_)));
    }
}
