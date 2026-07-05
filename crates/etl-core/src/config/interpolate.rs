//! `${VAR}` environment interpolation, applied to the raw config text
//! before YAML parsing.
//!
//! Supported forms (Vector-compatible):
//!
//! | Form | Behaviour |
//! |---|---|
//! | `${VAR}` | Substitute the variable; **error** if unset (empty is allowed). |
//! | `${VAR:-default}` | Substitute the variable; use `default` if unset **or empty**. |
//! | `${VAR:?message}` | Substitute the variable; **error** with `message` if unset **or empty**. |
//! | `$$` | A literal `$`. |
//!
//! A `$` not followed by `$` or `{` passes through literally. Substitution
//! is applied to the whole text — including YAML comments and quoted
//! strings — exactly like Vector; there is no YAML-aware skipping.
//! Variable names are `[A-Za-z0-9_]+`; a default/message runs to the first
//! `}` (no nesting).

use super::ConfigError;

/// Interpolate against the process environment.
pub(super) fn interpolate(input: &str) -> Result<String, ConfigError> {
    interpolate_with(input, |name| std::env::var(name).ok())
}

/// Interpolate with an explicit lookup function (deterministic in tests —
/// mutating the process environment is unsafe and racy under a parallel
/// test runner).
pub(super) fn interpolate_with<F>(input: &str, lookup: F) -> Result<String, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;

    while i < bytes.len() {
        // Copy everything up to the next `$` in one shot.
        let Some(rel) = input[i..].find('$') else {
            out.push_str(&input[i..]);
            break;
        };
        let dollar = i + rel;
        out.push_str(&input[i..dollar]);

        // `$$` escape.
        if bytes.get(dollar + 1) == Some(&b'$') {
            out.push('$');
            i = dollar + 2;
            continue;
        }
        // Bare `$` (not an interpolation opener) passes through.
        if bytes.get(dollar + 1) != Some(&b'{') {
            out.push('$');
            i = dollar + 1;
            continue;
        }

        // `${NAME ...`
        let name_start = dollar + 2;
        let mut j = name_start;
        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
        if j == name_start {
            return Err(err_at(
                input,
                dollar,
                "empty or invalid variable name after `${`",
            ));
        }
        let name = &input[name_start..j];

        let (value, close) = match bytes.get(j) {
            // `${NAME}` — required, empty allowed.
            Some(b'}') => match lookup(name) {
                Some(v) => (v, j),
                None => {
                    return Err(err_at(
                        input,
                        dollar,
                        format!(
                            "undefined environment variable `{name}` (use `${{{name}:-default}}` to provide a fallback)"
                        ),
                    ));
                }
            },
            // `${NAME:-default}` / `${NAME:?message}` — unset or empty
            // triggers the operator.
            Some(b':') => {
                let op = *bytes
                    .get(j + 1)
                    .ok_or_else(|| unclosed(input, dollar, name))?;
                if op != b'-' && op != b'?' {
                    return Err(err_at(
                        input,
                        dollar,
                        format!("malformed interpolation for `{name}`: expected `:-` or `:?`"),
                    ));
                }
                let arg_start = j + 2;
                let close = input[arg_start..]
                    .find('}')
                    .map(|o| arg_start + o)
                    .ok_or_else(|| unclosed(input, dollar, name))?;
                let arg = &input[arg_start..close];
                match (op, lookup(name).filter(|v| !v.is_empty())) {
                    (_, Some(v)) => (v, close),
                    (b'-', None) => (arg.to_owned(), close),
                    (_, None) => {
                        return Err(err_at(
                            input,
                            dollar,
                            format!("required environment variable `{name}` is not set: {arg}"),
                        ));
                    }
                }
            }
            _ => return Err(unclosed(input, dollar, name)),
        };

        out.push_str(&value);
        i = close + 1;
    }

    Ok(out)
}

fn unclosed(input: &str, offset: usize, name: &str) -> ConfigError {
    err_at(
        input,
        offset,
        format!("unclosed interpolation for `{name}` (missing `}}`)"),
    )
}

fn err_at(input: &str, offset: usize, reason: impl Into<String>) -> ConfigError {
    let before = &input[..offset];
    let line = before.matches('\n').count() + 1;
    let column = offset - before.rfind('\n').map_or(0, |p| p + 1) + 1;
    ConfigError::Interpolation {
        line,
        column,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn run(input: &str, pairs: &[(&str, &str)]) -> Result<String, ConfigError> {
        interpolate_with(input, env(pairs))
    }

    #[test]
    fn substitutes_set_variables() {
        assert_eq!(
            run("brokers: ${KAFKA}", &[("KAFKA", "k1:9092")]).unwrap(),
            "brokers: k1:9092"
        );
    }

    #[test]
    fn bare_requires_variable_but_allows_empty() {
        assert_eq!(run("x: '${E}'", &[("E", "")]).unwrap(), "x: ''");
        let err = run("x: ${MISSING}", &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("undefined environment variable `MISSING`")
        );
        assert!(err.to_string().contains("line 1, column 4"));
    }

    #[test]
    fn default_applies_when_unset_or_empty() {
        assert_eq!(run("${P:-9090}", &[]).unwrap(), "9090");
        assert_eq!(run("${P:-9090}", &[("P", "")]).unwrap(), "9090");
        assert_eq!(run("${P:-9090}", &[("P", "8080")]).unwrap(), "8080");
        assert_eq!(run("${P:-}", &[]).unwrap(), "");
    }

    #[test]
    fn required_operator_errors_with_message() {
        let err = run("key: ${SECRET:?set SECRET in the pod env}", &[]).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("SECRET"));
        assert!(text.contains("set SECRET in the pod env"));
        assert_eq!(run("${S:?msg}", &[("S", "v")]).unwrap(), "v");
    }

    #[test]
    fn dollar_escapes_and_passthrough() {
        assert_eq!(run("cost: $$5", &[]).unwrap(), "cost: $5");
        assert_eq!(run("regex: ^a$ then", &[]).unwrap(), "regex: ^a$ then");
        assert_eq!(run("$", &[]).unwrap(), "$");
        assert_eq!(run("$$$$", &[]).unwrap(), "$$");
    }

    #[test]
    fn substitutes_inside_quoted_strings_and_comments() {
        assert_eq!(
            run("a: \"${V}\" # uses ${V:-fallback}", &[("V", "x")]).unwrap(),
            "a: \"x\" # uses x"
        );
    }

    #[test]
    fn multiple_and_adjacent_interpolations() {
        assert_eq!(run("${A}${B:-b}/${A}", &[("A", "a")]).unwrap(), "ab/a");
    }

    #[test]
    fn malformed_forms_error_with_position() {
        for (input, needle) in [
            ("x:\n  y: ${}", "empty or invalid variable name"),
            ("${VAR", "unclosed interpolation for `VAR`"),
            ("${VAR:-no_close", "unclosed interpolation for `VAR`"),
            ("${VAR:x}", "expected `:-` or `:?`"),
            ("${VAR:", "unclosed interpolation for `VAR`"),
        ] {
            let err = run(input, &[("VAR", "v")]).unwrap_err();
            assert!(
                err.to_string().contains(needle),
                "input {input:?}: expected {needle:?} in {err}"
            );
        }
        // Position points at the `$`, 1-based.
        let err = run("x:\n  y: ${}", &[]).unwrap_err();
        assert!(err.to_string().contains("line 2, column 6"), "{err}");
    }

    #[test]
    fn default_may_contain_any_text_up_to_first_brace() {
        assert_eq!(
            run("${U:-http://h:9000/p?q=1}", &[]).unwrap(),
            "http://h:9000/p?q=1"
        );
        // No nesting: the default ends at the FIRST `}`.
        assert_eq!(run("${A:-${B}}", &[]).unwrap(), "${B}");
    }

    #[test]
    fn utf8_text_survives() {
        assert_eq!(
            run("name: caf\u{e9} ${V:-\u{1f680} default}", &[]).unwrap(),
            "name: caf\u{e9} \u{1f680} default"
        );
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// For text containing no `$`, interpolation is the identity.
            #[test]
            fn dollar_free_text_is_identity(s in "[^$]*") {
                prop_assert_eq!(run(&s, &[]).unwrap(), s);
            }

            /// Escaping every `$` as `$$` round-trips arbitrary text.
            #[test]
            fn escaped_text_round_trips(s in ".*") {
                let escaped = s.replace('$', "$$");
                prop_assert_eq!(run(&escaped, &[]).unwrap(), s);
            }
        }
    }
}
