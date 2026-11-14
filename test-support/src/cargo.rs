//! Reading where cargo put a build artifact from its JSON messages.

use std::path::PathBuf;

use serde_json::Value;

/// The executable cargo reported for the `kind` target named `name`, read from
/// the stdout of `cargo build --message-format=json*`.
///
/// Cargo keeps its build directory's layout internal and says to locate
/// artifacts through these messages instead. Lines that are not JSON, a
/// `reason` other than `compiler-artifact` and a null `executable` are
/// skipped, so a message kind a later cargo adds is not fatal.
#[must_use]
pub fn reported_executable(stdout: &str, kind: &str, name: &str) -> Option<PathBuf> {
    stdout.lines().find_map(|line| {
        let message: Value = serde_json::from_str(line).ok()?;
        if message["reason"] != "compiler-artifact" || message["target"]["name"] != name {
            return None;
        }
        let kinds = message["target"]["kind"].as_array()?;
        if !kinds.iter().any(|k| k == kind) {
            return None;
        }
        message["executable"].as_str().map(PathBuf::from)
    })
}

#[cfg(test)]
mod tests {
    use super::reported_executable;
    use std::path::PathBuf;

    /// Lines in the shape cargo emits, plus the ones the reader must skip.
    const LINES: &str = concat!(
        r#"{"reason":"compiler-artifact","target":{"name":"load","kind":["bench"]},"executable":"/t/bench/load-abc"}"#,
        "\n",
        r#"{"reason":"compiler-artifact","target":{"name":"load","kind":["example"]},"executable":"/t/examples/load"}"#,
        "\n",
        r#"{"reason":"compiler-artifact","target":{"name":"spate","kind":["lib"]},"executable":null}"#,
        "\n",
        r#"{"reason":"compiler-artifact","target":{"name":"e2e","kind":["test"]},"executable":"/t/out/e2e-def"}"#,
        "\n",
        r#"{"reason":"something-new","target":{"name":"later","kind":["example"]},"executable":"/t/examples/later"}"#,
        "\n",
        "not json at all\n",
    );

    /// Two targets that share a name are told apart by their kind.
    #[test]
    fn kind_and_name_select_one_artifact() {
        assert_eq!(
            reported_executable(LINES, "example", "load"),
            Some(PathBuf::from("/t/examples/load"))
        );
        assert_eq!(
            reported_executable(LINES, "bench", "load"),
            Some(PathBuf::from("/t/bench/load-abc"))
        );
        assert_eq!(
            reported_executable(LINES, "test", "e2e"),
            Some(PathBuf::from("/t/out/e2e-def"))
        );
    }

    /// A target cargo did not report, or reported with no executable, has no
    /// path.
    #[test]
    fn a_missing_or_null_executable_is_none() {
        assert_eq!(reported_executable(LINES, "example", "absent"), None);
        assert_eq!(reported_executable(LINES, "lib", "spate"), None);
    }

    /// A message that is not `compiler-artifact` is skipped even when its
    /// target matches, and a line that is not JSON is skipped without failing.
    #[test]
    fn only_compiler_artifacts_are_read() {
        assert_eq!(reported_executable(LINES, "example", "later"), None);
        assert_eq!(reported_executable("not json\n", "example", "load"), None);
    }
}
