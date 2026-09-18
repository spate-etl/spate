//! Child-process execution, and the error every command reports through.

use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;

/// A command that did not succeed.
#[derive(Debug)]
pub(crate) struct Error {
    pub(crate) message: String,
    /// The child's exit status, where one was produced, so it can be
    /// propagated unchanged.
    pub(crate) code: Option<i32>,
}

impl Error {
    pub(crate) fn msg(message: impl Into<String>) -> Self {
        let message = message.into();
        // The top level prints nothing for an empty message, so an error built
        // with one exits non-zero in silence. `Error::status` covers the case
        // where that is intended.
        debug_assert!(!message.is_empty(), "an error carries a diagnostic");
        Self {
            message,
            code: None,
        }
    }

    /// An exit status a caller has already accounted for on stderr, reported
    /// with no further diagnostic.
    pub(crate) fn status(code: i32) -> Self {
        Self {
            message: String::new(),
            code: Some(code),
        }
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self::msg(message)
    }
}

pub(crate) type Outcome = Result<(), Error>;

/// One child process: what to run, where, and with which environment.
pub(crate) struct Step<'a> {
    pub(crate) program: &'a str,
    pub(crate) args: Vec<String>,
    /// Applied to this child alone.
    pub(crate) env: Vec<(&'a str, String)>,
    /// Relative to the repository root.
    pub(crate) dir: Option<&'a str>,
}

impl<'a> Step<'a> {
    pub(crate) fn new(program: &'a str, args: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        Self {
            program,
            args: args.into_iter().map(|a| a.as_ref().to_owned()).collect(),
            env: Vec::new(),
            dir: None,
        }
    }

    pub(crate) fn arg(mut self, arg: impl AsRef<str>) -> Self {
        self.args.push(arg.as_ref().to_owned());
        self
    }

    pub(crate) fn args(mut self, args: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.args
            .extend(args.into_iter().map(|a| a.as_ref().to_owned()));
        self
    }

    pub(crate) fn env(mut self, key: &'a str, value: impl Into<String>) -> Self {
        self.env.push((key, value.into()));
        self
    }

    pub(crate) fn dir(mut self, dir: &'a str) -> Self {
        self.dir = Some(dir);
        self
    }

    /// The shell form, for `--explain` and for the failure message.
    pub(crate) fn display(&self) -> String {
        let mut s = String::new();
        for (k, v) in &self.env {
            s.push_str(&format!("{k}=\"{v}\" "));
        }
        s.push_str(self.program);
        for a in &self.args {
            s.push(' ');
            s.push_str(&quote(a));
        }
        if let Some(d) = self.dir {
            s = format!("(cd {d} && {s})");
        }
        s
    }
}

/// Runs steps in order, stopping at the first failure.
pub(crate) fn steps(root: &Path, explain: bool, steps: &[Step<'_>]) -> Outcome {
    for step in steps {
        run(root, explain, step)?;
    }
    Ok(())
}

/// Runs one step with stdio inherited, so output streams as the child writes
/// it and nothing is buffered or re-encoded.
pub(crate) fn run(root: &Path, explain: bool, step: &Step<'_>) -> Outcome {
    let line = step.display();
    if explain {
        println!("{line}");
        return Ok(());
    }
    let dir = step
        .dir
        .map_or_else(|| root.to_path_buf(), |d| root.join(d));
    // `current_dir` with a relative program is platform-specific and unstable.
    let status = Command::new(program_path(root, step.program))
        .args(step.args.iter().map(OsStr::new))
        .envs(step.env.iter().map(|(k, v)| (*k, v.as_str())))
        .current_dir(&dir)
        .status()
        .map_err(|e| Error::msg(format!("{}: {e}", step.program)))?;
    if status.success() {
        return Ok(());
    }
    Err(Error {
        message: format!("`{line}` exited {}", describe(&status)),
        code: status.code(),
    })
}

/// Resolves a repository-relative script against the root, leaving a bare name
/// for `PATH` to find.
fn program_path(root: &Path, program: &str) -> std::path::PathBuf {
    program
        .strip_prefix("./")
        .map_or_else(|| std::path::PathBuf::from(program), |rel| root.join(rel))
}

/// Whether a bare program name resolves to an executable file on `PATH`.
pub(crate) fn on_path(program: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| executable(&dir.join(program)))
}

#[cfg(unix)]
fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn executable(path: &Path) -> bool {
    path.is_file()
}

/// Runs one step and returns its stdout. Its stderr is inherited, so a failing
/// child's own diagnostic reaches the terminal alongside the exit status.
pub(crate) fn capture(root: &Path, step: &Step<'_>) -> Result<String, Error> {
    let dir = step
        .dir
        .map_or_else(|| root.to_path_buf(), |d| root.join(d));
    // `output()` would pipe stderr as well, and nothing here reads it.
    let out = Command::new(program_path(root, step.program))
        .args(step.args.iter().map(OsStr::new))
        .envs(step.env.iter().map(|(k, v)| (*k, v.as_str())))
        .current_dir(&dir)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| Error::msg(format!("{}: {e}", step.program)))?
        .wait_with_output()
        .map_err(|e| Error::msg(format!("{}: {e}", step.program)))?;
    if !out.status.success() {
        return Err(Error {
            message: format!("`{}` exited {}", step.display(), describe(&out.status)),
            code: out.status.code(),
        });
    }
    String::from_utf8(out.stdout)
        .map_err(|e| Error::msg(format!("{}: stdout is not UTF-8: {e}", step.program)))
}

/// Runs one step with its stdio inherited and its stdin closed, reporting
/// whether it succeeded. A child reading stdin would otherwise consume what the
/// caller's own loop is reading.
pub(crate) fn succeeded(root: &Path, step: &Step<'_>) -> Result<bool, Error> {
    let dir = step
        .dir
        .map_or_else(|| root.to_path_buf(), |d| root.join(d));
    let status = Command::new(program_path(root, step.program))
        .args(step.args.iter().map(OsStr::new))
        .envs(step.env.iter().map(|(k, v)| (*k, v.as_str())))
        .current_dir(&dir)
        .stdin(std::process::Stdio::null())
        .status()
        .map_err(|e| Error::msg(format!("{}: {e}", step.program)))?;
    Ok(status.success())
}

/// Runs one step with its stdout discarded and its stderr inherited.
pub(crate) fn quiet(root: &Path, explain: bool, step: &Step<'_>) -> Outcome {
    let line = step.display();
    if explain {
        println!("{line}");
        return Ok(());
    }
    let dir = step
        .dir
        .map_or_else(|| root.to_path_buf(), |d| root.join(d));
    let status = Command::new(program_path(root, step.program))
        .args(step.args.iter().map(OsStr::new))
        .envs(step.env.iter().map(|(k, v)| (*k, v.as_str())))
        .current_dir(&dir)
        .stdout(std::process::Stdio::null())
        .status()
        .map_err(|e| Error::msg(format!("{}: {e}", step.program)))?;
    if status.success() {
        return Ok(());
    }
    Err(Error {
        message: format!("`{line}` exited {}", describe(&status)),
        code: status.code(),
    })
}

fn describe(status: &std::process::ExitStatus) -> String {
    status
        .code()
        .map_or_else(|| status.to_string(), |c| c.to_string())
}

/// Wraps an argument in single quotes when it holds anything a shell would
/// split or expand, so a printed line can be pasted back into a terminal.
fn quote(arg: &str) -> String {
    if !arg.is_empty()
        && arg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_=/.:,+@".contains(c))
    {
        return arg.to_owned();
    }
    format!("'{}'", arg.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_printed_line_quotes_what_a_shell_would_split() {
        let step = Step::new("cargo", ["run"]).arg("two words").arg("-p=x");
        assert_eq!(step.display(), "cargo run 'two words' -p=x");
    }

    #[test]
    fn a_printed_line_carries_the_environment_and_the_directory() {
        let step = Step::new("npm", ["run", "build"])
            .env("CI", "true")
            .dir("website");
        assert_eq!(step.display(), r#"(cd website && CI="true" npm run build)"#);
    }

    #[test]
    fn an_embedded_single_quote_survives_the_round_trip() {
        let step = Step::new("sh", ["-c"]).arg("echo 'hi'");
        assert_eq!(step.display(), r#"sh -c 'echo '\''hi'\'''"#);
    }

    /// A step's own environment reaches the child `succeeded` spawns.
    #[cfg(unix)]
    #[test]
    fn a_step_environment_reaches_the_child_succeeded_spawns() {
        let root = crate::repo_root().unwrap();
        let step = Step::new("sh", ["-c", r#"test "$SPATE_STEP_ENV" = reached"#])
            .env("SPATE_STEP_ENV", "reached");
        assert!(succeeded(&root, &step).unwrap());
    }
}
