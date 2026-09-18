//! The semver gate: the tree's public API against the release the registry
//! serves, and the key the parsed baselines are cached under.
//!
//! `--packages` restricts the comparison to a named set. An empty set is an
//! error, so a workflow expression that resolves to nothing fails the gate.
//!
//! The published release is the baseline a version number is a claim about, and
//! it moves only at a release. A break is expected once an announced one has
//! landed, so a finding passes when the pull request title carries the
//! conventional marker, or when a commit since the last tag already carries
//! one. That second excuse is workspace-wide: after the first announced break
//! of a release cycle, a later pull request breaking a different crate passes
//! with no marker of its own. The version still derives as a minor, and the
//! fragment and release-note line for that second break are given up.
//!
//! Pre-1.0 a breaking change ships in a minor bump, so the gate exists to force
//! the announcement. A finding with no marker fails, and retitling re-runs it.
//!
//! Two variables shape the marker scan, both set on a pull request and absent
//! elsewhere:
//!
//! - `BASE_SHA` ends the scan. Scanning to `HEAD` would read this branch's own
//!   commit subjects, which squash into body lines the release derivation
//!   classifies as plain.
//! - `PR_TITLE` is free text somebody typed. It is matched against, and never
//!   evaluated or passed to a shell.

use std::ffi::OsStr;
use std::path::Path;

use serde_json::Value;

use crate::run::{self, Completed, Error, Outcome, Step, Streams};

/// The prefix on every line this check writes for itself.
const TOOL: &str = "semver-checks";

/// The agent the sparse index sees.
const UA: &str = "spate-release (github.com/spate-etl/spate)";

/// The tool's verdict for a run.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Verdict {
    Clean,
    Breaking,
    Error,
}

/// The tool's exit codes, from its own documentation: 0 is clean, 100 is
/// "required bump not satisfied", 101 is "could not complete". Every other code
/// is could-not-complete, since a gate that cannot evaluate must not pass.
fn classify_exit(code: i32) -> Verdict {
    match code {
        0 => Verdict::Clean,
        100 => Verdict::Breaking,
        _ => Verdict::Error,
    }
}

/// Whether a subject carries the conventional breaking marker, the shape the
/// release derivation reads the bump from.
///
/// Matches `^[a-zA-Z]+(\([^)]*\))?!:` against the whole string, so a newline
/// inside a scope is part of it.
fn subject_is_breaking(subject: &str) -> bool {
    let after_type = subject.trim_start_matches(|c: char| c.is_ascii_alphabetic());
    if after_type.len() == subject.len() {
        return false;
    }
    let after_scope = match after_type.strip_prefix('(') {
        // An unterminated scope leaves the optional group unmatched.
        Some(inner) => inner
            .find(')')
            .map_or(after_type, |close| &inner[close + 1..]),
        None => after_type,
    };
    after_scope.starts_with("!:")
}

/// Whether any line of a commit log carries the marker.
fn log_has_marker(log: &str) -> bool {
    log.split('\n').any(subject_is_breaking)
}

/// A crate's path in the sparse index. The scheme keys on name length, and the
/// short arms keep a future short name from probing a URL that answers 404 for
/// the wrong reason.
fn index_path(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let slice = |from: usize, len: usize| -> String { chars.iter().skip(from).take(len).collect() };
    match chars.len() {
        1 => format!("1/{name}"),
        2 => format!("2/{name}"),
        3 => format!("3/{}/{name}", slice(0, 1)),
        _ => format!("{}/{}/{name}", slice(0, 2), slice(2, 2)),
    }
}

/// The version the tree is compared against, from a crate's sparse-index entry.
///
/// The index lists one JSON object per published version in publish order, so
/// the newest live version is the last entry that is not yanked. A crate whose
/// every version is yanked has no baseline, and neither does one whose newest
/// live entry names an empty version.
fn baseline_version(body: &str) -> Result<Option<String>, String> {
    let mut latest = None;
    for entry in serde_json::Deserializer::from_str(body).into_iter::<Value>() {
        let entry = entry.map_err(|e| e.to_string())?;
        if yanked(&entry) {
            continue;
        }
        latest = Some(version_field(&entry));
    }
    Ok(latest.filter(|v| !v.is_empty()))
}

/// Whether an index entry is withdrawn. Only `false` and `null` leave a version
/// live, so a `yanked` of any other shape withdraws it.
fn yanked(entry: &Value) -> bool {
    !matches!(
        entry.get("yanked"),
        None | Some(Value::Null) | Some(Value::Bool(false))
    )
}

/// An entry's `vers`, rendered as a raw JSON filter renders it: a string bare,
/// anything else in its JSON spelling, and an absent field as `null`.
fn version_field(entry: &Value) -> String {
    match entry.get("vers") {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => "null".to_owned(),
    }
}

/// The body and status code `curl -w '\n%{http_code}'` produces, split at the
/// last newline once the trailing newlines a command substitution drops are
/// gone.
fn split_reply(raw: &str) -> (&str, &str) {
    let reply = raw.trim_end_matches('\n');
    reply.rsplit_once('\n').unwrap_or((reply, reply))
}

/// The workspace version, from the root manifest's own `version` key. Several
/// matching lines join on newlines, as a capture of the same search would.
fn workspace_version(manifest: &str) -> String {
    manifest
        .split('\n')
        .filter_map(|line| line.strip_prefix("version = \"")?.strip_suffix('"'))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The fields a shell word split yields under the default separators.
fn fields(list: &str) -> impl Iterator<Item = &str> {
    list.split([' ', '\t', '\n']).filter(|f| !f.is_empty())
}

/// Whether a `--packages` value names crates this tree holds.
///
/// The value arrives from a workflow expression, so a name is matched against a
/// character class and never used as a pattern. A name that is not a crate
/// directory means the closure table went stale, and checking nothing would
/// report a pass over an unexamined surface.
fn packages_valid(root: &Path, list: &str) -> bool {
    if list.replace(' ', "").is_empty() {
        return false;
    }
    fields(list).all(|name| {
        name.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            && root.join("crates").join(name).is_dir()
    })
}

/// The cache key for the parsed baselines under `target/semver-checks/cache`.
///
/// The release tag names the versions the registry serves. A rustdoc format is
/// tied to the compiler that emitted it and the reader is tied to the tool
/// version, so both join the key and never produce a hit the reader rejects.
fn cache_key(uname: &str, tag: &str, rustc: &str, tool: &str) -> String {
    format!(
        "semver-baseline-{uname}-{tag}-{}-{}",
        sanitize(rustc),
        sanitize(tool)
    )
}

/// Every byte outside `A-Za-z0-9.` becomes a hyphen, then one trailing hyphen
/// is dropped.
fn sanitize(raw: &str) -> String {
    let mut out: String = raw
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b == b'.' {
                char::from(b)
            } else {
                '-'
            }
        })
        .collect();
    if out.ends_with('-') {
        out.pop();
    }
    out
}

/// The invocation one group takes: every package it names, then the release
/// type where the group pins one.
fn group_step(release_type: Option<&str>, pkgs: &[String]) -> Step<'static> {
    let mut step = Step::new("cargo", ["semver-checks"]);
    for pkg in pkgs {
        step = step.args(["--package", pkg]);
    }
    if let Some(rt) = release_type {
        step = step.args(["--release-type", rt]);
    }
    step
}

/// The publishable crates, by directory name under `crates/`, sorted.
fn crates_now(root: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(root.join("crates")) else {
        return Vec::new();
    };
    let mut out: Vec<String> = entries
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| !name.starts_with('.'))
        .collect();
    out.sort();
    out
}

/// The newest release tag, which names the versions the registry serves.
fn last_tag(root: &Path) -> Result<String, Error> {
    let step = Step::new("git", ["tag", "--list", "v[0-9]*", "--sort=-v:refname"]);
    let listing = run::capture(root, &step)?;
    Ok(listing.split('\n').next().unwrap_or_default().to_owned())
}

/// One batched comparison over the named crates, accumulating into `checked`
/// and `breaking`.
///
/// One invocation per group: the tool shares a rustdoc build across the
/// packages of a single run and shares nothing between runs. A break re-runs
/// the group one crate at a time to name which crates broke, and the findings
/// are already on the log from the batch, so the re-run discards its output and
/// reads only the verdict.
fn check_group(
    root: &Path,
    release_type: Option<&str>,
    pkgs: &[String],
    checked: &mut usize,
    breaking: &mut Vec<String>,
) -> Outcome {
    if pkgs.is_empty() {
        return Ok(());
    }
    println!(
        "{TOOL}: checking {} crate(s) against their published baseline",
        pkgs.len()
    );
    let code = run::complete(root, &group_step(release_type, pkgs), Streams::Inherit)?.code;
    match classify_exit(code) {
        Verdict::Clean => *checked += pkgs.len(),
        Verdict::Breaking => {
            *checked += pkgs.len();
            for pkg in pkgs {
                attribute(root, release_type, pkg, breaking)?;
            }
        }
        Verdict::Error => {
            return Err(Error::msg(format!(
                "cargo semver-checks exited {code} without a verdict. That is the\n  \
                 tool failing to complete, not an API judgement; read its output above."
            )));
        }
    }
    Ok(())
}

/// Re-runs one crate of a broken batch and records its verdict.
fn attribute(
    root: &Path,
    release_type: Option<&str>,
    pkg: &str,
    breaking: &mut Vec<String>,
) -> Outcome {
    let alone = [pkg.to_owned()];
    let code = run::complete(root, &group_step(release_type, &alone), Streams::Discard)?.code;
    match classify_exit(code) {
        Verdict::Breaking => breaking.push(pkg.to_owned()),
        Verdict::Clean => {}
        Verdict::Error => {
            return Err(Error::msg(format!(
                "cargo semver-checks exited {code} for {pkg} while attributing a break.\n  \
                 Read its output above; the batch already reported the findings."
            )));
        }
    }
    Ok(())
}

/// Fetches a crate's sparse-index entry, answering the status code and body.
///
/// Only 200 and 404 are answers. A transport failure fails the run before it
/// can claim anything.
fn fetch_index(root: &Path, name: &str) -> Result<(String, String), Error> {
    let step = Step::new(
        "curl",
        [
            "-sS",
            "--retry",
            "3",
            "--max-time",
            "30",
            "-w",
            r"\n%{http_code}",
            "-H",
        ],
    )
    .arg(format!("User-Agent: {UA}"))
    .arg(format!("https://index.crates.io/{}", index_path(name)));
    let raw = run::capture(root, &step).map_err(|_| {
        Error::msg(format!(
            "the index request for {name} failed outright; the check cannot evaluate"
        ))
    })?;
    let (body, code) = split_reply(&raw);
    Ok((code.to_owned(), body.to_owned()))
}

/// Compares the tree against what is published.
pub(crate) fn registry(root: &Path, explain: bool, packages: Option<&str>) -> Outcome {
    let selected = match packages {
        Some(list) => {
            if !packages_valid(root, list) {
                return Err(Error::msg(format!(
                    "--packages needs a space-separated list of crate directory names under crates/,\n  \
                     and got '{list}'. An empty or unrecognized value is a wiring fault in the\n  \
                     caller, and passing it would check the wrong set or nothing at all."
                )));
            }
            fields(list).map(str::to_owned).collect()
        }
        None => crates_now(root),
    };
    if explain {
        println!(
            "(reads the sparse index for {} crate(s), then runs cargo semver-checks)",
            selected.len()
        );
        return Ok(());
    }

    let manifest = std::fs::read_to_string(root.join("Cargo.toml"))
        .map_err(|e| Error::msg(format!("Cargo.toml: {e}")))?;
    let current = workspace_version(&manifest);
    let mut plain: Vec<String> = Vec::new();
    let mut pinned: Vec<String> = Vec::new();
    let mut skipped = 0usize;
    for name in &selected {
        let (code, body) = fetch_index(root, name)?;
        match code.as_str() {
            "200" => {}
            "404" => {
                println!(
                    "::notice::{name} has no published baseline, so there is nothing to diff against."
                );
                println!(
                    "  Its name is claimed by hand at the first release including it; see RELEASING.md."
                );
                skipped += 1;
                continue;
            }
            other => {
                return Err(Error::msg(format!(
                    "the sparse index answered {other} for {name}; refusing to report a pass it cannot evaluate"
                )));
            }
        }
        let Some(latest) = baseline_version(&body)
            .map_err(|e| Error::msg(format!("the index entry for {name} does not parse: {e}")))?
        else {
            println!(
                "::notice::every published version of {name} is yanked; nothing to diff against."
            );
            skipped += 1;
            continue;
        };
        // Between a release merge and its publish the tree's version is ahead
        // of the registry, and the default classification then drops every
        // lint. Pinning the expectation to a minor keeps the major-breaking
        // lints running across that window.
        if latest == current {
            plain.push(name.clone());
        } else {
            pinned.push(name.clone());
        }
    }

    let mut checked = 0usize;
    let mut breaking: Vec<String> = Vec::new();
    check_group(root, None, &plain, &mut checked, &mut breaking)?;
    check_group(root, Some("minor"), &pinned, &mut checked, &mut breaking)?;

    if checked + skipped == 0 {
        return Err(Error::msg(
            "the selection named no crate to check, so the check evaluated nothing",
        ));
    }

    breaking.extend(removed_since(
        root,
        last_tag(root).unwrap_or_default().as_str(),
    ));

    if !breaking.is_empty() {
        return verdict(root, &breaking);
    }
    if checked == 0 {
        println!("{TOOL}: every crate was skipped for want of a baseline; the check is silent");
        println!("  until the first release publishes one.");
        return Ok(());
    }
    println!("{TOOL}: {checked} crate(s) hold their published API surface.");
    Ok(())
}

/// The crates a tag published that the tree no longer holds, each marked as a
/// removal.
///
/// A removal is breaking whatever the tool says about what remains. It needs no
/// build, so it is judged over every crate. The listing is read whatever status
/// `git` reports, so a tag naming no tree yields nothing.
fn removed_since(root: &Path, tag: &str) -> Vec<String> {
    let step = Step::new("git", ["ls-tree", "--name-only"]).arg(format!("{tag}:crates"));
    let Ok(Completed { stdout, .. }) = run::complete(root, &step, Streams::Collect) else {
        return Vec::new();
    };
    fields(&stdout)
        .filter(|name| !root.join("crates").join(name).is_dir())
        .map(|name| format!("{name}(removed)"))
        .collect()
}

/// Reports a break, passing it where a marker already announces it.
fn verdict(root: &Path, breaking: &[String]) -> Outcome {
    let list: String = breaking.iter().map(|b| format!(" {b}")).collect();
    let last = last_tag(root)?;
    if last.is_empty() {
        return Err(Error::msg(
            "breaking changes found but no vX.Y.Z tag to scan for their markers",
        ));
    }

    // This pull request's own announcement is its title and nothing else: the
    // squash subject is the title, and a constituent subject inside a squash
    // body is not a subject the release derivation reads.
    if subject_is_breaking(&std::env::var("PR_TITLE").unwrap_or_default()) {
        println!("{TOOL}: breaking against the registry:{list}");
        println!("  The title carries the marker; the next release derives as a minor.");
        return Ok(());
    }

    let base = std::env::var("BASE_SHA").unwrap_or_default();
    let base = if base.is_empty() { "HEAD" } else { &base };
    let step =
        Step::new("git", ["log", "--no-merges", "--format=%B"]).arg(format!("{last}..{base}"));
    if log_has_marker(&run::capture(root, &step)?) {
        println!("{TOOL}: breaking against the registry:{list}");
        println!(
            "  A marker since {last} already announces it; the next release derives as a minor."
        );
        return Ok(());
    }

    println!("::error::Breaking against the registry with no marker since {last}:{list}");
    println!(
        "::error::The release derivation reads the log for the marker, so this break would \
         under-bump the next version. Land a commit carrying `!` in its subject and a changelog \
         fragment opening with **Breaking:**."
    );
    Err(Error::status(1))
}

/// Prints the baseline cache key, on `$GITHUB_OUTPUT` where a runner named one.
pub(crate) fn print_cache_key(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(reads the newest release tag, and the rustc and cargo-semver-checks versions)");
        return Ok(());
    }
    let tag = last_tag(root)?;
    if tag.is_empty() {
        return Err(Error::msg(
            "no vX.Y.Z tag, so there is no published baseline to key on",
        ));
    }
    let uname = run::capture(root, &Step::new("uname", ["-s"]))?;
    let rustc = run::capture(root, &Step::new("rustc", ["-V"]))?;
    let tool = run::capture(root, &Step::new("cargo", ["semver-checks", "--version"]))?;
    let key = cache_key(uname.trim_end_matches('\n'), &tag, &rustc, &tool);
    eprintln!("{TOOL}: {key}");
    match std::env::var_os("GITHUB_OUTPUT") {
        Some(path) if !path.is_empty() => append_output(&path, &key),
        _ => {
            println!("{key}");
            Ok(())
        }
    }
}

/// Appends one `key=value` line to a workflow's step-output file.
fn append_output(path: &OsStr, key: &str) -> Outcome {
    use std::io::Write;

    let named = |e: std::io::Error| Error::msg(format!("{}: {e}", Path::new(path).display()));
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .map_err(named)?;
    writeln!(file, "key={key}").map_err(named)
}

#[cfg(test)]
mod tests;
