//! The release note entry for the root dependency requirements that moved
//! since the previous release.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use toml::{Table, Value};

use crate::run::{self, Error, Step};

/// What the entry says above its list.
const PROSE: &str = "\
The published manifests of these crates carry new requirements for the
dependencies listed below. Cargo resolves your lockfile against these
requirements, so a raised requirement can raise the version your project builds
with. Each line shows the requirement in this release first, then the
requirement in the previous release. A crate can enable more features than a
line shows.";

/// One root `[workspace.dependencies]` entry as the workspace requires it. A
/// crate inheriting it may add features of its own.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Spec {
    /// Set only where it differs from the key.
    package: Option<String>,
    version: String,
    default_features: bool,
    features: BTreeSet<String>,
}

/// The crates that depend on each key at one revision, outside their
/// dev-dependencies.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
struct Declared {
    /// Through `workspace = true`.
    inherited: BTreeMap<String, BTreeSet<String>>,
    /// In any form, inherited or their own.
    any: BTreeMap<String, BTreeSet<String>>,
}

/// The root table and its declarations at one revision.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
struct Snapshot {
    table: BTreeMap<String, Spec>,
    declared: Declared,
}

/// How one key moved between the two revisions.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Move {
    Added(Spec),
    Removed(Spec),
    Changed(Spec, Spec),
}

/// One line of the entry.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Line {
    key: String,
    crates: BTreeSet<String>,
    change: Move,
}

/// The entry for `previous..HEAD`, or `None` without a previous tag, without a
/// root `Cargo.toml` at either end, or when nothing moved.
///
/// `HEAD` is read through git, so an uncommitted edit to a manifest is not
/// part of the comparison.
pub(super) fn entry(root: &Path, previous: Option<&str>) -> Result<Option<String>, Error> {
    let Some(tag) = previous else {
        return Ok(None);
    };
    let (Some(before), Some(now)) = (snapshot(root, tag)?, snapshot(root, "HEAD")?) else {
        return Ok(None);
    };
    Ok(render(&lines(&before, &now)))
}

/// The root table and the crate manifests at `rev`, or `None` where `rev` has
/// no root `Cargo.toml`.
fn snapshot(root: &Path, rev: &str) -> Result<Option<Snapshot>, Error> {
    let listing = run::capture(
        root,
        &Step::new("git", ["ls-tree", "-r", "--name-only", rev, "--"])
            .args(["Cargo.toml", "crates/"]),
    )?;
    let files: Vec<&str> = listing.split('\n').filter(|f| !f.is_empty()).collect();
    if !files.contains(&"Cargo.toml") {
        return Ok(None);
    }
    let table = table(&show(root, rev, "Cargo.toml")?)
        .map_err(|e| Error::msg(format!("{rev}:Cargo.toml: {e}")))?;
    let mut manifests = Vec::new();
    for file in files.iter().filter(|f| is_crate_manifest(f)) {
        manifests.push(show(root, rev, file)?);
    }
    let declared = declared(&manifests).map_err(|e| Error::msg(format!("{rev}:crates/: {e}")))?;
    Ok(Some(Snapshot { table, declared }))
}

/// Whether a path is `crates/<name>/Cargo.toml`.
fn is_crate_manifest(path: &str) -> bool {
    path.strip_prefix("crates/")
        .and_then(|rest| rest.strip_suffix("/Cargo.toml"))
        .is_some_and(|name| !name.is_empty() && !name.contains('/'))
}

/// One file's content at `rev`.
fn show(root: &Path, rev: &str, path: &str) -> Result<String, Error> {
    run::capture(
        root,
        &Step::new("git", ["show"]).arg(format!("{rev}:{path}")),
    )
}

/// The registry requirements of the root `[workspace.dependencies]` table, by
/// key. An entry with no `version`, or whose `path` is under `crates/`, is left
/// out.
fn table(manifest: &str) -> Result<BTreeMap<String, Spec>, String> {
    let parsed: Table = manifest.parse().map_err(|e| format!("{e}"))?;
    let Some(deps) = parsed
        .get("workspace")
        .and_then(|w| w.get("dependencies"))
        .and_then(Value::as_table)
    else {
        return Ok(BTreeMap::new());
    };
    let mut out = BTreeMap::new();
    for (key, value) in deps {
        if let Some(spec) = spec(key, value)? {
            out.insert(key.clone(), spec);
        }
    }
    Ok(out)
}

/// One entry's requirement, or `None` where no published manifest carries it.
fn spec(key: &str, value: &Value) -> Result<Option<Spec>, String> {
    if let Some(version) = value.as_str() {
        return Ok(Some(Spec {
            package: None,
            version: version.to_owned(),
            default_features: true,
            features: BTreeSet::new(),
        }));
    }
    let Some(entry) = value.as_table() else {
        return Err(format!("`{key}` is neither a string nor a table"));
    };
    let Some(version) = entry.get("version").and_then(Value::as_str) else {
        return Ok(None);
    };
    if entry
        .get("path")
        .and_then(Value::as_str)
        .is_some_and(|path| path.starts_with("crates/"))
    {
        return Ok(None);
    }
    let package = entry
        .get("package")
        .and_then(Value::as_str)
        .filter(|package| *package != key)
        .map(str::to_owned);
    // Cargo reads the underscore spelling too.
    let default_features = entry
        .get("default-features")
        .or_else(|| entry.get("default_features"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let features = entry
        .get("features")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    Ok(Some(Spec {
        package,
        version: version.to_owned(),
        default_features,
        features,
    }))
}

/// Which crate depends on which key, read from `[dependencies]` and
/// `[build-dependencies]`, and from both inside every `[target.<cfg>]` table.
fn declared(manifests: &[String]) -> Result<Declared, String> {
    let mut out = Declared::default();
    for manifest in manifests {
        let parsed: Table = manifest.parse().map_err(|e| format!("{e}"))?;
        let Some(name) = parsed
            .get("package")
            .and_then(|p| p.get("name"))
            .and_then(Value::as_str)
        else {
            return Err("a crate manifest has no package.name".to_owned());
        };
        let mut sections: Vec<&Value> = Vec::new();
        let targets = parsed.get("target").and_then(Value::as_table);
        for scope in std::iter::once(&parsed).chain(
            targets
                .into_iter()
                .flat_map(|t| t.values().filter_map(Value::as_table)),
        ) {
            sections.extend(
                ["dependencies", "build-dependencies"]
                    .iter()
                    .filter_map(|section| scope.get(*section)),
            );
        }
        for (key, value) in sections.iter().filter_map(|s| s.as_table()).flatten() {
            let inherited = value.get("workspace").and_then(Value::as_bool) == Some(true);
            if inherited {
                out.inherited
                    .entry(key.clone())
                    .or_default()
                    .insert(name.to_owned());
            }
            out.any
                .entry(key.clone())
                .or_default()
                .insert(name.to_owned());
        }
    }
    Ok(out)
}

/// Every key whose requirement differs between the two snapshots, sorted by
/// key.
///
/// A changed line names the crates inheriting the key at both revisions, an
/// added line the ones inheriting it now, and a removed line the ones that
/// inherited it before. A changed line leaves off a crate that starts or stops
/// inheriting the key, and every line leaves off a crate that declares the
/// dependency itself on the other side: in both cases its own manifest
/// changed, and the changelog gate asks for a fragment there. A line naming no
/// crate is dropped.
fn lines(before: &Snapshot, now: &Snapshot) -> Vec<Line> {
    let keys: BTreeSet<&String> = before.table.keys().chain(now.table.keys()).collect();
    let mut out = Vec::new();
    for key in keys {
        let (crates, change) = match (before.table.get(key), now.table.get(key)) {
            (Some(old), Some(new)) if old != new => (
                inheriting(now, key)
                    .intersection(&inheriting(before, key))
                    .cloned()
                    .collect(),
                Move::Changed(old.clone(), new.clone()),
            ),
            (None, Some(new)) => (
                without(inheriting(now, key), before, key),
                Move::Added(new.clone()),
            ),
            (Some(old), None) => (
                without(inheriting(before, key), now, key),
                Move::Removed(old.clone()),
            ),
            _ => continue,
        };
        if !crates.is_empty() {
            out.push(Line {
                key: key.clone(),
                crates,
                change,
            });
        }
    }
    out
}

/// The crates inheriting `key` in one snapshot.
fn inheriting(snapshot: &Snapshot, key: &str) -> BTreeSet<String> {
    snapshot
        .declared
        .inherited
        .get(key)
        .cloned()
        .unwrap_or_default()
}

/// `crates` less those that depend on `key` in `other`.
fn without(crates: BTreeSet<String>, other: &Snapshot, key: &str) -> BTreeSet<String> {
    let Some(there) = other.declared.any.get(key) else {
        return crates;
    };
    crates.difference(there).cloned().collect()
}

/// The entry, as fragment prose, or `None` when there are no lines.
fn render(lines: &[Line]) -> Option<String> {
    if lines.is_empty() {
        return None;
    }
    let crates: BTreeSet<&String> = lines.iter().flat_map(|line| &line.crates).collect();
    let mut out = format!(
        "**Dependency requirements** ({})\n\n{PROSE}\n\n",
        code_list(crates)
    );
    for line in lines {
        let what = match &line.change {
            Move::Added(new) => {
                format!("{}. New in this release.", describe(&line.key, new, false))
            }
            Move::Removed(old) => format!(
                "no longer a dependency. Previously {}.",
                describe(&line.key, old, false)
            ),
            Move::Changed(old, new) => {
                let renamed = old.package != new.package;
                format!(
                    "{}. Previously {}.",
                    describe(&line.key, new, renamed),
                    describe(&line.key, old, renamed)
                )
            }
        };
        out.push_str(&format!(
            "- `{}` ({}): {what}\n",
            line.key,
            code_list(&line.crates)
        ));
    }
    Some(out)
}

/// One requirement in words. The package is named where it differs from the
/// key, or on both sides of a line whose package changed.
fn describe(key: &str, spec: &Spec, name_package: bool) -> String {
    let mut out = format!("`{}`", spec.version);
    if name_package || spec.package.is_some() {
        let package = spec.package.as_deref().unwrap_or(key);
        out.push_str(&format!(" of the `{package}` package"));
    }
    let mut qualifiers = Vec::new();
    if !spec.default_features {
        qualifiers.push("without default features".to_owned());
    }
    if !spec.features.is_empty() {
        let features: Vec<String> = spec.features.iter().map(|f| format!("`{f}`")).collect();
        qualifiers.push(format!("with features {}", features.join(", ")));
    }
    if !qualifiers.is_empty() {
        out.push(' ');
        out.push_str(&qualifiers.join(", "));
    }
    out
}

/// Names in backticks, comma-separated.
fn code_list<'a>(names: impl IntoIterator<Item = &'a String>) -> String {
    names
        .into_iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests;
