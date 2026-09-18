//! Builds each publishable crate's rustdoc the way docs.rs builds it.
//!
//! One crate at a time, on nightly, with that crate's own
//! `[package.metadata.docs.rs]` table applied. `cargo xtask doc` builds the
//! workspace together on stable, which unifies features across members and
//! never sets `docsrs`, so nothing else here compiles that cfg or the nightly
//! rustdoc features gated on it.
//!
//! Six of the table's keys are applied: `all-features`, `no-default-features`,
//! `features`, `cargo-args`, `rustdoc-args` and `rustc-args`. `default-target`
//! and `targets` are refused, because this builds for the host alone and
//! honouring them means installing a target.

use std::path::Path;

use serde::Deserialize;

use crate::run::{self, Error, Outcome, Step};

/// The keys this gate refuses rather than ignores.
const UNSUPPORTED: [&str; 2] = ["default-target", "targets"];

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    pub(crate) name: String,
    pub(crate) flags: Vec<String>,
    pub(crate) rustdoc_args: Vec<String>,
    pub(crate) rustc_args: Vec<String>,
    /// The refused keys this crate sets, in the order `UNSUPPORTED` lists them.
    pub(crate) unsupported: Vec<String>,
}

pub(crate) fn run(root: &Path, explain: bool, nightly: &str) -> Outcome {
    let toolchain = format!("+{nightly}");
    if !explain
        && run::quiet(
            root,
            false,
            &Step::new("cargo", [&toolchain]).arg("--version"),
        )
        .is_err()
    {
        return Err(Error::msg(format!(
            "needs the {nightly} toolchain (rustup toolchain install {nightly})"
        )));
    }

    let metadata = run::capture(
        root,
        &Step::new(
            "cargo",
            ["metadata", "--no-deps", "--format-version", "1", "--locked"],
        ),
    )?;
    let plans = plans(&metadata)?;

    let mut built = 0;
    let mut failed = 0;
    for plan in &plans {
        if !plan.unsupported.is_empty() {
            eprintln!(
                "::error::{} sets {}, which this gate does not model: it builds for the host alone",
                plan.name,
                plan.unsupported.join(", ")
            );
            failed += 1;
            continue;
        }
        built += 1;
        let shown = if plan.flags.is_empty() {
            "(default features)".to_owned()
        } else {
            plan.flags.join(" ")
        };
        println!("docsrs: {} {shown}", plan.name);

        // `broken_intra_doc_links` denied on top of the crate's own args: a
        // dangling link renders as dead text on the published page and the
        // docs.rs build reports it nowhere. Denied by name rather than through
        // `-D warnings`, which would make every merge wait on the next rustdoc
        // lint arriving with a toolchain bump.
        let mut rustdocflags = plan.rustdoc_args.join(" ");
        if !rustdocflags.is_empty() {
            rustdocflags.push(' ');
        }
        rustdocflags.push_str("-D rustdoc::broken_intra_doc_links");

        let inherited = std::env::var("RUSTFLAGS").unwrap_or_default();
        let rustflags = format!("{inherited} {}", plan.rustc_args.join(" "));

        let step = Step::new("cargo", [&toolchain])
            .args(["doc", "-p", &plan.name, "--no-deps", "--locked"])
            .args(&plan.flags)
            .env("RUSTDOCFLAGS", rustdocflags)
            .env("RUSTFLAGS", rustflags);
        if run::run(root, explain, &step).is_err() {
            println!(
                "::error::rustdoc failed for {} as docs.rs would build it",
                plan.name
            );
            failed += 1;
        }
    }

    if built == 0 && failed == 0 {
        return Err(Error::msg(
            "no publishable crate found; this run checked nothing",
        ));
    }
    if failed != 0 {
        return Err(Error::msg(format!("{failed} crate(s) failed")));
    }
    println!("docsrs: {built} crate(s) documented as docs.rs builds them.");
    Ok(())
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    publish: Option<Vec<String>>,
    #[serde(default)]
    metadata: serde_json::Value,
}

/// One plan per publishable crate. `publish: []` marks a crate that is never
/// uploaded, so docs.rs never sees it.
pub(crate) fn plans(metadata: &str) -> Result<Vec<Plan>, Error> {
    let parsed: Metadata =
        serde_json::from_str(metadata).map_err(|e| Error::msg(format!("cargo metadata: {e}")))?;
    Ok(parsed
        .packages
        .iter()
        .filter(|p| p.publish.as_ref().is_none_or(|allow| !allow.is_empty()))
        .map(plan_for)
        .collect())
}

fn plan_for(package: &Package) -> Plan {
    // Cargo nests `[package.metadata.docs.rs]` as metadata.docs.rs, two levels.
    let table = package.metadata.get("docs").and_then(|d| d.get("rs"));
    let flag = |key: &str| {
        table
            .and_then(|t| t.get(key))
            .and_then(serde_json::Value::as_bool)
    };
    let list = |key: &str| -> Vec<String> {
        table
            .and_then(|t| t.get(key))
            .and_then(serde_json::Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    };

    let mut flags = Vec::new();
    if flag("all-features") == Some(true) {
        flags.push("--all-features".to_owned());
    }
    if flag("no-default-features") == Some(true) {
        flags.push("--no-default-features".to_owned());
    }
    let features = list("features");
    if !features.is_empty() {
        flags.push(format!("--features={}", features.join(",")));
    }
    flags.extend(list("cargo-args"));

    Plan {
        name: package.name.clone(),
        flags,
        rustdoc_args: list("rustdoc-args"),
        rustc_args: list("rustc-args"),
        unsupported: UNSUPPORTED
            .iter()
            .filter(|k| table.and_then(|t| t.get(*k)).is_some())
            .map(|k| (*k).to_owned())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(packages: &str) -> String {
        format!(r#"{{"packages":[{packages}]}}"#)
    }

    #[test]
    fn a_crate_with_no_table_builds_on_its_default_features() {
        let p = plans(&meta(r#"{"name":"a","publish":null,"metadata":null}"#)).unwrap();
        assert_eq!(p[0].flags, Vec::<String>::new());
        assert!(p[0].unsupported.is_empty());
    }

    #[test]
    fn publish_false_is_left_out() {
        let json = meta(
            r#"{"name":"a","publish":[],"metadata":null},{"name":"b","publish":null,"metadata":null}"#,
        );
        let names: Vec<_> = plans(&json).unwrap().into_iter().map(|p| p.name).collect();
        assert_eq!(names, ["b"]);
    }

    #[test]
    fn the_six_applied_keys_become_flags_in_order() {
        let json = meta(
            r#"{"name":"a","publish":null,"metadata":{"docs":{"rs":{
                "all-features":true,"no-default-features":true,
                "features":["x","y"],"cargo-args":["-Zbuild-std"],
                "rustdoc-args":["--cfg","docsrs"],"rustc-args":["--cfg","other"]}}}}"#,
        );
        let p = &plans(&json).unwrap()[0];
        assert_eq!(
            p.flags,
            [
                "--all-features",
                "--no-default-features",
                "--features=x,y",
                "-Zbuild-std"
            ]
        );
        assert_eq!(p.rustdoc_args, ["--cfg", "docsrs"]);
        assert_eq!(p.rustc_args, ["--cfg", "other"]);
    }

    #[test]
    fn a_refused_key_is_named_and_not_applied() {
        let json = meta(
            r#"{"name":"a","publish":null,"metadata":{"docs":{"rs":{
                "targets":["x86_64-unknown-linux-gnu"],"default-target":"x86_64-unknown-linux-gnu"}}}}"#,
        );
        let p = &plans(&json).unwrap()[0];
        assert_eq!(p.unsupported, ["default-target", "targets"]);
    }

    #[test]
    fn an_empty_features_list_adds_no_flag() {
        let json =
            meta(r#"{"name":"a","publish":null,"metadata":{"docs":{"rs":{"features":[]}}}}"#);
        assert_eq!(plans(&json).unwrap()[0].flags, Vec::<String>::new());
    }
}
