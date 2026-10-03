//! Offline checks of the release generator dispatch and its required CI gate.

use std::{fs, os::unix::fs::PermissionsExt, process::Command};

use super::scratch::Scratch;

const SCRIPT: &str = include_str!("../../../scripts/release.sh");
const CI: &str = include_str!("../../../.github/workflows/ci.yml");
const CARGO: &str = r#"#!/usr/bin/env bash
set -eu
printf '%s|epoch=%s\n' "$*" "${SOURCE_DATE_EPOCH:-}" >> calls
case "$*" in
'about --version') [ "$MODE" != about-fail ] || exit 7; [ "$MODE" = about-empty ] || echo "$ABOUT" ;;
'cyclonedx --version') [ "$MODE" != sbom-fail ] || exit 7; [ "$MODE" = sbom-empty ] || echo "$SBOM" ;;
'xtask attribution')
  [ "$MODE" = attribution-missing ] || echo 'fresh inventory' > THIRD-PARTY.md
  [ "$MODE" != attribution-fail ] || exit 7 ;;
'cyclonedx -f json --describe crate --all-features --target all --spec-version 1.5 -q')
  [ "$MODE" != generation-missing ] || exit 0
  data='{"bomFormat":"CycloneDX","specVersion":"1.5","metadata":{"component":{"name":"sample","version":"1.2.3"}}}'
  case "$MODE" in
    malformed) data='{' ;;
    format) data=${data/CycloneDX/Other} ;;
    spec) data=${data/1.5/1.4} ;;
    name) data=${data/sample/other} ;;
    version) data=${data/1.2.3/9.9.9} ;;
  esac
  echo "$data" > crates/sample/sample.cdx.json
  echo '{}' > private/private.cdx.json
  [ "$MODE" != generation-fail ] || exit 7 ;;
'metadata --locked --no-deps --format-version 1'|'metadata --no-deps --format-version 1')
  printf '{"packages":[{"name":"sample","publish":null,"manifest_path":"%s/crates/sample/Cargo.toml"},{"name":"private","publish":[],"manifest_path":"%s/private/Cargo.toml"}]}\n' "$PWD" "$PWD" ;;
*) echo "forbidden cargo: $*" >&2; exit 99 ;;
esac
"#;

fn executable(path: &std::path::Path, content: &str) {
    fs::write(path, content).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn run(mode: &str, about: &str, sbom: &str) -> (std::process::Output, String) {
    let scratch = Scratch::new("release-generators").unwrap();
    for dir in ["scripts", "bin", "crates/sample", "private"] {
        fs::create_dir_all(scratch.join(dir)).unwrap();
    }
    fs::write(
        scratch.join("Cargo.toml"),
        "[workspace.package]\nversion = \"1.2.3\"\n",
    )
    .unwrap();
    fs::write(scratch.join("THIRD-PARTY.md"), "stale inventory").unwrap();
    fs::write(scratch.join("scripts/release.sh"), SCRIPT).unwrap();
    executable(&scratch.join("bin/cargo"), CARGO);
    executable(
        &scratch.join("bin/git"),
        "#!/bin/sh\n[ \"$*\" = 'log -1 --format=%ct' ] || exit 99\necho 123456\n",
    );
    for tool in ["gh", "curl"] {
        executable(
            &scratch.join(&format!("bin/{tool}")),
            "#!/bin/sh\necho forbidden >> forbidden\nexit 99\n",
        );
    }
    let output = Command::new("bash")
        .arg(scratch.join("scripts/release.sh"))
        .arg("check-generators")
        .env(
            "PATH",
            format!(
                "{}:{}",
                scratch.join("bin").display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env("MODE", mode)
        .env("ABOUT", about)
        .env("SBOM", sbom)
        .output()
        .unwrap();
    assert!(!scratch.join("forbidden").exists());
    let calls = fs::read_to_string(scratch.join("calls")).unwrap_or_default();
    if output.status.success() {
        assert_eq!(
            fs::read_to_string(scratch.join("THIRD-PARTY.md")).unwrap(),
            "fresh inventory\n"
        );
        assert!(!scratch.join("private/private.cdx.json").exists());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("sample-1.2.3.cdx.json"), "{stdout}");
        assert!(stdout.contains("checked 1 SBOM"), "{stdout}");
        assert!(calls.contains("xtask attribution|epoch="));
        assert!(calls.contains("--spec-version 1.5 -q|epoch=123456"));
    }
    (output, calls)
}

/// Runnable versions exercise both real generation paths. Regression for #801.
#[test]
fn action_selected_generators_pass_release_checks() {
    for (about, sbom) in [
        ("cargo-about 0.9.2", "cargo-cyclonedx-cyclonedx 0.5.9"),
        ("about arbitrary", "cyclonedx arbitrary"),
    ] {
        let (output, calls) = run("ok", about, sbom);
        assert!(
            output.status.success(),
            "{calls}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains(about) && stdout.contains(sbom));
        assert!(calls.contains("about --version") && calls.contains("cyclonedx --version"));
    }
}

/// Missing or empty version responses fail before generation. Regression for #801.
#[test]
fn missing_generator_stops_before_generation() {
    for mode in ["about-fail", "about-empty", "sbom-fail", "sbom-empty"] {
        let (output, calls) = run(mode, "about", "cyclonedx");
        assert!(!output.status.success(), "{mode}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        let tool = if mode.starts_with("about") {
            "cargo-about --locked --features cli"
        } else {
            "cargo-cyclonedx --locked"
        };
        assert!(stderr.contains(tool), "{mode}: {stderr}");
        assert!(!calls.contains("xtask attribution") && !calls.contains("-f json"));
    }
}

/// Failed generators and absent fresh outputs reject the checkout. Regression for #801.
#[test]
fn generation_failure_and_missing_output_fail_the_check() {
    for mode in [
        "attribution-fail",
        "attribution-missing",
        "generation-fail",
        "generation-missing",
    ] {
        let (output, calls) = run(mode, "about", "cyclonedx");
        assert!(calls.contains("xtask attribution"), "{mode}: {calls}");
        if !mode.starts_with("attribution") {
            assert!(
                calls.contains("--spec-version 1.5 -q|epoch=123456"),
                "{mode}: {calls}"
            );
        }
        assert!(!output.status.success(), "{mode}");
    }
}

/// The collected documents satisfy the release SBOM contract. Regression for #801.
#[test]
fn release_sbom_contract_and_options_are_checked() {
    for mode in ["malformed", "format", "spec", "name", "version"] {
        let (output, calls) = run(mode, "about", "cyclonedx");
        assert!(calls.contains("xtask attribution"), "{mode}: {calls}");
        if !mode.starts_with("attribution") {
            assert!(
                calls.contains("--spec-version 1.5 -q|epoch=123456"),
                "{mode}: {calls}"
            );
        }
        assert!(!output.status.success(), "{mode}");
    }
}

fn function(name: &str) -> &str {
    SCRIPT
        .split_once(&format!("\n{name}() {{\n"))
        .unwrap()
        .1
        .split_once("\n}\n")
        .unwrap()
        .0
}

/// Release entry points share the availability checks exercised in CI. Regression for #801.
#[test]
fn release_preflight_uses_the_exercised_availability_checks() {
    assert!(function("preflight").contains("\n    preflight_about_tool\n"));
    for name in ["preflight", "preflight_about_tool", "preflight_sbom_tool"] {
        let block = function(name);
        assert!(
            !block.contains("0.9.1") && !block.contains("0.5.9") && !block.contains("--version 0.")
        );
    }
    let check = function("check_generators");
    assert!(
        check.contains("\n    preflight_about_tool\n")
            && check.contains("\n    preflight_sbom_tool\n")
    );
    for name in ["dry_run", "prepare"] {
        assert!(function(name).contains("preflight_sbom_tool"), "{name}");
    }
}

fn block<'a>(source: &'a str, indent: usize, key: &str) -> &'a str {
    let marker = format!("{}{}:\n", " ".repeat(indent), key);
    let matches: Vec<_> = source
        .match_indices(&marker)
        .filter(|(i, _)| *i == 0 || source.as_bytes()[i - 1] == b'\n')
        .collect();
    assert_eq!(matches.len(), 1, "ambiguous or absent {key}");
    let start = matches[0].0 + marker.len();
    let rest = &source[start..];
    let mut end = 0;
    for line in rest.split_inclusive('\n') {
        let trimmed = line.trim();
        if !trimmed.is_empty()
            && !trimmed.starts_with('#')
            && line.len() - line.trim_start().len() <= indent
        {
            break;
        }
        end += line.len();
    }
    &rest[..end]
}

fn value(block: &str, indent: usize, key: &str) -> String {
    let prefix = format!("{}{key}:", " ".repeat(indent));
    let values: Vec<_> = block
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .collect();
    assert_eq!(values.len(), 1, "absent or ambiguous {key}");
    values[0].trim().to_owned()
}

fn tools(source: &str, job: &str, expected: &str) {
    let job = block(source, 2, job);
    let setup: Vec<_> = job
        .split("      - ")
        .filter(|step| {
            step.lines()
                .any(|line| line.trim() == "uses: ./.github/actions/setup-rust")
        })
        .collect();
    assert_eq!(setup.len(), 1);
    assert_eq!(value(setup[0], 10, "tools"), expected);
}

/// Ordinary CI requires generation through the shared installer. Regression for #801.
#[test]
fn generator_gate_is_required_and_uses_the_shared_installer() {
    let job = block(CI, 2, "release-generators");
    assert_eq!(value(job, 4, "needs"), "[lint, workflows]");
    assert_eq!(value(job, 4, "timeout-minutes"), "30");
    assert_eq!(value(job, 4, "runs-on"), "ubuntu-latest");
    assert!(
        !job.lines()
            .any(|line| line.starts_with("    if:") || line.starts_with("    continue-on-error:"))
    );
    tools(CI, "release-generators", "cargo-about,cargo-cyclonedx");
    assert!(
        job.lines().any(|line| line.trim().trim_start_matches("- ")
            == "run: ./scripts/release.sh check-generators")
    );
    assert!(
        job.lines()
            .any(|line| line.trim() == "persist-credentials: false")
    );
    assert!(
        value(block(CI, 2, "ci-gate"), 4, "needs")
            .split(|c: char| !c.is_alphanumeric() && c != '-')
            .any(|key| key == "release-generators")
    );
    let release = include_str!("../../../.github/workflows/release.yml");
    tools(release, "assemble", "cargo-about");
    tools(release, "publish", "cargo-cyclonedx");
    tools(
        include_str!("../../../.github/workflows/scheduled.yml"),
        "attribution",
        "cargo-about",
    );
    let action = include_str!("../../../.github/actions/setup-rust/action.yml");
    let installer: Vec<_> = action
        .split("    - ")
        .filter(|step| {
            step.lines().any(|line| {
                line.trim_start()
                    .starts_with("uses: taiki-e/install-action@")
            })
        })
        .collect();
    assert_eq!(installer.len(), 1);
    assert_eq!(value(installer[0], 8, "tool"), "${{ inputs.tools }}");
}

/// YAML extraction excludes comments and neighboring jobs.
#[test]
fn yaml_blocks_are_bounded_and_unambiguous() {
    let source =
        "jobs:\n  wanted:\n    name: yes\n    # if: misleading\n  neighbor:\n    if: true\n";
    let wanted = block(source, 2, "wanted");
    assert_eq!(value(wanted, 4, "name"), "yes");
    assert!(!wanted.contains("    if: true"));
    assert!(std::panic::catch_unwind(|| block("  wanted:\n  wanted:\n", 2, "wanted")).is_err());
    assert!(std::panic::catch_unwind(|| value("    # name: yes\n", 4, "name")).is_err());
}
