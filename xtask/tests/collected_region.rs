//! The process-level contract of `cargo xtask bench region`: the exit status
//! each outcome reports, which stream carries what, and the bytes of every
//! diagnostic.
//!
//! The two measured fixtures are callgrind profiles this repository's benches
//! produced under valgrind on Linux, reduced to one cost line per function.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The s3 descriptor bench with its measured work written inline in the benchmark
/// function: 99.53% glibc free path.
const DEGENERATE: &str = r#"# callgrind format
version: 1
creator: callgrind-3.19.0
cmd:  /target/release/deps/descriptor_gungraun-875c8d1e266b9134 --gungraun-run 00000 00000 00000
positions: line
events: Ir
summary: 942805

ob=/usr/lib/aarch64-linux-gnu/libc.so.6
fl=./stdlib/./stdlib/cxa_atexit.c
fn=__cxa_atexit
cfn=__internal_atexit
calls=1 36
70 942805
fn=__aarch64_swp8_acq
0 60
fn=_int_free
3389 55884
fn=_int_free'2
4674 62
fn=unlink_chunk.constprop.0
1657 277502
fn=malloc_consolidate
4670 26
fn=malloc_consolidate'2
4776 589270
fn=free'2
162 15589

ob=/usr/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1

ob=/target/release/deps/descriptor_gungraun-875c8d1e266b9134
fn=descriptor_gungraun::decode::__gungraun_wrapper_mod::decode
48 24
fn=descriptor_gungraun::decode::__gungraun_wrapper_mod::decode'2
48 4388
"#;

/// The ClickHouse RowBinary encoder, an allocation-heavy case at 84.99%.
const HEALTHY: &str = r#"# callgrind format
version: 1
creator: callgrind-3.19.0
cmd:  /target/release/deps/encode_gungraun-435f3f43eb3bf4fe --gungraun-run 00000 00003 00000
positions: line
events: Ir
summary: 730429

ob=/usr/lib/aarch64-linux-gnu/libc.so.6
fl=./stdlib/./stdlib/cxa_atexit.c
fn=__cxa_atexit
cfn=__internal_atexit
calls=1 36
70 730429
fn=__GI_memcpy
133 109623

ob=/usr/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1

ob=/target/release/deps/encode_gungraun-435f3f43eb3bf4fe
fn=<spate_clickhouse::encoder::ClickHouseEncoder<F> as spate_core::sink::RowEncoder<F>>::encode
171 34
fn=<spate_clickhouse::encoder::ClickHouseEncoder<F> as spate_core::sink::RowEncoder<F>>::encode'2
119 40968
fn=encode_gungraun::rows::_::<impl serde_core::ser::Serialize for encode_gungraun::rows::EventRow>::serialize
547 124000
fn=encode_gungraun::rows::_::<impl serde_core::ser::Serialize for encode_gungraun::rows::EventRow>::serialize'2
547 140711
fn=serde_core::ser::Serializer::collect_seq
547 80400
fn=serde_core::ser::Serializer::collect_seq'2
547 21800
fn=encode_gungraun::encode_chunk
189 30
fn=encode_gungraun::encode_rowbinary_events::__gungraun_wrapper_mod::encode_rowbinary_events
182 9
fn=spate_clickhouse::rowbinary::put_leb128
192 44000
fn=spate_clickhouse::rowbinary::put_leb128'2
192 106854
fn=<&mut spate_clickhouse::rowbinary::RowBinarySer as serde_core::ser::SerializeStruct>::serialize_field
562 29000
fn=<&mut spate_clickhouse::rowbinary::RowBinarySer as serde_core::ser::Serializer>::serialize_seq
402 33000
"#;

/// A region that collapsed to the instructions of the toggled wrapper.
const COLLAPSED: &str = r#"cmd:  /target/release/deps/encode_gungraun-0000000000000000 --gungraun-run 00000 00000 00000
positions: line
events: Ir
summary: 22

ob=/target/release/deps/encode_gungraun-0000000000000000
fn=encode_gungraun::encode::__gungraun_wrapper_mod::encode
48 22
"#;

/// The case path the degenerate fixture was measured under.
const DEG_CASE: &str = "spate-s3/descriptor_gungraun/descriptor/decode.full_splits";

/// The case path the healthy fixture was measured under.
const HEAL_CASE: &str =
    "spate-clickhouse/encode_gungraun/encode/encode_rowbinary_events.rowbinary_events";

/// A tree of profiles, removed with its contents.
struct Tree(PathBuf);

impl Tree {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-region-cli-{}-{name}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// Writes one profile at `rel` under the tree.
    fn profile(&self, rel: &str, body: &str) -> &Self {
        let path = self.0.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
        self
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// The tree's own path.
    fn root(&self) -> String {
        self.0.display().to_string()
    }

    fn at(&self, rel: &str) -> String {
        self.0.join(rel).display().to_string()
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

/// A profile carrying `app` application instructions out of `total`.
fn share(app: i64, total: i64) -> String {
    format!(
        "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: {total}\n\n\
         ob=/lib/libc.so.6\nfn=other\n1 {}\n\nob=/bin/app\nfn=measured\n2 {app}\n",
        total - app
    )
}

fn xtask(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(["bench", "region"])
        .args(args)
        // Both would otherwise depend on the host.
        .env_remove("GITHUB_ACTIONS")
        .env_remove("CARGO_TARGET_DIR")
        .output()
        .unwrap()
}

/// The exit status, stdout and stderr of one run, all three held at once.
#[track_caller]
fn held(args: &[&str], code: i32, out: &str, err: &str) {
    let got = xtask(args);
    let stdout = String::from_utf8_lossy(&got.stdout);
    let stderr = String::from_utf8_lossy(&got.stderr);
    assert_eq!(stdout, out, "stdout");
    assert_eq!(stderr, err, "stderr");
    assert_eq!(got.status.code(), Some(code), "{stdout}{stderr}");
}

/// The line a passing case reports.
fn passed(case: &str, pct: &str, total: i64) -> String {
    format!("{case}: {pct}% of {total} Ir in the binary under measurement\n")
}

/// The line closing a run that refused nothing.
fn summary(count: u32) -> String {
    format!(
        "collected-region: {count} case(s) attribute at least 10% of their collected instructions to the binary under measurement\n"
    )
}

/// The measured degenerate profile is refused at its measured share, and the
/// failure names the shard, the case, the profile and what to do about it.
#[test]
fn the_measured_degenerate_profile_is_refused_at_its_measured_share() {
    let tree = Tree::new("degenerate");
    tree.profile(
        &format!("{DEG_CASE}/callgrind.decode.full_splits.out"),
        DEGENERATE,
    );
    held(
        &["--shard", "spate-s3 (default)", &tree.root()],
        1,
        &format!(
            "::error::spate-s3 (default) — {DEG_CASE}: the collected region is 0.47% application code (4412 of 942805 Ir);\n\
             \x20 the rest is the C runtime, so this case is measuring the allocator rather than the code it names.\n\
             \x20 Profile: {}\n\
             \x20 The usual cause is the measured work being written inline in the #[library_benchmark]\n\
             \x20 function, where the optimizer may reshape it out of the collected region. Move it into a\n\
             \x20 named #[inline(never)] function the benchmark calls. See DEVELOPING.md.\n",
            tree.at(&format!("{DEG_CASE}/callgrind.decode.full_splits.out"))
        ),
        "",
    );
}

/// The allocation-heavy healthy profile keeps its measured share, and a saved
/// baseline sitting beside it is not judged.
#[test]
fn the_healthy_profile_is_accepted_and_its_baseline_ignored() {
    let tree = Tree::new("healthy");
    let profile = format!("{HEAL_CASE}/callgrind.encode_rowbinary_events.rowbinary_events.out");
    tree.profile(&profile, HEALTHY);
    let expected = passed(HEAL_CASE, "84.99", 730_429) + &summary(1);
    held(&[&tree.root()], 0, &expected, "");
    // A *degenerate* baseline, which judging would fail the run on.
    tree.profile(&format!("{profile}.base@base"), DEGENERATE);
    held(&[&tree.root()], 0, &expected, "");
}

/// A case split across threads is one measurement, judged once on the sum of
/// its parts, and a part that produced no records is refused.
#[test]
fn a_threaded_case_is_judged_once_and_a_lost_part_is_refused() {
    let tree = Tree::new("threaded");
    let case = "spate-core/chain_gungraun/chain/push_batch.owned";
    tree.profile(
        &format!("{case}/callgrind.x.t1.p1.out"),
        &share(4000, 10_000),
    );
    tree.profile(
        &format!("{case}/callgrind.x.t2.p1.out"),
        "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 0\n\ntotals: 0\n",
    );
    held(
        &[&tree.root()],
        0,
        &(passed(case, "40.00", 10_000) + &summary(1)),
        "",
    );

    std::fs::write(tree.path().join(case).join("callgrind.x.t2.p1.out"), "").unwrap();
    held(
        &[&tree.root()],
        1,
        &format!(
            "::error::{case}: its callgrind profile could not be read (unreadable-part  ).\n\
             \x20 {} (2 parts)\n\
             \x20 The guard refuses to judge a profile it cannot account for; see xtask/src/checks/collected_region.rs.\n",
            tree.at(case)
        ),
        "",
    );
}

/// A region that collapsed to the toggled wrapper is 100% application code, so
/// the magnitude floor is what catches it.
#[test]
fn a_collapsed_region_is_refused_at_the_magnitude_floor() {
    let tree = Tree::new("collapsed");
    let case = "spate-kafka/encode_gungraun/encode/encode.bytes_keyless";
    tree.profile(
        &format!("{case}/callgrind.encode.bytes_keyless.out"),
        COLLAPSED,
    );
    held(
        &[&tree.root()],
        1,
        &format!(
            "::error::{case}: the collected region is 22 Ir, below the 1000 floor;\n\
             \x20 a bench case cannot do meaningful work in that many instructions, so the region was lost\n\
             \x20 rather than measured: the same defect as a runtime-dominated region, wearing the other face.\n\
             \x20 Profile: {}\n\
             \x20 Move the measured work into a named #[inline(never)] function the benchmark calls,\n\
             \x20 and see DEVELOPING.md.\n",
            tree.at(&format!("{case}/callgrind.encode.bytes_keyless.out"))
        ),
        "",
    );
}

/// Ten percent and a thousand instructions are both admitted; a step below
/// either is refused.
#[test]
fn each_threshold_admits_its_own_boundary() {
    let tree = Tree::new("thresholds");
    let profile = "c/callgrind.x.out";
    for (app, total, code) in [
        (1000, 10_000, 0),
        (999, 10_000, 1),
        (1000, 1000, 0),
        (999, 999, 1),
    ] {
        tree.profile(profile, &share(app, total));
        let got = xtask(&[&tree.root()]);
        assert_eq!(
            got.status.code(),
            Some(code),
            "{app} of {total}: {}",
            String::from_utf8_lossy(&got.stdout)
        );
    }
}

/// A profile the parser cannot account for is refused by name.
#[test]
fn a_profile_that_does_not_account_for_itself_is_refused_by_name() {
    let tree = Tree::new("refusals");
    let profile = "c/callgrind.x.out";
    for (body, reason) in [
        (
            "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 999\n\nob=/bin/app\nfn=f\n1 40\n"
                .to_owned(),
            "totals-mismatch 40 999",
        ),
        (
            "cmd: /bin/app\npositions: line\nevents: Dr Dw\nsummary: 100 50\n\nob=/bin/app\nfn=f\n1 100 50\n"
                .to_owned(),
            "no-ir-column  ",
        ),
        (
            "positions: line\nevents: Ir\nsummary: 10\n\nob=/bin/app\nfn=f\n1 10\n".to_owned(),
            "no-cmd  ",
        ),
        (
            "cmd: /bin/real\npositions: line\nevents: Ir\nsummary: 40\n\nob=/bin/other\nfn=f\n1 40\n"
                .to_owned(),
            "no-binary  ",
        ),
    ] {
        tree.profile(profile, &body);
        held(
            &[&tree.root()],
            1,
            &format!(
                "::error::c: its callgrind profile could not be read ({reason}).\n\
                 \x20 {}\n\
                 \x20 The guard refuses to judge a profile it cannot account for; see xtask/src/checks/collected_region.rs.\n",
                tree.at(profile)
            ),
            "",
        );
    }
}

/// A tree holding no measurement and one that is not there both stop the run: a
/// job that measured nothing and one that measured well are otherwise the same
/// green job.
#[test]
fn a_tree_with_no_measurement_fails_closed() {
    let tree = Tree::new("bare");
    held(
        &["--shard", "spate-s3 (simd)", &tree.root()],
        1,
        &format!(
            "::error::spate-s3 (simd) — no callgrind profile under {}; the benches wrote no measurement to check.\n",
            tree.root()
        ),
        "",
    );
    // A saved baseline is no measurement of this shard's.
    tree.profile("c/callgrind.x.out.base@base", HEALTHY);
    held(
        &[&tree.root()],
        1,
        &format!(
            "::error::no callgrind profile under {}; the benches wrote no measurement to check.\n",
            tree.root()
        ),
        "",
    );
    let absent = tree.at("absent");
    held(
        &[&absent],
        1,
        "",
        &format!("xtask: {absent} does not exist; there are no profiles to check\n"),
    );
}

/// Every case is judged, and one refusal fails the run. A profile sitting
/// directly in the tree is named for its own directory.
#[test]
fn every_case_is_judged_and_one_refusal_fails_the_run() {
    let tree = Tree::new("mixed");
    tree.profile("callgrind.flat.out", &share(4000, 10_000));
    tree.profile("b/callgrind.x.out", &share(100, 10_000));
    let name = tree
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let got = xtask(&[&tree.root()]);
    assert_eq!(got.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&got.stdout),
        format!(
            "{}::error::b: the collected region is 1.00% application code (100 of 10000 Ir);\n\
             \x20 the rest is the C runtime, so this case is measuring the allocator rather than the code it names.\n\
             \x20 Profile: {}\n\
             \x20 The usual cause is the measured work being written inline in the #[library_benchmark]\n\
             \x20 function, where the optimizer may reshape it out of the collected region. Move it into a\n\
             \x20 named #[inline(never)] function the benchmark calls. See DEVELOPING.md.\n",
            passed(&name, "40.00", 10_000),
            tree.at("b/callgrind.x.out")
        )
    );
}

/// A `DIR` ending in `/` names its case by the relative path, and the profile
/// path the failure prints carries no doubled separator.
#[test]
fn a_trailing_separator_on_the_tree_does_not_reach_the_paths() {
    let tree = Tree::new("slash");
    tree.profile("b/callgrind.x.out", &share(100, 10_000));
    held(
        &[&format!("{}/", tree.root())],
        1,
        &format!(
            "::error::b: the collected region is 1.00% application code (100 of 10000 Ir);\n\
             \x20 the rest is the C runtime, so this case is measuring the allocator rather than the code it names.\n\
             \x20 Profile: {}\n\
             \x20 The usual cause is the measured work being written inline in the #[library_benchmark]\n\
             \x20 function, where the optimizer may reshape it out of the collected region. Move it into a\n\
             \x20 named #[inline(never)] function the benchmark calls. See DEVELOPING.md.\n",
            tree.at("b/callgrind.x.out")
        ),
        "",
    );
}

/// `--explain` names the tree it would read and reads none of it.
#[test]
fn explain_names_the_tree_and_reads_nothing() {
    held(&["--explain"], 0, "(reads target/gungraun)\n", "");
    held(
        &["--explain", "/no/such/tree"],
        0,
        "(reads /no/such/tree)\n",
        "",
    );
}

/// A part the process cannot read is refused by that name, where reading it as
/// empty would report the part nobody could count instead.
#[cfg(unix)]
#[test]
fn a_part_the_process_cannot_read_is_named_unreadable() {
    use std::os::unix::fs::PermissionsExt;
    let tree = Tree::new("unreadable");
    let profile = "c/callgrind.x.out";
    tree.profile(profile, HEALTHY);
    let path = tree.path().join(profile);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
    held(
        &[&tree.root()],
        1,
        &format!(
            "::error::c: its callgrind profile could not be read (unreadable  ).\n\
             \x20 {}\n\
             \x20 The guard refuses to judge a profile it cannot account for; see xtask/src/checks/collected_region.rs.\n",
            tree.at(profile)
        ),
        "",
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
