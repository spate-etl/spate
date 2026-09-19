//! What the guard makes of a callgrind profile: the attribution, the share it
//! derives, and the refusals that stop it deriving one.
//!
//! The fixtures are real. They are callgrind profiles this repository's benches
//! produced under valgrind on Linux, reduced to one cost line per function, so
//! every self cost is the measured one to the instruction.

use super::*;
use crate::checks::scratch::Scratch;

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

/// A profile whose cost lines do not add up to its own summary.
const INCONSISTENT: &str = r#"cmd:  /target/release/deps/chain_gungraun-a7b171c5ce77f5ac --gungraun-run 00000 00000 00001
positions: line
events: Ir
summary: 260758

ob=/target/release/deps/chain_gungraun-a7b171c5ce77f5ac
fn=chain_gungraun::chain::__gungraun_wrapper_mod::push_batch
48 151576
"#;

/// The Confluent-framing case's first thread, which carries every instruction.
const THREADED_T1: &str = r#"# callgrind format
version: 1
creator: callgrind-3.19.0
cmd:  /target/release/deps/decode_gungraun-5c06e881b9bb3e01 --gungraun-run 00001 00000 00002
part: 1
thread: 1
positions: line
events: Ir
summary: 2180066

ob=/usr/lib/aarch64-linux-gnu/libc.so.6
fn=clock_gettime@@GLIBC_2.17
86 40000
fn=free'2
0 132000
fn=_int_free
3389 264000
fn=_int_free'2
4698 56000
fn=malloc
1473 216000
fn=__GI_memcpy
183 152000

ob=/target/release/deps/decode_gungraun-5c06e881b9bb3e01
fn=__aarch64_cas4_acq
154 10000
fn=__aarch64_ldadd8_relax
252 10000
fn=__aarch64_ldadd4_rel
252 10000
fn=__aarch64_ldadd8_rel
252 10000
fn=spate_avro::cache::SchemaCache::eval
48 196000
fn=spate_avro::cache::SchemaCache::eval'2
0 114000
fn=core::hash::BuildHasher::hash_one
703 284000
fn=std::sys::pal::unix::time::Timespec::now
143 200000
fn=std::sys::pal::unix::time::Timespec::sub_timespec
179 128000
fn=<core::hash::sip::Hasher<S> as core::hash::Hasher>::write
130 76000
fn=<core::hash::sip::Hasher<S> as core::hash::Hasher>::write'2
301 16000
fn=spate_avro::deser::DecoderCore::decode
120 24
fn=spate_avro::deser::DecoderCore::decode'2
120 47976
fn=spate_avro::deser::DecoderCore::resolve
160 92000
fn=spate_avro::deser::DecoderCore::resolve'2
164 96000
fn=decode_gungraun::decode_confluent::__gungraun_wrapper_mod::decode_confluent
513 9
fn=decode_gungraun::decode_batch
165 54
fn=decode_gungraun::decode_batch'2
231 30003

ob=/usr/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1
"#;

/// The same case's second thread, which never entered the collected region.
const THREADED_T2: &str = r#"# callgrind format
version: 1
creator: callgrind-3.19.0
pid: 771
cmd:  /target/release/deps/decode_gungraun-5c06e881b9bb3e01 --gungraun-run 00001 00000 00002
part: 1
thread: 2

desc: Timerange: Basic block 0 - 38958101
desc: Trigger: Program termination

positions: line
events: Ir Dr Dw I1mr D1mr D1mw ILmr DLmr DLmw
summary: 0


totals: 0
"#;

/// A part carrying cost with no object of its own.
const ORPHAN_T2: &str = r#"cmd:  /target/release/deps/encode_gungraun-435f3f43eb3bf4fe --gungraun-run 00000 00003 00000
positions: line
events: Ir
summary: 730429

fn=an_orphan_frame_before_any_ob
0 730429
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

/// A profile declaring no `Ir` column.
const NO_IR: &str = r#"cmd:  /target/release/deps/chain_gungraun-a7b171c5ce77f5ac --gungraun-run 00000 00000 00001
positions: line
events: Dr Dw
summary: 100 50

ob=/target/release/deps/chain_gungraun-a7b171c5ce77f5ac
fn=chain_gungraun::chain::__gungraun_wrapper_mod::push_batch
48 100 50
"#;

/// A profile whose only cost line is charged to an object the command line does
/// not start with.
const FOREIGN: &str =
    "cmd: /bin/real\npositions: line\nevents: Ir\nsummary: 40\n\nob=/bin/other\nfn=f\n1 40\n";

/// A profile carrying `n` application instructions out of `total`.
fn share(app: i64, total: i64) -> String {
    format!(
        "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: {total}\n\n\
         ob=/lib/libc.so.6\nfn=other\n1 {}\n\nob=/bin/app\nfn=measured\n2 {app}\n",
        total - app
    )
}

#[track_caller]
fn ok(parts: &[&str]) -> (i64, String, i64, i64) {
    let texts: Vec<String> = parts.iter().map(|p| (*p).to_owned()).collect();
    match read_case(&texts) {
        Verdict::Ok {
            hundredths,
            pct,
            app,
            total,
        } => (hundredths, pct, app, total),
        other => panic!("{other:?}"),
    }
}

#[track_caller]
fn refusal(parts: &[&str]) -> Refusal {
    let texts: Vec<String> = parts.iter().map(|p| (*p).to_owned()).collect();
    match read_case(&texts) {
        Verdict::Refused(r) => r,
        other => panic!("{other:?}"),
    }
}

// ── The measured profiles ──────────────────────────────────────────────

/// The degenerate profile's 4,412 application instructions out of 942,805, the
/// share the measurement reported.
#[test]
fn the_degenerate_profile_attributes_its_cost_to_the_runtime() {
    assert_eq!(ok(&[DEGENERATE]), (46, "0.47".to_owned(), 4412, 942_805));
}

/// The allocation-heavy healthy profile's measured 84.99%. A parser that
/// started counting call costs would report another number here.
#[test]
fn the_healthy_profile_keeps_its_measured_share() {
    assert_eq!(ok(&[HEALTHY]), (8499, "84.99".to_owned(), 620_806, 730_429));
}

/// A case split across threads is one measurement, summed before it is judged.
/// Its second part declares `summary: 0` because that thread never entered the
/// collected region.
#[test]
fn a_case_split_across_threads_is_judged_on_the_sum() {
    assert_eq!(
        ok(&[THREADED_T1, THREADED_T2]),
        (6055, "60.55".to_owned(), 1_320_066, 2_180_066)
    );
}

/// Cost preceding a part's own `ob=` belongs to no object. Charging it to
/// whatever the previous part left current would report 92.50%, and the totals
/// check would not notice.
#[test]
fn cost_with_no_object_of_its_own_is_charged_to_no_object() {
    assert_eq!(
        ok(&[HEALTHY, ORPHAN_T2]),
        (4249, "42.50".to_owned(), 620_806, 1_460_858)
    );
}

/// A region that collapsed to the toggled wrapper is 100% application code, so
/// the composition rule passes it and the magnitude floor is what catches it.
#[test]
fn a_collapsed_region_still_reads_as_application_code() {
    assert_eq!(ok(&[COLLAPSED]), (10000, "100.00".to_owned(), 22, 22));
}

// ── The refusals ───────────────────────────────────────────────────────

/// A profile whose cost lines disagree with its own summary is refused, and the
/// refusal carries both totals.
#[test]
fn a_profile_disagreeing_with_its_own_totals_is_refused() {
    assert_eq!(
        refusal(&[INCONSISTENT]),
        Refusal {
            reason: "totals-mismatch",
            detail: Some((151_576, 260_758)),
        }
    );
}

/// A part that produced no records is refused. Counted only by the parser, a
/// healthy part beside an empty one would pass at the healthy part's share.
#[test]
fn a_part_that_produced_no_records_is_refused() {
    assert_eq!(refusal(&[THREADED_T1, ""]).reason, "unreadable-part");
}

/// Left unnamed, a missing `Ir` column surfaces as a totals mismatch from
/// summing the position field.
#[test]
fn a_profile_with_no_ir_column_says_so() {
    assert_eq!(refusal(&[NO_IR]).reason, "no-ir-column");
}

/// Without a command line there is nothing to match an object against.
#[test]
fn a_profile_naming_no_command_is_refused() {
    let no_cmd = HEALTHY
        .lines()
        .filter(|l| !l.starts_with("cmd:"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(refusal(&[&no_cmd]).reason, "no-cmd");
}

/// Zero across every part, where one part at zero is ordinary.
#[test]
fn a_case_that_collected_nothing_at_all_is_refused() {
    assert_eq!(refusal(&[THREADED_T2]).reason, "no-cost");
}

/// Every part has to declare a total of its own.
#[test]
fn a_part_declaring_no_total_is_refused() {
    let silent = "cmd: /bin/app\npositions: line\nevents: Ir\n\nob=/bin/app\nfn=f\n1 5\n";
    assert_eq!(
        refusal(&[&share(40, 100), silent]).reason,
        "partial-summary"
    );
}

/// An object the command line does not start with cannot be the binary under
/// measurement.
#[test]
fn a_profile_naming_no_object_of_the_command_is_refused() {
    assert_eq!(refusal(&[FOREIGN]).reason, "no-binary");
}

/// The refusals are ordered, so a profile carrying several is named for the
/// first the parser can be sure of.
#[test]
fn the_refusals_are_reported_in_order() {
    let neither = "positions: line\nevents: Dr\nsummary: 0\n";
    assert_eq!(refusal(&[neither]).reason, "no-cmd");
    let no_ir_and_no_cost = "cmd: /bin/app\nevents: Dr\nsummary: 0\n";
    assert_eq!(refusal(&[no_ir_and_no_cost]).reason, "no-ir-column");
    // An empty part beside one that collected nothing: the zero total is
    // reported before the part nobody could read.
    assert_eq!(refusal(&[THREADED_T2, ""]).reason, "no-cost");
    // A truncated part beside a healthy one, where both totals also disagree.
    assert_eq!(refusal(&[HEALTHY, ""]).reason, "unreadable-part");
    // A part declaring nothing, whose absence also breaks the totals.
    let quiet = "cmd: /bin/app\npositions: line\nevents: Ir\nob=/bin/app\nfn=f\n1 7\n";
    assert_eq!(refusal(&[quiet]).reason, "partial-summary");
}

// ── What a cost line is ────────────────────────────────────────────────

/// A cost line after a call or a jump belongs to the callee or the branch, and
/// callgrind excludes both from its totals.
#[test]
fn a_call_or_a_jump_cost_is_excluded_from_the_total() {
    for lead in ["calls=1 36", "jump=1 40", "jcnd=1/2 40"] {
        let text = format!(
            "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 5\n\n\
             ob=/bin/app\nfn=f\ncfn=g\n{lead}\n70 900\n1 5\n"
        );
        assert_eq!(ok(&[&text]).3, 5, "{lead}");
    }
}

/// Only the cost line directly after a call or a jump is the callee's.
#[test]
fn anything_between_a_call_and_a_cost_line_clears_the_exclusion() {
    let text = "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 900\n\n\
                ob=/bin/app\nfn=f\ncalls=1 36\n\n70 900\n";
    assert_eq!(ok(&[text]).3, 900);
}

/// The cost column is the one the `events:` line puts `Ir` in, offset by the
/// position columns the `positions:` line declares.
#[test]
fn the_cost_column_follows_the_declared_positions_and_events() {
    let text = "cmd: /bin/app\npositions: instr line\nevents: Dr Ir Dw\nsummary: 3 7 9\n\n\
                ob=/bin/app\nfn=f\n0x10 48 3 7 9\n";
    assert_eq!(ok(&[text]), (10000, "100.00".to_owned(), 7, 7));
}

/// `totals:` is the same quantity under another name, so a part carrying both
/// declares one total.
#[test]
fn a_part_carrying_both_summary_and_totals_declares_one_total() {
    let text = "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 9\ntotals: 9\n\n\
                ob=/bin/app\nfn=f\n1 9\n";
    assert_eq!(ok(&[text]).3, 9);
}

// ── Name compression ───────────────────────────────────────────────────

/// A position line may introduce an id and later refer to it.
#[test]
fn a_compressed_object_name_resolves_to_its_introduction() {
    let text = "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 30\n\n\
                ob=(1) /lib/libc.so.6\nfn=a\n1 10\nob=(2) /bin/app\nfn=b\n1 10\n\
                ob=(1)\nfn=c\n1 10\n";
    assert_eq!(ok(&[text]), (3333, "33.33".to_owned(), 10, 30));
}

/// The called-side forms share the namespace of the forms they mirror, so a
/// name introduced there resolves later.
#[test]
fn the_called_side_introduces_a_name_too() {
    let text = "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 10\n\n\
                cob=(1) /bin/app\nob=(1)\nfn=a\n1 10\n";
    assert_eq!(ok(&[text]), (10000, "100.00".to_owned(), 10, 10));
}

/// A name introduced in one part does not resolve in the next: header state
/// belongs to a file.
#[test]
fn name_compression_does_not_cross_a_part_boundary() {
    let first = "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 10\n\n\
                 ob=(1) /bin/app\nfn=a\n1 10\n";
    let second = "positions: line\nevents: Ir\nsummary: 10\n\nob=(1)\nfn=b\n1 10\n";
    assert_eq!(ok(&[first, second]), (5000, "50.00".to_owned(), 10, 20));
}

/// An id with no introduction resolves to nothing, and a malformed reference is
/// the name itself.
#[test]
fn deref_reads_the_forms_callgrind_writes() {
    let mut names = HashMap::new();
    assert_eq!(deref(&mut names, "/lib/libc.so.6"), "/lib/libc.so.6");
    assert_eq!(deref(&mut names, "(7"), "(7");
    assert_eq!(deref(&mut names, "(9)"), "");
    assert_eq!(deref(&mut names, "(9) \t/bin/app"), "/bin/app");
    assert_eq!(deref(&mut names, "(9)"), "/bin/app");
    // A later introduction under the same id replaces the name.
    assert_eq!(deref(&mut names, "(9) /bin/other"), "/bin/other");
    // A name carrying a parenthesis of its own, the form a deleted mapping
    // takes, is a name.
    assert_eq!(
        deref(&mut names, "/bin/app (deleted)"),
        "/bin/app (deleted)"
    );
    // Blanks are the space and the tab, so any other separator is the name.
    assert_eq!(deref(&mut names, "(3) \u{c}/bin/app"), "\u{c}/bin/app");
}

// ── Which object is the binary ─────────────────────────────────────────

/// Longest match wins, so a path that is a prefix of another cannot claim it.
/// An object that carried no cost is a candidate, since the profile named it.
#[test]
fn the_longest_object_the_command_starts_with_is_the_binary() {
    let seen: BTreeSet<String> = ["/bin", "/bin/app", "/bin/app-extra", "/lib/libc.so.6", ""]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(binary_object("/bin/app --run", &seen), Some("/bin/app"));
    assert_eq!(
        binary_object("/bin/app-extra --run", &seen),
        Some("/bin/app-extra")
    );
    assert_eq!(binary_object("/usr/bin/app", &seen), None);
    assert_eq!(binary_object("", &seen), None);
}

/// An object named by a `cob=` line alone is still a candidate.
#[test]
fn an_object_named_only_on_the_called_side_can_be_the_binary() {
    let text = "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 10\n\n\
                ob=/lib/libc.so.6\nfn=a\ncob=/bin/app\n1 10\n";
    assert_eq!(ok(&[text]), (0, "0.00".to_owned(), 0, 10));
}

// ── The thresholds ─────────────────────────────────────────────────────

/// Ten percent exactly passes; a hundredth below it does not. The share is
/// truncated, so 9.999% reads as 999 and fails where rounding would let it
/// through.
#[test]
fn the_composition_rule_admits_exactly_ten_percent() {
    assert_eq!(ok(&[&share(1000, 10_000)]).0, 1000);
    assert!(!report("", "case", "where", &verdict(1000, 10_000)));
    assert_eq!(ok(&[&share(999, 10_000)]).0, 999);
    assert!(report("", "case", "where", &verdict(999, 10_000)));
}

/// A thousand instructions is the floor; one below it is refused whatever the
/// surviving instructions belong to.
#[test]
fn the_magnitude_floor_admits_exactly_a_thousand_instructions() {
    assert!(!report("", "case", "where", &verdict(1000, 1000)));
    assert!(report("", "case", "where", &verdict(999, 999)));
}

/// The magnitude floor is judged before the composition rule, so a collapsed
/// region is named for the instructions it lost.
#[test]
fn the_magnitude_floor_is_judged_first() {
    assert!(report("", "case", "where", &verdict(22, 22)));
    assert_eq!(MIN_COLLECTED_IR, 1000);
    assert_eq!(MIN_APPLICATION_PCT, 10);
}

/// The verdict a case of `app` application instructions out of `total` carries.
fn verdict(app: i64, total: i64) -> Verdict {
    let texts = vec![share(app, total)];
    read_case(&texts)
}

// ── Discovery ──────────────────────────────────────────────────────────

/// A saved baseline lands beside the head measurement and is not this shard's
/// business.
#[test]
fn only_a_head_leg_profile_is_a_profile() {
    assert!(is_profile("callgrind.decode.full_splits.out"));
    assert!(is_profile("callgrind.x.out.t1.p1.out"));
    assert!(is_profile("callgrind..out"));
    assert!(!is_profile("callgrind.out"));
    assert!(!is_profile("callgrind.x.out.base@base"));
    assert!(!is_profile("callgrind.a@b.out"));
    assert!(!is_profile("summary.json"));
    assert!(!is_profile("acallgrind.x.out"));
    assert!(!is_profile("callgrind.x.outx"));
    // Both dots are part of the pattern.
    assert!(!is_profile("callgrindxx.out"));
    assert!(!is_profile("callgrind.xxout"));
}

#[test]
fn basename_reports_the_last_component() {
    assert_eq!(basename("target/gungraun"), "gungraun");
    assert_eq!(basename("target/gungraun/"), "gungraun");
    assert_eq!(basename("gungraun"), "gungraun");
    assert_eq!(basename("/"), "/");
    assert_eq!(basename(""), "");
}

#[test]
fn the_target_tree_falls_back_to_the_default_cargo_directory() {
    assert_eq!(target_tree(None), "target/gungraun");
    assert_eq!(target_tree(Some("")), "target/gungraun");
    assert_eq!(target_tree(Some("/build")), "/build/gungraun");
}

/// One directory per case, deduplicated and sorted bytewise, each against the
/// path it is read by, with the tree itself named by the empty path.
#[test]
fn every_directory_holding_a_profile_is_one_case() {
    let scratch = Scratch::new("spate-xtask-region-discovery").unwrap();
    let base = scratch.dir();
    for (dir, name) in [
        ("", "callgrind.top.out"),
        ("b/one", "callgrind.a.t1.p1.out"),
        ("b/one", "callgrind.a.t2.p1.out"),
        ("a-later", "callgrind.c.out"),
        ("b/two", "callgrind.d.out.base@base"),
        ("empty", "summary.json"),
    ] {
        std::fs::create_dir_all(base.join(dir)).unwrap();
        std::fs::write(base.join(dir).join(name), "").unwrap();
    }
    assert_eq!(
        case_dirs(base).unwrap().into_iter().collect::<Vec<_>>(),
        [
            (String::new(), base.to_path_buf()),
            ("a-later".to_owned(), base.join("a-later")),
            ("b/one".to_owned(), base.join("b/one")),
        ]
    );
    let parts = case_parts(&base.join("b/one"), "T/b/one");
    assert_eq!(
        parts.iter().map(|(d, _)| d.as_str()).collect::<Vec<_>>(),
        [
            "T/b/one/callgrind.a.t1.p1.out",
            "T/b/one/callgrind.a.t2.p1.out"
        ]
    );
}

/// A directory named like a profile is a part of the case it holds, and its
/// parent is a case of its own.
#[test]
fn a_directory_named_like_a_profile_is_a_part() {
    let scratch = Scratch::new("spate-xtask-region-dir-part").unwrap();
    let base = scratch.dir();
    let nested = base.join("c/callgrind.x.out");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("callgrind.y.out"), "").unwrap();
    assert_eq!(
        case_dirs(base).unwrap().into_keys().collect::<Vec<_>>(),
        ["c", "c/callgrind.x.out"]
    );
    let parts = case_parts(&nested, "T/c/callgrind.x.out");
    assert_eq!(
        parts.iter().map(|(d, _)| d.as_str()).collect::<Vec<_>>(),
        ["T/c/callgrind.x.out", "T/c/callgrind.x.out/callgrind.y.out"]
    );
}

/// A case whose directory name is not UTF-8 is judged, where losing it would
/// leave the rest of the run green.
#[cfg(target_os = "linux")]
#[test]
fn a_case_named_in_invalid_utf8_is_judged() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let scratch = Scratch::new("spate-xtask-region-non-utf8").unwrap();
    let base = scratch.join("t");
    let lossy = base.join(OsStr::from_bytes(b"case\xff"));
    std::fs::create_dir_all(&lossy).unwrap();
    std::fs::write(lossy.join("callgrind.x.out"), share(100, 10_000)).unwrap();
    let healthy = base.join("plain");
    std::fs::create_dir_all(&healthy).unwrap();
    std::fs::write(healthy.join("callgrind.y.out"), share(9000, 10_000)).unwrap();
    assert_eq!(
        check_dir(scratch.dir(), "t", "").map_err(|e| e.code),
        Err(Some(1))
    );
}

/// A tree the process cannot read stops the guard, where losing the cases under
/// it would leave the rest of the run green.
#[cfg(unix)]
#[test]
fn an_unreadable_directory_stops_the_walk() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("spate-xtask-region-unreadable").unwrap();
    let shut = scratch.join("shut");
    std::fs::create_dir_all(&shut).unwrap();
    std::fs::set_permissions(&shut, std::fs::Permissions::from_mode(0o000)).unwrap();
    let out = case_dirs(scratch.dir());
    std::fs::set_permissions(&shut, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(out.is_err(), "{out:?}");
}

// ── Record, field and number conventions ───────────────────────────────

#[test]
fn a_trailing_newline_does_not_make_a_record() {
    assert!(records("").is_empty());
    assert_eq!(records("\n"), [""]);
    assert_eq!(records("a\n"), ["a"]);
    assert_eq!(records("a"), ["a"]);
    assert_eq!(records("a\n\nb\n"), ["a", "", "b"]);
}

#[test]
fn blanks_separate_fields_and_zero_names_the_whole_line() {
    let line = "  a \t b  ";
    let f = fields(line);
    assert_eq!(f, ["a", "b"]);
    assert_eq!(field(line, &f, 0), line);
    assert_eq!(field(line, &f, 1), "a");
    assert_eq!(field(line, &f, 2), "b");
    assert_eq!(field(line, &f, 3), "");
    assert!(fields("").is_empty());
}

/// A field carries the number its longest numeric prefix names.
#[test]
fn a_field_carries_its_longest_numeric_prefix() {
    for (text, want) in [
        ("0", 0.0),
        ("942805", 942_805.0),
        (" \t12", 12.0),
        ("+5", 5.0),
        ("-5", -5.0),
        ("12abc", 12.0),
        ("1.5", 1.5),
        (".5", 0.5),
        ("5.", 5.0),
        ("+.5", 0.5),
        ("-.5", -0.5),
        ("5.e3", 5000.0),
        ("1e3", 1000.0),
        ("1E+3", 1000.0),
        ("1e-3", 0.001),
        ("1e", 1.0),
        ("1e+", 1.0),
        ("0x10", 0.0),
        ("", 0.0),
        ("*", 0.0),
        ("-", 0.0),
        ("+", 0.0),
        ("abc", 0.0),
    ] {
        assert!(
            (numeric(text) - want).abs() < f64::EPSILON,
            "{text:?} read as {}",
            numeric(text)
        );
    }
}

#[test]
fn a_line_opens_with_one_of_the_prefixes_or_none() {
    assert!(starts_with_any("fe=x", &["fl=", "fi=", "fe="]));
    assert!(!starts_with_any("fn=x", &["fl=", "fi=", "fe="]));
    assert!(!starts_with_any("x", &[]));
}

/// A refusal carrying no detail still pads to the three fields the diagnostic
/// names.
#[test]
fn a_refusal_renders_three_fields() {
    assert_eq!(Refusal::bare("no-cmd").fields(), "no-cmd  ");
    assert_eq!(
        Refusal {
            reason: "totals-mismatch",
            detail: Some((1, 2)),
        }
        .fields(),
        "totals-mismatch 1 2"
    );
}

// ── The tree as a whole ────────────────────────────────────────────────

/// A tree that is not there, and one holding no profile, both stop the run: a
/// job that measured nothing and one that measured well are otherwise the same
/// green job.
#[test]
fn a_tree_with_no_measurement_fails_closed() {
    let scratch = Scratch::new("spate-xtask-region-empty").unwrap();
    let missing = check_dir(scratch.dir(), "absent", "");
    assert_eq!(
        missing.map_err(|e| e.message),
        Err("absent does not exist; there are no profiles to check".to_owned())
    );
    std::fs::create_dir_all(scratch.join("bare")).unwrap();
    assert_eq!(
        check_dir(scratch.dir(), "bare", "").map_err(|e| e.code),
        Err(Some(1))
    );
}

/// An object path carrying a parenthesis is read as a name.
#[test]
fn an_object_path_with_a_parenthesis_is_not_a_reference() {
    let text = "cmd: /bin/app (deleted)\npositions: line\nevents: Ir\nsummary: 5\n\n\
                ob=/bin/app (deleted)\nfn=f\n1 5\n";
    assert_eq!(ok(&[text]), (10000, "100.00".to_owned(), 5, 5));
}

/// A part carrying no `positions:` or `events:` line takes callgrind's own
/// defaults: one position column, and `Ir` first.
#[test]
fn a_part_declaring_no_header_takes_the_default_columns() {
    for header in ["", "positions: line\n", "events: Ir\n"] {
        let text = format!("cmd: /bin/app\n{header}summary: 5\n\nob=/bin/app\nfn=f\n1 5\n");
        assert_eq!(
            ok(&[&text]),
            (10000, "100.00".to_owned(), 5, 5),
            "{header:?}"
        );
    }
}

/// The command line is the first a case declares, so a later part cannot move
/// which object counts as the binary.
#[test]
fn the_first_command_line_a_case_declares_is_the_one() {
    let first =
        "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 5\n\nob=/bin/app\nfn=f\n1 5\n";
    let second =
        "cmd: /bin/other\npositions: line\nevents: Ir\nsummary: 5\n\nob=/bin/app\nfn=g\n1 5\n";
    assert_eq!(ok(&[first, second]), (10000, "100.00".to_owned(), 10, 10));
}

/// The last `Ir` an events line names is the column read.
#[test]
fn the_last_ir_column_an_events_line_names_is_the_one() {
    let text = "cmd: /bin/app\npositions: line\nevents: Ir Ir\nsummary: 5 7\n\n\
                ob=/bin/app\nfn=f\n1 5 7\n";
    assert_eq!(ok(&[text]).3, 7);
}

/// A part declaring only `totals:` has declared its total.
#[test]
fn totals_alone_is_a_declaration() {
    let text = "cmd: /bin/app\npositions: line\nevents: Ir\ntotals: 5\n\nob=/bin/app\nfn=f\n1 5\n";
    assert_eq!(ok(&[text]).3, 5);
}

/// Ids are per name kind, so an id introduced by a file or function line does
/// not resolve an object reference.
#[test]
fn only_an_object_line_introduces_an_object_name() {
    for form in ["fl", "fi", "fe", "cfi", "cfl", "fn", "cfn"] {
        let text = format!(
            "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 5\n\n\
             {form}=(1) /bin/app\nob=(1)\n1 5\n"
        );
        assert_eq!(refusal(&[&text]).reason, "no-binary", "{form}");
    }
    for form in ["ob", "cob"] {
        let text = format!(
            "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 5\n\n\
             {form}=(1) /bin/app\nob=(1)\n1 5\n"
        );
        assert_eq!(ok(&[&text]).3, 5, "{form}");
    }
}

/// A name line between a call and the cost line leaves the exclusion standing,
/// so the callee's cost stays out of the totals.
#[test]
fn a_name_line_does_not_clear_the_call_exclusion() {
    for form in [
        "ob=/bin/app",
        "cob=/bin/app",
        "fl=./x.c",
        "fi=./x.c",
        "fe=./x.c",
        "cfi=./x.c",
        "cfl=./x.c",
        "fn=g",
        "cfn=g",
    ] {
        let text = format!(
            "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 5\n\n\
             ob=/bin/app\nfn=f\ncalls=1 36\n{form}\n70 900\n1 5\n"
        );
        assert_eq!(ok(&[&text]).3, 5, "{form}");
    }
}

/// A compressed position is a cost line: callgrind writes the position relative
/// to the last one, and the line still carries a cost.
#[test]
fn a_compressed_position_still_carries_a_cost() {
    let text = "cmd: /bin/app\npositions: line\nevents: Ir\nsummary: 20\n\n\
                ob=/bin/app\nfn=f\n1 5\n+2 5\n-1 5\n* 5\n";
    assert_eq!(ok(&[text]).3, 20);
}

/// A record ends at a newline alone, so a carriage return stays in the line it
/// belongs to: it cannot clear a call exclusion, and `Ir\r` is not the `Ir`
/// column.
#[test]
fn a_carriage_return_does_not_end_a_record() {
    let text = "cmd: /bin/app\r\npositions: line\r\nsummary: 5\r\n\r\n\
                ob=/bin/app\r\nfn=f\r\ncalls=1 36\r\n70 900\r\n1 5\r\n";
    assert_eq!(ok(&[text]), (10000, "100.00".to_owned(), 5, 5));
    let declared = "cmd: /bin/app\r\nevents: Ir\r\nsummary: 5\r\n\r\nob=/bin/app\r\n1 5\r\n";
    assert_eq!(refusal(&[declared]).reason, "no-ir-column");
}
