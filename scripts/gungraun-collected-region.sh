#!/usr/bin/env bash
#
# Reject a bench case whose collected region measured the runtime instead of
# the code under test.
#
#   scripts/gungraun-collected-region.sh [--shard LABEL] [DIR]
#   scripts/gungraun-collected-region.sh --self-test
#
# DIR defaults to `target/gungraun`, one callgrind profile per bench case.
# `--shard LABEL` prefixes every line with the (package, arm) being measured.
#
# A gungraun bench can report a plausible number while measuring nothing: the
# callgrind toggle bounding the collected region flips collection rather than
# forcing it on, so work the optimizer reshapes can fall outside the region and
# glibc's free path is counted in its place. The bench builds, runs, exits 0,
# reports a number in the millions, and renders a report row.
#
# Every case is judged on its own.
#
# The classification axis is the ELF object each instruction executed in, from
# callgrind's `ob=` lines. Every crate and dependency is compiled into the bench
# executable, so "the binary under measurement" is the whole application.
#
# DEVELOPING.md states the bench-authoring rule this enforces and where the
# thresholds come from.
set -euo pipefail

# The share of a case's collected instructions that must land in the binary
# under measurement. The measured spread it sits below is in DEVELOPING.md.
MIN_APPLICATION_PCT=10

# The second signal. A lost region can also leave almost nothing: a handful of
# instructions belonging to the toggled wrapper. That is application code, and
# the composition rule passes it. The floor is 1,000 against a smallest real
# case of 6,656.
MIN_COLLECTED_IR=1000

cd "$(dirname "$0")/.."

fail() {
    echo "gungraun-collected-region.sh: $1" >&2
    exit 1
}

# One *case's* verdict, on stdout, as a single line:
#
#   OK <hundredths of a percent> <percent, for reading> <application Ir> <total Ir>
#   ERROR <reason> [detail...]
#
# Every argument is one part of the same case, summed before anything is
# judged. callgrind writes one output per thread, `<base>.t<thread>.p<part>.out`,
# and a thread that never entered the collected region declares `summary: 0`.
# Judged one file at a time that part reads as a region that collected nothing.
#
# The gate reads the integer; the decimal is for the human line. `LC_ALL=C`
# because `%f` honours `LC_NUMERIC`: under a comma-decimal locale awk renders
# 84.99 as `84,99` and the shell reads a different number.
read_case() {
    local expected=$#
    LC_ALL=C awk -v parts="$expected" '
        # Name compression: a position line may introduce an id
        # (`ob=(1) /lib/libc.so.6`) and later refer to it (`ob=(1)`). Ids are
        # per name kind, and `cob=`/`cfn=`/`cfi=` share the namespace of
        # `ob=`/`fn=`/`fl=`, so a name introduced on the called side is recorded
        # too. `endpos` rather than `close`: `close` is an awk builtin and cannot
        # be a parameter name on every awk this runs under.
        function deref(kind, v,   endpos, id, name) {
            if (substr(v, 1, 1) != "(") return v
            endpos = index(v, ")")
            if (endpos == 0) return v
            id = substr(v, 2, endpos - 2)
            name = substr(v, endpos + 1)
            sub(/^[ \t]+/, "", name)
            if (name != "") names[kind SUBSEP id] = name
            return names[kind SUBSEP id]
        }

        # Header state belongs to a file; cost belongs to the case, so `ir[]`,
        # `total`, `seen[]` and the declared total accumulate across parts.
        # `positions: line` and `Ir` first are the defaults, re-read per part.
        # The current object is per file too: a part whose first cost line
        # precedes its own `ob=` would inherit whatever the last part left.
        function reset_file() {
            npos = 1
            iri = 1
            ob = ""
            declared = ""
            declared_here = 0
            pending = 0
            delete names
        }

        BEGIN { reset_file() }

        # A part boundary. The part just finished contributes its declared
        # total to the sum the aggregate is compared against.
        FNR == 1 && NR > 1 {
            declared_total += declared
            declared_files += declared_here
            reset_file()
        }
        FNR == 1 { seen_files++ }

        # Identical across every part of one case; the first is as good as any.
        /^cmd:/ { if (cmd == "") { cmd = substr($0, 5); sub(/^[ \t]+/, "", cmd) } next }
        /^positions:/ { npos = NF - 1; next }
        /^events:/ {
            iri = 0
            for (i = 2; i <= NF; i++) if ($i == "Ir") iri = i - 1
            if (iri == 0) no_ir = 1
            next
        }
        # What this part says it collected. `totals:` is the same quantity
        # under another name; a part carrying both carries them equal, so the
        # last one read is the part total, not their sum.
        /^(summary|totals):/ { declared = $(1 + iri); declared_here = 1; next }

        /^ob=/  { ob = deref("ob", substr($0, 4)); seen[ob] = 1; next }
        /^cob=/ { seen[deref("ob", substr($0, 5))] = 1; next }
        /^(fl|fi|fe)=/  { deref("fl", substr($0, 4)); next }
        /^(cfi|cfl)=/   { deref("fl", substr($0, 5)); next }
        /^fn=/  { deref("fn", substr($0, 4)); next }
        /^cfn=/ { deref("fn", substr($0, 5)); next }

        # A cost line after one of these belongs to the callee: `calls=` carries
        # the inclusive cost of a call, the jump forms a branch cost. Callgrind
        # excludes both from its totals; the END check holds this parser to that.
        /^(calls|jump|jcnd)=/ { pending = 1; next }

        # A cost line: position field(s) then one column per declared event,
        # with trailing zero columns omitted.
        /^[0-9*+-]/ {
            if (pending) { pending = 0; next }
            ir[ob] += $(npos + iri)
            total += $(npos + iri)
            next
        }
        { pending = 0 }

        END {
            # The last part never hit the boundary rule above.
            declared_total += declared
            declared_files += declared_here

            if (cmd == "") { print "ERROR no-cmd"; exit }
            # Any part missing the Ir column is enough. Left unnamed this
            # surfaces as a totals mismatch, from summing the position field.
            if (no_ir) { print "ERROR no-ir-column"; exit }
            # Zero across *every* part. One part at zero is ordinary: a
            # thread that never entered the collected region.
            if (total == 0) { print "ERROR no-cost"; exit }
            # Every part has to have declared a total. Counted against
            # `parts`, which the *shell* counted: an empty file has no records,
            # so awk never sees it, and a truncated part beside a healthy one
            # would slip through every check here.
            if (seen_files != parts) { print "ERROR unreadable-part"; exit }
            if (declared_files != parts) { print "ERROR partial-summary"; exit }
            if (declared_total != total) {
                printf "ERROR totals-mismatch %d %d\n", total, declared_total
                exit
            }
            # The binary under measurement is the object whose path the
            # command line starts with, compared against every object the
            # profile named, including one that carried no cost. Longest match
            # wins, so a prefix of another path cannot claim it. An ignore-list
            # of `libc.so`-shaped names would fail open on an unknown platform.
            binary = ""
            for (o in seen)
                if (index(cmd, o) == 1 && length(o) > length(binary)) binary = o
            if (binary == "") { print "ERROR no-binary"; exit }
            # Truncated rather than rounded: 9.999% reads as 999 and fails,
            # where rounding would let it through as 10%.
            printf "OK %d %.2f %d %d\n", \
                10000 * ir[binary] / total, 100 * ir[binary] / total, \
                ir[binary], total
        }
    ' "$@"
}

check_dir() {
    local dir=$1 shard=$2 case_dir case_id profile verdict hundredths pct app total
    local failed=0 checked=0 where status
    local -a parts
    [[ -d "$dir" ]] || fail "$dir does not exist; there are no profiles to check"
    # Joined here so one shard's message is greppable by the same pattern
    # whether or not the tier is fanned out.
    [[ -z "$shard" ]] || shard="$shard — "

    # One iteration per *case*, one directory per case: a threaded case's
    # parts are only a measurement together.
    while IFS= read -r case_dir; do
        # `spate-s3/descriptor_gungraun/descriptor/decode.full_splits`, relative
        # to the tree the caller named. A profile sitting directly in that tree
        # has no relative path to strip to, so it is named for its own directory.
        if [[ "$case_dir" == "$dir" ]]; then
            case_id=$(basename "$case_dir")
        else
            case_id=${case_dir#"$dir"/}
        fi
        parts=()
        while IFS= read -r profile; do
            parts+=("$profile")
        done < <(find "$case_dir" -maxdepth 1 -name 'callgrind.*.out' ! -name '*@*' | LC_ALL=C sort)
        [[ ${#parts[@]} -gt 0 ]] || continue
        if [[ ${#parts[@]} -eq 1 ]]; then
            where="${parts[0]}"
        else
            where="$case_dir (${#parts[@]} parts)"
        fi
        # An awk that died becomes a verdict rather than aborting the loop
        # under `set -e` with the case unnamed and the rest unchecked.
        #
        # Stdout only: anything the environment writes to stderr, such as a
        # locale warning, would land in front of the verdict and be parsed as
        # one, reporting a good profile as unreadable.
        verdict=$(read_case "${parts[@]}") || verdict="ERROR unreadable"
        read -r status hundredths pct app total <<<"$verdict" || true
        case "$status" in
        OK) ;;
        *)
            # On this branch `$hundredths` carries the reason and `$pct`
            # onwards any detail, following the ERROR line's shape.
            echo "::error::$shard$case_id: its callgrind profile could not be read ($hundredths $pct $app)."
            echo "  $where"
            echo "  The guard refuses to judge a profile it cannot account for; see scripts/gungraun-collected-region.sh."
            failed=1
            checked=$((checked + 1))
            continue
            ;;
        esac
        # The magnitude corroborator first: a case that collected almost nothing
        # has lost its region whatever the surviving instructions belong to.
        if [[ "$total" -lt "$MIN_COLLECTED_IR" ]]; then
            echo "::error::$shard$case_id: the collected region is $total Ir, below the $MIN_COLLECTED_IR floor;"
            echo "  a bench case cannot do meaningful work in that many instructions, so the region was lost"
            echo "  rather than measured: the same defect as a runtime-dominated region, wearing the other face."
            echo "  Profile: $where"
            echo "  Move the measured work into a named #[inline(never)] function the benchmark calls,"
            echo "  and see DEVELOPING.md."
            failed=1
            checked=$((checked + 1))
            continue
        fi
        # Integers only. The decimal beside it is for reading.
        if [[ "$hundredths" -ge $((MIN_APPLICATION_PCT * 100)) ]]; then
            echo "$shard$case_id: ${pct}% of $total Ir in the binary under measurement"
        else
            echo "::error::$shard$case_id: the collected region is ${pct}% application code ($app of $total Ir);"
            echo "  the rest is the C runtime, so this case is measuring the allocator rather than the code it names."
            echo "  Profile: $where"
            echo "  The usual cause is the measured work being written inline in the #[library_benchmark]"
            echo "  function, where the optimizer may reshape it out of the collected region. Move it into a"
            echo "  named #[inline(never)] function the benchmark calls. See DEVELOPING.md."
            failed=1
        fi
        checked=$((checked + 1))
        # One directory per case, deduplicated: a threaded case is judged
        # once, on the sum of its profiles.
        #
        # The head leg's profiles only, here and in the per-case glob above. A
        # saved baseline lands as `callgrind.<case>.out.<label>@<label>`, and
        # judging it fails a pull request for a bench its author did not write.
        # `@` cannot appear in a bench, group or case name.
    done < <(find "$dir" -name 'callgrind.*.out' ! -name '*@*' -exec dirname {} \; |
        LC_ALL=C sort -u)

    # Fails closed: a run that measured nothing and a run that measured well
    # are otherwise the same green job.
    if [[ "$checked" -eq 0 ]]; then
        echo "::error::${shard}no callgrind profile under $dir; the benches wrote no measurement to check."
        return 1
    fi
    [[ "$failed" -eq 0 ]] || return 1
    echo "gungraun-collected-region.sh: ${shard}$checked case(s) attribute at least ${MIN_APPLICATION_PCT}% of their collected instructions to the binary under measurement"
    return 0
}

# ---------------------------------------------------------------------------
# Self-test.
#
# The fixtures are real: callgrind profiles this repository's benches produced
# under valgrind on Linux, reduced to one cost line per function. Every self
# cost is preserved, so each object's share is the measured one to the
# instruction.
#
# The degenerate fixture is the s3 descriptor bench with its measured work
# written inline in the benchmark function, 99.53% glibc free path; the healthy
# one is the ClickHouse RowBinary encoder at 84.99%. The exact shares are
# asserted, so a parser that started counting call costs fails here.
#
# SPATE_COLLECTED_REGION_UNDER_TEST points the fixtures at another copy of this
# script.
# ---------------------------------------------------------------------------
self_test() {
    local tmp under_test rc out
    tmp=$(mktemp -d)
    # shellcheck disable=SC2064  # expand the path now, not at trap time
    trap "rm -rf '$tmp'" RETURN
    under_test="${SPATE_COLLECTED_REGION_UNDER_TEST:-$0}"

    # The real case path: the failure message quotes it.
    mkdir -p "$tmp/degenerate/spate-s3/descriptor_gungraun/descriptor/decode.full_splits"
    cat >"$tmp/degenerate/spate-s3/descriptor_gungraun/descriptor/decode.full_splits/callgrind.decode.full_splits.out" <<'PROFILE'
# callgrind format
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
PROFILE

    mkdir -p "$tmp/healthy/spate-clickhouse/encode_gungraun/encode/encode_rowbinary_events.rowbinary_events"
    cat >"$tmp/healthy/spate-clickhouse/encode_gungraun/encode/encode_rowbinary_events.rowbinary_events/callgrind.encode_rowbinary_events.rowbinary_events.out" <<'PROFILE'
# callgrind format
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
PROFILE

    # A profile whose totals do not add up. The parser is trustworthy only
    # while it agrees with callgrind's own arithmetic, so a disagreement is a
    # refusal rather than a share computed from whatever it managed to read.
    mkdir -p "$tmp/inconsistent/spate-core/chain_gungraun/chain/push_batch.owned"
    cat >"$tmp/inconsistent/spate-core/chain_gungraun/chain/push_batch.owned/callgrind.push_batch.owned.out" <<'PROFILE'
cmd:  /target/release/deps/chain_gungraun-a7b171c5ce77f5ac --gungraun-run 00000 00000 00001
positions: line
events: Ir
summary: 260758

ob=/target/release/deps/chain_gungraun-a7b171c5ce77f5ac
fn=chain_gungraun::chain::__gungraun_wrapper_mod::push_batch
48 151576
PROFILE

    # --- the degenerate case is rejected, and for the measured reason -------
    rc=0
    out=$("$under_test" --shard 'spate-s3 (default)' "$tmp/degenerate" 2>&1) || rc=$?
    [[ "$rc" -eq 1 ]] || fail "the degenerate fixture exited $rc, not 1: a bench measuring the allocator was accepted"
    grep -qF '0.47% application code' <<<"$out" || fail \
        "the degenerate fixture's share is not the measured 0.47%; the parser's arithmetic has moved:
$out"
    # The shard label and the case path both: the label says which job, the
    # path says which of that job's cases.
    grep -q 'spate-s3 (default) — spate-s3/descriptor_gungraun/descriptor/decode.full_splits' <<<"$out" || fail \
        "the failure does not name the shard and the case:
$out"
    grep -q 'inline(never)' <<<"$out" || fail \
        "the failure does not say what to do about it:
$out"

    # --- the allocation-heavy healthy case is accepted ----------------------
    rc=0
    out=$("$under_test" "$tmp/healthy" 2>&1) || rc=$?
    [[ "$rc" -eq 0 ]] || fail "the healthy fixture exited $rc, not 0: a legitimate bench was condemned:
$out"
    grep -qF '84.99% of 730429 Ir' <<<"$out" || fail \
        "the healthy fixture's share is not the measured 84.99%:
$out"

    # --- a profile the parser cannot account for is a refusal, not a share --
    rc=0
    out=$("$under_test" "$tmp/inconsistent" 2>&1) || rc=$?
    [[ "$rc" -eq 1 ]] || fail "the inconsistent fixture exited $rc, not 1"
    grep -q 'totals-mismatch' <<<"$out" || fail \
        "a profile disagreeing with its own summary was not refused:
$out"

    # --- an empty tree is a failure, not a vacuous pass ---------------------
    mkdir -p "$tmp/empty"
    rc=0
    "$under_test" "$tmp/empty" >/dev/null 2>&1 || rc=$?
    [[ "$rc" -eq 1 ]] || fail "a directory with no profiles exited $rc, not 1: this guard must fail closed"

    # --- a saved baseline is not this shard's business ----------------------
    #
    # The healthy tree is given a *degenerate* baseline file and must still pass.
    cp -R "$tmp/healthy" "$tmp/with-baseline"
    cp "$tmp/degenerate/spate-s3/descriptor_gungraun/descriptor/decode.full_splits/callgrind.decode.full_splits.out" \
        "$tmp/with-baseline/spate-clickhouse/encode_gungraun/encode/encode_rowbinary_events.rowbinary_events/callgrind.encode_rowbinary_events.rowbinary_events.out.base@base"
    rc=0
    out=$("$under_test" "$tmp/with-baseline" 2>&1) || rc=$?
    [[ "$rc" -eq 0 ]] || fail \
        "a saved baseline was judged alongside the head measurement:
$out"

    # --- a case split across threads is judged once, on the sum -------------
    #
    # Both parts are the real artifact, from the Confluent-framing case whose
    # setup starts a loopback stub on a thread: the first carries every
    # instruction, the second declares `summary: 0`. The verdict count matters
    # as much as its text, because judging per file produces two lines here.
    mkdir -p "$tmp/threaded/spate-avro/decode_gungraun/confluent/decode_confluent.poisoned_schema_id"
    cat >"$tmp/threaded/spate-avro/decode_gungraun/confluent/decode_confluent.poisoned_schema_id/callgrind.decode_confluent.poisoned_schema_id.t1.p1.out" <<'PROFILE'
# callgrind format
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
PROFILE
    # Verbatim, including the blank lines.
    cat >"$tmp/threaded/spate-avro/decode_gungraun/confluent/decode_confluent.poisoned_schema_id/callgrind.decode_confluent.poisoned_schema_id.t2.p1.out" <<'PROFILE'
# callgrind format
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
PROFILE
    rc=0
    out=$("$under_test" "$tmp/threaded" 2>&1) || rc=$?
    [[ "$rc" -eq 0 ]] || fail \
        "a case split across threads was condemned; its zero-cost part is a thread that never
    entered the collected region, not a region that collected nothing:
$out"
    # The summed total, and also the number gungraun reports for the case.
    grep -qF '60.55% of 2180066 Ir' <<<"$out" || fail \
        "the threaded case's parts were not summed into one measurement:
$out"
    # One case, one verdict. Counted rather than grepped: judging per file
    # produces two lines here.
    [[ "$(grep -cF 'decode_confluent.poisoned_schema_id' <<<"$out")" -eq 1 ]] || fail \
        "a case split across threads produced more than one verdict:
$out"

    # --- one part cannot inherit the object another part left current -------
    #
    # A part whose cost lines precede its own `ob=` would be charged to whatever
    # the previous part left current, inflating the share this guard floors.
    # Asserted by the share, not the verdict: both readings clear the threshold.
    mkdir -p "$tmp/orphan/spate-clickhouse/encode_gungraun/encode/encode.rowbinary"
    cp "$tmp/healthy/spate-clickhouse/encode_gungraun/encode/encode_rowbinary_events.rowbinary_events/callgrind.encode_rowbinary_events.rowbinary_events.out" \
        "$tmp/orphan/spate-clickhouse/encode_gungraun/encode/encode.rowbinary/callgrind.encode.rowbinary.t1.p1.out"
    cat >"$tmp/orphan/spate-clickhouse/encode_gungraun/encode/encode.rowbinary/callgrind.encode.rowbinary.t2.p1.out" <<'PROFILE'
cmd:  /target/release/deps/encode_gungraun-435f3f43eb3bf4fe --gungraun-run 00000 00003 00000
positions: line
events: Ir
summary: 730429

fn=an_orphan_frame_before_any_ob
0 730429
PROFILE
    rc=0
    out=$("$under_test" "$tmp/orphan" 2>&1) || rc=$?
    [[ "$rc" -eq 0 ]] || fail "the orphan-cost fixture exited $rc, not 0:
$out"
    # 620806 of 1460858. Reading the orphaned cost as the binary would report
    # 92.50% instead, and the totals check would not notice.
    grep -qF '42.50% of 1460858 Ir' <<<"$out" || fail \
        "cost with no object of its own was attributed to another part's object:
$out"

    # --- a part that produced no records is refused, not skipped ------------
    #
    # A truncated or zero-byte part never reaches awk's per-file rule. Counted
    # only by awk, a healthy part beside an empty one passes at the healthy
    # part's share while the caller reports two parts.
    cp -R "$tmp/threaded" "$tmp/truncated"
    : >"$tmp/truncated/spate-avro/decode_gungraun/confluent/decode_confluent.poisoned_schema_id/callgrind.decode_confluent.poisoned_schema_id.t2.p1.out"
    rc=0
    out=$("$under_test" "$tmp/truncated" 2>&1) || rc=$?
    [[ "$rc" -eq 1 ]] || fail \
        "a case with an empty part exited $rc, not 1: a part the parser never saw was counted as
    agreeing with the sum:
$out"
    grep -qF 'unreadable-part' <<<"$out" || fail \
        "an empty part was not named as the reason:
$out"

    # --- a stray diagnostic on stderr cannot make the guard misjudge --------
    #
    # A wrapper `awk` earlier on PATH that writes to stderr and then does the
    # real work. Folded into the stream the verdict is parsed from, that line
    # arrives first and a good profile is reported as unreadable. A locale
    # warning once condemned a healthy bench that way.
    real_awk=$(command -v awk)
    mkdir -p "$tmp/bin"
    {
        printf '#!/bin/sh\n'
        printf 'echo "awk: warning: a diagnostic that is none of the guard'"'"'s business" >&2\n'
        printf 'exec %s "$@"\n' "$real_awk"
    } >"$tmp/bin/awk"
    chmod +x "$tmp/bin/awk"
    rc=0
    out=$(PATH="$tmp/bin:$PATH" "$under_test" "$tmp/healthy" 2>/dev/null) || rc=$?
    [[ "$rc" -eq 0 ]] || fail \
        "a diagnostic on stderr made the guard misjudge a healthy profile:
$out"
    grep -qF '84.99% of 730429 Ir' <<<"$out" || fail \
        "a diagnostic on stderr reached the parsed verdict:
$out"

    # --- a case that collected almost nothing is rejected --------------------
    #
    # A handful of instructions belonging to the toggled wrapper is application
    # code, and passes the composition rule.
    mkdir -p "$tmp/collapsed/spate-kafka/encode_gungraun/encode/encode.bytes_keyless"
    cat >"$tmp/collapsed/spate-kafka/encode_gungraun/encode/encode.bytes_keyless/callgrind.encode.bytes_keyless.out" <<'PROFILE'
cmd:  /target/release/deps/encode_gungraun-0000000000000000 --gungraun-run 00000 00000 00000
positions: line
events: Ir
summary: 22

ob=/target/release/deps/encode_gungraun-0000000000000000
fn=encode_gungraun::encode::__gungraun_wrapper_mod::encode
48 22
PROFILE
    rc=0
    out=$("$under_test" "$tmp/collapsed" 2>&1) || rc=$?
    [[ "$rc" -eq 1 ]] || fail \
        "a case collecting 22 Ir exited $rc, not 1. A collapsed region is 100% application code
    and passes the composition rule, so the magnitude floor is what catches it:
$out"
    grep -qF 'below the 1000 floor' <<<"$out" || fail \
        "a collapsed region was rejected for the wrong reason:
$out"

    # --- the verdict does not depend on how a number is formatted -----------
    #
    # `%f` honours LC_NUMERIC, so under a comma-decimal locale the share
    # renders as `84,99` and a gate parsing that string reads a different
    # number. The gate reads the integer; this asserts the rendering. Naming a
    # locale rather than selecting an installed one makes the check vacuous:
    # hosted runners carry only C and C.UTF-8.
    #
    # `locale -a` is read once into a variable rather than piped into each
    # `grep -q`. Under `pipefail` that pipeline reports failure even when the
    # locale is present: `grep -q` exits at the first match, `locale -a` takes
    # SIGPIPE, and 141 becomes the pipeline's status, so every locale reads as
    # missing and the check silently never runs.
    installed_locales=$(locale -a 2>/dev/null || true)
    comma_locale=""
    for loc in de_DE.UTF-8 de_DE.utf8 fr_FR.UTF-8 fr_FR.utf8; do
        if grep -qxF "$loc" <<<"$installed_locales"; then
            comma_locale=$loc
            break
        fi
    done
    if [[ -n "$comma_locale" ]]; then
        rc=0
        out=$(LC_ALL="$comma_locale" LC_NUMERIC="$comma_locale" "$under_test" "$tmp/healthy" 2>&1) || rc=$?
        [[ "$rc" -eq 0 ]] || fail \
            "under $comma_locale a healthy profile was condemned:
$out"
        # `-F`: `.` is a regex wildcard, so the unanchored pattern `84.99`
        # matches the very `84,99` this assertion exists to reject.
        grep -qF '84.99% of 730429 Ir' <<<"$out" || fail \
            "under $comma_locale the share is not rendered with a decimal point:
$out"
    fi

    # --- a profile with no Ir column says so ---------------------------------
    #
    # Unnamed, it surfaces as a totals mismatch from summing the position field.
    mkdir -p "$tmp/no-ir/spate-core/chain_gungraun/chain/push_batch.owned"
    cat >"$tmp/no-ir/spate-core/chain_gungraun/chain/push_batch.owned/callgrind.push_batch.owned.out" <<'PROFILE'
cmd:  /target/release/deps/chain_gungraun-a7b171c5ce77f5ac --gungraun-run 00000 00000 00001
positions: line
events: Dr Dw
summary: 100 50

ob=/target/release/deps/chain_gungraun-a7b171c5ce77f5ac
fn=chain_gungraun::chain::__gungraun_wrapper_mod::push_batch
48 100 50
PROFILE
    rc=0
    out=$("$under_test" "$tmp/no-ir" 2>&1) || rc=$?
    [[ "$rc" -eq 1 ]] || fail "a profile with no Ir column exited $rc, not 1"
    grep -q 'no-ir-column' <<<"$out" || fail \
        "a profile with no Ir column was not named as such:
$out"

    # --- nothing but this rule rejects the fixture --------------------------
    #
    # The degenerate fixture is run past a command that accepts whatever it is
    # given, and required to pass.
    printf '#!/bin/sh\nexit 0\n' >"$tmp/no-guard"
    chmod +x "$tmp/no-guard"
    rc=0
    "$tmp/no-guard" --shard x "$tmp/degenerate" >/dev/null 2>&1 || rc=$?
    [[ "$rc" -eq 0 ]] || fail \
        "the degenerate fixture is rejected by a script that implements no rule at all,
    so it is not evidence that this one works"

    if [[ -z "$comma_locale" ]]; then
        echo "gungraun-collected-region.sh --self-test: SKIPPED the comma-decimal locale case.
    No such locale is installed here, so LC_NUMERIC cannot be made to bite. The verdict is an
    integer for exactly this reason; the rendering check runs where a locale exists."
    fi

    echo "gungraun-collected-region.sh --self-test: the measured degenerate profile is rejected at
    0.47% and a collapsed 22 Ir region at the magnitude floor; the allocation-heavy healthy one is
    accepted at 84.99%, with a diagnostic on stderr${comma_locale:+, and under $comma_locale}; and a case
    split across threads is judged once, on the sum of its parts, at 60.55%. A part that produced
    no records and a part carrying cost with no object of its own are both refused rather than
    absorbed. A saved baseline is not judged, a profile disagreeing with its own totals and one
    with no Ir column are refused by name, an empty tree fails closed, and nothing but this rule
    rejects the fixture."
}

shard=""
if [[ "${1:-}" == "--shard" ]]; then
    [[ $# -ge 2 ]] || fail "--shard needs a value"
    shard="$2"
    shift 2
fi

# Where the benches just wrote, when the caller does not say. A relative
# `CARGO_TARGET_DIR` resolves from the repository root.
case "${1:-}" in
--self-test) self_test ;;
--*) fail "unknown argument '$1' (expected --shard, --self-test, or a directory)" ;;
*) check_dir "${1:-${CARGO_TARGET_DIR:-target}/gungraun}" "$shard" ;;
esac
