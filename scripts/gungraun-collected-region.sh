#!/usr/bin/env bash
#
# Reject a bench case whose collected region measured the runtime instead of
# the code under test.
#
#   scripts/gungraun-collected-region.sh [--shard LABEL] [DIR]
#   scripts/gungraun-collected-region.sh --self-test
#
# DIR defaults to `target/gungraun`, which is where the counter tier leaves a
# callgrind profile per bench case. `--shard LABEL` prefixes every line with
# the (package, arm) the caller is measuring, because the tier fans out over a
# matrix and a bare complaint about `decode.full_splits` would not say which
# of six jobs produced it.
#
# ## What goes wrong, and why nothing else catches it
#
# A gungraun bench can report a plausible number while measuring nothing. The
# collected region is bounded by a callgrind toggle on the module the
# `#[library_benchmark]` macro wraps the function in, and a toggle *flips*
# collection rather than forcing it on — so work the optimiser leaves in an
# unstable shape inside that module can end up outside the region entirely,
# and whatever else happens to run while collection is on is counted in its
# place. What that usually is, is glibc's free path tearing down a corpus the
# fixture built.
#
# Every existing signal stays green through this. The bench builds, runs,
# exits 0, and reports a number in the millions; the summary schema is
# satisfied; the report renders a row. The only tell is the profile itself,
# and the shape it takes is unmistakable — a real occurrence on this
# repository's own s3 descriptor bench:
#
#     589,264 (68.60%)  malloc_consolidate'2      [libc]
#     268,865 (31.30%)  unlink_chunk.constprop.0  [libc]
#     858,925 (100.0%)  PROGRAM TOTALS
#
# No application frame at all, and the same total whether the corpus held 400
# documents or 6,400. Reproduced natively while writing this guard, the same
# shape counted 942,805 Ir of which 99.53% was glibc — `malloc_consolidate'2`
# at 589,270, within six instructions of the number above.
#
# ## The rule
#
# **At least MIN_APPLICATION_PCT of a case's collected instructions must be
# attributed to the binary under measurement, and there must be at least
# MIN_COLLECTED_IR of them.**
#
# Composition is the primary signal and magnitude corroborates it, because a
# lost region has two faces. The one this guard was written for leaves the C
# runtime behind and reports a number nobody would question. The other leaves
# almost nothing — a few instructions of the toggled wrapper, which is
# application code, so composition reads 100% and has no objection. Each rule
# is blind to the face the other catches, and a case is judged on both.
#
# Every case is judged on its own. A bench losing its region does not lose it
# in every case: on a sibling bench four of five collapsed together and the
# fifth did not, so a rule that concluded anything from the bench as a whole
# would have called that bench healthy or condemned its one good case.
#
# The classification axis is the ELF object each instruction was executed in,
# which callgrind records itself on the `ob=` lines — not a pattern over
# function names. Every crate in this workspace, and every Rust dependency it
# pulls, is compiled *into* the bench executable, so "the binary under
# measurement" is the whole application: the crate, the framework, serde,
# the codecs. What is left outside it is the C runtime the process links
# dynamically — glibc's allocator, `memcpy`, the dynamic loader. A bench that
# spends nearly all of its collected instructions there is not measuring the
# code it names, whatever the number says.
#
# The binary is identified from the profile's own `cmd:` header rather than by
# recognising library paths: the object whose path the command line begins
# with is by construction the program valgrind ran, where an ignore-list of
# `libc.so`-shaped names would have to be extended for every platform and
# would fail open on the one it did not know.
#
# ## The threshold, and why it is where it is
#
# Measured over every case in the tree — 40 of them across the six shards, on
# both architectures this repository is measured on:
#
#                        runner (x86_64)   development (arm64)
#   lowest healthy case    33.35%            28.67%   spate-avro, decode_value
#   highest               100.00%           100.00%   spate-clickhouse route_*
#   degenerate case             —             0.47%   the reproduction above
#
# So the observed distribution has a gap between 0.47% and the high twenties
# with nothing in it, and any threshold inside that gap separates the two
# populations. It is set at the bottom of the gap rather than the middle
# because the two errors are not symmetric: failing to catch a degenerate bench
# costs a wrong baseline that every later comparison inherits, while condemning
# a healthy one blocks a pull request that has done nothing wrong. 10% is 2.8x
# below the lowest real case on the architecture where it reads lowest — room
# for a bench more allocation-heavy than anything here — and 21x above the
# degenerate one.
#
# The share is a composition ratio rather than a count, which is why the two
# architectures agree to within a few points where their absolute instruction
# counts do not agree at all.
#
# The low cases are not accidental and are the reason the threshold is not set
# higher: the avro `Value` decoder allocates a tree of `String`s and `Vec`s per
# record, so most of its instructions genuinely are inside glibc's allocator.
# That is a legitimate profile, and a guard that condemned it would be
# describing allocation as a defect.
#
# A future bench that genuinely spends more than nine tenths of its
# instructions inside the C runtime — one measuring `memcpy` throughput, say —
# would fail this. That is the right outcome to argue about in review rather
# than to pre-empt with a looser number: such a bench is measuring glibc, and
# whether that is what it means to measure is a question worth asking out loud.
#
# ## Whether the arithmetic can be trusted
#
# The parser is checked against callgrind's own arithmetic on every file it
# reads: the self costs it sums must equal the `summary:` line the profile
# declares. That is what makes the share safe to act on. A cost line following
# `calls=` is the *inclusive* cost of a call and is excluded — counting it
# would inflate the runtime's share, since the outermost call chains in these
# profiles run through glibc's startup — and a parser that stopped excluding
# them would fail the totals check on the first real profile rather than
# quietly condemn a healthy bench.
set -euo pipefail

# The share of a case's collected instructions that must land in the binary
# under measurement. See "The threshold" above; it is a floor on composition,
# not on a count, so it does not move when a bench gets faster or slower.
MIN_APPLICATION_PCT=10

# The second, cruder signal, and deliberately the secondary one. Losing the
# collected region does not always leave the C runtime behind: it can leave
# almost nothing at all, a handful of instructions belonging to the toggled
# wrapper itself — which is *application* code, so the composition rule above
# has nothing to object to. A sibling bench lost this way went from 3,500,338
# Ir to 22 and still ran, still reported, still rendered a row.
#
# The magnitude cannot be the primary signal, because the failure worth
# catching is the plausible one: the case this guard was written for reported
# 858,925 Ir, and no size rule distinguishes that from real work. It is a
# corroborator for the other end of the same failure. The floor is 1,000
# against a smallest real case of 6,656 — a factor of six of room below
# anything here, and a factor of forty-five above a collapse.
MIN_COLLECTED_IR=1000

cd "$(dirname "$0")/.."

fail() {
    echo "gungraun-collected-region.sh: $1" >&2
    exit 1
}

# One case's verdict, on stdout, as a single line:
#
#   OK <hundredths of a percent> <percent, for reading> <application Ir> <total Ir>
#   ERROR <reason> [detail...]
#
# Parsed rather than acted on here so the caller owns every message: the
# awk is the measurement and the shell is the report.
#
# The verdict is carried twice on purpose. The gate reads the integer, so no
# formatting decision can reach it; the decimal is for the human line and
# nothing branches on it. `LC_ALL=C` because `%f` honours `LC_NUMERIC` — under
# a comma-decimal locale awk renders 84.99 as `84,99`, which the shell then
# reads as a different number entirely. Left unset, every healthy bench on a
# European developer's machine is condemned; the assertion at the end of
# `--self-test` is what keeps that fixed.
read_profile() {
    LC_ALL=C awk '
        # Name compression: a position line may introduce an id
        # (`ob=(1) /lib/libc.so.6`) and later refer to it (`ob=(1)`). Ids are
        # per name kind, and `cob=`/`cfn=`/`cfi=` share the namespace of
        # `ob=`/`fn=`/`fl=` respectively — so a compressed name introduced on
        # the called side has to be recorded even though it does not move the
        # current position.
        # `endpos` rather than the obvious `close`, which is an awk builtin
        # and cannot be a parameter name on every awk this runs under.
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

        # Defaults for a profile that declares neither header. `positions:` is
        # one field (`line`) and `Ir` is the first event in every callgrind
        # profile; both are re-read below if the file says otherwise.
        BEGIN { npos = 1; iri = 1 }

        /^cmd:/ { cmd = substr($0, 5); sub(/^[ \t]+/, "", cmd); next }
        /^positions:/ { npos = NF - 1; next }
        /^events:/ {
            iri = 0
            for (i = 2; i <= NF; i++) if ($i == "Ir") iri = i - 1
            next
        }
        # What the profile says it collected, for the totals check. `totals:`
        # is the same quantity under the name callgrind uses when the run was
        # dumped rather than ended.
        /^(summary|totals):/ { declared = $(1 + iri); next }

        /^ob=/  { ob = deref("ob", substr($0, 4)); seen[ob] = 1; next }
        /^cob=/ { seen[deref("ob", substr($0, 5))] = 1; next }
        /^(fl|fi|fe)=/  { deref("fl", substr($0, 4)); next }
        /^(cfi|cfl)=/   { deref("fl", substr($0, 5)); next }
        /^fn=/  { deref("fn", substr($0, 4)); next }
        /^cfn=/ { deref("fn", substr($0, 5)); next }

        # A cost line after one of these belongs to the callee, not to the
        # frame being read: `calls=` carries the inclusive cost of a call, and
        # the jump forms carry a branch cost. Callgrind excludes both from its
        # own totals, which the check in END holds this parser to.
        /^(calls|jump|jcnd)=/ { pending = 1; next }

        # A cost line: position field(s) then one column per declared event,
        # with trailing zero columns omitted. An absent column reads as the
        # empty string, which awk evaluates as 0 — which is what an omitted
        # column means.
        /^[0-9*+-]/ {
            if (pending) { pending = 0; next }
            ir[ob] += $(npos + iri)
            total += $(npos + iri)
            next
        }
        { pending = 0 }

        END {
            if (cmd == "") { print "ERROR no-cmd"; exit }
            # Named rather than left to surface as a totals mismatch, which is
            # what a missing Ir column produces: the parser would be summing
            # the position field, and the resulting complaint would send a
            # reader looking at the wrong thing entirely.
            if (iri == 0) { print "ERROR no-ir-column"; exit }
            if (total == 0) { print "ERROR no-cost"; exit }
            if (declared != "" && declared != total) {
                printf "ERROR totals-mismatch %d %d\n", total, declared
                exit
            }
            # The binary under measurement is the object whose path the
            # command line starts with. Compared against every object the
            # profile named — including one that carried no cost of its own,
            # which is exactly the case this guard exists to catch — and the
            # longest match wins, so a path that is a prefix of another cannot
            # claim it.
            binary = ""
            for (o in seen)
                if (index(cmd, o) == 1 && length(o) > length(binary)) binary = o
            if (binary == "") { print "ERROR no-binary"; exit }
            # The integer is truncated rather than rounded, which is the
            # conservative direction for a floor: a share of 9.999% reads as
            # 999 and fails, where rounding would have let it through as 10%.
            printf "OK %d %.2f %d %d\n", \
                10000 * ir[binary] / total, 100 * ir[binary] / total, \
                ir[binary], total
        }
    ' "$1"
}

check_dir() {
    local dir=$1 shard=$2 profile case_id verdict hundredths pct app total
    local failed=0 checked=0
    [[ -d "$dir" ]] || fail "$dir does not exist; there are no profiles to check"
    # Joined here rather than by the caller so a label and its absence render
    # the same way everywhere: one shard's message must be greppable by the
    # same pattern whether or not the tier is fanned out.
    [[ -z "$shard" ]] || shard="$shard — "

    while IFS= read -r profile; do
        # `spate-s3/descriptor_gungraun/descriptor/decode.full_splits` — the
        # directory gungraun writes one case into, which is the identity a
        # reader needs to find the bench again.
        case_id=${profile#"$dir"/}
        case_id=${case_id%/*}
        # An awk that died — an unreadable file, a build without a working
        # awk — is turned into a verdict rather than left to `set -e`, which
        # would abort the loop with the profile unnamed and the remaining
        # cases unchecked.
        #
        # Stdout only, deliberately. Folding stderr in here looks like it
        # would improve the diagnostic and instead destroys it: anything the
        # environment writes to stderr — a shell complaining about an
        # uninstalled locale, an awk deprecation notice — lands in front of
        # the verdict and is parsed as one, so a perfectly good profile is
        # reported as unreadable. The real diagnostic is already in the job
        # log, where stderr goes.
        verdict=$(read_profile "$profile") || verdict="ERROR unreadable"
        read -r status hundredths pct app total <<<"$verdict" || true
        case "$status" in
        OK) ;;
        *)
            # `$hundredths` carries the reason on this branch and `$pct`
            # onwards any detail, which is what the ERROR line's shape puts
            # in those positions.
            echo "::error::$shard$case_id: its callgrind profile could not be read ($hundredths $pct $app)."
            echo "  $profile"
            echo "  The guard refuses to judge a profile it cannot account for; see scripts/gungraun-collected-region.sh."
            failed=1
            checked=$((checked + 1))
            continue
            ;;
        esac
        # The magnitude corroborator, checked first because it is the cruder
        # question: a case that collected almost nothing has lost its region
        # whatever the surviving instructions belong to, and reporting its
        # composition would be answering the wrong one.
        if [[ "$total" -lt "$MIN_COLLECTED_IR" ]]; then
            echo "::error::$shard$case_id: the collected region is $total Ir, below the $MIN_COLLECTED_IR floor;"
            echo "  a bench case cannot do meaningful work in that many instructions, so the region was lost"
            echo "  rather than measured — the same defect as a runtime-dominated region, wearing the other face."
            echo "  Profile: $profile"
            echo "  Move the measured work into a named #[inline(never)] function the benchmark calls,"
            echo "  and see CONTRIBUTING.md."
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
            echo "  Profile: $profile"
            echo "  The usual cause is the measured work being written inline in the #[library_benchmark]"
            echo "  function, where the optimiser may reshape it out of the collected region. Move it into a"
            echo "  named #[inline(never)] function the benchmark calls — see CONTRIBUTING.md."
            failed=1
        fi
        checked=$((checked + 1))
        # The head leg's profiles only. A saved baseline lands beside them as
        # `callgrind.<case>.out.<label>@<label>`, which today's pattern already
        # misses because it does not end in `.out` — but only by accident, and
        # the accident is worth not relying on: judging the merge base would
        # fail a pull request for a degenerate bench in code its author did not
        # write, which is the one thing every other part of this tier is
        # careful not to do. `@` is gungraun's baseline separator and cannot
        # appear in a bench, group or case name, all of which are Rust
        # identifiers.
    done < <(find "$dir" -name 'callgrind.*.out' ! -name '*@*' | LC_ALL=C sort)

    # Fails closed, for the same reason the rest of this tier does: a run that
    # measured nothing and a run that measured well are otherwise the same
    # green job, and this guard is the last place that difference is visible.
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
# The fixtures are real. Both are callgrind profiles this repository's own
# benches produced under valgrind on Linux, reduced to one cost line per
# function — which drops the call chains and the source-position detail while
# preserving every self cost, so each object's total, and therefore the share
# the guard computes, is the measured one to the instruction.
#
# The degenerate fixture is the s3 descriptor bench with its measured work
# written inline in the benchmark function instead of behind a named
# `#[inline(never)]` callee: the shape that reached review on that bench, and
# the reason this guard exists. Its profile is 99.53% glibc free path.
#
# The healthy fixture is the ClickHouse RowBinary encoder, which allocates on
# every row and still attributes 84.99% of its instructions to itself.
#
# The pair is what makes either assertion worth anything: a guard that
# rejected every profile would fail the healthy case, and one that accepted
# every profile would fail the degenerate case. The exact shares are asserted
# rather than just the verdicts, so a parser that started counting call costs
# — which would inflate the runtime's share on every profile — fails here
# rather than in six months on somebody's pull request.
#
# SPATE_COLLECTED_REGION_UNDER_TEST points the fixtures at another copy of
# this script, which is how a reviewer confirms a fixture is load-bearing by
# running it against a revision that should not pass it.
# ---------------------------------------------------------------------------
self_test() {
    local tmp under_test rc out
    tmp=$(mktemp -d)
    # shellcheck disable=SC2064  # $tmp is expanded now on purpose
    trap "rm -rf '$tmp'" RETURN
    under_test="${SPATE_COLLECTED_REGION_UNDER_TEST:-$0}"

    # `spate-s3/descriptor_gungraun/descriptor/decode.full_splits`, the real
    # path, because the failure message quotes it and a reader has to be able
    # to find the bench from it.
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

    # A profile whose totals do not add up. The parser is only trustworthy
    # while it agrees with callgrind's own arithmetic, so disagreeing has to
    # be a refusal rather than a share computed from whatever it managed to
    # read.
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
    [[ "$rc" -eq 1 ]] || fail "the degenerate fixture exited $rc, not 1 — a bench measuring the allocator was accepted"
    grep -qF '0.47% application code' <<<"$out" || fail \
        "the degenerate fixture's share is not the measured 0.47%; the parser's arithmetic has moved:
$out"
    # The shard label and the case path both, because the tier fans out over
    # six jobs and either half alone leaves a reader hunting: the label says
    # which job, the path says which of that job's cases and where its
    # profile is.
    grep -q 'spate-s3 (default) — spate-s3/descriptor_gungraun/descriptor/decode.full_splits' <<<"$out" || fail \
        "the failure does not name the shard and the case:
$out"
    grep -q 'inline(never)' <<<"$out" || fail \
        "the failure does not say what to do about it:
$out"

    # --- the allocation-heavy healthy case is accepted ----------------------
    rc=0
    out=$("$under_test" "$tmp/healthy" 2>&1) || rc=$?
    [[ "$rc" -eq 0 ]] || fail "the healthy fixture exited $rc, not 0 — a legitimate bench was condemned:
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
    [[ "$rc" -eq 1 ]] || fail "a directory with no profiles exited $rc, not 1 — this guard must fail closed"

    # --- a saved baseline is not this shard's business ----------------------
    #
    # The merge-base leg leaves its measurement beside the head leg's, under a
    # name gungraun suffixes with the baseline label. Judging it would fail a
    # pull request for a bench its author did not write — so the healthy tree
    # is given a *degenerate* baseline file and must still pass.
    cp -R "$tmp/healthy" "$tmp/with-baseline"
    cp "$tmp/degenerate/spate-s3/descriptor_gungraun/descriptor/decode.full_splits/callgrind.decode.full_splits.out" \
        "$tmp/with-baseline/spate-clickhouse/encode_gungraun/encode/encode_rowbinary_events.rowbinary_events/callgrind.encode_rowbinary_events.rowbinary_events.out.base@base"
    rc=0
    out=$("$under_test" "$tmp/with-baseline" 2>&1) || rc=$?
    [[ "$rc" -eq 0 ]] || fail \
        "a saved baseline was judged alongside the head measurement:
$out"

    # --- a stray diagnostic on stderr cannot make the guard misjudge --------
    #
    # Constructed rather than waited for, and constructed at the exact seam
    # where it bit: a wrapper `awk` earlier on PATH that writes a line to
    # stderr and then does the real work. If the measurement's stderr is
    # folded into the stream the verdict is parsed from, that line arrives in
    # front of the verdict and a perfectly good profile is reported as
    # unreadable — which is how a shell warning about an uninstalled locale
    # once condemned a healthy bench. Nothing here depends on the
    # environment, so it holds on every host.
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
    # The other face of a lost region: not the runtime left behind, but almost
    # nothing left at all — a handful of instructions belonging to the toggled
    # wrapper, which is application code and so passes the composition rule.
    # A sibling bench lost this way reported 22 Ir and rendered a row.
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
        "a case collecting 22 Ir exited $rc, not 1 — a collapsed region is 100% application code
    and passes the composition rule, which is why the magnitude floor exists:
$out"
    grep -qF 'below the 1000 floor' <<<"$out" || fail \
        "a collapsed region was rejected for the wrong reason:
$out"

    # --- the verdict does not depend on how a number is formatted -----------
    #
    # `%f` honours LC_NUMERIC, so under a comma-decimal locale the share
    # renders as `84,99` — and a gate that parsed that string read a different
    # number and condemned every healthy bench. The gate reads the integer
    # instead, which is what makes it safe on any machine; this asserts the
    # rendering too, under a comma-decimal locale where the host has one.
    #
    # Selected from what is installed rather than named. Naming one that is
    # absent does not make the check strict, it makes it noisy and vacuous:
    # the shell warns on stderr, the locale does not take effect, and the
    # assertion passes for the wrong reason. Hosted runners carry only C and
    # C.UTF-8, so this is a developer-machine check by nature.
    # `locale -a` is read once into a variable rather than piped into each
    # `grep -q`. Under `pipefail` that pipeline reports failure even when the
    # locale is present: `grep -q` exits at the first match, `locale -a` takes
    # SIGPIPE writing to the closed pipe, and 141 becomes the pipeline's
    # status — so every locale reads as missing and the check silently never
    # runs. It did exactly that here before this line changed.
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
        # `-F`, and it matters here more than anywhere: `.` is a regex
        # wildcard, so the unanchored pattern `84.99` matches the very
        # `84,99` this assertion exists to reject, and the check would pass
        # on the defect.
        grep -qF '84.99% of 730429 Ir' <<<"$out" || fail \
            "under $comma_locale the share is not rendered with a decimal point:
$out"
    fi

    # --- a profile with no Ir column says so ---------------------------------
    #
    # Left unnamed this surfaces as a totals mismatch, because the parser is
    # then summing the position field — a complaint that sends a reader to the
    # wrong question.
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

    # --- the fixtures are load-bearing rather than decorative ---------------
    #
    # Every assertion above is about what *this* script does with the
    # fixtures, which says nothing about whether the fixtures could catch
    # anything. So the degenerate one is run past a stand-in for the state
    # this repository was in before the guard existed — a command that
    # accepts whatever it is given — and required to pass. A fixture that
    # any script rejects would be proving something about itself rather
    # than about the rule.
    printf '#!/bin/sh\nexit 0\n' >"$tmp/no-guard"
    chmod +x "$tmp/no-guard"
    rc=0
    "$tmp/no-guard" --shard x "$tmp/degenerate" >/dev/null 2>&1 || rc=$?
    [[ "$rc" -eq 0 ]] || fail \
        "the degenerate fixture is rejected by a script that implements no rule at all,
    so it is not evidence that this one works"

    # Said out loud rather than passed over. A check whose premise the host
    # cannot meet has not run, and a reader of a green log is entitled to know
    # which of these were actually exercised.
    if [[ -z "$comma_locale" ]]; then
        echo "gungraun-collected-region.sh --self-test: SKIPPED the comma-decimal locale case —
    no such locale is installed here, so LC_NUMERIC cannot be made to bite. The verdict is an
    integer for exactly this reason; the rendering check runs where a locale exists."
    fi

    echo "gungraun-collected-region.sh --self-test: the measured degenerate profile is rejected at
    0.47%, a collapsed 22 Ir region at the magnitude floor, and the allocation-heavy healthy one is
    accepted at 84.99% — with a diagnostic on stderr${comma_locale:+, and under $comma_locale}. A saved
    baseline is not judged, a profile disagreeing with its own totals and one with no Ir column are
    refused by name, an empty tree fails closed, and nothing but this rule rejects the fixture."
}

shard=""
if [[ "${1:-}" == "--shard" ]]; then
    [[ $# -ge 2 ]] || fail "--shard needs a value"
    shard="$2"
    shift 2
fi

# Where the benches just wrote, when the caller does not say. `CARGO_TARGET_DIR`
# is honoured because cargo honours it: a machine that redirects its build
# output would otherwise have this guard looking at a directory cargo never
# writes, and the fail-closed answer would be a failure on every run rather
# than a verdict. A relative value resolves from the repository root, which is
# where cargo resolves it from for every invocation in the Makefile.
case "${1:-}" in
--self-test) self_test ;;
--*) fail "unknown argument '$1' (expected --shard, --self-test, or a directory)" ;;
*) check_dir "${1:-${CARGO_TARGET_DIR:-target}/gungraun}" "$shard" ;;
esac
