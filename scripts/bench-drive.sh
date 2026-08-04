#!/usr/bin/env bash
#
# Run the wall-clock rigs against two builds and emit one JSONL leg per build,
# for `bench-compare.sh` to pair.
#
#   bench-drive.sh [--base <ref>] [--reps N] [--out DIR] [rig ...]
#   bench-drive.sh --self-test
#
# With no rig named it drives the runnable set, read from
# `ci-changes.sh --wallclock-rigs-all` rather than listed here — the same seam
# the weekly smoke job reads, so the two cannot name different sets.
#
# ## Why a driver rather than "run it twice"
#
# Two builds measured one after the other are not comparable on a machine that
# is doing anything else, and every machine is. A sequential A-then-B run in
# this repository once reported a 30% win that was the machine warming up.
# Three properties are what make the difference a difference:
#
#   - **Both builds are compiled before either runs.** A build between arms puts
#     minutes of full-machine load in the middle of the measurement.
#   - **Arms alternate.** Base, head, base, head — so drift over the run splits
#     evenly between the legs instead of landing on whichever went second.
#   - **The first pass is discarded.** Page cache, CPU frequency and allocator
#     arenas all reach steady state after one pass, and the first pass is where
#     they are not.
#
# ## One arm and one repetition per process
#
# Every invocation runs a single arm once. This is not a style choice:
#
#   - `peak_rss_mb` comes from `getrusage(RUSAGE_SELF)`, which is a high-water
#     mark for the whole process. Two arms in one process report the larger of
#     the two for both, and the second arm of a sweep inherits the first arm's
#     peak forever.
#   - `bench-compare.sh` pairs repetitions by position within an identity, so a
#     repetition has to be a record, and records append in run order.
#
# So the driver sets `REPS=1` and loops itself. A rig's own sweep mode is not
# used here for the same reason.
#
# ## The corpus is staged once and shared
#
# `DATA_DIR` points both arms at the same bytes. A rig that regenerates its
# corpus per process would otherwise measure corpus generation, and — worse —
# the two legs would be reading different data while the report claimed they
# were not. Rigs that take no corpus ignore it.

set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: bench-drive.sh [--base <ref>] [--reps N] [--out DIR] [rig ...]
       bench-drive.sh --self-test

  --base <ref>  what to compare against (default: main)
  --reps N      recorded repetitions per arm (default: 3); one extra
                unrecorded pass always runs first and is discarded
  --out DIR     where to write base.jsonl and head.jsonl (default: a
                temporary directory, printed on completion)
USAGE
}

# Resolved from this script rather than the caller's cwd, as its siblings do.
# Invoked by absolute path from another checkout, `git rev-parse` would answer
# about that checkout and this run would register a worktree in it.
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)

# ---------------------------------------------------------------------------
# Self-test.
# ---------------------------------------------------------------------------
# Exercises the ordering the whole script exists to produce, without building
# anything: a stub rig records the order it was called in, and the assertions
# below are the three properties above stated as facts about that order.
#
# `SPATE_BENCH_DRIVE_UNDER_TEST` points at another copy of this script, which is
# how a fixture is proven load-bearing — mutate the copy, watch the assertion
# fail.
if [[ "${1:-}" == "--self-test" ]]; then
    under_test=${SPATE_BENCH_DRIVE_UNDER_TEST:-"$repo_root/scripts/bench-drive.sh"}
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT

    # A stub that appends "<arm> <rep>" per invocation, standing in for a rig.
    mkdir -p "$work/bin"
    cat >"$work/bin/stub" <<'STUB'
#!/bin/sh
printf '%s %s\n' "${SPATE_ARM:-?}" "${SPATE_PASS:-?}" >>"$SPATE_ORDER_LOG"
printf 'reps=%s commit=%s data=%s\n' "${REPS:-unset}" "${GIT_COMMIT:-unset}" \
    "${DATA_DIR:-unset}" >>"$SPATE_ENV_LOG"
[ -z "${RESULTS:-}" ] || printf '{"bench":"stub"}\n' >>"$RESULTS"
exit 0
STUB
    chmod +x "$work/bin/stub"

    export SPATE_ORDER_LOG="$work/order" SPATE_ENV_LOG="$work/env"
    : >"$SPATE_ORDER_LOG"
    : >"$SPATE_ENV_LOG"
    # A real rig name with a stub binary behind it, so the runnable-set check
    # below is exercised rather than bypassed.
    if ! SPATE_BENCH_DRIVE_DRY=1 SPATE_BENCH_DRIVE_STUB="$work/bin/stub" \
        "$under_test" --reps 2 --out "$work/out" deser_formats >"$work/log" 2>&1; then
        echo "::error::bench-drive.sh --self-test: a dry run failed."
        cat "$work/log" >&2
        exit 1
    fi

    order=$(tr '\n' '|' <"$SPATE_ORDER_LOG")
    # Both builds first, then one discarded pass per arm, then two recorded,
    # alternating throughout and swapping which arm leads on alternate passes.
    #
    # The two `build` entries are what makes "both binaries are built before
    # either is measured" a tested claim rather than a described one: the real
    # builds and this dry-mode marker go through the same function, so removing
    # one or moving it after the run fails here.
    want='build base|build head|base 0|head 0|head 1|base 1|base 2|head 2|'
    if [[ "$order" != "$want" ]]; then
        echo "::error::the run order is wrong."
        echo "got:  $order"
        echo "want: $want"
        exit 1
    fi

    # Pass 0 is discarded: it must reach no results file.
    # `wc -l`, not `grep -c ''`: grep exits 1 on an empty file, and under
    # `set -e` that aborted the assignment before the diagnostic below could
    # print, making a genuinely broken driver and a crashed self-test
    # indistinguishable -- both a bare non-zero exit with no output.
    base_lines=$(wc -l <"$work/out/base.jsonl" | tr -d ' ')
    head_lines=$(wc -l <"$work/out/head.jsonl" | tr -d ' ')
    if [[ "$base_lines" != "2" || "$head_lines" != "2" ]]; then
        echo "::error::the discarded pass reached the results: base=$base_lines head=$head_lines, want 2 each."
        exit 1
    fi

    # One repetition per process, on every invocation — the `peak_rss_mb` and
    # pairing constraints at the top of this file. Asserted rather than assumed
    # because the rigs all take `REPS` and all default it to more than one.
    if grep -qv '^reps=1 ' "$SPATE_ENV_LOG"; then
        echo "::error::a rig was invoked with something other than REPS=1:"
        grep -v '^reps=1 ' "$SPATE_ENV_LOG" >&2
        exit 1
    fi
    # Both arms read one corpus, and each arm names its own commit.
    if [[ "$(sed 's/.*data=//' "$SPATE_ENV_LOG" | sort -u | grep -c '')" != "1" ]]; then
        echo "::error::the arms did not share one DATA_DIR."
        exit 1
    fi
    commits=$(sed 's/.* commit=//; s/ .*//' "$SPATE_ENV_LOG" | sort -u | tr '\n' ' ')
    if [[ "$commits" != "basecommit headcommit " ]]; then
        echo "::error::arms did not carry one commit each: '$commits'."
        exit 1
    fi

    # A rig the classifier does not name is refused before anything is built.
    # Every excluded rig is a real binary that runs without the infrastructure
    # it needs, so this is the difference between a clear refusal and a run
    # that costs two release builds and then measures nonsense.
    if SPATE_BENCH_DRIVE_DRY=1 SPATE_BENCH_DRIVE_STUB="$work/bin/stub" \
        "$under_test" --reps 1 --out "$work/out2" no_such_rig >"$work/log2" 2>&1; then
        echo "::error::an unknown rig name was accepted."
        exit 1
    fi

    echo "bench-drive.sh: self-test ok — both builds compile before either runs," \
        "arms alternate, the first pass is discarded, and one arm and one" \
        "repetition run per process."
    exit 0
fi

# ---------------------------------------------------------------------------
# Arguments.
# ---------------------------------------------------------------------------
base_ref=main
reps=3
out_dir=""
rigs=()
while [[ $# -gt 0 ]]; do
    case "$1" in
    --base)
        [[ $# -ge 2 ]] || { usage; exit 1; }
        base_ref=$2
        shift 2
        ;;
    --reps)
        [[ $# -ge 2 ]] || { usage; exit 1; }
        reps=$2
        shift 2
        ;;
    --out)
        [[ $# -ge 2 ]] || { usage; exit 1; }
        out_dir=$2
        shift 2
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    -*)
        echo "::error::unknown option '$1'." >&2
        usage
        exit 1
        ;;
    *)
        rigs+=("$1")
        shift
        ;;
    esac
done

if ! [[ "$reps" =~ ^[1-9][0-9]*$ ]]; then
    echo "::error::--reps wants a positive integer, got '$reps'." >&2
    exit 1
fi

# The runnable set, from the one place that knows it. Captured rather than
# consumed from a process substitution: a producer that died mid-output would
# iterate over what it managed to print and report success, silently shrinking
# the set — the shape the weekly smoke job rejects in prose for the same seam.
runnable=$("$repo_root/scripts/ci-changes.sh" --wallclock-rigs-all) || {
    echo "::error::the classifier could not name the runnable rigs." >&2
    exit 1
}
if [[ ${#rigs[@]} -eq 0 ]]; then
    while IFS= read -r rig; do
        [[ -n "$rig" ]] || continue
        rigs+=("$rig")
    done <<<"$runnable"
else
    # A name given on the command line still has to be one the classifier
    # knows. Every excluded rig is a real binary that builds and runs without
    # the broker or server it needs, so a typo or an excluded name would
    # otherwise cost two release builds and then measure nonsense.
    for name in "${rigs[@]}"; do
        if ! grep -qxF -- "$name" <<<"$runnable"; then
            echo "::error::'$name' is not in the runnable set; the classifier names:" >&2
            printf '  %s\n' "$runnable" >&2
            exit 1
        fi
    done
fi
if [[ ${#rigs[@]} -eq 0 ]]; then
    echo "::error::no rig to drive." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# The two builds.
# ---------------------------------------------------------------------------
dry=${SPATE_BENCH_DRIVE_DRY:-}
# A test hook, honoured only in dry mode. Read unconditionally it would be
# inherited by a real run from a shell that had once debugged the self-test:
# both release trees built for real, then the stub measured instead of them,
# two identical legs, exit 0, no warning.
stub=""
[[ -z "$dry" ]] || stub=${SPATE_BENCH_DRIVE_STUB:-}

# One cleanup, registered once. Composing traps by re-reading `trap -p` and
# splicing the old handler into the new one expands at registration rather than
# at signal time, which is the difference between removing the directory you
# meant and removing whatever that variable happened to hold.
worktree=""
data_dir=""
own_out=""
cleanup() {
    [[ -z "$worktree" ]] || {
        git worktree remove --force "$worktree/base" 2>/dev/null || true
        rm -rf "$worktree"
        # `remove` fails if the worktree is missing or locked, and the
        # registration under .git/worktrees then survives forever, with every
        # later run allocating base1, base2, ...
        git -C "$repo_root" worktree prune 2>/dev/null || true
    }
    [[ -z "$data_dir" ]] || rm -rf "$data_dir"
    [[ -z "$own_out" ]] || rm -rf "$own_out"
}
trap cleanup EXIT

if [[ -n "$dry" ]]; then
    base_commit=basecommit
    head_commit=headcommit
else
    # `--verify` and a `^{commit}` peel. Without `--verify`, `rev-parse` echoes
    # an unrecognised word back and exits 0, so a path-like typo passes here and
    # dies later inside git. Without the peel, an annotated tag resolves to the
    # tag object's own id, so `AB_BASE=v0.1.0` -- the documented example --
    # would label every base record with an id that is not a commit, which is
    # the one thing this driver exists to get right.
    head_commit=$(git rev-parse --verify --quiet "HEAD^{commit}") || {
        echo "::error::cannot resolve HEAD to a commit." >&2
        exit 1
    }
    base_commit=$(git rev-parse --verify --quiet "$base_ref^{commit}") || {
        echo "::error::'$base_ref' does not name a commit." >&2
        exit 1
    }
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "::error::the working tree is dirty; the head arm would not be the commit it names." >&2
        exit 1
    fi
fi

# Only now are the legs created. Truncating them before the base ref is
# resolved and the tree is checked would destroy the previous comparison on
# behalf of a typo.
if [[ -z "$out_dir" ]]; then
    out_dir=$(mktemp -d)
    own_out=$out_dir
fi
mkdir -p -- "$out_dir"
: >"$out_dir/base.jsonl"
: >"$out_dir/head.jsonl"

# Both arms are built before either is measured: a build between arms is
# minutes of full-machine load in the middle of the measurement. Routed through
# one function so `--self-test` can observe that ordering rather than assert it
# in prose.
base_target=""
head_target=""
build_arms() {
    local name
    if [[ -n "$dry" ]]; then
        printf 'build %s\n' base head >>"${SPATE_ORDER_LOG:-/dev/null}"
        return 0
    fi
    worktree=$(mktemp -d)
    git worktree add --detach "$worktree/base" "$base_commit" >/dev/null
    base_target="$worktree/base-target"
    # Explicit for BOTH arms. A command-line `--target-dir` overrides
    # `CARGO_TARGET_DIR` while a bare `cargo build` obeys it, so naming it for
    # base alone meant that with that variable exported the head build landed
    # elsewhere and the head arm ran whatever binary an earlier ordinary build
    # had left in ./target -- a stale binary carrying the current commit, which
    # is the misattribution this driver exists to prevent.
    head_target="$repo_root/target/bench-ab-head"
    echo "building base ($base_commit) and head ($head_commit)..." >&2
    cargo build --release -p benchmarks --locked --target-dir "$base_target" \
        --manifest-path "$worktree/base/Cargo.toml"
    cargo build --release -p benchmarks --locked --target-dir "$head_target" \
        --manifest-path "$repo_root/Cargo.toml"
    # A rig the branch added does not exist on the base side. Caught by one
    # cheap check rather than by an exec failure part-way through a run that
    # has already cost both builds.
    for name in "${rigs[@]}"; do
        if [[ ! -x "$base_target/release/$name" ]]; then
            echo "::error::rig '$name' has no binary in the base build; it does not exist at $base_ref." >&2
            exit 1
        fi
    done
}
build_arms

# The corpus, staged once and shared by both arms. Only rigs that take a corpus
# read it.
data_dir=$(mktemp -d)

# ---------------------------------------------------------------------------
# The run: alternate arms, discard the first pass.
# ---------------------------------------------------------------------------
run_arm() {
    local arm=$1 pass=$2 rig=$3 results=$4 bin commit
    if [[ -n "$stub" ]]; then
        bin=$stub
    elif [[ "$arm" == base ]]; then
        bin="$base_target/release/$rig"
    else
        bin="$head_target/release/$rig"
    fi
    if [[ "$arm" == base ]]; then commit=$base_commit; else commit=$head_commit; fi

    # `REPS=1`: one repetition per process, for the reasons at the top.
    # `env -u SMOKE`: a smoke run measures nothing, and an exported SMOKE would
    # otherwise be inherited by both arms and produce a full set of well-formed,
    # symmetric, meaningless records with nothing in them saying so.
    # A rig prints each record to stdout as well as appending it to RESULTS, so
    # stdout is dropped here and this script's own output stays usable.
    env -u SMOKE \
        SPATE_ARM="$arm" SPATE_PASS="$pass" \
        BENCH_TRIGGER=dispatched \
        GIT_COMMIT="$commit" \
        DATA_DIR="$data_dir" \
        REPS=1 \
        RESULTS="$results" \
        "$bin" </dev/null >/dev/null
}

for rig in "${rigs[@]}"; do
    for ((pass = 0; pass <= reps; pass++)); do
        # Pass 0 is the warm-up: run, discard. Its records go to a file nothing
        # reads, rather than being suppressed, so a rig that only emits on a
        # results path behaves identically on every pass.
        if [[ "$pass" -eq 0 ]]; then
            base_out=/dev/null
            head_out=/dev/null
        else
            base_out="$out_dir/base.jsonl"
            head_out="$out_dir/head.jsonl"
        fi
        # Which arm leads swaps on alternate passes: ABBA, not ABAB. Strict
        # base-then-head leaves a constant one-slot bias on whichever arm goes
        # second under any monotonic drift, which is the thing alternating is
        # supposed to remove rather than merely bound.
        if (((pass % 2) == 0)); then
            run_arm base "$pass" "$rig" "$base_out"
            run_arm head "$pass" "$rig" "$head_out"
        else
            run_arm head "$pass" "$rig" "$head_out"
            run_arm base "$pass" "$rig" "$base_out"
        fi
    done
done

echo "$out_dir"
