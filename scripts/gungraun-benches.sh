#!/usr/bin/env bash
#
# Discovers the workspace's gungraun bench targets, and runs them. This is the
# single reader of the filesystem, and every consumer asks it:
#
#   Makefile: bench-gungraun        --run
#   Makefile: bench-gungraun-check  --check
#   ci.yml, both legs               --run
#
# Discovery is by naming convention: `crates/<pkg>/benches/<name>_gungraun.rs`.
# The base leg runs the *merge base's* copy of this script against the merge
# base's tree, so a merge base predating a bench contributes no baseline for it
# rather than failing.
#
# Usage:
#   ./scripts/gungraun-benches.sh                   # print `pkg bench` lines
#   ./scripts/gungraun-benches.sh --pkgs-json       # ["spate-avro","spate-core"]
#   ./scripts/gungraun-benches.sh --run [PKG...]    # cargo bench each, in order
#   ./scripts/gungraun-benches.sh --check [PKG...]  # cargo bench --no-run each
#   ./scripts/gungraun-benches.sh --self-test
#
# `--features LIST` may precede any of those and is forwarded verbatim to every
# cargo invocation. The empty string means the package's default features and
# no `--features` flag at all.
#
# `--run` exits 0 when it ran at least one bench and all succeeded, 1 when any
# bench failed, and 2 when nothing was discovered. `GUNGRAUN_*` variables are
# inherited by the cargo child: the base leg passes `GUNGRAUN_SAVE_BASELINE`
# that way.
#
# The merge-base leg runs the merge base's own copy of this file, which may
# predate `--features` and build the default arm while the head leg builds
# another. The workflow probes first: `--features X --run __no_such_package__`
# matches no package and answers 2 without invoking cargo, while a copy that
# does not know the flag answers 1. A shard whose merge base answers anything
# but 2 has no baseline.
set -euo pipefail

# Resolved before the `cd` below, because `$0` may be relative: `--self-test`
# re-executes this file to assert the argument parser's own exit codes.
self=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")

cd "$(dirname "$0")/.."

fail() {
    echo "gungraun-benches.sh: $1" >&2
    exit 1
}

# The compiled feature arm, parsed before the subcommand. Empty means default
# features, expressed as no `--features` flag rather than `--features default`.
features=""
if [[ "${1:-}" == "--features" ]]; then
    # `$#` rather than `-z "$2"`: the empty string is a legitimate value and
    # must not be confused with the flag having been given no value at all.
    [[ $# -ge 2 ]] || fail "--features needs a value ('' means default features)"
    features="$2"
    shift 2
fi

# The `--features` pair, or nothing. An array so the feature list reaches cargo
# as one argument whatever it contains, expanded with the `${a[@]+...}` guard
# because `set -u` treats an empty array as unset on bash 3.2.
feature_args=()
[[ -z "$features" ]] || feature_args=(--features "$features")

# One `pkg bench` line per discovered target, sorted under `LC_ALL=C` so every
# consumer sees the same order on every machine: glob order is
# filesystem-dependent and collation is locale-dependent.
discover() {
    local bench_src pkg bench
    for bench_src in crates/*/benches/*_gungraun.rs; do
        [[ -e "$bench_src" ]] || continue
        pkg=${bench_src#crates/}
        pkg=${pkg%%/*}
        bench=$(basename "$bench_src" .rs)
        printf '%s %s\n' "$pkg" "$bench"
    done | LC_ALL=C sort
}

# Every `*_gungraun.rs` must carry a `[[bench]]` stanza naming it with
# `harness = false`. Without it cargo auto-discovers the file under the default
# libtest harness, which reports `0 measured` and exit 0: green while measuring
# nothing.
declares_target() {
    local manifest=$1 bench=$2
    # Key ordering inside a block is free, so the verdict can only be reached
    # at the block's end: either the next table header or EOF.
    awk -v want="$bench" '
        /^[[:space:]]*\[/ {
            if (in_bench && name == want) { found = harness_false; settled = 1; exit }
            in_bench = ($0 ~ /^[[:space:]]*\[\[bench\]\]/)
            name = ""; harness_false = 0
            next
        }
        in_bench && /^[[:space:]]*name[[:space:]]*=/ {
            # name = "chain_gungraun"  ->  chain_gungraun
            if (match($0, /"[^"]*"/)) name = substr($0, RSTART + 1, RLENGTH - 2)
        }
        in_bench && /^[[:space:]]*harness[[:space:]]*=[[:space:]]*false/ { harness_false = 1 }
        # awk runs END on an explicit exit too, so the last-block case must not
        # overwrite a verdict already settled above.
        END { if (!settled && in_bench && name == want) found = harness_false
              exit(found ? 0 : 1) }
    ' "$manifest"
}

# A stub `cargo` that records the argument list it was handed, one line per
# invocation, and exits with $2. A stub that discarded its arguments would hold
# every assertion built on it whether or not the caller forwarded the flag.
make_stub_cargo() {
    local dir=$1 exit_code=$2 log=$3
    {
        printf '#!/bin/sh\n'
        # `%q` quotes the log path for the generated shell; `"$*"` is the
        # whole argument list on one line, the form the greps below read.
        printf 'printf "%%s\\n" "$*" >>%q\n' "$log"
        printf 'exit %s\n' "$exit_code"
    } >"$dir/cargo"
    chmod +x "$dir/cargo"
}

# The base leg branches on `--run`'s exit status to decide whether it has a
# usable baseline, so the trichotomy is a contract. A stub `cargo` on PATH
# exercises every branch without valgrind or a real bench.
assert_run_exit() {
    local cargo_exit=$1 want=$2 desc=$3
    shift 3
    local stub got=0
    stub=$(mktemp -d)
    make_stub_cargo "$stub" "$cargo_exit" /dev/null
    PATH="$stub:$PATH" run "$@" >/dev/null 2>&1 || got=$?
    rm -rf "$stub"
    [[ "$got" -eq "$want" ]] || fail "--run with $desc: expected exit $want, got $got"
}

# Runs `$3 $4...` (a subcommand function) with the feature arm $1 in force and
# a recording stub `cargo` on PATH, leaving the recorded argv in file $2.
capture_argv() {
    local arm=$1 log=$2
    shift 2
    local stub
    stub=$(mktemp -d)
    make_stub_cargo "$stub" 0 "$log"
    # A subprocess through the real argument parser, not a call into the
    # subcommand with the globals assigned by hand: a rename that missed the
    # parser at file scope would keep every assertion here green while real runs
    # measured the default arm.
    PATH="$stub:$PATH" "$self" --features "$arm" "$@" >/dev/null 2>&1 || true
    rm -rf "$stub"
}

# Every recorded line carries $2, or fail with $3. Counted rather than
# `grep -q`ed: one bench measured under a different arm than its siblings is
# not comparable with them.
assert_every_line() {
    local log=$1 needle=$2 desc=$3 lines matches
    lines=$(grep -c '' "$log" || true)
    matches=$(grep -c -F -- "$needle" "$log" || true)
    [[ "$lines" -gt 0 ]] || fail "$desc: the stub cargo was never invoked at all"
    [[ "$matches" -eq "$lines" ]] || fail \
        "$desc: only $matches of $lines cargo invocation(s) carried '$needle'"
}

assert_no_line() {
    local log=$1 needle=$2 desc=$3 matches
    matches=$(grep -c -F -- "$needle" "$log" || true)
    [[ "$matches" -eq 0 ]] || fail "$desc: $matches cargo invocation(s) carried '$needle'"
}

self_test() {
    local pkg bench manifest checked=0 sample log rc

    # First, because everything below asserts something about discovered
    # benches: an empty tree would surface as an exit-code mismatch.
    [[ -n "$(discover)" ]] || fail \
        "discovered no gungraun benches at all. The naming convention or the
    glob has changed, and both CI legs are measuring nothing."

    assert_run_exit 0 0 "every bench succeeding"
    assert_run_exit 3 1 "a bench failing"
    # An empty selection and a failure must not look alike: an old merge base
    # legitimately has no benches, a failure means wipe the measurement.
    assert_run_exit 0 2 "a filter matching no package" __no_such_package__

    # --- the feature axis reaches cargo ------------------------------------
    #
    # A sample package taken from discovery rather than named, for the same
    # reason ci-changes.sh derives its samples.
    sample=$(discover)
    sample=${sample%%$'\n'*}
    sample=${sample%% *}

    log=$(mktemp)
    capture_argv "" "$log" --run "$sample"
    assert_every_line "$log" " -p $sample " "--run"
    assert_no_line "$log" "--features" "--run with the default arm"
    : >"$log"

    capture_argv "simd" "$log" --run "$sample"
    assert_every_line "$log" "--features simd" "--run with a feature arm"
    : >"$log"

    # `--check` is what the cache-warming job builds each arm with, so a
    # feature it failed to forward would warm the wrong graph.
    capture_argv "simd" "$log" --check "$sample"
    assert_every_line "$log" "--no-run" "--check"
    assert_every_line "$log" "--features simd" "--check with a feature arm"
    : >"$log"

    capture_argv "" "$log" --check "$sample"
    assert_no_line "$log" "--features" "--check with the default arm"
    rm -f "$log"

    # --- the capability probe the merge-base leg depends on -----------------
    #
    # `--features X --run <no such package>` must reach the empty-selection exit
    # without invoking cargo. A copy predating the feature axis answers 1 from
    # its `unknown argument` arm. A subprocess, because the parser runs at file
    # scope.
    rc=0
    "$self" --features simd --run __no_such_package__ >/dev/null 2>&1 || rc=$?
    [[ "$rc" -eq 2 ]] || fail \
        "'--features … --run <no such package>' answered $rc, not the empty-selection 2 that
    ci.yml's merge-base probe reads as 'this copy understands the feature axis'."
    rc=0
    "$self" --features >/dev/null 2>&1 || rc=$?
    [[ "$rc" -eq 1 ]] || fail "'--features' with no value answered $rc, not 1"

    while read -r pkg bench; do
        manifest="crates/$pkg/Cargo.toml"
        [[ -f "$manifest" ]] || fail "$manifest not found for bench $bench"
        declares_target "$manifest" "$bench" || fail \
            "crates/$pkg/benches/$bench.rs has no '[[bench]] name = \"$bench\"' with
    harness = false in $manifest. Cargo would auto-discover it under the
    default libtest harness, which reports 0 measured and exits 0."
        checked=$((checked + 1))
    done < <(discover)

    echo "gungraun-benches.sh --self-test: exit codes hold, --features reaches every cargo
    invocation and no other, and $checked bench target(s) are declared correctly"
}

pkgs_json() {
    local pkg first=1 out="["
    while read -r pkg; do
        [[ "$first" -eq 1 ]] || out+=","
        out+="\"$pkg\""
        first=0
    done < <(discover | cut -d' ' -f1 | sort -u)
    printf '%s]\n' "$out"
}

# Is package $1 in the filter given as $2..? An empty filter selects everything.
# The right-hand side of `[[ == ]]` is quoted so a package name is compared as
# data: unquoted, a filter of `spate-*` would be a glob.
selected_pkg() {
    local pkg=$1 w
    shift
    [[ $# -gt 0 ]] || return 0
    for w in "$@"; do
        [[ "$w" == "$pkg" ]] && return 0
    done
    return 1
}

# Builds each discovered bench without running it, optionally filtered to the
# named packages. The filter lets the cache-warming job build one feature arm
# for the one package that has it; cargo rejects `--features simd` elsewhere.
check_only() {
    local wanted=("$@") pkg bench failed=0 ran=0
    while read -r pkg bench; do
        selected_pkg "$pkg" ${wanted[@]+"${wanted[@]}"} || continue
        if (set -x; cargo bench --no-run -p "$pkg" --locked --bench "$bench" \
            ${feature_args[@]+"${feature_args[@]}"} </dev/null); then
            ran=$((ran + 1))
        else
            echo "gungraun-benches.sh: $pkg --bench $bench failed to build" >&2
            failed=1
        fi
    done < <(discover)
    [[ "$failed" -eq 0 ]] || return 1
    [[ "$ran" -gt 0 ]] || return 2
    return 0
}

# Runs each discovered bench, optionally filtered to the named packages.
# Named explicitly with `--bench`: an unnamed `cargo bench -p X` also builds and
# runs the lib's own test harness, and that is not the measurement.
run() {
    local wanted=("$@") pkg bench failed=0 ran=0
    while read -r pkg bench; do
        selected_pkg "$pkg" ${wanted[@]+"${wanted[@]}"} || continue
        # Traced in a subshell so the log shows the real cargo invocation
        # without the surrounding bookkeeping.
        # `</dev/null`: this loop reads `discover` on stdin, so a child that read
        # stdin would swallow the remaining benches, the run would report success
        # having measured a subset, and the base leg would save that as a
        # complete baseline.
        if (set -x; cargo bench -p "$pkg" --locked --bench "$bench" \
            ${feature_args[@]+"${feature_args[@]}"} </dev/null); then
            ran=$((ran + 1))
        else
            echo "gungraun-benches.sh: $pkg --bench $bench failed" >&2
            failed=1
        fi
    done < <(discover)

    [[ "$failed" -eq 0 ]] || return 1
    [[ "$ran" -gt 0 ]] || return 2
    return 0
}

case "${1:-}" in
    --self-test) self_test ;;
    --pkgs-json) pkgs_json ;;
    --run) shift; run "$@" ;;
    --check) shift; check_only "$@" ;;
    "") discover ;;
    *) fail "unknown argument '$1' (expected --features, --run, --check, --pkgs-json, --self-test, or none)" ;;
esac
