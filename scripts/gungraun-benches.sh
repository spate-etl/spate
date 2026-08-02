#!/usr/bin/env bash
#
# Discovers the workspace's gungraun bench targets, and runs them.
#
# The instruction-count job measures a pull request twice: once on the merge
# base and once on the head. Those two legs used to derive their bench list
# from different places — the base leg globbed the merge-base tree, the head
# leg ran a hand-written list in the Makefile — so adding a bench to one and
# not the other was a silent, undetectable drift. This script is the single
# reader of the filesystem, and every consumer asks it:
#
#   Makefile: bench-gungraun   --run
#   Makefile: bench-gungraun-check, and any machine without valgrind  --check
#   ci.yml, both legs          --run
#
# Discovery is by naming convention — `crates/<pkg>/benches/<name>_gungraun.rs`
# — which is what lets the base leg work at all: it runs the *merge base's*
# copy of this script against the merge base's tree, so a merge base that
# predates a bench simply contributes no baseline for it rather than failing.
# Crate directory names equal package names in this workspace.
#
# Usage:
#   ./scripts/gungraun-benches.sh                 # print `pkg bench` lines
#   ./scripts/gungraun-benches.sh --pkgs-json     # ["spate-avro","spate-core"]
#   ./scripts/gungraun-benches.sh --run [PKG...]  # cargo bench each, in order
#   ./scripts/gungraun-benches.sh --self-test
#
# `--run` exits 0 when it ran at least one bench and all succeeded, 1 when any
# bench failed, and 2 when nothing was discovered. Today's callers act on
# zero-versus-nonzero only — the base leg wipes and reports no baseline either
# way — but the two nonzero cases mean different things and the log line each
# emits says which, so a reader of a failed run is not left guessing whether
# the tree had no benches or a bench died. `GUNGRAUN_*` variables are
# inherited by the cargo child, which is how the base leg passes
# `GUNGRAUN_SAVE_BASELINE`.
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
    echo "gungraun-benches.sh: $1" >&2
    exit 1
}

# One `pkg bench` line per discovered target, sorted so every consumer sees
# the same order on every machine (glob order is filesystem-dependent and
# `sort` is locale-dependent, and the report reads better when the tables do not shuffle).
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
# `harness = false`. Without the stanza cargo still auto-discovers the file as
# a bench target — but with the default libtest harness, which rejects
# gungraun's own arguments. The bench therefore compiles, ships, and dies at
# run time with an error about arguments rather than about the missing
# stanza. This check is the reason that mistake cannot reach CI.
declares_target() {
    local manifest=$1 bench=$2
    # Key ordering inside a block is free, so the verdict can only be reached
    # at the block's end — which is either the next table header or EOF.
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

# The base leg branches on `--run`'s exit status to decide whether it has a
# usable baseline, so the trichotomy is a contract rather than an
# implementation detail. Proving it needs no valgrind and no real bench: a
# stub `cargo` on PATH exercises every branch in milliseconds, on any host.
assert_run_exit() {
    local cargo_exit=$1 want=$2 desc=$3
    shift 3
    local stub got=0
    stub=$(mktemp -d)
    printf '#!/bin/sh\nexit %s\n' "$cargo_exit" >"$stub/cargo"
    chmod +x "$stub/cargo"
    PATH="$stub:$PATH" run "$@" >/dev/null 2>&1 || got=$?
    rm -rf "$stub"
    [[ "$got" -eq "$want" ]] || fail "--run with $desc: expected exit $want, got $got"
}

self_test() {
    local pkg bench manifest checked=0

    # First, because everything below asserts something about discovered
    # benches: an empty tree would otherwise surface as an exit-code mismatch
    # and send the reader looking in the wrong place.
    [[ -n "$(discover)" ]] || fail \
        "discovered no gungraun benches at all — the naming convention or the
    glob has changed, and both CI legs are measuring nothing."

    assert_run_exit 0 0 "every bench succeeding"
    assert_run_exit 3 1 "a bench failing"
    # An empty selection and a failure must not look alike: an old merge base
    # legitimately has no benches, while a failure means wipe the partial
    # measurement. Filtering to a package that cannot exist is the cheapest
    # way to reach the empty branch without touching the tree.
    assert_run_exit 0 2 "a filter matching no package" __no_such_package__
    while read -r pkg bench; do
        manifest="crates/$pkg/Cargo.toml"
        [[ -f "$manifest" ]] || fail "$manifest not found for bench $bench"
        declares_target "$manifest" "$bench" || fail \
            "crates/$pkg/benches/$bench.rs has no '[[bench]] name = \"$bench\"' with
    harness = false in $manifest. Cargo would auto-discover it under the
    default libtest harness, which rejects gungraun's arguments at run time."
        checked=$((checked + 1))
    done < <(discover)

    echo "gungraun-benches.sh --self-test: exit codes hold; $checked bench target(s) declared correctly"
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

# Builds each discovered bench without running it. The useful check on a
# machine without valgrind, and far cheaper than the workspace-wide
# `bench-check`, which builds every rig in the release profile.
check_only() {
    local pkg bench failed=0 ran=0
    while read -r pkg bench; do
        if (set -x; cargo bench --no-run -p "$pkg" --locked --bench "$bench" </dev/null); then
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
# Named explicitly with `--bench` for the same reason the Makefile does: an
# unnamed `cargo bench -p X` also drives the lib's default libtest harness,
# which rejects any argument the real harness forwards.
run() {
    local wanted=("$@") pkg bench failed=0 ran=0
    while read -r pkg bench; do
        if [[ ${#wanted[@]} -gt 0 ]]; then
            local match=0 w
            for w in "${wanted[@]}"; do
                [[ "$w" == "$pkg" ]] && match=1
            done
            [[ "$match" -eq 1 ]] || continue
        fi
        # Traced in a subshell so the log shows the real cargo invocation —
        # `make` echoes its recipe lines and this preserves that — without
        # tracing the surrounding bookkeeping.
        # `</dev/null`: this loop reads `discover` on stdin, so a child that
        # read stdin would swallow the remaining benches and the run would
        # report success having measured a subset — which the base leg would
        # then save as a complete baseline.
        # THROWAWAY PROBE — do not merge. spate-json's `simd` backend uses
        # runtime CPU-feature dispatch, and nothing has ever built it under
        # callgrind. Hardcoded here to find out whether valgrind survives it
        # and whether the resulting counts are reproducible; a real feature
        # axis would be declared, not baked into the runner.
        local probe_features=()
        if [[ "$pkg" == "spate-json" ]]; then
            probe_features=(--features simd)
        fi
        if (set -x; cargo bench -p "$pkg" --locked --bench "$bench" \
            ${probe_features[@]+"${probe_features[@]}"} </dev/null); then
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
    --check) check_only ;;
    "") discover ;;
    *) fail "unknown argument '$1' (expected --run, --check, --pkgs-json, --self-test, or none)" ;;
esac
