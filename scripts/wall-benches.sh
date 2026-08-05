#!/usr/bin/env bash
#
# Discovers the workspace's wall-clock bench targets and checks how they are
# declared.
#
# The wall tier is driven by `bench/`'s own CLI — `make bench-ab` and friends —
# which finds its targets through `cargo metadata`. This script exists for the
# one thing that reader cannot do cheaply: a missing `harness = false` stanza,
# which it cannot detect until both legs have been built and it has started the
# target. A newly added bench does not exist on the base ref, so the base leg
# builds and lists cleanly and the fault lands at the head leg's first run.
#
#   Makefile: check-wall-benches (and so `ci-lint`, and so `gates`)  --self-test
#
# That stanza check is the one that earns its keep on build cost. The other two
# below are lint-time copies of refusals the driver already makes early.
#
# Deliberately separate from `scripts/gungraun-benches.sh`, which performs the
# same stanza check for the counted tier. Three reasons, the first decisive:
#
#   1. That script's discovery feeds `scripts/ci-changes.sh`, which decides
#      which `perf-counters` matrix shards run. A wall target appearing in that
#      output would allocate a callgrind shard for a bench valgrind never runs.
#   2. Its glob is `crates/*/benches/*_gungraun.rs` and it derives the package
#      from the directory name. Both assumptions break here:
#      `bench/benches/selftest_wall.rs` lives outside `crates/`, and its package
#      is `spate-bench` rather than its directory name `bench`. This script
#      therefore reports the manifest path and never derives a package name.
#   3. Its `--run` exit-code trichotomy is a contract with `ci.yml`'s
#      merge-base probe. A second subcommand set in that file would put the
#      contract in the path of an unrelated change.
#
# The `declares_target` awk below is duplicated from that script for the same
# reasons. Sharing it would mean one filesystem reader for two tiers, which is
# what (1) rules out.
#
# Usage:
#   ./scripts/wall-benches.sh              # print `manifest target` lines
#   ./scripts/wall-benches.sh --self-test
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
    echo "wall-benches.sh: $1" >&2
    exit 1
}

# One `manifest target` line per discovered target, sorted so every consumer
# sees the same order on every machine — glob order is filesystem-dependent.
#
# Two globs, because a workspace member may sit at the top level (`bench/`) or
# under `crates/`. They cannot overlap: `*/benches/` is one path component
# before `benches`, `crates/*/benches/` is two.
discover() {
    local bench_src manifest bench
    for bench_src in ./*/benches/*_wall.rs ./crates/*/benches/*_wall.rs; do
        [[ -e "$bench_src" ]] || continue
        bench_src=${bench_src#./}
        manifest=${bench_src%/benches/*}/Cargo.toml
        bench=$(basename "$bench_src" .rs)
        printf '%s %s\n' "$manifest" "$bench"
    done | LC_ALL=C sort -u
}

# Every `*_wall.rs` must carry a `[[bench]]` stanza naming it with
# `harness = false`. Without the stanza cargo still auto-discovers the file as a
# bench target — but with the default libtest harness, which rejects the runner
# protocol's arguments before the target's own `main` is reached. The driver
# detects that and says so, but only after building both legs of a comparison.
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
            # name = "chain_wall"  ->  chain_wall
            if (match($0, /"[^"]*"/)) name = substr($0, RSTART + 1, RLENGTH - 2)
        }
        in_bench && /^[[:space:]]*harness[[:space:]]*=[[:space:]]*false/ { harness_false = 1 }
        # awk runs END on an explicit exit too, so the last-block case must not
        # overwrite a verdict already settled above.
        END { if (!settled && in_bench && name == want) found = harness_false
              exit(found ? 0 : 1) }
    ' "$manifest"
}

self_test() {
    local manifest bench checked=0 names dupes

    # First, because everything below asserts something about discovered
    # targets: an empty tree would otherwise pass silently, and `make bench-ab`
    # would be measuring nothing at all.
    [[ -n "$(discover)" ]] || fail \
        "discovered no wall-clock benches at all — the naming convention or the
    glob has changed, and every A/B comparison is measuring nothing."

    # A duplicate target name across two packages, and a name that is not a
    # valid Rust identifier, are both refusals inside the A/B driver too — and
    # early ones, in its `cargo metadata` read, before either leg is built. What
    # they cost there is a comparison that cannot run at all until somebody
    # edits a manifest; what they cost here is a lint. Checked for that reason
    # rather than for a build the driver never gets as far as.
    names=$(discover | cut -d' ' -f2)
    dupes=$(printf '%s\n' "$names" | LC_ALL=C sort | uniq -d)
    [[ -z "$dupes" ]] || fail \
        "two packages declare a wall bench target named: $(echo "$dupes" | tr '\n' ' ')
    The A/B driver keys its records by target name and refuses a collision."

    while read -r manifest bench; do
        [[ "$bench" != *-* ]] || fail \
            "the target name '$bench' contains a hyphen. The record carries the crate
    name cargo compiles the target under, where a hyphen becomes an underscore,
    so the two would disagree."
        [[ -f "$manifest" ]] || fail "$manifest not found for bench $bench"
        declares_target "$manifest" "$bench" || fail \
            "the wall bench '$bench' has no '[[bench]] name = \"$bench\"' with
    harness = false in $manifest. Cargo would auto-discover it under the default
    libtest harness, which rejects the runner protocol's arguments before the
    target's own main is reached."
        checked=$((checked + 1))
    done < <(discover)

    echo "wall-benches.sh --self-test: $checked wall bench target(s) are declared correctly"
}

case "${1:---discover}" in
    --discover) discover ;;
    --self-test) self_test ;;
    *) fail "unknown argument '$1' (expected --self-test or no argument)" ;;
esac
