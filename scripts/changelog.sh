#!/usr/bin/env bash
#
# The changelog fragment tool: adds fragments, checks a change carries one, and
# assembles them into CHANGELOG.md at release.
#
# Entries are written per pull request into `changelog.d/` rather than straight
# into CHANGELOG.md. `changelog.d/README.md` says why and states the format; the
# short version is that a fragment is a separate reviewable diff, and that
# checking "a file was added" has no fail-open mode, where checking "the
# `## [Unreleased]` section grew" does — a section extractor that loses its end
# boundary silently accepts any edit anywhere in the file.
#
# The conventions follow towncrier. The implementation does not: `towncrier
# check` can only ask "was any fragment added", and the interesting half of the
# question here is *whether one was required*, which is this repository's own
# policy and is not expressible there.
#
# Usage:
#   ./scripts/changelog.sh --check              # the gate (reads env, see below)
#   ./scripts/changelog.sh --new <type> <slug>  # scaffold a fragment
#   ./scripts/changelog.sh --build <version>    # assemble, at release
#   ./scripts/changelog.sh --self-test          # the classifier, alone
#
# Environment (--check only; all optional, all set by ci.yml):
#   EVENT_NAME  github.event_name
#   BASE_SHA    github.event.pull_request.base.sha  (pull_request only)
#   HEAD_SHA    github.event.pull_request.head.sha  (pull_request only)
#   PR_TITLE    github.event.pull_request.title     (pull_request only)
#   PR_BODY     github.event.pull_request.body      (pull_request only)
#
# Both PR_TITLE and PR_BODY are free text somebody else typed. They arrive
# through the environment and are only ever matched against, never evaluated —
# same handling as the filenames in ci-changes.sh, for the same reason.
#
# What this cannot defend against: a `pull_request` run executes the pull
# request's *own* copy of this script, so a change here narrows the gate of the
# very change proposing it. That is inherent to `pull_request` — the
# alternative, `pull_request_target`, hands a writable token to a job running
# alongside untrusted code, which is a far worse trade. `scripts/` is a
# CODEOWNERS path, and reviewing what a diff does to CI is part of reviewing the
# diff. Same property, same compensating control, as ci-changes.sh.
#
# Targets `bash` 3.2, which is what stock macOS ships as /bin/bash: no
# associative arrays, no `mapfile`, no `${var,,}`, and every array expansion
# guarded, because `"${arr[@]}"` on an empty array is an unbound-variable error
# under `set -u` there.
set -euo pipefail

cd "$(dirname "$0")/.."

fragments=changelog.d
changelog=CHANGELOG.md
repo_url=https://github.com/spate-etl/spate

# The Keep a Changelog six, in the order a release renders them. 2.0.0 is
# explicit that there are "only six types on purpose" — a breaking change is not
# a seventh type, it is a `**Breaking:**` marker on whichever of these it is.
TYPES="added changed deprecated removed fixed security"

# The scopes that do not reach a crate. Typed out rather than derived from the
# `area:` labels in .github/labels.yml, because that list also contains
# `supply-chain` — and a `fix(supply-chain):` closing an advisory is precisely a
# release note.
#
# `bench` is the benchmark tier: the unpublished `spate-bench` harness and the
# `benches/` targets inside published crates. Both are exempt for the same
# reason, and it is a semantic one rather than a convenience: neither reaches a
# published crate's *surface*. The harness is `publish = false`, and a bench
# target is compiled by `cargo bench` and by nothing a consumer runs — cargo
# does not build `benches/` for a dependency, and a versionless path
# dev-dependency on the harness is stripped when the crate is packaged. So a
# change confined to the tier cannot be something a reader upgrading has to be
# told.
#
# What that leaves open is generic and is not this gate's job. A change that
# does reach a crate's surface, filed under `bench` because it also touched a
# bench, is exempted by a subject that is not true — exactly as `feat(docs)`
# would exempt a feature. The gate reads the subject; review reads the diff.
# Deriving the exemption from changed paths instead would trade this for a worse
# failure: a pull request that legitimately touches both would then need a
# fragment for a change nobody upgrading can observe.
EXEMPT_SCOPES="ci docs examples bench workspace website"

fail() {
    echo "changelog.sh: $1" >&2
    exit 1
}

# A scratch directory rather than a list of files, so the cleanup is one quoted
# expansion. Declared here rather than `local` to whichever function makes it:
# an EXIT trap runs after that function has returned, so a `local` is already
# out of scope by then and `set -u` turns the cleanup itself into the error.
#
# `return 0` is load-bearing, and only bash 3.2 shows why: with `scratch` empty
# the `[ -n ]` test is the last command in the function, so the trap exits
# non-zero and takes the whole script's status with it — printing success and
# returning 1.
scratch=""
cleanup() {
    [ -n "$scratch" ] && rm -rf "$scratch"
    return 0
}
trap cleanup EXIT

# The crate scopes are derived, never typed: a tenth crate must not become
# exempt by being left out of a list. Only `crates/*` is read, so a workspace
# member outside it keeps whatever exemption its area scope carries, and
# reading the directory needs no toolchain — which matters, because the job that
# runs this has none.
crate_scopes() {
    local dir
    for dir in crates/*/; do
        [ -d "$dir" ] && basename "$dir"
    done
}

# ---------------------------------------------------------------------------
# The classifier.
# ---------------------------------------------------------------------------
# Returns 0 = "this needs a fragment", 1 = "exempt". Reads a subject line and
# the crate list, and nothing else, so --self-test can drive it with no git
# state at all.
#
# An ignore-list, not an allow-list, on *both* axes — the same doctrine
# ci-changes.sh states in its own header, and for the same reason. Stated the
# other way round ("required iff the scope names a crate") it fails *open*, and
# this repository's history is the proof: `feat: multi-sink split`,
# `feat: record-aware sink sharding`, `feat!: leader-computed sticky
# assignment`, `feat: dynamic work-stealing coordination` and `refactor!: rename
# the framework to spate` name no scope at all, and are among the largest
# user-visible changes ever made here. So:
#
#   * no scope is not an exemption. An exemption is earned by *naming* one of
#     the non-crate areas, not by declining to name anything;
#   * an unrecognised scope is not an exemption. `feat(spate-kafkaa):` is one
#     keystroke from a crate;
#   * an unparseable subject is not an exemption. `feature(spate-core):` must
#     not be a silent way out.
needs_entry() {
    local subject=$1 type scopes bang scope reaches_crate=0 known

    # type(scope)!: text — the scope and the bang optional, text required.
    [[ "$subject" =~ ^([a-zA-Z]+)(\(([^\)]*)\))?(!)?:[[:space:]]*[^[:space:]] ]] || return 0
    type=$(printf '%s' "${BASH_REMATCH[1]}" | tr '[:upper:]' '[:lower:]')
    scopes="${BASH_REMATCH[3]}"
    bang="${BASH_REMATCH[4]}"

    # `!` decides on its own, before either axis. It is the author declaring a
    # breaking change, and a breaking change is the one thing a reader upgrading
    # cannot afford to have omitted — so the scope it was filed under does not
    # get to overrule it.
    #
    # This is not hypothetical. `c6a7a5c docs(workspace)!:` was scoped to a
    # documentation area and carried, inside it, `fix(spate-core,spate-kafka,
    # spate-clickhouse)!: validate breaker config at load` with a BREAKING
    # CHANGE footer — `breaker.open_for: 0s` stopped loading. Reading the scope
    # first exempted it, and 0.2.0 was assembled without the entry it owed.
    [ -n "$bang" ] && return 0

    # --- scope axis: can this reach something somebody depends on? ---
    if [ -z "$scopes" ]; then
        reaches_crate=1
    else
        known=" $EXEMPT_SCOPES "
        local IFS=,
        for scope in $scopes; do
            # `feat( spate-core ):` is legal conventional-commits; trim it.
            scope="${scope#"${scope%%[![:space:]]*}"}"
            scope="${scope%"${scope##*[![:space:]]}"}"
            case "$known" in
            *" $scope "*) ;;
            *) reaches_crate=1 ;;
            esac
        done
    fi
    [ "$reaches_crate" = "1" ] || return 1

    # --- type axis: would somebody upgrading care? ---
    case "$type" in
    # `revert` and `build` are in with the obvious three. Reverting a released
    # feature takes something away that people are using, and a `build` scoped to
    # a crate is where an MSRV floor moves — both are things a reader upgrading
    # has to be told, and neither is inferable from the version number.
    feat | fix | perf | revert | build) return 0 ;;
    # What is left cannot reach a consumer of the published crates: prose, tests,
    # formatting, a dependency bump the lockfile already records, and a refactor
    # — which is by definition behaviour-preserving, and carries `!` if it is not.
    docs | test | chore | style | ci | refactor) return 1 ;;
    *) return 0 ;;
    esac
}

# ---------------------------------------------------------------------------
# Self-test.
# ---------------------------------------------------------------------------
# Runs inline on every invocation as well as under --self-test. That is a
# departure from `ci-changes.sh --self-test`, which is a separate `make` target
# because it needs a toolchain; this one is a few dozen pattern matches against
# a here-document and costs microseconds, and a separate target is a target
# somebody forgets to run.
self_test() {
    local failures=0 subject want got crate scope n=0 sample extracted probe

    while IFS='|' read -r subject want; do
        case "$subject" in '' | '#'*) continue ;; esac
        if needs_entry "$subject"; then got=need; else got=exempt; fi
        if [ "$got" != "$want" ]; then
            echo "changelog.sh: classifier: '$subject' -> $got, expected $want" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
# --- crate-scoped and user-visible: a fragment is required ---
feat(spate-core): a windowed operator|need
fix(spate-kafka): stop dropping offsets on revoke|need
perf(spate-clickhouse): halve the encode cost|need
feat(spate-core,docs): a thing and its page|need
fix(spate-core,docs): stop the quarantine wait consuming the ladder|need
feat(spate-avro,bench): decode datums straight into typed records|need
# --- crate-scoped, but the type says nobody upgrading cares ---
docs(spate-core): rewrite the module documentation|exempt
test(spate-kafka): retry the container suite once|exempt
chore(spate-core): tidy an import|exempt
refactor(spate-core): extract a helper|exempt
style(spate-kafka): rustfmt|exempt
ci(spate-core): pin an action|exempt
# --- ...but reverting a release and moving an MSRV floor are not that ---
revert(spate-core): back out the windowed operator|need
build(spate-core): raise the MSRV floor to 1.95|need
# --- a user-visible type, but the scope names no crate ---
feat(docs): give Spate a mark that works on a square canvas|exempt
feat(ci): a new job|exempt
feat(website): restyle the navigation|exempt
fix(bench): pin the iteration count for both legs|exempt
fix(ci,docs): lowercase the Pages project name|exempt
# --- the automation's own subjects, verbatim from its config ---
chore(workspace): bump the cargo-compatible group|exempt
chore(ci): bump mikepenz/action-junit-report|exempt
chore(docs): bump typescript in /website|exempt
chore(examples): bump a dependency|exempt
chore: release v0.2.0|exempt
# --- the breaking marker decides on its own, before either axis ---
refactor(spate-core)!: rename a public trait|need
perf(spate-kafka)!: change the batch shape|need
feat(spate-s3)!: fence the split leases|need
# `docs(workspace)!:` is real history — c6a7a5c — and it carried a BREAKING
# CHANGE to `breaker.open_for` inside a documentation scope. Reading the scope
# first exempted it and 0.2.0 shipped without the entry.
docs(workspace)!: migrate CLAUDE.md to AGENTS.md|need
chore(ci)!: drop a workflow input|need
test(spate-core)!: rename a test helper somebody imports|need
# --- no scope is not an exemption. All five are real history. ---
refactor!: rename the framework to spate|need
feat!: leader-computed sticky assignment for source coordination|need
feat: dynamic work-stealing source coordination over NATS JetStream KV|need
feat: multi-sink split — per-type ClickHouse tables from one pipeline|need
feat: record-aware sink sharding with ClickHouse Distributed parity|need
# --- nothing unparseable gets a free pass ---
Relicense under Apache-2.0, drop the LGPL dependency|need
Update the readme|need
WIP|need
# --- one keystroke from an exemption is not an exemption ---
feature(spate-core): the type is misspelt|need
feat(spate-kafkaa): the scope is misspelt|need
feat(sapte-core): the scope is transposed|need
# --- tolerated spellings that must still classify ---
feat( spate-core ): a spaced scope|need
FEAT(spate-core): a shouty type|need
TABLE

    # The table above stays green if `crate_scopes` returns nothing: every scope
    # would become "unrecognised", every case would still classify as `need`,
    # and the classifier would have silently stopped consulting the tree. That
    # is the failure ci-changes.sh describes at the top of its own self-test, so
    # both sides are derived here too.
    while IFS= read -r crate; do
        [ -n "$crate" ] || continue
        n=$((n + 1))
        if ! needs_entry "feat($crate): x"; then
            echo "changelog.sh: 'feat($crate): x' is exempt — crates/ is not being read" >&2
            failures=$((failures + 1))
        fi
        if needs_entry "docs($crate): x"; then
            echo "changelog.sh: 'docs($crate): x' needs a fragment — the type axis is dead" >&2
            failures=$((failures + 1))
        fi
    done < <(crate_scopes)
    # Zero, not a count. A hard-coded floor would mean retiring a crate fails
    # `--check` for everybody with "this script is wrong, not your change" — and
    # the failure this is guarding against is the extractor going blind, which
    # looks like nothing at all, not like one fewer.
    if [ "$n" -lt 1 ]; then
        echo "changelog.sh: no crate scopes derived from crates/ — the extractor has gone" >&2
        echo "  blind, and every scope is now unrecognised rather than checked." >&2
        failures=$((failures + 1))
    fi

    for scope in $EXEMPT_SCOPES; do
        if needs_entry "feat($scope): x"; then
            echo "changelog.sh: '$scope' is in EXEMPT_SCOPES but still requires a fragment" >&2
            failures=$((failures + 1))
        fi
        # No exempt scope may name a crate: that would exempt a crate through a
        # typo in our own list rather than through a decision.
        if [ -d "crates/$scope" ]; then
            echo "changelog.sh: EXEMPT_SCOPES names 'crates/$scope', which is a crate" >&2
            failures=$((failures + 1))
        fi
    done

    # The fragment-name parser, driven with no filesystem state.
    sample=$(fragment_type "changelog.d/retry-ladder.fixed.md")
    if [ "$sample" != "fixed" ]; then
        echo "changelog.sh: fragment_type read '$sample' from a .fixed.md name, expected 'fixed'" >&2
        failures=$((failures + 1))
    fi
    extracted=$(fragment_type "changelog.d/README.md" || true)
    if [ -n "$extracted" ]; then
        echo "changelog.sh: fragment_type accepted README.md as type '$extracted'" >&2
        failures=$((failures + 1))
    fi
    # A nested path must be rejected. `--build` globs one level, so accepting it
    # here would pass the gate on a fragment the release cannot see.
    extracted=$(fragment_type "changelog.d/sub/x.fixed.md" || true)
    if [ -n "$extracted" ]; then
        echo "changelog.sh: fragment_type accepted a nested path as type '$extracted' —" >&2
        echo "  --build globs one level, so the gate would pass and the release would omit it" >&2
        failures=$((failures + 1))
    fi

    # A fragment has to say something. This is the guard that keeps the
    # design claim in this file's header true.
    probe="$(mktemp -d)/probe.fixed.md"
    : >"$probe"
    if fragment_has_prose "$probe"; then
        echo "changelog.sh: an empty file counts as a fragment — the gate is fail-open" >&2
        failures=$((failures + 1))
    fi
    printf '   \n\n\t\n' >"$probe"
    if fragment_has_prose "$probe"; then
        echo "changelog.sh: a whitespace-only file counts as a fragment" >&2
        failures=$((failures + 1))
    fi
    printf 'A real note.\n' >"$probe"
    if ! fragment_has_prose "$probe"; then
        echo "changelog.sh: a fragment with prose does not count as one" >&2
        failures=$((failures + 1))
    fi
    rm -rf "$(dirname "$probe")"

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s) — this script is wrong, not your change"
}

# The type embedded in a fragment filename, or nothing if the name is not a
# fragment. Keeping this a function rather than a glob is what lets the
# self-test assert that README.md is not mistaken for one.
#
# The path must be exactly `changelog.d/<slug>.<type>.md` — one level, no
# subdirectories. `--build` globs one level, so accepting a nested path here
# would let `--check` pass on a fragment the release then cannot see: the gate
# would say the note exists and the release would ship without it.
fragment_type() {
    local path=$1 base type
    case "$path" in
    */*/*) return 1 ;;
    esac
    base=$(basename "$path")
    case "$base" in
    *.*.md) ;;
    *) return 1 ;;
    esac
    type="${base%.md}"
    type="${type##*.}"
    case " $TYPES " in
    *" $type "*) printf '%s' "$type" ;;
    *) return 1 ;;
    esac
}

# A fragment has to say something. An empty or whitespace-only file satisfied
# the gate and shipped as an empty bullet — which refutes the property this
# whole approach is chosen for, that checking a file was added has no fail-open
# mode. It has one if the file's contents are never looked at.
fragment_has_prose() {
    [ -s "$1" ] || return 1
    grep -qE '[^[:space:]]' "$1"
}

# Does the message this subject came from carry `Changelog: none`?
#
# `git interpret-trailers --parse`, never a grep. The commit bodies here are
# dense prose, and a line like `Tests: the two fault-injection knobs ...` starts
# a sentence, not a trailer. git's own parser reads only the final block of a
# message and gets that right.
#
# One message at a time, too, and that is not stylistic: interpret-trailers
# reads *its whole input* as a single message and takes the trailers from the
# last block of it, so handing it a concatenated log finds only the oldest
# commit's trailers and silently reports none for every other commit.
has_changelog_none() {
    local source=$1 parsed
    if [ "$source" = body ]; then
        [ -n "${PR_BODY:-}" ] || return 1
        parsed=$(printf '%s\n' "$PR_BODY" | git interpret-trailers --parse 2>/dev/null || true)
    else
        parsed=$(git log -1 --format='%B' "$source" 2>/dev/null |
            git interpret-trailers --parse 2>/dev/null || true)
    fi
    printf '%s\n' "$parsed" | grep -qiE '^Changelog:[[:space:]]*none[[:space:]]*$'
}

# ---------------------------------------------------------------------------
# --check
# ---------------------------------------------------------------------------
cmd_check() {
    local base="" head="" mode=structure candidate ref
    local subjects_file offenders_file empty_file subject origin source file sha line
    local offenders=0 added=0 excused=0

    [ -d "$fragments" ] || fail "$fragments/ not found — it holds the changelog fragments"
    [ -f "$fragments/README.md" ] ||
        fail "$fragments/README.md not found. It states the format and, less obviously,
  is what keeps the directory in git once a release has consumed every fragment."

    case "${EVENT_NAME:-}" in
    pull_request)
        # Against the *merge base*, not `base.sha`. `base.sha` is the base
        # branch tip, so a two-dot diff also reports everything main has gained
        # since this branch last moved — the same note ci-changes.sh carries.
        #
        # A missing merge base is a hard failure here, where ci-changes.sh falls
        # open to running everything. The safe direction there is "run more"; the
        # analogue here would be "demand a fragment", which the contributor
        # cannot act on. A missing merge base means the checkout lost its
        # `fetch-depth: 0`, and that should be loud.
        if ! base=$(git merge-base "${BASE_SHA:-}" "${HEAD_SHA:-}" 2>/dev/null) || [ -z "$base" ]; then
            fail "no merge base for ${BASE_SHA:-?}..${HEAD_SHA:-?} — does the checkout still set fetch-depth: 0?"
        fi
        head="${HEAD_SHA}"
        mode=require
        ;;
    "")
        # A laptop. Orient against the obvious upstream so `make gates` answers
        # the question before you push, and fall back to structure-only rather
        # than failing on a repository we cannot orient ourselves in.
        for ref in origin/main upstream/main main; do
            if candidate=$(git rev-parse --verify --quiet "$ref^{commit}" 2>/dev/null) &&
                base=$(git merge-base HEAD "$candidate" 2>/dev/null) &&
                [ -n "$base" ] && [ "$base" != "$(git rev-parse HEAD)" ]; then
                mode=require
                break
            fi
            base=""
        done
        ;;
    *)
        # push, merge_group, schedule, workflow_dispatch. On `push` the
        # requirement was already proven on the pull request and `main` cannot be
        # rewritten, so a failure here is one nobody can fix. A merge queue entry
        # has no pull request title of its own, and inventing a subject for its
        # synthetic merge commit would be a second classifier with no test behind
        # it. Structure only — and it still reports, rather than skipping, so
        # ci-gate sees a real success.
        ;;
    esac

    # Structure-only is the honest answer on a laptop with no branch and on the
    # events that have no pull request to reason about. It must never be the
    # answer on a pull request, and the way that could happen is a workflow edit:
    # `ci-lint` and the CI job that runs its members have diverged before, and
    # collapsing the four steps into `- run: make ci-lint` would call this with
    # no `EVENT_NAME`, in a shallow checkout, where it would take the laptop arm
    # and report success having evaluated nothing.
    #
    # So: inside Actions, on a pull request, refusing to be inert is the point.
    if [ "$mode" = structure ] && [ -n "${GITHUB_ACTIONS:-}" ] &&
        [ "${GITHUB_EVENT_NAME:-}" = pull_request ]; then
        fail "running on a pull request inside GitHub Actions with no EVENT_NAME, so this
  would have checked nothing and passed.

  The job that runs this has to pass EVENT_NAME, BASE_SHA, HEAD_SHA, PR_TITLE and
  PR_BODY through \`env:\`, and its checkout needs \`fetch-depth: 0\`. See the
  \`changelog\` job in .github/workflows/ci.yml."
    fi

    if [ "$mode" = structure ]; then
        echo "changelog.sh: $fragments/ is present with its README; no base to compare against,"
        echo "  so no fragment requirement was evaluated (EVENT_NAME='${EVENT_NAME:-}')."
        return 0
    fi

    # The union of the pull request title and the branch's own subjects, where
    # the branch can only ever *add* the requirement — the same add-only shape
    # as ci-changes.sh's `apply_ci_labels`.
    #
    # The title is authoritative: this repository squashes with it as the commit
    # subject, so it is what lands on main and what release-plz reads to pick the
    # version. But title-only fails open — a pull request titled `chore: tidy up`
    # carrying a `feat(spate-core)` commit would escape — so the commits are a
    # second opinion.
    #
    # `--no-merges`, because `Merge branch 'main' into x` is unparseable, and an
    # unparseable subject is not exempt: without this every rebase-by-merge would
    # demand a fragment.
    scratch=$(mktemp -d)
    subjects_file="$scratch/subjects"
    offenders_file="$scratch/offenders"
    empty_file="$scratch/empty"
    : >"$subjects_file"
    : >"$offenders_file"
    : >"$empty_file"

    # Third column is what to read a `Changelog: none` trailer from: the pull
    # request body for the title, the commit's own message for a commit.
    [ -n "${PR_TITLE:-}" ] &&
        printf '%s\tpull request title\tbody\n' "$PR_TITLE" >>"$subjects_file"
    while IFS=$'\t' read -r subject sha; do
        [ -n "$subject" ] && printf '%s\tcommit %s\t%s\n' "$subject" "$sha" "$sha" >>"$subjects_file"
    done < <(git log --no-merges --format='%s%x09%h' "$base..${head:-HEAD}" 2>/dev/null || true)

    # Collected quietly and reported only if it turns out to matter. Classifying
    # straight to stderr would put an alarming list in front of a contributor
    # whose change goes on to satisfy the gate on the very next line.
    #
    # A trailer excuses the message it is written on, and the scope of that
    # differs by where it is written.
    #
    # In the **pull request body** it excuses the whole pull request, because
    # the body is what the squash commit carries: it is the author's deliberate,
    # reviewable statement about the one commit that will exist on `main`.
    #
    # On an individual **commit** it excuses only that commit's subject. Any
    # trailer anywhere used to excuse the whole range, which is wider than the
    # trailer says — a pull request carrying a genuine feature *and* a
    # never-released fix was let through on the fix's trailer, and the feature
    # shipped with no note.
    if has_changelog_none body; then
        echo "changelog.sh: the pull request body carries a 'Changelog: none' trailer, which"
        echo "  is what the squash commit will carry. Taken at its word for this pull request."
        return 0
    fi

    while IFS=$'\t' read -r subject origin source; do
        [ -n "$subject" ] || continue
        needs_entry "$subject" || continue
        if [ "$source" != body ] && has_changelog_none "$source"; then
            excused=$((excused + 1))
            continue
        fi
        printf '    %-70s (%s)\n' "$subject" "$origin" >>"$offenders_file"
        offenders=$((offenders + 1))
    done <"$subjects_file"

    if [ "$offenders" -eq 0 ] && [ "$excused" -gt 0 ]; then
        echo "changelog.sh: $excused subject(s) would require a changelog fragment, and each"
        echo "  carries a 'Changelog: none' trailer saying it is not user-visible."
        return 0
    fi

    if [ "$offenders" -eq 0 ]; then
        echo "changelog.sh: nothing in $base..${head:-HEAD} requires a changelog fragment."
        return 0
    fi

    # Added, not modified. Editing an existing fragment — someone else's, or one
    # from an earlier unreleased pull request — is not this change's release
    # note, and counting it would let a typo fix satisfy the gate for a feature.
    while IFS= read -r file; do
        [ -n "$file" ] || continue
        fragment_type "$file" >/dev/null 2>&1 || continue
        # Read the worktree, not the indexed blob: locally the file is right
        # there, and in CI the checkout is of the head commit anyway. An empty
        # fragment is not a fragment.
        if [ -f "$file" ] && ! fragment_has_prose "$file"; then
            printf '    %s\n' "$file" >>"$empty_file"
            continue
        fi
        added=$((added + 1))
    done < <(git diff --no-ext-diff --no-textconv --name-only --diff-filter=A \
        "$base" "${head:-HEAD}" -- "$fragments/" 2>/dev/null || true)

    # Locally, a fragment that has been written but not yet committed counts.
    # `make gates` runs this, and a gate that tells you to create the file you
    # just created is one people learn to ignore. In CI `head` is a real commit
    # and the worktree is a clean checkout of it, so this finds nothing new.
    if [ -z "${head:-}" ]; then
        while IFS= read -r line; do
            [ -n "$line" ] || continue
            # Only new files: `A ` staged-added and `??` untracked. A modified
            # fragment is somebody else's note being edited, which is the same
            # reason the committed side filters on `--diff-filter=A`.
            case "$line" in
            'A '* | '??'*) ;;
            *) continue ;;
            esac
            file=${line#???}
            fragment_type "$file" >/dev/null 2>&1 || continue
            [ -f "$file" ] || continue
            fragment_has_prose "$file" || continue
            added=$((added + 1))
        done < <(git status --porcelain -- "$fragments/" 2>/dev/null || true)
    fi

    if [ "$added" -eq 0 ] && [ -s "$empty_file" ]; then
        fail "these fragment(s) were added but are empty:

$(cat "$empty_file")
  A fragment is the release note. Write what the change means for somebody
  upgrading — $fragments/README.md has the conventions."
    fi

    if [ "$added" -gt 0 ]; then
        echo "changelog.sh: $offenders subject(s) require a changelog fragment, $added added."
        return 0
    fi

    echo "changelog.sh: these subject(s) say this change is visible to somebody" >&2
    echo "  upgrading, and no fragment was added under $fragments/:" >&2
    echo >&2
    cat "$offenders_file" >&2

    {
        echo
        echo "  Add one with:"
        echo
        echo "      make changelog-new TYPE=fixed SLUG=short-description"
        echo
        echo "  and write what the change means for somebody upgrading — not what moved."
        echo "  $fragments/README.md has the format and the conventions."
        echo
        echo "  If it is genuinely not user-visible, say so in the subject. There is no"
        echo "  label and no opt-out checkbox for this, on purpose — .github/labels.yml"
        echo "  says why. The exemption is derived from the type and scope you write:"
        echo
        echo "      feat(spate-core): ...  ->  refactor(spate-core): ...  nothing user-facing moved"
        echo "      fix(spate-core): ...   ->  test(spate-core): ...      it only touched tests"
        echo "      feat(spate-core): ...  ->  feat(docs): ...            it only touched docs"
        echo
        echo "  For a fix to a bug that was never released, a 'Changelog: none' trailer on"
        echo "  the commit is the honest way out."
        echo
        echo "  The pull request title is what lands on main — this repository squashes with"
        echo "  the title as the subject — so the title is the one that has to be right."
    } >&2
    exit 1
}

# ---------------------------------------------------------------------------
# --new
# ---------------------------------------------------------------------------
cmd_new() {
    local type=${1:-} slug=${2:-} path

    [ -n "$type" ] && [ -n "$slug" ] ||
        fail "usage: ./scripts/changelog.sh --new <type> <slug>
  type is one of: $TYPES"

    case " $TYPES " in
    *" $type "*) ;;
    *) fail "'$type' is not a fragment type. The Keep a Changelog six are: $TYPES" ;;
    esac

    # `LC_ALL=C grep`, not a `case` glob. A bracket range in a glob is collated,
    # and under bash 3.2 — the version this targets, and what stock macOS ships
    # — `[!a-z0-9-]` in a UTF-8 locale accepts uppercase, so `BarUpper` passed
    # there and was rejected everywhere else. Nothing dangerous followed from it;
    # the point is that the check meant different things on different machines.
    if ! printf '%s' "$slug" | LC_ALL=C grep -qE '^[a-z0-9]([a-z0-9-]*[a-z0-9])?$'; then
        fail "'$slug' should be lowercase letters, digits and hyphens, starting and ending
  with one of the first two — it becomes a filename."
    fi
    path="$fragments/$slug.$type.md"
    [ -e "$path" ] && fail "$path already exists"

    mkdir -p "$fragments"
    cat >"$path" <<'TEMPLATE'
**A short bold lead-in** (`spate-crate`) — what this means for somebody
upgrading, in one to five sentences. Present tense, impersonal. Say what it
means, not what moved; the commit message already says what moved.

Delete this template text and write the entry. If the change is breaking, open
with `**Breaking:**`.
TEMPLATE
    echo "changelog.sh: wrote $path"
    echo "  Edit it, then commit it with your change. $fragments/README.md has the conventions."
}

# ---------------------------------------------------------------------------
# --build
# ---------------------------------------------------------------------------
cmd_build() {
    local version=${1:-} today previous range explicit n
    local block links contributors type file body pr found=0

    [ -n "$version" ] || fail "usage: ./scripts/changelog.sh --build <version>"
    [ -f "$changelog" ] || fail "$changelog not found"

    grep -qxF '## [Unreleased]' "$changelog" ||
        fail "no '## [Unreleased]' heading in $changelog — --build inserts the new release below it,
  so a release that removed it has to put it back, empty, before the next one."

    # Running the same version twice would insert a second section and rewrite
    # the links to match it, and the result reads like a released version that
    # has changed. A published version can never be replaced, so the number is
    # the one thing here that has to be right before anything else happens.
    grep -qF "## [$version]" "$changelog" &&
        fail "$changelog already has a '## [$version]' section. Pick the next version,
  or if the previous attempt failed part-way, undo it before running this again."

    # The Unreleased section has to be empty. Anything sitting under it would
    # otherwise fall through the insertion below and land *inside* the new
    # release, out of section order and dated into a version it was not part of
    # — and the heading it came from would be left empty, so nothing would look
    # wrong. There is one legitimate way to get text under there (the
    # no-fragments error a few lines down says to write a section by hand), so
    # this says what to do rather than only refusing.
    if awk '/^## \[Unreleased\]$/{f=1;next} f&&/^## /{exit} f&&/[^[:space:]]/{found=1} END{exit !found}' \
        "$changelog"; then
        fail "the '## [Unreleased]' section in $changelog is not empty.

  --build assembles from $fragments/, and anything written under that heading by
  hand would be swept into '## [$version]' below the link definitions rather than
  read as part of it. Move it into a fragment — one file per entry, typed by its
  Keep a Changelog section — and run this again."
    fi

    # A fragment has to say something; an empty one would ship as an empty
    # bullet. `--check` rejects these on the pull request, so reaching here means
    # one was committed before that gate existed, or edited to nothing since.
    for file in "$fragments"/*.md; do
        [ -e "$file" ] || continue
        fragment_type "$file" >/dev/null 2>&1 || continue
        fragment_has_prose "$file" ||
            fail "$file is empty. A fragment is the release note — write it, or delete the file."
    done

    today=$(date -u +%Y-%m-%d)
    previous=$(git tag --list 'v*' --sort=-v:refname | head -n 1)
    range="${previous:+$previous..}HEAD"

    scratch=$(mktemp -d)
    block="$scratch/block"
    links="$scratch/links"
    : >"$block"
    : >"$links"

    for type in $TYPES; do
        found=0
        for file in "$fragments"/*."$type".md; do
            [ -e "$file" ] || continue
            if [ "$found" -eq 0 ]; then
                # Sentence case for the section heading, as Keep a Changelog
                # spells them: Added, Changed, Fixed. The leading blank is
                # emitted here rather than after each entry so that entries
                # within a section stay adjacent — a blank line between list
                # items makes it a *loose* list, which renders every bullet
                # wrapped in its own paragraph and does not match 0.1.0.
                [ -s "$block" ] && printf '\n' >>"$block"
                printf '### %s%s\n\n' "$(printf '%s' "${type%"${type#?}"}" | tr '[:lower:]' '[:upper:]')" "${type#?}" >>"$block"
                found=1
            fi

            body=$(sed -e 's/[[:space:]]*$//' "$file")
            body=$(printf '%s\n' "$body" | sed -e '/./,$!d')

            # A `([#N])` written at the very *end* of the entry wins over the
            # derived one, and only its link definitions are emitted. That is
            # what lets an entry point at the pull request that actually did the
            # work when the fragment was written somewhere else — a retroactive
            # entry, or one restored after a release went out without it.
            #
            # Anchored to the end on purpose. Matching anywhere would read a
            # mid-sentence citation of some earlier pull request as this entry's
            # own reference, drop the derived link, and point the reader at the
            # wrong change — and citing a prior pull request mid-sentence is a
            # style the README itself models.
            #
            # Every `[#N]` the prose mentions gets a link definition regardless,
            # or a citation renders as literal text instead of a link. Only the
            # trailing one decides whether to skip deriving.
            printf '%s' "$body" | grep -oE '\[#[0-9]+\]' | tr -d '[]#' |
                while IFS= read -r n; do
                    [ -n "$n" ] && printf '[#%s]: %s/pull/%s\n' "$n" "$repo_url" "$n"
                done >>"$links" || true

            explicit=$(printf '%s' "$body" | tail -n 1 |
                sed -n 's/.*(\[#\([0-9][0-9]*\)\])[[:space:]]*$/\1/p')
            if [ -n "$explicit" ]; then
                : # already collected above
            else
                # Otherwise the number comes from the subject of the commit that
                # *added* the fragment — this repository's squash subjects end in
                # `(#NN)`, and every commit since v0.1.0 does. A fragment added by
                # a direct push has no number, and that renders without a link
                # rather than failing: the entry is still the point.
                pr=$(git log --diff-filter=A --format='%s' -- "$file" 2>/dev/null |
                    sed -n 's/.*(#\([0-9][0-9]*\))$/\1/p' | head -n 1)
                if [ -n "$pr" ]; then
                    # On its own line, never appended to the last one. A fragment
                    # may legitimately end in a fenced code block, and CommonMark
                    # allows only whitespace after a closing fence — appending
                    # there leaves the fence unclosed and the block swallows every
                    # section below it, including earlier releases.
                    body="$body
([#$pr])"
                    printf '[#%s]: %s/pull/%s\n' "$pr" "$repo_url" "$pr" >>"$links"
                fi
            fi

            # A fragment is prose, not a list item: the bullet and its
            # continuation indent are applied here so the file stays readable on
            # its own. Blank lines stay blank — indenting them to `"  "` leaves
            # trailing whitespace and makes the section a *loose* list, undoing
            # the adjacency the section heading above works to keep.
            printf '%s\n' "$(printf '%s\n' "$body" |
                sed -e '1s/^/- /' -e '2,$s/^\(.\)/  \1/')" >>"$block"
        done
    done

    [ -s "$block" ] || fail "no fragments in $fragments/ — nothing to release.
  Every user-visible change since $previous should have left one; if the release
  genuinely contains none, write the section by hand and say why in the commit."

    # Contributors over the whole range, not only the ones who left a fragment:
    # the CI, documentation and test work that never earns an entry is still the
    # release. Bots are filtered; they are not contributors and listing them
    # reads as padding.
    contributors=$(git shortlog -sn "$range" 2>/dev/null |
        sed -e 's/^[[:space:]]*[0-9][0-9]*[[:space:]]*//' |
        grep -v '\[bot\]$' || true)
    if [ -n "$contributors" ]; then
        {
            printf '\n### Contributors\n\n'
            printf '%s\n' "$contributors" | sed -e 's/^/- /'
        } >>"$block"
    fi

    if [ -s "$links" ]; then
        printf '\n' >>"$block"
        # `sort -u` over whole lines, then a numeric sort for the order. Doing
        # both at once with `-u -t'#' -k2 -n` compares only the numeric key, so
        # `[#031]` and `[#31]` collapse to one and a definition is dropped.
        sort -u "$links" | sort -t'#' -k2 -n >>"$block"
    fi

    # Trim trailing blank lines. Whichever section ended the block left one, and
    # the line the block is inserted above already supplies the separator — two
    # of them leave a double gap over the previous release. `$(cat)` drops every
    # trailing newline and the `printf` puts exactly one back.
    printf '%s\n' "$(cat "$block")" >"$block.trimmed"
    mv "$block.trimmed" "$block"

    # Insert the new release directly below the (emptied) Unreleased heading,
    # and rewrite the two link references at the foot of the file.
    VERSION="$version" TODAY="$today" BLOCK="$block" REPO="$repo_url" \
        awk '
        BEGIN { version = ENVIRON["VERSION"]; today = ENVIRON["TODAY"] }
        /^## \[Unreleased\]$/ {
            print
            print ""
            printf "## [%s] — %s\n\n", version, today
            while ((getline line < ENVIRON["BLOCK"]) > 0) print line
            inserted = 1
            next
        }
        /^\[Unreleased\]: / {
            printf "[Unreleased]: %s/compare/v%s...HEAD\n", ENVIRON["REPO"], version
            printf "[%s]: %s/releases/tag/v%s\n", version, ENVIRON["REPO"], version
            rewritten = 1
            next
        }
        { print }
        END {
            if (!inserted)  { print "changelog.sh: the Unreleased heading vanished mid-write" > "/dev/stderr"; exit 1 }
            if (!rewritten) { print "changelog.sh: no [Unreleased]: link reference to rewrite"  > "/dev/stderr"; exit 1 }
        }
    ' "$changelog" >"$scratch/changelog.new"

    # Everything that can fail happens before anything is written back.
    #
    # The removals used to run after the file had already been replaced, with
    # `git rm` as the last command of an `&&` list — so an uncommitted fragment
    # (`--new` and forgot to `git add`) aborted the loop under `set -e` with the
    # changelog rewritten, some fragments staged for deletion and others not, and
    # the duplicate-version guard then blocking the retry. `git rm --cached`
    # cannot fail that way, but the ordering is the real fix: prove the removals
    # first, then commit to the write.
    #
    # The scratch copy also means an awk failure leaves nothing behind. It used
    # to write `CHANGELOG.md.new` into the repository root, untracked and not
    # ignored.
    for type in $TYPES; do
        for file in "$fragments"/*."$type".md; do
            [ -e "$file" ] || continue
            git ls-files --error-unmatch "$file" >/dev/null 2>&1 ||
                fail "$file is not tracked. Commit it before assembling a release —
  a fragment that never reached git is not part of what is being released."
        done
    done

    mv "$scratch/changelog.new" "$changelog"

    for type in $TYPES; do
        for file in "$fragments"/*."$type".md; do
            [ -e "$file" ] && git rm --quiet --force "$file"
        done
    done

    echo "changelog.sh: wrote ## [$version] — $today into $changelog and consumed the fragments."
    echo "  Read what it wrote before committing — the assembly is mechanical, the release note is not."
}

# ---------------------------------------------------------------------------
# Dispatch.
# ---------------------------------------------------------------------------
self_test

case "${1:-}" in
--self-test)
    echo "changelog.sh: the classifier agrees with the table, the crate list is derived from"
    echo "  crates/, and no exempt scope names a crate."
    ;;
--check) cmd_check ;;
--new)
    shift
    cmd_new "$@"
    ;;
--build)
    shift
    cmd_build "$@"
    ;;
*)
    fail "usage: ./scripts/changelog.sh --check | --new <type> <slug> | --build <version> | --self-test"
    ;;
esac
