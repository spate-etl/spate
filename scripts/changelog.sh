#!/usr/bin/env bash
#
# The changelog fragment tool: adds fragments, checks a change carries one, and
# assembles them into CHANGELOG.md at release. `changelog.d/README.md` states
# the format and the policy.
#
# The conventions follow towncrier.
#
# Usage:
#   ./scripts/changelog.sh --check              # the gate (reads env, see below)
#   ./scripts/changelog.sh --new <type> <slug>  # scaffold a fragment
#   ./scripts/changelog.sh --build <version>    # assemble, at release
#   ./scripts/changelog.sh --notes <version>    # one version's section, on stdout
#   ./scripts/changelog.sh --self-test          # the classifier, alone
#
# Environment (--check only; all optional, all set by ci.yml):
#   EVENT_NAME  github.event_name
#   BASE_SHA    github.event.pull_request.base.sha  (pull_request only)
#   HEAD_SHA    github.event.pull_request.head.sha  (pull_request only)
#   PR_TITLE    github.event.pull_request.title     (pull_request only)
#   PR_BODY     github.event.pull_request.body      (pull_request only)
#
# PR_TITLE and PR_BODY are free text somebody else typed: matched against, never
# evaluated. A `pull_request` run executes the pull request's own copy of this
# script. See the note in scripts/ci-changes.sh.
#
# Targets `bash` 3.2, the version stock macOS ships as /bin/bash: no
# associative arrays, no `mapfile`, no `${var,,}`, and every array expansion
# guarded, because `"${arr[@]}"` on an empty array is an unbound-variable error
# under `set -u` there.
set -euo pipefail

cd "$(dirname "$0")/.."

fragments=changelog.d
changelog=CHANGELOG.md
repo_url=https://github.com/spate-etl/spate

# The Keep a Changelog six, in the order a release renders them. A breaking
# change is not a seventh type; it is a `**Breaking:**` marker on one of these.
TYPES="added changed deprecated removed fixed security"

# The scopes that do not reach a crate. Typed out rather than derived from the
# `area:` labels in .github/labels.yml: that list also carries `supply-chain`,
# and a `fix(supply-chain):` closing an advisory is a release note.
#
# `bench` covers the unpublished `spate-bench` harness and the `benches/`
# targets inside published crates: neither reaches a published crate's surface.
EXEMPT_SCOPES="ci docs examples bench workspace website"

fail() {
    echo "changelog.sh: $1" >&2
    exit 1
}

# A scratch directory, not a list of files. Declared here rather than `local`
# to whichever function makes it: an EXIT trap runs after that function has
# returned, and `set -u` would turn the cleanup itself into the error.
#
# `return 0` matters on bash 3.2: with `scratch` empty the `[ -n ]` test is the
# last command in the function, so the trap exits non-zero and takes the whole
# script's status with it, printing success and returning 1.
scratch=""
cleanup() {
    [ -n "$scratch" ] && rm -rf "$scratch"
    return 0
}
trap cleanup EXIT

# The crate scopes are derived, never typed: a tenth crate must not become
# exempt by being left out of a list. Only `crates/*` is read, so no toolchain
# is needed.
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
# the crate list and nothing else, so --self-test can drive it with no git
# state at all.
#
# An ignore-list on *both* axes. Stated the other way round ("required iff the
# scope names a crate") it fails *open*.
needs_entry() {
    local subject=$1 type scopes bang scope reaches_crate=0 known

    # type(scope)!: text. The scope and the bang are optional, the text is not.
    [[ "$subject" =~ ^([a-zA-Z]+)(\(([^\)]*)\))?(!)?:[[:space:]]*[^[:space:]] ]] || return 0
    type=$(printf '%s' "${BASH_REMATCH[1]}" | tr '[:upper:]' '[:lower:]')
    scopes="${BASH_REMATCH[3]}"
    bang="${BASH_REMATCH[4]}"

    # `!` decides on its own, before either axis.
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
    feat | fix | perf | revert | build) return 0 ;;
    docs | test | chore | style | ci | refactor) return 1 ;;
    *) return 0 ;;
    esac
}

# ---------------------------------------------------------------------------
# Self-test. Runs inline on every invocation as well as under --self-test.
# ---------------------------------------------------------------------------
self_test() {
    local failures=0 subject want got crate scope n=0 sample extracted probe probe_dir

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
# `docs(workspace)!:` is real history (c6a7a5c) and it carried a BREAKING
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

    # The table above stays green if `crate_scopes` returns nothing: every
    # scope would become "unrecognized", every case would still classify as
    # `need`, and the classifier would have silently stopped reading the tree.
    while IFS= read -r crate; do
        [ -n "$crate" ] || continue
        n=$((n + 1))
        if ! needs_entry "feat($crate): x"; then
            echo "changelog.sh: 'feat($crate): x' is exempt, so crates/ is not being read" >&2
            failures=$((failures + 1))
        fi
        if needs_entry "docs($crate): x"; then
            echo "changelog.sh: 'docs($crate): x' needs a fragment, so the type axis is dead" >&2
            failures=$((failures + 1))
        fi
    done < <(crate_scopes)
    # Zero, not a count: a hard-coded floor would fail `--check` for everybody
    # the day a crate is retired.
    if [ "$n" -lt 1 ]; then
        echo "changelog.sh: no crate scopes derived from crates/, so the extractor has gone" >&2
        echo "  blind, and every scope is now unrecognized rather than checked." >&2
        failures=$((failures + 1))
    fi

    for scope in $EXEMPT_SCOPES; do
        if needs_entry "feat($scope): x"; then
            echo "changelog.sh: '$scope' is in EXEMPT_SCOPES but still requires a fragment" >&2
            failures=$((failures + 1))
        fi
        # No exempt scope may name a crate.
        if [ -d "crates/$scope" ]; then
            echo "changelog.sh: EXEMPT_SCOPES names 'crates/$scope', a crate" >&2
            failures=$((failures + 1))
        fi
    done

    # The subject half of the reference lookup. `-` is an empty result, where
    # the subject carries no number and `--build` asks the API for one.
    while IFS='|' read -r subject want; do
        case "$subject" in '' | '#'*) continue ;; esac
        if [ "$want" = '-' ]; then want=''; fi
        got=$(pr_from_subject "$subject")
        if [ "$got" != "$want" ]; then
            echo "changelog.sh: pr_from_subject: '$subject' -> '$got', expected '$want'" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
# --- a squash subject, which GitHub numbers ---
fix(spate-core): enforce max_pending_batches at the poll boundary (#200)|200
feat(spate-avro,bench): decode datums into typed records (#31)|31
# --- a rebase merge appends nothing. All three are real history. ---
fix(spate-kafka): count logical coordinator links toward broker_up|-
refactor(examples)!: name the JSON example for what it teaches|-
chore: release v0.2.0|-
# --- a citation mid-subject is not the merge's own number ---
docs(workspace): supersede (#12) with a record of its own|-
fix(spate-s3): restore what (#42) changed, and pin the ETag|-
# --- the last one wins when the subject ends in two ---
fix(spate-core): revert (#41) (#57)|57
TABLE

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
        echo "changelog.sh: fragment_type accepted a nested path as type '$extracted':" >&2
        echo "  --build globs one level, so the gate would pass and the release would omit it" >&2
        failures=$((failures + 1))
    fi

    # A fragment has to say something.
    probe="$(mktemp -d)/probe.fixed.md"
    : >"$probe"
    if fragment_has_prose "$probe"; then
        echo "changelog.sh: an empty file counts as a fragment, so the gate is fail-open" >&2
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

    # The section extractor behind --notes, against the two boundaries that
    # exist: a following heading, and the link foot that ends the last section
    # instead of one. The subshells keep a `fail` inside the extractor from
    # ending the self-test instead of counting.
    probe_dir=$(mktemp -d)
    cat >"$probe_dir/changelog.md" <<'FIXTURE'
# Changelog

## [Unreleased]

## [0.3.0] — 2026-09-01

### Added

- **A thing** (`spate-core`) — what it means.
  ([#301])

[#301]: https://github.com/spate-etl/spate/pull/301

## [0.2.0] — 2026-08-22

### Fixed

- An older thing.

[Unreleased]: https://github.com/spate-etl/spate/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/spate-etl/spate/releases/tag/v0.3.0
[0.2.0]: https://github.com/spate-etl/spate/releases/tag/v0.2.0
FIXTURE
    cat >"$probe_dir/want" <<'FIXTURE'
### Added

- **A thing** (`spate-core`) — what it means.
  ([#301])

[#301]: https://github.com/spate-etl/spate/pull/301
FIXTURE
    if ! (section_notes "$probe_dir/changelog.md" "0.3.0" >"$probe_dir/got" 2>/dev/null); then
        echo "changelog.sh: section_notes refused the 0.3.0 fixture" >&2
        failures=$((failures + 1))
    elif ! diff -u "$probe_dir/want" "$probe_dir/got" >&2; then
        echo "changelog.sh: the 0.3.0 notes drifted from the expected output above" >&2
        failures=$((failures + 1))
    fi
    cat >"$probe_dir/want" <<'FIXTURE'
### Fixed

- An older thing.
FIXTURE
    if ! (section_notes "$probe_dir/changelog.md" "0.2.0" >"$probe_dir/got" 2>/dev/null); then
        echo "changelog.sh: section_notes refused the last section" >&2
        failures=$((failures + 1))
    elif ! diff -u "$probe_dir/want" "$probe_dir/got" >&2; then
        echo "changelog.sh: the last section leaked the link foot into the notes; diff above" >&2
        failures=$((failures + 1))
    fi
    if (section_notes "$probe_dir/changelog.md" "9.9.9" >/dev/null 2>&1); then
        echo "changelog.sh: section_notes invented notes for a version the file lacks" >&2
        failures=$((failures + 1))
    fi
    rm -rf "$probe_dir"

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s). This script is wrong, not your change"
}

# The type embedded in a fragment filename, or nothing if the name is not a
# fragment.
#
# One level exactly: `--build` globs one level, so accepting a nested path
# would let `--check` pass on a fragment the release cannot see.
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

# A fragment has to say something. An empty or whitespace-only file would
# satisfy the gate and ship as an empty bullet.
fragment_has_prose() {
    [ -s "$1" ] || return 1
    grep -qE '[^[:space:]]' "$1"
}

# Does the message this subject came from carry `Changelog: none`?
#
# `git interpret-trailers --parse`, never a grep: a body line like `Tests: the
# two fault-injection knobs ...` starts a sentence, not a trailer.
#
# One message at a time: interpret-trailers takes the trailers from the last
# block of its whole input, so a concatenated log reports none for every commit
# but the oldest.
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

# The body of one release's section: everything between its heading and the
# next one, the heading itself excluded. The slice is self-contained because
# `--build` writes each section's `[#N]` definitions inside it; the
# definitions at the foot of the file belong to the headings, which the slice
# drops. The last section is followed by that foot rather than a heading, so
# the `[Unreleased]:` line terminates a section too.
section_notes() {
    local file=$1 version=$2 body
    grep -qF "## [$version] " "$file" ||
        fail "no '## [$version]' section in $file. --notes reads what --build wrote, so the
  release is assembled first."
    body=$(awk -v heading="## [$version] " '
        index($0, heading) == 1 { in_section = 1; next }
        in_section && (/^## / || /^\[Unreleased\]: /) { exit }
        in_section { print }
    ' "$file")
    body=$(printf '%s\n' "$body" | sed -e '/./,$!d')
    [ -n "$body" ] || fail "the '## [$version]' section in $file is empty"
    printf '%s\n' "$body"
}

# ---------------------------------------------------------------------------
# --check
# ---------------------------------------------------------------------------
cmd_check() {
    local base="" head="" mode=structure candidate ref
    local subjects_file offenders_file empty_file subject origin source file sha line
    local offenders=0 added=0 excused=0

    [ -d "$fragments" ] || fail "$fragments/ not found. It holds the changelog fragments"
    [ -f "$fragments/README.md" ] ||
        fail "$fragments/README.md not found. It states the format and, less obviously,
  is what keeps the directory in git once a release has consumed every fragment."

    case "${EVENT_NAME:-}" in
    pull_request)
        # Against the *merge base*, not `base.sha`. A missing merge base is a
        # hard failure here, where ci-changes.sh falls open: "demand a fragment"
        # is not something the contributor can act on. It means the checkout
        # lost `fetch-depth: 0`.
        if ! base=$(git merge-base "${BASE_SHA:-}" "${HEAD_SHA:-}" 2>/dev/null) || [ -z "$base" ]; then
            fail "no merge base for ${BASE_SHA:-?}..${HEAD_SHA:-?}. Does the checkout still set fetch-depth: 0?"
        fi
        head="${HEAD_SHA}"
        mode=require
        ;;
    "")
        # A laptop. Orient against the obvious upstream so `make gates` answers
        # the question before you push, and fall back to structure-only.
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
        # push, merge_group, schedule, workflow_dispatch. The requirement was
        # proven on the pull request, so this is structure only and still
        # reports success.
        ;;
    esac

    # Structure-only must never be the answer on a pull request. A workflow
    # edit collapsing the four steps into `- run: make ci-lint` would call this
    # with no `EVENT_NAME`, in a shallow checkout, where it takes the laptop arm
    # and reports success having evaluated nothing.
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
    # the branch can only ever *add* the requirement. The title is
    # authoritative, since this repository squashes with it as the commit
    # subject, but title-only fails open: a pull request titled `chore: tidy
    # up` carrying a `feat(spate-core)` commit would escape.
    #
    # `--no-merges`, because `Merge branch 'main' into x` is unparseable and an
    # unparseable subject is not exempt.
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

    # A trailer excuses the message it is written on. In the pull request
    # **body** that is the whole pull request, because the body is what the
    # squash commit carries; on a **commit** it is that commit's subject only.
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

    # Added, not modified. Editing an existing fragment is not this change's
    # release note, and counting it would let a typo fix satisfy the gate.
    while IFS= read -r file; do
        [ -n "$file" ] || continue
        fragment_type "$file" >/dev/null 2>&1 || continue
        # Read the worktree, not the indexed blob: locally the file is there,
        # and in CI the checkout is of the head commit anyway.
        if [ -f "$file" ] && ! fragment_has_prose "$file"; then
            printf '    %s\n' "$file" >>"$empty_file"
            continue
        fi
        added=$((added + 1))
    done < <(git diff --no-ext-diff --no-textconv --name-only --diff-filter=A \
        "$base" "${head:-HEAD}" -- "$fragments/" 2>/dev/null || true)

    # Locally, a fragment written but not yet committed counts. In CI the
    # worktree is a clean checkout.
    if [ -z "${head:-}" ]; then
        while IFS= read -r line; do
            [ -n "$line" ] || continue
            # Only new files: `A ` staged-added and `??` untracked. A modified
            # fragment is somebody else's note being edited.
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
  upgrading. $fragments/README.md has the conventions."
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
        echo "  and write what the change means for somebody upgrading, not what moved."
        echo "  $fragments/README.md has the format and the conventions."
        echo
        echo "  If it is not user-visible, say so in the subject. There is no label and"
        echo "  no opt-out checkbox for this, and .github/labels.yml says why. The exemption"
        echo "  is derived from the type and scope you write:"
        echo
        echo "      feat(spate-core): ...  ->  refactor(spate-core): ...  nothing user-facing moved"
        echo "      fix(spate-core): ...   ->  test(spate-core): ...      it only touched tests"
        echo "      feat(spate-core): ...  ->  feat(docs): ...            it only touched docs"
        echo
        echo "  For a fix to a bug that was never released, put a 'Changelog: none'"
        echo "  trailer on the commit."
        echo
        echo "  The pull request title is what lands on main, since this repository squashes"
        echo "  with the title as the subject, so the title is the one that has to be right."
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
    # and under bash 3.2, the version this targets and what stock macOS ships,
    # `[!a-z0-9-]` in a UTF-8 locale accepts uppercase, so `BarUpper` passed
    # there and was rejected everywhere else.
    if ! printf '%s' "$slug" | LC_ALL=C grep -qE '^[a-z0-9]([a-z0-9-]*[a-z0-9])?$'; then
        fail "'$slug' should be lowercase letters, digits and hyphens, starting and ending
  with one of the first two. It becomes a filename."
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

# The pull request number GitHub appends to a squash subject, or nothing. A
# `(#12)` written mid-subject cites another pull request and is not read.
pr_from_subject() {
    printf '%s\n' "$1" | sed -n 's/.*(#\([0-9][0-9]*\))$/\1/p'
}

# What an entry carrying no reference of its own points at. That is the pull
# request that merged the fragment, or the commit that added it.
#
# Three sources in order. A squash subject ends in `(#N)`. A rebase merge
# appends nothing, so the adding commit is looked up on the API next. A commit
# that reached `main` outside a pull request links to itself.
#
# Prints `pr <number>` or `commit <sha>`. A fragment with no history, written
# but not yet committed, prints nothing.
#
# Called from `--build` alone. `--check` runs on every pull request, forks
# included, and makes no network call.
fragment_reference() {
    local file=$1 sha subject pr

    sha=$(git log --diff-filter=A --format='%H' -- "$file" 2>/dev/null | head -n 1)
    [ -n "$sha" ] || return 0

    subject=$(git log --diff-filter=A --format='%s' -- "$file" 2>/dev/null | head -n 1)
    pr=$(pr_from_subject "$subject")
    if [ -n "$pr" ]; then
        printf 'pr %s\n' "$pr"
        return 0
    fi

    # Merged pull requests only, and the first of them. A commit can also be
    # associated with one that never landed.
    if command -v gh >/dev/null 2>&1; then
        pr=$(gh api "repos/${repo_url#https://github.com/}/commits/$sha/pulls" \
            --jq 'map(select(.merged_at)) | first | .number // empty' 2>/dev/null || true)
        if [ -n "$pr" ]; then
            printf 'pr %s\n' "$pr"
            return 0
        fi
    fi

    printf 'commit %s\n' "$sha"
}

# ---------------------------------------------------------------------------
# --build
# ---------------------------------------------------------------------------
cmd_build() {
    local version=${1:-} today previous range explicit n
    local block links contributors type file body pr reference sha found=0

    [ -n "$version" ] || fail "usage: ./scripts/changelog.sh --build <version>"
    [ -f "$changelog" ] || fail "$changelog not found"

    grep -qxF '## [Unreleased]' "$changelog" ||
        fail "no '## [Unreleased]' heading in $changelog. --build inserts the new release below it,
  so a release that removed it has to put it back, empty, before the next one."

    # Running the same version twice would insert a second section and rewrite
    # the links to match it.
    grep -qF "## [$version]" "$changelog" &&
        fail "$changelog already has a '## [$version]' section. Pick the next version,
  or if the previous attempt failed part-way, undo it before running this again."

    # The Unreleased section has to be empty. Anything under it would fall
    # through the insertion below into the new release, out of section order and
    # dated into a version it was not part of, leaving its own heading empty.
    if awk '/^## \[Unreleased\]$/{f=1;next} f&&/^## /{exit} f&&/[^[:space:]]/{found=1} END{exit !found}' \
        "$changelog"; then
        fail "the '## [Unreleased]' section in $changelog is not empty.

  --build assembles from $fragments/, and anything written under that heading by
  hand would be swept into '## [$version]' below the link definitions rather than
  read as part of it. Move it into a fragment, one file per entry, typed by its
  Keep a Changelog section, and run this again."
    fi

    # A fragment has to say something; an empty one would ship as an empty
    # bullet. `--check` rejects these on the pull request.
    for file in "$fragments"/*.md; do
        [ -e "$file" ] || continue
        fragment_type "$file" >/dev/null 2>&1 || continue
        fragment_has_prose "$file" ||
            fail "$file is empty. A fragment is the release note: write it, or delete the file."
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
                # Sentence case for the heading, as Keep a Changelog spells
                # them. The leading blank is emitted here rather than after each
                # entry: a blank line between list items makes it a *loose*
                # list, which renders every bullet in its own paragraph.
                [ -s "$block" ] && printf '\n' >>"$block"
                printf '### %s%s\n\n' "$(printf '%s' "${type%"${type#?}"}" | tr '[:lower:]' '[:upper:]')" "${type#?}" >>"$block"
                found=1
            fi

            body=$(sed -e 's/[[:space:]]*$//' "$file")
            body=$(printf '%s\n' "$body" | sed -e '/./,$!d')

            # A `([#N])` at the very *end* of the entry wins over the derived
            # one, for an entry pointing at the pull request that did the work.
            #
            # Anchored to the end: matching anywhere would read a mid-sentence
            # citation of an earlier pull request as this entry's reference.
            #
            # Every `[#N]` in the prose gets a link definition regardless, or it
            # renders as literal text. Only the trailing one skips deriving.
            printf '%s' "$body" | grep -oE '\[#[0-9]+\]' | tr -d '[]#' |
                while IFS= read -r n; do
                    [ -n "$n" ] && printf '[#%s]: %s/pull/%s\n' "$n" "$repo_url" "$n"
                done >>"$links" || true

            explicit=$(printf '%s' "$body" | tail -n 1 |
                sed -n 's/.*(\[#\([0-9][0-9]*\)\])[[:space:]]*$/\1/p')
            if [ -n "$explicit" ]; then
                : # already collected above
            else
                # Otherwise it comes from the commit that *added* the fragment.
                #
                # Each form goes on its own line, never appended to the last
                # one. A fragment may end in a fenced code block, and CommonMark
                # allows only whitespace after a closing fence: appending leaves
                # it unclosed and swallows every section below.
                reference=$(fragment_reference "$file")
                case "$reference" in
                "pr "*)
                    pr=${reference#pr }
                    body="$body
([#$pr])"
                    printf '[#%s]: %s/pull/%s\n' "$pr" "$repo_url" "$pr" >>"$links"
                    ;;
                "commit "*)
                    # An inline link. The definition list holds `[#N]` alone
                    # and sorts on that number.
                    sha=${reference#commit }
                    body="$body
([\`${sha:0:7}\`]($repo_url/commit/$sha))"
                    ;;
                esac
            fi

            # The bullet and continuation indent are applied here so the file
            # stays readable on its own. Blank lines stay blank: indenting them
            # leaves trailing whitespace and makes the section a *loose* list.
            printf '%s\n' "$(printf '%s\n' "$body" |
                sed -e '1s/^/- /' -e '2,$s/^\(.\)/  \1/')" >>"$block"
        done
    done

    [ -s "$block" ] || fail "no fragments in $fragments/, so nothing to release.
  Every user-visible change since $previous should have left one; if the release
  genuinely contains none, write the section by hand and say why in the commit."

    # Contributors over the whole range, not only the ones who left a fragment.
    # Bots are filtered.
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

    # Trim trailing blank lines: the insertion line already supplies the
    # separator. `$(cat)` drops every trailing newline; `printf` puts one back.
    printf '%s\n' "$(cat "$block")" >"$block.trimmed"
    mv "$block.trimmed" "$block"

    # Insert the new release below the Unreleased heading, and rewrite the two
    # link references at the foot of the file.
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

    # Everything that can fail happens before anything is written back: with
    # the removals after the rewrite, an uncommitted fragment aborts the loop
    # under `set -e` with the changelog rewritten and fragments half-staged.
    for type in $TYPES; do
        for file in "$fragments"/*."$type".md; do
            [ -e "$file" ] || continue
            git ls-files --error-unmatch "$file" >/dev/null 2>&1 ||
                fail "$file is not tracked. Commit it before assembling a release:
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
    echo "  Read what it wrote before committing: the assembly is mechanical, the release note is not."
}

# ---------------------------------------------------------------------------
# --notes
# ---------------------------------------------------------------------------
# One version's section on stdout, for the GitHub release body. The heading is
# dropped because the release title already carries the version.
cmd_notes() {
    local version=${1:-}
    [ -n "$version" ] || fail "usage: ./scripts/changelog.sh --notes <version>"
    [ -f "$changelog" ] || fail "$changelog not found"
    section_notes "$changelog" "$version"
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
--notes)
    shift
    cmd_notes "$@"
    ;;
*)
    fail "usage: ./scripts/changelog.sh --check | --new <type> <slug> | --build <version> | --notes <version> | --self-test"
    ;;
esac
