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
EXEMPT_SCOPES="ci docs examples benchmarks workspace website"

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
# exempt by being left out of a list. `benchmarks/` is a workspace member but
# sits outside `crates/`, so the non-crate area scope is excluded for free, and
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
    # `!` promotes any type, but only this far down: the scope axis has already
    # exempted `docs(workspace)!:`, which is a documentation rename and breaks
    # no API.
    [ -n "$bang" ] && return 0
    case "$type" in
    # Exactly cliff.toml's non-skipped, user-facing groups. `perf` is in because
    # that file already groups it under a "Performance" heading — the repository
    # decided perf was release-note-worthy before this script existed.
    feat | fix | perf) return 0 ;;
    docs | test | chore | style | ci | build | revert | refactor) return 1 ;;
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
    local failures=0 subject want got crate scope n=0 sample extracted

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
feat(spate-avro,benchmarks): decode datums straight into typed records|need
# --- crate-scoped, but the type says nobody upgrading cares ---
docs(spate-core): rewrite the module documentation|exempt
test(spate-kafka): retry the container suite once|exempt
chore(spate-core): tidy an import|exempt
refactor(spate-core): extract a helper|exempt
style(spate-kafka): rustfmt|exempt
ci(spate-core): pin an action|exempt
# --- a user-visible type, but the scope names no crate ---
feat(docs): give Spate a mark that works on a square canvas|exempt
feat(ci): a new job|exempt
feat(website): restyle the navigation|exempt
fix(benchmarks): make the run's host label opt-in|exempt
fix(ci,docs): lowercase the Pages project name|exempt
# --- the automation's own subjects, verbatim from its config ---
chore(workspace): bump the cargo-compatible group|exempt
chore(ci): bump mikepenz/action-junit-report|exempt
chore(docs): bump typescript in /website|exempt
chore(examples): bump a dependency|exempt
chore: release v0.2.0|exempt
# --- the breaking marker promotes an otherwise-exempt type ---
refactor(spate-core)!: rename a public trait|need
perf(spate-kafka)!: change the batch shape|need
feat(spate-s3)!: fence the split leases|need
# --- ...but only where the scope already reaches a crate ---
docs(workspace)!: migrate CLAUDE.md to AGENTS.md|exempt
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
    if [ "$n" -lt 9 ]; then
        echo "changelog.sh: derived $n crate scope(s) from crates/, expected at least 9" >&2
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

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s) — this script is wrong, not your change"
}

# The type embedded in a fragment filename, or nothing if the name is not a
# fragment. Keeping this a function rather than a glob is what lets the
# self-test assert that README.md is not mistaken for one.
fragment_type() {
    local base type
    base=$(basename "$1")
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

# ---------------------------------------------------------------------------
# --check
# ---------------------------------------------------------------------------
cmd_check() {
    local base="" head="" mode=structure candidate ref
    local subjects_file offenders_file trailers subject origin file sha
    local offenders=0 added=0

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
    : >"$subjects_file"
    : >"$offenders_file"

    [ -n "${PR_TITLE:-}" ] && printf '%s\t%s\n' "$PR_TITLE" "pull request title" >>"$subjects_file"
    while IFS=$'\t' read -r subject sha; do
        [ -n "$subject" ] && printf '%s\tcommit %s\n' "$subject" "$sha" >>"$subjects_file"
    done < <(git log --no-merges --format='%s%x09%h' "$base..${head:-HEAD}" 2>/dev/null || true)

    # Collected quietly and reported only if it turns out to matter. Classifying
    # straight to stderr would put an alarming list in front of a contributor
    # whose change goes on to satisfy the gate on the very next line.
    while IFS=$'\t' read -r subject origin; do
        [ -n "$subject" ] || continue
        if needs_entry "$subject"; then
            printf '    %-70s (%s)\n' "$subject" "$origin" >>"$offenders_file"
            offenders=$((offenders + 1))
        fi
    done <"$subjects_file"

    if [ "$offenders" -eq 0 ]; then
        echo "changelog.sh: nothing in $base..${head:-HEAD} requires a changelog fragment."
        return 0
    fi

    # Added, not modified. Editing an existing fragment — someone else's, or one
    # from an earlier unreleased pull request — is not this change's release
    # note, and counting it would let a typo fix satisfy the gate for a feature.
    while IFS= read -r file; do
        [ -n "$file" ] || continue
        fragment_type "$file" >/dev/null 2>&1 && added=$((added + 1))
    done < <(git diff --no-ext-diff --no-textconv --name-only --diff-filter=A \
        "$base" "${head:-HEAD}" -- "$fragments/" 2>/dev/null || true)

    if [ "$added" -gt 0 ]; then
        echo "changelog.sh: $offenders subject(s) require a changelog fragment, $added added."
        return 0
    fi

    # `git interpret-trailers --parse`, never a grep. The commit bodies in this
    # repository are dense prose, and lines like `Tests: the two fault-injection
    # knobs ...` start a sentence, not a trailer. git's own parser reads only the
    # final block of a message and gets this right; a regex does not.
    #
    # One message at a time, too, and that is not a stylistic choice:
    # `interpret-trailers` reads *its whole input* as a single message and takes
    # the trailers from the last block of it, so handing it a concatenated log
    # finds only the oldest commit's trailers and silently reports none for
    # every other commit in the range.
    trailers=""
    while IFS= read -r sha; do
        [ -n "$sha" ] || continue
        trailers="$trailers
$(git log -1 --format='%B' "$sha" | git interpret-trailers --parse 2>/dev/null || true)"
    done < <(git log --no-merges --format='%H' "$base..${head:-HEAD}" 2>/dev/null || true)

    if [ -n "${PR_BODY:-}" ]; then
        trailers="$trailers
$(printf '%s\n' "$PR_BODY" | git interpret-trailers --parse 2>/dev/null || true)"
    fi

    # Whole-pull-request semantics, which is the honest reading: the repository
    # squashes, so one pull request becomes one commit, and the trailer is a
    # statement about that commit.
    if grep -qiE '^Changelog:[[:space:]]*none[[:space:]]*$' <<<"$trailers"; then
        echo "changelog.sh: $offenders subject(s) would require a changelog fragment, and a"
        echo "  'Changelog: none' trailer says this one is not user-visible. Taken at its word."
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

    case "$slug" in
    *[!a-z0-9-]* | "" | -* | *-)
        fail "'$slug' should be lowercase letters, digits and hyphens — it becomes a filename"
        ;;
    esac

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
                # spells them: Added, Changed, Fixed.
                printf '### %s%s\n\n' "$(printf '%s' "${type%"${type#?}"}" | tr '[:lower:]' '[:upper:]')" "${type#?}" >>"$block"
                found=1
            fi

            body=$(sed -e 's/[[:space:]]*$//' "$file")
            body=$(printf '%s\n' "$body" | sed -e '/./,$!d')

            # A `([#N])` written into the fragment wins over the derived one,
            # and only its link definitions are emitted. That is what lets an
            # entry point at the pull request that actually did the work when
            # the fragment was written somewhere else — a retroactive entry, or
            # one restored after a release went out without it.
            explicit=$(printf '%s' "$body" | grep -oE '\(\[#[0-9]+\]\)' |
                grep -oE '[0-9]+' || true)
            if [ -n "$explicit" ]; then
                while IFS= read -r n; do
                    [ -n "$n" ] && printf '[#%s]: %s/pull/%s\n' "$n" "$repo_url" "$n" >>"$links"
                done <<<"$explicit"
            else
                # Otherwise the number comes from the subject of the commit that
                # *added* the fragment — this repository's squash subjects end in
                # `(#NN)`, and 12 of the 12 commits since v0.1.0 do. A fragment
                # added by a direct push has no number, and that renders without
                # a link rather than failing: the entry is still the point.
                pr=$(git log --diff-filter=A --format='%s' -- "$file" 2>/dev/null |
                    sed -n 's/.*(#\([0-9][0-9]*\))$/\1/p' | head -n 1)
                if [ -n "$pr" ]; then
                    body="$body ([#$pr])"
                    printf '[#%s]: %s/pull/%s\n' "$pr" "$repo_url" "$pr" >>"$links"
                fi
            fi

            # A fragment is prose, not a list item: the bullet and its
            # continuation indent are applied here so the file stays readable on
            # its own.
            printf '%s\n\n' "$(printf '%s\n' "$body" | sed -e '1s/^/- /' -e '2,$s/^/  /')" >>"$block"
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
            printf '### Contributors\n\n'
            printf '%s\n' "$contributors" | sed -e 's/^/- /'
            printf '\n'
        } >>"$block"
    fi

    if [ -s "$links" ]; then
        sort -u -t'#' -k2 -n "$links" >>"$block"
        printf '\n' >>"$block"
    fi

    # Insert the new release directly below the (emptied) Unreleased heading,
    # and rewrite the two link references at the foot of the file.
    VERSION="$version" TODAY="$today" PREVIOUS="${previous:-}" BLOCK="$block" REPO="$repo_url" \
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
    ' "$changelog" >"$changelog.new"

    mv "$changelog.new" "$changelog"

    for type in $TYPES; do
        for file in "$fragments"/*."$type".md; do
            [ -e "$file" ] && git rm --quiet "$file"
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
