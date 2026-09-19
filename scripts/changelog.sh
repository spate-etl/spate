#!/usr/bin/env bash
#
# The changelog release assembler: gathers the fragments into CHANGELOG.md at
# release, and prints one version's section. `changelog.d/README.md` states the
# format and the policy, and `cargo xtask tidy changelog` is the gate.
#
# The conventions follow towncrier.
#
# Usage:
#   ./scripts/changelog.sh --build <version>    # assemble, at release
#   ./scripts/changelog.sh --notes <version>    # one version's section, on stdout
#   ./scripts/changelog.sh --self-test          # the parsers, alone
#
# Runs on `bash` 3.2 and later: no associative arrays, no `mapfile`, no
# `${var,,}`, and every array expansion guarded, because `"${arr[@]}"` on an
# empty array is an unbound-variable error under `set -u` there.
set -euo pipefail

cd "$(dirname "$0")/.."

fragments=changelog.d
changelog=CHANGELOG.md
repo_url=https://github.com/spate-etl/spate

# The Keep a Changelog six, in the order a release renders them. A breaking
# change is not a seventh type; it is a `**Breaking:**` marker on one of these.
TYPES="added changed deprecated removed fixed security"

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

# ---------------------------------------------------------------------------
# Self-test. Runs inline on every invocation as well as under --self-test.
# ---------------------------------------------------------------------------
self_test() {
    local failures=0 subject want got sample extracted probe probe_dir

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
    if ! (section_notes "$probe_dir/changelog.md" "0.3.0" >"$probe_dir/got"); then
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
    if ! (section_notes "$probe_dir/changelog.md" "0.2.0" >"$probe_dir/got"); then
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

    # A hand-edited section may quote a heading or the link foot inside a
    # fence; the slice must carry it as content rather than stop there.
    cat >"$probe_dir/fenced.md" <<'FIXTURE'
## [Unreleased]

## [0.3.0] — 2026-09-01

### Changed

- **The heading writer** (`spate-core`) — emits this shape:

  ```markdown
## [Unreleased]
[Unreleased]: quoted-inside-a-fence
  ```

  and keeps going.
  ([#302])

[#302]: https://github.com/spate-etl/spate/pull/302

## [0.2.0] — 2026-08-22

[Unreleased]: https://github.com/spate-etl/spate/compare/v0.3.0...HEAD
FIXTURE
    cat >"$probe_dir/want" <<'FIXTURE'
### Changed

- **The heading writer** (`spate-core`) — emits this shape:

  ```markdown
## [Unreleased]
[Unreleased]: quoted-inside-a-fence
  ```

  and keeps going.
  ([#302])

[#302]: https://github.com/spate-etl/spate/pull/302
FIXTURE
    if ! (section_notes "$probe_dir/fenced.md" "0.3.0" >"$probe_dir/got"); then
        echo "changelog.sh: section_notes stopped at a boundary quoted inside a fence" >&2
        failures=$((failures + 1))
    elif ! diff -u "$probe_dir/want" "$probe_dir/got" >&2; then
        echo "changelog.sh: the fenced section drifted from the expected output above" >&2
        failures=$((failures + 1))
    fi

    # Two headings for one version is the part-finished --build state; the
    # slice must refuse it rather than splice the two bodies.
    cat >"$probe_dir/dup.md" <<'FIXTURE'
## [0.5.0] — 2026-09-01

- One body.

## [0.5.0] — 2026-09-02

- Another body.
FIXTURE
    if (section_notes "$probe_dir/dup.md" "0.5.0" >/dev/null 2>&1); then
        echo "changelog.sh: section_notes spliced two sections carrying the same version" >&2
        failures=$((failures + 1))
    fi

    # A reference used with no definition inside the slice renders as
    # literal text on the release page.
    cat >"$probe_dir/undef.md" <<'FIXTURE'
## [0.6.0] — 2026-09-01

- A thing. ([#9])
FIXTURE
    if (section_notes "$probe_dir/undef.md" "0.6.0" >/dev/null 2>&1); then
        echo "changelog.sh: section_notes passed a reference with no definition in the slice" >&2
        failures=$((failures + 1))
    fi
    rm -rf "$probe_dir"

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s). This script is wrong, not your change"
}

# The type embedded in a fragment filename, or nothing if the name is not a
# fragment.
#
# One level exactly: `--build` globs one level, so accepting a nested path
# would let the gate pass on a fragment the release cannot see.
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

# The body of one release's section: everything between its heading and the
# next one, the heading itself excluded. The slice is self-contained because
# `--build` writes each section's `[#N]` definitions inside it; the
# definitions at the foot of the file belong to the headings, which the slice
# drops. The last section is followed by that foot rather than a heading, so
# the `[Unreleased]:` line terminates a section too.
#
# Fenced code is opaque: a heading-shaped or foot-shaped line inside a fence
# is content, and a hand-edited section may quote one. Both fence kinds
# toggle one state, so a fence of one kind holding the other kind's marker at
# column zero is not modelled. A second heading for the same version is the
# part-finished --build state and is refused rather than spliced.
section_notes() {
    local file=$1 version=$2 body status=0 refs n
    body=$(awk -v heading="## [$version] " '
        /^ {0,3}(```|~~~)/ {
            if (in_section) print
            fence = !fence
            next
        }
        fence {
            if (in_section) print
            next
        }
        index($0, heading) == 1 {
            if (found) exit 3
            found = 1
            in_section = 1
            next
        }
        in_section && (/^## / || /^\[Unreleased\]: /) { in_section = 0; next }
        in_section { print }
        END { if (!found) exit 2 }
    ' "$file") || status=$?
    case "$status" in
    0) ;;
    2) fail "no '## [$version]' section in $file. --notes reads what --build wrote, so the
  release is assembled first." ;;
    3) fail "two '## [$version]' headings in $file. A part-finished --build has to be undone
  before its section can be read." ;;
    *) fail "the section scan of $file failed ($status)" ;;
    esac
    body=$(printf '%s\n' "$body" | sed -e '/./,$!d')
    [ -n "$body" ] || fail "the '## [$version]' section in $file is empty"

    # Every reference the slice uses must be defined inside it, or the
    # release body renders the literal text.
    refs=$(printf '%s\n' "$body" | grep -oE '\[#[0-9]+\]' | tr -d '[]#' | sort -u) || true
    for n in $refs; do
        printf '%s\n' "$body" | grep -qE "^\[#$n\]: " ||
            fail "the '## [$version]' section uses [#$n] with no definition in the section"
    done

    printf '%s\n' "$body"
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
# Called from `--build` alone. The gate runs on every pull request, forks
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
    #
    # On an HTTP error `gh api` exits non-zero and prints the response body
    # to stdout, so the answer is used only when the call succeeded and it is
    # a number. A commit the API does not know (assembled locally, never
    # pushed) takes the commit link below; any other failure aborts, because
    # --build runs unattended and a bad token here would otherwise turn every
    # derived reference into a commit link with nothing saying so.
    if command -v gh >/dev/null 2>&1; then
        local status=0
        pr=$(gh api "repos/${repo_url#https://github.com/}/commits/$sha/pulls" \
            --jq 'map(select(.merged_at)) | first | .number // empty' \
            2>"$scratch/gh-stderr") || status=$?
        if [ "$status" -eq 0 ]; then
            case "$pr" in
            '') ;;
            *[!0-9]*)
                fail "the pull-request lookup for ${sha:0:12} answered with something that is
  not a number: $pr"
                ;;
            *)
                printf 'pr %s\n' "$pr"
                return 0
                ;;
            esac
        else
            case "$pr" in
            *'No commit found'* | *'"status": "422"'* | *'"status":"422"'*) ;;
            *)
                fail "the pull-request lookup for ${sha:0:12} failed rather than answering:
  $pr $(cat "$scratch/gh-stderr" 2>/dev/null)
  Fix the token or the network and run --build again; falling back to a
  commit link here would look identical to a commit that has no pull request."
                ;;
            esac
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
    # bullet. The gate rejects these on the pull request.
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
    echo "changelog.sh: the fragment-name parser, the reference lookup and the section"
    echo "  extractor agree with their tables."
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
    fail "usage: ./scripts/changelog.sh --build <version> | --notes <version> | --self-test"
    ;;
esac
