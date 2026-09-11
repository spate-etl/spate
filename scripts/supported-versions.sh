#!/usr/bin/env bash
#
# Holds a supported-versions table to the servers CI pins.
#
# A connector page states a support guarantee; `ci/<service>/` states what CI
# runs. This asserts that every version the table names is a release line the
# service still pins, so the guarantee cannot outlive the lane under it.
#
# Nothing here names a service or a page. A service opts in by listing its
# page(s) in `ci/<service>/DOCS`, one path per line; a service without that file
# is skipped.
#
# The rule is a subset, not an equality: every number in the table must be
# pinned, and a lane may exist without appearing. That is what lets a row read
# "Newest stable release" with no version in it, and it is why a Dependabot bump
# never fails here. Moving the `stable` lane adds a line to the pinned set and
# removes none. Moving an LTS line removes the number the table names, which is
# the case this exists to catch.
#
# Usage:
#   ./scripts/supported-versions.sh --check      # the gate
#   ./scripts/supported-versions.sh --self-test  # the parsers, alone
#
# Runs on `bash` 3.2 and later: no associative arrays, no `mapfile`.
set -euo pipefail

cd "$(dirname "$0")/.."

# The tree of pinned images, and the heading a support table sits under. Both
# overridden by `--self-test`.
ci_root="${SPATE_CI_ROOT:-ci}"
heading='## Supported'

fail() {
    echo "supported-versions.sh: $1" >&2
}

# Every service with pinned images.
services() {
    local dir
    for dir in "$ci_root"/*/; do
        [[ -d "$dir" ]] || continue
        dir="${dir%/}"
        printf '%s\n' "${dir##*/}"
    done
}

# Every release line a service pins, as `<major>.<minor>`, across all its lanes.
pinned_lines_for() { # service
    local dir lane tag
    for dir in "$ci_root/$1"/*/; do
        [[ -d "$dir" ]] || continue
        dir="${dir%/}"
        lane="${dir##*/}"
        tag=$(SPATE_CI_ROOT="$ci_root" ./scripts/container-image.sh "$1" "$lane")
        printf '%s\n' "$(echo "${tag##*:}" | cut -d. -f1-2)"
    done
}

# The section of a page under the support heading, up to the next heading.
support_section() { # page
    sed -n "/^$heading/,/^## /{/^## /!p;}" "$1"
}

# The release lines a table names: the leading `<major>.<minor>` of a row's
# first cell. A cell with no number contributes nothing, so a row naming a
# moving target by description is exempt.
claimed_lines_in() { # page
    support_section "$1" |
        sed -n 's/^| *\([0-9][0-9]*\.[0-9][0-9]*\)\([^0-9].*\)*|.*/\1/p'
}

# Sorted and space-separated, so two sets compare as strings.
as_set() {
    tr ' ' '\n' | sed '/^$/d' | sort -u | tr '\n' ' ' | sed 's/ $//'
}

# Returns non-zero when a page names a line its service no longer pins.
check_page() { # service, page
    local service="$1" page="$2" pinned claimed line
    if [[ ! -f "$page" ]]; then
        fail "ci/$service/DOCS names $page, which does not exist"
        return 1
    fi
    if ! grep -q "^$heading" "$page"; then
        fail "$page: no '${heading}' heading; the section moved or was renamed"
        return 1
    fi
    pinned=$(pinned_lines_for "$service" | as_set)
    claimed=$(claimed_lines_in "$page" | as_set)

    local bad=""
    for line in $claimed; do
        case " $pinned " in
        *" $line "*) ;;
        *) bad="$bad $line" ;;
        esac
    done
    if [[ -n "$bad" ]]; then
        fail "$page: claims${bad}, which ci/$service no longer pins (pinned: $pinned)"
        echo "  Update the table, or the lane under ci/$service/." >&2
        return 1
    fi
}

# Every (service, page) pair a `DOCS` file declares.
pairs() {
    local service line
    for service in $(services); do
        [[ -f "$ci_root/$service/DOCS" ]] || continue
        while IFS= read -r line; do
            line="${line%%#*}"
            line=$(printf '%s' "$line" | tr -d '[:space:]')
            [[ -n "$line" ]] || continue
            printf '%s\t%s\n' "$service" "$line"
        done <"$ci_root/$service/DOCS"
    done
}

# Script-scoped, so the EXIT trap can still see it once self_test has returned.
scratch=""

self_test() {
    local page errs=0 digest
    digest=$(printf 'a%.0s' $(seq 1 64))
    scratch=$(mktemp -d)
    # EXIT, not RETURN: a RETURN trap set here is the shell's, so it fires on
    # the first inner function return and takes the scratch tree with it.
    trap 'rm -rf "$scratch"' EXIT
    mkdir -p "$scratch/db/lts" "$scratch/db/lts-previous" "$scratch/db/stable"
    write_lane() { # lane, version
        echo "FROM vendor/db:$2@sha256:$digest" >"$scratch/db/$1/Dockerfile"
    }
    write_lane lts 9.4.1.2
    write_lane lts-previous 9.1.7.3
    write_lane stable 9.4.1.2

    page="$scratch/page.mdx"
    printf '## Supported server versions\n\n| Vendor | Support |\n| --- | --- |\n| 9.4 LTS | Guaranteed |\n| 9.1 LTS | Guaranteed |\n| Newest stable release | Guaranteed |\n\n## Something else\n\n| 1.0 | not a version table |\n' \
        >"$page"
    printf '# a comment\n%s\n' "$page" >"$scratch/db/DOCS"

    expect() { # got, want, desc
        if [[ "$1" != "$2" ]]; then
            echo "::error::$3: expected '$2', got '$1'" >&2
            errs=$((errs + 1))
        fi
    }

    ci_root="$scratch"

    expect "$(claimed_lines_in "$page" | as_set)" "9.1 9.4" \
        "a numbered row is claimed and a described row is not"
    expect "$(pinned_lines_for db | as_set)" "9.1 9.4" \
        "the pinned set collapses lanes sharing a line"
    expect "$(pairs)" "$(printf 'db\t%s' "$page")" \
        "DOCS pairs skip comments and blank lines"

    # Dependabot's monthly move of the stable lane adds a line and removes none.
    write_lane stable 9.5.0.1
    if ! check_page db "$page" >/dev/null 2>&1; then
        echo "::error::a stable-lane move was reported; Dependabot would fail this gate" >&2
        errs=$((errs + 1))
    fi

    # Moving an LTS line takes away a number the table still names.
    write_lane lts 9.6.0.1
    if check_page db "$page" >/dev/null 2>&1; then
        echo "::error::an unpinned claim was not caught" >&2
        errs=$((errs + 1))
    fi

    # A renamed section is a silent pass without this.
    write_lane lts 9.4.1.2
    sed 's/^## Supported.*/## Versions/' "$page" >"$scratch/renamed.mdx"
    if check_page db "$scratch/renamed.mdx" >/dev/null 2>&1; then
        echo "::error::a renamed section was not caught" >&2
        errs=$((errs + 1))
    fi

    # A service with no DOCS file is not checked at all.
    rm -f "$scratch/db/DOCS"
    expect "$(pairs)" "" "a service without DOCS is skipped"

    ci_root="${SPATE_CI_ROOT:-ci}"
    if [[ "$errs" -gt 0 ]]; then
        return 1
    fi
    echo "supported-versions.sh: self-test passed"
}

case "${1:-}" in
--self-test)
    self_test
    exit
    ;;
--check)
    failures=0
    checked=0
    while IFS=$'\t' read -r service page; do
        [[ -n "$service" ]] || continue
        checked=$((checked + 1))
        check_page "$service" "$page" || failures=$((failures + 1))
    done < <(pairs)
    if [[ "$failures" -gt 0 ]]; then
        exit 1
    fi
    echo "supported-versions.sh: $checked table(s) match the lines CI pins"
    ;;
*)
    echo "usage: $0 --check | --self-test" >&2
    exit 2
    ;;
esac
