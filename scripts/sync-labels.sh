#!/usr/bin/env bash
#
# Applies `.github/labels.yml` to the repository: creates a label that is
# missing, updates the colour and description of one that exists, and never
# deletes.
#
# Uses `gh`, which is preinstalled on the runners, rather than a labeling
# action. The organisation restricts Actions to GitHub-owned, verified-publisher
# and an explicit pattern list, so a third-party action here needs an allowlist
# entry — and a refused one does not fail the job, it reports `startup_failure`
# with nothing naming the action.
#
# Deleting is deliberately not implemented. Left to prune, this would remove
# GitHub's stock labels that open issues still carry, and deleting a label
# destroys the record that anything was filed under it. Retiring one is a
# manual step.
#
# Usage:
#   scripts/sync-labels.sh [--dry-run] [--repo OWNER/NAME]
#
# Environment:
#   GH_TOKEN  a token with `issues: write` on the repository
#   DRY_RUN   `true` is the same as passing --dry-run, so a caller can select
#             the mode without building an argument list that is sometimes
#             empty — an unquoted expansion the workflow linter rejects, and a
#             quoted one that arrives as an empty argument
set -euo pipefail

cd "$(dirname "$0")/.."

definitions=.github/labels.yml
dry_run=0
repo_args=()

[ "${DRY_RUN:-}" = "true" ] && dry_run=1

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) dry_run=1 ;;
        --repo)
            shift
            repo_args=(--repo "$1")
            ;;
        *)
            echo "sync-labels.sh: unknown argument '$1'" >&2
            exit 2
            ;;
    esac
    shift
done

fail() {
    echo "sync-labels.sh: $1" >&2
    exit 1
}

[ -f "$definitions" ] || fail "$definitions not found"
command -v gh >/dev/null || fail "gh is not installed"

# One TAB-separated name/colour/description row per entry. The quoted, fixed
# key order is the file's own convention — check-labels.sh depends on it too —
# so this stays a fixed-string parse rather than pulling in a YAML reader.
rows=$(awk '
    function value(line,   s) {
        s = line
        sub(/^[^:]*: "/, "", s)
        sub(/"[[:space:]]*$/, "", s)
        return s
    }
    function emit() {
        if (name != "") printf "%s\t%s\t%s\n", name, color, desc
    }
    /^- name: "/       { emit(); name = value($0); color = ""; desc = "" }
    /^  color: "/      { color = value($0) }
    /^  description: "/ { desc = value($0) }
    END                { emit() }
' "$definitions")

[ -n "$rows" ] || fail "no labels parsed from $definitions — has the format changed?"

created=0
total=0
while IFS=$'\t' read -r name color desc; do
    [ -n "$name" ] || continue
    total=$((total + 1))
    [ -n "$color" ] || fail "'$name' has no colour in $definitions"

    if [ "$dry_run" -eq 1 ]; then
        printf 'would sync  %-28s #%s  %s\n' "$name" "$color" "$desc"
        continue
    fi

    # `--force` updates colour and description when the label already exists,
    # which is what repairs one created by something else with a default grey
    # and no description.
    if gh label create "$name" \
        --color "$color" \
        --description "$desc" \
        --force \
        "${repo_args[@]+"${repo_args[@]}"}" >/dev/null; then
        created=$((created + 1))
    else
        fail "failed to sync '$name'"
    fi
done <<<"$rows"

if [ "$dry_run" -eq 1 ]; then
    echo "sync-labels.sh: $total label(s) would be synced (dry run, nothing changed)"
else
    echo "sync-labels.sh: $created of $total label(s) synced"
fi
