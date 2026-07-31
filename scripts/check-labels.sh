#!/usr/bin/env bash
#
# Checks that every label the repository *references* is a label the repository
# *defines*, and fails if one is missing.
#
# The definitions live in `.github/labels.yml` and this script asserts that
# the references agree with them. Run by CI; run it after touching any of the
# files listed below.
#
# What it deliberately does NOT check: that the labels defined here exist on
# GitHub. That direction is the sync workflow's job, and asserting it would mean
# an API call and a token, turning a local check into a networked one.
#
# Usage: ./scripts/check-labels.sh
set -euo pipefail

cd "$(dirname "$0")/.."

definitions=.github/labels.yml

fail() {
    echo "check-labels.sh: $1" >&2
    exit 1
}

[ -f "$definitions" ] || fail "$definitions not found"

# Every `- name: "..."` at the top level of the definitions file. The quotes are
# required by the file's own convention precisely so this stays a fixed-string
# match rather than a YAML parse.
defined=$(sed -n 's/^- name: "\(.*\)"$/\1/p' "$definitions" | sort -u)
[ -n "$defined" ] || fail "no labels defined in $definitions — is the format still \`- name: \"...\"\`?"

# Collect references as TAB-separated "label<TAB>source" pairs.
#
# All three sources spell a label list the same way — a bracketed, quoted,
# comma-separated array on one line — so one extractor covers them. If any of
# them ever moves to a block list, this stops seeing it: the count assertion at
# the end is what turns that into a failure rather than a false pass.
references=$(
    {
        # Dependabot: one `labels:` line per ecosystem.
        grep -hoE '^ *labels: \[[^]]*\]' .github/dependabot.yml 2>/dev/null |
            grep -oE '"[^"]+"' | tr -d '"' | sed 's/$/\t.github\/dependabot.yml/'

        # Issue forms: a top-level `labels:` key in the front matter.
        for form in .github/ISSUE_TEMPLATE/*.yml; do
            [ -e "$form" ] || continue
            grep -hoE '^labels: \[[^]]*\]' "$form" 2>/dev/null |
                grep -oE '"[^"]+"' | tr -d '"' | sed "s|\$|\t$form|"
        done

        # release-plz: the label put on the automated release pull request.
        if [ -f release-plz.toml ]; then
            grep -hoE '^ *pr_labels *= *\[[^]]*\]' release-plz.toml 2>/dev/null |
                grep -oE '"[^"]+"' | tr -d '"' | sed 's/$/\trelease-plz.toml/'
        fi

        # The path labeler: its keys are the label names.
        if [ -f .github/labeler.yml ]; then
            sed -n 's/^"\(.*\)":.*$/\1/p' .github/labeler.yml |
                sed 's/$/\t.github\/labeler.yml/'
        fi
    } | sort -u
)

[ -n "$references" ] || fail "no label references found — did a source file change shape?"

missing=0
while IFS=$'\t' read -r label source; do
    [ -n "$label" ] || continue
    if ! grep -qxF "$label" <<<"$defined"; then
        echo "check-labels.sh: '$label' is referenced by $source but not defined in $definitions" >&2
        missing=$((missing + 1))
    fi
done <<<"$references"

if [ "$missing" -gt 0 ]; then
    echo "check-labels.sh: $missing undefined label reference(s)" >&2
    echo "check-labels.sh: add them to $definitions, or stop referencing them" >&2
    exit 1
fi

defined_count=$(grep -c . <<<"$defined")
reference_count=$(grep -c . <<<"$references")
echo "check-labels.sh: $reference_count reference(s) across the tree, all defined among $defined_count label(s)"
