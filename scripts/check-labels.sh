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

# Each source spells its labels its own way, so each gets its own extractor.
#
# An extractor that stops matching is the dangerous failure: the labels it was
# watching become unchecked and the script still exits 0. `extract` guards
# against it by pairing every extractor with a loose pattern for "this file
# still mentions labels at all". If the loose pattern hits and the strict one
# does not, the extractor has gone blind and that is a hard failure — the file
# has changed shape and nobody taught this script the new one.
# extract <source> <loose> <strict> <label-filter>
#
# `loose` matches every line that mentions labels at all; `strict` matches the
# spelling this script can actually read. The counts must agree. Comparing them
# per occurrence rather than per file is what matters: dependabot.yml carries
# four `labels:` entries, so "did we get any" would still pass with three of
# them parsed and one silently unread.
extract() {
    local source=$1 loose=$2 strict=$3 filter=$4 n_loose n_strict
    [ -f "$source" ] || return 0

    n_loose=$(grep -cE "$loose" "$source" || true)
    n_strict=$(grep -cE "$strict" "$source" || true)
    if [ "$n_loose" -ne "$n_strict" ]; then
        fail "$source has $n_loose line(s) mentioning labels but $n_strict this script can read.
  The file has changed shape and this extractor has not. Fix the extractor —
  do not delete the check, it is the one that notices."
    fi

    while IFS= read -r label; do
        [ -n "$label" ] && printf '%s\t%s\n' "$label" "$source"
    done < <(grep -hE "$strict" "$source" | eval "$filter" || true)
}

references=$(
    {
        quoted='grep -oE "\"[^\"]+\"" | tr -d "\""'

        # Dependabot: one bracketed `labels:` array per ecosystem.
        extract .github/dependabot.yml '^ *labels:' '^ *labels: \[[^]]*\]' "$quoted"

        # Issue forms: a top-level `labels:` key in the front matter. A form
        # carrying none is legitimate — several set only a native issue type.
        for form in .github/ISSUE_TEMPLATE/*.yml; do
            [ -e "$form" ] || continue
            extract "$form" '^labels:' '^labels: \[[^]]*\]' "$quoted"
        done

        # release-plz: the label put on the automated release pull request.
        extract release-plz.toml '^ *pr_labels *=' '^ *pr_labels *= *\[[^]]*\]' "$quoted"

        # The path labeler: its top-level keys are the label names.
        extract .github/labeler.yml '^"' '^"[^"]+":' \
            'sed -E "s/^\"([^\"]+)\":.*/\1/"'

        # The CI classifier matches two label names as string literals, so a
        # rename there is executable rather than declarative: the gate would
        # stay green while `apply_ci_labels` silently stopped firing.
        extract scripts/ci-changes.sh '== \*",' '== \*",[^"]+,"\*' \
            'grep -oE "\*\",[^\"]+,\"\*" | sed -E "s/^\*\",(.*),\"\*$/\1/"'

        # The performance-label workflow syncs one label, and routes its name
        # through a `PERF_LABEL:` env key precisely so this extractor can read
        # it declaratively instead of parsing shell. Renaming the key trips
        # the gone-blind guard above rather than going unchecked.
        extract .github/workflows/perf-label.yml '_LABEL:' '^ *PERF_LABEL: "[^"]+"$' "$quoted"
    } | sort -u
)

[ -n "$references" ] || fail "no label references found — did every source change shape at once?"

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
source_count=$(cut -f2 <<<"$references" | sort -u | grep -c .)
echo "check-labels.sh: $reference_count reference(s) from $source_count source(s), all defined among $defined_count label(s)"
