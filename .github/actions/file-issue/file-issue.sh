#!/usr/bin/env bash
# Comments on the open issue titled $TITLE, or files one from $BODY_FILE and
# sets its type to Bug. The comment is $COMMENT_FILE when set, else a "Still
# failing" line naming $RUN_URL. A body or comment file that is missing or
# empty gets a line naming $RUN_URL in its place. Runs by hand with `gh` on
# PATH.
set -euo pipefail

: "${TITLE:?}" "${BODY_FILE:?}" "${RUN_URL:?}"

# The files come from a step that builds and runs xtask, which a toolchain or
# dependency break can stop. The failure is filed all the same.
if [ -s "$BODY_FILE" ]; then
  body=(--body-file "$BODY_FILE")
else
  body=(--body "The body of this issue could not be rendered. The [run](${RUN_URL}) has the failure and its output.")
fi
if [ -n "${COMMENT_FILE:-}" ] && [ -s "$COMMENT_FILE" ]; then
  comment=(--body-file "$COMMENT_FILE")
else
  comment=(--body "Still failing: ${RUN_URL}")
fi

# The search is fuzzy, so the exact title is matched here.
matches=$(gh issue list --state open --search "$TITLE in:title" --json number,title)
existing=$(jq -r --arg t "$TITLE" 'map(select(.title == $t)) | .[0].number // empty' <<<"$matches")
if [ -n "$existing" ]; then
  gh issue comment "$existing" "${comment[@]}"
  exit 0
fi

# `gh issue create` applies `--type` after creating the issue, so a refused
# type would fail the step with the URL unprinted. The type is set afterwards,
# and a refusal leaves the issue for a person to fix.
url=$(gh issue create --title "$TITLE" "${body[@]}")
echo "$url"
gh issue edit "$url" --type Bug ||
  echo "::warning::Could not set the type on $url; set it to Bug by hand."
