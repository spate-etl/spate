#!/usr/bin/env bash
#
# The built-site metadata gate: every HTML page the build produced carries a
# `<meta name="description">`. This reaches what the source check cannot: the
# benchmark pages rendered from the submodule, the homepage, the brand page
# and the blog.
#
# Usage:
#   ./scripts/site-meta.sh --check     # after `npm run build` in website/
set -euo pipefail
cd "$(dirname "$0")/.."

build=website/build

fail() {
    echo "site-meta.sh: $1" >&2
    exit 1
}

# Pages the site does not describe: the not-found page, the search page, the
# generated license texts, and the client-redirect stubs, which carry only a
# refresh.
skip() { # path
    case "$1" in
    "$build/404.html" | "$build/search.html" | "$build/licenses/index.html") return 0 ;;
    esac
    grep -qE 'http-equiv="?refresh"?' "$1"
}

case "${1:-}" in
--check) ;;
*) fail "usage: $0 --check" ;;
esac

[ -d "$build" ] || fail "$build not found; run the site build first"

failures=0
count=0
while IFS= read -r file; do
    skip "$file" && continue
    count=$((count + 1))
    # The build drops the quotes around attribute values it can.
    if ! grep -qE '<meta[^>]*name="?description"?[^>]*content="[^"]' "$file"; then
        echo "site-meta.sh: ${file#"$build/"}: no description" >&2
        failures=$((failures + 1))
    fi
done < <(find "$build" -type f -name '*.html' ! -path "$build/assets/*" | sort)

[ "$count" -gt 0 ] || fail "no pages found under $build"
[ "$failures" -eq 0 ] || fail "$failures page(s) without a description"
echo "site-meta.sh: $count page(s); every one carries a description."
