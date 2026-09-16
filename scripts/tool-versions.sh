#!/usr/bin/env bash
#
# Resolves a bare cargo-tool name to the version versions.mk pins for it.
#
# `taiki-e/install-action`'s `tool:`/`tools:` input takes a literal string, so
# a workflow step cannot read a Make variable directly; this is what a resolve
# step calls instead. The mapping from tool name to variable is mechanical:
# uppercase, `-` to `_`, suffix `_VERSION` (`cargo-hack` -> `CARGO_HACK_VERSION`).
# An unresolvable name is an error: install-action reads a bare name as
# "install latest", and a silent fallback would float a gate that exists to
# stay pinned.
#
# Usage:
#   ./scripts/tool-versions.sh --resolve <tool>[,<tool>...]  # name@version, comma-joined
#   ./scripts/tool-versions.sh --version <tool>               # one tool's version, alone
#   ./scripts/tool-versions.sh --self-test                    # the parsers, alone
#
# Runs on bash 3.2 and later: no associative arrays, no mapfile.
set -euo pipefail

cd "$(dirname "$0")/.."

# Overridden by self_test, and by a caller checking a fixture pin file.
versions_file="${SPATE_VERSIONS_MK:-versions.mk}"

fail() {
    echo "tool-versions.sh: $1" >&2
}

# A tool name's Make variable: uppercase, `-` to `_`, suffix `_VERSION`.
var_name_for() { # tool
    local upper
    upper=$(printf '%s' "$1" | tr '[:lower:]-' '[:upper:]_')
    printf '%s_VERSION' "$upper"
}

# The version versions_file pins for a tool, or failure when no entry exists.
version_for() { # tool
    local var val
    var=$(var_name_for "$1")
    val=$(sed -n "s/^${var}[[:space:]]*:=[[:space:]]*\([^[:space:]]*\).*/\1/p" "$versions_file")
    if [[ -z "$val" ]]; then
        return 1
    fi
    printf '%s' "$val"
}

# `tool@<pinned version>` for a bare tool name, or failure when versions_file
# has no entry for it.
resolve_one() { # tool
    local tool="$1" version
    if ! version=$(version_for "$tool"); then
        echo "::error::tool-versions.sh: no $(var_name_for "$tool") entry in $versions_file for '$tool'" >&2
        return 1
    fi
    printf '%s@%s' "$tool" "$version"
}

# Comma-joined resolution of every entry in a comma-separated tool list.
# Prints nothing on failure: a partial list would read as a resolved one.
resolve_list() { # tool[,tool...]
    local csv="$1" tool resolved out="" failed=0
    local IFS=,
    for tool in $csv; do
        tool=$(printf '%s' "$tool" | tr -d '[:space:]')
        [[ -n "$tool" ]] || continue
        if resolved=$(resolve_one "$tool"); then
            if [[ -n "$out" ]]; then
                out="$out,$resolved"
            else
                out="$resolved"
            fi
        else
            failed=1
        fi
    done
    if [[ "$failed" -ne 0 ]]; then
        return 1
    fi
    printf '%s' "$out"
}

# Script-scoped, so the EXIT trap can still see it once self_test has returned.
scratch=""

self_test() {
    local errs=0 got
    scratch=$(mktemp -d)
    trap 'rm -rf "$scratch"' EXIT

    cat >"$scratch/versions.mk" <<'EOF'
CARGO_HACK_VERSION := 0.6.45
NEXTEST_VERSION  :=  0.9.140
CARGO_LLVM_COV_VERSION := 0.8.7
EOF
    versions_file="$scratch/versions.mk"

    got=$(var_name_for cargo-hack)
    if [[ "$got" != CARGO_HACK_VERSION ]]; then
        echo "::error::var_name_for did not map cargo-hack to CARGO_HACK_VERSION: got '$got'" >&2
        errs=$((errs + 1))
    fi

    got=$(var_name_for nextest)
    if [[ "$got" != NEXTEST_VERSION ]]; then
        echo "::error::var_name_for did not map nextest to NEXTEST_VERSION: got '$got'" >&2
        errs=$((errs + 1))
    fi

    got=$(resolve_one cargo-hack)
    if [[ "$got" != cargo-hack@0.6.45 ]]; then
        echo "::error::resolve_one did not resolve cargo-hack: got '$got'" >&2
        errs=$((errs + 1))
    fi

    # Extra whitespace around `:=` in versions.mk still parses.
    got=$(resolve_one nextest)
    if [[ "$got" != nextest@0.9.140 ]]; then
        echo "::error::resolve_one did not tolerate whitespace around ':=': got '$got'" >&2
        errs=$((errs + 1))
    fi

    got=$(resolve_list nextest,cargo-llvm-cov)
    if [[ "$got" != nextest@0.9.140,cargo-llvm-cov@0.8.7 ]]; then
        echo "::error::resolve_list did not join a comma list correctly: got '$got'" >&2
        errs=$((errs + 1))
    fi

    # install-action reads an unrecognized bare name as "install latest"; a
    # hard failure here is what stands between a pin and that fallback.
    if resolve_one not-a-real-tool >/dev/null 2>&1; then
        echo "::error::resolve_one accepted a bare name with no versions.mk entry" >&2
        errs=$((errs + 1))
    fi

    # One bad name fails the whole list; no partial output.
    if got=$(resolve_list nextest,not-a-real-tool 2>/dev/null); then
        echo "::error::resolve_list accepted a list containing an unresolvable name" >&2
        errs=$((errs + 1))
    elif [[ -n "$got" ]]; then
        echo "::error::resolve_list printed a partial result on failure: got '$got'" >&2
        errs=$((errs + 1))
    fi

    got=$(version_for cargo-llvm-cov)
    if [[ "$got" != 0.8.7 ]]; then
        echo "::error::version_for did not return the bare version: got '$got'" >&2
        errs=$((errs + 1))
    fi

    rm -rf "$scratch"
    trap - EXIT
    scratch=""
    versions_file="${SPATE_VERSIONS_MK:-versions.mk}"

    if [[ "$errs" -gt 0 ]]; then
        return 1
    fi
    echo "tool-versions.sh: self-test passed"
}

case "${1:-}" in
--self-test)
    self_test
    exit
    ;;
--resolve)
    list="${2:-}"
    if [[ -z "$list" ]]; then
        echo "usage: $0 --resolve <tool>[,<tool>...]" >&2
        exit 2
    fi
    if ! out=$(resolve_list "$list"); then
        exit 1
    fi
    printf '%s\n' "$out"
    ;;
--version)
    tool="${2:-}"
    if [[ -z "$tool" ]]; then
        echo "usage: $0 --version <tool>" >&2
        exit 2
    fi
    if ! out=$(version_for "$tool"); then
        echo "::error::tool-versions.sh: no $(var_name_for "$tool") entry in $versions_file for '$tool'" >&2
        exit 1
    fi
    printf '%s\n' "$out"
    ;;
*)
    fail "usage: $0 --resolve <tool>[,<tool>...] | --version <tool> | --self-test"
    exit 2
    ;;
esac
