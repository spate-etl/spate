#!/usr/bin/env bash
#
# Holds the Node and cargo-tool identities the workflows resolve to a single
# pin apiece.
#
# `actions/setup-node`'s version input is optional, so a step naming neither
# `node-version` nor `node-version-file` installs nothing and leaves the
# runner's own Node on `PATH`. This checks that every `setup-node` step names
# `node-version-file: .node-version` exactly, that no step names a version
# inline, that `.node-version` itself holds an exact `X.Y.Z` release, and that
# `website/package.json`'s `engines.node` floor stays at or below it.
#
# `taiki-e/install-action`'s `tool:`/`tools:` input has the same shape of gap:
# a version written directly into a workflow is invisible to whatever moves
# `versions.mk`. This also checks that no workflow carries an inline
# `tool@version` literal, and that every bare tool name a workflow asks for
# resolves through `scripts/tool-versions.sh`. The second check treats
# `versions.mk` as a superset: a `versions.mk` entry with no workflow
# reference, such as `CARGO_FUZZ_VERSION`, stays legal.
#
# Usage:
#   ./scripts/version-pins.sh --check      # the gate
#   ./scripts/version-pins.sh --self-test  # the parsers, alone
#
# Runs on bash 3.2 and later: no associative arrays, no mapfile.
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
    echo "version-pins.sh: $1" >&2
}

# Every `node-version:` scalar under a directory, found by substring so that
# `node-version-file:` (and a `.node-version` path mentioned in prose) never
# matches.
literal_node_version_in() { # dir
    grep -rn 'node-version:' "$1" 2>/dev/null || true
}

check_no_literal_node_version() { # dir
    local dir="$1" matches
    matches=$(literal_node_version_in "$dir")
    if [[ -n "$matches" ]]; then
        fail "a setup-node step names a version inline instead of node-version-file:"
        printf '%s\n' "$matches" >&2
        return 1
    fi
}

# Every `actions/setup-node` step under a directory whose `with:` block does
# not carry a `node-version-file: .node-version` key of its own, one
# `file:line` per offending step. A step's block runs to the next YAML
# sequence item (`- ...`, including a bare `-` with its keys on later lines)
# at any indentation, or end of file; `with:` itself runs to the next line at
# or above its own indentation. A commented-out key, or the same text inside
# another key's value, is outside that scope and does not count. Empty output
# means every step names the file, a step with no version input at all
# included.
setup_node_steps_unpinned_in() { # dir
    local dir="$1" file
    while IFS= read -r -d '' file; do
        awk '
            {
                match($0, /^[[:space:]]*/)
                line_indent = RLENGTH
            }
            # A sequence item boundary: a dash followed by content, or a bare
            # dash alone on the line with its keys on the lines after it.
            $0 ~ /^[[:space:]]*-([[:space:]]|$)/ {
                if (instep && !pinned) {
                    print FILENAME ":" stepline ": setup-node step has no node-version-file: .node-version"
                }
                instep = 0
                inwith = 0
            }
            $0 ~ /uses:[[:space:]]*actions\/setup-node@/ {
                instep = 1
                pinned = 0
                inwith = 0
                stepline = FNR
                next
            }
            !instep { next }
            inwith && line_indent <= with_indent {
                inwith = 0
            }
            !inwith && $0 ~ /^[[:space:]]*with:[[:space:]]*$/ {
                inwith = 1
                with_indent = line_indent
                next
            }
            inwith && $0 ~ /^[[:space:]]*node-version-file:[[:space:]]*"?\.node-version"?[[:space:]]*$/ {
                pinned = 1
            }
            END {
                if (instep && !pinned) {
                    print FILENAME ":" stepline ": setup-node step has no node-version-file: .node-version"
                }
            }
        ' "$file"
    done < <(find "$dir" -type f \( -name '*.yml' -o -name '*.yaml' \) -print0)
}

check_every_setup_node_pinned() { # dir
    local dir="$1" matches
    matches=$(setup_node_steps_unpinned_in "$dir")
    if [[ -n "$matches" ]]; then
        fail "a setup-node step does not name node-version-file: .node-version"
        printf '%s\n' "$matches" >&2
        return 1
    fi
}

# The exact pin, or a failure for anything but a bare `X.Y.Z`: a leading `v`,
# a bare major, or a range all defeat the point of a file nothing else reads.
check_node_version_file() { # file
    local file="$1" pin
    if [[ ! -f "$file" ]]; then
        fail "$file does not exist"
        return 1
    fi
    pin=$(tr -d '[:space:]' <"$file")
    if [[ ! "$pin" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        fail "$file: '$pin' is not an exact X.Y.Z version"
        return 1
    fi
    printf '%s' "$pin"
}

# The lines of package.json between the "engines" key and its closing brace.
engines_block_in() { # file
    sed -n '/"engines"[[:space:]]*:[[:space:]]*{/,/}/p' "$1"
}

# The floor `package.json` declares for `engines.node`, anchored to a `"node"`
# key alone on its own line. A collapsed or otherwise reshaped block matches
# nothing, which `check_engines_floor` below treats as a failure rather than
# an absent constraint.
node_floor_spec_in() { # file
    engines_block_in "$1" |
        sed -n 's/^[[:space:]]*"node"[[:space:]]*:[[:space:]]*">=\([0-9][0-9.]*\)"[,]*[[:space:]]*$/\1/p'
}

# True (exit 0) when `floor` is at or below `pin`, comparing dot-separated
# numeric components left to right; a shorter side pads with 0.
floor_at_or_below_pin() { # floor, pin
    local floor="$1" pin="$2"
    local -a f p
    IFS=. read -r -a f <<<"$floor"
    IFS=. read -r -a p <<<"$pin"
    local n=${#f[@]}
    if [[ ${#p[@]} -gt $n ]]; then
        n=${#p[@]}
    fi
    local i fi_c pi_c
    for ((i = 0; i < n; i++)); do
        fi_c="${f[i]:-0}"
        pi_c="${p[i]:-0}"
        if ((10#$fi_c < 10#$pi_c)); then
            return 0
        elif ((10#$fi_c > 10#$pi_c)); then
            return 1
        fi
    done
    return 0
}

check_engines_floor() { # package_json, pin
    local pkg="$1" pin="$2" floor
    if [[ ! -f "$pkg" ]]; then
        fail "$pkg does not exist"
        return 1
    fi
    floor=$(node_floor_spec_in "$pkg")
    if [[ -z "$floor" ]]; then
        fail "$pkg: no '\"node\": \">=X.Y\"' floor found under \"engines\"; the block moved or was reshaped"
        return 1
    fi
    if ! floor_at_or_below_pin "$floor" "$pin"; then
        fail "$pkg: engines.node floor '>=$floor' is above the pinned $pin"
        return 1
    fi
}

# Every `tool:`/`tools:` value under a directory that carries an inline
# `name@version`, found by anchoring to the key at line start so that a
# `toolchain:` key (no colon directly after "tool") never matches. An
# optional leading quote, single or double, covers a quoted YAML scalar, and
# the version half accepts any text, so `tool@latest` matches too. No `@` is
# legitimate here: nothing resolves an already-versioned name.
literal_tool_version_in() { # dir
    grep -rnE "^[[:space:]]*tools?:[[:space:]]*['\"]?[A-Za-z0-9_.,-]+@" "$1" 2>/dev/null || true
}

check_no_literal_tool_version() { # dir
    local dir="$1" matches
    matches=$(literal_tool_version_in "$dir")
    if [[ -n "$matches" ]]; then
        fail "a workflow step names a tool version inline instead of a bare name versions.mk resolves"
        printf '%s\n' "$matches" >&2
        return 1
    fi
}

# Every distinct bare tool name a workflow asks for: a `tools?:` value that is
# itself a bare, comma-separated list, an optional surrounding quote, single
# or double, allowed and stripped (excluding a `${{ ... }}` expression, which
# resolves elsewhere, and an `@`-carrying value, which
# `check_no_literal_tool_version` already rejects), plus a literal argument
# to `tool-versions.sh --resolve`, such as the `workflows` job's direct
# install-action call.
bare_tool_names_in() { # dir
    {
        grep -rhoE "^[[:space:]]*tools?:[[:space:]]*['\"]?[A-Za-z][A-Za-z0-9_.,-]*['\"]?[[:space:]]*\$" "$1" 2>/dev/null \
            | sed -E "s/^[[:space:]]*tools?:[[:space:]]*['\"]?//; s/['\"]?[[:space:]]*\$//" || true
        grep -rhoE -- '--resolve[[:space:]]+[A-Za-z][A-Za-z0-9_.,-]*' "$1" 2>/dev/null \
            | sed -E 's/^--resolve[[:space:]]+//' || true
    } | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed '/^$/d' | sort -u
}

# Every bare name `bare_tool_names_in` finds must resolve through
# `tool-versions.sh` against `versions_mk`; a `versions.mk` entry with no such
# reference stays legal, since versions.mk is treated as a superset.
check_every_tool_resolves() { # dir, [versions_mk]
    local dir="$1" versions_mk="${2:-versions.mk}" name bad=""
    for name in $(bare_tool_names_in "$dir"); do
        if ! SPATE_VERSIONS_MK="$versions_mk" ./scripts/tool-versions.sh --version "$name" >/dev/null 2>&1; then
            bad="$bad $name"
        fi
    done
    if [[ -n "$bad" ]]; then
        fail "a workflow asks for a bare tool with no versions.mk entry:$bad"
        return 1
    fi
}

# Script-scoped, so the EXIT trap can still see it once self_test has returned.
scratch=""

self_test() {
    local errs=0 got
    scratch=$(mktemp -d)
    trap 'rm -rf "$scratch"' EXIT

    mkdir -p "$scratch/gh/workflows"

    cat >"$scratch/gh/workflows/ci.yml" <<'EOF'
      - uses: actions/setup-node@sha
        with:
          node-version: 24
EOF
    if check_no_literal_node_version "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::an inline node-version: literal was not caught" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/gh/workflows/ci.yml" <<'EOF'
      - uses: actions/setup-node@sha
        with:
          node-version-file: .node-version
EOF
    if ! check_no_literal_node_version "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::node-version-file: was rejected as if it were a literal" >&2
        errs=$((errs + 1))
    fi
    if ! check_every_setup_node_pinned "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a correctly pinned step was rejected" >&2
        errs=$((errs + 1))
    fi

    # A step naming neither `node-version` nor `node-version-file` installs
    # nothing, per the action's own optional-input behavior. Only a positive
    # assertion catches it; `check_no_literal_node_version` sees no forbidden
    # token and passes.
    cat >"$scratch/gh/workflows/ci.yml" <<'EOF'
      - uses: actions/setup-node@sha
        with:
          cache: npm
EOF
    if check_every_setup_node_pinned "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a setup-node step with no version input at all was not caught" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/gh/workflows/ci.yml" <<'EOF'
      - uses: actions/setup-node@sha
        with:
          node-version-file: some-other-file
EOF
    if check_every_setup_node_pinned "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::node-version-file: naming a different file was not caught" >&2
        errs=$((errs + 1))
    fi

    # A bare `-` on its own line is still a sequence-item boundary. Without
    # that rule, the unpinned first step here is never closed off and the
    # second step's real pin is read as if it belonged to the first.
    cat >"$scratch/gh/workflows/ci.yml" <<'EOF'
      - uses: actions/setup-node@sha
        with:
          cache: npm
      -
        uses: actions/setup-node@sha2
        with:
          node-version-file: .node-version
EOF
    if check_every_setup_node_pinned "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::an unpinned step before a bare-dash step was not caught" >&2
        errs=$((errs + 1))
    fi

    # The pin match is scoped to the `with:` block. A commented-out key does
    # not satisfy it.
    cat >"$scratch/gh/workflows/ci.yml" <<'EOF'
      - uses: actions/setup-node@sha
        with:
          # node-version-file: .node-version
          cache: npm
EOF
    if check_every_setup_node_pinned "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a commented-out node-version-file: was not caught" >&2
        errs=$((errs + 1))
    fi

    # Nor does the same text sitting in another key's value, such as a block
    # scalar under `env:`.
    cat >"$scratch/gh/workflows/ci.yml" <<'EOF'
      - uses: actions/setup-node@sha
        env:
          FOO: |
            node-version-file: .node-version
EOF
    if check_every_setup_node_pinned "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::node-version-file: text outside with: was read as a pin" >&2
        errs=$((errs + 1))
    fi

    # A real pin under `with:` still passes with that kind of decoy present
    # elsewhere in the same step.
    cat >"$scratch/gh/workflows/ci.yml" <<'EOF'
      - uses: actions/setup-node@sha
        with:
          node-version-file: .node-version
        env:
          FOO: |
            node-version-file: .node-version
EOF
    if ! check_every_setup_node_pinned "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a real pin under with: was rejected because of an env: decoy" >&2
        errs=$((errs + 1))
    fi

    printf '24\n' >"$scratch/bare-major"
    if check_node_version_file "$scratch/bare-major" >/dev/null 2>&1; then
        echo "::error::a bare major in .node-version was not caught" >&2
        errs=$((errs + 1))
    fi

    printf 'v24.21.0\n' >"$scratch/leading-v"
    if check_node_version_file "$scratch/leading-v" >/dev/null 2>&1; then
        echo "::error::a leading 'v' in .node-version was not caught" >&2
        errs=$((errs + 1))
    fi

    printf '24.21.0\n' >"$scratch/exact"
    got=$(check_node_version_file "$scratch/exact")
    if [[ "$got" != "24.21.0" ]]; then
        echo "::error::an exact X.Y.Z version was not accepted: got '$got'" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/pkg-high.json" <<'EOF'
{
  "engines": {
    "node": ">=26"
  }
}
EOF
    if check_engines_floor "$scratch/pkg-high.json" "24.21.0" >/dev/null 2>&1; then
        echo "::error::a floor above the pin was not caught" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/pkg-low.json" <<'EOF'
{
  "engines": {
    "node": ">=22.6"
  }
}
EOF
    if ! check_engines_floor "$scratch/pkg-low.json" "24.21.0" >/dev/null 2>&1; then
        echo "::error::a floor at or below the pin was rejected" >&2
        errs=$((errs + 1))
    fi

    # A formatter that collapses the block onto one line leaves no line where
    # "node" stands alone, so the anchored regex matches nothing. That must
    # fail the check, not pass it for lack of a claim to violate.
    cat >"$scratch/pkg-collapsed.json" <<'EOF'
{
  "engines": { "node": ">=22.6" }
}
EOF
    if check_engines_floor "$scratch/pkg-collapsed.json" "24.21.0" >/dev/null 2>&1; then
        echo "::error::a reformatted engines block silently passed instead of failing" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: cargo-deny@0.20.2
EOF
    if check_no_literal_tool_version "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::an inline tool@version literal was not caught" >&2
        errs=$((errs + 1))
    fi

    # Regression: a quoted value and a non-numeric version both defeated the
    # first cut of this pattern.
    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: "cargo-deny@0.20.2"
EOF
    if check_no_literal_tool_version "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a quoted tool@version literal was not caught" >&2
        errs=$((errs + 1))
    fi

    # Regression: a single-quoted value defeated the double-quote-only fix.
    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: 'cargo-deny@0.20.2'
EOF
    if check_no_literal_tool_version "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a single-quoted tool@version literal was not caught" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: cargo-deny@latest
EOF
    if check_no_literal_tool_version "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a non-numeric tool@version literal was not caught" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: cargo-deny
EOF
    if ! check_no_literal_tool_version "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a bare tool name was rejected as if it were a literal" >&2
        errs=$((errs + 1))
    fi

    # A resolved `${{ ... }}` expression, the shape `install-action` itself
    # receives, passes too.
    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: taiki-e/install-action@sha
        with:
          tool: ${{ steps.resolve-tools.outputs.tools }}
EOF
    if ! check_no_literal_tool_version "$scratch/gh" >/dev/null 2>&1; then
        echo "::error::a resolved expression was rejected as if it were a literal" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: nextest,cargo-llvm-cov
      - uses: ./.github/actions/setup-rust
        with:
          tools: cargo-hack
EOF
    got=$(bare_tool_names_in "$scratch/gh" | tr '\n' ' ' | sed 's/ $//')
    if [[ "$got" != "cargo-hack cargo-llvm-cov nextest" ]]; then
        echo "::error::bare_tool_names_in did not extract and dedupe the expected names: got '$got'" >&2
        errs=$((errs + 1))
    fi

    # The `workflows` job's direct call has no `tools:` key at all: the tool
    # list is an argument to `tool-versions.sh --resolve` inside a `run:`
    # block, and the subset check must still reach it.
    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
        run: |
          echo "tools=$(./scripts/tool-versions.sh --resolve cargo-hack)" >>"$GITHUB_OUTPUT"
EOF
    got=$(bare_tool_names_in "$scratch/gh")
    if [[ "$got" != "cargo-hack" ]]; then
        echo "::error::bare_tool_names_in did not extract a --resolve argument: got '$got'" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/versions.mk" <<'EOF'
CARGO_HACK_VERSION := 0.6.45
CARGO_FUZZ_VERSION := 0.13.2
EOF

    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: cargo-hack
EOF
    if ! check_every_tool_resolves "$scratch/gh" "$scratch/versions.mk" >/dev/null 2>&1; then
        echo "::error::a bare name with a versions.mk entry was rejected" >&2
        errs=$((errs + 1))
    fi

    # CARGO_FUZZ_VERSION has no workflow reference in this fixture, and
    # treating versions.mk as a superset keeps that legal.
    if ! check_every_tool_resolves "$scratch/gh" "$scratch/versions.mk" >/dev/null 2>&1; then
        echo "::error::an unreferenced versions.mk entry caused a false failure" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: not-a-real-tool
EOF
    if check_every_tool_resolves "$scratch/gh" "$scratch/versions.mk" >/dev/null 2>&1; then
        echo "::error::a bare name with no versions.mk entry was not caught" >&2
        errs=$((errs + 1))
    fi

    # One bad name in a comma list fails the whole step, matching how
    # `install-action` would receive it.
    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: cargo-hack,not-a-real-tool
EOF
    if check_every_tool_resolves "$scratch/gh" "$scratch/versions.mk" >/dev/null 2>&1; then
        echo "::error::one bad name in a comma list was not caught" >&2
        errs=$((errs + 1))
    fi

    # Regression: a single-quoted bare name reached neither check, so an
    # unpinned tool written this way passed silently.
    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: 'not-a-real-tool'
EOF
    if check_every_tool_resolves "$scratch/gh" "$scratch/versions.mk" >/dev/null 2>&1; then
        echo "::error::a single-quoted bare name with no versions.mk entry was not caught" >&2
        errs=$((errs + 1))
    fi

    cat >"$scratch/gh/workflows/tools.yml" <<'EOF'
      - uses: ./.github/actions/setup-rust
        with:
          tools: 'cargo-hack'
EOF
    if ! check_every_tool_resolves "$scratch/gh" "$scratch/versions.mk" >/dev/null 2>&1; then
        echo "::error::a single-quoted bare name with a versions.mk entry was rejected" >&2
        errs=$((errs + 1))
    fi

    rm -rf "$scratch"
    trap - EXIT
    scratch=""

    if [[ "$errs" -gt 0 ]]; then
        return 1
    fi
    echo "version-pins.sh: self-test passed"
}

case "${1:-}" in
--self-test)
    self_test
    exit
    ;;
--check)
    failures=0
    check_no_literal_node_version .github || failures=$((failures + 1))
    check_every_setup_node_pinned .github || failures=$((failures + 1))
    if pin=$(check_node_version_file .node-version); then
        check_engines_floor website/package.json "$pin" || failures=$((failures + 1))
    else
        failures=$((failures + 1))
    fi
    check_no_literal_tool_version .github || failures=$((failures + 1))
    check_every_tool_resolves .github || failures=$((failures + 1))
    if [[ "$failures" -gt 0 ]]; then
        exit 1
    fi
    echo "version-pins.sh: node pinned to $pin, no inline node-version: scalar, engines.node floor holds, every workflow tool resolves through versions.mk"
    ;;
*)
    echo "usage: $0 --check | --self-test" >&2
    exit 2
    ;;
esac
