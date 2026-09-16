#!/usr/bin/env bash
#
# Compares every pinned tool version against its upstream latest release, and
# renders the result as a table and (for the scheduled job) an issue body.
#
# `versions.mk` is the tool list: every `<TOOL>_VERSION` entry there is one row
# to check. Node is not in versions.mk, since `.node-version` already pins it
# for a different reason (`scripts/version-pins.sh` checks that pin's shape);
# this checks its content the same way, restricted to the pinned major, since
# moving to a new major is an LTS-calendar decision rather than a version to
# chase.
#
# A tool's upstream source is not always its crates.io name: `nextest`
# installs under that name but publishes as `cargo-nextest`, and `shellcheck`
# is not on crates.io at all. `versions.d/<tool>/SOURCE` makes that mapping an
# explicit, per-tool declaration instead of a case arm in this script.
# `versions.d/README.md` has the format.
#
# Every network call goes through `http_get`, so `--self-test` can point
# `SPATE_TOOL_DRIFT_FIXTURES` at a fixture directory and run offline; a
# request's answer comes from a file there, named after the URL.
#
# A tool this cannot resolve, or an unreachable source, fails `--check`
# outright and reports nothing: an index that did not answer is not a drifted
# pin, and the two must never be confused.
#
# Usage:
#   ./scripts/tool-drift.sh --check [--table-out FILE]   # hits the network
#   ./scripts/tool-drift.sh --render-issue-body \
#       --sha SHA --run-url URL --table FILE             # the filed issue's body
#   ./scripts/tool-drift.sh --self-test                  # the parsers and comparator, offline
#
# Runs on bash 3.2 and later: no associative arrays, no mapfile, and every
# array expansion guarded, because `"${arr[@]}"` on an empty array is an
# unbound-variable error under `set -u` there.
set -euo pipefail

cd "$(dirname "$0")/.."

versions_file="${SPATE_VERSIONS_MK:-versions.mk}"
sources_root="${SPATE_TOOL_SOURCES:-versions.d}"
node_version_file="${SPATE_NODE_VERSION_FILE:-.node-version}"
fixtures_dir="${SPATE_TOOL_DRIFT_FIXTURES:-}"

fail() {
    echo "tool-drift.sh: $1" >&2
}

# ---------------------------------------------------------------------------
# The one network entry point.
# ---------------------------------------------------------------------------

# A URL turned into a fixture file name: strip the scheme, flatten every
# separator to `_`. Deterministic, so a fixture file and the request that
# reads it are written against the same rule.
fixture_name_for() { # url
    printf '%s' "$1" | sed -e 's#^https\{0,1\}://##' -e 's#[/:?&]#_#g'
}

# The body of a GET request: from the network, or (`fixtures_dir` set) from a
# file there. Extra arguments are passed to curl, for a header; a fixture
# answers the same way regardless of headers, since the self-test never needs
# to distinguish an authenticated request from an anonymous one.
http_get() { # url [curl-arg...]
    local url="$1"
    shift
    if [[ -n "$fixtures_dir" ]]; then
        local fixture
        fixture="$fixtures_dir/$(fixture_name_for "$url")"
        if [[ ! -f "$fixture" ]]; then
            fail "no fixture for $url (expected $fixture)"
            return 1
        fi
        cat "$fixture"
        return 0
    fi
    curl -fsS --retry 3 --max-time 20 "$@" "$url"
}

# ---------------------------------------------------------------------------
# versions.mk: the tool list and its pins.
# ---------------------------------------------------------------------------

# `CARGO_LLVM_COV` -> `cargo-llvm-cov`: the reverse of tool-versions.sh's
# `var_name_for`.
tool_name_from_var() { # VAR (without the _VERSION suffix)
    printf '%s' "$1" | tr '[:upper:]_' '[:lower:]-'
}

# Every tool `versions_file` pins, as `<tool> <version>` lines.
pinned_tools() {
    sed -n 's/^\([A-Z][A-Z0-9_]*\)_VERSION[[:space:]]*:=[[:space:]]*\([^[:space:]]*\).*/\1 \2/p' \
        "$versions_file" |
        while read -r var val; do
            printf '%s %s\n' "$(tool_name_from_var "$var")" "$val"
        done
}

# ---------------------------------------------------------------------------
# versions.d: one tool, one upstream source.
# ---------------------------------------------------------------------------

# `crate:<name>` or `github:<owner>/<repo>`, from `versions.d/<tool>/SOURCE`.
# `#`-comments and blank lines are skipped, `ci/<service>/DOCS`-style.
source_for() { # tool
    local file="$sources_root/$1/SOURCE" line
    if [[ ! -f "$file" ]]; then
        fail "$file is missing; every versions.mk tool needs a source declaration"
        return 1
    fi
    while IFS= read -r line; do
        line="${line%%#*}"
        line=$(printf '%s' "$line" | tr -d '[:space:]')
        [[ -n "$line" ]] || continue
        printf '%s\n' "$line"
        return 0
    done <"$file"
    fail "$file declares no source"
    return 1
}

# ---------------------------------------------------------------------------
# Numeric, component-wise version comparison. `sort -V` is GNU-only and these
# scripts run on darwin too; a string compare gets `0.9.140` against `0.9.99`
# backwards, nextest's own live case.
# ---------------------------------------------------------------------------

# True (exit 0) when `a` is strictly greater than `b`: dot-separated numeric
# components, compared left to right, the shorter side padded with 0.
version_gt() { # a, b
    local a="$1" b="$2"
    local -a av bv
    IFS=. read -r -a av <<<"$a"
    IFS=. read -r -a bv <<<"$b"
    local n=${#av[@]}
    if [[ ${#bv[@]} -gt $n ]]; then
        n=${#bv[@]}
    fi
    local i ac bc
    for ((i = 0; i < n; i++)); do
        ac="${av[i]:-0}"
        bc="${bv[i]:-0}"
        if ((10#$ac > 10#$bc)); then
            return 0
        elif ((10#$ac < 10#$bc)); then
            return 1
        fi
    done
    return 1
}

# The greatest version on stdin, one per line; prints nothing for empty input.
highest_version() {
    local best="" line
    while IFS= read -r line; do
        [[ -n "$line" ]] || continue
        if [[ -z "$best" ]] || version_gt "$line" "$best"; then
            best="$line"
        fi
    done
    if [[ -n "$best" ]]; then
        printf '%s\n' "$best"
    fi
}

# ---------------------------------------------------------------------------
# Upstream queries, one per source kind.
# ---------------------------------------------------------------------------

# A crate's path in the sparse index, keyed on name length.
index_path_for() { # crate
    local crate="$1"
    case "${#crate}" in
    1) printf '1/%s\n' "$crate" ;;
    2) printf '2/%s\n' "$crate" ;;
    3) printf '3/%s/%s\n' "${crate:0:1}" "$crate" ;;
    *) printf '%s/%s/%s\n' "${crate:0:2}" "${crate:2:2}" "$crate" ;;
    esac
}

# The highest non-yanked, non-prerelease version the sparse index lists for a
# crate. crates.io's crawler policy directs automated traffic to the sparse
# index rather than the search API; it needs no contact User-Agent, and every
# line carries `yanked`.
latest_crate_version() { # crate
    local crate="$1" body
    body=$(http_get "https://index.crates.io/$(index_path_for "$crate")") || return 1
    printf '%s\n' "$body" |
        jq -r 'select(.yanked | not) | .vers' |
        grep -v -- '-' |
        highest_version
}

# The latest GitHub release's tag, with a leading `v` stripped. Authenticated
# when `GH_TOKEN` is set: the unauthenticated rate limit is shared with every
# other unauthenticated caller at the runner's address.
latest_github_release() { # owner/repo
    local repo="$1" body tag
    local -a hdr=()
    if [[ -n "${GH_TOKEN:-}" ]]; then
        hdr=(-H "Authorization: Bearer $GH_TOKEN")
    fi
    body=$(http_get "https://api.github.com/repos/$repo/releases/latest" ${hdr[@]+"${hdr[@]}"}) || return 1
    tag=$(printf '%s' "$body" | jq -r '.tag_name // empty')
    if [[ -z "$tag" ]]; then
        fail "$repo: releases/latest carried no tag_name"
        return 1
    fi
    printf '%s\n' "${tag#v}"
}

# The highest release Node ships within a pinned major, minor included. Never
# crosses a major: that move is an LTS-calendar decision, not a version to
# chase.
latest_node_patch_in_major() { # major
    local major="$1" body
    body=$(http_get "https://nodejs.org/dist/index.json") || return 1
    printf '%s\n' "$body" |
        jq -r --arg prefix "v${major}." 'map(select(.version | startswith($prefix))) | .[].version' |
        sed 's/^v//' |
        highest_version
}

# The version a tool's declared source finds upstream.
latest_for_tool() { # tool
    local tool="$1" src kind arg
    src=$(source_for "$tool") || return 1
    kind="${src%%:*}"
    arg="${src#*:}"
    case "$kind" in
    crate) latest_crate_version "$arg" ;;
    github) latest_github_release "$arg" ;;
    *)
        fail "$sources_root/$tool/SOURCE: unrecognized source kind '$kind'"
        return 1
        ;;
    esac
}

# ---------------------------------------------------------------------------
# --check: the query, rendered as a table.
# ---------------------------------------------------------------------------

table_header() {
    printf '| Tool | Pinned | Latest | Source |\n| --- | --- | --- | --- |\n'
}

# Queries every pinned tool plus Node, prints the drift table, and (given a
# path) writes it there too. A network or parse failure for any one of them
# fails the whole check: a partial table would read as a complete one.
#
# Exit 0 covers both outcomes a caller can act on: nothing moved, or something
# did. Only "could not tell" is an error, so a caller gating on drift reads
# `GITHUB_OUTPUT`'s `drifted` rather than this exit code.
run_check() { # table_out
    local table_out="${1:-}" tool pinned latest src failed=0 drifted=0 rows=""

    while read -r tool pinned; do
        [[ -n "$tool" ]] || continue
        if ! latest=$(latest_for_tool "$tool"); then
            fail "$tool: could not determine the upstream latest release"
            failed=1
            continue
        fi
        if [[ -z "$latest" ]]; then
            fail "$tool: upstream has no usable release (every version yanked or a prerelease)"
            failed=1
            continue
        fi
        if version_gt "$latest" "$pinned"; then
            drifted=1
            src=$(source_for "$tool")
            rows="${rows}| \`$tool\` | $pinned | $latest | \`$src\` |"$'\n'
        fi
    done < <(pinned_tools)

    if [[ -f "$node_version_file" ]]; then
        local node_pin node_major node_latest
        node_pin=$(tr -d '[:space:]' <"$node_version_file")
        node_major="${node_pin%%.*}"
        if ! node_latest=$(latest_node_patch_in_major "$node_major"); then
            fail "node: could not determine the upstream latest release for major $node_major"
            failed=1
        elif [[ -z "$node_latest" ]]; then
            fail "node: upstream lists no release under major $node_major"
            failed=1
        elif version_gt "$node_latest" "$node_pin"; then
            drifted=1
            rows="${rows}| \`node\` | $node_pin | $node_latest | \`node (major $node_major)\` |"$'\n'
        fi
    fi

    if [[ "$failed" -ne 0 ]]; then
        fail "one or more upstream queries failed; reporting nothing rather than a partial table"
        return 1
    fi

    local table
    table=$(table_header)
    if [[ -n "$rows" ]]; then
        table="${table}
${rows%$'\n'}"
    else
        table='No pinned tool is behind its upstream latest release.'
    fi

    printf '%s\n' "$table"
    if [[ -n "$table_out" ]]; then
        printf '%s\n' "$table" >"$table_out"
    fi
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        if [[ "$drifted" -ne 0 ]]; then
            echo "drifted=true" >>"$GITHUB_OUTPUT"
        else
            echo "drifted=false" >>"$GITHUB_OUTPUT"
        fi
    fi
}

# ---------------------------------------------------------------------------
# The filed issue's body, conforming to .github/ISSUE_TEMPLATE/2-bug.yml.
# ---------------------------------------------------------------------------

render_issue_body() { # sha, run_url, table_file
    local sha="$1" run_url="$2" table_file="$3" table
    table=$(cat "$table_file")
    # Every backtick below is Markdown, not a command substitution: single
    # quotes keep them literal.
    # shellcheck disable=SC2016
    printf '%s\n' \
        '### Where' \
        '' \
        'Build, CI, or the examples' \
        '' \
        '### Version' \
        '' \
        "$sha" \
        '' \
        '### Cargo features' \
        '' \
        'n/a (CI configuration)' \
        '' \
        '### Rust version and platform' \
        '' \
        'n/a (CI configuration)' \
        '' \
        '### What happened' \
        '' \
        'The weekly scheduled run compares every tool version pinned in `versions.mk`,' \
        'plus `.node-version`, against its upstream latest release. This run found at' \
        'least one pin behind upstream.' \
        '' \
        "Run: $run_url" \
        '' \
        "$table" \
        '' \
        '### What you expected instead' \
        '' \
        'Every pinned version tracks upstream, or a maintainer records that it stays' \
        'fixed and why.'
}

# True (exit 0) when every line of `rendered` (newline-separated) appears in
# `template` (newline-separated) in the same relative order; `rendered` need
# not be the whole of `template`. Proves a heading is both real and never
# reordered past the one before it.
ordered_subset() { # rendered, template
    local rendered="$1" template="$2"
    local -a tmpl=()
    local line
    while IFS= read -r line; do
        [[ -n "$line" ]] && tmpl+=("$line")
    done <<<"$template"
    local idx=0 h found
    while IFS= read -r h; do
        [[ -n "$h" ]] || continue
        found=0
        while [[ "$idx" -lt "${#tmpl[@]}" ]]; do
            if [[ "${tmpl[idx]}" == "$h" ]]; then
                found=1
                idx=$((idx + 1))
                break
            fi
            idx=$((idx + 1))
        done
        [[ "$found" -eq 1 ]] || return 1
    done <<<"$rendered"
    return 0
}

# The `label:` value of every field in the bug-report form, in declaration
# order.
bug_template_labels() {
    sed -n 's/^[[:space:]]*label:[[:space:]]*//p' .github/ISSUE_TEMPLATE/2-bug.yml
}

# ---------------------------------------------------------------------------
# --self-test
# ---------------------------------------------------------------------------

scratch=""

self_test() {
    local errs=0 got status

    check_eq() { # want, desc, command...
        local want="$1" desc="$2"
        shift 2
        got=$("$@" 2>&1) || true
        if [[ "$got" != "$want" ]]; then
            echo "::error::$desc: expected '$want', got '$got'" >&2
            errs=$((errs + 1))
        fi
    }

    check_eq "cargo-llvm-cov" "tool_name_from_var reverses tool-versions.sh's mapping" \
        tool_name_from_var CARGO_LLVM_COV

    scratch=$(mktemp -d)
    trap 'rm -rf "$scratch"' EXIT

    # --- pinned_tools --------------------------------------------------
    cat >"$scratch/versions.mk" <<'EOF'
# a comment line, like the real file
CARGO_HACK_VERSION := 0.6.45
NEXTEST_VERSION  :=  0.9.140
EOF
    versions_file="$scratch/versions.mk"
    got=$(pinned_tools | tr '\n' '|')
    if [[ "$got" != "cargo-hack 0.6.45|nextest 0.9.140|" ]]; then
        echo "::error::pinned_tools misread versions.mk: got '$got'" >&2
        errs=$((errs + 1))
    fi
    versions_file="${SPATE_VERSIONS_MK:-versions.mk}"

    # --- index_path_for --------------------------------------------------
    check_eq "1/a" "a one-character crate name" index_path_for a
    check_eq "2/ab" "a two-character crate name" index_path_for ab
    check_eq "3/a/abc" "a three-character crate name" index_path_for abc
    check_eq "ab/cd/abcd" "a four-character crate name" index_path_for abcd
    check_eq "ca/rg/cargo-hack" "cargo-hack's own prefix" index_path_for cargo-hack
    check_eq "ca/rg/cargo-nextest" "nextest's crate name, not its tool name" \
        index_path_for cargo-nextest

    # --- version_gt / highest_version --------------------------------------
    # nextest's live case: a plain string compare puts "0.9.99" ahead of
    # "0.9.140", because '1' sorts below '9' in the third component.
    # shellcheck disable=SC2050  # a fixed string comparison, demonstrating the trap
    if [[ ! "0.9.99" > "0.9.140" ]]; then
        echo "::error::the fixture no longer demonstrates the string-compare trap this guards" >&2
        errs=$((errs + 1))
    fi
    if ! version_gt "0.9.140" "0.9.99"; then
        echo "::error::version_gt got nextest's own 0.9.140 vs 0.9.99 backwards" >&2
        errs=$((errs + 1))
    fi
    if version_gt "0.9.99" "0.9.140"; then
        echo "::error::version_gt called 0.9.99 greater than 0.9.140" >&2
        errs=$((errs + 1))
    fi
    if version_gt "1.2.3" "1.2.3"; then
        echo "::error::version_gt called two equal versions different" >&2
        errs=$((errs + 1))
    fi
    got=$(printf '0.9.99\n0.9.140\n' | highest_version)
    if [[ "$got" != "0.9.140" ]]; then
        echo "::error::highest_version ordered the nextest pair lexically: got '$got'" >&2
        errs=$((errs + 1))
    fi
    got=$(printf '0.9.140\n0.9.99\n' | highest_version)
    if [[ "$got" != "0.9.140" ]]; then
        echo "::error::highest_version is sensitive to input order: got '$got'" >&2
        errs=$((errs + 1))
    fi

    # --- source_for ----------------------------------------------------
    mkdir -p "$scratch/versions.d/cargo-hack" "$scratch/versions.d/nextest" "$scratch/versions.d/shellcheck"
    printf '# a comment\ncrate:cargo-hack\n' >"$scratch/versions.d/cargo-hack/SOURCE"
    printf 'crate:cargo-nextest\n' >"$scratch/versions.d/nextest/SOURCE"
    printf 'github:koalaman/shellcheck\n' >"$scratch/versions.d/shellcheck/SOURCE"
    sources_root="$scratch/versions.d"
    check_eq "crate:cargo-hack" "source_for strips a leading comment" source_for cargo-hack
    check_eq "crate:cargo-nextest" "nextest's declared source is its crate name, not its tool name" \
        source_for nextest
    check_eq "github:koalaman/shellcheck" "shellcheck's declared source is a GitHub repo" \
        source_for shellcheck
    if source_for not-a-tool >/dev/null 2>&1; then
        echo "::error::source_for accepted a tool with no versions.d entry" >&2
        errs=$((errs + 1))
    fi
    sources_root="${SPATE_TOOL_SOURCES:-versions.d}"

    # --- every real versions.mk tool has a real versions.d declaration -----
    # A tool bump needs no edit here; a new pinned tool does, and this is what
    # catches the one left out.
    local tool
    while read -r tool _; do
        [[ -n "$tool" ]] || continue
        if ! source_for "$tool" >/dev/null 2>&1; then
            echo "::error::versions.mk pins $tool, and versions.d/$tool/SOURCE is missing" >&2
            errs=$((errs + 1))
        fi
    done < <(pinned_tools)

    # --- latest_crate_version / latest_github_release / latest_node_patch_in_major ---
    mkdir -p "$scratch/fixtures"
    fixtures_dir="$scratch/fixtures"

    cat >"$scratch/fixtures/$(fixture_name_for 'https://index.crates.io/ca/rg/cargo-hack')" <<'EOF'
{"name":"cargo-hack","vers":"0.6.44","yanked":false}
{"name":"cargo-hack","vers":"0.6.45","yanked":false}
{"name":"cargo-hack","vers":"0.6.46","yanked":true}
{"name":"cargo-hack","vers":"0.7.0-beta.1","yanked":false}
EOF
    check_eq "0.6.45" "latest_crate_version skips a yanked release and a prerelease" \
        latest_crate_version cargo-hack

    cat >"$scratch/fixtures/$(fixture_name_for 'https://api.github.com/repos/koalaman/shellcheck/releases/latest')" <<'EOF'
{"tag_name":"v0.11.5"}
EOF
    check_eq "0.11.5" "latest_github_release strips the tag's leading v" \
        latest_github_release koalaman/shellcheck

    cat >"$scratch/fixtures/$(fixture_name_for 'https://nodejs.org/dist/index.json')" <<'EOF'
[
  {"version":"v25.1.0"},
  {"version":"v24.21.0"},
  {"version":"v24.22.1"},
  {"version":"v22.9.0"}
]
EOF
    check_eq "24.22.1" "latest_node_patch_in_major stays within the pinned major" \
        latest_node_patch_in_major 24

    fixtures_dir=""

    # --- run_check: the failure-vs-report distinction ----------------------
    mkdir -p "$scratch/run/versions.d/toolok" "$scratch/run/fixtures"
    cat >"$scratch/run/versions.mk" <<'EOF'
TOOLOK_VERSION := 1.0.0
EOF
    printf 'crate:toolok\n' >"$scratch/run/versions.d/toolok/SOURCE"
    cat >"$scratch/run/fixtures/$(fixture_name_for 'https://index.crates.io/to/ol/toolok')" <<'EOF'
{"name":"toolok","vers":"1.0.0","yanked":false}
{"name":"toolok","vers":"1.1.0","yanked":false}
EOF
    versions_file="$scratch/run/versions.mk"
    sources_root="$scratch/run/versions.d"
    fixtures_dir="$scratch/run/fixtures"
    node_version_file="$scratch/run/no-such-node-version"

    local out_file="$scratch/run/table.md" gh_out="$scratch/run/gh-output"

    : >"$gh_out"
    GITHUB_OUTPUT="$gh_out"
    status=0
    run_check "$out_file" >/dev/null || status=$?
    unset GITHUB_OUTPUT
    if [[ "$status" -ne 0 ]]; then
        echo "::error::run_check failed on a resolvable query that found real drift" >&2
        errs=$((errs + 1))
    fi
    if ! grep -q 'drifted=true' "$gh_out"; then
        echo "::error::run_check did not report drifted=true for toolok 1.0.0 -> 1.1.0" >&2
        errs=$((errs + 1))
    fi
    if ! grep -q 'toolok' "$out_file"; then
        echo "::error::run_check's table omitted the drifted tool" >&2
        errs=$((errs + 1))
    fi

    # No drift: the same tool, pinned at what upstream now shows as latest.
    cat >"$scratch/run/fixtures/$(fixture_name_for 'https://index.crates.io/to/ol/toolok')" <<'EOF'
{"name":"toolok","vers":"1.0.0","yanked":false}
EOF
    : >"$gh_out"
    GITHUB_OUTPUT="$gh_out"
    status=0
    run_check "$out_file" >/dev/null || status=$?
    unset GITHUB_OUTPUT
    if [[ "$status" -ne 0 ]]; then
        echo "::error::run_check failed when nothing had drifted" >&2
        errs=$((errs + 1))
    fi
    if ! grep -q 'drifted=false' "$gh_out"; then
        echo "::error::run_check did not report drifted=false when nothing moved" >&2
        errs=$((errs + 1))
    fi

    # An unreachable source: this must fail the check and write no verdict at
    # all, never a false "nothing moved" and never a false "it moved".
    rm -f "$scratch/run/fixtures/$(fixture_name_for 'https://index.crates.io/to/ol/toolok')"
    : >"$gh_out"
    GITHUB_OUTPUT="$gh_out"
    status=0
    run_check "$out_file" >/dev/null 2>&1 || status=$?
    unset GITHUB_OUTPUT
    if [[ "$status" -eq 0 ]]; then
        echo "::error::run_check succeeded despite an unresolvable upstream query" >&2
        errs=$((errs + 1))
    fi
    if [[ -s "$gh_out" ]]; then
        echo "::error::run_check wrote a drifted= verdict despite a failed query" >&2
        errs=$((errs + 1))
    fi

    versions_file="${SPATE_VERSIONS_MK:-versions.mk}"
    sources_root="${SPATE_TOOL_SOURCES:-versions.d}"
    fixtures_dir="${SPATE_TOOL_DRIFT_FIXTURES:-}"
    node_version_file="${SPATE_NODE_VERSION_FILE:-.node-version}"

    # --- render_issue_body conforms to the bug-report template -------------
    echo '| Tool | Pinned | Latest | Source |' >"$scratch/table.md"
    local body rendered_headings template_labels
    body=$(render_issue_body abc1234 'https://example.invalid/run/1' "$scratch/table.md")
    rendered_headings=$(printf '%s\n' "$body" | sed -n 's/^### //p')
    template_labels=$(bug_template_labels)

    if ! ordered_subset "$rendered_headings" "$template_labels"; then
        echo "::error::render_issue_body's headings are not an ordered subset of 2-bug.yml's labels" >&2
        errs=$((errs + 1))
    fi

    # Mutation proof: ordered_subset itself must reject what it exists to
    # catch, an edit to 2-bug.yml this check would otherwise miss.
    if ordered_subset "$(printf '%s\n' 'What happened' 'Where')" "$template_labels"; then
        echo "::error::ordered_subset accepted an out-of-order heading list" >&2
        errs=$((errs + 1))
    fi
    if ordered_subset "$(printf '%s\n' 'Not a real field')" "$template_labels"; then
        echo "::error::ordered_subset accepted a heading absent from the template" >&2
        errs=$((errs + 1))
    fi

    rm -rf "$scratch"
    trap - EXIT
    scratch=""

    if [[ "$errs" -gt 0 ]]; then
        return 1
    fi
    echo "tool-drift.sh: self-test passed"
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

case "${1:-}" in
--self-test)
    self_test
    exit
    ;;
--check)
    shift
    table_out=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
        --table-out)
            table_out="${2:-}"
            [[ -n "$table_out" ]] || { fail "--table-out needs a path"; exit 2; }
            shift 2
            ;;
        *)
            fail "unknown flag: $1"
            exit 2
            ;;
        esac
    done
    run_check "$table_out"
    ;;
--render-issue-body)
    shift
    sha="" run_url="" table_file=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
        --sha)
            sha="${2:-}"
            shift 2
            ;;
        --run-url)
            run_url="${2:-}"
            shift 2
            ;;
        --table)
            table_file="${2:-}"
            shift 2
            ;;
        *)
            fail "unknown flag: $1"
            exit 2
            ;;
        esac
    done
    if [[ -z "$sha" || -z "$run_url" || -z "$table_file" ]]; then
        fail "usage: $0 --render-issue-body --sha SHA --run-url URL --table FILE"
        exit 2
    fi
    render_issue_body "$sha" "$run_url" "$table_file"
    ;;
*)
    fail "usage: $0 --check [--table-out FILE] | --render-issue-body --sha SHA --run-url URL --table FILE | --self-test"
    exit 2
    ;;
esac
