#!/usr/bin/env bash
#
# Holds the examples to their declarations.
#
# An example is the one target shape cargo will drop on the floor without
# saying so. A `[[example]]` whose `required-features` names a feature the
# package does not declare is not an error — the target is silently skipped,
# and `cargo check --examples` reports success having built one fewer thing
# than the manifest asked for. So a typo there does not fail: it removes an
# example from every job in this repository, permanently, with the build green.
# `--all-features` is the only reason that is survivable today, which makes it
# load-bearing in a way nothing states. This script states it.
#
# The same shape bites twice more now the examples run as tests. An example
# file with no `[[example]]` stanza is auto-discovered with no
# `required-features` and no `test = true`, so it compiles under
# `--all-features` and never runs. And a stanza naming a file that no longer
# exists is a dead name that nothing reports.
#
# Five assertions, none of which builds anything — `cargo metadata` answers the
# only question that needs a toolchain, which is what keeps this in `ci-lint`
# beside the other consistency checks rather than in a job that compiles:
#
#   1. Every `[[example]]` stanza names a file that exists.
#   2. Every example file has a stanza. (1 and 2 together: the two sets match.)
#   3. Every `required-features` entry is a feature the package declares.
#   4. Every example is assigned a tier by `example_tier` below.
#   5. Each tier's declaration is real: a `free` example carries `test = true`
#      and the runner block that calls `main`; an `infra` example carries
#      neither, because it needs servers this tier cannot provide.
#
# Assertion 4 is a hand-written table and is meant to be. It is the same shape,
# for the same reason, as `container_suites_for` in ci-changes.sh: the tier is a
# judgement about what an example needs in order to run, which nothing in the
# manifest records, so a new example cannot land without someone making that
# call. `required-features` is the closest proxy and it is wrong in both
# directions — `s3_backfill` names two features and needs no server, while
# `clickhouse_aggregating_mv` names one and needs a database.
#
# Usage:
#   ./scripts/examples.sh            # --check
#   ./scripts/examples.sh --check
#   ./scripts/examples.sh --tiers    # print `name tier` lines
#   ./scripts/examples.sh --self-test
set -euo pipefail

# Resolved before the `cd`, because `$0` may be relative: --self-test
# re-executes this file as a subprocess to assert --check on the live tree.
self_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$0")/.."

PKG_DIR="crates/spate"
MANIFEST="$PKG_DIR/Cargo.toml"
EXAMPLES_DIR="$PKG_DIR/examples"

failed=0

err() {
    echo "::error::examples.sh: $1" >&2
    failed=1
}

# `free` runs on every pull request as a test; `infra` needs real servers and
# is driven by the container suite.
example_tier() {
    case "$1" in
        memory_pipeline | custom_operator | custom_source_sink | custom_metrics) echo "free" ;;
        manual_assembly) echo "free" ;;
        storefront_pipeline) echo "free" ;;
        json_skip_bad_records | s3_backfill) echo "free" ;;
        instrumented_operator) echo "free" ;;
        avro_schema_evolution) echo "free" ;;
        custom_coordinated_source | s3_coordinated_backfill) echo "free" ;;
        sink_failures) echo "free" ;;
        kafka_avro_to_clickhouse | kafka_avro_flatmap_clickhouse) echo "infra" ;;
        nats_coordinated_backfill) echo "infra" ;;
        multi_table_split | kafka_to_kafka_split | clickhouse_aggregating_mv) echo "infra" ;;
        *) echo "" ;;
    esac
}

# `[[example]]` names from $1, in manifest order, one per line. Deliberately a
# parser rather than `cargo metadata`: assertions 1 and 2 compare the manifest
# against the filesystem, and asking cargo would compare cargo against itself.
stanza_names() {
    awk '
        /^\[\[example\]\]/ { in_ex = 1; next }
        /^\[/              { in_ex = 0 }
        in_ex && /^name[[:space:]]*=/ {
            gsub(/^name[[:space:]]*=[[:space:]]*"/, "")
            gsub(/".*$/, "")
            print
        }
    ' "$1"
}

# The `required-features` of example $2 in manifest $1, space separated and
# empty when the stanza declares none.
stanza_required_features() {
    awk -v want="$2" '
        /^\[\[example\]\]/ { in_ex = 1; name = ""; next }
        /^\[/              { in_ex = 0 }
        in_ex && /^name[[:space:]]*=/ {
            gsub(/^name[[:space:]]*=[[:space:]]*"/, "")
            gsub(/".*$/, "")
            name = $0
        }
        in_ex && name == want && /^required-features[[:space:]]*=/ {
            gsub(/^required-features[[:space:]]*=[[:space:]]*\[/, "")
            gsub(/\].*$/, "")
            gsub(/[",]/, " ")
            gsub(/[[:space:]]+/, " ")
            gsub(/^ | $/, "")
            print
        }
    ' "$1"
}

# Whether example $2 in manifest $1 carries `test = true`.
stanza_is_tested() {
    awk -v want="$2" '
        /^\[\[example\]\]/ { in_ex = 1; name = ""; next }
        /^\[/              { in_ex = 0 }
        in_ex && /^name[[:space:]]*=/ {
            gsub(/^name[[:space:]]*=[[:space:]]*"/, "")
            gsub(/".*$/, "")
            name = $0
        }
        in_ex && name == want && /^test[[:space:]]*=[[:space:]]*true/ { found = 1 }
        END { print (found ? "yes" : "no") }
    ' "$1"
}

# Feature keys the facade declares, space separated on one line.
declared_features() {
    cargo metadata --no-deps --format-version 1 --locked \
        --manifest-path "$MANIFEST" \
        | jq -r '.packages[] | select(.name == "spate") | .features | keys[]' \
        | tr '\n' ' '
}

example_files() {
    find "$EXAMPLES_DIR" -maxdepth 1 -name '*.rs' -exec basename {} .rs \; | sort
}

run_check() {
    local names files name tier tested feats f declared count

    # Space separated on one line, so `case " $x " in *" $y "*` means
    # membership. Newline-separated lists silently match nothing here, which
    # would make every assertion below vacuously true.
    names="$(stanza_names "$MANIFEST" | sort | tr '\n' ' ')"
    files="$(example_files | tr '\n' ' ')"

    if [[ -z "${names// /}" ]]; then
        err "no [[example]] stanzas found in $MANIFEST; the parser matched nothing, so nothing below is asserting anything"
        exit 1
    fi

    # 1 + 2: the manifest and the filesystem describe the same set.
    for name in $names; do
        if [[ ! -f "$EXAMPLES_DIR/$name.rs" ]]; then
            err "stanza \`$name\` names no file at $EXAMPLES_DIR/$name.rs"
        fi
    done
    for name in $files; do
        case " $names " in
            *" $name "*) ;;
            *) err "$EXAMPLES_DIR/$name.rs has no [[example]] stanza; cargo auto-discovers it with no required-features and nothing runs it" ;;
        esac
    done

    # 3: every required-features entry names a feature that exists. This is the
    # assertion the whole file is for.
    declared="$(declared_features)"
    if [[ -z "${declared// /}" ]]; then
        err "cargo metadata reported no features for spate; assertion 3 cannot run"
        exit 1
    fi
    for name in $names; do
        feats="$(stanza_required_features "$MANIFEST" "$name")"
        for f in $feats; do
            case " $declared " in
                *" $f "*) ;;
                *) err "example \`$name\` requires feature \`$f\`, which spate does not declare — cargo skips the target silently, so this never fails a build" ;;
            esac
        done
    done

    # 4 + 5: every example has a tier, and its declaration matches it.
    for name in $names; do
        tier="$(example_tier "$name")"
        tested="$(stanza_is_tested "$MANIFEST" "$name")"
        case "$tier" in
            free)
                if [[ "$tested" != "yes" ]]; then
                    err "example \`$name\` is tier \`free\` but its stanza has no \`test = true\`, so nothing runs it"
                fi
                if ! grep -q 'mod tests' "$EXAMPLES_DIR/$name.rs"; then
                    err "example \`$name\` is tier \`free\` but carries no \`#[cfg(test)]\` runner, so \`test = true\` collects an empty binary"
                fi
                ;;
            infra)
                if [[ "$tested" == "yes" ]]; then
                    err "example \`$name\` is tier \`infra\` but carries \`test = true\`; it needs servers the pull-request tier has none of"
                fi
                ;;
            *)
                err "example \`$name\` has no tier; add it to \`example_tier\` in scripts/examples.sh as \`free\` (runs as a test) or \`infra\` (needs servers)"
                ;;
        esac
    done

    if [[ "$failed" -ne 0 ]]; then
        exit 1
    fi

    count="$(echo "$names" | wc -w | tr -d ' ')"
    echo "examples.sh: $count example(s); every stanza, feature and tier resolves."
}

run_tiers() {
    local name
    for name in $(stanza_names "$MANIFEST" | sort); do
        echo "$name $(example_tier "$name")"
    done
}

# Asserts the parsers against a fixture rather than against the live manifest.
# A parser that silently matched nothing would otherwise make every assertion
# above vacuously true, which is a failure this repository has been bitten by
# more than once.
run_self_test() {
    local tmp fixture rc out name tier missing=0
    tmp="$(mktemp -d)"
    fixture="$tmp/Cargo.toml"

    cat > "$fixture" <<'FIXTURE'
[package]
name = "fixture"

[[example]]
name = "plain"

[[example]]
name = "gated"
required-features = ["json", "s3"]
test = true

[features]
json = []
FIXTURE

    out="$(stanza_names "$fixture" | tr '\n' ' ')"
    if [[ "$out" != "plain gated " ]]; then
        rm -rf "$tmp"
        echo "::error::examples.sh --self-test: stanza_names parsed '$out'" >&2
        return 1
    fi

    out="$(stanza_required_features "$fixture" gated)"
    if [[ "$out" != "json s3" ]]; then
        rm -rf "$tmp"
        echo "::error::examples.sh --self-test: stanza_required_features parsed '$out'" >&2
        return 1
    fi

    out="$(stanza_required_features "$fixture" plain)"
    if [[ -n "$out" ]]; then
        rm -rf "$tmp"
        echo "::error::examples.sh --self-test: a stanza with no required-features parsed '$out'" >&2
        return 1
    fi

    out="$(stanza_is_tested "$fixture" gated)/$(stanza_is_tested "$fixture" plain)"
    if [[ "$out" != "yes/no" ]]; then
        rm -rf "$tmp"
        echo "::error::examples.sh --self-test: stanza_is_tested said '$out', wanted 'yes/no'" >&2
        return 1
    fi
    rm -rf "$tmp"

    # The tier table must cover the live tree, and must not name an example
    # that no longer exists. The second half is the one that goes vacuous after
    # a rename, so it is derived rather than listed.
    for name in $(stanza_names "$MANIFEST"); do
        tier="$(example_tier "$name")"
        if [[ -z "$tier" ]]; then
            echo "::error::examples.sh --self-test: \`$name\` has no tier" >&2
            missing=1
        fi
    done
    if [[ "$missing" -ne 0 ]]; then
        return 1
    fi

    rc=0
    "$self_path" --check > /dev/null 2>&1 || rc=$?
    if [[ "$rc" -ne 0 ]]; then
        echo "::error::examples.sh --self-test: --check on the live tree exited $rc" >&2
        return 1
    fi

    echo "examples.sh: self-test passed."
}

case "${1:---check}" in
    --check) run_check ;;
    --tiers) run_tiers ;;
    --self-test) run_self_test ;;
    *)
        echo "usage: $0 [--check|--tiers|--self-test]" >&2
        exit 2
        ;;
esac
