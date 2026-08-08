#!/usr/bin/env bash
#
# Generates crates/spate/examples/README.md, and holds it to the tree.
#
# The root README links a reader straight at the examples directory, so what
# they meet there is a map rather than an unannotated file listing. Keeping
# that map true needs a machine, because the failure is silent — an example
# nobody can find is indistinguishable from an example that does not exist,
# and no build breaks.
#
# Generated from a block in each example's own header:
#
#   // INDEX-TIER:  getting-started
#   // INDEX-GOAL:  build, drive and assert on a whole pipeline
#   // INDEX-TECH:  no infrastructure
#   // INDEX-NEEDS: nothing
#
# An optional `// INDEX-RANK: <0-999>` orders an example within its tier;
# unranked examples take 50 and fall back to name order. A field's value is the
# rest of its line — there is no trailing-comment syntax — and a value carrying
# a `|` is rejected, because it would split the table row it renders into.
#
# Plain `//` comments, deliberately: they sit outside the `//!` module docs, so
# they never reach docs.rs, and they read the same way as the `ANCHOR:` markers
# already in these files. The `required-features` column is NOT among them — it
# is in the manifest already, and a field duplicating the manifest is a field
# that disagrees with it.
#
# Fields rather than free prose is the whole design: a wording change to the
# commentary around these lines cannot fail anything, while a missing or
# orphaned entry always does.
#
# The tier here is the *reading* tier — where an example sits in the guide. It
# is a different axis from examples.sh's `free`/`infra`, which is whether a
# pull request can run it. The two diverge as soon as an example needs no
# infrastructure and still belongs under production pipelines.
#
# Usage:
#   ./scripts/examples-index.sh --write      # regenerate the file
#   ./scripts/examples-index.sh --check      # fail if it is out of date
#   ./scripts/examples-index.sh --self-test  # the parsers, against fixtures
set -euo pipefail

cd "$(dirname "$0")/.."

PKG_DIR="crates/spate"
MANIFEST="$PKG_DIR/Cargo.toml"
EXAMPLES_DIR="$PKG_DIR/examples"
INDEX="$EXAMPLES_DIR/README.md"

# Reading tiers, in the order they appear in the file.
TIER_SLUGS="getting-started production bounded-jobs operating extending"

# The fields every example declares, in the order the table renders them.
REQUIRED_FIELDS="TIER GOAL TECH NEEDS"

# Examples the generated header names in prose. The tables are rendered from
# the tree and cannot go stale; these are hand-written and are asserted instead.
HEADER_EXAMPLES="memory_pipeline kafka_avro_to_clickhouse"

err() {
    echo "::error::examples-index.sh: $1" >&2
}

# Scratch space for the render, removed however the script leaves. Global
# rather than local to each caller: the trap runs after the function that set
# it has returned, so a `local` would be out of scope by then and `set -u`
# would turn the cleanup itself into the failure.
SCRATCH=""
cleanup() {
    [[ -z "$SCRATCH" ]] || rm -rf "$SCRATCH"
    return 0
}
trap cleanup EXIT

tier_heading() {
    case "$1" in
        getting-started) echo "1. Getting started" ;;
        production)      echo "2. Production pipelines" ;;
        bounded-jobs)    echo "3. Bounded jobs and scaling out" ;;
        operating)       echo "4. Operating" ;;
        extending)       echo "5. Extending" ;;
        *)               echo "" ;;
    esac
}

tier_blurb() {
    case "$1" in
        getting-started) echo "No infrastructure. Read them in this order." ;;
        production)      echo "The shapes a real deployment takes." ;;
        bounded-jobs)    echo "Work that finishes, and work shared across instances." ;;
        operating)       echo "What the pipeline tells you, and what it does when something breaks." ;;
        extending)       echo "Writing your own components against the v1 contracts." ;;
        *)               echo "" ;;
    esac
}

# One `INDEX-<FIELD>:` value from an example source. Empty when absent, and the
# first occurrence when repeated — `validate` refuses a repeat rather than
# letting a stale copy above the live block win.
index_field() {
    awk -v field="$2" '
        $0 ~ "^//[[:space:]]*INDEX-" field ":" {
            sub("^//[[:space:]]*INDEX-" field ":[[:space:]]*", "")
            sub(/[[:space:]]+$/, "")
            print
            exit
        }
    ' "$1"
}

# How many times an example declares one field.
field_occurrences() {
    awk -v field="$2" '
        $0 ~ "^//[[:space:]]*INDEX-" field ":" { n++ }
        END { print n + 0 }
    ' "$1"
}

# `required-features` for one example, rendered for the table. Reads $2 when
# given, so the self-test can point it at a fixture.
manifest_features() {
    awk -v want="$1" '
        /^\[\[example\]\]/ { in_ex = 1; name = ""; next }
        /^\[/              { in_ex = 0 }
        in_ex && /^name[[:space:]]*=/ {
            line = $0
            gsub(/^name[[:space:]]*=[[:space:]]*"/, "", line)
            gsub(/".*$/, "", line)
            name = line
        }
        # The array may wrap across lines, so the value is accumulated until
        # its closing bracket. Reading it off the opening line renders an empty
        # Features column — which reads as "needs no flags", the one wrong
        # answer this column can give.
        in_ex && name == want && /^required-features[[:space:]]*=/ { taking = 1; buf = "" }
        taking {
            buf = buf $0
            if (buf ~ /\]/) {
                sub(/^[^[]*\[/, "", buf)
                sub(/\].*$/, "", buf)
                gsub(/"/, "", buf)
                gsub(/[[:space:]]/, "", buf)
                sub(/,$/, "", buf)
                print buf
                exit
            }
        }
    ' "${2:-$MANIFEST}"
}

# `LC_ALL=C` on every sort here: the generated order has to be the same on a
# contributor's machine as in CI, and a UTF-8 collation orders `_` against a
# letter differently from a byte comparison.
example_names() {
    find "$EXAMPLES_DIR" -maxdepth 1 -name '*.rs' -exec basename {} .rs \; | LC_ALL=C sort
}

# The examples in one tier, ordered by `INDEX-RANK` (default 50) and then by
# name, so a tier that claims a reading order has one.
tier_members() {
    local name rank
    for name in $(example_names); do
        [[ "$(index_field "$EXAMPLES_DIR/$name.rs" TIER)" == "$1" ]] || continue
        rank="$(index_field "$EXAMPLES_DIR/$name.rs" RANK)"
        [[ -n "$rank" ]] || rank=50
        printf '%03d %s\n' "$rank" "$name"
    done | LC_ALL=C sort | awk '{ print $2 }'
}

# Everything the rendered file cannot express a defect in. Runs before both
# `--write` and `--check`, because a blank or split cell renders happily and
# the diff then accepts it for good.
validate() {
    local name field value rank count bad=0

    count=0
    for name in $(example_names); do
        count=$((count + 1))
        for field in $REQUIRED_FIELDS; do
            if [[ "$(field_occurrences "$EXAMPLES_DIR/$name.rs" "$field")" -gt 1 ]]; then
                err "$name declares \`// INDEX-$field:\` more than once; the first one wins"
                bad=1
                continue
            fi
            value="$(index_field "$EXAMPLES_DIR/$name.rs" "$field")"
            if [[ -z "$value" ]]; then
                err "$EXAMPLES_DIR/$name.rs has no \`// INDEX-$field:\` line"
                bad=1
            elif [[ "$value" == *"|"* ]]; then
                err "$name's INDEX-$field carries a \`|\`, which splits its table row"
                bad=1
            fi
        done

        if [[ -z "$(tier_heading "$(index_field "$EXAMPLES_DIR/$name.rs" TIER)")" ]]; then
            err "$name declares an unknown INDEX-TIER; known tiers are $TIER_SLUGS"
            bad=1
        fi

        rank="$(index_field "$EXAMPLES_DIR/$name.rs" RANK)"
        if [[ -n "$rank" && ! "$rank" =~ ^[0-9]{1,3}$ ]]; then
            err "$name declares INDEX-RANK \`$rank\`; a rank is 0-999, and a value printf cannot read sorts to the front of its tier"
            bad=1
        fi
    done

    # A search that stops matching renders empty tiers, and the diff below
    # would accept them against an equally empty file.
    if [[ "$count" -lt 10 ]]; then
        err "only $count example(s) found under $EXAMPLES_DIR; the search has stopped matching"
        bad=1
    fi

    for name in $HEADER_EXAMPLES; do
        [[ -f "$EXAMPLES_DIR/$name.rs" ]] && continue
        err "the generated header names \`$name\`, which no longer exists"
        bad=1
    done

    [[ "$bad" -eq 0 ]]
}

render() {
    local slug name goal tech needs feats heading

    cat <<'HEADER'
<!--
Generated by scripts/examples-index.sh from the INDEX- block in each example's
header. Do not edit by hand: `make check-examples-index` compares this file
against the tree and fails with the command that regenerates it.
-->

# Examples

Every example is a whole program: build it, run it, read what it printed. Each
one's header comment says what it demonstrates and what it needs, and the
tables below group them by what you are trying to do.

New here, read [Getting started](#1-getting-started) in order. Nothing outside
that tier depends on reading order — pick the one matching your task.

## Running one

```sh
cargo run -p spate --example memory_pipeline
cargo run --release -p spate --features full --example kafka_avro_to_clickhouse
```

An example whose **Features** column is not `—` needs those features on the
command line. Naming one without them fails with exactly what is missing:

```text
error: target `s3_backfill` in package `spate` requires the features: `s3`, `json`
```

Examples that talk to real servers ship no `docker-compose`. Each one's header
comment names exactly what it needs and which environment variables point at
it, and every shipped config reads its endpoints through `${VAR:-default}`, so
the file you run against your own servers is the file in this directory.

Configuration comes from `SPATE_CONFIG` where an example loads YAML from disk:

```sh
SPATE_CONFIG=/etc/spate/pipeline.yaml cargo run --release -p spate \
  --features full --example kafka_avro_to_clickhouse
```

## The storefront stream

`spate-datagen` generates one dataset with nothing installed behind it: a
storefront whose payments and refunds name orders that were really placed, for
amounts matching their lines.

```text
order_placed     { order_id, customer_id, region, placed_at, lines: [{ sku, qty, unit_cents }] }
payment_captured { order_id, amount_cents }
refund_issued    { order_id, amount_cents, reason }
```

The nested `lines` array is what `flat_map` fans out, and the three event kinds
are what a split terminal separates. Sharding keys on the **order id**, which
the generator sets as each record's key: a payment and a refund carry only the
`order_id` of the order they settle, so that is the only field all three share
and the only one that can colocate them on a shard.

The types are `spate_datagen::storefront`, so an example and your own code can
share them.

HEADER

    for slug in $TIER_SLUGS; do
        heading="$(tier_heading "$slug")"
        printf '## %s\n\n%s\n\n' "$heading" "$(tier_blurb "$slug")"
        printf '| Example | What it shows | Features | Needs |\n'
        printf '|---|---|---|---|\n'
        # Sorted by rank, then by name. Rank is what lets the first tier say
        # "read them in this order" and mean it; everywhere else it is absent
        # and the tier falls back to alphabetical, which is a stable order
        # rather than a meaningful one.
        for name in $(tier_members "$slug"); do
            goal="$(index_field "$EXAMPLES_DIR/$name.rs" GOAL)"
            tech="$(index_field "$EXAMPLES_DIR/$name.rs" TECH)"
            needs="$(index_field "$EXAMPLES_DIR/$name.rs" NEEDS)"
            feats="$(manifest_features "$name")"
            if [[ -n "$feats" ]]; then
                feats='`'"$feats"'`'
            else
                feats="—"
            fi
            # The backticks in the format string are markdown, not command
            # substitution — the format is single-quoted precisely so nothing
            # in it expands.
            # shellcheck disable=SC2016
            printf '| [`%s`](%s.rs) | How to %s with **%s** | %s | %s |\n' \
                "$name" "$name" "$goal" "$tech" "$feats" "$needs"
        done
        printf '\n'
    done

    # The container link is absolute: this file ships inside the published
    # crate, where a relative path climbing out of `crates/spate` resolves to
    # nothing.
    cat <<'FOOTER'
## Containers

[`examples/docker`](https://github.com/spate-etl/spate/tree/main/examples/docker)
builds the flagship example into a distroless image, and its README covers
probes, drain timeouts and sizing.

## Related

- [User guide](https://spate.kainth.dev/docs/user-guide/) — the concepts these
  examples are worked instances of
- [`spate` on docs.rs](https://docs.rs/spate) — every type and feature named here
- [`spate-test`](https://docs.rs/spate-test) — the in-memory source and sink the
  infrastructure-free examples are built on
FOOTER
}

run_check() {
    local rc

    validate || exit 1

    if [[ ! -f "$INDEX" ]]; then
        err "$INDEX does not exist; run \`make examples-index\`"
        exit 1
    fi

    SCRATCH="$(mktemp)"
    render > "$SCRATCH"
    rc=0
    diff -u "$INDEX" "$SCRATCH" || rc=$?
    if [[ "$rc" -ne 0 ]]; then
        err "$INDEX is out of date; run \`make examples-index\`"
        exit 1
    fi
    echo "examples-index.sh: $INDEX matches the tree."
}

run_write() {
    validate || exit 1

    SCRATCH="$(mktemp)"
    render > "$SCRATCH"
    # `mktemp` creates 0600 and `mv` carries the mode across, so a generated
    # file left unreadable to everyone else is the default without this.
    chmod 644 "$SCRATCH"
    mv "$SCRATCH" "$INDEX"
    echo "examples-index.sh: wrote $INDEX."
}

# The parsers, against fixtures. Every case here is one the rendered file
# accepts silently: a wrong value renders a plausible row, and the diff then
# holds the tree to it.
run_self_test() {
    local out

    SCRATCH="$(mktemp -d)"

    cat > "$SCRATCH/ex.rs" <<'FIXTURE'
//! Module docs, which must not be read as index fields.
//!
//! INDEX-GOAL: this line is inside the module docs and must be ignored

// INDEX-TIER:  operating
// INDEX-GOAL:  count what an operator you wrote is doing
// INDEX-TECH:  no infrastructure
// INDEX-NEEDS: nothing
fn main() {}
FIXTURE

    out="$(index_field "$SCRATCH/ex.rs" TIER)/$(index_field "$SCRATCH/ex.rs" NEEDS)"
    if [[ "$out" != "operating/nothing" ]]; then
        err "--self-test: index_field parsed '$out'"
        return 1
    fi

    # A `//!` line must not satisfy a field: the module docs are prose and
    # would otherwise silently win, since they come first in the file.
    out="$(index_field "$SCRATCH/ex.rs" GOAL)"
    if [[ "$out" != "count what an operator you wrote is doing" ]]; then
        err "--self-test: a //! line was read as a field: '$out'"
        return 1
    fi

    out="$(index_field "$SCRATCH/ex.rs" ABSENT)"
    if [[ -n "$out" ]]; then
        err "--self-test: an absent field parsed '$out'"
        return 1
    fi

    # A repeated field is what `validate` refuses; the count is what it reads.
    if [[ "$(field_occurrences "$SCRATCH/ex.rs" GOAL)" != "1" || \
          "$(field_occurrences "$SCRATCH/ex.rs" ABSENT)" != "0" ]]; then
        err "--self-test: field_occurrences miscounted a single and an absent field"
        return 1
    fi
    printf '// INDEX-GOAL:  a stale copy left above the live block\n' >> "$SCRATCH/ex.rs"
    if [[ "$(field_occurrences "$SCRATCH/ex.rs" GOAL)" != "2" ]]; then
        err "--self-test: a repeated field was not counted twice"
        return 1
    fi

    # `required-features` written across lines, which the manifest is free to
    # do and which renders an empty Features column when it is read off the
    # opening line alone.
    cat > "$SCRATCH/Cargo.toml" <<'FIXTURE'
[[example]]
name = "one_line"
required-features = ["s3", "json"]

[[example]]
name = "wrapped"
required-features = [
    "s3",
    "json",
    "coordination",
]

[[example]]
name = "bare"
test = true

[features]
s3 = []
FIXTURE

    out="$(manifest_features one_line "$SCRATCH/Cargo.toml")"
    if [[ "$out" != "s3,json" ]]; then
        err "--self-test: a single-line required-features parsed '$out'"
        return 1
    fi
    out="$(manifest_features wrapped "$SCRATCH/Cargo.toml")"
    if [[ "$out" != "s3,json,coordination" ]]; then
        err "--self-test: a wrapped required-features parsed '$out'"
        return 1
    fi
    out="$(manifest_features bare "$SCRATCH/Cargo.toml")"
    if [[ -n "$out" ]]; then
        err "--self-test: a stanza with no required-features parsed '$out'"
        return 1
    fi

    echo "examples-index.sh: self-test passed — module docs cannot satisfy a field, a repeat is countable, and a wrapped required-features array parses."
}

case "${1:---check}" in
    --check) run_check ;;
    --write) run_write ;;
    --self-test) run_self_test ;;
    *)
        echo "usage: $0 [--check|--write|--self-test]" >&2
        exit 2
        ;;
esac
