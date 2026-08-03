#!/usr/bin/env bash
# Refuse a committed benchmark record whose trigger bars publication.
#
#   check-results-publishable.sh [DIR]     # DIR defaults to benchmarks/results
#   check-results-publishable.sh --self-test
#
# `benchmarks/src/report.rs` defines `Trigger::bars_publication` as the single
# authority on which triggers may reach `results/`. This script is the half of
# that contract which runs over the committed tree, and it cannot call Rust —
# so the accepted set is written out below, and a unit test in `report.rs`
# (`the_lint_script_greps_exactly_the_barred_triggers`) reads this file and
# asserts the two agree in both directions.
#
# It is also the backstop for a variant added to the enum and forgotten
# everywhere else: anything outside the accepted set is refused, whether this
# script has heard of it or not. That is why the list below is an allow-list
# rather than a deny-list, and why `BARRED` exists only to name the known
# offenders in the error message.
#
# Why this exists at all: the documentation site draws everything under
# `results/` without knowing which machine was busy, so a number measured in CI
# on a contended runner renders identically to one recorded on a quiet host.
# Provenance travels with every record and describes *where* it came from; this
# is what decides *whether* it may be drawn.
set -euo pipefail

# Every trigger a committed record may carry. Anything else — a barred trigger,
# a misspelling, a JSON `null`, a number — is a refusal.
known_publishable='["manual"]'
# Named only so the error message can say which ones are the expected mistake.
# Kept in step with `Trigger::publication_bar` by the test above.
BARRED=("ci" "dispatched")

# One pass over one file. Prints `record-number:what-was-wrong` per offending
# record, and a `#n` line per record so the caller can count them — the last
# one seen is how many that file held.
#
# `foreach` rather than `reduce`, and one pass rather than two, for two reasons
# that each cost a wrong answer before they were understood:
#
#   - `inputs` is a stream that can be consumed once, so a program with two
#     generators leaves the second with nothing. Counting in a second `reduce
#     inputs` returned zero while looking entirely plausible.
#   - `reduce` emits only when it finishes, so a parse error part-way through a
#     file discarded every offender already found. `foreach` streams, so what
#     was found before the malformed record still reaches the caller.
#
# Records are numbered by position rather than by `input_line_number`, which
# counts newlines *consumed* and so names the previous record when the last one
# has no trailing newline.
#
# Three refusals, not one:
#
#   - a record that is not a JSON object. `null` and `7` are valid JSON, and
#     `null | has("trigger")` is `false` rather than an error, so a bare `null`
#     would otherwise read as `manual`, pass, and count toward the
#     examined-nothing guard while it did.
#   - `has("trigger")` rather than jq's `//`, which treats `null` and `false`
#     as absent: `{"trigger": null}` would read as `manual`. serde rejects it,
#     because `#[serde(default)]` fills in a *missing* key, not a null one.
#   - a trigger outside the accepted set, whatever its type.
#
# The rule behind all three: the gate must not certify a record that
# `benchmarks/src/report.rs` — the schema it defers to — cannot parse.
offenders_in() {
    jq -rn --argjson ok "$known_publishable" '
        foreach inputs as $rec (0; . + 1;
            . as $n
            | (
                if ($rec | type) != "object" then
                    "\($n):is a JSON \($rec | type), not a record"
                else
                    ($rec | if has("trigger") then .trigger else "manual" end) as $t
                    | if ($t | type) != "string" then
                        "\($n):has a \($t | type) trigger, which is not a name"
                      elif ($ok | index($t)) == null then
                        # `tojson` rather than a quoted interpolation: a literal
                        # single quote here would close the shell string that
                        # holds this whole program.
                        "\($n):carries trigger \($t | tojson), which may not be published"
                      else empty end
                end
              ),
              "#\($n)"
        )
    ' "$1"
}

if [[ "${1:-}" == "--self-test" ]]; then
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    fail_self() {
        echo "check-results-publishable.sh --self-test: $1" >&2
        exit 1
    }
    # SPATE_RESULTS_LINT_UNDER_TEST points the fixtures at another copy of this
    # script, which is how a reviewer confirms a fixture is load-bearing by
    # running it against a revision that should not pass it.
    under_test="${SPATE_RESULTS_LINT_UNDER_TEST:-$0}"

    # One directory per case, so no fixture can leak into the next and make a
    # later assertion pass or fail for the wrong reason.
    case_dir() {
        local d="$tmp/$1"
        mkdir -p "$d"
        printf '%s' "$d"
    }
    # Captures rather than discards: a case that is refused for the wrong
    # reason is a fixture that passes without holding anything, so every
    # assertion below can check the message as well as the verdict.
    last_out=""
    accepts() {
        last_out=$("$under_test" "$1" 2>&1)
        return $?
    }
    refuses_saying() { # dir needle description
        if accepts "$1"; then
            fail_self "$3 was accepted"
        fi
        grep -q -- "$2" <<<"$last_out" \
            || fail_self "$3 was refused without saying why (wanted '$2'): $last_out"
    }

    ok=$(case_dir ok)
    printf '%s\n' \
        '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"manual"}' \
        >"$ok/a.jsonl"
    # A record with no trigger at all: what every line committed before the
    # field existed looks like. It reads as `manual` and must pass, or this
    # check would refuse the archive it is meant to protect.
    printf '%s\n' \
        '{"schema":1,"bench":"s3_backfill","kind":"measurement"}' \
        >>"$ok/a.jsonl"
    accepts "$ok" || fail_self "publishable records were refused"

    # Every barred trigger, and a name the script has never heard of.
    for bad in ci dispatched speculative; do
        d=$(case_dir "bad-$bad")
        printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"%s"}\n' \
            "$bad" >"$d/a.jsonl"
        refuses_saying "$d" "may not be published" "a '$bad' record"
    done

    # `null` and `false` are what jq's `//` would silently read as `manual`,
    # and what serde refuses. Neither may pass.
    for bad in null false 7; do
        d=$(case_dir "type-$bad")
        printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":%s}\n' \
            "$bad" >"$d/a.jsonl"
        refuses_saying "$d" "is not a name" "a record with trigger $bad"
    done

    # A record that is not an object at all. `null | has("trigger")` is `false`
    # rather than an error, so a bare `null` would read as `manual`, pass, and
    # count toward the examined-nothing guard while it did. serde rejects every
    # one of these outright.
    i=0
    for bad in 'null' '7' '"manual"' '[]'; do
        i=$((i + 1))
        d=$(case_dir "nonobject-$i")
        printf '%s\n' "$bad" >"$d/a.jsonl"
        refuses_saying "$d" "not a record" "a top-level $bad"
    done

    # An empty object is still an object, and still reads as `manual` — which
    # is what a record predating the field is.
    d=$(case_dir bare-object)
    printf '%s\n' '{}' >"$d/a.jsonl"
    accepts "$d" || fail_self "an object with no trigger was refused: $last_out"

    # A file whose last record has no trailing newline. The offending record is
    # named by its own position, not by the count of newlines consumed before
    # it — which is the previous record's.
    d=$(case_dir no-trailing-newline)
    printf '{"schema":1,"trigger":"manual"}\n{"schema":1,"trigger":"ci"}' >"$d/a.jsonl"
    refuses_saying "$d" "line=2" "the second of two records, with no trailing newline,"

    # A barred record followed by a malformed one. Both must be reported: the
    # offender already found, and the parse failure that stopped the read.
    d=$(case_dir offender-then-malformed)
    printf '%s\n' '{"schema":1,"trigger":"ci"}' '{"schema":1,' >"$d/a.jsonl"
    refuses_saying "$d" "may not be published" "an offender preceding a parse error"
    grep -q "could not be read" <<<"$last_out" \
        || fail_self "a parse error after an offender was not reported: $last_out"

    # A directory with no records at all — no files, or files holding none.
    # Both must be refused rather than reported as passing, or a renamed
    # results directory leaves the gate green having read nothing.
    refuses_saying "$(case_dir empty-dir)" "examined nothing" \
        "a directory with no .jsonl files"
    d=$(case_dir empty-file)
    : >"$d/a.jsonl"
    refuses_saying "$d" "examined nothing" \
        "a directory whose only file holds no records"

    # Malformed JSON is a failure with something readable attached, not a raw
    # jq crash and not a pass.
    d=$(case_dir malformed)
    printf '%s\n' '{"schema":1,' >"$d/a.jsonl"
    refuses_saying "$d" "could not be read" "a malformed record"

    echo "check-results-publishable.sh: self-test ok — barred, unknown, mistyped, non-object, malformed and empty are all refused, each saying why, and a record predating the field still passes"
    exit 0
fi

dir="${1:-benchmarks/results}"
if [[ ! -d "$dir" ]]; then
    echo "::error::$dir is not a directory" >&2
    exit 1
fi

# Materialised rather than streamed into the loop. A `while read` fed by a
# process substitution whose producer died iterates zero times and reports
# success, so an unreadable subtree would leave this green having examined
# nothing — `find`'s exit status is invisible from inside the loop.
files=$(mktemp)
errs=$(mktemp)
trap 'rm -f "$files" "$errs"' EXIT
if ! find "$dir" -name '*.jsonl' -type f -print0 >"$files"; then
    echo "::error::could not enumerate $dir" >&2
    exit 1
fi

status=0
records=0
while IFS= read -r -d '' file; do
    # jq's stdout, stderr and exit status are taken separately. Branching on
    # failure alone discarded every offender jq had already accumulated, so a
    # file holding one barred record and one truncated one reported only the
    # truncation and never named the record.
    found=$(offenders_in "$file" 2>"$errs") && jq_ok=1 || jq_ok=0

    # `#n` arrives once per record and counts up, so the last one is how many
    # this file yielded. Streamed rather than summed at the end, because a
    # parse error part-way through aborts jq — and everything it had already
    # found would go with it.
    in_file=0
    while IFS= read -r line; do
        [[ -n "$line" ]] || continue
        if [[ "$line" == '#'* ]]; then
            in_file=${line#\#}
            continue
        fi
        status=1
        echo "::error file=$file,line=${line%%:*}::record ${line#*:} — see Trigger::bars_publication in benchmarks/src/report.rs" >&2
    done <<<"$found"
    records=$((records + in_file))

    if [[ "$jq_ok" -eq 0 ]]; then
        status=1
        # jq's own message names the byte offset, which is the only thing that
        # locates a truncated record. Passed through rather than swallowed.
        echo "::error file=$file::could not be read as one JSON object per line: $(tr '\n' ' ' <"$errs")" >&2
    fi
done <"$files"

if [[ "$status" -ne 0 ]]; then
    echo "::error::committed benchmark records must be publishable; the barred ones are ${BARRED[*]}" >&2
    exit 1
fi

# A check that examined nothing is not a check that passed. Counted in records
# rather than files, because a directory of empty files is the same silent
# no-op as a directory of none.
if [[ "$records" -eq 0 ]]; then
    echo "::error::no records under $dir — this check examined nothing, which is not the same as passing" >&2
    exit 1
fi

echo "check-results-publishable.sh: $records record(s) under $dir, every one publishable"
