#!/usr/bin/env bash
#
# Builds each publishable crate's rustdoc the way docs.rs builds it: one crate
# at a time, on nightly, with that crate's own `[package.metadata.docs.rs]`
# table applied.
#
# `make doc` builds the workspace together on stable, which unifies features
# across members and never sets `docsrs`. Neither shape reaches what docs.rs
# does, so nothing else in the repository compiles the `docsrs` cfg or the
# nightly rustdoc features gated on it.
#
# Six of the table's keys are applied: `all-features`, `no-default-features`,
# `features`, `cargo-args`, `rustdoc-args` and `rustc-args`. `default-target`
# and `targets` are refused rather than ignored, because this builds for the
# host alone and honouring them means installing a target; a crate that sets
# one fails here and names itself.
#
# Usage:
#   ./scripts/docsrs.sh
#
# Runs on `bash` 3.2 and later: no associative arrays, no `mapfile`, no
# `${var,,}`.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! cargo +nightly --version >/dev/null 2>&1; then
  echo "docsrs.sh: needs the nightly toolchain (rustup toolchain install nightly)" >&2
  exit 1
fi

# One row per publishable crate: name, cargo flags, rustdoc args, rustc args,
# and the names of any keys this script refuses, separated by US (\x1f). Tab
# does not work here: `read` folds runs of IFS whitespace into one delimiter,
# so a crate with no cargo flags loses its empty field and every later column
# shifts left.
#
# `publish: []` marks a crate that is never uploaded, so docs.rs never sees it.
plan=$(cargo metadata --no-deps --format-version 1 --locked | jq -r '
  .packages[]
  | select(.publish != [])
  | . as $p
  # Cargo nests `[package.metadata.docs.rs]` as metadata.docs.rs, two levels.
  | (($p.metadata.docs.rs) // {}) as $m
  | [
      $p.name,
      ([
        (if $m["all-features"] then "--all-features" else empty end),
        (if $m["no-default-features"] then "--no-default-features" else empty end),
        (if (($m.features // []) | length) > 0
         then "--features=" + (($m.features) | join(","))
         else empty end),
        (($m["cargo-args"] // []) | .[])
      ] | join(" ")),
      (($m["rustdoc-args"] // []) | join(" ")),
      (($m["rustc-args"] // []) | join(" ")),
      ([ "default-target", "targets" ] | map(select($m[.] != null)) | join(", "))
    ]
  | join("\u001f")')

failed=0
built=0
while IFS=$'\x1f' read -r name flags docargs rustcargs unsupported; do
  [ -n "$name" ] || continue
  if [ -n "$unsupported" ]; then
    echo "::error::$name sets $unsupported, which this gate does not model: it builds for the host alone" >&2
    failed=$((failed + 1))
    continue
  fi
  built=$((built + 1))
  echo "docsrs.sh: $name ${flags:-(default features)}"
  # `broken_intra_doc_links` denied on top of the crate's own args: a dangling
  # link renders as dead text on the published page, and the docs.rs build
  # reports it nowhere. Denied by name rather than through `-D warnings`, which
  # hands an unpinned nightly the power to block every merge in the repository
  # the day rustdoc gains a lint. `make doc` holds the whole warning surface, on
  # stable, where a new lint arrives with a release worth reacting to.
  # Unquoted on purpose; each field is a space-separated argument list.
  # shellcheck disable=SC2086
  if ! RUSTFLAGS="${RUSTFLAGS:-} $rustcargs" \
    RUSTDOCFLAGS="$docargs -D rustdoc::broken_intra_doc_links" \
    cargo +nightly doc -p "$name" --no-deps --locked $flags; then
    echo "::error::rustdoc failed for $name as docs.rs would build it"
    failed=$((failed + 1))
  fi
done <<EOF
$plan
EOF

if [ "$built" -eq 0 ] && [ "$failed" -eq 0 ]; then
  echo "docsrs.sh: no publishable crate found; this run checked nothing" >&2
  exit 1
fi

if [ "$failed" -ne 0 ]; then
  echo "docsrs.sh: $failed crate(s) failed" >&2
  exit 1
fi

echo "docsrs.sh: $built crate(s) documented as docs.rs builds them."
