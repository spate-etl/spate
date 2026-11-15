#!/usr/bin/env bash
# Installs $TOOLCHAIN with the rustup components in $COMPONENTS, split on commas
# and spaces, and makes it the default. Called by the setup-rust action. Runs
# under the macOS runner's bash 3.2.
set -euo pipefail
set -f

: "${TOOLCHAIN:?}"

flags=()
IFS=$', \t\n'
for component in ${COMPONENTS:-}; do
  flags+=(--component "$component")
done
unset IFS

out=$(mktemp)
trap 'rm -f "$out"' EXIT

# The retry below matches rustup's wording, so the log names the version.
rustup --version

# rustup reports a checksum failure while the release server is mid-update,
# and the same install succeeds a few minutes later. Only that failure is
# retried, judged on each attempt's own output.
attempt=1
while :; do
  status=0
  rustup toolchain install "$TOOLCHAIN" ${flags[@]+"${flags[@]}"} \
    --profile minimal --no-self-update 2>&1 | tee "$out" || status=$?
  if [ "$status" -eq 0 ]; then
    break
  fi
  if [ "$attempt" -ge 5 ] ||
    ! grep -qF 'checksum failed for ' "$out" ||
    ! grep -qF 'this is likely due to an ongoing update of the official release server' "$out"; then
    exit "$status"
  fi
  echo "Attempt $attempt of 5 hit a release-server update; retrying in 30 s."
  sleep 30
  attempt=$((attempt + 1))
done

rustup default "$TOOLCHAIN"
