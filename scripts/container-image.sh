#!/usr/bin/env bash
#
# Resolves the pinned image for one service lane, and pulls it by digest.
#
# Each lane is a directory under `ci/<service>/` holding a `Dockerfile` whose
# single `FROM` carries an exact tag and the digest that tag resolves to. That
# file is the one source of truth: Dependabot edits it, the test harness reads
# it, and CI pulls through it.
#
# `--pull` fetches the image by digest and re-tags it locally. testcontainers
# builds its image reference as `name:tag` and has no digest form, so the local
# tag is what makes a test run the digest-pinned bytes. It creates the container
# first and pulls only on a miss, so it never overwrites the re-tagged image.
#
# Usage:
#   ./scripts/container-image.sh clickhouse              # the selected lane's name:tag
#   ./scripts/container-image.sh clickhouse lts          # print name:tag
#   ./scripts/container-image.sh --ref clickhouse lts    # print name:tag@digest
#   ./scripts/container-image.sh --pull clickhouse       # pull the selected lane
#   ./scripts/container-image.sh --pull-all              # every service's selected lane
#   ./scripts/container-image.sh --extra-lanes clickhouse  # lanes needing their own CI job
#   ./scripts/container-image.sh --self-test             # the parser, alone
#
# `--pull-all` takes each service's lane from `SPATE_<SERVICE>_LANE`, falling
# back to the lane named in `ci/<service>/PRIMARY`. It discovers services from
# the tree, so adding one needs no edit here or in the Makefile.
#
# Runs on `bash` 3.2 and later: no associative arrays, no `mapfile`.
set -euo pipefail

cd "$(dirname "$0")/.."

die() {
    echo "error: $1" >&2
    exit 1
}

# Where the lane directories live. `SPATE_CI_ROOT` is a test hook: `--self-test`
# and scripts/supported-versions.sh point the parsers at a scratch tree with it.
ci_root="${SPATE_CI_ROOT:-ci}"

# Every service with pinned images, one per line, discovered from the tree.
services() {
    local dir
    for dir in "$ci_root"/*/; do
        [[ -d "$dir" ]] || continue
        dir="${dir%/}"
        printf '%s\n' "${dir##*/}"
    done
}

# The lane a service runs when nothing selects one, from `ci/<service>/PRIMARY`.
primary_lane_for() { # service
    local file="$ci_root/$1/PRIMARY" lane
    [[ -f "$file" ]] || die "$file is missing; every service declares a primary lane"
    lane=$(sed -n '1{s/[[:space:]]*$//;p;}' "$file")
    [[ -n "$lane" ]] || die "$file is empty"
    printf '%s\n' "$lane"
}

# The lane selected for a service: `SPATE_<SERVICE>_LANE`, else its primary.
#
# `printenv` rather than indirect expansion, so a service name never reaches a
# shell evaluation.
selected_lane_for() { # service
    local upper value
    upper=$(printf '%s' "$1" | tr '[:lower:]-' '[:upper:]_')
    value=$(printenv "SPATE_${upper}_LANE" || true)
    if [[ -n "$value" ]]; then
        printf '%s\n' "$value"
    else
        primary_lane_for "$1"
    fi
}

# Prints `name:tag@sha256:...` for a lane, from its Dockerfile's `FROM`.
#
# A reference carrying no digest is rejected: an unpinned lane pulls whatever
# the tag points at today, which is the state this mechanism exists to end.
reference_for() { # service, lane
    local service="$1" lane="$2" manifest ref
    manifest="$ci_root/$service/$lane/Dockerfile"
    [[ -f "$manifest" ]] || die "no such lane: $manifest"

    ref=$(sed -n 's/^FROM  *\([^ ]*\).*/\1/p' "$manifest" | sed -n '1p')
    [[ -n "$ref" ]] || die "$manifest has no FROM line"
    case "$ref" in
    *@sha256:*) ;;
    *) die "$manifest is not pinned by digest: $ref" ;;
    esac
    printf '%s\n' "$ref"
}

# Prints the `name:tag` half, which is what testcontainers can express.
tagged_for() { # service, lane
    reference_for "$1" "$2" | sed 's/@sha256:.*//'
}

# Every lane of a service, one per line, discovered from the tree.
lanes_for() { # service
    local dir
    for dir in "$ci_root/$1"/*/; do
        [[ -d "$dir" ]] || continue
        dir="${dir%/}"
        printf '%s\n' "${dir##*/}"
    done
}

# The lanes needing a CI job beyond the one covering the primary lane: those
# resolving to a different image. A lane matching the primary image would boot a
# second identical server.
extra_lanes_for() { # service
    local primary primary_ref lane
    primary=$(primary_lane_for "$1")
    primary_ref=$(reference_for "$1" "$primary")
    for lane in $(lanes_for "$1"); do
        [[ "$lane" != "$primary" ]] || continue
        [[ "$(reference_for "$1" "$lane")" != "$primary_ref" ]] || continue
        printf '%s\n' "$lane"
    done
}

# Fetches a lane by digest and re-tags it locally, then prints the local
# `name:tag` so a caller can pull and use the result in one invocation.
pull_lane() { # service, lane
    local ref tagged digested
    ref=$(reference_for "$1" "$2")
    tagged=$(tagged_for "$1" "$2")
    # `%:*` strips the tag only. `%%:*` would cut at the first colon, which is
    # the registry port in `localhost:5000/db:1.2`.
    digested="${tagged%:*}@sha256:${ref##*@sha256:}"
    docker pull --quiet "$digested" >&2
    docker tag "$digested" "$tagged"
    printf '%s\n' "$tagged"
}

# Script-scoped, so the EXIT trap can still see it once the probe has returned.
scratch=""

# A scratch tree holding one fictional service, so the value assertions below
# rest on fixtures rather than on whatever the real lanes pin today. A bump
# moving a real pin must not fail a parser test.
make_fixture() {
    local digest
    digest=$(printf 'a%.0s' $(seq 1 64))
    scratch=$(mktemp -d)
    # EXIT, not RETURN: a RETURN trap set here is the shell's, so it fires on
    # the first inner function return and takes the scratch tree with it.
    trap 'rm -rf "$scratch"' EXIT
    mkdir -p "$scratch/db/vendor-main" "$scratch/db/old" "$scratch/db/tracking" \
        "$scratch/bad/unpinned"
    echo "vendor-main" >"$scratch/db/PRIMARY"
    echo "FROM vendor/db:9.4.1.2@sha256:$digest" >"$scratch/db/vendor-main/Dockerfile"
    echo "FROM vendor/db:9.1.7.3@sha256:$digest" >"$scratch/db/old/Dockerfile"
    # Byte-identical to the primary lane, which is the state `stable` sits in
    # whenever the newest release is also the newest LTS. `extra_lanes_for` has
    # to drop it; without this lane its reference-equality rule can be deleted
    # and every test still passes.
    echo "FROM vendor/db:9.4.1.2@sha256:$digest" >"$scratch/db/tracking/Dockerfile"
    # Its own service, so it never enters a walk over `db`'s lanes.
    echo "unpinned" >"$scratch/bad/PRIMARY"
    echo "FROM vendor/db:9.4" >"$scratch/bad/unpinned/Dockerfile"
}

# Resolves a lane with the service's variable set, for the self-test.
env_selected_lane_probe() {
    SPATE_DB_LANE=old selected_lane_for db
}

self_test() {
    local failed=0 got

    check() { # want, desc, command...
        local want="$1" desc="$2"
        shift 2
        got=$("$@" 2>&1) || true
        if [[ "$got" != "$want" ]]; then
            echo "::error::$desc: expected '$want', got '$got'"
            failed=1
        fi
    }

    # Every lane this repository ships parses and is pinned by a digest of the
    # right shape. Discovered, so a new service or lane is covered on arrival.
    local svc lane ref digest
    for svc in $(services); do
        for lane in $(lanes_for "$svc"); do
            ref=$(reference_for "$svc" "$lane")
            # Emptying the string of hex characters leaves nothing behind when
            # every character was one.
            digest="${ref##*@sha256:}"
            if [[ ${#digest} -ne 64 || -n "${digest//[0-9a-f]/}" ]]; then
                echo "::error::$svc/$lane: digest is not 64 hex characters: '$digest'"
                failed=1
            fi
        done
    done

    # Every service declares a primary lane, and that lane exists.
    local service primary
    for service in $(services); do
        primary=$(primary_lane_for "$service")
        if [[ ! -d "$ci_root/$service/$primary" ]]; then
            echo "::error::$service: PRIMARY names '$primary', which is not a lane"
            failed=1
        fi
        case " $(lanes_for "$service" | tr '\n' ' ') " in
        *" $primary "*) ;;
        *)
            echo "::error::$service: '$primary' is missing from its lane list"
            failed=1
            ;;
        esac
    done

    # The primary lane never needs a job of its own, and a lane pointing at the
    # primary lane's image needs none either.
    for service in $(services); do
        primary=$(primary_lane_for "$service")
        case " $(extra_lanes_for "$service" | tr '\n' ' ') " in
        *" $primary "*)
            echo "::error::$service: the primary lane '$primary' is in --extra-lanes"
            failed=1
            ;;
        esac
    done

    # Values, against the fixture. A lane name that is not `lts` also proves the
    # resolver reads PRIMARY instead of assuming a name.
    make_fixture
    ci_root="$scratch"

    check "vendor-main" "the primary lane comes from PRIMARY" primary_lane_for db
    check "vendor-main" "an unset variable falls back to the primary lane" \
        selected_lane_for db
    check "old" "the environment overrides the primary lane" env_selected_lane_probe
    check "vendor/db:9.4.1.2" "the tagged half drops the digest" tagged_for db vendor-main
    check "old" "only a lane whose image differs from the primary needs its own job" \
        extra_lanes_for db
    check "error: no such lane: $scratch/db/nope/Dockerfile" \
        "an unknown lane fails loudly" reference_for db nope
    check "error: $scratch/bad/unpinned/Dockerfile is not pinned by digest: vendor/db:9.4" \
        "a lane without a digest is rejected" reference_for bad unpinned

    ci_root="${SPATE_CI_ROOT:-ci}"

    if [[ "$failed" -eq 0 ]]; then
        echo "container-image.sh: self-test passed"
    fi
    return "$failed"
}

mode=tagged
case "${1:-}" in
--self-test)
    self_test
    exit
    ;;
# Every service's selected lane, so a caller wanting "the images this tree
# pins" names no service and needs no edit when one is added.
--pull-all)
    [[ $# -eq 1 ]] || die "usage: $0 --pull-all"
    for service in $(services); do
        lane=$(selected_lane_for "$service")
        echo "$service: $lane" >&2
        pull_lane "$service" "$lane" >/dev/null
    done
    exit
    ;;
# The lanes CI needs a job for, given that another job already covers the
# primary one.
--extra-lanes)
    [[ $# -eq 2 ]] || die "usage: $0 --extra-lanes <service>"
    extra_lanes_for "$2"
    exit
    ;;
--pull)
    mode=pull
    shift
    ;;
--ref)
    mode=ref
    shift
    ;;
-*) die "unknown flag: $1" ;;
esac

# The lane is optional. Omitting it takes the selected one, so a caller wanting
# "whatever this service runs by default" names no lane and keeps working when
# `PRIMARY` moves.
[[ $# -eq 1 || $# -eq 2 ]] || die "usage: $0 [--ref|--pull] <service> [lane]"
service="$1"
lane="${2:-$(selected_lane_for "$service")}"

case "$mode" in
tagged) tagged_for "$service" "$lane" ;;
ref) reference_for "$service" "$lane" ;;
pull) pull_lane "$service" "$lane" ;;
esac
