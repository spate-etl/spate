# Pinned service images

The container suites boot real servers. This tree records the version each one
runs against, one directory per service and one per release line under it:

```
ci/<service>/PRIMARY              the lane to use when nothing selects one
ci/<service>/<lane>/Dockerfile    the pin for that lane
```

Each `Dockerfile` holds a single `FROM` carrying an exact patch tag and the
digest that tag resolves to. Nothing builds them. They exist so one file names
the server, readable by Dependabot, by `scripts/container-image.sh`, and by the
test harness.

Services and lanes are discovered from this tree. The Makefile, the CI
classifier and the test harness name no lane and no service beyond the one they
are testing, so adding either is a change inside `ci/`.

## Services

| Service | Lanes | Suite |
| --- | --- | --- |
| [`clickhouse`](clickhouse/README.md) | `lts-previous`, `lts`, `stable` | `spate-clickhouse`, and `spate`'s examples tier |

A service's own README carries what is specific to it: which release lines it
has, its vendor's support window, and why those lanes.

## The convention

**Lane names describe a release line.** `lts` and `stable` mean whatever they
mean for that vendor today, so a lane keeps its name across a bump. A vendor with
no LTS line has no `lts` lane. The names are per-service and there is no shared
vocabulary to conform to.

**One environment variable per service**, `SPATE_<SERVICE>_LANE`, falling back
to the lane in `ci/<service>/PRIMARY`. Lanes are per-service, so a shared
variable would impose one vendor's release vocabulary on every other. The name
is derived from the directory, so declaring it is the directory.

**Resolving and pulling** goes through `scripts/container-image.sh`:

```sh
./scripts/container-image.sh <service> <lane>            # print name:tag
./scripts/container-image.sh --ref <service> <lane>      # print name:tag@digest
./scripts/container-image.sh --pull <service> <lane>     # pull, re-tag, print name:tag
./scripts/container-image.sh --pull-all                  # every service's selected lane
./scripts/container-image.sh --extra-lanes <service>     # lanes needing their own CI job
```

`make test-docker` runs `--pull-all`, which walks this tree. Select a lane
through the environment:

```sh
SPATE_CLICKHOUSE_LANE=stable make test-docker
```

`--pull` fetches by digest and re-tags locally, which is what makes a run use the
pinned bytes. testcontainers builds its image reference as `name:tag` and has no
digest form, and it creates the container before it pulls, so the local tag is
what it finds. `make test-docker` and CI both run it first.

A bare `cargo nextest run --profile docker` skips that step, and testcontainers
then pulls the tag unverified. An exact patch tag is not re-pushed, so the bytes
are the same; the digest is simply not checked.

**Exact four-component tags**, so a bump diff names the patch version it moved
to.

## What CI runs

The `containers` job runs the whole container tier on each service's primary
lane. Any other lane resolving to a different image gets a job of its own, one
runner each. A lane resolving to the primary lane's image is dropped, and
`scripts/ci-changes.sh` logs which and why, so a service whose `stable` and `lts`
point at one release costs no extra job until they diverge.

## Bumps

Dependabot moves the pins, with one `.github/dependabot.yml` entry per policy:
lanes held on a line share an entry and an `ignore` rule, and a lane that follows
every release gets its own. Moving a *line* when a vendor's support window shifts
is a maintainer edit.

## When a lane goes red

The bump usually cannot be fixed inside its own pull request, so:

1. Leave the pull request open. Nothing here auto-merges, so the lane stays on
   its last green pin and no other pull request is blocked.
2. File the defect as its own issue if none is open.
3. Add an `ignore` entry to `.github/dependabot.yml` naming the exact versions,
   with a `Delete this when …` sentence and the issue number. The `rust` entry
   there is the worked example.
4. Close the bump pull request. The next run proposes the version after the
   ignored one.

## Adding a service

1. `ci/<service>/<lane>/Dockerfile` per release line you want covered, each
   pinned by tag and digest, and a `ci/<service>/PRIMARY` naming the default.
2. A `ci/<service>/README.md` naming those lines and the reasoning.
3. A row in the table above.
4. The suite's harness reads its manifest through the same helper.
5. `scripts/ci-changes.sh` maps `ci/<service>/*` to that suite and calls
   `--extra-lanes` for its matrix; `ci.yml` gains a job consuming it.
6. The Dependabot entries.

Steps 1 to 3 are this tree, and `make test-docker` picks the service up from
them with no edit. Steps 5 and 6 name the service once each, because the
service-to-suite mapping and the bump policy are the two things that cannot be
derived. `DEVELOPING.md` names no service at all.
