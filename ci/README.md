# Pinned service images

The container suites boot real servers, and some run their clients in a pinned
image too. This tree records the image each one runs, one directory per service
and one per release line under it:

```
ci/<service>/PRIMARY              the lane to use when nothing selects one
ci/<service>/<lane>/Dockerfile    the pin for that lane
ci/<service>/DOCS                 pages whose support claims the lanes back (optional)
ci/<service>/UNSUPPORTED          lanes that back no support claim (optional)
```

Each `Dockerfile` holds a single `FROM` carrying an exact release tag and the
digest that tag resolves to. Nothing builds them. They exist so one file names
the image, readable by Dependabot, by the task runner, and by the test
harness.

Services and lanes are discovered from this tree. The task runner, the CI
classifier and the test harness name no lane and no service beyond the one they
are testing, so adding either is a change inside `ci/`.

## Services

| Service | Lanes | Suite |
| --- | --- | --- |
| [`clickhouse`](clickhouse/README.md) | `lts-previous`, `lts`, `stable` | `spate-clickhouse`, and `spate`'s examples tier |
| [`debian`](debian/README.md) | `trixie` | `spate-kafka`'s `tls_system_ca`, as its vendored OpenSSL client image |
| [`dynamodb`](dynamodb/README.md) | `stable` | `spate-coordination`'s DynamoDB store |
| [`kafka`](kafka/README.md) | `stable` | `spate-kafka`, and `spate`'s end-to-end suites |
| [`nats`](nats/README.md) | `floor`, `below-floor`, `async` | `spate-coordination`, `spate-s3`'s `coordinated_nats`, and `spate`'s examples tier |
| [`rust`](rust/README.md) | `stable` | `spate-kafka`'s `tls_system_ca`, as its system OpenSSL builder and client image |
| [`toxiproxy`](toxiproxy/README.md) | `stable` | `spate-faults`, in the weekly fault run |

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

**Resolving and pulling** goes through `cargo xtask container-image`:

```sh
cargo xtask container-image <service> <lane>           # print name:tag
cargo xtask container-image --ref <service> <lane>     # print name:tag@digest
cargo xtask container-image --pull <service> <lane>    # pull, re-tag, print name:tag
cargo xtask container-image --pull-all                 # every service's selected lane
cargo xtask container-image --extra-lanes <service>    # lanes needing their own CI job
```

`cargo xtask integration-test` runs `--pull-all`, which walks this tree. Select a lane
through the environment:

```sh
SPATE_CLICKHOUSE_LANE=stable cargo xtask integration-test
```

`--pull` fetches by digest and re-tags locally, which is what makes a run use the
pinned bytes. testcontainers builds its image reference as `name:tag` and has no
digest form, and it creates the container before it pulls, so the local tag is
what it finds. `cargo xtask integration-test` runs it first, and so does CI for a
suite that does not pull its own image.

A bare `cargo nextest run --profile docker` skips that step, and testcontainers
then pulls the tag unverified. An exact release tag is not re-pushed, so the
bytes are the same; the digest is simply not checked. A suite that reads its
image with `--pull`, through `spate_test_support::container_image`, pulls by
digest on every run.

**Exact tags**, the vendor's full release version, so a bump diff names the
release it moved to: `YY.M.P.B` for ClickHouse, `MAJOR.POINT` for Debian,
`MAJOR.MINOR.PATCH` for DynamoDB Local, Kafka and Toxiproxy,
`MAJOR.MINOR.PATCH-alpine` for NATS, `MAJOR.MINOR.PATCH-bookworm` for Rust.

## Support claims

`cargo xtask tidy supported-versions` holds a page listed in `DOCS` to the lanes.
Every release line its `## Supported` table names, and every image tag its code
blocks run, must be a line some lane pins. A lane listed in `UNSUPPORTED` pins a
fixture and backs no claim: NATS's `below-floor` is a server the software
refuses, and `async` runs one test.
Nothing else reads either file.

## What CI runs

The `containers` job runs the whole container tier on each service's primary
lane. For a service whose extra lanes `cargo xtask ci-changes` reads, any other
lane resolving to a different image gets a job of its own, one runner each. A
lane resolving to the primary lane's image is dropped, and
`cargo xtask ci-changes` logs which and why, so a service whose `stable` and
`lts` point at one release costs no extra job until they diverge.

## Bumps

Dependabot moves the pins, with one `.github/dependabot.yml` entry per policy:
lanes held on a line share an entry and an `ignore` rule, and a lane that follows
every release gets its own. The `rust` lane shares its entry with
`examples/docker`, which pins the same image. Moving a *line* when a vendor's
support window shifts is a maintainer edit.

## When a lane goes red

The bump usually cannot be fixed inside its own pull request, so:

1. Leave the pull request open. Nothing here auto-merges, so the lane stays on
   its last green pin and no other pull request is blocked.
2. File the defect as its own issue if none is open.
3. Add an `ignore` entry to `.github/dependabot.yml` naming the exact versions,
   with a `Delete this when …` sentence and the issue number. The Rust
   workspace's `cargo` entry there is the worked example.
4. Close the bump pull request. The next run proposes the version after the
   ignored one.

## Adding a service

1. `ci/<service>/<lane>/Dockerfile` per release line you want covered, each
   pinned by tag and digest, and a `ci/<service>/PRIMARY` naming the default.
2. A `ci/<service>/README.md` naming those lines and the reasoning.
3. A row in the table above.
4. The suite's harness reads its manifest through the same helper.
5. `cargo xtask ci-changes` maps `ci/<service>/*` to that suite. A service with
   more than one lane the suite runs on also has its extra lanes read for the
   matrix, and a `ci.yml` job consuming them. A lane only one test boots, such as
   NATS's `below-floor`, is not read.
6. The Dependabot entries.

Steps 1 to 3 are this tree, and `cargo xtask integration-test` picks the service up from
them with no edit. Steps 5 and 6 name the service once each, because the
service-to-suite mapping and the bump policy are the two things that cannot be
derived. `DEVELOPING.md` needs no edit.
