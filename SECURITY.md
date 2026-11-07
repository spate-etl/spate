# Security policy

## Reporting a vulnerability

**Report privately, through
[GitHub's private vulnerability reporting](https://github.com/spate-etl/spate/security/advisories/new).**
That opens a draft advisory visible only to the maintainers.

Please do not open a public issue, a pull request, or a discussion for something
you believe is exploitable. A public report starts a clock on every deployment
running the affected version, and there is no way to take it back.

That holds for a defect in a released version. A defect that exists only on
`main` and has not shipped carries no such clock, so the project handles it as a
public `[bug]` issue.

### What to include

A report is easier to act on with:

- the version or commit, and which features were enabled;
- what an attacker gains, and what they need in order to reach it;
- the smallest reproduction you have. A failing test against `spate-test`'s mocks
  is ideal, but a description of the sequence is enough.

You do not need a working exploit, and you do not need to be sure. A report that
turns out to be a non-issue costs one reply; an unreported one can cost data.

### What to expect

Response targets for a single-maintainer project:

| | |
|---|---|
| Acknowledgement | within 3 working days |
| Initial assessment | within 10 working days |
| Fix or documented mitigation | depends on severity, discussed with you in the advisory |

You will be credited in the advisory unless you ask not to be. There is no bug
bounty.

## What counts as a vulnerability here

This framework is a library that runs inside your own pipelines. A report is
judged by one question: does the party behind it gain a power it did not already
have? Every report meets the same bar, however it was found.

**Trusted**

- The pipeline's code and configuration.
- The backends the configuration names: brokers, object stores, schema
  registries and ClickHouse servers. A backend already holds the data and the
  progress. It can drop or change a record, answer any offset or stop answering,
  so a wrong reply gives it nothing new.
- The members of the pipeline's consumer group.

**Not trusted**

- Anyone who can write to a topic, bucket, prefix or queue the pipeline reads.
  Record bytes, object bytes, keys, headers and sizes are theirs, and so is
  schema content.
- Anyone who can publish to a feed the pipeline reads work from.
- Anyone on the network path while TLS is on, and where a default turns
  verification off. Someone rewriting a plaintext connection can forge what a
  backend can, so a reply bug only they reach is a bug.
- Anyone who can reach the admin port.

**In scope**

- A way for an untrusted party to make the pipeline lose or duplicate data in a
  manner the at-least-once contract forbids, most importantly committing a
  source watermark past unacknowledged data.
- Memory unsafety, from an untrusted party or a backend's reply.
- A panic reachable from record data rather than from configuration.
- A parser, queue or cache that an untrusted party can drive to unbounded memory
  or CPU, and connection exhaustion on the admin port.
- A request outside the configured scope, made with the pipeline's credentials
  on an untrusted party's input.
- Credentials or record contents leaking into logs, metric labels, or error
  messages.
- Anything that lets configuration under an attacker's control execute code.

**Bugs rather than vulnerabilities**

A panic, hang or unbounded resource use that only a configured backend's reply
can cause, and data loss from a routine backend event, are still worth fixing.
Please open a public issue.

**Not in scope**

- Vulnerabilities in the systems this framework connects to. Report those to
  Kafka, ClickHouse, or your object-store vendor.
- Resource exhaustion from configuration you chose, such as setting a queue
  capacity larger than available memory.
- Advisories against a dependency where the vulnerable code path is not
  reachable from this framework. Those are still worth reporting, and are
  triaged as maintenance rather than as an incident. Every advisory this project
  has accepted is recorded where the tool reporting it is configured, in
  `deny.toml` for cargo-deny and in an `osv-scanner.toml` beside each lockfile,
  with its reason and the condition for removing it.
- Anything requiring an attacker who already has the ability to run code in your
  pipeline process.

## Supported versions

**The newest `0.x` minor.**

| Version | Supported |
|---|---|
| Newest `0.x` minor | Yes |
| Anything older | No |

A fix lands on `main` and ships as the next patch of that minor. There are no
maintenance branches and no backports by default.

If a specific older version blocks you, say so on the advisory and a branch can
be cut from its tag. That is a conversation, not a standing commitment.

All crate versions in lockstep, so "the newest minor" means the same thing
for every one of them.

## How the project defends itself

Each control below is recorded where it is enforced, in `deny.toml`, in the
workflow that runs it, or in the script it calls:

- `Cargo.lock` and `package-lock.json` are committed, and CI builds `--locked` /
  `npm ci`, so a build cannot resolve a different graph.
  [Commands and gates](docs/developer/01-commands.md#commands) lists the
  commands that cannot take `--locked`.
- Every GitHub Action is pinned to a full commit SHA, and `zizmor` fails CI if
  a pin, a token scope, or a checkout credential setting regresses.
- `cargo deny check advisories` runs on every pull request **and** nightly. It
  fails a pull request that changes `Cargo.lock`, a manifest, `deny.toml` or
  the workflow that runs it, and lists its findings in the job summary of any
  other. The nightly scan files an issue for an advisory against `main`, so an
  advisory published against a crate the workspace compiles is raised within a
  day.
- `osv-scanner` runs nightly over every tracked manifest, including lockfile
  entries no build compiles and the documentation site's npm dependencies. It
  files an issue on a finding and does not gate pull requests.
- Dependency updates arrive as reviewed pull requests after a soak period on the
  registry: 7 days, or 14 where a breaking change can hide. Security updates
  bypass the soak entirely.
- npm lifecycle scripts are disabled at install time, in both places npm runs.
