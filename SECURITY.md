# Security policy

## Reporting a vulnerability

**Report privately, through
[GitHub's private vulnerability reporting](https://github.com/spate-etl/spate/security/advisories/new).**
That opens a draft advisory visible only to the maintainers.

Please do not open a public issue, a pull request, or a discussion for something
you believe is exploitable. A public report starts a clock on every deployment
running the affected version, and there is no way to take it back.

### What to include

A report is easier to act on with:

- the version or commit, and which features were enabled (`kafka`, `s3`,
  `coordination-nats`, …) — the feature set decides which code is even linked;
- what an attacker gains, and what they need in order to reach it;
- the smallest reproduction you have. A failing test against `spate-test`'s mocks
  is ideal, but a description of the sequence is enough.

You do not need a working exploit, and you do not need to be sure. A report that
turns out to be a non-issue costs one reply; an unreported one can cost data.

### What to expect

This is a single-maintainer project, so these are honest expectations rather
than a corporate SLA:

| | |
|---|---|
| Acknowledgement | within 3 working days |
| Initial assessment | within 10 working days |
| Fix or documented mitigation | depends on severity, discussed with you in the advisory |

You will be credited in the advisory unless you ask not to be. There is no bug
bounty.

## What counts as a vulnerability here

This framework is a library that runs inside your own pipelines. That shapes
what is in scope.

**In scope**

- A way to make the pipeline **lose or duplicate data** in a manner the
  at-least-once contract forbids — most importantly, committing a source
  watermark past unacknowledged data.
- Memory unsafety, or a panic reachable from record data rather than from
  configuration.
- Credentials or record contents leaking into logs, metric labels, or error
  messages.
- A parser that can be driven to unbounded memory or CPU by input from the
  source — record payloads and object-store responses are the interesting ones.
- Anything that lets configuration under an attacker's control execute code.

**Not in scope**

- Vulnerabilities in the systems this framework connects to. Report those to
  Kafka, ClickHouse, NATS, or your object-store vendor.
- Resource exhaustion from configuration you chose, such as setting a queue
  capacity larger than available memory.
- Advisories against a dependency where the vulnerable code path is not
  reachable from this framework. Those are still worth reporting — they are just
  triaged as maintenance rather than as an incident. `deny.toml` records every
  advisory this project has accepted, each with its reason and the condition for
  removing it.
- Anything requiring an attacker who already has the ability to run code in your
  pipeline process.

## Supported versions

**The newest `0.x` minor.** One row, and that is the whole policy.

| Version | Supported |
|---|---|
| Newest `0.x` minor | Yes |
| Anything older | No |

A fix lands on `main` and ships as the next patch of that minor. There are no
maintenance branches and no backports by default, because a support matrix
nobody can staff is worse than a short one honestly stated — it reads as a
promise while behaving like nothing at all.

If a specific older version genuinely blocks you, say so on the advisory and a
branch can be cut from its tag. That is a conversation, not a standing
commitment.

All ten crates version in lockstep, so "the newest minor" means the same thing
for every one of them.

## How the project defends itself

Each control below is recorded where it is enforced — in `deny.toml`, in the
workflow that runs it, or in the script it calls. In short:

- `Cargo.lock` and `package-lock.json` are committed, and CI builds `--locked` /
  `npm ci`, so a build cannot silently resolve a different graph. Two
  invocations are deliberately outside that and are documented where they
  appear.
- Every GitHub Action is pinned to a full commit SHA, and `zizmor` fails CI if
  a pin, a token scope, or a checkout credential setting regresses.
- `cargo deny check advisories` runs on every pull request **and** nightly, so
  an advisory published against a crate already in the lockfile is caught within
  a day rather than whenever somebody next opens a pull request. This covers the
  Rust tree; the documentation site's npm dependencies are not gated in CI.
- Dependency updates arrive as reviewed pull requests after a soak period on the
  registry — 7 days, or 14 where a breaking change can hide. Security updates
  bypass the soak entirely.
- npm lifecycle scripts are disabled at install time, in both places npm runs.
