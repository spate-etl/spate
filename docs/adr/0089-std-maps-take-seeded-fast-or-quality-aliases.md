---
description: "Std HashMap and HashSet are banned; maps use spate-core's seeded fast aliases, or the quality aliases for keys an outside party chooses."
---

# ADR-0089 — Hash maps use seeded fast or quality aliases, and std's maps are banned

- **Status:** accepted
- **Date:** 2026-10-30
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Every crate kept its maps and sets in std's `HashMap` and `HashSet` under std's
default hasher, SipHash-1-3. Several sit on a per-record or per-batch path: the
checkpointer's trackers in `spate-core`, the JSON duplicate-key guard, the Avro
schema cache and named-type index, and the ClickHouse LowCardinality dictionary.
Each site picked its hasher on its own, and one, the LowCardinality dictionary,
had already moved to foldhash through an alias of its own. #1497 asked whether a
faster hasher pays on the benched paths and, if so, how to keep every site on
it.

The keys fall into two groups. Most are values Spate produces: partition ids,
lane ids, receipt ids, offsets. Some are chosen by whoever writes the input:
JSON object keys, row values interned into a dictionary, object keys in a
bucket, Avro schema ids and type names.

## Considered options

- Leave the choice to each site, with no ban
- Ban std's maps and offer one seeded fast alias
- Ban std's maps and offer a seeded fast alias for keys Spate produces and a
  seeded quality alias for keys an outside party chooses

## Decision outcome

Chosen option: "Ban std's maps and offer a seeded fast alias and a seeded
quality alias", because the measured paths gain from foldhash and a ban is the
only thing that keeps a new map off SipHash, while keys chosen outside Spate get
the variant that spreads every input bit into the bits a table indexes on.

`spate_core::hash` provides `FastMap`/`FastSet` and `QualityMap`/`QualitySet`:
std's map and set over `FastState` and `QualityState`, newtypes over foldhash's
`fast` and `quality` random states that seed each instance. The newtypes keep
foldhash out of `spate-core`'s public signatures, so INV-6 holds and a foldhash
bump is not a breaking change. `clippy.toml` bans `std::collections::HashMap`
and `std::collections::HashSet`. A site that keeps a std map takes an `expect`
naming why: a type a user decodes into, a field of a library's own type, or the
alias definitions themselves. `xtask` cannot depend on `spate-core`, so it uses
`BTreeMap` and `BTreeSet`.

Quality shares fast's seeds and adds one mixing step when the hash is finished.
Foldhash calls itself minimally DoS-resistant and claims no HashDoS resistance
against an attacker who observes timing or iteration order. Neither alias is a
defence against a crafted flood; the quality alias is chosen for its bit spread.

One seeded fast alias for every key was rejected because a key an outside party
chooses can differ from its neighbours in a few bits, and the fast variant's
spread of those bits into the table index is weaker. Leaving each site to choose
was rejected because the one site that had chosen did so in an alias of its own,
and nothing stopped the next map from defaulting to SipHash.

### Consequences

- Good, because every hash map Spate owns hashes with foldhash, and a new map
  cannot default to SipHash without an `expect` that says why.
- Good, because the measured paths cost fewer instructions; see Evidence.
- Bad, because the quality alias costs a little over fast where a path hashes
  many keys. On the ClickHouse encode bench it costs about 1% on x86_64 and 2%
  on aarch64 for the schema built around LowCardinality columns.
- Bad, because every map's iteration order differs per instance and per run, as
  it did under std's hasher. Code that needs an order must sort or use a `BTree`
  collection.
- Bad, because clippy does not lint doctests, so a doctest can still use std's
  maps.
- Neutral, because the third-party maps Spate reads (rdkafka's `Statistics`,
  apache-avro's `Value::Map`, the AWS SDK's attribute map) stay std's,
  re-collected into an alias where Spate keeps them.

### Confirmation

The two `disallowed-types` entries in `clippy.toml`, run by `cargo xtask
clippy`. `disallowed_std_maps_fail_the_gate` in `xtask/tests/clippy_gate.rs`
pins both entries and the spellings the lint reports, and
`std_map_aliases_pass_the_gate` pins that an alias defined under an `expect`
passes. The per-instance seeding is pinned by `fast_state_seeds_each_instance`
and `quality_state_seeds_each_instance` in `crates/spate-core/src/hash.rs`.

## Evidence

Callgrind instruction counts from the counted tier in CI, pull request against
`main`:

- Fast hasher on the checkpointer and the JSON guard, measurement draft #1564
  (run 41064214434): `commit_batches` 20–30% fewer; the JSON guard's
  `dup_guard_hit` 37–40% fewer, `dup_guard_wide` 12–14%, `dup_guard_deep` 9–11%.
- Quality hasher on the JSON guard, #1566 (run 41083059689): `dup_guard_hit`
  36–40% fewer, `dup_guard_wide` 11–14%, `dup_guard_deep` 8–11%, both backends
  and both architectures. Against the fast run, quality costs under one point
  on most of these rows and up to two on `dup_guard_hit` on aarch64.
- Quality hasher on the ClickHouse LowCardinality dictionary, #1567 (run
  41087588527): `encode_native_events` +1.02% on x86_64 and +1.89% on aarch64. A
  probe with the fast alias on the same dictionary (run 41089127464) measured
  −0.05% and +0.15%, so the cost is the quality step and the newtype layer costs
  nothing measurable.
- Quality hasher on the Avro cache and named-type index, #1569 (run
  41095758025): `decode_recursive` 20–24% fewer, `decode_confluent
  poisoned_schema_id` about 20%, the resolving readers up to 2%.

Maps on no benched path moved for the rule, with no claimed gain.

## More information

- Landed in the pull request that closed #1497, after the per-crate pull
  requests #1565 (`spate-core` aliases), #1566, #1567, #1569, #1571, #1572,
  #1573, #1574, #1575 and #1576.
- [INV-6](../INVARIANTS.md) — why the states are newtypes.
- [ADR-0088](0088-a-connector-builds-its-vendor-client-in-one-place.md) — the
  other `clippy.toml` ban recorded as a decision.
