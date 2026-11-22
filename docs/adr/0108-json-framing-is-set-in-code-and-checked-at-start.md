---
description: "JSON framing is set on JsonDeserializerBuilder in code, and a pipeline whose source and deserializer both split payloads refuses to start."
---

# ADR-0108 — JSON framing is set in code, and a double split is refused at start

- **Status:** accepted
- **Date:** 2026-11-22
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A JSON pipeline split its input in two places. A source that frames its own
byte stream, such as `S3Source` with a `JsonLinesFramer`, was given its framer
in Rust. The JSON deserializer took a `framing` key in YAML. Only the chain
closure tied the two together, by calling
`JsonDeserializerBuilder::for_source_framing`. A closure that skipped the call
ran a line-framed source with `framing: array` to completion and wrote no rows.

The question is where the deserializer's framing is set, and what stops a
source and a deserializer that both split each payload.

## Considered options

Where the framing is set:

- In code, on `JsonDeserializerBuilder`
- In YAML, as the `framing` key

What stops a double split:

- The runtime asks each chain's deserializer at start, through a provided
  `Deserializer::check_source_framing`
- The chain closure calls a builder method that checks it, as before

## Decision outcome

Chosen option: "In code, on `JsonDeserializerBuilder`", checked by "the runtime
at start", because the framing follows from code the pipeline already has to
write, and a check the runtime always runs cannot be skipped.

A payload's framing is set by its producer and rarely changes. When it does,
the pipeline's code usually changes with it, so a rebuild is acceptable.
The source's framer is chosen in code, and the deserializer's framing now sits
beside it, where one reader sees both. The YAML section keeps the knobs that do
change per deployment: `on_error` and `reject_duplicate_keys`.

`PipelineRuntime::run` builds every chain and asks each one about
`Source::framing_contract` before it starts any task or driver of its own. The
deserializer answers, so its refusal names what to change. The JSON
deserializer refuses `JsonFraming::Lines` and `JsonFraming::Array` under a
`PerRecord` source, and the run returns `StartError::Config` before the source
opens.

Keeping the key in YAML with the same start check was the other complete option.
It would let a deployment change framing without a rebuild, and it would keep a
key that has to agree with a framer chosen in code. The chain-side check was
rejected because a closure that omits it runs without any error.

### Consequences

- Good, because no pipeline can start with a source and a deserializer that
  both split each payload, whichever way it was assembled.
- Good, because the framing and the framer that must agree with it are read in
  one place.
- Bad, because changing a JSON pipeline's framing needs a rebuild, where a YAML
  edit used to be enough.
- Bad, because a `framing` key in an existing pipeline file is now an unknown
  key, and building the deserializer from that section fails until the key is
  removed.
- Neutral, because the Avro deserializer's `mode` and the datagen source's
  `encoding` stay in YAML. Each picks how one payload is encoded, and neither
  splits a payload into records.

### Confirmation

`a_chain_that_refuses_the_source_framing_stops_the_start` in
`crates/spate-core/src/pipeline/tests.rs` fails when `run` does not stop on a
refusal. `a_per_record_source_is_refused_unless_framing_is_single` in
`crates/spate-json/src/deser.rs` checks each framing against each contract on
both JSON deserializers.
`array_framing_under_a_line_framed_s3_source_refuses_to_start` in
`crates/spate/tests/json_framing_refusal.rs` runs the case above end to end.
`a_framing_key_is_rejected` in `crates/spate-json/src/config.rs` pins the YAML
refusal.

## More information

- Landed in the pull request that added `JsonDeserializerBuilder::framing`. The
  start check landed in the pull request before it.
- [Framing vs deserialization](../user-guide/04-connectors/formats/README.mdx#framing-vs-deserialization)
  and the
  [JSON format](../user-guide/04-connectors/formats/json/README.mdx#the-three-framings)
  page describe the behaviour.
