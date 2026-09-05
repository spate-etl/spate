/** The homepage questions. The same text feeds the FAQPage structured data. */
export const FAQ: Array<{q: string; a: string}> = [
  {
    q: 'Is delivery exactly-once?',
    a: 'No. Delivery is at-least-once: an offset commits only after every record derived from it is durably written or intentionally dropped. Crash replay re-batches with new boundaries and can land rows twice, so design target tables to tolerate duplicates.',
  },
  {
    q: 'Which Rust version does it need?',
    a: 'The minimum supported Rust version is 1.94 with edition 2024. The newest 0.x minor is the supported line, and breaking changes ship in a minor bump.',
  },
  {
    q: 'Which connectors ship?',
    a: 'Kafka as a source and a sink, a ClickHouse sink, an S3 backfill source, Avro and JSON formats, a coordination store and a synthetic source for demos and tests. Each sits behind one feature on the facade crate, and nothing is enabled by default.',
  },
  {
    q: 'How is it tested?',
    a: 'Property tests cover the checkpoint tracker, the codecs and the assignment protocol; loom models the tracker directly. Kafka runs against a mock cluster on every pull request, and brokers, ClickHouse and object stores run against real containers whenever a change reaches them.',
  },
  {
    q: 'Can I write my own source or sink?',
    a: 'Yes. A source or sink is a small trait, and writing one is a supported path with a worked example in the repository. Connector types never enter the core crate’s public API.',
  },
];
