/** The connector inventory the homepage renders. One entry per crate, as the README's table. */
export type Connector = {
  name: string;
  role: 'source' | 'sink' | 'source · sink' | 'format' | 'store';
  crate: string;
  feature: string;
  summary: string;
  docs: string;
};

export const CONNECTORS: Connector[] = [
  {
    name: 'Kafka',
    role: 'source · sink',
    crate: 'spate-kafka',
    feature: 'kafka',
    summary: 'One consumer per process; partitions fan across pipeline threads as zero-copy lanes.',
    docs: '/docs/user-guide/connectors/sources/kafka',
  },
  {
    name: 'ClickHouse',
    role: 'sink',
    crate: 'spate-clickhouse',
    feature: 'clickhouse',
    summary: 'Native or RowBinary encoded on pipeline threads; one deduplication-tokened INSERT per batch.',
    docs: '/docs/user-guide/connectors/sinks/clickhouse',
  },
  {
    name: 'S3',
    role: 'source',
    crate: 'spate-s3',
    feature: 's3',
    summary: 'Coordinated backfill: a leader plans a prefix into splits, workers lease them with fenced progress.',
    docs: '/docs/user-guide/connectors/sources/s3',
  },
  {
    name: 'Avro',
    role: 'format',
    crate: 'spate-avro',
    feature: 'avro',
    summary: 'Confluent wire format; schema-registry fetches never block a pipeline thread.',
    docs: '/docs/user-guide/connectors/formats/avro',
  },
  {
    name: 'JSON',
    role: 'format',
    crate: 'spate-json',
    feature: 'json',
    summary: 'Single, NDJSON and array framings, with an optional SIMD backend.',
    docs: '/docs/user-guide/connectors/formats/json',
  },
  {
    name: 'Coordination',
    role: 'store',
    crate: 'spate-coordination',
    feature: 'coordination',
    summary: 'Leader-computed sticky assignment over a pluggable store.',
    docs: '/docs/user-guide/connectors/coordination/',
  },
  {
    name: 'Datagen',
    role: 'source',
    crate: 'spate-datagen',
    feature: 'datagen',
    summary: 'Referentially consistent storefront events; no broker to stand up first.',
    docs: '/docs/user-guide/connectors/sources/datagen',
  },
];
