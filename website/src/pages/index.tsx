import type { ReactNode } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';

import styles from './index.module.css';

type Feature = {
  title: string;
  body: ReactNode;
};

const FEATURES: Feature[] = [
  {
    title: 'At-least-once, honestly',
    body: (
      <>
        A source offset commits only after every record derived from it is
        durably written or intentionally dropped — held across rebalances,
        graceful shutdown, and crashes. The watermark stalls rather than ever
        advancing past unacknowledged data.
      </>
    ),
  },
  {
    title: 'Sources never block',
    body: (
      <>
        Backpressure pauses lanes and keeps polling, so a source thread never
        parks in a channel send. Sink slowness throttles throughput without
        ever triggering a consumer-group eviction.
      </>
    ),
  },
  {
    title: 'No hidden costs on the hot path',
    body: (
      <>
        Operator chains are fully monomorphized — one virtual call per batch,
        not per record — and every metric handle is pre-registered at build
        time. Measured at <b>~9&nbsp;ns/record with zero per-record
        allocations</b>.
      </>
    ),
  },
  {
    title: 'Batteries-included connectors',
    body: (
      <>
        Kafka in, Avro decoding, sharded and replicated ClickHouse out — each a
        small, stable trait you can swap for your own. First-class Prometheus
        metrics, <code>/healthz</code>/<code>/readyz</code> probes, and
        drain-on-SIGTERM come standard.
      </>
    ),
  },
];

const TASTE = `let chains = move |_thread| {
    chain_owned::<OrderPlaced, _>(avro.clone())
        .with_metrics("orders", "main")
        .try_map(validate, ErrorPolicy::Skip)
        .map(enrich)
        .sink(ClickHouseEncoder::new(), KeyHashRouter,
              ChunkConfig::default(), queues.clone(), budget.clone())
        .build()
};
PipelineRuntime::new(config, kafka_source, chains, sink, budget).run()?;`;

function Hero() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={styles.hero}>
      <div className="container">
        <span className={styles.perfPill}>
          ~9 ns/record · zero per-record allocations
        </span>
        <h1 className={styles.heroTitle}>{siteConfig.title}</h1>
        <p className={styles.heroTagline}>{siteConfig.tagline}</p>
        <p className={styles.heroPitch}>
          Streaming Extract-Transform-Load pipelines: an operator graph you
          write in Rust and chain into a single monomorphized loop, CPU-pinned
          processing threads over zero-copy borrowed records, checkpoint-driven
          source
          commits, sharded and replicated asynchronous sinks, built-in
          backpressure, and first-class Prometheus metrics.
        </p>
        <div className={styles.buttons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/user-guide/getting-started/"
          >
            Get started
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/user-guide/"
          >
            Read the guide
          </Link>
        </div>
      </div>
    </header>
  );
}

function Features() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FEATURES.map((feature) => (
            <div key={feature.title} className="col col--6 margin-bottom--lg">
              <div className={styles.featureCard}>
                <h3 className={styles.featureTitle}>{feature.title}</h3>
                <p className={styles.featureBody}>{feature.body}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function Taste() {
  return (
    <section className={styles.taste}>
      <div className="container">
        <h2 className={styles.sectionHeading}>A taste</h2>
        <p className={styles.sectionSub}>
          Operators are stateful closures composed into one monomorphized loop;
          YAML carries the tuning and connector configuration, never the
          topology.
        </p>
        <div className={styles.tasteInner}>
          <CodeBlock language="rust">{TASTE}</CodeBlock>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="High-performance, at-least-once ETL pipeline framework for Rust."
    >
      <Hero />
      <main>
        <Features />
        <Taste />
      </main>
    </Layout>
  );
}
