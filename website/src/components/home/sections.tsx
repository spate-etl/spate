import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {usePluginData} from '@docusaurus/useGlobalData';
import CodeBlock from '@theme/CodeBlock';
import MDXContent from '@theme/MDXContent';
import clsx from 'clsx';
import React, {useEffect, useRef, useState} from 'react';

import {CONNECTORS} from '../../data/connectors';
import {FAQ} from '../../data/faq';
import {githubUrl} from '../../repoUrl';
import Taste from '../../pages/_home/taste.mdx';
import Field from '../benchmarks/Field';
import {isoDate, useRichestGroup, useVendorArm} from '../benchmarks/vendorArm';
import {useReveal} from '../motion/useReveal';
import {PRIMARY} from '../Results/data';
import {fmt} from '../Results/format';
import Pipeline from './Pipeline';

function SplitSection({
  id,
  eyebrow,
  title,
  lead,
  aside,
  children,
}: {
  id: string;
  eyebrow: string;
  title: string;
  lead?: React.ReactNode;
  aside?: React.ReactNode;
  children: React.ReactNode;
}) {
  const ref = useReveal<HTMLElement>();
  return (
    <section id={id} ref={ref} className="home-section reveal" aria-labelledby={`${id}-title`}>
      <div className="site-container home-split">
        <div className="home-split__aside">
          <span className="home-eyebrow">{eyebrow}</span>
          <h2 id={`${id}-title`} className="home-h2">
            {title}
          </h2>
          {lead && <p className="home-lead">{lead}</p>}
          {aside}
        </div>
        <div className="home-split__code">{children}</div>
      </div>
    </section>
  );
}

export const HEADLINE = 'Write the transform. Spate owns delivery.';
export const SUBLINE =
  'At-least-once streaming ETL for Rust. Transformations are ordinary functions compiled into one loop. Delivery, backpressure, checkpointing and drain belong to the framework, and each property is numbered and tested.';

function Section({
  id,
  eyebrow,
  title,
  lead,
  children,
  className,
}: {
  id: string;
  eyebrow: string;
  title: string;
  lead?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  const ref = useReveal<HTMLElement>();
  return (
    <section id={id} ref={ref} className={clsx('home-section reveal', className)} aria-labelledby={`${id}-title`}>
      <div className="site-container">
        <div className="home-section__head">
          <span className="home-eyebrow">{eyebrow}</span>
          <h2 id={`${id}-title`} className="home-h2">
            {title}
          </h2>
          {lead && <p className="home-lead">{lead}</p>}
        </div>
        {children}
      </div>
    </section>
  );
}

export function Hero(): React.JSX.Element {
  return (
    <section className="home-hero" aria-labelledby="hero-title">
      <div className="site-container home-hero__grid">
        <div className="home-hero__copy">
          <span className="home-eyebrow">spate /speɪt/ · a river in sudden flood</span>
          <h1 id="hero-title" className="home-h1">
            Write the transform.
            <br />
            Spate owns delivery.
          </h1>
          <p className="home-lead home-hero__lead">{SUBLINE}</p>
          <div className="home-ctas">
            <Link className="home-btn home-btn--primary" to="/docs/user-guide/getting-started/">
              Get started
            </Link>
            <Link className="home-btn home-btn--ghost" to="/benchmarks/">
              Read the benchmarks
            </Link>
          </div>
        </div>
        <div className="home-hero__art">
          <Pipeline className="pipeline" />
        </div>
      </div>
    </section>
  );
}

export function ProofStrip(): React.JSX.Element | null {
  const arm = useVendorArm();
  if (!arm) return null;
  const {row, env, basePath} = arm;
  const tiles: Array<[string, string]> = [
    [PRIMARY, 'rows/s per core'],
    ['rows_per_s', 'rows/s'],
    ['cores_used', 'cores'],
    ['peak_anon_bytes', 'peak memory'],
  ].filter(([id]) => row.metrics[id]) as Array<[string, string]>;
  return (
    <section className="home-proof" aria-label="Benchmark headline figures">
      <div className="site-container">
        <div className="home-proof__tiles">
          {tiles.map(([id, label]) => (
            <div key={id} className="home-proof__tile">
              <span className="home-proof__value">{fmt(row.metrics[id].value, row.metrics[id].unit)}</span>
              <span className="home-proof__label">{label}</span>
            </div>
          ))}
        </div>
        <p className="home-proof__prov">
          Measured on one fixed pipeline, {row.variant_id} arm, at-least-once. {row.version ?? row.commit} ·{' '}
          {env?.id ?? row.env_id} · harness v{row.harness_version} · {isoDate(row.ts_ms)} ·{' '}
          <span title="Run by the vendor of this benchmark">vendor-run †</span> ·{' '}
          <Link to={`${basePath}contract/rules`}>how this was measured</Link>
        </p>
      </div>
    </section>
  );
}

const SHAPES = ['Stream processor', 'Hand-rolled loop', 'Spate'] as const;
const SHAPE_ROWS: Array<[string, string, string, string]> = [
  ['The transform is written in', 'the runtime’s language', 'your language', 'Rust, monomorphized into the loop'],
  ['Profiler and allocator', 'the runtime’s', 'yours', 'yours'],
  ['Delivery guarantee', 'the runtime’s', 'your problem, in production', 'the framework’s · INV-1'],
  ['Backpressure and drain', 'the runtime’s', 'yours to build', 'the framework’s · INV-2, INV-5'],
  ['Properties written down and tested', 'some', 'none', 'ten, numbered'],
];

/**
 * The three-shape comparison. Every column is in the markup; the tabs hide
 * columns only once script runs, so the server-rendered page reads whole.
 */
export function Shapes(): React.JSX.Element {
  const [active, setActive] = useState<number | null>(null);
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  useEffect(() => setActive(2), []);
  const onKey = (e: React.KeyboardEvent, i: number) => {
    const n = SHAPES.length;
    const next = e.key === 'ArrowRight' ? (i + 1) % n : e.key === 'ArrowLeft' ? (i + n - 1) % n : null;
    if (next === null) return;
    e.preventDefault();
    setActive(next);
    tabs.current[next]?.focus();
  };
  return (
    <Section
      id="shapes"
      eyebrow="Why a third shape"
      title="Moving a stream into a warehouse usually means choosing between two shapes."
      lead="Take a general-purpose stream processor and inherit its guarantees with its language. Write the loop yourself and every guarantee becomes your problem. Spate is the third shape.">
      <div role="tablist" aria-label="Shape" className="home-tabs">
        {SHAPES.map((s, i) => (
          <button
            key={s}
            ref={(el) => {
              tabs.current[i] = el;
            }}
            type="button"
            role="tab"
            id={`shape-tab-${i}`}
            aria-selected={active === i}
            aria-controls="shapes-table"
            tabIndex={active === null || active === i ? 0 : -1}
            className={clsx('home-tab', active === i && 'home-tab--on')}
            onClick={() => setActive(i)}
            onKeyDown={(e) => onKey(e, i)}>
            {s}
          </button>
        ))}
      </div>
      <div className="home-table-wrap">
        <table id="shapes-table" className={clsx('home-shapes', active !== null && `home-shapes--focus-${active}`)}>
          <thead>
            <tr>
              <th scope="col">
                <span className="sr-only">Property</span>
              </th>
              {SHAPES.map((s, i) => (
                <th key={s} scope="col" className={clsx(i === 2 && 'home-shapes__spate')}>
                  {s}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {SHAPE_ROWS.map(([h, a, b, c]) => (
              <tr key={h}>
                <th scope="row">{h}</th>
                <td>{a}</td>
                <td>{b}</td>
                <td className="home-shapes__spate">{c}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Section>
  );
}

const STAGES: Array<[string, string, string]> = [
  [
    'Extract',
    'INV-2',
    'One consumer per process. Partitions fan out across CPU-pinned threads as zero-copy lanes. A thread that cannot keep up pauses its lanes and keeps polling; it never blocks on a channel send.',
  ],
  [
    'Transform',
    'INV-7',
    'Operators are stateful closures chained in Rust. A chain compiles to a single loop over borrowed records with no per-record allocation. Failure is Skip or Fail, never a silent drop.',
  ],
  [
    'Load',
    'INV-5',
    'Sinks are sharded and replicated on a shared I/O runtime. Bounded per-shard queues are the backpressure signal that reaches all the way back to Extract.',
  ],
  [
    'Observe',
    'INV-1',
    'A source watermark advances only behind data the sink has acknowledged as durable, so commits trail delivery. Metrics ride the metrics facade; probes ship on the admin server.',
  ],
];

export function HowItWorks(): React.JSX.Element {
  return (
    <Section
      id="how"
      eyebrow="How it works"
      title="One process runs one pipeline, in four stages."
      lead={
        <>
          The property each stage holds to is stated and numbered in the{' '}
          <Link to="/docs/INVARIANTS">invariants</Link>.
        </>
      }>
      <ol className="home-stages">
        {STAGES.map(([name, inv, body], i) => (
          <li key={name} className="home-stage">
            <div className="home-stage__head">
              <span className="home-stage__dot" aria-hidden="true" />
              <span className="home-stage__name">{name}</span>
              <Link className="home-chip" to="/docs/INVARIANTS" aria-label={`${name}: invariant ${inv}`}>
                {inv}
              </Link>
            </div>
            <p>{body}</p>
            {i < STAGES.length - 1 && <span className="home-stage__edge" aria-hidden="true" />}
          </li>
        ))}
      </ol>
    </Section>
  );
}

export function Code(): React.JSX.Element {
  return (
    <SplitSection
      id="taste"
      eyebrow="A taste"
      title="Operators are closures. The chain is one loop."
      lead="YAML carries the tuning and connector configuration, never the topology. This program runs against in-memory mocks, so it needs no infrastructure."
      aside={
        <>
          <p>
            The whole program is{' '}
            <Link href={`${githubUrl}/blob/main/crates/spate/examples/memory_pipeline.rs`}>
              <code>memory_pipeline.rs</code>
            </Link>
            , rendered here from the compiled source.
          </p>
          <code className="home-cmd">
            <span aria-hidden="true">$ </span>cargo run -p spate --example memory_pipeline
          </code>
        </>
      }>
      <MDXContent>
        <Taste />
      </MDXContent>
    </SplitSection>
  );
}

function Hub() {
  const left = [
    ['Kafka', 50],
    ['S3', 100],
    ['Datagen', 150],
  ] as const;
  const right = [
    ['ClickHouse', 70],
    ['Kafka', 130],
  ] as const;
  return (
    <svg className="home-hub" viewBox="0 0 900 200" fill="none" aria-hidden="true">
      <g className="home-hub__edges" strokeWidth="1.4" strokeLinecap="round">
        {left.map(([, y]) => (
          <path key={y} d={`M150 ${y} L400 100`} />
        ))}
        {right.map(([, y]) => (
          <path key={y} d={`M500 100 L750 ${y}`} />
        ))}
        <path d="M450 60 L450 40" strokeDasharray="3 4" />
        <path d="M450 140 L450 160" strokeDasharray="3 4" />
      </g>
      {[...left.map(([l, y]) => [l, 150, y] as const), ...right.map(([l, y]) => [l, 750, y] as const)].map(([l, x, y]) => (
        <g key={`${l}-${x}-${y}`}>
          <circle className="home-hub__node" cx={x} cy={y} r="7" />
          <text className="home-hub__label" x={x < 400 ? x - 14 : x + 14} y={y + 5} textAnchor={x < 400 ? 'end' : 'start'}>
            {l}
          </text>
        </g>
      ))}
      <rect className="home-hub__core" x="400" y="70" width="100" height="60" rx="10" />
      <text className="home-hub__core-label" x="450" y="105" textAnchor="middle">
        one loop
      </text>
      <text className="home-hub__mono" x="450" y="30" textAnchor="middle">
        avro · json
      </text>
      <text className="home-hub__mono" x="450" y="178" textAnchor="middle">
        coordination
      </text>
    </svg>
  );
}

export function Connectors(): React.JSX.Element {
  return (
    <Section
      id="connectors"
      eyebrow="Connectors"
      title="Each connector is one crate behind one feature."
      lead="Nothing is enabled by default. A pipeline that only writes to one sink never compiles the others.">
      <Hub />
      <ul className="home-grid home-grid--4">
        {CONNECTORS.map((c) => (
          <li key={c.crate} className="home-card">
            <div className="home-card__head">
              <span className="home-card__title">{c.name}</span>
              <span className="home-mono home-muted">{c.role}</span>
            </div>
            <p>{c.summary}</p>
            <div className="home-card__foot">
              <Link to={c.docs}>Docs</Link>
              <Link href={`https://crates.io/crates/${c.crate}`} className="home-mono">
                {c.crate}
              </Link>
            </div>
          </li>
        ))}
        <li className="home-card home-card--dashed">
          <div className="home-card__head">
            <span className="home-card__title">Yours</span>
          </div>
          <p>A source or sink is a small trait. Writing your own is a supported path, not a fork.</p>
          <div className="home-card__foot">
            <Link to="/docs/user-guide/extending/">Extending</Link>
            <Link href={`${githubUrl}/blob/main/crates/spate/examples/custom_source_sink.rs`} className="home-mono">
              custom_source_sink.rs
            </Link>
          </div>
        </li>
      </ul>
    </Section>
  );
}

export function Benchmarks(): React.JSX.Element | null {
  const {rows, entrants, basePath} = useRichestGroup();
  if (!rows.length) return null;
  const systems = new Set(rows.map((r) => r.entrant)).size;
  return (
    <Section
      id="benchmarks"
      eyebrow="Benchmarks"
      title={`${systems} systems, one fixed pipeline, one machine.`}
      lead="Every system consumes the same topic, decodes and flattens each message, applies the same filters and lands the rows in the same warehouse. Nothing published is reported by the system that produced it.">
      <div className="home-panel">
        <Field rows={rows} entrants={entrants} metric={PRIMARY} basePath={basePath} compact />
      </div>
      <div className="home-ctas home-ctas--after">
        <Link className="home-btn home-btn--ghost" to={basePath}>
          All results and the fairness contract →
        </Link>
      </div>
    </Section>
  );
}

const PROBES = `readinessProbe: { httpGet: { path: /readyz, port: 9090 } }
livenessProbe:  { httpGet: { path: /healthz, port: 9090 }, periodSeconds: 10 }
terminationGracePeriodSeconds: 30   # above checkpoint.drain_timeout`;

export function Deploy(): React.JSX.Element {
  return (
    <SplitSection
      id="deploy"
      eyebrow="Deploy anywhere"
      title="One binary. Probes, drain and metrics come standard."
      lead="On SIGTERM the pipeline stops consuming, flushes its chains, gives sink batches the drain timeout, commits offsets and exits. Set the grace period above the drain timeout and nothing is lost either way."
      aside={
        <>
          <div className="home-chips">
            {['Docker', 'Kubernetes', 'Prometheus', 'distroless, non-root'].map((c) => (
              <span key={c} className="home-chip">
                {c}
              </span>
            ))}
          </div>
          <p>
            <Link to="/docs/user-guide/deployment/">The deployment guide</Link> covers sizing, scaling out and
            monitoring.
          </p>
        </>
      }>
      <CodeBlock language="yaml">{PROBES}</CodeBlock>
    </SplitSection>
  );
}

const INSTALL = `[dependencies]
spate = { version = "0.2", features = ["kafka", "clickhouse", "avro"] }`;

export function Install(): React.JSX.Element {
  return (
    <SplitSection
      id="install"
      eyebrow="Install"
      title="Add the facade. Turn on what you use."
      lead={
        <>
          Each connector feature turns on one crate. Finer knobs are separate features, listed on{' '}
          <Link href="https://docs.rs/spate">docs.rs</Link> with what they pull in.
        </>
      }>
      <CodeBlock language="toml">{INSTALL}</CodeBlock>
    </SplitSection>
  );
}

type Proof = {stars?: number; releases?: number; downloads?: number; version?: string; asOf?: string};

export function Facts(): React.JSX.Element {
  const proof = (usePluginData('social-proof') as Proof | undefined) ?? {};
  const {siteConfig} = useDocusaurusContext();
  const invariants = siteConfig.customFields?.invariants;
  const facts: Array<[string, string]> = [
    [proof.version ?? '0.x', 'latest release'],
    ['Apache-2.0', 'license, no CLA'],
    ['1.94', 'MSRV, edition 2024'],
    ...(typeof invariants === 'number' ? [[String(invariants), 'numbered invariants'] as [string, string]] : []),
    ...(typeof proof.downloads === 'number' ? [[proof.downloads.toLocaleString('en-US'), 'crates.io downloads'] as [string, string]] : []),
  ];
  return (
    <section className="home-facts" aria-label="Project facts">
      <div className="site-container">
        <ul className="home-facts__row">
          {facts.map(([n, l]) => (
            <li key={l}>
              <span className="home-facts__n">{n}</span>
              <span className="home-facts__l">{l}</span>
            </li>
          ))}
        </ul>
        {proof.asOf && <p className="home-mono home-muted home-facts__asof">Figures as of {proof.asOf}.</p>}
      </div>
    </section>
  );
}

export function Faq(): React.JSX.Element {
  return (
    <SplitSection id="faq" eyebrow="Questions" title="The ones a streaming engineer asks first.">
      <div className="home-faq">
        {FAQ.map((item, i) => (
          <details key={item.q} open={i === 0}>
            <summary>{item.q}</summary>
            <p>{item.a}</p>
          </details>
        ))}
      </div>
    </SplitSection>
  );
}
