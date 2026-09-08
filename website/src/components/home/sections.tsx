import Link from '@docusaurus/Link';
import CodeBlock from '@theme/CodeBlock';
import MDXContent from '@theme/MDXContent';
import clsx from 'clsx';
import React, {useState} from 'react';

import {CONNECTORS} from '../../data/connectors';
import {FAQ} from '../../data/faq';
import {githubUrl} from '../../repoUrl';
import Taste from '../../pages/_home/taste.mdx';
import Field from '../benchmarks/Field';
import {useRichestGroup} from '../benchmarks/vendorArm';
import {useReveal} from '../motion/useReveal';
import {isRanked, PRIMARY, type Row} from '../Results/data';
import Incident from './Incident';
import Pipeline from './Pipeline';

/** The heading takes `${id}-title`, which the enclosing section names in `aria-labelledby`. */
function SectionHead({
  id,
  eyebrow,
  title,
  lead,
}: {
  id: string;
  eyebrow?: string;
  title: string;
  lead?: React.ReactNode;
}) {
  return (
    <>
      {eyebrow && <span className="home-eyebrow">{eyebrow}</span>}
      <h2 id={`${id}-title`} className="home-h2">
        {title}
      </h2>
      {lead && <p className="home-lead">{lead}</p>}
    </>
  );
}

function SplitSection({
  id,
  eyebrow,
  title,
  lead,
  aside,
  children,
}: {
  id: string;
  eyebrow?: string;
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
          <SectionHead id={id} eyebrow={eyebrow} title={title} lead={lead} />
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
  center,
}: {
  id: string;
  eyebrow?: string;
  title: string;
  lead?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  center?: boolean;
}) {
  const ref = useReveal<HTMLElement>();
  return (
    <section id={id} ref={ref} className={clsx('home-section reveal', className)} aria-labelledby={`${id}-title`}>
      <div className="site-container">
        <div className={clsx('home-section__head', center && 'home-section__head--center')}>
          <SectionHead id={id} eyebrow={eyebrow} title={title} lead={lead} />
        </div>
        {children}
      </div>
    </section>
  );
}

/**
 * Each entrant's best headline-eligible arm by the primary metric, one lane per
 * system.
 */
function headlineLanes(rows: Row[]): Row[] {
  const eligible = rows.filter((r) => isRanked(r) && r.metrics[PRIMARY]);
  const hib = eligible[0]?.metrics[PRIMARY].higher_is_better ?? true;
  const best = new Map<string, Row>();
  for (const r of eligible) {
    const held = best.get(r.entrant);
    const v = r.metrics[PRIMARY].value;
    const h = held?.metrics[PRIMARY].value;
    if (h === undefined || (hib ? v > h : v < h)) best.set(r.entrant, r);
  }
  return [...best.values()];
}

/**
 * The benchmark chart in the fold, every figure and label read from the
 * published results. Renders nothing when no results are published.
 */
function HeroField(): React.JSX.Element | null {
  const {rows, entrants, basePath} = useRichestGroup();
  const lanes = headlineLanes(rows);
  if (!lanes.length) return null;
  const systems = new Set(lanes.map((r) => r.entrant)).size;
  return (
    <figure className="home-hero__chart">
      <div className="home-panel home-hero__panel">
        <Field rows={lanes} entrants={entrants} metric={PRIMARY} basePath={basePath} compact />
      </div>
      <figcaption className="home-hero__chart-cap">
        {systems} systems, one fixed pipeline, one machine, each at its best headline-eligible arm.{' '}
        <Link to={basePath}>All results and the fairness contract →</Link>
      </figcaption>
    </figure>
  );
}

const QUICKSTART = '/docs/user-guide/getting-started/quickstart/';
const INSTALL_COMMAND = 'cargo add spate --features kafka,clickhouse,avro';

function InstallCommand(): React.JSX.Element {
  const [status, setStatus] = useState('');
  const [copying, setCopying] = useState(false);
  async function copy() {
    setStatus('');
    setCopying(true);
    try {
      await navigator.clipboard.writeText(INSTALL_COMMAND);
      setStatus('Command copied.');
    } catch {
      setStatus('Could not copy. Select and copy the command above.');
    } finally {
      setCopying(false);
    }
  }
  return (
    <div className="home-install">
      <p className="home-muted">Add Spate to an existing Cargo project</p>
      <div className="home-install__row">
        <code className="home-cmd"><span aria-hidden="true">$ </span>{INSTALL_COMMAND}</code>
        <button type="button" className="home-btn home-btn--ghost" onClick={copy}
          disabled={copying} aria-label="Copy installation command">Copy</button>
      </div>
      <p className="home-install__status" role="status" aria-live="polite" aria-atomic="true">{status}</p>
    </div>
  );
}

export function Hero(): React.JSX.Element {
  return (
    <section className="home-hero" aria-labelledby="hero-title">
      <div className="home-hero__substrate" aria-hidden="true" />
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
          <InstallCommand />
        </div>
        <HeroField />
      </div>
    </section>
  );
}

export function WhyFast(): React.JSX.Element {
  return (
    <Section
      id="why-fast"
      center
      eyebrow="Why spate is fast"
      title="Your function is compiled into the loop, and the loop runs in your process."
      lead="A general-purpose stream processor runs your transformation inside a runtime you do not own, and every record crosses into your function and back out. A consumer loop you write yourself runs in your process with your allocator and your profiler, and each delivery guarantee exists once you have written it. Spate compiles your transformation into a loop that runs in your process, while delivery, backpressure, checkpointing and drain belong to the framework and hold to properties that are numbered and tested.">
      <Incident />
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
    'Sinks are sharded and replicated on a shared I/O runtime. Everything the intake path of a sink worker blocks on waits alongside the drain deadline, so the deadline stays polled while the worker is blocked.',
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
      <div className="home-how__art">
        <Pipeline className="pipeline" />
      </div>
      <ol className="home-stages">
        {STAGES.map(([name, inv, body], i) => (
          <li key={name} className="home-stage">
            <div className="home-stage__head">
              <span className="home-stage__dot" aria-hidden="true" />
              <span className="home-stage__name">{name}</span>
              <Link
                className="home-chip"
                to={`/docs/INVARIANTS#${inv.toLowerCase()}`}
                aria-label={`${name}: invariant ${inv}`}>
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
      lead={
        <>
          The example splits comma-separated input into fields. Each <code>word</code> is a{' '}
          <code>Vec&lt;u8&gt;</code> containing one field’s bytes.
        </>
      }
      aside={
        <>
          <p>
            The filter removes empty fields. The map uppercases each remaining field.
            The excerpt shows those two steps.
          </p>
          <p>
            See the{' '}
            <Link href={`${githubUrl}/blob/main/crates/spate/examples/memory_pipeline.rs`}>
              complete program
            </Link>{' '}
            for the source, sink and pipeline setup.
          </p>
          <Link className="home-btn home-btn--primary" to={QUICKSTART}>
            Run the in-memory example
          </Link>
        </>
      }>
      <MDXContent>
        <Taste />
      </MDXContent>
      <dl className="home-example-flow">
        <div><dt>Input</dt><dd><code>delta,,epsilon</code></dd></div>
        <div>
          <dt><code>word</code> values</dt>
          <dd><code>b"delta"</code>, <code>b""</code> (empty), <code>b"epsilon"</code></dd>
        </div>
        <div><dt>Output</dt><dd><code>DELTA</code>, <code>EPSILON</code></dd></div>
      </dl>
    </SplitSection>
  );
}

export function Connectors(): React.JSX.Element {
  return (
    <Section
      id="connectors"
      eyebrow="Connectors"
      title="Each connector is one crate behind one feature."
      lead={
        <>
          Nothing is enabled by default. A pipeline that only writes to one sink never compiles the others. Finer knobs
          are separate features, listed on <Link href="https://docs.rs/spate">docs.rs</Link> with what they pull in.
        </>
      }>
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
          <p>A source or sink is a small trait. Writing your own is a supported path.</p>
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

const PROBES = `readinessProbe:
  httpGet: { path: /readyz, port: 9090 }
livenessProbe:
  httpGet: { path: /healthz, port: 9090 }
  periodSeconds: 10
# Above checkpoint.drain_timeout.
terminationGracePeriodSeconds: 30`;

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

export function Faq(): React.JSX.Element {
  return (
    <SplitSection id="faq" title="A streaming engineer asks these first.">
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
