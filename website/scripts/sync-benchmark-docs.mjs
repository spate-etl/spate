// Renders the benchmark submodule's pages into `website/.benchmarks/`, the
// docs tree the `benchmarks` docs instance reads.
//
// The submodule's `docs/` pages are copied as they are. The fairness contract
// lives in its `methodology/` directory, where an implementer of an arm reads
// it, so each part is copied with front matter prepended. The output directory
// is generated and gitignored: one source, rendered at build time.

import {cpSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..', 'benchmark');
const docs = join(here, '..', '.benchmarks');

/** Root document → docs page, with the front matter the site needs. */
const SYNCED = [
  {
    from: 'methodology/README.md',
    to: 'contract/rules.md',
    frontMatter: {
      id: 'rules',
      title: 'The fairness contract',
      description:
        'The goal, the pipeline, the delivery guarantee, and the seven rules every arm conforms to.',
      sidebar_label: 'Goal, pipeline and rules',
    },
    // The heading is supplied by front matter, so the source's own H1 would
    // render twice.
    stripFirstHeading: true,
  },
  {
    from: 'methodology/envelope.md',
    to: 'contract/envelope.md',
    frontMatter: {
      id: 'envelope',
      title: 'The resource envelope',
      description:
        'What each system is given, what the infrastructure around it is given, and the headroom rule.',
      sidebar_label: 'The resource envelope',
    },
    stripFirstHeading: true,
  },
  {
    from: 'methodology/measurement.md',
    to: 'contract/measurement.md',
    frontMatter: {
      id: 'measurement',
      title: 'How you are measured',
      description:
        'Why an arm must not instrument itself, what the instrument can resolve, and which mode measures what.',
      sidebar_label: 'How you are measured',
    },
    stripFirstHeading: true,
  },
  {
    from: 'methodology/comparability.md',
    to: 'contract/comparability.md',
    frontMatter: {
      id: 'comparability',
      title: 'What makes two numbers comparable',
      description:
        'How the corpus is generated, what invalidates a comparison outright, and why tuning is not measurement.',
      sidebar_label: 'Comparability',
    },
    stripFirstHeading: true,
  },
];

function toFrontMatter(fields) {
  const lines = Object.entries(fields).map(([k, v]) =>
    typeof v === 'string' && (v.includes(':') || v.includes('\n'))
      ? `${k}: ${JSON.stringify(v)}`
      : `${k}: ${v}`,
  );
  return `---\n${lines.join('\n')}\n---\n\n`;
}

rmSync(docs, {recursive: true, force: true});
mkdirSync(docs, {recursive: true});
cpSync(join(root, 'docs'), docs, {recursive: true});
process.stdout.write(`copied benchmark/docs -> .benchmarks\n`);

for (const spec of SYNCED) {
  const src = readFileSync(join(root, spec.from), 'utf8');
  let body = src;
  if (spec.stripFirstHeading) {
    body = body.replace(/^#\s+.*\n+/, '');
  }
  // Links in the root document are relative to the repository; on the site they
  // have to point at the repository on GitHub, since the site has no page for
  // `workload/schema/sensor_batch.avsc`.
  body = body.replace(
    /\]\((workload|entrants|environments|harness)\//g,
    '](https://github.com/spate-etl/benchmark/blob/main/$1/',
  );

  // A link from one synced document to another has to name the docs-tree file,
  // not the root one: right for a reader on GitHub, and a path that does not
  // exist under that name here. The site's broken-link guard fails the build
  // rather than shipping it. One source, two audiences, rewritten at the seam.
  for (const other of SYNCED) {
    const base = other.from.split('/').pop();
    const page = other.to.split('/').pop();
    body = body.replaceAll(`](${base})`, `](./${page})`);
    body = body.replaceAll(`](${other.from})`, `](./${page})`);
  }

  const banner =
    `{/* Generated from benchmark/${spec.from} by website/scripts/sync-benchmark-docs.mjs. ` +
    `Edit that file, not this one. */}\n\n`;

  const out = join(docs, spec.to);
  mkdirSync(dirname(out), {recursive: true});
  writeFileSync(out, toFrontMatter(spec.frontMatter) + banner + body);
  process.stdout.write(`synced benchmark/${spec.from} -> .benchmarks/${spec.to}\n`);
}
