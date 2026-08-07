/**
 * Render a fenced code block from a region of a compiled source file.
 *
 * A page names the source and the region on the info string and leaves the
 * fence empty:
 *
 *     ```rust file=crates/spate/examples/memory_pipeline.rs region=chain
 *     ```
 *
 * The source marks the region with mdBook's anchor comments, which are the
 * convention the Rust Book and every mdBook-based project already use:
 *
 *     // ANCHOR: chain
 *     // ANCHOR_END: chain
 *
 * Markers are matched anywhere on a line and carry no comment syntax of their
 * own, so `# ANCHOR: x` in a YAML file beside an example works the same way.
 *
 * Why this exists: nothing compiles a fenced block in an `.mdx` file, so a
 * hand-written snippet survives every gate this repository has. Anything under
 * `crates/` is compiled by `cargo clippy --workspace --all-targets`, which
 * `make gates` runs. See `docs/STYLE.md` § 10.
 *
 * Every failure below throws, and there is no way to switch that off. A fence
 * that silently renders empty is the defect this mechanism exists to remove,
 * and `onBrokenLinks: 'throw'` already sets the standard for a pointer that
 * stops resolving.
 *
 * One thing this file cannot do: a remark plugin runs inside the MDX loader's
 * `process()` call with no loader context, so it cannot call `addDependency`
 * to tell the bundler a page depends on a `.rs` file. Two other pieces cover
 * that, and neither lives here — `../plugins/transcludeDeps.cjs` registers the
 * real dependency, and `scripts/transclude.sh` re-checks both sides off disk
 * with no cache of any kind.
 */
import fs from 'node:fs';
import path from 'node:path';

// website/src/remark -> website/src -> website -> repository root.
const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');

// Which trees a page may quote from. `crates/` rather than
// `crates/spate/examples/` because a trait definition worth quoting lives in a
// connector's `src/`, and `clippy --workspace --all-targets` compiles all of it
// alike. `docs/STYLE.md` § 10 states the editorial preference for an example.
const ALLOWED_PREFIXES = ['crates/'];

// `ANCHOR_END` contains `ANCHOR`, so the end pattern is always tested FIRST.
// Neither carries the `g` flag on purpose: a global regex keeps `lastIndex`
// between calls, which would make `.test()` alternate on identical input.
const ANCHOR_END_RE = /ANCHOR_END:\s*([A-Za-z0-9_-]+)/;
const ANCHOR_RE = /ANCHOR:\s*([A-Za-z0-9_-]+)/;

type CodeNode = {
  type: 'code';
  lang?: string | null;
  meta?: string | null;
  value: string;
  position?: {start?: {line?: number}};
};

type AnyNode = {type: string; children?: AnyNode[]};

type VFileLike = {path?: string};

type Region = {start: number; end: number};

type SourceIndex = {lines: string[]; regions: {[name: string]: Region}};

/**
 * Read-and-index cache, keyed on path and invalidated on mtime.
 *
 * A production build reads each source once however many pages quote it. The
 * mtime half is what keeps `npm start` honest: the dev server keeps this module
 * alive across rebuilds, so a cache keyed on path alone would serve the
 * contents a file had when the session started for the rest of the session.
 */
const cache = new Map<string, {mtimeMs: number; index: SourceIndex}>();

/** `file=a/b.rs region=x title="two words"` -> ordered [key, value|null]. */
function parseMeta(meta: string): [string, string | null][] {
  const pairs: [string, string | null][] = [];
  const re = /([A-Za-z_][A-Za-z0-9_-]*)(?:=(?:"([^"]*)"|'([^']*)'|([^\s]+)))?/g;
  let m: RegExpExecArray | null;
  // Every alternative needs at least one character, so this cannot spin on a
  // zero-length match.
  while ((m = re.exec(meta)) !== null) {
    pairs.push([m[1], m[2] ?? m[3] ?? m[4] ?? null]);
  }
  return pairs;
}

/**
 * Values are always quoted, whitespace or not.
 *
 * Not cosmetic: Docusaurus parses a code-block title with
 * `/title=(?<quote>["'])(?<title>.*?)\1/` (theme-common's `codeBlockUtils`),
 * which *requires* the quotes. An unquoted `title=crates/…/memory_pipeline.rs`
 * parses as no title at all and the header silently does not render.
 */
function serializeMeta(pairs: [string, string | null][]): string {
  return pairs
    .map(([k, v]) => {
      if (v === null) {
        return k;
      }
      const quote = v.includes('"') ? "'" : '"';
      return `${k}=${quote}${v}${quote}`;
    })
    .join(' ');
}

function isMarker(line: string): boolean {
  return ANCHOR_END_RE.test(line) || ANCHOR_RE.test(line);
}

/**
 * Index every marker in a file, and reject a malformed set.
 *
 * The whole file is validated rather than only the region a page asked for, for
 * two reasons: the error is then the same whichever page happens to compile
 * first, and it is the same set `scripts/transclude.sh` checks — so the build
 * and the gate cannot disagree about whether a file is well formed.
 */
function indexRegions(lines: string[], rel: string): {[name: string]: Region} {
  const starts: {[name: string]: number} = Object.create(null);
  const ends: {[name: string]: number} = Object.create(null);

  lines.forEach((line, i) => {
    const end = ANCHOR_END_RE.exec(line);
    if (end) {
      const name = end[1];
      if (name in ends) {
        throw new Error(
          `${rel}:${i + 1}: duplicate \`ANCHOR_END: ${name}\`; the first is at ` +
            `line ${ends[name] + 1}.`,
        );
      }
      ends[name] = i;
      return;
    }
    const start = ANCHOR_RE.exec(line);
    if (start) {
      const name = start[1];
      if (name in starts) {
        throw new Error(
          `${rel}:${i + 1}: duplicate \`ANCHOR: ${name}\`; the first is at line ` +
            `${starts[name] + 1}. A region name is unique within a file — two ` +
            `disjoint stretches cannot share one.`,
        );
      }
      starts[name] = i;
    }
  });

  const regions: {[name: string]: Region} = Object.create(null);
  Object.keys(starts).forEach((name) => {
    const s = starts[name];
    if (!(name in ends)) {
      throw new Error(
        `${rel}:${s + 1}: \`ANCHOR: ${name}\` has no matching \`ANCHOR_END: ${name}\`.`,
      );
    }
    const e = ends[name];
    if (e < s) {
      throw new Error(
        `${rel}: \`ANCHOR_END: ${name}\` (line ${e + 1}) comes before ` +
          `\`ANCHOR: ${name}\` (line ${s + 1}).`,
      );
    }
    regions[name] = {start: s, end: e};
  });
  Object.keys(ends).forEach((name) => {
    if (!(name in starts)) {
      throw new Error(
        `${rel}:${ends[name] + 1}: \`ANCHOR_END: ${name}\` has no matching ` +
          `\`ANCHOR: ${name}\`.`,
      );
    }
  });
  return regions;
}

function readSource(abs: string, rel: string): SourceIndex {
  const mtimeMs = fs.statSync(abs).mtimeMs;
  const hit = cache.get(abs);
  if (hit && hit.mtimeMs === mtimeMs) {
    return hit.index;
  }
  const lines = fs.readFileSync(abs, 'utf8').split('\n');
  const index: SourceIndex = {lines, regions: indexRegions(lines, rel)};
  cache.set(abs, {mtimeMs, index});
  return index;
}

/**
 * Marker lines out, blank edges out, common indentation out.
 *
 * Dropping *every* marker line rather than only the extracted pair is mdBook's
 * rule, and it is what makes nesting work: a region containing another region's
 * markers renders without them. It also keeps scaffolding out of what a reader
 * pastes — the markers exist for the build, not for them.
 */
function render(lines: string[]): string {
  const kept = lines.filter((l) => !isMarker(l));
  while (kept.length > 0 && kept[0].trim() === '') {
    kept.shift();
  }
  while (kept.length > 0 && kept[kept.length - 1].trim() === '') {
    kept.pop();
  }

  let indent = Number.POSITIVE_INFINITY;
  for (const line of kept) {
    if (line.trim() === '') {
      continue;
    }
    indent = Math.min(indent, /^[ \t]*/.exec(line)![0].length);
  }
  if (!Number.isFinite(indent) || indent === 0) {
    return kept.join('\n');
  }
  return kept.map((l) => (l.trim() === '' ? '' : l.slice(indent))).join('\n');
}

/**
 * Resolve a `file=` value against the repository root.
 *
 * The value is author-controlled text out of a Markdown file, so it is handled
 * as data throughout: matched against a prefix list, and re-checked after the
 * join rather than trusted because the prefix looked right.
 */
function resolveSource(rel: string): string {
  if (rel === '' || rel.startsWith('/') || path.isAbsolute(rel)) {
    throw new Error(`file="${rel}" must be a path relative to the repository root.`);
  }
  if (rel.split('/').includes('..')) {
    throw new Error(`file="${rel}" must not contain a \`..\` segment.`);
  }
  if (!ALLOWED_PREFIXES.some((p) => rel.startsWith(p))) {
    throw new Error(
      `file="${rel}" is outside the trees a page may quote from ` +
        `(${ALLOWED_PREFIXES.join(', ')}). Only compiled sources are ` +
        `transcludable — see docs/STYLE.md § 10.`,
    );
  }
  const abs = path.join(REPO_ROOT, rel);
  if (!abs.startsWith(REPO_ROOT + path.sep)) {
    throw new Error(`file="${rel}" resolves outside the repository.`);
  }
  if (!fs.existsSync(abs)) {
    throw new Error(`file="${rel}" does not exist.`);
  }
  return abs;
}

/**
 * Depth-first walk over `children`.
 *
 * Hand-written rather than `unist-util-visit`, which is in `node_modules` only
 * because something else depends on it. Importing a package this project does
 * not declare works until the day the tree flattens differently. MDX JSX nodes
 * carry `children` too, so a fence inside a `<Tabs>` is reached.
 */
function visitCode(node: AnyNode, fn: (n: CodeNode) => void): void {
  if (node.type === 'code') {
    fn(node as unknown as CodeNode);
    return;
  }
  if (!node.children) {
    return;
  }
  for (const child of node.children) {
    visitCode(child, fn);
  }
}

export default function remarkTransclude() {
  return function transformer(tree: AnyNode, file: VFileLike): void {
    const mdxRel = file.path ? path.relative(REPO_ROOT, file.path) : '<unknown page>';

    visitCode(tree, (node) => {
      const meta = node.meta ?? '';
      if (!/\bfile=/.test(meta)) {
        return;
      }

      const line = node.position?.start?.line ?? 0;
      const pairs = parseMeta(meta);
      const get = (k: string): string | null | undefined =>
        pairs.find(([key]) => key === k)?.[1];

      const fail = (msg: string): never => {
        throw new Error(
          `[transclude] ${mdxRel}:${line}\n  ${msg}\n  ` +
            `Fence: \`\`\`${node.lang ?? ''} ${meta}`,
        );
      };

      const fileAttr = get('file');
      const regionAttr = get('region');

      if (typeof fileAttr !== 'string' || fileAttr === '') {
        return fail(
          '`file=` needs a value, e.g. `file=crates/spate/examples/memory_pipeline.rs`.',
        );
      }
      if (regionAttr === null) {
        return fail(
          '`region=` was given with no value. Drop it to render the whole file.',
        );
      }
      if (node.value.trim() !== '') {
        return fail(
          'this fence carries `file=` and hand-written content. One or the ' +
            'other: rendering the region would discard what is written here.',
        );
      }

      let abs: string;
      let index: SourceIndex;
      try {
        abs = resolveSource(fileAttr);
        index = readSource(abs, fileAttr);
      } catch (e) {
        return fail((e as Error).message);
      }

      let slice: string[];
      if (typeof regionAttr === 'string') {
        const region = index.regions[regionAttr];
        if (!region) {
          const known = Object.keys(index.regions);
          return fail(
            `${fileAttr} defines no region "${regionAttr}".\n  ` +
              (known.length > 0
                ? `It defines: ${known.join(', ')}.`
                : 'It defines no regions at all.') +
              `\n  Add \`// ANCHOR: ${regionAttr}\` and \`// ANCHOR_END: ` +
              `${regionAttr}\` around the lines this page shows.`,
          );
        }
        slice = index.lines.slice(region.start + 1, region.end);
      } else {
        slice = index.lines;
      }

      const value = render(slice);
      if (value === '') {
        return fail(
          `${fileAttr}${regionAttr ? ` region "${regionAttr}"` : ''} is empty ` +
            'once the markers are stripped. An empty fence is exactly the ' +
            'silent failure this mechanism exists to prevent.',
        );
      }

      node.value = value;

      // `file` and `region` are ours and mean nothing to @theme/CodeBlock,
      // which parses what is left (title, showLineNumbers, ...). A title is
      // filled in when the author gave none, because where a snippet came from
      // is the reason a reader can trust it.
      const rest = pairs.filter(([k]) => k !== 'file' && k !== 'region');
      if (!rest.some(([k]) => k === 'title')) {
        rest.push(['title', fileAttr]);
      }
      node.meta = serializeMeta(rest);
    });
  };
}
