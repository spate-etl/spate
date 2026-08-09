/**
 * Resolve `repo:` links into URLs of files in this repository on GitHub.
 *
 * A page names a repository-relative path and lets the build produce the URL:
 *
 *     [`custom_source_sink.rs`](repo:crates/spate/examples/custom_source_sink.rs)
 *
 * which renders as a link to
 * `https://github.com/spate-etl/spate/blob/main/crates/spate/examples/custom_source_sink.rs`.
 * A path naming a directory resolves to `/tree/` instead of `/blob/`, so a
 * reference to a crate rather than a file lands on the crate.
 *
 * Why this exists: a reader of the site is not holding this repository, so a
 * bare path in prose describes a layout only a contributor can act on. Writing
 * the full URL by hand instead would put the host, the organization, the
 * repository, the ref and the path beyond the reach of any check. Only the path
 * is authored here, and only the path can be wrong — which is why this file can
 * verify it. See `docs/STYLE.md` § 7.
 *
 * Every failure below throws, and there is no way to switch that off. A link
 * that resolves to nothing is what `onBrokenLinks: 'throw'` already refuses for
 * an internal page, and a pointer at a source file is no weaker a promise.
 *
 * Registered alongside `./transclude.ts` in `beforeDefaultRemarkPlugins`, ahead
 * of the default plugin that resolves Markdown links — which would otherwise
 * meet a scheme it does not know. As with transclusion, a remark plugin has no
 * loader context and cannot call `addDependency`; the loader in
 * `../plugins/transcludeDepsLoader.cjs` registers link targets too, and
 * `scripts/transclude.sh` re-checks them off disk.
 */
import fs from 'node:fs';
import path from 'node:path';

// website/src/remark -> website/src -> website -> repository root.
const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');

const SCHEME = 'repo:';

type LinkNode = {
  type: string;
  url?: string;
  position?: {start?: {line?: number}};
};

type AnyNode = {type: string; url?: string; children?: AnyNode[]};

type VFileLike = {path?: string};

/**
 * Depth-first walk over `children`, visiting every node that carries a `url`.
 *
 * Both `link` and `definition` are matched: a reference-style link keeps its
 * destination on the definition, so visiting only `link` would let one through
 * unresolved. Hand-written rather than `unist-util-visit`, which is in
 * `node_modules` only because something else depends on it — see the note in
 * `./transclude.ts`. MDX JSX nodes carry `children` too, so a link inside a
 * `<Tabs>` is reached.
 */
function visitLinks(node: AnyNode, fn: (n: LinkNode) => void): void {
  if (node.type === 'link' || node.type === 'definition') {
    fn(node as LinkNode);
  }
  if (!node.children) {
    return;
  }
  for (const child of node.children) {
    visitLinks(child, fn);
  }
}

/**
 * Resolve a `repo:` target against the repository root.
 *
 * The value is author-controlled text out of a Markdown file, so it is handled
 * as data throughout: checked in pieces, and re-checked after the join rather
 * than trusted because the pieces looked right. Returns which of GitHub's two
 * views the target belongs in.
 *
 * Unlike a transcluded fence, a link is not restricted to the trees a snippet
 * may be quoted from: `examples/docker/Dockerfile` and `CONTRIBUTING.md` are
 * legitimate destinations, and neither is compiled by anything.
 */
function resolveTarget(rel: string): 'blob' | 'tree' {
  if (rel === '' || rel.startsWith('/') || path.isAbsolute(rel)) {
    throw new Error(
      `${SCHEME}${rel} must name a path relative to the repository root.`,
    );
  }
  if (rel.split('/').includes('..')) {
    throw new Error(`${SCHEME}${rel} must not contain a \`..\` segment.`);
  }
  // A `#L42` suffix is the one addressing form this repository has ruled out:
  // it repoints silently the next time anything above it moves, which is the
  // rot the anchor comments in `./transclude.ts` exist to avoid. Rendering a
  // region is how a page shows specific lines.
  if (rel.includes('#')) {
    throw new Error(
      `${SCHEME}${rel} carries a fragment. A link addresses a file, not lines ` +
        'within it — render the lines as a fence instead (docs/STYLE.md § 10).',
    );
  }
  const abs = path.join(REPO_ROOT, rel);
  if (!abs.startsWith(REPO_ROOT + path.sep)) {
    throw new Error(`${SCHEME}${rel} resolves outside the repository.`);
  }
  let stat: fs.Stats;
  try {
    stat = fs.statSync(abs);
  } catch {
    throw new Error(`${SCHEME}${rel} does not exist.`);
  }
  return stat.isDirectory() ? 'tree' : 'blob';
}

export default function remarkRepoLinks({
  githubUrl,
  sourceRef,
}: {
  githubUrl: string;
  sourceRef: string;
}) {
  return function transformer(tree: AnyNode, file: VFileLike): void {
    const mdxRel = file.path ? path.relative(REPO_ROOT, file.path) : '<unknown page>';

    visitLinks(tree, (node) => {
      const url = node.url ?? '';
      if (!url.startsWith(SCHEME)) {
        return;
      }
      const rel = url.slice(SCHEME.length);
      try {
        node.url = `${githubUrl}/${resolveTarget(rel)}/${sourceRef}/${rel}`;
      } catch (e) {
        const line = node.position?.start?.line ?? 0;
        throw new Error(
          `[repo-link] ${mdxRel}:${line}\n  ${(e as Error).message}\n  ` +
            `Link: (${url})`,
        );
      }
    });
  };
}
