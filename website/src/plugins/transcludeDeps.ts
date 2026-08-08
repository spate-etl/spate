import path from 'node:path';
import type {Plugin} from '@docusaurus/types';

/**
 * Register the pass-through loader that makes a docs page depend on the
 * sources its fences are rendered from.
 *
 * See `./transcludeDepsLoader.cjs` for what the loader does and why a remark
 * plugin cannot do it. This half is only the wiring.
 *
 * The loader's basename deliberately differs from this file's. Docusaurus
 * resolves `./src/plugins/transcludeDeps` through jiti, whose extension order
 * is `.js`, `.mjs`, `.cjs`, `.ts` — so a sibling `transcludeDeps.cjs` wins the
 * extensionless import and Docusaurus tries to initialize the *loader* as a
 * plugin (`TypeError: this.getOptions is not a function`).
 */
export default function transcludeDepsPlugin(context: {siteDir: string}): Plugin {
  const repoRoot = path.resolve(context.siteDir, '..');
  const docsDir = path.join(repoRoot, 'docs');
  return {
    name: 'spate-transclude-deps',
    configureWebpack: () => ({
      module: {
        rules: [
          {
            // `enforce: 'pre'` is load-bearing. Without it, whether this loader
            // sees the raw Markdown or the JSX the MDX loader produced depends
            // on the order the two rules happen to land in — normal loaders run
            // right to left across the concatenated match. A `pre` loader is
            // always first, so the regex always sees fences.
            enforce: 'pre',
            test: /\.mdx?$/,
            include: [docsDir],
            use: [
              {
                loader: path.resolve(__dirname, 'transcludeDepsLoader.cjs'),
                options: {repoRoot},
              },
            ],
          },
        ],
      },
    }),
  };
}
