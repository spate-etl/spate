import path from 'node:path';
import type {Plugin} from '@docusaurus/types';

/**
 * Register the pass-through loader that makes a docs page, or a page partial
 * under src/pages, depend on the sources its fences are rendered from.
 *
 * See `./transcludeDepsLoader.cjs` for what the loader does and why a remark
 * plugin cannot do it. This half is only the wiring.
 *
 * The loader's basename deliberately differs from this file's. Docusaurus
 * resolves `./src/plugins/transcludeDeps` through jiti, whose extension order
 * is `.js`, `.mjs`, `.cjs`, `.ts`. A sibling `transcludeDeps.cjs` would win the
 * extensionless import, and Docusaurus would initialize the *loader* as a
 * plugin (`TypeError: this.getOptions is not a function`).
 */
export default function transcludeDepsPlugin(context: {siteDir: string}): Plugin {
  const repoRoot = path.resolve(context.siteDir, '..');
  const docsDir = path.join(repoRoot, 'docs');
  const pagesDir = path.join(context.siteDir, 'src', 'pages');
  return {
    name: 'spate-transclude-deps',
    configureWebpack: () => ({
      module: {
        rules: [
          {
            // `enforce: 'pre'` decides what this loader sees. Without it,
            // whether it gets the raw Markdown or the JSX the MDX loader
            // produced depends on the order the two rules land in, since
            // normal loaders run right to left across the concatenated match.
            // A `pre` loader is always first, so the regex always sees fences.
            enforce: 'pre',
            test: /\.mdx?$/,
            include: [docsDir, pagesDir],
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
