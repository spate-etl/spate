// A pass-through loader whose only job is to tell the bundler that this MDX
// module depends on the repository files it points at — the sources it renders
// fences from, and the files its `repo:` links resolve against.
//
// The transclusion itself is ../remark/transclude.ts and the link resolution is
// ../remark/repoLinks.ts. A remark plugin runs inside the MDX loader's
// process() call with no loader context, so it cannot call this.addDependency —
// and without a registered dependency, editing a `.rs` file leaves every cached
// MDX compile untouched. `npm start` then shows the old snippet, and a
// warm-cache `npm run build` ships it. This site runs `future.faster`, which
// turns on the Rspack persistent cache, and CI restores that cache, so this is
// not a theoretical window.
//
// Deliberately over-approximate. A regex over the raw text can be wrong in two
// directions and only one of them matters: naming a file that is not really
// used costs a needless rebuild, while missing one ships a stale page. So it
// matches loosely and never tries to be exact — the exact parse is the remark
// plugin's job, and the gate is scripts/transclude.sh.
//
// CommonJS on purpose: rspack requires a loader by path, outside the jiti
// instance that transpiles the TypeScript config and everything it imports.
const path = require('node:path');

const FENCE_FILE_RE =
  /^[ \t]*(?:`{3,}|~{3,})[^\n]*?\bfile=(?:"([^"]*)"|'([^']*)'|(\S+))/gm;

// A Markdown link destination beginning `repo:`, up to the closing paren. Not
// anchored to a line start — these sit mid-sentence, wherever the paragraph
// happened to wrap.
const REPO_LINK_RE = /\]\(repo:([^)\s]+)\)/g;

module.exports = function transcludeDepsLoader(source) {
  const {repoRoot} = this.getOptions();
  // Module-level regexes with /g carry lastIndex between calls.
  for (const re of [FENCE_FILE_RE, REPO_LINK_RE]) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(source)) !== null) {
      const rel = m[1] ?? m[2] ?? m[3];
      if (!rel || rel.startsWith('/') || rel.split('/').includes('..')) {
        continue;
      }
      // A directory target — a link naming a crate rather than a file — would
      // need addContextDependency, which registers a whole watched tree for a
      // pointer that changes only when the crate is renamed. scripts/transclude.sh
      // reads both sides off disk and covers it without the cost.
      this.addDependency(path.join(repoRoot, rel));
    }
  }
  return source;
};
