// A pass-through loader that tells the bundler which repository files an MDX
// module depends on, both the sources it renders fences from and the files its
// `repo:` links resolve against.
//
// The transclusion itself is ../remark/transclude.ts and the link resolution is
// ../remark/repoLinks.ts. A remark plugin runs inside the MDX loader's
// process() call with no loader context, so it cannot call this.addDependency.
// Without a registered dependency, editing a `.rs` file leaves every cached MDX
// compile untouched, `npm start` shows the old snippet, and a warm-cache
// `npm run build` ships it.
//
// Deliberately over-approximate. Naming a file that is not used costs a
// needless rebuild; missing one ships a stale page. The exact parse is the
// remark plugin's job, and the gate is scripts/transclude.sh.
//
// CommonJS on purpose: rspack requires a loader by path, outside the jiti
// instance that transpiles the TypeScript config and everything it imports.
const path = require('node:path');

const FENCE_FILE_RE =
  /^[ \t]*(?:`{3,}|~{3,})[^\n]*?\bfile=(?:"([^"]*)"|'([^']*)'|(\S+))/gm;

// A Markdown link destination beginning `repo:`, up to the closing paren. Not
// anchored to a line start, since these sit mid-sentence.
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
      // A directory target, meaning a link naming a crate rather than a file,
      // would need addContextDependency, which registers a whole watched tree.
      // scripts/transclude.sh reads both sides off disk and covers it.
      this.addDependency(path.join(repoRoot, rel));
    }
  }
  return source;
};
