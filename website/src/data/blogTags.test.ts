// The blog's tags are declared once, in `blog/tags.yml`. These tests read the
// posts and the config as text because the config loads through the bundler's
// resolver, which `node --test` does not have.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {test} from 'node:test';
import {fileURLToPath} from 'node:url';

const SITE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const BLOG = path.join(SITE, 'blog');

/** The top-level keys of `tags.yml`, each with the fields it declares. */
function declaredTags(): Map<string, Set<string>> {
  const tags = new Map<string, Set<string>>();
  let current: Set<string> | undefined;
  for (const line of fs.readFileSync(path.join(BLOG, 'tags.yml'), 'utf8').split('\n')) {
    const key = /^([a-z][a-z0-9-]*):\s*$/.exec(line);
    const field = /^ {2}([a-z]+):\s*\S/.exec(line);
    if (key) {
      current = new Set();
      tags.set(key[1], current);
    } else if (field && current) {
      current.add(field[1]);
    }
  }
  return tags;
}

/** Each post's file name and the tags its front matter lists. */
function postTags(): Array<[string, string[]]> {
  return fs
    .readdirSync(BLOG)
    .filter((f) => /^\d{4}-\d{2}-\d{2}-.*\.mdx?$/.test(f))
    .map((f) => {
      const front = /^---\n([\s\S]*?)\n---\n/.exec(fs.readFileSync(path.join(BLOG, f), 'utf8'));
      assert.ok(front, `${f} has no front matter`);
      const list = /^tags:\s*\[([^\]]*)\]\s*$/m.exec(front[1]);
      return [f, list ? list[1].split(',').map((t) => t.trim()).filter(Boolean) : []];
    });
}

test('every tag declares a label, a permalink and a description', () => {
  const tags = declaredTags();
  assert.deepEqual([...tags.keys()].sort(), ['engineering', 'release']);
  for (const [tag, fields] of tags) {
    assert.deepEqual([...fields].sort(), ['description', 'label', 'permalink'], tag);
  }
});

test('every tag a post lists is declared in tags.yml', () => {
  const tags = declaredTags();
  for (const [post, listed] of postTags()) {
    for (const tag of listed) assert.ok(tags.has(tag), `${post} lists ${tag}, which tags.yml does not declare`);
  }
});

/** A tag missing from `tags.yml` stops the build, so the list above cannot drift from the posts. */
test('the blog options refuse a tag that tags.yml does not declare', () => {
  const config = fs.readFileSync(path.join(SITE, 'docusaurus.config.ts'), 'utf8');
  const blog = /\n {8}blog: \{\n([\s\S]*?)\n {8}\},\n/.exec(config);
  assert.ok(blog, 'docusaurus.config.ts has no blog options block');
  assert.deepEqual([...blog[1].matchAll(/onInlineTags: '(\w+)'/g)].map((m) => m[1]), ['throw']);
});
