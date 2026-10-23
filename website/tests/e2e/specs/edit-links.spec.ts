import {existsSync, readdirSync, readFileSync} from 'node:fs';
import path from 'node:path';

import {expect, test} from '../fixtures';
import {ROUTES} from '../helpers/routes';
import {githubUrl, SOURCE_REF} from '../../../src/repoUrl';

const REPO_ROOT = path.resolve(__dirname, '../../../..');
const EDIT_BASE = `${githubUrl}/edit/${SOURCE_REF}/`;

/**
 * The post files a production build publishes: the blog preset's `exclude`
 * list applied, and a `draft: true` post left out.
 */
function postFiles(): string[] {
  const dir = path.join(REPO_ROOT, 'website/blog');
  return readdirSync(dir).filter((name) => {
    if (!/\.mdx?$/.test(name) || name === 'README.md' || name.startsWith('_')) return false;
    const frontMatter = /^---\n([\s\S]*?)\n---/.exec(readFileSync(path.join(dir, name), 'utf8'))?.[1] ?? '';
    return !/^draft:\s*true\s*$/m.test(frontMatter);
  });
}

/**
 * Pins that every blog post's "Edit this page" link opens a file in this
 * repository. Regression for #1341.
 */
test("every blog post's edit link names its source file", async ({page}) => {
  const posts: string[] = [];
  let list: string | null = ROUTES.blog;
  while (list !== null) {
    const response = await page.goto(list);
    expect(response?.status(), `GET ${list}`).toBe(200);
    const links = page.locator('article header h2 a');
    await expect(links.first()).toBeVisible();
    for (const href of await links.evaluateAll((els) => els.map((el) => el.getAttribute('href')))) {
      if (href !== null) posts.push(href);
    }
    const next = page.locator('a.pagination-nav__link--next');
    list = (await next.count()) > 0 ? await next.getAttribute('href') : null;
  }
  expect(posts.length, 'posts listed against post files on disk').toBe(postFiles().length);

  for (const post of posts) {
    const response = await page.goto(post);
    expect(response?.status(), `GET ${post}`).toBe(200);
    const href = (await page.locator('a.theme-edit-this-page').getAttribute('href')) ?? '';
    expect(href.startsWith(EDIT_BASE), `edit link ${href} on ${post} starts with ${EDIT_BASE}`).toBe(true);
    const source = href.slice(EDIT_BASE.length);
    expect(existsSync(path.join(REPO_ROOT, source)), `${source} exists, from ${post}`).toBe(true);
  }
});
