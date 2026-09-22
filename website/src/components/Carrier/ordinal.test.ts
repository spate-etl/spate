// `./ordinal.ts` is imported with its extension because `node --test` strips
// types but does not resolve the bundler's extensionless imports.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {test} from 'node:test';
import {fileURLToPath} from 'node:url';

import {numeral, pageOrdinal, sectionOrdinal} from './ordinal.ts';

const WEBSITE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..', '..');

/** Only a user-guide page with a number prefix has a place; a decision record's prefix is not one. */
test('a page ordinal comes from a user-guide number prefix', () => {
  assert.equal(pageOrdinal({id: 'user-guide/concepts/delivery-guarantees', sidebarPosition: 2}), 2);
  assert.equal(pageOrdinal({id: 'user-guide/guides/graceful-shutdown'}), null);
  assert.equal(pageOrdinal({id: 'adr/at-least-once-delivery', sidebarPosition: 2}), null);
  assert.equal(pageOrdinal({id: 'workload'}), null);
});

const TOC = [
  {id: 'the-model', level: 2},
  {id: 'splits', level: 3},
  {id: 'contract', level: 2},
  {id: 'revocation', level: 2},
];

/** A section's place counts the page's H2s only, so it matches its table-of-contents position. */
test('a section ordinal is the place among H2s', () => {
  assert.equal(sectionOrdinal(TOC, 'the-model', false), 1);
  assert.equal(sectionOrdinal(TOC, 'revocation', false), 3);
  assert.equal(sectionOrdinal(TOC, 'contract', true), 2);
});

/** A stale id, an H3, or a first section under a header carrier fails the build instead of rendering. */
test('a section carrier the rules do not allow throws', () => {
  assert.throws(() => sectionOrdinal(TOC, 'renamed', false), /no H2 with id "renamed"/);
  assert.throws(() => sectionOrdinal(TOC, 'splits', false), /no H2 with id "splits"/);
  assert.throws(() => sectionOrdinal(TOC, 'the-model', true), /first section/);
});

/** A numeral names an image, so one short of two digits would miss it. */
test('a numeral has two digits', () => {
  assert.equal(numeral(3), '03');
  assert.equal(numeral(12), '12');
});

/** The brand generator writes the images; a prefix it does not cover would render a broken image. */
test('every user-guide number prefix has a carrier on both grounds', () => {
  const docs = path.join(WEBSITE, '..', 'docs', 'user-guide');
  const prefixed = fs
    .readdirSync(docs, {recursive: true, encoding: 'utf8'})
    .flatMap((file) => {
      const m = /(?:^|\/)(\d+)-[^/]+\.mdx?$/.exec(file);
      return m ? [{file, prefix: m[1]}] : [];
    });
  assert.ok(prefixed.length > 0, 'no prefixed page found under docs/user-guide');
  for (const {file, prefix} of prefixed) {
    for (const suffix of ['', '-dark']) {
      const image = path.join(WEBSITE, 'static', 'img', 'brand', `carrier-${numeral(Number(prefix))}${suffix}.svg`);
      assert.ok(fs.existsSync(image), `${file} has no ${path.basename(image)}`);
    }
  }
});
