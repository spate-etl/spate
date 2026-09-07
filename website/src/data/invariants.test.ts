// `./invariants.ts` is imported with its extension because `node --test` strips
// types but does not resolve the bundler's extensionless imports. The parser is
// the entry point under test because `countInvariants` reaches the file through
// `__dirname`, which an ES module scope does not have.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {test} from 'node:test';
import {fileURLToPath} from 'node:url';

import {parseInvariants} from './invariants.ts';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SOURCE = fs.readFileSync(path.resolve(HERE, '..', '..', '..', 'docs', 'INVARIANTS.md'), 'utf8');

const STATED = new Set([...SOURCE.matchAll(/\*\*INV-(\d+)\b/g)].map((m) => m[1]));

/** Markup the counter's regex stops matching yields zero, so this reads the same file a second way. */
test('every invariant the list states is counted', () => {
  assert.ok(STATED.size > 0);
  assert.equal(parseInvariants(SOURCE), STATED.size);
});

/** The homepage deep-links each stage's chip to `#inv-N`, which resolves only if the list carries it. */
test('every invariant the list states carries an anchor', () => {
  const anchored = new Set([...SOURCE.matchAll(/<Anchor id="inv-(\d+)" \/>/g)].map((m) => m[1]));
  assert.deepEqual([...anchored].sort(), [...STATED].sort());
});
