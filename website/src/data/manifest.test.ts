// `./manifest.ts` is imported with its extension because `node --test` strips
// types but does not resolve the bundler's extensionless imports. The parser is
// the entry point under test because `readToolchain` reaches the file through
// `__dirname`, which an ES module scope does not have.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {test} from 'node:test';
import {fileURLToPath} from 'node:url';

import {parseToolchain} from './manifest.ts';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const MANIFEST = fs.readFileSync(path.resolve(HERE, '..', '..', '..', 'Cargo.toml'), 'utf8');

/** The site renders both values as prose, so a pattern the manifest stops matching must not yield a string. */
test('the workspace manifest yields an X.Y MSRV and a four-digit edition', () => {
  const {msrv, edition} = parseToolchain(MANIFEST);
  assert.match(msrv, /^\d+\.\d+$/);
  assert.match(edition, /^\d{4}$/);
});

/** `.github/actions/setup-rust` reads the MSRV with the same anchored pattern and fails the same way. */
test('a key declared other than once is rejected', () => {
  assert.throws(() => parseToolchain(`${MANIFEST}rust-version = "1.99"\n`), /rust-version 2 times/);
  assert.throws(() => parseToolchain('edition = "2024"\n'), /rust-version 0 times/);
  assert.throws(() => parseToolchain('rust-version = "1.96"\n'), /edition 0 times/);
});
