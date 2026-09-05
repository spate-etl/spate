const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {test} = require('node:test');

const FALLBACK = {stars: 1, releases: 1, downloads: 10, version: '0.0.1', asOf: '2026-01-01'};

/** A site directory holding only the committed figures. */
function siteDir() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'social-proof-'));
  fs.mkdirSync(path.join(dir, 'src', 'data'), {recursive: true});
  fs.writeFileSync(path.join(dir, 'src', 'data', 'social-proof.json'), JSON.stringify(FALLBACK));
  return dir;
}

const json = (body) => ({ok: true, json: async () => body});

test('the live figures are published when every source answers', async () => {
  const fetchImpl = async (url) => {
    if (url.endsWith('/releases?per_page=100')) return json([{}, {}]);
    if (url.includes('api.github.com')) return json({stargazers_count: 42});
    return json({crate: {downloads: 500, max_stable_version: '0.2.0'}});
  };
  const plugin = require('./index.js')({siteDir: siteDir()}, {fetchImpl, token: null});
  delete process.env.SPATE_SITE_OFFLINE;
  const content = await plugin.loadContent();
  assert.equal(content.source, 'live');
  assert.equal(content.stars, 42);
  assert.equal(content.releases, 2);
  assert.equal(content.downloads, 500);
  assert.equal(content.version, '0.2.0');
  assert.match(content.asOf, /^\d{4}-\d{2}-\d{2}$/);
});

test('a failing source falls back to the committed figures', async () => {
  const fetchImpl = async () => ({ok: false, status: 403, json: async () => ({})});
  const plugin = require('./index.js')({siteDir: siteDir()}, {fetchImpl, token: null});
  delete process.env.SPATE_SITE_OFFLINE;
  const content = await plugin.loadContent();
  assert.equal(content.source, 'fallback');
  assert.equal(content.stars, FALLBACK.stars);
  assert.equal(content.asOf, FALLBACK.asOf);
});

test('SPATE_SITE_OFFLINE skips the fetch', async () => {
  let called = false;
  const fetchImpl = async () => {
    called = true;
    return json({});
  };
  const plugin = require('./index.js')({siteDir: siteDir()}, {fetchImpl, token: null});
  process.env.SPATE_SITE_OFFLINE = '1';
  try {
    const content = await plugin.loadContent();
    assert.equal(content.source, 'fallback');
    assert.equal(called, false);
  } finally {
    delete process.env.SPATE_SITE_OFFLINE;
  }
});
