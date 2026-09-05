// The site must not know any system's name.
//
// `entrants/spate/entrant.toml` claims: "nothing in the site branches on the
// literal id `spate` — a CI lint enforces that, so the neutrality claim is
// checkable rather than asserted." The claim was true and the lint did not
// exist, which for a vendor-run benchmark is the wrong way round: the whole
// point of putting `vendor = "self"` in a descriptor rather than hardcoding the
// disclosure is that a reader can verify the site treats every entrant the same
// way. An unenforced neutrality claim is one refactor from being false.
//
// So this checks the general property rather than the one word. Every rendering
// decision — the vendor marker, the ordering, the accent colour, headline
// eligibility — must be driven by a descriptor field. If the site ever needs to
// know that a particular system is special, that knowledge belongs in that
// system's descriptor, where a reader can see it.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {test} = require('node:test');

const TOML = require('smol-toml');

const SITE = path.resolve(__dirname, '..');
const REPO = path.join(SITE, 'benchmark');

/**
 * Source the reader is asking about: what renders benchmark data. The rest of
 * the site names its own crate, which is the same word as the vendor's
 * entrant id, and is not a rendering decision about any system.
 */
const ROOTS = [
  path.join(SITE, 'src', 'components', 'Results'),
  path.join(SITE, 'src', 'components', 'home'),
  path.join(SITE, 'src', 'plugins', 'benchData'),
].filter((r) => fs.existsSync(r));
const EXTENSIONS = new Set(['.ts', '.tsx', '.js', '.jsx', '.mjs', '.css']);

function sourceFiles(dir, out = []) {
  for (const e of fs.readdirSync(dir, {withFileTypes: true})) {
    const p = path.join(dir, e.name);
    // Fixtures are synthetic descriptors and tests name ids on purpose; neither
    // renders anything.
    if (e.isDirectory()) {
      if (e.name !== '__fixtures__') sourceFiles(p, out);
    } else if (EXTENSIONS.has(path.extname(e.name)) && !e.name.endsWith('.test.js')) {
      out.push(p);
    }
  }
  return out;
}

/**
 * Strips comments, so prose explaining the neutrality rule does not violate it.
 *
 * Deliberately crude — it over-strips inside string literals containing `//`,
 * which can only ever cause a false PASS on a URL-like string, never a false
 * failure. An entrant id is not a URL.
 */
function stripComments(src) {
  return src
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/^\s*\/\/.*$/gm, ' ')
    .replace(/\s\/\/.*$/gm, ' ');
}

function entrantIds() {
  const dir = path.join(REPO, 'entrants');
  return fs
    .readdirSync(dir, {withFileTypes: true})
    .filter((e) => e.isDirectory())
    .map((e) => path.join(dir, e.name, 'entrant.toml'))
    .filter((p) => fs.existsSync(p))
    .map((p) => TOML.parse(fs.readFileSync(p, 'utf8')).entrant.id);
}

test('the site does not branch on any entrant id', () => {
  const ids = entrantIds();
  assert.ok(ids.length >= 2, 'need at least two entrants for this to mean anything');
  assert.ok(ids.includes('spate'), 'the vendor entrant must be among those checked');

  const offences = [];
  for (const file of ROOTS.flatMap((r) => sourceFiles(r))) {
    const src = stripComments(fs.readFileSync(file, 'utf8'));
    for (const id of ids) {
      // Quoted, because that is what a branch looks like: `entrant === 'spate'`,
      // `{spate: ...}`, `className={styles.spate}`. A bare substring would flag
      // the word inside an unrelated identifier.
      const quoted = new RegExp(`['"\`]${id.replace(/[.*+?^$()|[\]\\]/g, '\\$&')}['"\`]`);
      const line = src.split('\n').findIndex((l) => quoted.test(l));
      if (line >= 0) {
        offences.push(`${path.relative(SITE, file)}:${line + 1} names the entrant "${id}"`);
      }
    }
  }

  assert.deepEqual(
    offences,
    [],
    `The site must render every system from its descriptor, never from its name.\n` +
      `If one system genuinely needs different treatment, add a field to its\n` +
      `entrant.toml and branch on that, so a reader can see the rule.\n\n` +
      offences.join('\n'),
  );
});

test('the vendor disclosure is driven by a descriptor field that some entrant sets', () => {
  // The other half of the same claim: neutrality is worthless if the marker it
  // replaced never renders. Exactly one entrant should declare itself the vendor.
  const dir = path.join(REPO, 'entrants');
  const vendors = fs
    .readdirSync(dir, {withFileTypes: true})
    .filter((e) => e.isDirectory())
    .map((e) => path.join(dir, e.name, 'entrant.toml'))
    .filter((p) => fs.existsSync(p))
    .map((p) => TOML.parse(fs.readFileSync(p, 'utf8')).entrant);

  const own = vendors.filter((v) => v.vendor === 'self');
  assert.equal(own.length, 1, 'exactly one entrant is run by the vendor of this benchmark');

  const component = fs.readFileSync(
    path.join(SITE, 'src/components/Results/index.tsx'),
    'utf8',
  );
  assert.match(
    component,
    /vendor === 'self'/,
    'the disclosure must be rendered from the descriptor field',
  );
});
