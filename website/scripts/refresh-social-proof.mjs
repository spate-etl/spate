// Rewrites src/data/social-proof.json from the live sources. Run it when the
// committed figures are stale; a build that can reach the network fetches
// live figures itself.
import {writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const require = createRequire(import.meta.url);
const {fetchProof} = require('../src/plugins/socialProof/fetch.js');

const here = dirname(fileURLToPath(import.meta.url));
const out = join(here, '..', 'src', 'data', 'social-proof.json');
const proof = await fetchProof();
writeFileSync(out, `${JSON.stringify(proof, null, 2)}\n`);
process.stdout.write(`wrote ${out}: ${JSON.stringify(proof)}\n`);
