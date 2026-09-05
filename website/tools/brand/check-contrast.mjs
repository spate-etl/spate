// Holds every text and mark color in brand.css to WCAG 2.2 AA on the ground
// it sits on: 4.5:1 for text, 3:1 for a mark or a control. Exits non-zero
// naming each pair that falls short.
import {readFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const css = readFileSync(join(here, '..', '..', 'src', 'css', 'brand.css'), 'utf8');

/** Token tables per ground, from the two blocks the generator writes. */
function tokens(block) {
  const out = {};
  for (const m of block.matchAll(/--spate-([a-z0-9-]+):\s*(#[0-9a-f]{6})\s*;/gi)) out[m[1]] = m[2];
  return out;
}
const light = tokens(css.split("[data-theme='dark']")[0]);
const dark = tokens(css.split("[data-theme='dark']")[1]);

function luminance(hex) {
  const c = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255)
    .map((v) => (v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4));
  return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2];
}
function ratio(a, b) {
  const [l1, l2] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (l1 + 0.05) / (l2 + 0.05);
}

// [foreground, background, floor]
const PAIRS = [
  ['ink', 'bg', 4.5],
  ['ink', 'surface', 4.5],
  ['ink', 'surface-2', 4.5],
  ['muted', 'bg', 4.5],
  ['muted', 'surface', 4.5],
  ['muted', 'surface-2', 4.5],
  ['accent', 'bg', 4.5],
  ['accent', 'surface', 4.5],
  ['accent-ink', 'accent', 4.5],
  ['primary-dark', 'bg', 4.5],
  ['primary-dark', 'surface-2', 4.5],
  ['danger', 'bg', 4.5],
  ['warning', 'bg', 4.5],
  ['code-ink', 'code-bg', 4.5],
  ['mark-node', 'bg', 3],
  ['mark-edge', 'bg', 3],
  ['primary', 'bg', 3],
];

let failed = 0;
for (const [ground, t] of [['light', light], ['dark', dark]]) {
  for (const [fg, bg, floor] of PAIRS) {
    if (!t[fg] || !t[bg]) {
      console.error(`${ground}: missing token ${!t[fg] ? fg : bg}`);
      failed += 1;
      continue;
    }
    const r = ratio(t[fg], t[bg]);
    const ok = r >= floor;
    if (!ok) failed += 1;
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${ground.padEnd(5)} ${fg.padEnd(11)} on ${bg.padEnd(9)} ${r.toFixed(2)}:1 (floor ${floor})`);
  }
}
if (failed) {
  console.error(`${failed} pair(s) below the floor`);
  process.exit(1);
}
