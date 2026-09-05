import fs from 'node:fs';
import path from 'node:path';

/** How many invariants docs/INVARIANTS.md defines, counted from the list itself. */
export function countInvariants(): number {
  const file = path.resolve(__dirname, '..', '..', '..', 'docs', 'INVARIANTS.md');
  const text = fs.readFileSync(file, 'utf8');
  return new Set([...text.matchAll(/^- \*\*INV-(\d+)/gm)].map((m) => m[1])).size;
}
