import fs from 'node:fs';
import path from 'node:path';

/** How many invariants an INVARIANTS.md body defines, counted from the list markup. */
export function parseInvariants(text: string): number {
  return new Set([...text.matchAll(/^- .*?\*\*INV-(\d+) —/gm)].map((m) => m[1])).size;
}

/** How many invariants docs/INVARIANTS.md defines. */
export function countInvariants(): number {
  const file = path.resolve(__dirname, '..', '..', '..', 'docs', 'INVARIANTS.md');
  return parseInvariants(fs.readFileSync(file, 'utf8'));
}
