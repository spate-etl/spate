import fs from 'node:fs';
import path from 'node:path';

/**
 * The value a workspace manifest key declares. Member crates inherit it, so the
 * workspace manifest holds the only declaration.
 */
function declaration(text: string, key: string): string {
  const found = [...text.matchAll(new RegExp(`^${key} = "(.*)"$`, 'gm'))];
  if (found.length !== 1) {
    throw new Error(`the workspace manifest declares ${key} ${found.length} times, expected 1`);
  }
  return found[0][1];
}

/** The MSRV and edition a workspace manifest declares. */
export function parseToolchain(text: string): {msrv: string; edition: string} {
  return {msrv: declaration(text, 'rust-version'), edition: declaration(text, 'edition')};
}

/** The MSRV and edition Cargo.toml declares. */
export function readToolchain(): {msrv: string; edition: string} {
  const file = path.resolve(__dirname, '..', '..', '..', 'Cargo.toml');
  return parseToolchain(fs.readFileSync(file, 'utf8'));
}
