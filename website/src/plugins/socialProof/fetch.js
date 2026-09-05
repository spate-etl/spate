// Fetches the figures the site shows as social proof: GitHub stars and
// release count, crates.io downloads and the newest version.

/** One JSON request with a deadline. Rejects on any non-2xx status. */
async function getJson(url, headers, timeoutMs, fetchImpl) {
  const control = new AbortController();
  const timer = setTimeout(() => control.abort(), timeoutMs);
  try {
    const res = await fetchImpl(url, {headers, signal: control.signal});
    if (!res.ok) throw new Error(`${url}: ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

/**
 * The live figures, or a rejection when any source fails or times out.
 *
 * `GH_TOKEN` raises the GitHub rate limit from the per-address default to
 * the per-repository one; crates.io asks callers to identify themselves,
 * which the user agent does.
 */
async function fetchProof({
  repo = 'spate-etl/spate',
  crate = 'spate',
  timeoutMs = 5000,
  fetchImpl = globalThis.fetch,
  token = process.env.GH_TOKEN,
} = {}) {
  const agent = 'spate-site-build (+https://spate.kainth.dev)';
  const github = {
    'User-Agent': agent,
    Accept: 'application/vnd.github+json',
    ...(token ? {Authorization: `Bearer ${token}`} : {}),
  };
  const [repoJson, releases, crateJson] = await Promise.all([
    getJson(`https://api.github.com/repos/${repo}`, github, timeoutMs, fetchImpl),
    getJson(`https://api.github.com/repos/${repo}/releases?per_page=100`, github, timeoutMs, fetchImpl),
    getJson(`https://crates.io/api/v1/crates/${crate}`, {'User-Agent': agent}, timeoutMs, fetchImpl),
  ]);
  return {
    stars: repoJson.stargazers_count,
    releases: releases.length,
    downloads: crateJson.crate.downloads,
    version: crateJson.crate.max_stable_version ?? crateJson.crate.max_version,
    asOf: new Date().toISOString().slice(0, 10),
  };
}

module.exports = {fetchProof, getJson};
