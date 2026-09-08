// Fetches the figures the site shows as social proof: the crates.io download
// count and the newest published version.

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
 * The live figures, or a rejection when the source fails or times out.
 *
 * crates.io asks callers to identify themselves, which the user agent does.
 */
async function fetchProof({crate = 'spate', timeoutMs = 5000, fetchImpl = globalThis.fetch} = {}) {
  const agent = 'spate-site-build (+https://spate.kainth.dev)';
  const crateJson = await getJson(
    `https://crates.io/api/v1/crates/${crate}`,
    {'User-Agent': agent},
    timeoutMs,
    fetchImpl,
  );
  return {
    downloads: crateJson.crate.downloads,
    version: crateJson.crate.max_stable_version ?? crateJson.crate.max_version,
    asOf: new Date().toISOString().slice(0, 10),
  };
}

module.exports = {fetchProof, getJson};
