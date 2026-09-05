// Publishes the social-proof figures as global data.
//
// A build fetches them live and falls back to the committed
// src/data/social-proof.json when the network or either API is unavailable,
// so an offline build still succeeds and the page always carries the date the
// figures are from. `SPATE_SITE_OFFLINE=1` skips the fetch outright;
// `npm run refresh-proof` rewrites the committed file.

const fs = require('node:fs');
const path = require('node:path');

const {fetchProof} = require('./fetch.js');

const PLUGIN = 'social-proof';

module.exports = function socialProof(context, options = {}) {
  const fallbackPath = path.resolve(context.siteDir, 'src', 'data', 'social-proof.json');
  return {
    name: PLUGIN,

    async loadContent() {
      const readFallback = () => ({...JSON.parse(fs.readFileSync(fallbackPath, 'utf8')), source: 'fallback'});
      if (process.env.SPATE_SITE_OFFLINE === '1') return readFallback();
      try {
        return {...(await fetchProof(options)), source: 'live'};
      } catch (e) {
        console.warn(`[${PLUGIN}] using the committed figures: ${e.message}`);
        return readFallback();
      }
    },

    async contentLoaded({content, actions}) {
      actions.setGlobalData(content);
    },
  };
};
