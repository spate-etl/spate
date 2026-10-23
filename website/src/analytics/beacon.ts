/** The Cloudflare Web Analytics beacon that counts page views on the deployed site. */
export const BEACON_SRC = 'https://static.cloudflareinsights.com/beacon.min.js';

/**
 * The Web Analytics site token for the deployed hostname. Every page that
 * loads the beacon carries it, and it grants no access to the account.
 */
export const BEACON_TOKEN = 'cbfa8f66e4b74badabc75489bb683040';

/** What decides whether a page view loads the beacon. */
export type Visit = {
  /** The hostname the page was served from. */
  pageHostname: string;
  /** The hostname of the site's configured `url`. */
  siteHostname: string;
  /** `navigator.globalPrivacyControl`, which only some browsers define. */
  globalPrivacyControl?: boolean;
  /** `navigator.doNotTrack`: `'1'` when the visitor asked not to be tracked. */
  doNotTrack?: string | null;
};

/**
 * Whether this page view loads the beacon: only on the deployed hostname, so
 * a local or preview build is never counted, and never for a browser that
 * sends Global Privacy Control or Do Not Track, which the privacy page names
 * as the way to opt out.
 */
export function shouldLoadBeacon(visit: Visit): boolean {
  return (
    visit.pageHostname === visit.siteHostname &&
    visit.globalPrivacyControl !== true &&
    visit.doNotTrack !== '1'
  );
}
