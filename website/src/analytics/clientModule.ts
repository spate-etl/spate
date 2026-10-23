import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';
import siteConfig from '@generated/docusaurus.config';

import {BEACON_SRC, BEACON_TOKEN, shouldLoadBeacon} from './beacon';

// The opt-out is read in the visitor's browser before the script loads, so
// the beacon is added here and the built HTML carries none. It loads once per
// page load and reports client-side route changes itself.
if (ExecutionEnvironment.canUseDOM) {
  const nav = navigator as Navigator & {globalPrivacyControl?: boolean};
  const load = shouldLoadBeacon({
    pageHostname: window.location.hostname,
    siteHostname: new URL(siteConfig.url).hostname,
    globalPrivacyControl: nav.globalPrivacyControl,
    doNotTrack: nav.doNotTrack,
  });
  if (load) {
    const script = document.createElement('script');
    script.defer = true;
    script.src = BEACON_SRC;
    script.dataset.cfBeacon = JSON.stringify({token: BEACON_TOKEN});
    document.head.appendChild(script);
  }
}
