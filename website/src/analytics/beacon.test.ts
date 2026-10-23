import assert from 'node:assert/strict';
import {test} from 'node:test';

import {shouldLoadBeacon, type Visit} from './beacon.ts';

const SITE = 'spate.kainth.dev';
const visit = (over: Partial<Visit> = {}): Visit => ({pageHostname: SITE, siteHostname: SITE, ...over});

test('a page view on the deployed hostname loads the beacon', () => {
  assert.equal(shouldLoadBeacon(visit()), true);
  assert.equal(shouldLoadBeacon(visit({globalPrivacyControl: false, doNotTrack: '0'})), true);
  assert.equal(shouldLoadBeacon(visit({doNotTrack: null})), true);
});

test('a local or preview hostname never loads it', () => {
  assert.equal(shouldLoadBeacon(visit({pageHostname: '127.0.0.1'})), false);
  assert.equal(shouldLoadBeacon(visit({pageHostname: 'localhost'})), false);
  assert.equal(shouldLoadBeacon(visit({pageHostname: `preview.${SITE}`})), false);
});

test('Global Privacy Control opts the visitor out', () => {
  assert.equal(shouldLoadBeacon(visit({globalPrivacyControl: true})), false);
});

test('Do Not Track opts the visitor out', () => {
  assert.equal(shouldLoadBeacon(visit({doNotTrack: '1'})), false);
});
