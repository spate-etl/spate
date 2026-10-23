import type {Page} from '@playwright/test';

import {BEACON_SRC, BEACON_TOKEN} from '../../../src/analytics/beacon';
import {expect, test} from '../fixtures';
import {ROUTES} from '../helpers/routes';

/** The deployed origin, which the page-view beacon loads on and nowhere else. */
const SITE = 'https://spate.kainth.dev';

/**
 * Serves the deployed origin from the local build, so a page reads the
 * deployed hostname without leaving the machine, and answers every request to
 * the analytics host with an empty script, so a run never sends a page view.
 * Returns the beacon URLs the page requested.
 */
async function serveAsDeployed(page: Page, baseURL: string): Promise<string[]> {
  const beacons: string[] = [];
  await page.route('https://static.cloudflareinsights.com/**', (route) => {
    beacons.push(route.request().url());
    return route.fulfill({contentType: 'text/javascript', body: ''});
  });
  await page.route('https://cloudflareinsights.com/**', (route) => route.abort());
  await page.route(`${SITE}/**`, async (route) => {
    const local = new URL(route.request().url());
    const response = await route.fetch({url: new URL(local.pathname + local.search, baseURL).toString()});
    return route.fulfill({response});
  });
  return beacons;
}

/**
 * Waits for hydration. Client modules run before it, so an absent beacon after
 * this point means the module chose not to load it.
 */
async function waitForHydration(page: Page): Promise<void> {
  await expect(page.locator('html[data-has-hydrated="true"]')).toHaveCount(1);
}

/** Opens the homepage on the deployed origin and waits for hydration. */
async function openHome(page: Page): Promise<void> {
  const response = await page.goto(`${SITE}${ROUTES.home}`);
  expect(response?.ok(), `GET ${SITE}${ROUTES.home}`).toBe(true);
  await waitForHydration(page);
}

test.describe('page-view beacon', () => {
  // A test can end while the page still fetches an asset through a route
  // above; releasing the routes this way drops that late fulfill.
  test.afterEach(async ({page}) => {
    await page.unrouteAll({behavior: 'ignoreErrors'});
  });

  test('loads once on the deployed hostname, carrying the site token', async ({page, baseURL}) => {
    const beacons = await serveAsDeployed(page, baseURL!);
    await openHome(page);

    const script = page.locator(`script[src="${BEACON_SRC}"]`);
    await expect(script).toHaveCount(1);
    await expect(script).toHaveAttribute('data-cf-beacon', JSON.stringify({token: BEACON_TOKEN}));
    expect(beacons).toEqual([BEACON_SRC]);
  });

  test('is never in the built HTML', async ({page, baseURL}) => {
    await serveAsDeployed(page, baseURL!);
    const response = await page.request.get(`${baseURL}${ROUTES.home}`);
    expect(await response.text()).not.toContain('cloudflareinsights');
  });

  test('does not load on a local hostname', async ({page}) => {
    const beacons: string[] = [];
    await page.route('https://static.cloudflareinsights.com/**', (route) => {
      beacons.push(route.request().url());
      return route.fulfill({contentType: 'text/javascript', body: ''});
    });
    await page.goto(ROUTES.home);
    await waitForHydration(page);

    await expect(page.locator(`script[src="${BEACON_SRC}"]`)).toHaveCount(0);
    expect(beacons).toEqual([]);
  });

  test('does not load when the browser sends Global Privacy Control', async ({page, baseURL}) => {
    await page.addInitScript(() => {
      Object.defineProperty(Navigator.prototype, 'globalPrivacyControl', {get: () => true});
    });
    const beacons = await serveAsDeployed(page, baseURL!);
    await openHome(page);

    await expect(page.locator(`script[src="${BEACON_SRC}"]`)).toHaveCount(0);
    expect(beacons).toEqual([]);
  });

  test('does not load when the browser sends Do Not Track', async ({page, baseURL}) => {
    await page.addInitScript(() => {
      Object.defineProperty(Navigator.prototype, 'doNotTrack', {get: () => '1'});
    });
    const beacons = await serveAsDeployed(page, baseURL!);
    await openHome(page);

    await expect(page.locator(`script[src="${BEACON_SRC}"]`)).toHaveCount(0);
    expect(beacons).toEqual([]);
  });
});
