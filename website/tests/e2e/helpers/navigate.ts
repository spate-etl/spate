import {expect, type Page} from '@playwright/test';

import type {ColorMode} from '../fixtures';
import {ROUTES, type RouteName} from './routes';

/**
 * Navigates to a named route and waits until the page is a reliable target
 * for an accessibility sweep. Checks the response was not silently
 * redirected to Docusaurus's (fully accessible) 404 page, that the requested
 * colour mode actually landed on `<html>` rather than whatever the storage
 * key defaulted to, and that web fonts have finished loading.
 */
export async function gotoRoute(page: Page, route: RouteName, colorMode: ColorMode): Promise<void> {
  const response = await page.goto(ROUTES[route]);
  expect(response?.status(), `GET ${ROUTES[route]}`).toBe(200);

  await expect(page.locator('html')).toHaveAttribute('data-theme', colorMode);
  await page.evaluate(() => document.fonts.ready);
}
