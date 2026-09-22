import {expect, type Page} from '@playwright/test';

import type {ColorMode} from '../fixtures';
import {ROUTES, type RouteName} from './routes';

const OTHER: Record<ColorMode, ColorMode> = {light: 'dark', dark: 'light'};

/**
 * The `--spate-ink` a colour mode declares, read off a probe element so the
 * value comes from the live stylesheet. Fails unless the token is `#rrggbb`,
 * the form `countColor` parses.
 */
async function inkFor(page: Page, mode: ColorMode): Promise<string> {
  const hex = await page.evaluate((m) => {
    const probe = document.createElement('div');
    probe.setAttribute('data-theme', m);
    probe.style.display = 'none';
    document.body.append(probe);
    const value = getComputedStyle(probe).getPropertyValue('--spate-ink').trim();
    probe.remove();
    return value;
  }, mode);
  expect(hex, `--spate-ink for ${mode}`).toMatch(/^#[0-9a-f]{6}$/i);
  return hex;
}

/**
 * Counts elements whose computed `color` is `hex`, reading every element so
 * the read forces the resolution it measures. An element inside a subtree that
 * pins its own `data-theme`, such as a swatch on the brand page, takes that
 * theme's ink by design and is not counted.
 */
function countColor(page: Page, hex: string): Promise<number> {
  return page.evaluate((h) => {
    const rgb = `rgb(${[1, 3, 5].map((i) => Number.parseInt(h.slice(i, i + 2), 16)).join(', ')})`;
    return Array.from(document.querySelectorAll('*')).filter(
      (el) => getComputedStyle(el).color === rgb && el.closest('body [data-theme]') === null,
    ).length;
  }, hex);
}

/**
 * Navigates to a named route and waits until the page is a reliable target
 * for an accessibility sweep. Checks the response was not silently
 * redirected to Docusaurus's (fully accessible) 404 page, then waits on
 * `settleColorMode`.
 */
export async function gotoRoute(page: Page, route: RouteName, colorMode: ColorMode): Promise<void> {
  const response = await page.goto(ROUTES[route]);
  expect(response?.status(), `GET ${ROUTES[route]}`).toBe(200);
  await settleColorMode(page, colorMode);
}

/**
 * Waits until the requested colour mode actually landed on `<html>` rather
 * than whatever the storage key defaulted to, web fonts have finished
 * loading, and no element computes the other colour mode's ink.
 *
 * Chromium can serve a `color` resolved before `data-theme` reached `<html>`
 * and repairs one DOM level per lifecycle update, so reading every element is
 * what drives the repair the poll measures. To see the state by hand, throttle
 * the CPU through CDP (`Emulation.setCPUThrottlingRate`) and run at
 * `--workers=1`; rate 15 is where it reproduces. The band of rates that
 * reproduce it moves with the speed of the host and is unstable at its edges,
 * so a green run with this poll removed means nothing until the rate has been
 * swept.
 */
export async function settleColorMode(page: Page, colorMode: ColorMode): Promise<void> {
  await expect(page.locator('html')).toHaveAttribute('data-theme', colorMode);
  await page.evaluate(() => document.fonts.ready);

  const stale = await inkFor(page, OTHER[colorMode]);
  // Two modes on one ink would make the poll pass against any state at all.
  expect(stale, 'both colour modes declare the same --spate-ink').not.toBe(await inkFor(page, colorMode));
  await expect
    .poll(() => countColor(page, stale), {
      message: 'elements still computing the other colour mode ink',
      timeout: 15_000,
      intervals: [50, 100, 200, 400],
    })
    .toBe(0);
}
