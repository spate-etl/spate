import type {Locator, Page} from '@playwright/test';

import {expect, test} from '../fixtures';
import {gotoRoute} from '../helpers/navigate';
import {VIEWPORTS} from '../helpers/routes';

test.use({colorMode: 'light', viewport: VIEWPORTS.desktop});

/**
 * Presses Tab until `target` is the active element, so the check exercises
 * real keyboard navigation rather than a direct `.focus()` call. The Kafka
 * source page's navbar, sidebar and in-page links put roughly 80 focusable
 * elements before the Metrics table; the bound leaves headroom for the page
 * growing without letting a broken tab order spin until the test timeout.
 */
async function tabTo(page: Page, target: Locator, maxPresses = 200): Promise<void> {
  for (let i = 0; i < maxPresses; i++) {
    await page.keyboard.press('Tab');
    if (await target.evaluate((el) => el === document.activeElement)) return;
  }
  throw new Error(`did not reach the target element within ${maxPresses} Tab presses`);
}

// Anchored on the Kafka source page's Metrics table, not docs/METRICS.md:
// axe's scrollable-region-focusable passes a scroll container that already
// has a focusable descendant, and 8 of METRICS.md's 99 rows hold a link.
// This table's cells hold only `<code>`, so a link added to a cell here
// would neuter the sweep the way it does not on METRICS.md.
test.describe('overflowing Metrics table (#396)', () => {
  test('the wrapper overflows the docs column at this viewport', async ({page, colorMode}) => {
    await gotoRoute(page, 'kafkaSource', colorMode);
    const wrapper = page.locator('h2#metrics ~ [data-table-overflow]');
    await expect(wrapper).toHaveCount(1);
    // A hard assertion, not a skip: the rest of this file only means
    // something if the table genuinely overflows here.
    await expect
      .poll(async () => wrapper.evaluate((el) => el.scrollWidth - el.clientWidth))
      .toBeGreaterThan(1);
  });

  test('the wrapper is a focusable, named region', async ({page, colorMode}) => {
    await gotoRoute(page, 'kafkaSource', colorMode);
    const wrapper = page.locator('h2#metrics ~ [data-table-overflow="true"]');
    await expect(wrapper).toHaveAttribute('tabindex', '0');
    await expect(wrapper).toHaveAttribute('role', 'region');
    // An exact match, not a pattern: a regression to `aria-labelledby`
    // pointing at the heading computes "MetricsDirect link to Metrics" by
    // concatenating the heading's text with its hash-link's own aria-label,
    // and a substring match like /Metrics/i would still accept that string.
    await expect(wrapper).toHaveAccessibleName('Metrics');
  });

  test('the table itself is not the scroll container', async ({page, colorMode}) => {
    await gotoRoute(page, 'kafkaSource', colorMode);
    const table = page.locator('h2#metrics ~ [data-table-overflow] table');
    await expect(table).toHaveCSS('display', 'table');
    await expect
      .poll(async () => table.evaluate((el) => el.scrollWidth - el.clientWidth))
      .toBeLessThanOrEqual(1);
  });

  // WebKit does not scroll a focused overflow container from the arrow keys;
  // chromium and firefox do. On a bare page holding a `tabindex` div with no
  // site code, `scrollLeft` after one ArrowRight is 40, 47 and 0. The
  // accessibility claim itself is covered on every engine by
  // `a11y.spec.ts`'s `scrollable-region-focusable` check, which passes on
  // webkit; this test covers the scrolling behaviour behind it.
  test('the wrapper scrolls when tabbed to and sent ArrowRight', async ({page, colorMode, browserName}) => {
    test.skip(browserName === 'webkit', 'WebKit does not arrow-key scroll a focused overflow container');
    await gotoRoute(page, 'kafkaSource', colorMode);
    const wrapper = page.locator('h2#metrics ~ [data-table-overflow="true"]');
    await tabTo(page, wrapper);
    await page.keyboard.press('ArrowRight');
    await expect.poll(async () => wrapper.evaluate((el) => el.scrollLeft)).toBeGreaterThan(0);
  });
});

// The negative case: a table that fits its column carries no tab stop, so a
// regression that stamps tabindex on every table (which would also pass
// axe) shows up here.
test('a table that fits its column gets no tabindex or role', async ({page, colorMode}) => {
  await gotoRoute(page, 'installation', colorMode);
  const wrapper = page.locator('[data-table-overflow="false"]');
  await expect(wrapper).toHaveCount(1);
  await expect(wrapper).not.toHaveAttribute('tabindex');
  await expect(wrapper).not.toHaveAttribute('role');
});

// Proves the ResizeObserver path rather than a one-shot measurement at
// mount: the installation page's table fits at 1000px and overflows at
// 360px, with no navigation between the two checks. Firefox compresses this
// table to 388px, so at 420px it still fits there while chromium and webkit
// overflow; 360px overflows on all three.
test('overflow state tracks a resize, wide then narrow', async ({page, colorMode}) => {
  await gotoRoute(page, 'installation', colorMode);
  const wrapper = page.locator('[data-table-overflow]');

  await page.setViewportSize({width: 1000, height: 900});
  await expect(wrapper).toHaveAttribute('data-table-overflow', 'false');

  await page.setViewportSize({width: 360, height: 900});
  await expect(wrapper).toHaveAttribute('data-table-overflow', 'true');
});
