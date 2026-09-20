import type {Page} from '@playwright/test';

import {expect, test} from '../fixtures';
import {gotoRoute} from '../helpers/navigate';

const BRAND_SCALE = {
  '--ifm-color-emphasis-100': '#22262f',
  '--ifm-color-emphasis-200': '#2b303a',
  '--ifm-color-emphasis-300': '#363c48',
  '--ifm-color-emphasis-600': '#9aa1a9',
  '--ifm-color-emphasis-700': '#b6bcc4',
  '--ifm-color-emphasis-800': '#d3d7dc',
};

/**
 * Reads custom properties off `<html>`, lowercased. The trim is required
 * because a value that arrives through `var()` substitution keeps the
 * whitespace that followed the colon in the declaration it came from.
 */
async function computedTokens(page: Page, names: string[]): Promise<Record<string, string>> {
  return page.evaluate((tokens) => {
    const style = getComputedStyle(document.documentElement);
    return Object.fromEntries(tokens.map((token) => [token, style.getPropertyValue(token).trim().toLowerCase()]));
  }, names);
}

/**
 * Pins the six emphasis tokens `custom.css` declares for dark mode, the search
 * placeholder colour the `:root` mapping supplies, and the navbar search pill
 * as painted.
 *
 * Regression for #568.
 */
test.describe('dark mode emphasis scale', () => {
  test.use({colorMode: 'dark'});

  test('the emphasis tokens resolve to the brand values', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    expect(await computedTokens(page, Object.keys(BRAND_SCALE))).toEqual(BRAND_SCALE);
  });

  // `--spate-muted` and `--ifm-color-emphasis-600` carry the same hex in dark,
  // so repointing the `:root` mapping at the emphasis token also satisfies this.
  test('the search placeholder colour is the muted brand ink', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    expect(await computedTokens(page, ['--ifm-navbar-search-input-placeholder-color'])).toEqual({
      '--ifm-navbar-search-input-placeholder-color': '#9aa1a9',
    });
  });

  test('the navbar search pill paints the emphasis-200 brand value', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expect(page.locator('.navbar__search-input')).toHaveCSS('background-color', 'rgb(43, 48, 58)');
  });
});

/** Pins the light emphasis scale to Infima's greys, for which the site declares no override. */
test.describe('light mode emphasis scale', () => {
  test.use({colorMode: 'light'});

  test('emphasis-200 holds the Infima value', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    expect(await computedTokens(page, ['--ifm-color-emphasis-200'])).toEqual({
      '--ifm-color-emphasis-200': '#ebedf0',
    });
  });
});
