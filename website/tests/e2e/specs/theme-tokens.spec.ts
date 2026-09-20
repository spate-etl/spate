import type {Page} from '@playwright/test';

import {expect, test} from '../fixtures';
import type {ColorMode} from '../fixtures';
import {gotoRoute} from '../helpers/navigate';

const BRAND_SCALE = {
  '--ifm-color-emphasis-100': '#22262f',
  '--ifm-color-emphasis-200': '#2b303a',
  '--ifm-color-emphasis-300': '#363c48',
  '--ifm-color-emphasis-600': '#9aa1a9',
  '--ifm-color-emphasis-700': '#b6bcc4',
  '--ifm-color-emphasis-800': '#d3d7dc',
};

/** The dark grounds and surfaces, spelled as Chromium computes them off the minified bundle. */
const BRAND_DARK = {
  '--ifm-background-color': '#16181d',
  '--ifm-background-surface-color': '#1c1f26',
  '--ifm-code-background': '#22262f',
  '--ifm-color-content-secondary': '#9aa1a9',
  '--ifm-table-stripe-background': '#22262f',
};

/**
 * The same names in light, where `:root` carries the only site declaration.
 * `--ifm-toc-border-color` joins them here because its light value differs
 * from Infima's, which its dark value does not.
 */
const BRAND_LIGHT = {
  '--ifm-background-color': '#fbfaf8',
  '--ifm-background-surface-color': '#fff',
  '--ifm-code-background': '#f3f1ed',
  '--ifm-color-content-secondary': '#5f646c',
  '--ifm-table-stripe-background': '#f3f1ed',
  '--ifm-toc-border-color': '#e2ded7',
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
 * Pins the six emphasis values and the grounds and surfaces `custom.css`
 * declares for dark mode, the search placeholder colour the `:root` mapping
 * supplies, and the navbar search pill as painted.
 *
 * Regression for #568 and #584.
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

  test('the grounds and surfaces resolve to the brand values', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    expect(await computedTokens(page, Object.keys(BRAND_DARK))).toEqual(BRAND_DARK);
  });

  // Both candidate declarations resolve to #2b303a, so a value assertion is
  // blind here. They differ in what they depend on: Infima's is
  // `var(--ifm-color-emphasis-200)` and ours is `var(--spate-border)`. Moving
  // emphasis-200 to a sentinel therefore moves the computed value only while
  // Infima's declaration is the one that won. The probe does not generalise.
  // The other five tokens are told apart by value, and asking which
  // declaration won for an arbitrary token means resolving the cascade across
  // the whole bundle. An Infima release that rewrites its declaration to a
  // literal retires this check, and it passes silently from then on.
  test('the site declaration decides the toc border', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    const moved = await page.evaluate(() => {
      const read = () => getComputedStyle(document.documentElement).getPropertyValue('--ifm-toc-border-color').trim();
      const before = read();
      document.documentElement.style.setProperty('--ifm-color-emphasis-200', 'rgb(1, 2, 3)');
      const after = read();
      document.documentElement.style.removeProperty('--ifm-color-emphasis-200');
      return before !== after;
    });
    expect(moved).toBe(false);
  });
});

/**
 * Pins the light emphasis scale to Infima's greys, for which the site declares
 * no override, and the grounds and surfaces to their `:root` mappings.
 *
 * Regression for #584.
 */
test.describe('light mode emphasis scale', () => {
  test.use({colorMode: 'light'});

  test('emphasis-200 holds the Infima value', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    expect(await computedTokens(page, ['--ifm-color-emphasis-200'])).toEqual({
      '--ifm-color-emphasis-200': '#ebedf0',
    });
  });

  test('the grounds and surfaces resolve to the brand values', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    expect(await computedTokens(page, Object.keys(BRAND_LIGHT))).toEqual(BRAND_LIGHT);
  });
});

/** The `<hr>` as Infima paints it, from `--ifm-hr-background-color` over `border: 0`. */
const RULE_COLOUR: Record<ColorMode, string> = {
  light: 'rgb(226, 222, 215)',
  dark: 'rgb(43, 48, 58)',
};

/**
 * Pins the site's one `<hr>` to the brand border in both colour modes. A
 * mapping declared on any name Infima does not read paints nothing, and the
 * rule keeps whatever `--ifm-color-emphasis-500` holds.
 *
 * Regression for #584.
 */
for (const colorMode of Object.keys(RULE_COLOUR) as ColorMode[]) {
  test.describe(`horizontal rule: ${colorMode}`, () => {
    test.use({colorMode});

    test('paints the brand border', async ({page}) => {
      await gotoRoute(page, 'clickdoomPost', colorMode);
      await expect(page.locator('.markdown hr').first()).toHaveCSS('background-color', RULE_COLOUR[colorMode]);
    });
  });
}
