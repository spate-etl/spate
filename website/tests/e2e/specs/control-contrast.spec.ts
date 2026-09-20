import type {Locator, Page} from '@playwright/test';

import {expect, test} from '../fixtures';
import {gotoRoute} from '../helpers/navigate';
import {VIEWPORTS} from '../helpers/routes';

/** WCAG 2.2 SC 1.4.11, non-text contrast. */
const FLOOR = 3;

/** Parses `rgb()`, `rgba()` or a three- or six-digit hex into 8-bit channels. Alpha is dropped. */
function channels(colour: string): [number, number, number] {
  const hex = colour.trim().match(/^#([\da-f]{3}|[\da-f]{6})$/i);
  if (hex) {
    const digits = hex[1].length === 3 ? [...hex[1]].map((d) => d + d) : hex[1].match(/../g)!;
    return digits.map((d) => Number.parseInt(d, 16)) as [number, number, number];
  }
  const rgb = colour.match(/^rgba?\(([^)]+)\)$/);
  if (!rgb) throw new Error(`unparseable colour: ${colour}`);
  const parts = rgb[1].split(/[\s,/]+/).filter(Boolean).slice(0, 3);
  if (parts.length !== 3) throw new Error(`unparseable colour: ${colour}`);
  return parts.map(Number) as [number, number, number];
}

function luminance(colour: string): number {
  const [r, g, b] = channels(colour).map((channel) => {
    const srgb = channel / 255;
    return srgb <= 0.03928 ? srgb / 12.92 : ((srgb + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

/** WCAG 2 relative-luminance ratio, 1 to 21. */
function ratio(a: string, b: string): number {
  const [light, dark] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (light + 0.05) / (dark + 0.05);
}

/** Composites a colour over a ground at `alpha`, the way an element's own opacity paints. */
function composite(colour: string, ground: string, alpha: number): string {
  const [front, back] = [channels(colour), channels(ground)];
  return `rgb(${front.map((channel, i) => channel * alpha + back[i] * (1 - alpha)).join(', ')})`;
}

function token(page: Page, name: string): Promise<string> {
  return page.evaluate((n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim(), name);
}

function background(locator: Locator): Promise<string> {
  return locator.evaluate((el) => getComputedStyle(el).backgroundColor);
}

/**
 * Asserts a control's top border is painted and clears the floor against the
 * colour beside it.
 *
 * The style and width come first. A `border: none` declaration leaves
 * `border-top-color` on `currentcolor`, which measured 8.70:1 light and
 * 12.17:1 dark on the search pill, so a ratio on its own passes an unpainted
 * border. Every read goes through a retrying assertion because both the
 * search hint and the code-block buttons arrive on hydration.
 */
async function expectBoundary(target: Locator, ground: () => Promise<string>): Promise<void> {
  await expect(target).toHaveCSS('border-top-style', 'solid');
  await expect
    .poll(async () => target.evaluate((el) => Number.parseFloat(getComputedStyle(el).borderTopWidth)))
    .toBeGreaterThan(0);
  await expect
    .poll(async () => ratio(await target.evaluate((el) => getComputedStyle(el).borderTopColor), await ground()))
    .toBeGreaterThanOrEqual(FLOOR);
}

const pill = (page: Page) => page.locator('.navbar__search-input');
// Two `<kbd>` elements, "ctrl" and "K", styled by one rule.
const hint = (page: Page) => page.locator('.navbar__search [class*="searchHint_"]').first();
const codeBlock = (page: Page) => page.locator('.theme-code-block').first();
const codeButton = (page: Page) => codeBlock(page).locator('[class*="buttonGroup_"] button').first();

/**
 * Asserts the ring a pointer user sees on a revealed code-block button clears
 * the floor.
 *
 * The buttons rest at `opacity: 0` and the theme reveals them part-way while
 * the block is hovered, so the border as painted is its colour composited over
 * the code ground at the element's own opacity. A read of `border-top-color`
 * alone is blind to that.
 */
async function expectRevealedRing(page: Page): Promise<void> {
  await codeBlock(page).hover();
  await expect
    .poll(async () => {
      const painted = await codeButton(page).evaluate((el) => {
        const style = getComputedStyle(el);
        const block = el.closest('.theme-code-block')!;
        return {
          border: style.borderTopColor,
          ground: getComputedStyle(block).backgroundColor,
          alpha: Number(style.opacity),
        };
      });
      return ratio(composite(painted.border, painted.ground, painted.alpha), painted.ground);
    })
    .toBeGreaterThanOrEqual(FLOOR);
}

/**
 * Pins the navbar search pill, the search hint's `<kbd>` and the code-block
 * copy and wrap buttons to the 3:1 WCAG 2.2 SC 1.4.11 asks of a control
 * boundary, in both colour modes.
 *
 * Regression for #569.
 */
test.use({viewport: VIEWPORTS.desktop});

test.describe('control boundaries in light mode', () => {
  test.use({colorMode: 'light'});

  // The ground is the `--spate-bg` token read off `<html>`. The navbar fills
  // with that token at 72% over the page ground, and `--ifm-background-color`
  // resolves to the same token in both colour modes, so the composite equals
  // the token. The navbar's own computed fill is a `color(srgb ... / 0.72)`
  // string, which `channels()` does not parse.
  test('the search pill is bounded against the navbar', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expectBoundary(pill(page), () => token(page, '--spate-bg'));
  });

  test('the search hint is bounded against the pill', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expectBoundary(hint(page), () => background(pill(page)));
  });

  test('the code-block buttons are bounded against the code ground', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expectBoundary(codeButton(page), () => background(codeBlock(page)));
  });

  test('the code-block buttons keep their ring while the block is hovered', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expectRevealedRing(page);
  });
});

test.describe('control boundaries in dark mode', () => {
  test.use({colorMode: 'dark'});

  test('the search pill is bounded against the navbar', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expectBoundary(pill(page), () => token(page, '--spate-bg'));
  });

  test('the search hint is bounded against the pill', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expectBoundary(hint(page), () => background(pill(page)));
  });

  test('the code-block buttons are bounded against the code ground', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expectBoundary(codeButton(page), () => background(codeBlock(page)));
  });

  test('the code-block buttons keep their ring while the block is hovered', async ({page, colorMode}) => {
    await gotoRoute(page, 'quickstart', colorMode);
    await expectRevealedRing(page);
  });
});
