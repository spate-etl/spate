import type {Locator, Page} from '@playwright/test';

import {expect, test} from '../fixtures';
import type {ColorMode} from '../fixtures';
import {expectNoAxeViolations} from '../helpers/axe';
import {gotoRoute} from '../helpers/navigate';
import {VIEWPORTS} from '../helpers/routes';

const MODES: ColorMode[] = ['light', 'dark'];

type Box = {left: number; top: number; right: number; bottom: number};

/** The boxes of a heading's rendered text, which a float beside it narrows without narrowing the element. */
function textBoxes(heading: Locator): Promise<Box[]> {
  return heading.evaluate((el) => {
    const range = document.createRange();
    range.selectNodeContents(el);
    return Array.from(range.getClientRects(), ({left, top, right, bottom}) => ({left, top, right, bottom}));
  });
}

function box(locator: Locator): Promise<Box> {
  return locator.evaluate((el) => {
    const {left, top, right, bottom} = el.getBoundingClientRect();
    return {left, top, right, bottom};
  });
}

function overlaps(a: Box, b: Box): boolean {
  return a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;
}

/** Asserts the carrier shows `numeral` in the colour mode's image, loaded, and hidden from assistive technology. */
async function expectCarrier(carrier: Locator, numeral: string, colorMode: ColorMode): Promise<void> {
  await expect(carrier).toHaveAttribute('aria-hidden', 'true');
  const image = carrier.locator('img:visible');
  await expect(image).toHaveAttribute('src', `/img/brand/carrier-${numeral}${colorMode === 'dark' ? '-dark' : ''}.svg`);
  await expect.poll(() => image.evaluate((img: HTMLImageElement) => img.naturalWidth)).toBeGreaterThan(0);
}

const SECTION_PAGES = ['workAssignment', 'customSource', 'scalingOut', 'instrumentingConnectors'] as const;

/**
 * Each section carrier on the page with the H2 that follows it and that H2's
 * place among the page's H2s.
 */
async function sections(page: Page): Promise<{carrier: Locator; heading: Locator; place: number}[]> {
  const places = await page.locator('.section-carrier').evaluateAll((carriers) =>
    carriers.map((c) => {
      const headings = Array.from(document.querySelectorAll('.markdown > h2'));
      return headings.indexOf(c.nextElementSibling as Element) + 1;
    }),
  );
  return places.map((place, i) => ({
    carrier: page.locator('.section-carrier').nth(i),
    heading: page.locator('.markdown > h2').nth(place - 1),
    place,
  }));
}

for (const colorMode of MODES) {
  test.describe(`section carriers, desktop: ${colorMode}`, () => {
    test.use({colorMode, viewport: VIEWPORTS.desktop});

    for (const route of SECTION_PAGES) {
      test(`${route}: each carrier shows its H2's place, beside the heading`, async ({page}) => {
        await gotoRoute(page, route, colorMode);
        const found = await sections(page);
        expect(found.length).toBeGreaterThan(0);
        for (const {carrier, heading, place} of found) {
          expect(place, 'a carrier directly above an H2').toBeGreaterThan(0);
          await expectCarrier(carrier, String(place).padStart(2, '0'), colorMode);
          const art = await box(carrier);
          const text = await textBoxes(heading);
          const top = Math.min(...text.map((t) => t.top));
          const bottom = Math.max(...text.map((t) => t.bottom));
          for (const line of text) expect(line.left).toBeGreaterThanOrEqual(art.right);
          // Firefox and WebKit round inline code boxes by up to a pixel.
          expect(Math.abs((top + bottom) / 2 - (art.top + art.bottom) / 2), 'heading centred on its carrier').toBeLessThanOrEqual(2);
        }
      });
    }

    test('the first section under a page carrier carries none', async ({page}) => {
      await gotoRoute(page, 'workAssignment', colorMode);
      await expect(page.locator('.page-carrier')).toHaveCount(1);
      const first = page.locator('.markdown > h2').first();
      expect(await first.evaluate((h) => h.previousElementSibling?.classList.contains('section-carrier'))).toBe(false);
      await expect(page.locator('.section-carrier')).toHaveCount(await page.locator('.markdown > h2').count() - 1);
    });

    test('section carrier pages have no WCAG violations', async ({page}, testInfo) => {
      for (const route of ['workAssignment', 'customSource'] as const) {
        await gotoRoute(page, route, colorMode);
        await expectNoAxeViolations(page, testInfo);
      }
    });
  });

  test.describe(`section carriers, phone: ${colorMode}`, () => {
    test.use({colorMode, viewport: VIEWPORTS.phone});

    test('in a narrow column a section carrier stands above its heading at 76 × 70', async ({page}) => {
      await gotoRoute(page, 'customSource', colorMode);
      for (const {carrier, heading} of await sections(page)) {
        const art = await box(carrier);
        const title = await box(heading);
        expect(art.right - art.left).toBeCloseTo(76, 0);
        expect(art.bottom - art.top).toBeCloseTo(70, 0);
        expect(title.top - art.bottom).toBeCloseTo(24, 0);
      }
    });

    test('a link to a heading scrolls its carrier into view below the navbar', async ({page}) => {
      await gotoRoute(page, 'customSource', colorMode);
      const carrier = page.locator('.section-carrier').nth(1);
      const id = await carrier.evaluate((c) => c.nextElementSibling!.id);
      await page.evaluate((hash) => (location.hash = hash), id);
      const navbar = await box(page.locator('.navbar'));
      await expect.poll(async () => (await box(carrier)).top).toBeLessThan(navbar.bottom + 24);
      expect((await box(carrier)).top).toBeGreaterThanOrEqual(navbar.bottom);
    });
  });

  test.describe(`page carrier, desktop: ${colorMode}`, () => {
    test.use({colorMode, viewport: VIEWPORTS.desktop});

    test('a numbered guide page carries its place right of the title', async ({page}) => {
      await gotoRoute(page, 'quickstart', colorMode);
      const carrier = page.locator('.page-carrier');
      await expectCarrier(carrier, '02', colorMode);

      const art = await box(carrier);
      expect(art.right - art.left).toBeCloseTo(181, 0);
      const title = page.locator('.markdown h1');
      for (const text of await textBoxes(title)) {
        expect(overlaps(text, art), 'title text beside the carrier').toBe(false);
        expect(text.right).toBeLessThanOrEqual(art.left);
      }
    });

    test('a page without a number prefix carries none', async ({page}) => {
      await gotoRoute(page, 'kafkaSource', colorMode);
      await expect(page.locator('.carrier')).toHaveCount(0);
    });
  });

  // At 800px the architecture page's opening diagram starts inside the page
  // carrier's bottom margin, which is where a float would narrow it.
  test.describe(`page carrier, tablet: ${colorMode}`, () => {
    test.use({colorMode, viewport: {width: 800, height: 900}});

    test('a code block below the carrier keeps the full column', async ({page}) => {
      await gotoRoute(page, 'architecture', colorMode);
      await expectCarrier(page.locator('.page-carrier'), '01', colorMode);
      const code = page.locator('.markdown > .theme-code-block').first();
      const [frame, pre] = await code.evaluate((el) => {
        const block = el.querySelector('pre')!;
        return [block.parentElement!.getBoundingClientRect().width, block.getBoundingClientRect().width];
      });
      expect(pre).toBeCloseTo(frame, 0);
    });
  });

  test.describe(`page carrier, phone: ${colorMode}`, () => {
    test.use({colorMode, viewport: VIEWPORTS.phone});

    test('in a narrow column the carrier stands above the title at 76 × 70', async ({page}) => {
      await gotoRoute(page, 'quickstart', colorMode);
      const carrier = page.locator('.page-carrier');
      await expectCarrier(carrier, '02', colorMode);

      const art = await box(carrier);
      const title = await box(page.locator('.markdown h1'));
      expect(art.right - art.left).toBeCloseTo(76, 0);
      expect(art.bottom - art.top).toBeCloseTo(70, 0);
      expect(art.left).toBeCloseTo(title.left, 0);
      expect(title.top - art.bottom).toBeCloseTo(24, 0);
    });
  });
}
