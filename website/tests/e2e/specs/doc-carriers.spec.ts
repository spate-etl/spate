import type {Locator} from '@playwright/test';

import {expect, test} from '../fixtures';
import type {ColorMode} from '../fixtures';
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

for (const colorMode of MODES) {
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
