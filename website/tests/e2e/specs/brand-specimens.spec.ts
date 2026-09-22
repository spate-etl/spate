import {expect, test} from '../fixtures';
import type {ColorMode} from '../fixtures';
import {gotoRoute} from '../helpers/navigate';
import {VIEWPORTS} from '../helpers/routes';

const MODES: ColorMode[] = ['light', 'dark'];

for (const colorMode of MODES) {
  test.describe(`brand page specimens on a phone: ${colorMode}`, () => {
    test.use({colorMode, viewport: VIEWPORTS.phone});

    /** Every specimen renders at its natural aspect ratio when its frame caps its width. */
    test('keep their aspect ratio', async ({page}) => {
      await gotoRoute(page, 'brand', colorMode);
      const images = page.locator('.brand-specimen img');
      await expect(images.first()).toBeVisible();
      await expect.poll(() => images.evaluateAll((els) => els.every((el) => (el as HTMLImageElement).complete))).toBe(true);

      const specimens = await images.evaluateAll((els) =>
        els.map((el) => {
          const img = el as HTMLImageElement;
          const box = img.getBoundingClientRect();
          return {
            src: img.getAttribute('src'),
            width: box.width,
            skew: box.width / box.height / (img.naturalWidth / img.naturalHeight),
          };
        }),
      );

      // A hard assertion, not a skip: the ratio check only means something
      // if the social card's 400px display width is capped here.
      const card = specimens.find((s) => s.src?.endsWith('social-spate.png'));
      expect(card, 'the social card specimen').toBeDefined();
      expect(card!.width).toBeLessThan(400);

      for (const s of specimens) {
        expect(Math.abs(s.skew - 1), `${s.src} rendered off its natural ratio`).toBeLessThan(0.02);
      }
    });
  });
}
