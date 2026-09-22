import {expect, test} from '../fixtures';
import type {ColorMode} from '../fixtures';
import {expectNoAxeViolations} from '../helpers/axe';
import {settleColorMode} from '../helpers/navigate';
import {MISSING_ROUTE as MISSING, VIEWPORTS} from '../helpers/routes';

const MODES: ColorMode[] = ['light', 'dark'];

for (const colorMode of MODES) {
  test.describe(`404 page: ${colorMode}`, () => {
    test.use({colorMode, viewport: VIEWPORTS.desktop});

    test('a missing path answers 404 with the status and both ways back', async ({page}) => {
      const response = await page.goto(MISSING);
      expect(response?.status(), `GET ${MISSING}`).toBe(404);
      await settleColorMode(page, colorMode);

      await expect(page.getByRole('heading', {level: 1})).toHaveText('No page at this address.');
      await expect(page.getByRole('link', {name: 'Open documentation'})).toHaveAttribute('href', '/docs/user-guide');
      await expect(page.getByRole('link', {name: 'Go to homepage'})).toHaveAttribute('href', '/');
      await expect(page.locator('.not-found__art')).toHaveAttribute('aria-hidden', 'true');
    });

    test('has no WCAG violations', async ({page}, testInfo) => {
      await page.goto(MISSING);
      await settleColorMode(page, colorMode);
      await expectNoAxeViolations(page, testInfo);
    });
  });
}
