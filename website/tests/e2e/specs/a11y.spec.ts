import AxeBuilder from '@axe-core/playwright';

import {expect, test} from '../fixtures';
import type {ColorMode} from '../fixtures';
import {expectNoAxeViolations, formatViolations} from '../helpers/axe';
import {gotoRoute} from '../helpers/navigate';
import {VIEWPORTS} from '../helpers/routes';
import type {KnownViolation} from '../helpers/axe';

const MODES: ColorMode[] = ['light', 'dark'];

for (const colorMode of MODES) {
  test.describe(`color mode: ${colorMode}`, () => {
    test.use({colorMode, viewport: VIEWPORTS.desktop});

    // #513: the docs search hint's `<kbd>` fails color-contrast in light
    // mode only. The widget is in the global navbar, so every route carries
    // it. The class suffix is a css-loader ident hash, so this matches the
    // authored prefix. It takes both kbd children of the shortcut hint (the
    // modifier key and "K"), and `page.$` resolves to the first, the only one
    // that ever violates. color-contrast still checks the rest of the page.
    // Dark mode uses different tokens and clears the floor.
    const searchHintKnown: KnownViolation[] =
      colorMode === 'light' ? [{ruleId: 'color-contrast', selector: '[class^="searchHint_"]'}] : [];

    test('home page has no WCAG violations', async ({page}, testInfo) => {
      await gotoRoute(page, 'home', colorMode);
      await expectNoAxeViolations(page, testInfo, {knownViolations: searchHintKnown});
    });

    test('quickstart page has no WCAG violations', async ({page}, testInfo) => {
      await gotoRoute(page, 'quickstart', colorMode);
      await expectNoAxeViolations(page, testInfo, {knownViolations: searchHintKnown});
    });

    // The Kafka source page carries the Metrics table #396 is about: at this
    // viewport it overflows its docs column and the resulting scroll frame
    // has no tabindex, so axe reports scrollable-region-focusable (wcag2a).
    // Anchored here rather than on docs/METRICS.md, whose wider table would
    // let the check pass for the wrong reason: axe treats a scroll container
    // as compliant once it has a focusable descendant, and 8 of METRICS.md's
    // 99 rows hold a link. This table's cells hold only `<code>`, so nothing
    // absorbs the check — adding a link to a cell here would.
    test('kafka source page has no WCAG violations', async ({page}, testInfo) => {
      await gotoRoute(page, 'kafkaSource', colorMode);
      await expectNoAxeViolations(page, testInfo, {
        knownViolations: [
          ...searchHintKnown, // #513, light mode only, see above
          // #396, tracked by the dedicated test below, which fails until the
          // table is fixed. Anchored on the "Metrics" heading's id rather
          // than the table's position, so prose added anywhere else on the
          // page does not renumber this entry out from under it.
          {ruleId: 'scrollable-region-focusable', selector: 'h2#metrics ~ table'},
        ],
      });
    });

    // Proves the gate catches #396: fails while the table is unfixed, so the
    // suite is green because the defect exists, not despite it. The stacked
    // pull request fixing the table deletes `test.fail()`.
    test('kafka source page Metrics table is keyboard scrollable (#396)', async ({page}, testInfo) => {
      test.fail();

      await gotoRoute(page, 'kafkaSource', colorMode);
      const results = await new AxeBuilder({page}).withRules(['scrollable-region-focusable']).analyze();

      await testInfo.attach('axe-results-396', {
        body: JSON.stringify(results, null, 2),
        contentType: 'application/json',
      });

      expect(results.violations, formatViolations(results.violations)).toEqual([]);
    });
  });
}
