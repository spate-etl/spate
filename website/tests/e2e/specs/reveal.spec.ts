import {expect, test} from '../fixtures';

import {gotoRoute} from '../helpers/navigate';

// Overrides the fixture's `reducedMotion: 'reduce'`, which hides the
// `.reveal` fade in `site.css` this spec checks.
test.use({colorMode: 'light', reducedMotion: 'no-preference'});

test('homepage reveal sections settle to opacity 1 once scrolled into view', async ({page, colorMode}) => {
  await gotoRoute(page, 'home', colorMode);

  const reveal = page.locator('.reveal');
  const count = await reveal.count();
  // A selector matching nothing would make every assertion below pass
  // vacuously, hiding the exact regression this spec exists to catch.
  expect(count, 'no .reveal elements found on the homepage').toBeGreaterThan(0);

  for (const handle of await reveal.elementHandles()) {
    await handle.scrollIntoViewIfNeeded();
  }
  await page.waitForTimeout(700); // clears the 600ms transition in site.css

  const unsettled = await page.evaluate(
    () => Array.from(document.querySelectorAll('.reveal')).filter((el) => getComputedStyle(el).opacity !== '1').length,
  );
  expect(unsettled, 'reveal sections stuck below opacity 1 after being scrolled into view').toBe(0);
});
