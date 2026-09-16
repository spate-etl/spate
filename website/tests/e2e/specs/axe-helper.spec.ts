import {expect, test} from '../fixtures';

import {expectNoAxeViolations} from '../helpers/axe';

// The site has no shadow root, so a cross-tree `target` needs a built page.
const SHADOW_ROOT_PAGE = `<!doctype html>
<html lang="en"><head><title>shadow</title></head><body>
  <x-card></x-card>
  <script>
    document.querySelector('x-card').attachShadow({mode: 'open'}).innerHTML =
      '<img src="data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==">';
  </script>
</body></html>`;

/** Pins that a violation whose `target` is nested (`[["x-card", "img"]]`) reaches the assertion. */
test('a violation inside a shadow root is reported', async ({page}, testInfo) => {
  await page.setContent(SHADOW_ROOT_PAGE);

  await expect(expectNoAxeViolations(page, testInfo)).rejects.toThrow(/image-alt/);
});

/** Pins that such a violation can be excluded by a selector reaching it through the host. */
test('a violation inside a shadow root can be excluded', async ({page}, testInfo) => {
  await page.setContent(SHADOW_ROOT_PAGE);

  await expectNoAxeViolations(page, testInfo, {
    knownViolations: [{ruleId: 'image-alt', selector: 'x-card img'}],
  });
});
