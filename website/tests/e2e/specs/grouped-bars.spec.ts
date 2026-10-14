import {expect, test} from '../fixtures';
import {expectNoAxeViolations} from '../helpers/axe';
import {gotoRoute} from '../helpers/navigate';
import {VIEWPORTS} from '../helpers/routes';

/**
 * Faults in the visible chart drawing, in SVG user units from the rendered fonts: a label outside
 * the viewBox, two labels touching, a label rendered below 11px, or a value label count other than
 * `values`.
 */
function chartFaults(values: number): string[] {
  type Box = {x0: number; y0: number; x1: number; y1: number};
  const found: string[] = [];
  const overlaps = (a: Box, b: Box) => a.x0 < b.x1 && b.x0 < a.x1 && a.y0 < b.y1 && b.y0 < a.y1;
  const svgs = Array.from(document.querySelectorAll<SVGSVGElement>('article figure svg[role="img"]')).filter(
    (svg) => svg.getBoundingClientRect().width > 0,
  );
  if (svgs.length !== 1) return [`${svgs.length} chart drawings visible`];
  const [svg] = svgs;
  const view = svg.viewBox.baseVal;
  const scale = svg.getBoundingClientRect().width / view.width;
  const labels = Array.from(svg.querySelectorAll<SVGTextElement>('text')).map((el) => {
    const b = el.getBBox();
    const px = Number.parseFloat(getComputedStyle(el).fontSize) * scale;
    return {name: JSON.stringify(el.textContent), px, box: {x0: b.x, y0: b.y, x1: b.x + b.width, y1: b.y + b.height}};
  });
  const shown = svg.querySelectorAll('[data-value] text').length;
  if (shown !== values) found.push(`${shown} value labels, expected ${values}`);
  labels.forEach((a, i) => {
    if (a.box.x0 < view.x || a.box.y0 < view.y || a.box.x1 > view.x + view.width || a.box.y1 > view.y + view.height) {
      found.push(`${a.name} leaves the viewBox`);
    }
    if (a.px < 11 - 1e-6) found.push(`${a.name} renders at ${a.px.toFixed(2)}px`);
    labels.slice(i + 1).forEach((b) => {
      if (overlaps(a.box, b.box)) found.push(`${a.name} overlaps ${b.name}`);
    });
  });
  return found;
}

/**
 * Pins that the 0.3 release post's chart shows its column drawing on a desktop and its row drawing
 * on a phone, in both colour modes, with every label inside the drawing and apart from every other,
 * no horizontal page scroll, and no axe violations.
 */
for (const colorMode of ['light', 'dark'] as const) {
  test.describe(`grouped bar chart: ${colorMode}`, () => {
    test.use({colorMode});
    for (const [viewport, size] of Object.entries(VIEWPORTS)) {
      test(`release post at ${viewport} width`, async ({page}, testInfo) => {
        await page.setViewportSize(size);
        await gotoRoute(page, 'releasePost03', colorMode);
        const figure = page.locator('article figure');
        await expect(figure).toHaveCount(1);
        const drawings = figure.locator('svg[role="img"]');
        // The column drawing comes first and the row drawing second.
        const visible = viewport === 'phone' ? 1 : 0;
        await expect(drawings.nth(visible)).toBeVisible();
        await expect(drawings.nth(1 - visible)).toBeHidden();
        expect(await page.evaluate(chartFaults, 8), 'chart labels').toEqual([]);
        const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
        expect(overflow, 'horizontal page scroll').toBeLessThanOrEqual(0);
        await expectNoAxeViolations(page, testInfo);
      });
    }
  });
}
