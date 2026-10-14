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
 * The bars in each chart drawing whose fill resolves to their own series' hatch inside that same
 * drawing, as `group-series`. A `url(#id)` resolves to the first element with that id in the
 * document, so a pattern in the other, hidden drawing does not count.
 */
function hatchedBars(): string[][] {
  return Array.from(document.querySelectorAll<SVGSVGElement>('article figure svg[role="img"]')).map((svg) =>
    Array.from(svg.querySelectorAll<SVGGElement>('[data-bar]'))
      .filter((g) => {
        const series = g.dataset.bar!.split('-')[1];
        const m = /url\("?#([^")]+)"?\)$/.exec(getComputedStyle(g.querySelector('rect')!).fill);
        if (!m || !m[1].endsWith(`-hatch-${series}`)) return false;
        const pattern = document.getElementById(m[1]);
        return pattern !== null && pattern.tagName === 'pattern' && svg.contains(pattern);
      })
      .map((g) => g.dataset.bar!),
  );
}

/**
 * Pins that the 0.3 release post's chart shows its column drawing on a desktop and its row drawing
 * on a phone, in both colour modes, with every label inside the drawing and apart from every other,
 * the three single-run bars hatched in each drawing, no horizontal page scroll, and no axe
 * violations.
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
        // The static split at both sizes and the Kafka queue at 8 instances are single runs.
        const single = ['0-0', '1-0', '1-3'];
        expect(await page.evaluate(hatchedBars), 'hatched bars per drawing').toEqual([single, single]);
        const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
        expect(overflow, 'horizontal page scroll').toBeLessThanOrEqual(0);
        await expectNoAxeViolations(page, testInfo);
      });
    }
  });
}

/**
 * Pins that the blog's feeds carry the chart's caption and none of its inline SVG or legend, which
 * a feed reader would draw without the site's CSS.
 */
test('the blog feeds carry the chart caption and no inline SVG or legend', async ({request}) => {
  for (const feed of ['/blog/rss.xml', '/blog/atom.xml']) {
    const response = await request.get(feed);
    expect(response.ok(), feed).toBe(true);
    const body = await response.text();
    expect(body, feed).toContain('Decoded bytes per second across the fleet.');
    expect(body, feed).not.toMatch(/<svg\b|&lt;svg\b/);
    expect(body, feed).not.toMatch(/aria-label="Legend"|aria-label=&quot;Legend&quot;/);
  }
});
