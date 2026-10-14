import {expect, test} from '../fixtures';
import {expectNoAxeViolations} from '../helpers/axe';
import {gotoRoute} from '../helpers/navigate';
import {ROUTES, VIEWPORTS} from '../helpers/routes';

const DESKTOP = {width: 1280, height: 900};
const SIZES = {desktop: DESKTOP, phone: VIEWPORTS.phone};
const DIAGRAM = 'article figure:has(svg[data-mode])';
const CAPTION =
  'In 0.3.0, two I/O workers compressed every insert body before sending it, and the pipeline threads waited for queue space. On 799d3640, each pipeline thread compresses the chunks it seals, and the I/O workers only send them. The drawing shows four of the 32 threads and gives each one queue. In Spate a thread can route rows to several shard queues, and one full queue stalls every thread with a chunk for it.';

/**
 * Faults in each chart's visible drawing, in SVG user units from the rendered fonts: a label
 * outside the viewBox, two labels touching, a label rendered below 11px, or a value label count
 * other than the chart's.
 */
function chartFaults(counts: number[]): string[] {
  type Box = {x0: number; y0: number; x1: number; y1: number};
  const found: string[] = [];
  const overlaps = (a: Box, b: Box) => a.x0 < b.x1 && b.x0 < a.x1 && a.y0 < b.y1 && b.y0 < a.y1;
  const figures = Array.from(document.querySelectorAll('article figure')).filter((f) => f.querySelector('[data-value]'));
  if (figures.length !== counts.length) return [`${figures.length} charts, expected ${counts.length}`];
  figures.forEach((figure, n) => {
    const svgs = Array.from(figure.querySelectorAll<SVGSVGElement>('svg[role="img"]')).filter((svg) => svg.getBoundingClientRect().width > 0);
    if (svgs.length !== 1) {
      found.push(`chart ${n}: ${svgs.length} drawings visible`);
      return;
    }
    const [svg] = svgs;
    const view = svg.viewBox.baseVal;
    const scale = svg.getBoundingClientRect().width / view.width;
    const labels = Array.from(svg.querySelectorAll<SVGTextElement>('text')).map((el) => {
      const b = el.getBBox();
      const px = Number.parseFloat(getComputedStyle(el).fontSize) * scale;
      return {name: `chart ${n} ${JSON.stringify(el.textContent)}`, px, box: {x0: b.x, y0: b.y, x1: b.x + b.width, y1: b.y + b.height}};
    });
    const shown = svg.querySelectorAll('[data-value] text').length;
    if (shown !== counts[n]) found.push(`chart ${n}: ${shown} value labels, expected ${counts[n]}`);
    labels.forEach((a, i) => {
      if (a.box.x0 < view.x || a.box.y0 < view.y || a.box.x1 > view.x + view.width || a.box.y1 > view.y + view.height) {
        found.push(`${a.name} leaves the viewBox`);
      }
      if (a.px < 11 - 1e-6) found.push(`${a.name} renders at ${a.px.toFixed(2)}px`);
      labels.slice(i + 1).forEach((b) => {
        if (overlaps(a.box, b.box)) found.push(`${a.name} overlaps ${b.name}`);
      });
    });
  });
  return found;
}

/** The diagram's two drawings as markup, and the animations running anywhere in its figure. */
function diagramState(selector: string): {markup: string[]; running: number} {
  const figure = document.querySelector(selector)!;
  return {
    markup: Array.from(figure.querySelectorAll('svg[data-mode]')).map((svg) => svg.innerHTML),
    running: document
      .getAnimations()
      .filter((a) => a.playState === 'running' && a.effect instanceof KeyframeEffect && a.effect.target && figure.contains(a.effect.target)).length,
  };
}

/**
 * Pins that the compression post draws both charts and the before-and-after figure inside its
 * column at desktop and phone width in both colour modes: chart labels inside their drawings and
 * apart, the figure's two drawings side by side on a desktop and stacked on a phone with its
 * caption, no horizontal page scroll, and no axe violations. The fixture asks for reduced motion,
 * so the figure must hold one frame that still shows a stalled thread and a compressed chunk.
 */
for (const colorMode of ['light', 'dark'] as const) {
  test.describe(`compression post: ${colorMode}`, () => {
    test.use({colorMode});
    for (const [viewport, size] of Object.entries(SIZES)) {
      test(`charts and figure at ${viewport} width`, async ({page}, testInfo) => {
        await page.setViewportSize(size);
        await gotoRoute(page, 'compressionPost', colorMode);
        expect(await page.evaluate(chartFaults, [6, 12]), 'chart labels').toEqual([]);

        const figure = page.locator(DIAGRAM);
        await expect(figure).toHaveCount(1);
        await expect(figure.locator('figcaption')).toHaveText(CAPTION);
        const before = figure.locator('svg[data-mode="before"][role="img"]');
        const after = figure.locator('svg[data-mode="after"][role="img"]');
        await expect(before).toHaveAttribute('aria-label', /^Before, on 0\.3\.0/);
        await expect(after).toHaveAttribute('aria-label', /^After, on 799d3640/);
        const [b, a] = [(await before.boundingBox())!, (await after.boundingBox())!];
        if (viewport === 'desktop') {
          expect(a.y, 'side by side').toBeCloseTo(b.y, 0);
          expect(a.x, 'after right of before').toBeGreaterThanOrEqual(b.x + b.width);
        } else {
          expect(a.y, 'stacked').toBeGreaterThanOrEqual(b.y + b.height);
        }

        await expect(figure).toHaveAttribute('data-motion', 'reduced');
        // The figure only animates on screen, so the samples are taken with it in view.
        await figure.scrollIntoViewIfNeeded();
        const first = await page.evaluate(diagramState, DIAGRAM);
        await page.waitForTimeout(500);
        const second = await page.evaluate(diagramState, DIAGRAM);
        expect(second.markup, 'the figure under reduced motion').toEqual(first.markup);
        expect(first.running + second.running, 'running animations under reduced motion').toBe(0);
        await expect(before.locator('[data-thread="stalled"]').first()).toBeAttached();
        await expect(before.locator('[data-chunk="uncompressed"]').first()).toBeAttached();
        await expect(after.locator('[data-chunk="compressed"]').first()).toBeAttached();
        await expect(after.locator('[data-thread="stalled"]')).toHaveCount(0);
        await expect(figure.getByRole('button')).toHaveCount(0);

        const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
        expect(overflow, 'horizontal page scroll').toBeLessThanOrEqual(0);
        await expectNoAxeViolations(page, testInfo);
      });
    }
  });
}

/**
 * Pins that, with motion allowed, the figure animates and its pause button stops it, so the
 * reduced-motion check above is measuring a figure that would otherwise move.
 */
test.describe('compression post with motion', () => {
  test.use({colorMode: 'light', reducedMotion: 'no-preference'});
  test('the figure moves, and pause holds it', async ({page}, testInfo) => {
    await page.setViewportSize(DESKTOP);
    await gotoRoute(page, 'compressionPost', 'light');
    const figure = page.locator(DIAGRAM);
    await figure.scrollIntoViewIfNeeded();
    await expect(figure).toHaveAttribute('data-motion', 'full');
    const first = await page.evaluate(diagramState, DIAGRAM);
    await expect.poll(async () => (await page.evaluate(diagramState, DIAGRAM)).markup).not.toEqual(first.markup);

    await figure.getByRole('button', {name: 'Pause'}).click();
    const paused = await page.evaluate(diagramState, DIAGRAM);
    await page.waitForTimeout(500);
    expect((await page.evaluate(diagramState, DIAGRAM)).markup, 'the paused figure').toEqual(paused.markup);
    await expect(figure.getByRole('button', {name: 'Play'})).toBeVisible();
    await expectNoAxeViolations(page, testInfo);
  });

  /** Pins that the figure stops drawing once it leaves the screen. */
  const markup = (sel: string) => Array.from(document.querySelector(sel)!.querySelectorAll('svg[data-mode]')).map((s) => s.innerHTML).join('\n');
  test('the figure holds still off screen', async ({page}) => {
    await page.setViewportSize(DESKTOP);
    await gotoRoute(page, 'compressionPost', 'light');
    const figure = page.locator(DIAGRAM);
    await expect(figure).toHaveAttribute('data-motion', 'full');
    await figure.scrollIntoViewIfNeeded();
    const shown = await page.evaluate(markup, DIAGRAM);
    await expect.poll(() => page.evaluate(markup, DIAGRAM)).not.toEqual(shown);
    await page.evaluate(() => window.scrollTo(0, 0));
    await expect.poll(async () => (await figure.boundingBox())!.y > DESKTOP.height).toBe(true);
    let last = '';
    await expect.poll(async () => { const now = await page.evaluate(markup, DIAGRAM); const still = now === last; last = now; return still; }, {intervals: [100]}).toBe(true);
    await page.waitForTimeout(500); // stillness over an interval is the property
    expect(await page.evaluate(markup, DIAGRAM), 'the off-screen figure').toEqual(last);
  });
});

/** Pins that the engineering tag page lists this post and the DOOM post. */
test('the engineering tag lists the compression and DOOM posts', async ({page}) => {
  await gotoRoute(page, 'engineeringTag', 'dark');
  const titles = page.locator('article h2');
  await expect(titles).toHaveText(['Moving insert compression onto the pipeline threads', 'Running DOOM inside ClickHouse']);
  await expect(page.locator(`article h2 a[href="${ROUTES.compressionPost}"]`)).toHaveCount(1);
});

/**
 * Pins that the blog's feeds carry the figure's caption and none of its inline SVG or legend, which
 * a feed reader would draw without the site's CSS.
 */
test('the blog feeds carry the figure caption and no inline SVG or legend', async ({request}) => {
  for (const feed of ['/blog/rss.xml', '/blog/atom.xml']) {
    const response = await request.get(feed);
    expect(response.ok(), feed).toBe(true);
    const body = await response.text();
    expect(body, feed).toContain(CAPTION);
    expect(body, feed).not.toMatch(/<svg\b|&lt;svg\b/);
    expect(body, feed).not.toMatch(/aria-label="Legend"|aria-label=&quot;Legend&quot;/);
  }
});
