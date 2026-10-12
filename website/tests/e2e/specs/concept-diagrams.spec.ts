import {expect, test} from '../fixtures';

const PAGES = ['/docs/user-guide/concepts/architecture', '/docs/user-guide/concepts/multi-sink'];

/** Landscape and portrait: each width shows one of a figure's two layouts. */
const WIDTHS = [1280, 390];

/**
 * Every collision between a label and something else drawn in the visible concept figures, as
 * readable strings. Geometry is read in SVG user units from the rendered fonts, so a font metric
 * change shows up here. A label may sit wholly inside a box or a circle (a chip, a callout) but
 * may not straddle its edge, cross a line, touch another label or leave the viewBox. Labels are
 * compared box to box with no margin, since a text box already carries the font's line gap; lines,
 * edges and nodes keep `PAD` of clearance.
 */
function collisions(): string[] {
  type Box = {x0: number; y0: number; x1: number; y1: number};
  const PAD = 1;
  const found: string[] = [];
  const overlaps = (a: Box, b: Box) => a.x0 < b.x1 && b.x0 < a.x1 && a.y0 < b.y1 && b.y0 < a.y1;
  const shrink = (a: Box) => ({x0: a.x0 + PAD, y0: a.y0 + PAD, x1: a.x1 - PAD, y1: a.y1 - PAD});
  const inside = (a: Box, b: Box) => a.x0 >= b.x0 && a.x1 <= b.x1 && a.y0 >= b.y0 && a.y1 <= b.y1;

  // Liang-Barsky clip: does the segment p→q pass through the box?
  const crosses = ([px, py]: number[], [qx, qy]: number[], b: Box) => {
    let t0 = 0;
    let t1 = 1;
    const dx = qx - px;
    const dy = qy - py;
    for (const [p, q] of [
      [-dx, px - b.x0],
      [dx, b.x1 - px],
      [-dy, py - b.y0],
      [dy, b.y1 - py],
    ]) {
      if (p === 0) {
        if (q < 0) return false;
      } else {
        const t = q / p;
        if (p < 0) t0 = Math.max(t0, t);
        else t1 = Math.min(t1, t);
      }
    }
    return t0 <= t1;
  };

  for (const svg of Array.from(document.querySelectorAll<SVGSVGElement>('article figure svg'))) {
    if (svg.getBoundingClientRect().width === 0) continue;
    const drawn = (sel: string) =>
      Array.from(svg.querySelectorAll<SVGGraphicsElement>(sel)).filter((el) => !el.closest('defs'));
    const view = svg.viewBox.baseVal;
    const labels = drawn('text').map((el) => {
      const b = el.getBBox();
      return {
        group: el.parentElement,
        name: JSON.stringify(el.textContent),
        onLine: !!el.closest('[data-callout]'),
        box: {x0: b.x - PAD, y0: b.y - PAD, x1: b.x + b.width + PAD, y1: b.y + b.height + PAD},
      };
    });
    const segments = drawn('path').flatMap((el) => {
      const pts = (el.getAttribute('d') ?? '').match(/-?[\d.]+/g)?.map(Number) ?? [];
      const out: number[][][] = [];
      for (let i = 0; i + 3 < pts.length; i += 2) out.push([[pts[i], pts[i + 1]], [pts[i + 2], pts[i + 3]]]);
      return out;
    });
    const rects = drawn('rect').map((el) => {
      const b = el.getBBox();
      return {x0: b.x, y0: b.y, x1: b.x + b.width, y1: b.y + b.height};
    });
    const circles = drawn('circle').map((el) => {
      const [cx, cy, r] = ['cx', 'cy', 'r'].map((a) => Number(el.getAttribute(a)));
      return {group: el.parentElement, box: {x0: cx - r, y0: cy - r, x1: cx + r, y1: cy + r}};
    });

    labels.forEach((a, i) => {
      const where = `${svg.getAttribute('class')} ${a.name}`;
      if (!inside(a.box, {x0: view.x, y0: view.y, x1: view.x + view.width, y1: view.y + view.height})) {
        found.push(`${where} leaves the viewBox`);
      }
      labels.slice(i + 1).forEach((b) => {
        if (overlaps(shrink(a.box), shrink(b.box))) found.push(`${where} overlaps ${b.name}`);
      });
      if (!a.onLine && segments.some(([p, q]) => crosses(p, q, a.box))) found.push(`${where} crosses a line`);
      if (rects.some((r) => overlaps(a.box, r) && !inside(a.box, r))) {
        found.push(`${where} straddles a box edge`);
      }
      // A callout's number sits on its own disc.
      const near = circles.filter((c) => !(a.onLine && c.group === a.group));
      if (near.some((c) => overlaps(a.box, c.box))) found.push(`${where} touches a node`);
    });
  }
  return found;
}

/**
 * Pins that each concept-page figure shows exactly one of its layouts at a width, and that no label
 * in it collides with another label, a line, a box edge or a node, in both colour modes.
 */
for (const colorMode of ['light', 'dark'] as const) {
  test.describe(`concept diagrams: ${colorMode}`, () => {
    test.use({colorMode});
    for (const path of PAGES) {
      for (const width of WIDTHS) {
        test(`${path} at ${width}px has no label collisions`, async ({page}) => {
          await page.setViewportSize({width, height: 900});
          await page.goto(path);
          await page.evaluate(() => document.fonts.ready);
          await expect(page.locator('article figure svg').first()).toBeAttached();
          const shown = await page.evaluate(() =>
            Array.from(document.querySelectorAll('article figure')).map(
              (figure) => Array.from(figure.querySelectorAll('svg')).filter((svg) => svg.getBoundingClientRect().width > 0).length,
            ),
          );
          expect(shown, 'layouts shown per figure').toEqual(shown.map(() => 1));
          expect(await page.evaluate(collisions)).toEqual([]);
        });
      }
    }
  });
}
