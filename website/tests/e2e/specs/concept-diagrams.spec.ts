import {expect, test} from '../fixtures';
import {gotoRoute} from '../helpers/navigate';
import type {RouteName} from '../helpers/routes';

/** The concept pages and how many figures each carries, so a page that lost one fails. */
const PAGES: Array<[RouteName, number]> = [
  ['architecture', 3],
  ['multiSink', 2],
];

/**
 * 1280 and 1100 are desktop windows whose docs columns (703 and 568px) must both hold the
 * landscape layouts; 390 is a phone, which takes the portrait ones. 587 and 1083 give a 555px
 * column, just under the breakpoint, where a landscape overview would render more than 5% smaller
 * than the sequence drawings.
 */
const WIDTHS = [1280, 1100, 1083, 587, 390];
/** The windows whose docs column is wider than 556px, where an overview shows its landscape layout. */
const LANDSCAPE = [1280, 1100];

/** The drawings a figure shows at the current width, not its legend swatches. */
const DRAWINGS = 'article figure svg[role="img"]';

/**
 * Every collision between a label and something else drawn in the visible figures, as readable
 * strings. Geometry is read in SVG user units from the rendered fonts. Every text line is pinned to
 * its layout width with `textLength`, so only a change in the face's vertical metrics shows up here.
 * A label may sit wholly inside a box or a circle (a chip, a step disc) but may not straddle its
 * edge, cross a line or an arrowhead, touch another label or leave the viewBox. Labels are compared
 * box to box with no margin, since a text box already carries the font's line gap; lines, edges and
 * nodes keep `PAD` of clearance. A line counts at its computed stroke width and caps, with its
 * arrowhead.
 */
function collisions(selector: string): string[] {
  type Box = {x0: number; y0: number; x1: number; y1: number};
  const PAD = 1;
  const found: string[] = [];
  const overlaps = (a: Box, b: Box) => a.x0 < b.x1 && b.x0 < a.x1 && a.y0 < b.y1 && b.y0 < a.y1;
  const shrink = (a: Box) => ({x0: a.x0 + PAD, y0: a.y0 + PAD, x1: a.x1 - PAD, y1: a.y1 - PAD});
  const inside = (a: Box, b: Box) => a.x0 >= b.x0 && a.x1 <= b.x1 && a.y0 >= b.y0 && a.y1 <= b.y1;

  // Separating-axis test: does the convex outline `poly` meet the box? Touching counts.
  const hits = (poly: number[][], b: Box) => {
    const corners = [[b.x0, b.y0], [b.x1, b.y0], [b.x1, b.y1], [b.x0, b.y1]];
    const axes = [[1, 0], [0, 1], ...poly.map(([x, y], i) => [poly[(i + 1) % poly.length][1] - y, x - poly[(i + 1) % poly.length][0]])];
    return axes.every(([ax, ay]) => {
      const along = (pts: number[][]) => pts.map(([x, y]) => x * ax + y * ay);
      const [p, q] = [along(poly), along(corners)];
      return Math.max(...p) >= Math.min(...q) && Math.max(...q) >= Math.min(...p);
    });
  };

  for (const svg of Array.from(document.querySelectorAll<SVGSVGElement>(selector))) {
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
    // Each drawn line as the outline its stroke covers, plus its arrowhead's outline.
    const strokes = drawn('path').flatMap((el) => {
      const pts = (el.getAttribute('d') ?? '').match(/-?[\d.]+/g)?.map(Number) ?? [];
      const style = getComputedStyle(el);
      const half = Number.parseFloat(style.strokeWidth) / 2 || 0;
      // A butt cap stops at the endpoint; a round or square one reaches `half` past it.
      const cap = style.strokeLinecap === 'butt' ? 0 : half;
      const out: number[][][] = [];
      let last: {q: number[]; u: number[]} | undefined;
      for (let i = 0; i + 3 < pts.length; i += 2) {
        const [p, q] = [[pts[i], pts[i + 1]], [pts[i + 2], pts[i + 3]]];
        const len = Math.hypot(q[0] - p[0], q[1] - p[1]);
        const u = len ? [(q[0] - p[0]) / len, (q[1] - p[1]) / len] : [1, 0];
        const at = ([x, y]: number[], along: number, across: number) => [x + along * u[0] - across * u[1], y + along * u[1] + across * u[0]];
        out.push([at(p, -cap, -half), at(q, cap, -half), at(q, cap, half), at(p, -cap, half)]);
        last = {q, u};
      }
      // An arrowhead is its marker's outline, placed at the path's end and turned to its last segment.
      const marker = /url\(#(.+)\)/.exec(el.getAttribute('marker-end') ?? '')?.[1];
      const head = marker ? document.getElementById(marker) : null;
      if (head instanceof SVGMarkerElement && last) {
        const {q, u} = last;
        const k = head.markerWidth.baseVal.value / head.viewBox.baseVal.width;
        const [rx, ry] = [head.refX.baseVal.value, head.refY.baseVal.value];
        const outline = (head.querySelector('path')?.getAttribute('d') ?? '').match(/-?[\d.]+/g)?.map(Number) ?? [];
        const corners: number[][] = [];
        for (let i = 0; i + 1 < outline.length; i += 2) {
          const [a, b] = [(outline[i] - rx) * k, (outline[i + 1] - ry) * k];
          corners.push([q[0] + a * u[0] - b * u[1], q[1] + a * u[1] + b * u[0]]);
        }
        out.push(corners);
      }
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
      const where = `${svg.getAttribute('aria-label')?.split(':')[0]} ${a.name}`;
      if (!inside(a.box, {x0: view.x, y0: view.y, x1: view.x + view.width, y1: view.y + view.height})) {
        found.push(`${where} leaves the viewBox`);
      }
      labels.slice(i + 1).forEach((b) => {
        if (overlaps(shrink(a.box), shrink(b.box))) found.push(`${where} overlaps ${b.name}`);
      });
      if (!a.onLine && strokes.some((outline) => hits(outline, a.box))) found.push(`${where} crosses a line`);
      if (rects.some((r) => overlaps(a.box, r) && !inside(a.box, r))) {
        found.push(`${where} straddles a box edge`);
      }
      // A step's number sits on its own disc.
      const near = circles.filter((c) => !(a.onLine && c.group === a.group));
      if (near.some((c) => overlaps(a.box, c.box))) found.push(`${where} touches a node`);
    });
  }
  return found;
}

/**
 * Alignment faults in the visible drawings, in SVG user units. Stages in a run share a centre line
 * and one gap; the arrows between them share a length and stop the same distance from the box they
 * point at; every region keeps one padding on all four sides, the same across regions; every region
 * has one title, set at the same inset from its top-left corner as every other region's; no text
 * wrapped inside a region leaves a single word on a line; sequence lifelines are single unbroken
 * segments, evenly spaced, and every step disc sits on one; no text renders below 11px.
 */
function misalignments(selector: string): string[] {
  const TOL = 0.5;
  // Chromium snaps a text's box to device pixels, so a padding measured to text moves by up to a
  // pixel with the scale a drawing renders at.
  const TEXT_TOL = 1;
  const found: string[] = [];
  const near = (a: number, b: number, tol = TOL) => Math.abs(a - b) <= tol;
  const spread = (xs: number[]) => Math.max(...xs) - Math.min(...xs);
  let padding: number | undefined;

  for (const svg of Array.from(document.querySelectorAll<SVGSVGElement>(selector))) {
    const width = svg.getBoundingClientRect().width;
    if (width === 0) continue;
    const name = svg.getAttribute('aria-label')?.split(':')[0] ?? '?';
    const drawn = (sel: string, root: Element = svg) =>
      Array.from(root.querySelectorAll<SVGGraphicsElement>(sel)).filter((el) => !el.closest('defs'));
    const box = (el: SVGGraphicsElement) => {
      const b = el.getBBox();
      return {x0: b.x, y0: b.y, x1: b.x + b.width, y1: b.y + b.height, cx: b.x + b.width / 2, cy: b.y + b.height / 2};
    };

    const scale = width / svg.viewBox.baseVal.width;
    for (const text of drawn('text')) {
      const px = Number.parseFloat(getComputedStyle(text).fontSize) * scale;
      if (px < 11 - 1e-6) found.push(`${name} ${JSON.stringify(text.textContent)} renders at ${px.toFixed(2)}px`);
    }

    drawn('[data-run]').forEach((run, r) => {
      const boxes = drawn(':scope > [data-chip] > rect', run).map(box);
      const horizontal = spread(boxes.map((b) => b.cy)) <= TOL;
      const vertical = spread(boxes.map((b) => b.cx)) <= TOL;
      if (!horizontal && !vertical) {
        found.push(`${name} run ${r} shares no centre line`);
        return;
      }
      const [lo, hi] = horizontal ? (['x0', 'x1'] as const) : (['y0', 'y1'] as const);
      boxes.sort((a, b) => a[lo] - b[lo]);
      const gaps = boxes.slice(1).map((b, i) => b[lo] - boxes[i][hi]);
      if (spread(gaps) > TOL) found.push(`${name} run ${r} gaps ${gaps.map((g) => g.toFixed(1)).join(', ')}`);

      const links = drawn(':scope > path[data-link]', run).map((el) => {
        const [x0, y0, x1, y1] = (el.getAttribute('d') ?? '').match(/-?[\d.]+/g)!.map(Number);
        const from = horizontal ? x0 : y0;
        const to = horizontal ? x1 : y1;
        const target = boxes.find((b) => b[lo] >= to - TOL)!;
        const source = [...boxes].reverse().find((b) => b[hi] <= from + TOL)!;
        return {length: Math.hypot(x1 - x0, y1 - y0), stop: target[lo] - to, start: from - source[hi]};
      });
      if (links.length !== boxes.length - 1) found.push(`${name} run ${r} has ${links.length} arrows for ${boxes.length} stages`);
      for (const key of ['length', 'stop', 'start'] as const) {
        const values = links.map((l) => l[key]);
        if (values.length && spread(values) > TOL) {
          found.push(`${name} run ${r} arrow ${key} ${values.map((v) => v.toFixed(1)).join(', ')}`);
        }
      }
    });

    for (const region of drawn('rect[data-region]')) {
      const outer = box(region);
      const contents = drawn('text, rect, circle')
        .filter((el) => el !== region)
        .map(box)
        .filter((b) => b.x0 >= outer.x0 && b.x1 <= outer.x1 && b.y0 >= outer.y0 && b.y1 <= outer.y1);
      const sides = {
        left: Math.min(...contents.map((b) => b.x0 - outer.x0)),
        top: Math.min(...contents.map((b) => b.y0 - outer.y0)),
        right: Math.min(...contents.map((b) => outer.x1 - b.x1)),
        bottom: Math.min(...contents.map((b) => outer.y1 - b.y1)),
      };
      padding ??= sides.left;
      for (const [side, value] of Object.entries(sides)) {
        if (!near(value, padding, TEXT_TOL)) {
          found.push(`${name} region at ${outer.x0.toFixed(0)},${outer.y0.toFixed(0)} pads ${side} ${value.toFixed(2)}, not ${padding.toFixed(2)}`);
        }
      }
    }

    // Every region carries one title, and every title sits the same distance in from its region's
    // top-left corner.
    const regionBoxes = drawn('rect[data-region]').map(box);
    const within = (b: {x0: number; y0: number; x1: number; y1: number}) =>
      regionBoxes.find((r) => b.x0 >= r.x0 && b.x1 <= r.x1 && b.y0 >= r.y0 && b.y1 <= r.y1);
    const offsets = regionBoxes.map((region) => {
      const titles = drawn('[data-block="title"]')
        .map((block) => box(block.querySelector('text')!))
        .filter((b) => within(b) === region);
      if (titles.length !== 1) {
        found.push(`${name} region at ${region.x0.toFixed(0)},${region.y0.toFixed(0)} has ${titles.length} titles`);
        return undefined;
      }
      return {at: region, dx: titles[0].x0 - region.x0, dy: titles[0].y0 - region.y0};
    });
    const placed = offsets.filter((o) => o !== undefined);
    for (const axis of ['dx', 'dy'] as const) {
      if (placed.length && spread(placed.map((o) => o[axis])) > TOL) {
        const list = placed.map((o) => `${o.at.x0.toFixed(0)},${o.at.y0.toFixed(0)}: ${o[axis].toFixed(2)}`).join('; ');
        found.push(`${name} region titles sit at different ${axis === 'dx' ? 'left' : 'top'} insets (${list})`);
      }
    }

    // A note or label wrapped over several lines inside a region never leaves one word on a line.
    for (const block of drawn('[data-block="note"], [data-block="label"]')) {
      const lines = Array.from(block.querySelectorAll('text'));
      if (lines.length < 2 || !within(box(lines[0]))) continue;
      for (const line of lines) {
        if (!/\S\s+\S/.test(line.textContent ?? '')) {
          found.push(`${name} ${JSON.stringify(line.textContent)} is a one-word line in a wrapped block`);
        }
      }
    }

    // A lifeline is one unbroken vertical segment.
    for (const lifeline of drawn('[data-lifeline]')) {
      const paths = lifeline.querySelectorAll('path');
      const d = paths[0]?.getAttribute('d') ?? '';
      const pts = d.match(/-?[\d.]+/g)?.map(Number) ?? [];
      if (paths.length !== 1 || (d.match(/[ML]/g) ?? []).length !== 2 || pts[0] !== pts[2]) {
        found.push(`${name} lifeline at x=${Number(lifeline.getAttribute('data-lifeline')).toFixed(1)} is ${paths.length} paths, not one segment`);
      }
    }

    const lifelines = [...new Set(drawn('[data-lifeline]').map((el) => Number(el.getAttribute('data-lifeline'))))].sort(
      (a, b) => a - b,
    );
    if (lifelines.length) {
      const pitches = lifelines.slice(1).map((x, i) => x - lifelines[i]);
      if (spread(pitches) > TOL) found.push(`${name} lifeline pitches ${pitches.map((p) => p.toFixed(1)).join(', ')}`);
      for (const disc of drawn('[data-callout] circle')) {
        const cx = Number(disc.getAttribute('cx'));
        if (!lifelines.some((x) => near(x, cx))) found.push(`${name} step disc at x=${cx.toFixed(1)} is on no lifeline`);
      }
    }
  }
  return found;
}

/**
 * Scale faults in the visible drawings, in CSS pixels per viewBox unit: no drawing renders above
 * one, the drawings on a page render within 5% of each other, and each starts at its figure's left
 * edge.
 */
function scaleFaults(selector: string): string[] {
  const found: string[] = [];
  const scales: Array<{name: string; scale: number}> = [];
  for (const svg of Array.from(document.querySelectorAll<SVGSVGElement>(selector))) {
    const rect = svg.getBoundingClientRect();
    if (rect.width === 0) continue;
    const name = svg.getAttribute('aria-label')?.split(':')[0] ?? '?';
    const scale = rect.width / svg.viewBox.baseVal.width;
    scales.push({name, scale});
    // Layout rounds a width to 1/64px, so a capped drawing can measure a hair over one.
    if (scale > 1 + 1e-3) found.push(`${name} renders at ${scale.toFixed(3)}px per unit`);
    const inset = rect.left - svg.closest('figure')!.getBoundingClientRect().left;
    if (Math.abs(inset) > 0.5) found.push(`${name} starts ${inset.toFixed(1)}px in from its figure's left edge`);
  }
  const values = scales.map((s) => s.scale);
  if (values.length && Math.min(...values) < 0.95 * Math.max(...values)) {
    found.push(`drawings render more than 5% apart: ${scales.map((s) => `${s.name} ${s.scale.toFixed(3)}`).join('; ')}`);
  }
  return found;
}

/**
 * Layout faults in the visible legends, in CSS pixels: a swatch and its label share a centre within
 * 1px, every swatch has one size, items never overlap, items on a row share a top within 1px and
 * one gap, and the legend sits as far below its drawing as the caption sits below what precedes it.
 */
function legendFaults(): string[] {
  const found: string[] = [];
  const sizes = new Set<string>();
  for (const [f, figure] of Array.from(document.querySelectorAll('article figure')).entries()) {
    const legend = figure.querySelector<HTMLElement>('ul[aria-label="Legend"]');
    if (!legend) {
      found.push(`figure ${f} has no legend`);
      continue;
    }
    const items = Array.from(legend.children).map((li) => {
      const swatch = li.querySelector('svg')!.getBoundingClientRect();
      const label = li.querySelector('span')!.getBoundingClientRect();
      const rect = li.getBoundingClientRect();
      sizes.add(`${Math.round(swatch.width * 100) / 100}x${Math.round(swatch.height * 100) / 100}`);
      const offset = swatch.top + swatch.height / 2 - (label.top + label.height / 2);
      if (Math.abs(offset) > 1) found.push(`figure ${f} "${li.textContent}" swatch is ${offset.toFixed(2)}px off its label`);
      return {name: li.textContent, rect};
    });
    items.forEach((a, i) =>
      items.slice(i + 1).forEach((b) => {
        const o = a.rect.left < b.rect.right && b.rect.left < a.rect.right && a.rect.top < b.rect.bottom && b.rect.top < a.rect.bottom;
        if (o) found.push(`figure ${f} "${a.name}" overlaps "${b.name}"`);
      }),
    );
    const rows: (typeof items)[] = [];
    for (const item of items) {
      const row = rows.find((r) => Math.abs(r[0].rect.top - item.rect.top) < item.rect.height / 2);
      if (row) row.push(item);
      else rows.push([item]);
    }
    rows.forEach((row, r) => {
      const tops = row.map((i) => i.rect.top);
      if (Math.max(...tops) - Math.min(...tops) > 1) found.push(`figure ${f} legend row ${r} tops differ`);
      const gaps = row.slice(1).map((item, i) => item.rect.left - row[i].rect.right);
      if (gaps.length && Math.max(...gaps) - Math.min(...gaps) > 1) {
        found.push(`figure ${f} legend row ${r} gaps ${gaps.map((g) => g.toFixed(1)).join(', ')}`);
      }
    });

    const drawing = Array.from(figure.querySelectorAll('svg[role="img"]')).find((s) => s.getBoundingClientRect().width > 0)!;
    const caption = figure.querySelector('figcaption')!.getBoundingClientRect();
    const above = legend.getBoundingClientRect().top - drawing.getBoundingClientRect().bottom;
    // The caption follows the last visible block before it in the same column, or sits beside it.
    const blocks = Array.from(figure.querySelectorAll('svg[role="img"], ul, ol'))
      .map((el) => el.getBoundingClientRect())
      .filter((r) => r.width > 0 && r.bottom <= caption.top + 0.5);
    if (blocks.length) {
      const below = caption.top - Math.max(...blocks.map((r) => r.bottom));
      if (Math.abs(above - below) > 1) {
        found.push(`figure ${f} legend sits ${above.toFixed(1)}px below its drawing, the caption ${below.toFixed(1)}px below its block`);
      }
    }
  }
  if (sizes.size > 1) found.push(`legend swatch sizes ${[...sizes].join(', ')}`);
  return found;
}

/**
 * Pins that each concept-page figure shows exactly one of its layouts at a width, that no label in
 * it collides with another label, a line, a box edge or a node, that its drawing holds its
 * alignment rules and the page's one scale, and that its legend lines up, in both colour modes.
 */
for (const colorMode of ['light', 'dark'] as const) {
  test.describe(`concept diagrams: ${colorMode}`, () => {
    test.use({colorMode});
    test.skip(
      ({browserName}) => browserName !== 'chromium',
      'CI runs Chromium only, and the tolerances here are measured in Chromium',
    );
    for (const [route, figures] of PAGES) {
      for (const width of WIDTHS) {
        test(`${route} at ${width}px`, async ({page}) => {
          await page.setViewportSize({width, height: 900});
          await gotoRoute(page, route, colorMode);
          const shown = await page.evaluate(() =>
            Array.from(document.querySelectorAll('article figure')).map((figure) => {
              const drawings = Array.from(figure.querySelectorAll('svg[role="img"]'));
              return {count: drawings.length, visible: drawings.flatMap((svg, i) => (svg.getBoundingClientRect().width > 0 ? [i] : []))};
            }),
          );
          expect(shown.length, 'figures on the page').toBe(figures);
          // An overview draws its landscape layout first and its portrait layout second.
          expect(shown.map((s) => s.visible), 'layout shown per figure').toEqual(
            shown.map((s) => [s.count === 2 && !LANDSCAPE.includes(width) ? 1 : 0]),
          );
          expect(await page.evaluate(collisions, DRAWINGS), 'label collisions').toEqual([]);
          expect(await page.evaluate(misalignments, DRAWINGS), 'drawing alignment').toEqual([]);
          expect(await page.evaluate(scaleFaults, DRAWINGS), 'drawing scale').toEqual([]);
          expect(await page.evaluate(legendFaults), 'legend layout').toEqual([]);
        });
      }
    }
  });
}
