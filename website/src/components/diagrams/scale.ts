/**
 * The type and spacing scale the site's figures share, in viewBox units. It holds no React, so a
 * layout computed from it runs under `node:test`.
 */

// ---- type scale, in viewBox units ----
// Labels and notes are set at 12 units and region titles at 13, in `styles.module.css`. The metrics
// below are those sizes as the monospace face renders them; a change of size or face changes them.

/** Advance of one 12-unit character; box widths derive from it. */
export const CHAR = 7.4;
export const TITLE_CHAR = 8;
/** A 12-unit line's box above and below its baseline. */
export const ASCENT = 10.31;
export const DESCENT = 4.3;
export const TITLE_ASCENT = 11.17;
export const TITLE_DESCENT = 5.16;
/** A 12-unit line box. */
export const LINE_BOX = ASCENT + DESCENT;
export const TITLE_BOX = TITLE_ASCENT + TITLE_DESCENT;
/** How far a baseline sits below the centre of its line box. */
export const MID = (ASCENT - DESCENT) / 2;
/** Baseline to baseline. */
export const LINE = 17;
/** Baseline to baseline for region titles, whose box is taller. */
export const TITLE_LINE = 18;

// ---- spacing scale ----

/** The small step: a note under the box it annotates. */
export const S1 = 4;
/** Text beside the line or node it names. */
export const S2 = 8;
/** Between stages in a run, between rows, between a region's content blocks. */
export const GAP = 12;
/** Inside a region, on all four sides. */
export const PAD = 12;

/** The rendered width of 13-unit title `text`. */
export function titleW(text: string): number {
  return text.length * TITLE_CHAR;
}

/** The rendered width of 12-unit `text`. */
export function textW(text: string): number {
  return text.length * CHAR;
}

/** The first baseline of `n` lines centred on `y`. */
export function centredBaseline(y: number, n = 1): number {
  return y + MID - ((n - 1) * LINE) / 2;
}
