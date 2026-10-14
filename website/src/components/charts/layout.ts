/**
 * The geometry of a grouped bar chart, in viewBox units on the figures' type and spacing scale.
 * `columns` stands the bars up over a value axis on the left; `rows` lays them along a value axis at
 * the foot, for a column too narrow to set a value label over each bar. Both scale from zero.
 */
import {ASCENT, DESCENT, GAP, LINE_BOX, MID, S1, S2, centredBaseline, textW} from '../diagrams/scale.ts';

export type ChartData = {
  /** One label per group, in drawing order. */
  groups: string[];
  /** One name per series, in drawing order within each group. */
  series: string[];
  /** `values[g][s]`: series `s` in group `g`. */
  values: number[][];
  /** Decimal places a value label shows. */
  decimals: number;
};

/** A line of 12-unit text: its anchor point, its baseline and its rendered width. */
export type Label = {x: number; y: number; text: string; anchor: 'start' | 'middle' | 'end'; w: number};

export type Rect = {x: number; y: number; w: number; h: number};

export type Bar = {group: number; series: number; rect: Rect; value: Label};

export type Scale = {top: number; step: number; ticks: number[]};

export type Layout = {
  width: number;
  height: number;
  bars: Bar[];
  /** A tick's position along the value axis, and its label. */
  ticks: {at: number; label: Label}[];
  groupLabels: Label[];
  axisLabel: Label;
  /** The value axis, from zero to the top of the scale. */
  axis: {x0: number; y0: number; x1: number; y1: number};
  /** The line every bar starts from. */
  base: {x0: number; y0: number; x1: number; y1: number};
};

/** The space round the drawing, inside the viewBox. */
const M = S1;
/** A column chart's value axis, from zero to the top of the scale. */
const PLOT_H = 200;
/** A bar's thickness in a row chart. */
const BAR_H = 16;
/** Between the groups of a column chart. */
const GROUP_GAP = 3 * GAP;

/** A label's box, from the top of its line box to the bottom. */
export function labelBox(l: Label): {x0: number; y0: number; x1: number; y1: number} {
  const x0 = l.anchor === 'start' ? l.x : l.anchor === 'middle' ? l.x - l.w / 2 : l.x - l.w;
  return {x0, y0: l.y - ASCENT, x1: x0 + l.w, y1: l.y + DESCENT};
}

function label(text: string, x: number, y: number, anchor: Label['anchor']): Label {
  return {x, y, text, anchor, w: textW(text)};
}

/** `l`, after checking it ends inside the margin of a `width`-unit viewBox. */
function inside(l: Label, width: number, what: string): Label {
  if (labelBox(l).x1 > width - M) throw new Error(`a ${width}-unit chart is too narrow for the ${what} ${l.text}`);
  return l;
}

/** A tick value as a label shows it, with the float noise of `i * step` removed. */
function tickText(t: number): string {
  return String(Number(t.toFixed(10)));
}

/**
 * A scale from zero whose top is the first tick at or above `max`, with about `target` steps. A
 * step is 1, 2, 2.5 or 5 times a power of ten.
 *
 * Throws unless `max` is a positive number.
 */
export function niceScale(max: number, target = 5): Scale {
  if (!(max > 0) || !Number.isFinite(max)) throw new Error(`a scale needs a positive maximum, got ${max}`);
  const raw = max / target;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((k) => k * magnitude).find((s) => s >= raw - 1e-12)!;
  const n = Math.ceil(max / step - 1e-9);
  const ticks = Array.from({length: n + 1}, (_, i) => Number((i * step).toFixed(10)));
  return {top: ticks[n], step, ticks};
}

/** The largest value, after checking every group holds one value per series, each finite and at least zero. */
function largest(data: ChartData): number {
  for (const row of data.values) {
    if (row.length !== data.series.length) throw new Error('a group needs one value per series');
  }
  if (data.values.length !== data.groups.length) throw new Error('the chart needs one value row per group');
  const bad = data.values.flat().find((v) => !(Number.isFinite(v) && v >= 0));
  if (bad !== undefined) throw new Error(`a bar from zero needs a finite value of at least zero, got ${bad}`);
  return Math.max(...data.values.flat());
}

/**
 * Bars standing over a value axis, `width` units wide. Each bar carries its value above it, and each
 * group its label under the base line.
 *
 * Throws when `width` leaves a bar narrower than its value label, a group narrower than its label,
 * or too little room for the axis label.
 */
export function columns(data: ChartData, width: number, axisLabel: string): Layout {
  const scale = niceScale(largest(data));
  const tickW = Math.max(...scale.ticks.map((t) => textW(tickText(t))));
  const left = M + tickW + S2;
  const right = width - M;
  // Room above the top of the scale for the value label of a bar that reaches it.
  const top = M + LINE_BOX + GAP + LINE_BOX + S1;
  const base = top + PLOT_H;
  const at = (v: number) => base - (v / scale.top) * PLOT_H;

  const inner = right - GAP - (left + GAP);
  const groupW = (inner - (data.groups.length - 1) * GROUP_GAP) / data.groups.length;
  const pitch = groupW / data.series.length;
  const barW = pitch - S1;

  const bars: Bar[] = [];
  data.values.forEach((row, g) => {
    const gx = left + GAP + g * (groupW + GROUP_GAP);
    row.forEach((v, s) => {
      const x = gx + s * pitch + S1 / 2;
      const y = at(v);
      const value = label(v.toFixed(data.decimals), x + barW / 2, y - S1 - DESCENT, 'middle');
      if (value.w > barW) throw new Error(`a ${width}-unit chart is too narrow for the value ${value.text}`);
      bars.push({group: g, series: s, rect: {x, y, w: barW, h: base - y}, value});
    });
  });

  const groupBaseline = base + S2 + ASCENT;
  const groupLabels = data.groups.map((text, g) => {
    const l = label(text, left + GAP + g * (groupW + GROUP_GAP) + groupW / 2, groupBaseline, 'middle');
    if (l.w > groupW) throw new Error(`a ${width}-unit chart is too narrow for the group ${text}`);
    return l;
  });

  return {
    width,
    height: groupBaseline + DESCENT + M,
    bars,
    ticks: scale.ticks.map((t) => ({at: at(t), label: label(tickText(t), left - S2, at(t) + MID, 'end')})),
    groupLabels,
    axisLabel: inside(label(axisLabel, M, M + ASCENT, 'start'), width, 'axis label'),
    axis: {x0: left, y0: base, x1: left, y1: at(scale.top)},
    base: {x0: left, y0: base, x1: right, y1: base},
  };
}

/**
 * Bars laid along a value axis at the foot, `width` units wide. Each group's label sits over its
 * bars, and each bar carries its value after its end. The axis is as long as the longest value
 * label allows.
 *
 * Throws when `width` leaves the axis too short to set its tick labels apart, or too little room
 * for a group label or the axis label.
 */
export function rows(data: ChartData, width: number, axisLabel: string): Layout {
  const scale = niceScale(largest(data));
  const tickTexts = scale.ticks.map(tickText);
  const x0 = M + textW(tickTexts[0]) / 2;
  const room = width - M - x0;
  const length = Math.min(
    room - textW(tickTexts[tickTexts.length - 1]) / 2,
    ...data.values.flat().map((v) => ((room - S1 - textW(v.toFixed(data.decimals))) * scale.top) / v),
  );
  const at = (v: number) => x0 + (v / scale.top) * length;

  let y = M + LINE_BOX + GAP;
  const firstBar = y + LINE_BOX + S1;
  const bars: Bar[] = [];
  const groupLabels: Label[] = [];
  data.values.forEach((row, g) => {
    // Clear of the zero line the bars start from.
    groupLabels.push(inside(label(data.groups[g], x0 + S2, y + ASCENT, 'start'), width, 'group'));
    y += LINE_BOX + S1;
    row.forEach((v, s) => {
      const w = at(v) - x0;
      const value = label(v.toFixed(data.decimals), x0 + w + S1, centredBaseline(y + BAR_H / 2), 'start');
      bars.push({group: g, series: s, rect: {x: x0, y, w, h: BAR_H}, value});
      y += BAR_H + S1;
    });
    y += GAP - S1;
  });
  const axisY = y - GAP + S1;

  const tickBaseline = axisY + S2 + ASCENT;
  const ticks = scale.ticks.map((t, i) => ({at: at(t), label: label(tickTexts[i], at(t), tickBaseline, 'middle' as const)}));
  ticks.slice(1).forEach((t, i) => {
    if (labelBox(ticks[i].label).x1 + S1 > labelBox(t.label).x0) {
      throw new Error(`a ${width}-unit chart is too narrow for its tick labels`);
    }
  });

  return {
    width,
    height: tickBaseline + DESCENT + M,
    bars,
    ticks,
    groupLabels,
    axisLabel: inside(label(axisLabel, M, M + ASCENT, 'start'), width, 'axis label'),
    axis: {x0, y0: axisY, x1: at(scale.top), y1: axisY},
    base: {x0, y0: firstBar - S1, x1: x0, y1: axisY},
  };
}
