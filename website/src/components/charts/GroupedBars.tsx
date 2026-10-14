/**
 * A grouped bar chart drawn as inline SVG on the concept figures' scale and frame. The figure's own
 * width picks one of two drawings: bars standing over a value axis, or, in a phone column, bars laid
 * along one. A chart whose value labels do not fit over its bars, or whose group labels do not fit
 * under its groups, lays them along the axis in both.
 */
import React, {useId} from 'react';

import {S1, Text, atMostOneToOne, styles as frame} from '../diagrams/parts';
import {landscape, rows, type ChartData, type Layout} from './layout';
import styles from './styles.module.css';

/** The landscape drawing's width, inside the 586 units the frame's landscape breakpoint allows for. */
const WIDE = 560;
/** The row chart's width, the sequence figures' width, which a phone column renders near 1:1. */
const NARROW = 360;
/** One class per series, in drawing order. */
const SERIES = [styles.s0, styles.s1, styles.s2, styles.s3];
/** A hatch's stripe pitch, and a stripe's width within it. */
const HATCH = 6;
const STRIPE = 2.5;

/** A legend swatch's box, the concept figures' swatch size. */
const SW_W = 33;
const SW_H = 20;

type Props = {
  /** The chart's accessible name. */
  name: string;
  groups: string[];
  /** At most four. */
  series: string[];
  /** `values[g][s]`: series `s` in group `g`. */
  values: number[][];
  /** `single[g][s]` marks a value from a single run, drawn hatched; others are a mean. */
  single?: boolean[][];
  /** What the value axis measures, and its unit. */
  measure: string;
  unit: string;
  decimals?: number;
  /** States every value in words, so the chart is not the only place a reader finds them. */
  caption: React.ReactNode;
};

/** One hatch per series: stripes of the series colour over the page ground. */
function Hatches({id, count}: {id: string; count: number}): React.JSX.Element {
  return (
    <defs>
      {SERIES.slice(0, count).map((cls, s) => (
        <pattern
          key={s}
          id={`${id}-hatch-${s}`}
          width={HATCH}
          height={HATCH}
          patternUnits="userSpaceOnUse"
          patternTransform="rotate(45)">
          <rect className={styles.ground} width={HATCH} height={HATCH} />
          <rect className={`${cls} ${styles.stripe}`} width={STRIPE} height={HATCH} />
        </pattern>
      ))}
    </defs>
  );
}

function Drawing({id, layout, single, vertical}: {id: string; layout: Layout; single?: boolean[][]; vertical: boolean}): React.ReactNode {
  const {bars, ticks, groupLabels, axisLabel, axis, base} = layout;
  const line = (l: {x0: number; y0: number; x1: number; y1: number}) => `M${l.x0} ${l.y0} L${l.x1} ${l.y1}`;
  const tickMarks = ticks
    .map(({at}) => (vertical ? `M${axis.x0 - S1} ${at} L${axis.x0} ${at}` : `M${at} ${axis.y0} L${at} ${axis.y0 + S1}`))
    .join(' ');
  return (
    <>
      <Hatches id={id} count={SERIES.length} />
      <path className={styles.axis} d={`${line(axis)} ${line(base)} ${tickMarks}`} />
      {bars.map(({group, series, rect, value}) => {
        const hatched = single?.[group]?.[series] ?? false;
        return (
          <g key={`${group}-${series}`} data-bar={`${group}-${series}`}>
            <rect
              className={`${SERIES[series]} ${hatched ? styles.single : styles.bar}`}
              style={hatched ? {fill: `url(#${id}-hatch-${series})`} : undefined}
              x={rect.x}
              y={rect.y}
              width={rect.w}
              height={rect.h}
            />
            <g data-value="">
              <Text x={value.x} y={value.y} lines={value.text} kind="label" anchor={value.anchor} />
            </g>
          </g>
        );
      })}
      {ticks.map(({label}) => (
        <Text key={label.text} x={label.x} y={label.y} lines={label.text} anchor={label.anchor} />
      ))}
      {groupLabels.map((l) => (
        <Text key={l.text} x={l.x} y={l.y} lines={l.text} kind="label" anchor={l.anchor} />
      ))}
      <Text x={axisLabel.x} y={axisLabel.y} lines={axisLabel.text} anchor={axisLabel.anchor} />
    </>
  );
}

/** A legend swatch: a bar in a series' colour, or a hatched one for the single-run key. */
function Swatch({id, series, hatched}: {id: string; series: number; hatched?: boolean}): React.JSX.Element {
  const cls = SERIES[series];
  return (
    <svg className={frame.swatch} viewBox={`0 0 ${SW_W} ${SW_H}`} width={SW_W} height={SW_H} aria-hidden="true">
      {hatched && <Hatches id={id} count={series + 1} />}
      <rect
        className={`${cls} ${hatched ? styles.single : styles.bar}`}
        style={hatched ? {fill: `url(#${id}-hatch-${series})`} : undefined}
        x={(SW_W - 22) / 2}
        y={SW_H / 2 - 6}
        width={22}
        height={12}
      />
    </svg>
  );
}

/**
 * Grouped bars from zero, one colour per series, each bar labelled with its value. A value flagged
 * in `single` is drawn hatched and outlined, and the legend keys it.
 */
export default function GroupedBars({name, groups, series, values, single, measure, unit, decimals = 2, caption}: Props): React.JSX.Element {
  const id = useId();
  const captionId = `${id}-caption`;
  if (series.length > SERIES.length) throw new Error(`GroupedBars draws at most ${SERIES.length} series`);
  const data: ChartData = {groups, series, values, decimals};
  const axisLabel = `${measure}, ${unit} · higher is better`;
  const {layout: wide, vertical} = landscape(data, WIDE, axisLabel);
  const narrow = rows(data, NARROW, axisLabel);
  const anySingle = single?.some((row) => row.some(Boolean)) ?? false;
  const viewBox = (l: Layout) => `0 0 ${l.width} ${Math.ceil(l.height)}`;
  return (
    <figure className={frame.figure}>
      <svg className={frame.wide} viewBox={viewBox(wide)} style={atMostOneToOne(viewBox(wide))} role="img" aria-label={name} aria-describedby={captionId}>
        <Drawing id={`${id}w`} layout={wide} single={single} vertical={vertical} />
      </svg>
      <svg className={frame.narrow} viewBox={viewBox(narrow)} style={atMostOneToOne(viewBox(narrow))} role="img" aria-label={name} aria-describedby={captionId}>
        <Drawing id={`${id}n`} layout={narrow} single={single} vertical={false} />
      </svg>
      <ul className={frame.legend} aria-label="Legend">
        {series.map((s, i) => (
          <li key={s}>
            <Swatch id={`${id}k${i}`} series={i} />
            <span>{s}</span>
          </li>
        ))}
        {anySingle && (
          <li>
            <Swatch id={`${id}k`} series={0} hatched />
            <span>single run</span>
          </li>
        )}
      </ul>
      <figcaption id={captionId} className={frame.caption}>
        {caption}
      </figcaption>
    </figure>
  );
}
