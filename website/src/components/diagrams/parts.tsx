/**
 * The drawing vocabulary the concept figures share: one type and spacing scale, the four line
 * kinds, the marks, the legend and the two figure frames. Every coordinate in a figure derives from
 * the constants here, so spacing reads the same across the set.
 */
import React, {useId} from 'react';

import {DESCENT, GAP, LINE, MID, S1, S2, TITLE_LINE, centredBaseline, textW, titleW} from './scale';
import styles from './styles.module.css';

export * from './scale';

/** Inside a box, between its edge and its label. */
export const CHIP_X = 6;
export const CHIP_H = 24;
/**
 * A line with an arrowhead stops this far short of what it points at. The head's tip runs an eighth
 * of the head's length past the line's end (`refX` in `Markers`), so a 7-unit head stops 0.125
 * short and the rail's 12-unit head reaches 0.5 into its target.
 */
export const ARROW_GAP = 1;
/** An arrowhead's length and width on a records, acks or control line. */
export const ARROW = 7;
/** The commit rail's arrowhead, drawn larger to clear the `RAIL` stroke. */
const RAIL_ARROW = 12;
/** Stroke width of a records or acks line, which a label beside it clears. */
export const STROKE = 2;
/** Stroke width of the commit rail. */
export const RAIL = 6;
/** Lane fan-out from a source node. */
export const LANE_SPREAD = 6;
/** An ack line's dash and the space after it. */
const ACK_DASH = 7;
const ACK_SPACE = 4;
/** A control line's dot pitch; zero-length dashes with round caps draw the dots. */
const CONTROL_PITCH = 4;

export const R_NODE = 10;
export const R_STEP = 8.5;
export const R_JOIN = 5;

const CELL = 8;
const CELL_PITCH = 9;
/** A shard queue's thickness across the flow. */
export const QUEUE_T = 14;
/** A four-slot shard queue's length. */
export const QUEUE_W = 3 * CELL_PITCH + CELL;
/** The space between two regions a queue crosses: the queue, `S2` clear of each edge. */
export const QUEUE_GAP = QUEUE_W + 2 * S2;
/** How far a stacked region's copies reach above and right of it. */
export const STACK = 2 * S1;

/**
 * Sets a text line to `width`, the width the layout gave it. Some renderers round glyph advances:
 * Linux Chromium sets this face at 7.04 units per 12-unit character against `CHAR`'s 7.4, which
 * would move every edge sized to text. Spacing absorbs the difference; glyphs keep their shape.
 */
function pinnedWidth(width: number): {textLength: number; lengthAdjust: 'spacing'} {
  return {textLength: width, lengthAdjust: 'spacing'};
}

/** A box wide enough for the longest of `label`'s lines. */
export function chipW(label: string | string[]): number {
  const lines = typeof label === 'string' ? [label] : label;
  return Math.max(...lines.map(textW)) + 2 * CHIP_X;
}

/** The bottom of `n` lines whose first baseline is `baseline`. */
export function linesBottom(baseline: number, n = 1): number {
  return baseline + (n - 1) * LINE + DESCENT;
}

// ---- lines ----

export type Point = [number, number];

function pathData(points: Point[]): string {
  return points.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x} ${y}`).join(' ');
}

type Kind = 'data' | 'ack' | 'control' | 'mark';

/** The dash pattern of each line kind, shared by the drawings and the legend swatches. */
const DASH: Record<Kind, string | undefined> = {
  data: undefined,
  ack: `${ACK_DASH} ${ACK_SPACE}`,
  control: `0 ${CONTROL_PITCH}`,
  mark: undefined,
};

const HEAD: Record<Kind, string> = {
  data: styles.dataHead,
  ack: styles.ackHead,
  control: styles.controlHead,
  mark: styles.markHead,
};

/** One arrowhead per line kind, ids prefixed with the figure's own `id`. */
export function Markers({id}: {id: string}): React.JSX.Element {
  return (
    <defs>
      {(Object.keys(HEAD) as Kind[]).map((kind) => {
        const size = kind === 'mark' ? RAIL_ARROW : ARROW;
        return (
          <marker
            key={kind}
            id={`${id}-${kind}`}
            viewBox="0 0 8 8"
            refX="7"
            refY="4"
            markerWidth={size}
            markerHeight={size}
            markerUnits="userSpaceOnUse"
            orient="auto-start-reverse">
            <path className={HEAD[kind]} d="M0 0 L8 4 L0 8 Z" />
          </marker>
        );
      })}
    </defs>
  );
}

/** `points` with the last segment shortened by `by`, so the rail's core stops short of its head. */
function trimEnd(points: Point[], by: number): Point[] {
  const [x0, y0] = points[points.length - 2];
  const [x1, y1] = points[points.length - 1];
  const len = Math.hypot(x1 - x0, y1 - y0);
  const k = Math.max(0, len - by) / len;
  return [...points.slice(0, -1), [x0 + (x1 - x0) * k, y0 + (y1 - y0) * k]];
}

type EdgeProps = {id: string; points: Point[]; kind?: Kind; head?: boolean; link?: boolean};

/**
 * A connection. `data` carries records, `ack` acknowledgements on their way to the checkpointer,
 * `control` pause, resume and driver signals, `mark` the watermark and the commit. `link` marks an
 * arrow between two stages of a run.
 */
export function Edge({id, points, kind = 'data', head = true, link}: EdgeProps): React.JSX.Element {
  const marker = head ? `url(#${id}-${kind})` : undefined;
  if (kind === 'mark') {
    return (
      <g>
        <path className={styles.markOuter} d={pathData(points)} markerEnd={marker} />
        <path className={styles.markInner} d={pathData(head ? trimEnd(points, 10) : points)} />
      </g>
    );
  }
  return (
    <path
      className={styles[kind]}
      d={pathData(points)}
      strokeDasharray={DASH[kind]}
      markerEnd={marker}
      data-link={link ? '' : undefined}
    />
  );
}

/** The arrows of a run whose boxes are given in order, each stopping `ARROW_GAP` short of the next box. */
function RunLinks({id, boxes, vertical}: {id: string; boxes: Box[]; vertical?: boolean}): React.JSX.Element {
  return (
    <>
      {boxes.slice(1).map((b, i) => {
        const a = boxes[i];
        const points: Point[] = vertical
          ? [
              [a.x + a.w / 2, a.y + a.h / 2],
              [b.x + b.w / 2, b.y - b.h / 2 - ARROW_GAP],
            ]
          : [
              [a.x + a.w, a.y],
              [b.x - ARROW_GAP, b.y],
            ];
        return <Edge key={i} id={id} points={points} link />;
      })}
    </>
  );
}

// ---- boxes and marks ----

/** A box: left edge `x`, centre line `y`. */
type Box = {x: number; y: number; w: number; h: number};

/** Lays out boxes left to right from `x` on centre line `y`, `GAP` apart. */
export function rowOf(labels: string[], x: number, y: number): Box[] {
  const out: Box[] = [];
  let left = x;
  for (const label of labels) {
    const w = chipW(label);
    out.push({x: left, y, w, h: CHIP_H});
    left += w + GAP;
  }
  return out;
}

/** Lays out boxes of width `w` top to bottom from centre line `y`, `GAP` apart. */
export function columnOf(count: number, x: number, y: number, w: number): Box[] {
  return Array.from({length: count}, (_, i) => ({x, y: y + i * (CHIP_H + GAP), w, h: CHIP_H}));
}

type ChipProps = {
  x: number;
  y: number;
  label: string | string[];
  w?: number;
  h?: number;
  head?: boolean;
};

/**
 * A stage: a rounded box, `y` being its centre line, with its label lines centred in it. With
 * `head` it is a participant header in a sequence figure, a thread, so it takes the region outline.
 */
export function Chip({x, y, label, w = chipW(label), h = CHIP_H, head}: ChipProps): React.JSX.Element {
  const lines = typeof label === 'string' ? [label] : label;
  const first = centredBaseline(y, lines.length);
  return (
    <g data-chip="">
      <rect className={head ? styles.head : styles.chip} x={x} y={y - h / 2} width={w} height={h} rx="4" />
      {lines.map((line, i) => (
        <text
          key={line}
          className={styles.chipLabel}
          x={x + w / 2}
          y={first + i * LINE}
          textAnchor="middle"
          {...pinnedWidth(textW(line))}>
          {line}
        </text>
      ))}
    </g>
  );
}

/** The stages of one run: one centre line, one gap between boxes, one arrow length. */
export function Run({id, labels, boxes, vertical}: {id: string; labels: string[]; boxes: Box[]; vertical?: boolean}): React.JSX.Element {
  return (
    <g data-run="">
      {boxes.map((b, i) => (
        <Chip key={labels[i]} x={b.x} y={b.y} w={b.w} label={labels[i]} />
      ))}
      <RunLinks id={id} boxes={boxes} vertical={vertical} />
    </g>
  );
}

type RegionProps = {x: number; y: number; w: number; h: number; stacked?: boolean};

/**
 * A thread or runtime boundary. `stacked` draws two copies behind it for "× N", each offset `S1`
 * up and right, so the stack shows along the top and right edges and reaches `STACK` past them.
 */
export function Region({x, y, w, h, stacked}: RegionProps): React.JSX.Element {
  return (
    <g>
      {stacked &&
        [2, 1].map((k) => (
          <rect key={k} className={styles.behind} x={x + k * S1} y={y - k * S1} width={w} height={h} rx="8" />
        ))}
      <rect className={styles.region} x={x} y={y} width={w} height={h} rx="8" data-region="" />
    </g>
  );
}

type TextProps = {
  x: number;
  /** The first line's baseline. */
  y: number;
  lines: string | string[];
  kind?: 'title' | 'note' | 'label';
  anchor?: 'start' | 'middle' | 'end';
};

/**
 * Lines of text `LINE` apart, or `TITLE_LINE` for a title. The lines form one block, marked
 * `data-block` with its kind so the layout tests can find a region's title and its wrapped notes.
 */
export function Text({x, y, lines, kind = 'note', anchor = 'start'}: TextProps): React.JSX.Element {
  const all = typeof lines === 'string' ? [lines] : lines;
  return (
    <g data-block={kind}>
      {all.map((line, i) => (
        <text
          key={line}
          className={styles[kind]}
          x={x}
          y={y + i * (kind === 'title' ? TITLE_LINE : LINE)}
          textAnchor={anchor}
          {...pinnedWidth(kind === 'title' ? titleW(line) : textW(line))}>
          {line}
        </text>
      ))}
    </g>
  );
}

/** A four-slot shard queue centred on `x`, `y`, the first slot holding a chunk. `full` fills them all. */
export function Queue({x, y, full, vertical}: {x: number; y: number; full?: boolean; vertical?: boolean}): React.JSX.Element {
  return (
    <g>
      {[0, 1, 2, 3].map((i) => {
        const along = -QUEUE_W / 2 + i * CELL_PITCH;
        return (
          <rect
            key={i}
            className={full || i === 0 ? styles.cellFull : styles.cell}
            x={vertical ? x - QUEUE_T / 2 : x + along}
            y={vertical ? y + along : y - QUEUE_T / 2}
            width={vertical ? QUEUE_T : CELL}
            height={vertical ? CELL : QUEUE_T}
          />
        );
      })}
    </g>
  );
}

/** A source or destination node. */
export function Node({x, y, r = R_NODE}: {x: number; y: number; r?: number}): React.JSX.Element {
  return <circle className={styles.node} cx={x} cy={y} r={r} />;
}

/** A destination's replica set: three overlapping discs centred on `x`. */
export function Replicas({x, y}: {x: number; y: number}): React.JSX.Element {
  return (
    <g>
      {[-1, 0, 1].map((i) => (
        <circle key={i} className={styles.replica} cx={x + i * R_NODE * 1.1} cy={y} r={R_NODE} />
      ))}
    </g>
  );
}

/** The batch ack the branches' acks join at. */
export function Join({x, y}: {x: number; y: number}): React.JSX.Element {
  return <circle className={styles.join} cx={x} cy={y} r={R_JOIN} />;
}

/** A numbered step: a filled disc, matching the numbered list under or beside the figure. */
export function Step({x, y, n}: {x: number; y: number; n: number}): React.JSX.Element {
  return (
    <g data-callout="">
      <circle className={styles.step} cx={x} cy={y} r={R_STEP} />
      <text className={styles.stepLabel} x={x} y={y + MID} textAnchor="middle">
        {n}
      </text>
    </g>
  );
}

/**
 * A participant's lifeline in a sequence figure: one unbroken line from under the headers to `end`,
 * the figure's last row. Labels keep clear of it; a rail drawn along it covers it.
 */
export function Lifeline({x, end}: {x: number; end: number}): React.JSX.Element {
  return (
    <g data-lifeline={x}>
      <path className={styles.lifeline} d={`M${x} ${HEAD_BOTTOM} L${x} ${end}`} />
    </g>
  );
}

/** A refused send: a cross centred on `x`, `y`. */
export function Reject({x, y}: {x: number; y: number}): React.JSX.Element {
  const r = 5;
  return <path className={styles.reject} d={`M${x - r} ${y - r} L${x + r} ${y + r} M${x + r} ${y - r} L${x - r} ${y + r}`} />;
}

/** A bar on a lifeline: `hold` for a batch the checkpointer holds, `wait` for rows a sink lingers on. */
export function Bar({x, y0, y1, kind}: {x: number; y0: number; y1: number; kind: 'hold' | 'wait'}): React.JSX.Element {
  return <rect className={styles[kind]} x={x - S1} y={y0} width={2 * S1} height={y1 - y0} />;
}

// ---- sequence figures ----

/** The width every sequence figure is drawn at, so a phone column renders it near 1:1. */
export const SEQ_W = 360;
/** A participant header: two label lines in a box. */
const HEAD_H = CHIP_H + LINE;
/** Where lifelines start, under the headers. */
export const HEAD_BOTTOM = S1 + HEAD_H;
/** Between consecutive messages. */
export const ROW = 3 * GAP;

/** Participant headers, each centred on its lifeline at `x`. */
export function Heads({heads}: {heads: {x: number; label: string[]}[]}): React.JSX.Element {
  return (
    <>
      {heads.map(({x, label}) => (
        <Chip key={x} x={x - chipW(label) / 2} y={S1 + HEAD_H / 2} h={HEAD_H} label={label} head />
      ))}
    </>
  );
}

type MessageProps = {
  id: string;
  from: number;
  to: number;
  y: number;
  kind?: Kind;
  label?: string;
  /** The stretch of the line to centre the label over, when the line crosses another lifeline. */
  over?: [number, number];
  step?: number;
};

/**
 * A message from one lifeline to another at height `y`, labelled above. The label clears the
 * arrowhead, which is wider than the stroke. With `step`, the numbered disc sits on the sending
 * lifeline and the line leaves from its edge.
 */
export function Message({id, from, to, y, kind = 'data', label, over, step}: MessageProps): React.JSX.Element {
  const dir = Math.sign(to - from);
  const start = from + (step ? dir * R_STEP : 0);
  const end = to - dir * ARROW_GAP;
  const [a, b] = over ?? [start, end];
  const half = kind === 'mark' ? RAIL_ARROW / 2 : ARROW / 2;
  return (
    <>
      <Edge id={id} kind={kind} points={[[start, y], [end, y]]} />
      {label && <Text x={(a + b) / 2} y={y - half - S1 - DESCENT} lines={label} anchor="middle" />}
      {step && <Step x={from} y={y} n={step} />}
    </>
  );
}

// ---- legend ----

export type LegendEntry =
  | 'data'
  | 'ack'
  | 'control'
  | 'mark'
  | 'region'
  | 'chip'
  | 'queue'
  | 'node'
  | 'replicas'
  | 'source'
  | 'join'
  | 'lifeline'
  | 'reject'
  | 'frame'
  | 'hold'
  | 'wait';

const LEGEND_TEXT: Record<LegendEntry, string> = {
  data: 'records',
  ack: 'acks',
  control: 'control signal',
  mark: 'watermark, commit',
  region: 'thread, runtime',
  chip: 'stage',
  queue: 'bounded queue',
  node: 'source, destination',
  replicas: 'destination replicas',
  source: 'source',
  join: 'batch ack',
  lifeline: 'participant over time',
  reject: 'send refused',
  frame: 'repeats while paused',
  hold: 'batch held',
  wait: 'rows wait in the sink',
};

/** A line swatch's length: three ack dashes and the two spaces between them. */
const SW_LINE = 3 * ACK_DASH + 2 * ACK_SPACE;
/** A line swatch's inset from each end of its box. */
const SW_INSET = 2;
/**
 * Every swatch is drawn in one box `SW_W` wide and the legend's line height tall, so items line up
 * whatever mark they show and a lifeline swatch runs the full height of its row.
 */
const SW_W = SW_LINE + 2 * SW_INSET;
const SW_H = 20;
const SW_MID = SW_H / 2;

function Swatch({entry}: {entry: LegendEntry}): React.JSX.Element {
  // A line swatch draws its kind's own dash pattern, at a length that ends the ack line on its
  // third dash.
  const line = (kind: Kind, className = styles[kind]) => (
    <path className={className} d={`M${SW_INSET} ${SW_MID} L${SW_INSET + SW_LINE} ${SW_MID}`} strokeDasharray={DASH[kind]} />
  );
  // A box mark `w` by `h`, centred in the swatch.
  const box = (className: string, w: number, h: number, rx = 3) => (
    <rect className={className} x={(SW_W - w) / 2} y={SW_MID - h / 2} width={w} height={h} rx={rx} />
  );
  const marks: Record<LegendEntry, React.ReactNode> = {
    data: line('data'),
    ack: line('ack'),
    control: line('control'),
    mark: (
      <>
        {line('mark', styles.markOuter)}
        {line('mark', styles.markInner)}
      </>
    ),
    region: box(styles.region, 18, 12),
    chip: box(styles.chip, 22, 10),
    queue: (
      <g>
        {[0, 1, 2, 3].map((i) => (
          <rect
            key={i}
            className={i === 0 ? styles.cellFull : styles.cell}
            x={(SW_W - 17.5) / 2 + i * 4.5}
            y={SW_MID - 5}
            width={4}
            height={10}
          />
        ))}
      </g>
    ),
    node: <circle className={styles.node} cx={SW_W / 2} cy={SW_MID} r={6} />,
    replicas: (
      <g>
        {[-1, 0, 1].map((i) => (
          <circle key={i} className={styles.replica} cx={SW_W / 2 + i * 6.6} cy={SW_MID} r={6} />
        ))}
      </g>
    ),
    source: <circle className={styles.node} cx={SW_W / 2} cy={SW_MID} r={6} />,
    join: <circle className={styles.join} cx={SW_W / 2} cy={SW_MID} r={4} />,
    lifeline: <path className={styles.lifeline} d={`M${SW_W / 2} 0 L${SW_W / 2} ${SW_H}`} />,
    reject: (
      <path
        className={styles.reject}
        d={`M${SW_W / 2 - 4} ${SW_MID - 4} L${SW_W / 2 + 4} ${SW_MID + 4} M${SW_W / 2 + 4} ${SW_MID - 4} L${SW_W / 2 - 4} ${SW_MID + 4}`}
      />
    ),
    frame: (
      <>
        {box(styles.frame, 22, 12, 2)}
        <rect className={styles.frameTag} x={(SW_W - 22) / 2} y={SW_MID - 6} width={10} height={5} rx="1" />
      </>
    ),
    hold: box(styles.hold, 6, 12, 0),
    wait: box(styles.wait, 6, 12, 0),
  };
  return (
    <svg className={styles.swatch} viewBox={`0 0 ${SW_W} ${SW_H}`} width={SW_W} height={SW_H} aria-hidden="true">
      {marks[entry]}
    </svg>
  );
}

/** The key to a figure's marks, in HTML so it wraps at phone width. List only what the figure draws. */
export function Legend({entries}: {entries: LegendEntry[]}): React.JSX.Element {
  return (
    <ul className={styles.legend} aria-label="Legend">
      {entries.map((entry) => (
        <li key={entry}>
          <Swatch entry={entry} />
          <span>{LEGEND_TEXT[entry]}</span>
        </li>
      ))}
    </ul>
  );
}

// ---- figure frames ----

/**
 * A drawing's width cap: one CSS pixel per viewBox unit. A drawing shrinks only in a column
 * narrower than its viewBox, so 12-unit text renders at 12px at most and every drawing on a page
 * shares one scale.
 */
export function atMostOneToOne(viewBox: string): React.CSSProperties {
  return {maxWidth: `${viewBox.split(' ')[2]}px`};
}

type OverviewProps = {
  name: string;
  wide: {viewBox: string; draw: (id: string) => React.ReactNode};
  narrow: {viewBox: string; draw: (id: string) => React.ReactNode};
  legend: LegendEntry[];
  /** The numbered key to the portrait layout's step discs, shown only with that layout. */
  steps?: React.ReactNode[];
  caption: React.ReactNode;
};

/**
 * An overview figure: a landscape and a portrait drawing, of which the figure's own width shows
 * one, then the legend and the caption. Both drawings take the short name and the caption as
 * their description.
 */
export function OverviewFigure({name, wide, narrow, legend, steps, caption}: OverviewProps): React.JSX.Element {
  const id = useId();
  const captionId = `${id}-caption`;
  return (
    <figure className={styles.figure}>
      <svg className={styles.wide} viewBox={wide.viewBox} style={atMostOneToOne(wide.viewBox)} role="img" aria-label={name} aria-describedby={captionId}>
        <Markers id={`${id}w`} />
        {wide.draw(`${id}w`)}
      </svg>
      <svg className={styles.narrow} viewBox={narrow.viewBox} style={atMostOneToOne(narrow.viewBox)} role="img" aria-label={name} aria-describedby={captionId}>
        <Markers id={`${id}n`} />
        {narrow.draw(`${id}n`)}
      </svg>
      <Legend entries={legend} />
      {steps && (
        <ol className={`${styles.steps} ${styles.key}`}>
          {steps.map((step, i) => (
            <li key={i}>{step}</li>
          ))}
        </ol>
      )}
      <figcaption id={captionId} className={styles.caption}>
        {caption}
      </figcaption>
    </figure>
  );
}

type SequenceProps = {
  name: string;
  viewBox: string;
  heading: string;
  legend: LegendEntry[];
  draw: (id: string) => React.ReactNode;
  steps: React.ReactNode[];
};

/**
 * A sequence figure: participants' lifelines read top to bottom, with numbered steps. The drawing
 * and its legend sit beside the caption where the column allows, above it otherwise.
 */
export function SequenceFigure({name, viewBox, heading, legend, draw, steps}: SequenceProps): React.JSX.Element {
  const id = useId();
  const captionId = `${id}-caption`;
  return (
    <div className={styles.figure}>
      <figure className={styles.sequence}>
        <div className={styles.drawing}>
          <svg viewBox={viewBox} style={atMostOneToOne(viewBox)} role="img" aria-label={name} aria-describedby={captionId}>
            <Markers id={id} />
            {draw(id)}
          </svg>
          <Legend entries={legend} />
        </div>
        <figcaption id={captionId} className={styles.caption}>
          <p className={styles.heading}>{heading}</p>
          <ol className={styles.steps}>
            {steps.map((step, i) => (
              <li key={i}>{step}</li>
            ))}
          </ol>
        </figcaption>
      </figure>
    </div>
  );
}

export {styles};
