import React from 'react';

import styles from './styles.module.css';

export type Point = [number, number];

export function pathData(points: Point[]): string {
  return points.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x} ${y}`).join(' ');
}

/**
 * The three arrowheads a figure draws with: data, control signal and watermark. Marker ids are
 * document-global, and a page can hold several figures, so each figure passes its own `id` prefix.
 */
export function Markers({id}: {id: string}): React.JSX.Element {
  const marker = (name: string, className: string) => (
    <marker
      id={`${id}-${name}`}
      viewBox="0 0 8 8"
      refX="7"
      refY="4"
      markerWidth="7"
      markerHeight="7"
      markerUnits="userSpaceOnUse"
      orient="auto-start-reverse">
      <path className={className} d="M0 0 L8 4 L0 8 Z" />
    </marker>
  );
  return (
    <defs>
      {marker('data', styles.dataHead)}
      {marker('signal', styles.signalHead)}
      {marker('mark', styles.markHead)}
    </defs>
  );
}

type ChipProps = {x: number; y: number; w: number; label: string | string[]; h?: number; small?: boolean};

/**
 * A stage inside a region: a rounded box with its label centred, `y` being the box's centre line.
 * An array label sets one line per entry, 16 units apart; `small` sets the label at 11px.
 */
export function Chip({x, y, w, label, h = 22, small}: ChipProps): React.JSX.Element {
  const lines = typeof label === 'string' ? [label] : label;
  const first = y + 4 - ((lines.length - 1) * 16) / 2;
  return (
    <g>
      <rect className={styles.chip} x={x} y={y - h / 2} width={w} height={h} rx="4" />
      {lines.map((line, i) => (
        <text
          key={line}
          className={small ? styles.chipLabelSmall : styles.chipLabel}
          x={x + w / 2}
          y={first + i * 16}
          textAnchor="middle">
          {line}
        </text>
      ))}
    </g>
  );
}

type RegionProps = {
  x: number;
  y: number;
  w: number;
  h: number;
  title: string | string[];
  note?: string | string[];
  stacked?: boolean;
};

/**
 * A thread or runtime boundary with its title and note lines at its top left. `stacked` draws two
 * offset copies behind it for "× N".
 */
export function Region({x, y, w, h, title, note = [], stacked}: RegionProps): React.JSX.Element {
  const titles = typeof title === 'string' ? [title] : title;
  const notes = typeof note === 'string' ? [note] : note;
  return (
    <g>
      {stacked && (
        <>
          <rect className={styles.behind} x={x + 12} y={y - 12} width={w} height={h} rx="8" />
          <rect className={styles.behind} x={x + 6} y={y - 6} width={w} height={h} rx="8" />
        </>
      )}
      <rect className={styles.region} x={x} y={y} width={w} height={h} rx="8" />
      {titles.map((line, i) => (
        <text key={line} className={styles.title} x={x + 14} y={y + 22 + i * 18}>
          {line}
        </text>
      ))}
      {notes.map((line, i) => (
        <text key={line} className={styles.note} x={x + 14} y={y + 22 + titles.length * 18 + i * 15}>
          {line}
        </text>
      ))}
    </g>
  );
}

type EdgeProps = {id: string; points: Point[]; kind?: 'data' | 'signal' | 'mark'; head?: boolean};

/** A connection. `data` carries records, `signal` carries control and acks, `mark` the watermark. */
export function Edge({id, points, kind = 'data', head = true}: EdgeProps): React.JSX.Element {
  return (
    <path
      className={styles[kind]}
      d={pathData(points)}
      markerEnd={head ? `url(#${id}-${kind})` : undefined}
    />
  );
}

/** Shard queue cells: four bounded slots, the first one holding a chunk. */
export function Queue({x, y}: {x: number; y: number}): React.JSX.Element {
  return (
    <g>
      {[0, 1, 2, 3].map((i) => (
        <rect
          key={i}
          className={i === 0 ? styles.cellFull : styles.cell}
          x={x + i * 9}
          y={y - 7}
          width="8"
          height="14"
        />
      ))}
    </g>
  );
}

/** A numbered callout on a line, keyed to the list under the portrait layout. */
export function Callout({x, y, n}: {x: number; y: number; n: number}): React.JSX.Element {
  return (
    <g data-callout>
      <circle className={styles.callout} cx={x} cy={y} r="8" />
      <text className={styles.calloutLabel} x={x} y={y + 4} textAnchor="middle">
        {n}
      </text>
    </g>
  );
}

export {styles};
