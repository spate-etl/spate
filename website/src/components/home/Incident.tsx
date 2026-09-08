import clsx from 'clsx';
import React, {useEffect, useReducer} from 'react';

import {DURATION, EVENTS, INITIAL, phaseAt, playback} from './incidentPlayback';

type Point = [number, number];

/** A path records ride, and the point a fraction of its length along it. */
type Curve = {d: string; at: (f: number) => Point};

function line(from: Point, to: Point): Curve {
  return {
    d: `M ${from[0]} ${from[1]} L ${to[0]} ${to[1]}`,
    at: (f) => [from[0] + (to[0] - from[0]) * f, from[1] + (to[1] - from[1]) * f],
  };
}

/** Flattened to chords, which is how `offset-distance` measures the same curve. */
function cubic(from: Point, c1: Point, c2: Point, to: Point): Curve {
  const steps = 512;
  const pts: Point[] = [];
  const run: number[] = [0];
  for (let i = 0; i <= steps; i += 1) {
    const t = i / steps;
    const u = 1 - t;
    pts.push([
      u * u * u * from[0] + 3 * u * u * t * c1[0] + 3 * u * t * t * c2[0] + t * t * t * to[0],
      u * u * u * from[1] + 3 * u * u * t * c1[1] + 3 * u * t * t * c2[1] + t * t * t * to[1],
    ]);
    if (i > 0) run.push(run[i - 1] + Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]));
  }
  const total = run[steps];
  return {
    d: `M ${from[0]} ${from[1]} C ${c1[0]} ${c1[1]} ${c2[0]} ${c2[1]} ${to[0]} ${to[1]}`,
    at: (f) => {
      const want = Math.min(Math.max(f, 0), 1) * total;
      let i = 1;
      while (i < steps && run[i] < want) i += 1;
      const k = (want - run[i - 1]) / (run[i] - run[i - 1]);
      return [pts[i - 1][0] + (pts[i][0] - pts[i - 1][0]) * k, pts[i - 1][1] + (pts[i][1] - pts[i - 1][1]) * k];
    },
  };
}

// The illustrative sequence lasts twelve seconds.
const W = 360;
const H = 270;
const CYCLE = DURATION;
const SPEED = 2; // seconds from the source to the sink
const L = 292; // the data path, in user units

const FWD = line([34, 120], [326, 120]);
const RET = cubic([326, 128], [326, 176], [34, 176], [34, 128]);

/** Where along the data path each part of the loop sits. */
const at = (x: number) => (x - 34) / L;
const AT = {fn: at(180), queue: at(262)};
const travel = (f: number) => f * SPEED;

const EMIT = Array.from({length: 15}, (_, i) => i * 0.6); // one record every 600 ms
const STALL: [number, number] = [3, 5.5]; // the sink stalls
const TERM = 9; // SIGTERM
const BAD = 10; // the record that fails to decode
const BAD_T = EMIT[BAD] + travel(AT.fn); // it reaches the transform at 7 s
const BOUND = 4; // the framework's queue bound

type Stops = Array<[number, number]>;
type Tracks = {offset?: Stops; opacity?: Stops; dash?: Stops; drop?: Stops};

/** Each track's CSS property, its React style key, and how a value is written. */
const PROP: Record<keyof Tracks, [string, string, (v: number) => string]> = {
  offset: ['offset-distance', 'offsetDistance', (v) => `${(v * 100).toFixed(2)}%`],
  opacity: ['opacity', 'opacity', (v) => v.toFixed(3)],
  dash: ['stroke-dashoffset', 'strokeDashoffset', (v) => v.toFixed(2)],
  drop: ['translate', 'translate', (v) => `0 ${v.toFixed(1)}px`],
};

/**
 * The value a track holds at `t`. Stops must run in non-decreasing time order,
 * which is the order the walk assumes; the track holds its first value before
 * the first stop and its last after the last.
 */
function valueAt(stops: Stops, t: number): number {
  for (let i = 1; i < stops.length; i += 1) {
    if (stops[i][0] < stops[i - 1][0]) {
      throw new Error(`incident stops run backwards at ${stops[i][0]} s`);
    }
  }
  if (t <= stops[0][0]) return stops[0][1];
  for (let i = 1; i < stops.length; i += 1) {
    if (t <= stops[i][0]) {
      const [t0, v0] = stops[i - 1];
      const [t1, v1] = stops[i];
      return t1 === t0 ? v1 : v0 + ((v1 - v0) * (t - t0)) / (t1 - t0);
    }
  }
  return stops[stops.length - 1][1];
}

/** Fade from the initial value to the final value and hold it. */
function dims(when: number, from: number, to: number): Stops {
  return [
    [0, from],
    [when, from],
    [Math.min(when + 0.5, CYCLE - 0.05), to],
    [CYCLE - 0.01, to],
    [CYCLE, to],
  ];
}

// The framework's loop, simulated once: a bounded queue, a source that pauses at
// the bound and resumes under hysteresis, and one acknowledgement per record.
type Rec = {e: number; q: number; depart: number; arrive: number};

function simulate() {
  const recs: Rec[] = [];
  const pauses: Array<[number, number]> = [];
  const depth = (t: number) => recs.filter((r) => r.q <= t && t < r.depart).length;
  let bad = {e: EMIT[BAD], fnT: EMIT[BAD] + travel(AT.fn)};
  let sinkFree = 0;
  EMIT.forEach((start, i) => {
    let e = start;
    for (const [a, b] of pauses) if (e >= a && e < b) e = b; // a paused source emits nothing
    if (i === BAD) {
      bad = {e, fnT: e + travel(AT.fn)};
      return;
    }
    const q = e + travel(AT.queue);
    let depart = Math.max(q, sinkFree);
    if (depart >= STALL[0] && depart < STALL[1]) depart = STALL[1];
    depart = Math.max(depart, sinkFree);
    sinkFree = depart + 0.2;
    recs.push({e, q, depart, arrive: depart + travel(1 - AT.queue)});
    if (depth(q) >= BOUND && !pauses.some(([a, b]) => q >= a && q < b)) {
      // the bound: pause the lanes, keep polling, resume under hysteresis
      const departs = recs
        .filter((r) => r.q <= q && q < r.depart)
        .map((r) => r.depart)
        .sort((a, b) => a - b);
      pauses.push([q, departs[BOUND - 3] + 0.1]);
    }
  });
  const acks = recs.map((r) => ({start: r.arrive + 0.1, end: r.arrive + 1.1}));
  return {recs, bad, pauses, acks, exit: Math.max(...acks.map((a) => a.end)) + 0.2};
}

const SIM = simulate();

if (Math.abs(SIM.bad.fnT - BAD_T) > 1e-9) {
  throw new Error(`the bad record reaches the transform at ${SIM.bad.fnT} s, expected ${BAD_T} s`);
}

type Piece = {still: React.ReactNode};

/** Evaluate the Spate illustration at the requested time. */
function build(time: number): Piece[] {
  let uid = 0;
  const key = () => `i${uid++}`;

  const el = (tag: string, props: Record<string, unknown>, kids?: React.ReactNode): Piece => ({
    still: React.createElement(tag, {key: key(), ...props}, kids),
  });

  const entriesOf = (tracks: Tracks) => Object.entries(tracks) as Array<[keyof Tracks, Stops]>;

  const frozen = (tracks: Tracks) => {
    const style: Record<string, string> = {};
    for (const [k, s] of entriesOf(tracks)) style[PROP[k][1]] = PROP[k][2](valueAt(s, time));
    return style as React.CSSProperties;
  };

  const tracked = (
    tag: string,
    props: Record<string, unknown>,
    tracks: Tracks,
    kids?: React.ReactNode,
  ): Piece => {
    const id = key();
    const base = (props.style ?? {}) as React.CSSProperties;
    const cls = props.className as string | undefined;
    return {
      still: React.createElement(
        tag,
        {...props, key: `${id}s`, className: clsx(cls, 'incident__still'), style: {...base, ...frozen(tracks)}},
        kids,
      ),
    };
  };

  /** Place a record on its path at the requested time. */
  const mover = (curve: Curve, className: string, r: number, stops: Array<[number, number, number]>): Piece => {
    const offset: Stops = stops.map(([t, f]) => [t, f]);
    const opacity: Stops = stops.map(([t, , o]) => [t, o]);
    const id = key();
    const [cx, cy] = curve.at(valueAt(offset, time));
    return {
      still: (
        <circle
          key={`${id}s`}
          className={clsx(className, 'incident__still')}
          r={r}
          cx={Number(cx.toFixed(2))}
          cy={Number(cy.toFixed(2))}
          style={{opacity: valueAt(opacity, time)}}
        />
      ),
    };
  };

  const fade = (on: number, off: number): Stops => [
    [0, 0],
    [on - 0.01, 0],
    [on, 1],
    [off, 1],
    [off + 0.3, 0],
  ];

  const endpoints = (): Piece[] => [
    el('circle', {cx: 26, cy: 120, r: 8, className: 'incident__node'}),
    el('circle', {cx: 334, cy: 120, r: 8, className: 'incident__node'}),
    el('text', {x: 26, y: 178, textAnchor: 'middle'}, 'source'),
    el('text', {x: 334, y: 178, textAnchor: 'middle'}, 'sink'),
  ];

  const fnPill = (opacity = 1) =>
    el(
      'g',
      {opacity},
      <>
        <rect x={138} y={106} width={84} height={28} rx={8} className="incident__mine-fill" />
        <text x={180} y={123.5} textAnchor="middle" className="incident__mono incident__on-accent">
          your fn
        </text>
      </>,
    );

  /** Dim the source node while intake is paused. */
  const stopped = (opacity: Stops) =>
    tracked('circle', {cx: 26, cy: 120, r: 8, className: 'incident__paused'}, {opacity});

  const spateLoop = (): Piece[] => {
    const {recs, bad, pauses, acks, exit} = SIM;
    const out: Piece[] = [];
    out.push(
      tracked(
        'rect',
        {x: 62, y: 28, width: 236, height: 196, rx: 6, className: 'incident__mine', strokeWidth: 1},
        {opacity: dims(exit, 0.8, 0.2)},
      ),
      el('text', {x: 74, y: 50, className: 'incident__mine-t'}, 'your process'),
      tracked(
        'rect',
        {x: 76, y: 62, width: 208, height: 148, rx: 5, className: 'incident__fw'},
        {opacity: dims(exit, 1, 0.25)},
      ),
      el('text', {x: 88, y: 82, className: 'incident__fw-t'}, 'Spate'),
      el('path', {d: RET.d, className: 'incident__ret'}),
      el('path', {d: FWD.d, className: 'incident__path'}),
      // Source event polling continues while the lanes are paused.
      el('text', {x: 108, y: 104, textAnchor: 'middle', className: 'incident__mono'}, 'poll'),
      {
        still: <circle key={key()} cx={142} cy={98} r={2.5} className="incident__tick"
          style={{opacity: time < exit ? 0.6 + 0.4 * Math.sin(time * Math.PI * 8) : 0}} />,
      },
    );
    out.push(
      mover(FWD, 'incident__rec', 4, [
        [0, 0, 0],
        [bad.e, 0, 0],
        [bad.e + 0.01, 0, 1],
        [bad.fnT, AT.fn, 1],
        [bad.fnT + 0.01, AT.fn, 0],
        [CYCLE, AT.fn, 0],
      ]),
    );
    const skipped = tracked('circle', {cx: 180, cy: 120, r: 4, className: 'incident__skip'}, {
      opacity: [
        [0, 0],
        [bad.fnT - 0.01, 0],
        [bad.fnT, 1],
        [bad.fnT + 0.5, 1],
        [bad.fnT + 0.7, 0],
      ],
      drop: [
        [0, 0],
        [bad.fnT, 0],
        [bad.fnT + 0.5, 22],
      ],
    });
    recs.forEach((r) => {
      const stops: Array<[number, number, number]> = [
        [0, 0, 0],
        [r.e, 0, 0],
        [r.e + 0.01, 0, 1],
        [r.q, AT.queue, 1],
      ];
      // waiting inside the queue, where the bars show it instead
      if (r.depart - r.q > 0.05) {
        stops.push([r.q + 0.01, AT.queue, 0], [r.depart, AT.queue, 0], [r.depart + 0.01, AT.queue, 1]);
      }
      stops.push([r.arrive, 1, 1], [r.arrive + 0.05, 1, 0], [CYCLE, 1, 0]);
      out.push(mover(FWD, 'incident__rec', 4, stops));
    });
    out.push(skipped, fnPill());
    // the bounded queue and its bars, from the same simulation
    out.push(
      el('rect', {x: 250, y: 100, width: 22, height: 40, rx: 3, className: 'incident__fw'}),
      el('text', {x: 278, y: 92, textAnchor: 'end', className: 'incident__mono incident__fw-t'}, 'queue'),
    );
    const edges = [...new Set(recs.flatMap((r) => [r.q, r.depart]))].sort((a, b) => a - b);
    const depth = (t: number) => recs.filter((r) => r.q <= t && t < r.depart).length;
    for (let k = 0; k < BOUND; k += 1) {
      const bar: Stops = [[0, 0]];
      edges.forEach((t) => {
        bar.push([t - 0.001, depth(t - 0.001) > k ? 1 : 0], [t, depth(t) > k ? 1 : 0]);
      });
      bar.push([CYCLE, 0]);
      out.push(
        tracked('rect', {x: 253, y: 133 - k * 9, width: 16, height: 7, className: 'incident__rec'}, {opacity: bar}),
      );
    }
    const paused: Stops = [
      [0, 0],
      ...pauses.flatMap(
        ([a, b]) =>
          [
            [a - 0.01, 0],
            [a, 1],
            [b, 1],
            [b + 0.1, 0],
          ] as Stops,
      ),
      [TERM - 0.01, 0],
      [TERM, 1],
      [CYCLE - 0.01, 1],
      [CYCLE, 1],
    ];
    // acknowledgements return, and the commit gauge advances only behind them
    const C = 2 * Math.PI * 10;
    acks.forEach((a) =>
      out.push(
        mover(RET, 'incident__ack', 2.5, [
          [0, 0, 0],
          [a.start, 0, 0],
          [a.start + 0.01, 0, 1],
          [a.end, 1, 1],
          [a.end + 0.01, 1, 0],
          [CYCLE, 1, 0],
        ]),
      ),
    );
    const level: Stops = [[0, C]];
    acks
      .map((a) => a.end)
      .sort((a, b) => a - b)
      .forEach((t, n) => level.push([t - 0.001, C * (1 - n / acks.length)], [t, C * (1 - (n + 1) / acks.length)]));
    level.push([CYCLE, 0]);
    out.push(
      el('circle', {cx: 180, cy: 184, r: 10, className: 'incident__track', strokeWidth: 3}),
      tracked(
        'circle',
        {
          cx: 180,
          cy: 184,
          r: 10,
          className: 'incident__fw',
          strokeWidth: 3,
          transform: 'rotate(-90 180 184)',
          strokeDasharray: C.toFixed(2),
        },
        {dash: level},
      ),
      el('text', {x: 160, y: 190, textAnchor: 'end', className: 'incident__mono incident__fw-t'}, 'commit'),
    );
    // SIGTERM: the drain deadline runs around the sink and is polled throughout;
    // the process exits before it lands
    const D = 2 * Math.PI * 14;
    out.push(
      tracked(
        'circle',
        {
          cx: 334,
          cy: 120,
          r: 14,
          className: 'incident__fw',
          transform: 'rotate(-90 334 120)',
          strokeDasharray: D.toFixed(2),
        },
        {
          opacity: [
            [0, 0],
            [TERM - 0.01, 0],
            [TERM, 1],
            [exit, 1],
            [Math.min(exit + 0.3, CYCLE), 0],
          ],
          dash: [
            [0, 0],
            [TERM, 0],
            [CYCLE, D],
          ],
        },
      ),
      tracked('text', {x: 334, y: 94, textAnchor: 'middle', className: 'incident__mono incident__fw-t'},
        {opacity: fade(TERM, exit)}, 'drain'),
      ...endpoints(),
      stopped(paused),
    );
    const annotation = time >= exit ? 'shutdown complete'
      : time >= TERM ? 'draining'
      : time >= bad.fnT ? (time >= bad.fnT + 0.5 ? 'skipped 1' : 'skipping record')
      : pauses.some(([start, end]) => time >= start && time < end) ? 'lanes paused, polling'
      : time >= STALL[0] && time < STALL[1] ? 'sink stalled'
      : 'records flowing';
    out.push(el('text', {x: 180, y: 250, textAnchor: 'middle', className: 'incident__fw-t'}, annotation));
    return out;
  };

  return spateLoop();
}

export default function Incident(): React.JSX.Element {
  const [state, dispatch] = useReducer(playback, INITIAL);
  useEffect(() => {
    const media = window.matchMedia('(prefers-reduced-motion: reduce)');
    const update = () => dispatch({type: 'motion', reduced: media.matches});
    update();
    media.addEventListener('change', update);
    return () => media.removeEventListener('change', update);
  }, []);
  useEffect(() => {
    if (!state.playing) return undefined;
    const start = performance.now() - state.time * 1000;
    let frame: number;
    const tick = (now: number) => {
      const time = (now - start) / 1000;
      dispatch({type: 'tick', time});
      if (time < DURATION) frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [state.playing, state.run]);
  const phase = phaseAt(state.time);
  return (
    <div className="incident">
      <figure className="incident__panel">
        <svg viewBox={`0 0 ${W} ${H}`} role="img"
          aria-label={`Illustrative Spate pipeline: ${phase.label}. ${phase.description}`}>
          <g aria-hidden="true">{build(state.time).map((piece) => piece.still)}</g>
        </svg>
        <figcaption>
          <b>Spate · illustrative delivery sequence</b>
          Records travel to the sink; acknowledgements return along the dashed path. The bad-record scenario uses the explicit Skip policy.
        </figcaption>
      </figure>
      <div className="incident__inspection">
        <div className="incident__controls" role="group" aria-label="Inspect an incident">
          {EVENTS.map((event) => (
            <button key={event.id} type="button" className="home-btn home-btn--ghost"
              aria-pressed={phase.id === event.id} onClick={() => dispatch({type: 'select', time: event.time})}>
              {event.label}
            </button>
          ))}
        </div>
        <div className="incident__explanation" role="status" aria-live="polite" aria-atomic="true">
          <h3>{phase.label}</h3>
          <p>{phase.description}</p>
        </div>
        <div className="incident__controls" role="group" aria-label="Playback">
          <button type="button" className="home-btn home-btn--primary" disabled={state.reduced}
            onClick={() => dispatch({type: 'toggle'})}>{state.playing ? 'Pause' : 'Play'}</button>
          <button type="button" className="home-btn home-btn--ghost" disabled={state.reduced}
            onClick={() => dispatch({type: 'replay'})}>Replay</button>
        </div>
        <p className="incident__hint">{state.reduced
          ? 'Reduced motion is enabled. Select an event to inspect its still frame.'
          : 'Play continues from this frame. Replay starts from the beginning and runs once.'}</p>
      </div>
    </div>
  );
}
