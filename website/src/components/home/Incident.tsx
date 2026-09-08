import clsx from 'clsx';
import React, {useEffect, useState} from 'react';

import {useOnScreen} from '../motion/useOnScreen';

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

// One twelve-second clock drives all three panels.
const W = 360;
const H = 250;
const CYCLE = 12;
const SPEED = 2; // seconds from the source to the sink
const L = 292; // the data path, in user units

const FWD = line([34, 120], [326, 120]);
const RET = cubic([326, 128], [326, 176], [34, 176], [34, 128]);

/** Where along the data path each part of the loop sits. */
const at = (x: number) => (x - 34) / L;
const AT = {boxIn: at(62), fn: at(180), send: at(250), queue: at(262), boxOut: at(298)};
const travel = (f: number) => f * SPEED;

const EMIT = Array.from({length: 15}, (_, i) => i * 0.6); // one record every 600 ms
const STALL: [number, number] = [3, 5.5]; // the sink stalls
const TERM = 9; // SIGTERM
const BAD = 10; // the record that fails to decode
const BAD_T = EMIT[BAD] + travel(AT.fn); // it reaches the transform at 7 s
const BOUND = 4; // the framework's queue bound
const GONE = TERM + 2; // when the runtime and the hand-rolled process are down

/**
 * The instant the still frame holds. It is the one window where all three panels
 * are in their stall outcome: the framework's lanes pause at 5.16 s and resume at
 * 5.80 s, and the hand-rolled loop is blocked in its send call until the sink
 * clears at 5.50 s.
 */
const T_STILL = 5.35;

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

/** Held at `from` until `at`, `to` for the rest of the cycle, and back at the wrap. */
function dims(when: number, from: number, to: number): Stops {
  return [
    [0, from],
    [when, from],
    [Math.min(when + 0.5, CYCLE - 0.05), to],
    [CYCLE - 0.01, to],
    [CYCLE, from],
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

// The three panels meet each event at one instant, and the clock beneath them
// marks it once. A pause that swallowed the failing record's emission would move
// the framework's instant away from the two panels that have no pauses.
if (Math.abs(SIM.bad.fnT - BAD_T) > 1e-9) {
  throw new Error(`the framework skips the bad record at ${SIM.bad.fnT} s, the other panels at ${BAD_T} s`);
}

const MARKS: Array<[number, string]> = [
  [STALL[0], 'sink stalls'],
  [SIM.bad.fnT, 'bad record'],
  [TERM, 'SIGTERM'],
];

const seconds = (t: number) => Number(t.toFixed(2));

/** A drawn thing: what the still frame shows, and the twin that moves. */
type Piece = {still: React.ReactNode; anim?: React.ReactNode};

type Panel = {title: string; cap: string; aria: string; pieces: Piece[]};

/**
 * Builds every panel once, and with it the keyframes the moving twins run. Each
 * still twin carries the same tracks evaluated at `T_STILL`, and a record's still
 * position comes from the curve its motion is bound to.
 */
function build(): {panels: Panel[]; css: string} {
  let css = '';
  let uid = 0;
  const key = () => `i${uid++}`;

  const el = (tag: string, props: Record<string, unknown>, kids?: React.ReactNode): Piece => ({
    still: React.createElement(tag, {key: key(), ...props}, kids),
  });

  const entriesOf = (tracks: Tracks) => Object.entries(tracks) as Array<[keyof Tracks, Stops]>;

  const keyframes = (name: string, tracks: Tracks) => {
    const entries = entriesOf(tracks);
    for (const [k, stops] of entries) {
      for (let i = 1; i < stops.length; i += 1) {
        if (stops[i][0] < stops[i - 1][0]) {
          throw new Error(`${name}: ${PROP[k][0]} stops run backwards at ${stops[i][0]} s`);
        }
      }
    }
    const times = [...new Set([0, CYCLE, ...entries.flatMap(([, s]) => s.map(([t]) => t))])]
      .filter((t) => t >= 0 && t <= CYCLE)
      .sort((a, b) => a - b);
    const frame = (t: number) =>
      `${((t / CYCLE) * 100).toFixed(3)}%{${entries.map(([k, s]) => `${PROP[k][0]}:${PROP[k][2](valueAt(s, t))}`).join(';')}}`;
    css += `@keyframes ${name}{${times.map(frame).join('')}}\n`;
  };

  const frozen = (tracks: Tracks) => {
    const style: Record<string, string> = {};
    for (const [k, s] of entriesOf(tracks)) style[PROP[k][1]] = PROP[k][2](valueAt(s, T_STILL));
    return style as React.CSSProperties;
  };

  /**
   * The animation, as longhands. The shorthand would set `animation-play-state`
   * inline, and the off-screen rule in `home.css` is the only thing that may set
   * it.
   */
  const running = (name: string): React.CSSProperties => ({
    animationName: name,
    animationDuration: `${CYCLE}s`,
    animationTimingFunction: 'linear',
    animationIterationCount: 'infinite',
  });

  /** An element that moves in place: the twin the still frame shows, and the twin that runs. */
  const twin = (
    tag: string,
    props: Record<string, unknown>,
    tracks: Tracks,
    kids?: React.ReactNode,
  ): Piece => {
    const id = key();
    const name = `incident-${id}`;
    keyframes(name, tracks);
    const base = (props.style ?? {}) as React.CSSProperties;
    const cls = props.className as string | undefined;
    return {
      still: React.createElement(
        tag,
        {...props, key: `${id}s`, className: clsx(cls, 'incident__still'), style: {...base, ...frozen(tracks)}},
        kids,
      ),
      anim: React.createElement(
        tag,
        {...props, key: `${id}a`, className: clsx(cls, 'incident__anim'), style: {...base, ...running(name)}},
        kids,
      ),
    };
  };

  /**
   * A record riding a curve, as `[time, fraction along it, opacity]`. The still
   * twin is placed by the same curve at the same fraction, so it cannot drift
   * from the path, and it carries no `offset-path`, which is what a browser
   * without one falls back to.
   */
  const mover = (curve: Curve, className: string, r: number, stops: Array<[number, number, number]>): Piece => {
    const offset: Stops = stops.map(([t, f]) => [t, f]);
    const opacity: Stops = stops.map(([t, , o]) => [t, o]);
    const id = key();
    const name = `incident-${id}`;
    keyframes(name, {offset, opacity});
    const [cx, cy] = curve.at(valueAt(offset, T_STILL));
    return {
      still: (
        <circle
          key={`${id}s`}
          className={clsx(className, 'incident__still')}
          r={r}
          cx={Number(cx.toFixed(2))}
          cy={Number(cy.toFixed(2))}
          style={{opacity: valueAt(opacity, T_STILL)}}
        />
      ),
      anim: (
        <circle
          key={`${id}a`}
          className={clsx(className, 'incident__anim')}
          r={r}
          style={{offsetPath: `path("${curve.d}")`, offsetRotate: '0deg', ...running(name)}}
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
    el('text', {x: 26, y: 142, textAnchor: 'middle'}, 'source'),
    el('text', {x: 334, y: 142, textAnchor: 'middle'}, 'sink'),
  ];

  const fnPill = (opacity = 1) =>
    el(
      'g',
      {opacity},
      <>
        <rect x={152} y={112} width={56} height={16} rx={8} className="incident__mine-fill" />
        <text x={180} y={123.5} textAnchor="middle" className="incident__mono incident__on-accent">
          your fn
        </text>
      </>,
    );

  /** The source, drawn over its own node once it is no longer being polled. */
  const stopped = (opacity: Stops) =>
    twin('circle', {cx: 26, cy: 120, r: 8, className: 'incident__paused'}, {opacity});

  // Panel 1. Records go into the runtime's box and come out of it, and every
  // event is answered by the one label the box carries.
  const streamProcessor = (): Piece[] => {
    let held = 0;
    const recs = EMIT.map((e) => {
      const enter = e + travel(AT.boxIn);
      let leave = e + travel(AT.boxOut);
      if (leave >= STALL[0] && leave < STALL[1]) {
        leave = STALL[1] + 0.2 * held;
        held += 1;
      }
      const done = leave + travel(1 - AT.boxOut);
      return mover(FWD, 'incident__rec', 4, [
        [0, 0, 0],
        [e, 0, 0],
        [e + 0.01, 0, 1],
        [enter, AT.boxIn, 1],
        [enter + 0.05, AT.boxIn + 0.005, 0],
        [leave, AT.boxOut, 0],
        [leave + 0.05, AT.boxOut + 0.005, 1],
        [done, 1, 1],
        [done + 0.05, 1, 0],
        [CYCLE, 1, 0],
      ]);
    });
    const inside: Stops = [
      [0, 0],
      [STALL[0] - 0.01, 0],
      [STALL[0], 1],
      [STALL[1], 1],
      [STALL[1] + 0.3, 0],
      [BAD_T - 0.01, 0],
      [BAD_T, 1],
      [BAD_T + 1.2, 1],
      [BAD_T + 1.5, 0],
      [TERM - 0.01, 0],
      [TERM, 1],
      [GONE, 1],
      [GONE + 0.3, 0],
    ];
    return [
      twin(
        'rect',
        {x: 62, y: 40, width: 236, height: 166, rx: 6, className: 'incident__runtime', fill: 'url(#incident-hatch)'},
        {opacity: dims(GONE, 1, 0.25)},
      ),
      el('text', {x: 70, y: 58, className: 'incident__runtime-t'}, 'the runtime'),
      el('path', {d: RET.d, className: 'incident__ret', opacity: 0.5}),
      el('path', {d: FWD.d, className: 'incident__path'}),
      el('text', {x: 180, y: 104, textAnchor: 'middle', className: 'incident__runtime-t'}, 'called from inside'),
      twin(
        'text',
        {x: 180, y: 176, textAnchor: 'middle', className: 'incident__runtime-t', style: {fontSize: 11}},
        {opacity: inside},
        'handled inside',
      ),
      ...recs,
      fnPill(0.55),
      ...endpoints(),
    ];
  };

  // Panel 2. The loop blocks in its send call for as long as the sink is stalled:
  // the record at the send call holds there, nothing behind it moves, and nothing
  // is polling the source. Every event lands on a part drawn dashed.
  const handRolled = (): Piece[] => {
    const blocked = EMIT.map((e) => e + travel(AT.send)).find((t) => t >= STALL[0] && t < STALL[1]) ?? STALL[0];
    const skipped = twin('circle', {cx: 180, cy: 120, r: 4, className: 'incident__skip'}, {
      opacity: [
        [0, 0],
        [BAD_T - 0.01, 0],
        [BAD_T, 1],
        [BAD_T + 0.6, 0],
      ],
    });
    const recs = EMIT.flatMap((e, i) => {
      if (i === BAD) {
        return [
          mover(FWD, 'incident__rec', 4, [
            [0, 0, 0],
            [e, 0, 0],
            [e + 0.01, 0, 1],
            [BAD_T, AT.fn, 1],
            [BAD_T + 0.01, AT.fn, 0],
            [CYCLE, AT.fn, 0],
          ]),
        ];
      }
      if (e >= blocked && e < STALL[1]) return []; // nothing polls the source while the loop is blocked
      let stops: Array<[number, number, number]> = [
        [0, 0, 0],
        [e, 0, 0],
        [e + 0.01, 0, 1],
        [e + SPEED, 1, 1],
        [e + SPEED + 0.05, 1, 0],
      ];
      if (e < blocked && e + SPEED > blocked) {
        const f = (blocked - e) / SPEED;
        const done = STALL[1] + travel(1 - f);
        stops = [
          [0, 0, 0],
          [e, 0, 0],
          [e + 0.01, 0, 1],
          [blocked, f, 1],
          [STALL[1], f, 1],
          [done, 1, 1],
          [done + 0.05, 1, 0],
        ];
      }
      // SIGTERM: whatever is in flight stops where it is and fades. What the loop
      // does here is unwritten.
      if (stops[stops.length - 1][0] > TERM) {
        const held = valueAt(
          stops.map(([t, f]) => [t, f] as [number, number]),
          TERM,
        );
        stops = stops.filter(([t]) => t < TERM).concat([
          [TERM, held, 1],
          [TERM + 0.5, held, 0],
        ]);
      }
      stops.push([CYCLE, stops[stops.length - 1][1], 0]);
      return [mover(FWD, 'incident__rec', 4, stops)];
    });
    const question = (x: number, y: number, on: number) =>
      twin('text', {x, y, textAnchor: 'middle', className: 'incident__q'}, {
        opacity: [
          [0, 0],
          [on - 0.01, 0],
          [on, 1],
          [GONE, 1],
          [GONE + 0.3, 0],
        ],
      }, '?');
    return [
      twin(
        'rect',
        {x: 62, y: 40, width: 236, height: 166, rx: 6, className: 'incident__mine', strokeWidth: 1.25},
        {opacity: dims(GONE, 1, 0.25)},
      ),
      el('text', {x: 70, y: 58, className: 'incident__mine-t'}, 'your process'),
      el('text', {x: 290, y: 58, textAnchor: 'end', className: 'incident__mine-t'}, 'dashed: yours to write'),
      el('path', {d: RET.d, className: 'incident__todo'}),
      el('path', {d: FWD.d, className: 'incident__path'}),
      el('text', {x: 261, y: 92, textAnchor: 'middle', className: 'incident__mono'}, 'send'),
      el('rect', {x: 250, y: 100, width: 22, height: 40, rx: 3, className: 'incident__todo'}),
      el('text', {x: 261, y: 150, textAnchor: 'middle', className: 'incident__mono incident__mine-t'}, 'queue'),
      el('rect', {x: 160, y: 80, width: 40, height: 14, rx: 3, className: 'incident__todo'}),
      el('text', {x: 180, y: 76, textAnchor: 'middle', className: 'incident__mono incident__mine-t'}, 'on error'),
      el('circle', {cx: 334, cy: 120, r: 14, className: 'incident__todo'}),
      el('text', {x: 334, y: 100, textAnchor: 'middle', className: 'incident__mono incident__mine-t'}, 'drain'),
      el('circle', {cx: 180, cy: 164, r: 10, className: 'incident__todo'}),
      el('text', {x: 180, y: 186, textAnchor: 'middle', className: 'incident__mono incident__mine-t'}, 'commit'),
      question(285, 92, STALL[0]),
      question(213, 92, BAD_T),
      question(334, 86, TERM),
      ...recs,
      fnPill(),
      skipped,
      ...endpoints(),
      stopped([
        [0, 0],
        [blocked - 0.01, 0],
        [blocked, 1],
        [STALL[1], 1],
        [STALL[1] + 0.1, 0],
        [TERM - 0.01, 0],
        [TERM, 1],
        [CYCLE - 0.01, 1],
        [CYCLE, 0],
      ]),
    ];
  };

  // Panel 3. Every event is met by a part that is drawn, and each carries the
  // number of the property it holds to.
  const spateLoop = (): Piece[] => {
    const {recs, bad, pauses, acks, exit} = SIM;
    const out: Piece[] = [];
    out.push(
      twin(
        'rect',
        {x: 62, y: 40, width: 236, height: 166, rx: 6, className: 'incident__mine', strokeWidth: 1},
        {opacity: dims(exit, 0.8, 0.2)},
      ),
      el('text', {x: 70, y: 58, className: 'incident__mine-t'}, 'your process'),
      twin(
        'rect',
        {x: 76, y: 62, width: 208, height: 134, rx: 5, className: 'incident__fw'},
        {opacity: dims(exit, 1, 0.25)},
      ),
      el('text', {x: 84, y: 76, className: 'incident__fw-t'}, 'Spate'),
      el('path', {d: RET.d, className: 'incident__ret'}),
      el('path', {d: FWD.d, className: 'incident__path'}),
      // the poll indicator blinks throughout, including while the lanes are paused
      el('text', {x: 90, y: 98, textAnchor: 'middle', className: 'incident__mono'}, 'poll'),
      {
        still: <circle key={key()} cx={90} cy={106} r={2.5} className="incident__tick incident__still" />,
        anim: <circle key={key()} cx={90} cy={106} r={2.5} className="incident__tick incident__anim" />,
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
    const skipped = twin('circle', {cx: 180, cy: 120, r: 4, className: 'incident__skip'}, {
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
    out.push(fnPill(), skipped);
    // the bounded queue and its bars, from the same simulation
    out.push(
      el('rect', {x: 250, y: 100, width: 22, height: 40, rx: 3, className: 'incident__fw'}),
      el('text', {x: 261, y: 150, textAnchor: 'middle', className: 'incident__mono incident__fw-t'}, 'queue'),
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
        twin('rect', {x: 253, y: 133 - k * 9, width: 16, height: 7, className: 'incident__rec'}, {opacity: bar}),
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
      [CYCLE, 0],
    ];
    out.push(
      twin('text', {x: 180, y: 82, textAnchor: 'middle', className: 'incident__fw-t'}, {
        opacity: [
          [0, 0],
          ...pauses.flatMap(
            ([a, b]) =>
              [
                [a - 0.01, 0],
                [a, 1],
                [b, 1],
                [b + 0.3, 0],
              ] as Stops,
          ),
        ],
      }, 'lanes paused, still polling'),
      twin('text', {x: 334, y: 158, textAnchor: 'middle'}, {opacity: fade(STALL[0], STALL[1])}, 'stalled'),
    );
    // the skipped record is counted
    out.push(
      twin('text', {x: 180, y: 148, textAnchor: 'middle', className: 'incident__fw-t'}, {
        opacity: [
          [0, 0],
          [bad.fnT + 0.4, 0],
          [bad.fnT + 0.6, 1],
          [exit, 1],
          [Math.min(exit + 0.4, CYCLE), 0],
        ],
      }, 'skipped 1'),
    );
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
    level.push([CYCLE - 0.01, 0], [CYCLE, C]);
    out.push(
      el('circle', {cx: 180, cy: 164, r: 10, className: 'incident__track', strokeWidth: 3}),
      twin(
        'circle',
        {
          cx: 180,
          cy: 164,
          r: 10,
          className: 'incident__fw',
          strokeWidth: 3,
          transform: 'rotate(-90 180 164)',
          strokeDasharray: C.toFixed(2),
        },
        {dash: level},
      ),
      el('text', {x: 180, y: 186, textAnchor: 'middle', className: 'incident__mono incident__fw-t'}, 'commit'),
    );
    // SIGTERM: the drain deadline runs around the sink and is polled throughout;
    // the process exits before it lands
    const D = 2 * Math.PI * 14;
    out.push(
      twin(
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
      el('text', {x: 334, y: 100, textAnchor: 'middle', className: 'incident__mono incident__fw-t'}, 'drain'),
      twin('text', {x: 180, y: 226, textAnchor: 'middle', className: 'incident__fw-t'}, {
        opacity: [
          [0, 0],
          [exit - 0.01, 0],
          [exit, 1],
          [CYCLE - 0.01, 1],
          [CYCLE, 0],
        ],
      }, 'exit 0, everything acknowledged committed'),
      ...endpoints(),
      stopped(paused),
    );
    return out;
  };

  const panels: Panel[] = [
    {
      title: 'Stream processor',
      cap: 'The same three events reach the runtime. Records go in and come out; what happens to each event happens where you cannot look.',
      aria: 'A sink stall, a record that fails to decode, and a SIGTERM reach a hatched box marked the runtime. Records enter and leave it; during each event the box reads handled inside and nothing else is visible.',
      pieces: streamProcessor(),
    },
    {
      title: 'Hand-rolled loop',
      cap: 'The same three events reach your loop. Each lands on a part drawn dashed, and what happens there is whatever you wrote.',
      aria: 'A sink stall, a record that fails to decode, and a SIGTERM reach a box marked your process. The loop stops at the send call for as long as the sink is stalled and the source stops with it, the failing record vanishes at your function, and in-flight records fade at SIGTERM. At each point a question mark stands on a part drawn as a dashed outline.',
      pieces: handRolled(),
    },
    {
      title: 'Spate',
      cap: 'The same three events reach the framework. Lanes pause at the queue bound and keep polling, the bad record is counted, and SIGTERM drains before it exits.',
      aria: 'A sink stall, a record that fails to decode, and a SIGTERM reach a box marked Spate. During the stall a queue fills to its bound of four, the source dims and a poll indicator keeps blinking. The failing record drops out and a counter reads skipped 1. At SIGTERM the source stops, in-flight records reach the sink, acknowledgements travel back and fill a commit gauge, a ring counts down around the sink, and the box reads exit 0.',
      pieces: spateLoop(),
    },
  ];
  return {panels, css};
}

const FIGURE = build();

const GROUP_LABEL =
  `Three loops meeting the same three events on one ${CYCLE} second clock: ` +
  `the sink stalls at ${seconds(STALL[0])} seconds, a record fails to decode at ${seconds(SIM.bad.fnT)} seconds, ` +
  `and SIGTERM arrives at ${seconds(TERM)} seconds.`;

/**
 * Three loops meeting the same three events on one twelve-second clock: the sink
 * stalls, a record fails to decode, and SIGTERM arrives.
 *
 * The server renders the still frame; the moving twins and the keyframes they run
 * arrive on mount, so a page without script shows the drawing at `T_STILL`.
 * `prefers-reduced-motion: reduce`, and a browser with no `offset-path`, keep
 * that frame. Motion pauses while the figure is off screen, and a viewport that
 * cannot report that runs it.
 */
export default function Incident(): React.JSX.Element {
  const [motion, setMotion] = useState(false);
  const [ref, onScreen] = useOnScreen<HTMLDivElement>();
  useEffect(() => {
    const sheet = document.createElement('style');
    sheet.textContent = FIGURE.css;
    document.head.append(sheet);
    setMotion(true);
    return () => sheet.remove();
  }, []);
  return (
    <div
      ref={ref}
      className={clsx('incident', motion && 'incident--motion', !onScreen && 'incident--offscreen')}>
      <svg className="incident__defs" width="0" height="0" aria-hidden="true">
        <defs>
          <pattern id="incident-hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <line className="incident__hatch" x1="0" y1="0" x2="0" y2="6" strokeWidth="1" />
          </pattern>
        </defs>
      </svg>
      <div className="incident__panels" role="group" aria-label={GROUP_LABEL}>
        {FIGURE.panels.map((p) => (
          <figure key={p.title} className="incident__panel">
            <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={p.aria}>
              {/* One list, so the moving twin paints where its own still twin does. */}
              {p.pieces.flatMap((q) => (motion && q.anim ? [q.still, q.anim] : [q.still]))}
            </svg>
            <figcaption>
              <b>{p.title}</b>
              {p.cap}
            </figcaption>
          </figure>
        ))}
      </div>
      <div className="incident__timeline" aria-hidden="true">
        <div className="incident__tl-track">
          {motion && <span className="incident__tl-cursor incident__anim" />}
          <span
            className="incident__tl-cursor incident__still"
            style={{left: `${((T_STILL / CYCLE) * 100).toFixed(1)}%`}}
          />
          {MARKS.map(([t, label], i) => (
            <span
              key={label}
              className={clsx('incident__tl-mark', i % 2 === 1 && 'incident__tl-mark--low')}
              style={{left: `${((t / CYCLE) * 100).toFixed(1)}%`}}>
              <span>{label}</span>
            </span>
          ))}
        </div>
        <span className="incident__tl-note incident__anim">one {CYCLE} s cycle</span>
        <span className="incident__tl-note incident__still">
          one {CYCLE} s cycle · still frame at {T_STILL} s
        </span>
      </div>
    </div>
  );
}
