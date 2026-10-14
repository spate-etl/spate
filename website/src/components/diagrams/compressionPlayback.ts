/**
 * The timeline behind the figure of where insert compression runs. It simulates pipeline threads
 * that seal chunks into bounded shard queues and I/O workers that take them, once per build, and
 * answers what the figure shows at any moment. It holds no React, so it runs under `node:test`.
 *
 * In `before`, an I/O worker compresses each chunk and then sends it, and a thread whose queue is
 * full waits with its sealed chunk. In `after`, a thread compresses each chunk before it seals it,
 * and an I/O worker only sends.
 */

export type Mode = 'before' | 'after';

/**
 * Where a chunk can be: at its thread, in a queue slot (0 is the head), on an I/O worker, or sent
 * by that worker.
 */
export type Place =
  | {kind: 'thread'; lane: number}
  | {kind: 'slot'; lane: number; k: number}
  | {kind: 'worker'; w: number}
  | {kind: 'sink'; w: number};

export type Point = [number, number];

/** A thread's current job and the fraction of it done. */
export type ThreadView = {work: 'encode' | 'compress' | 'stalled'; progress: number};
export type WorkerView = {work: 'idle' | 'compress' | 'send'; progress: number};
/**
 * A chunk on screen. `place` is where it is or where it is moving to, and `size` runs from 1,
 * uncompressed, to 0, compressed.
 */
export type ChunkView = {id: number; place: Place; moving: boolean; at: Point; size: number};
/** What the figure shows at one moment. `depth` counts the chunks each lane's queue holds. */
export type Frame = {threads: ThreadView[]; workers: WorkerView[]; depth: number[]; chunks: ChunkView[]};

/** The threads drawn, standing for 32. */
export const THREADS = 4;
export const WORKERS = 2;
/** Each lane's queue bound. */
export const SLOTS = 2;

// Seconds. Every duration and start is a binary fraction, so every event time is exact and the
// simulation repeats bit for bit from one cycle to the next.
const ENCODE = 0.5;
const COMPRESS = 1;
const SEND = 0.25;
/** A chunk's move from one place to the next. */
const HOP = 0.25;
const START = (lane: number) => lane * 0.375;

/** One loop of the figure. Both builds repeat within it once the queues have settled. */
export const CYCLE = 6;
/** The simulated time the loop starts at, after the queues have filled. */
export const WARM = 12;
/** The loop time of the frame shown without motion. */
export const STILL = 1.75;

type Step = {t: number; place: Place; dur: number};
type Chunk = {
  id: number;
  lane: number;
  sealed: number;
  queued: number;
  taken: number;
  compressAt: number;
  sendAt: number;
  done: number;
  path: Step[];
};
type Span = {from: number; to: number};
type Sim = {chunks: Chunk[]; jobs: Span[][]; stalls: Span[][]; work: Span[][]};

/** How long a thread works on one chunk before it seals it. */
const job = (mode: Mode) => (mode === 'before' ? ENCODE : ENCODE + COMPRESS);
/** How long an I/O worker holds a chunk before it starts sending it. */
const hold = (mode: Mode) => HOP + (mode === 'before' ? COMPRESS : 0);

function simulate(mode: Mode, horizon: number): Sim {
  const chunks: Chunk[] = [];
  const jobs: Span[][] = Array.from({length: THREADS}, () => []);
  const stalls: Span[][] = Array.from({length: THREADS}, () => []);
  const work: Span[][] = Array.from({length: WORKERS}, () => []);
  const queues: Chunk[][] = Array.from({length: THREADS}, () => []);
  const ready = Array.from({length: THREADS}, (_, lane) => START(lane) + job(mode));
  const holding: Array<Chunk | null> = Array(THREADS).fill(null);
  const free = Array(WORKERS).fill(0);
  for (let lane = 0; lane < THREADS; lane += 1) jobs[lane].push({from: START(lane), to: ready[lane]});

  let t = 0;
  while (t <= horizon) {
    for (let lane = 0; lane < THREADS; lane += 1) {
      if (holding[lane] || ready[lane] !== t) continue;
      const c: Chunk = {
        id: chunks.length,
        lane,
        sealed: t,
        queued: Infinity,
        taken: Infinity,
        compressAt: Infinity,
        sendAt: Infinity,
        done: Infinity,
        path: [{t, place: {kind: 'thread', lane}, dur: 0}],
      };
      chunks.push(c);
      holding[lane] = c;
    }
    // A put frees a thread and a take frees a slot, so repeat until neither moves anything.
    for (let moved = true; moved; ) {
      moved = false;
      for (let lane = 0; lane < THREADS; lane += 1) {
        const c = holding[lane];
        // The stall rule: a thread puts its sealed chunk only into a queue with a free slot.
        if (!c || queues[lane].length >= SLOTS) continue;
        if (t > c.sealed) stalls[lane].push({from: c.sealed, to: t});
        c.queued = t;
        c.path.push({t, place: {kind: 'slot', lane, k: queues[lane].length}, dur: HOP});
        queues[lane].push(c);
        holding[lane] = null;
        ready[lane] = t + job(mode);
        jobs[lane].push({from: t, to: ready[lane]});
        moved = true;
      }
      for (let w = 0; w < WORKERS; w += 1) {
        if (free[w] > t) continue;
        // The oldest queued chunk, the lowest lane on a tie.
        let pick = -1;
        for (let lane = 0; lane < THREADS; lane += 1) {
          const head = queues[lane][0];
          if (head && (pick < 0 || head.queued < queues[pick][0].queued)) pick = lane;
        }
        if (pick < 0) break;
        const c = queues[pick].shift()!;
        c.taken = t;
        c.compressAt = t + HOP;
        c.sendAt = t + hold(mode);
        c.done = c.sendAt + SEND;
        c.path.push({t, place: {kind: 'worker', w}, dur: HOP}, {t: c.sendAt, place: {kind: 'sink', w}, dur: SEND});
        queues[pick].forEach((q, k) => q.path.push({t, place: {kind: 'slot', lane: pick, k}, dur: HOP}));
        free[w] = c.done;
        work[w].push({from: t, to: c.done});
        moved = true;
      }
    }
    const next = [
      ...ready.filter((r, lane) => !holding[lane] && r > t),
      ...free.filter((f) => f > t),
    ];
    if (next.length === 0) break;
    t = Math.min(...next);
  }
  return {chunks, jobs, stalls, work};
}

// A full cycle past the last one the loop reads, so every stall and send it shows has ended.
const HORIZON = WARM + 3 * CYCLE;
const SIMS: Record<Mode, Sim> = {before: simulate('before', HORIZON), after: simulate('after', HORIZON)};

const within = (s: Span, t: number) => s.from <= t && t < s.to;
const clamp = (f: number) => Math.min(1, Math.max(0, f));
const lerp = (a: Point, b: Point, f: number): Point => [a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f];

/**
 * Where a chunk is at `t`. Each move starts from wherever the previous one had reached, so a chunk
 * redirected mid-move does not jump.
 */
function position(path: Step[], t: number, xy: (p: Place) => Point): Point {
  let start = xy(path[0].place);
  let i = 0;
  for (; i + 1 < path.length && path[i + 1].t <= t; i += 1) {
    const {t: t0, place, dur} = path[i];
    start = lerp(start, xy(place), dur === 0 ? 1 : clamp((path[i + 1].t - t0) / dur));
  }
  const {t: t0, place, dur} = path[i];
  return lerp(start, xy(place), dur === 0 ? 1 : clamp((t - t0) / dur));
}

/** What the figure of `mode` shows at simulated time `time`, with places drawn at `xy`. */
export function snapshot(mode: Mode, time: number, xy: (p: Place) => Point): Frame {
  const sim = SIMS[mode];
  const threads = sim.jobs.map((jobs, lane): ThreadView => {
    if (sim.stalls[lane].some((s) => within(s, time))) return {work: 'stalled', progress: 0};
    const j = jobs.find((s) => within(s, time));
    if (!j) return {work: 'encode', progress: 0};
    const done = time - j.from;
    return done < ENCODE ? {work: 'encode', progress: done / ENCODE} : {work: 'compress', progress: (done - ENCODE) / COMPRESS};
  });
  const workers = sim.work.map((spans): WorkerView => {
    const s = spans.find((x) => within(x, time));
    if (!s) return {work: 'idle', progress: 0};
    // In `after` the worker only sends, so the move that brings it the chunk is part of the send.
    const send = mode === 'before' ? s.to - SEND : s.from;
    return time < send
      ? {work: 'compress', progress: (time - s.from) / (send - s.from)}
      : {work: 'send', progress: (time - send) / (s.to - send)};
  });
  const depth = Array.from({length: THREADS}, (_, lane) =>
    sim.chunks.filter((c) => c.lane === lane && c.queued <= time && time < c.taken).length,
  );
  const chunks = sim.chunks
    .filter((c) => c.sealed <= time && time < c.done)
    .map((c): ChunkView => {
      let i = 0;
      while (i + 1 < c.path.length && c.path[i + 1].t <= time) i += 1;
      const step = c.path[i];
      const size = mode === 'after' ? 0 : time < c.compressAt ? 1 : clamp(1 - (time - c.compressAt) / COMPRESS);
      return {id: c.id, place: step.place, moving: time < step.t + step.dur, at: position(c.path, time, xy), size};
    });
  return {threads, workers, depth, chunks};
}

/** What the figure of `mode` shows `time` seconds into its loop. */
export function frameAt(mode: Mode, time: number, xy: (p: Place) => Point): Frame {
  return snapshot(mode, WARM + (((time % CYCLE) + CYCLE) % CYCLE), xy);
}

/** The chunks `mode` sends in one loop. */
export function delivered(mode: Mode): number {
  return SIMS[mode].chunks.filter((c) => WARM <= c.done && c.done < WARM + CYCLE).length;
}

// ---- playback ----

/** The loop time, whether the reader paused it, and whether the reader asked for reduced motion. */
export type Playback = {time: number; paused: boolean; reduced: boolean};
export type Action = {type: 'tick'; time: number} | {type: 'toggle'} | {type: 'motion'; reduced: boolean};

/** Reduced until the page has read the reader's preference, so nothing moves before it is known. */
export const INITIAL: Playback = {time: STILL, paused: false, reduced: true};

export function running(state: Playback): boolean {
  return !state.reduced && !state.paused;
}

/** Ticks carry the time since the loop's first frame; reduced motion pins the still frame. */
export function playback(state: Playback, action: Action): Playback {
  switch (action.type) {
    case 'tick':
      return running(state) ? {...state, time: action.time % CYCLE} : state;
    case 'toggle':
      return state.reduced ? state : {...state, paused: !state.paused};
    case 'motion':
      return action.reduced ? {...state, reduced: true, time: STILL} : {...state, reduced: false};
  }
}
