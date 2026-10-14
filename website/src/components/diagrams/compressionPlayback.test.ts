import assert from 'node:assert/strict';
import {test} from 'node:test';

import {
  CYCLE,
  INITIAL,
  SLOTS,
  STILL,
  THREADS,
  WARM,
  WORKERS,
  delivered,
  frameAt,
  playback,
  running,
  snapshot,
  type Frame,
  type Mode,
  type Place,
  type Point,
} from './compressionPlayback.ts';

const MODES: Mode[] = ['before', 'after'];

/** A distinct point per place, so a test can tell where a chunk is from its coordinates. */
const XY = (p: Place): Point =>
  p.kind === 'thread' ? [p.lane * 100, 0] : p.kind === 'slot' ? [p.lane * 100, 10 + p.k * 10] : p.kind === 'worker' ? [p.w * 100, 50] : [p.w * 100, 100];

/** Every 1/64 s of one cycle, exact in binary floating point. */
const TIMES = Array.from({length: CYCLE * 64}, (_, i) => i / 64);

const frames = (mode: Mode): Frame[] => TIMES.map((t) => frameAt(mode, t, XY));

test('a lane never holds more chunks than its queue has slots', () => {
  for (const mode of MODES) {
    for (const f of frames(mode)) {
      assert.equal(f.depth.length, THREADS);
      for (const d of f.depth) assert.ok(d >= 0 && d <= SLOTS, `${mode}: depth ${d}`);
      for (const c of f.chunks.filter((c) => c.place.kind === 'slot')) {
        assert.ok((c.place as {k: number}).k < SLOTS, `${mode}: chunk ${c.id} in slot ${JSON.stringify(c.place)}`);
      }
    }
  }
});

test('before: a thread stalls only while its queue is full, and the threads stall', () => {
  let stalled = 0;
  for (const f of frames('before')) {
    f.threads.forEach((th, lane) => {
      if (th.work !== 'stalled') return;
      stalled += 1;
      assert.equal(f.depth[lane], SLOTS, `lane ${lane} stalled with ${f.depth[lane]} queued`);
      // The stalled thread holds the chunk it sealed.
      assert.ok(f.chunks.some((c) => c.place.kind === 'thread' && c.place.lane === lane && !c.moving));
    });
  }
  // The I/O workers are the bottleneck, so the threads spend most of each cycle waiting.
  assert.ok(stalled > (TIMES.length * THREADS) / 2, `${stalled} stalled thread samples`);
});

test('after: no thread stalls, and every chunk leaves its thread compressed', () => {
  for (const f of frames('after')) {
    for (const th of f.threads) assert.notEqual(th.work, 'stalled');
    for (const c of f.chunks) assert.equal(c.size, 0, `chunk ${c.id} at ${JSON.stringify(c.place)}`);
  }
  // Each thread compresses before it seals.
  assert.ok(frames('after').some((f) => f.threads.some((th) => th.work === 'compress')));
});

test('before: a chunk is queued uncompressed and is compressed on an I/O worker before it is sent', () => {
  let shrinking = 0;
  for (const f of frames('before')) {
    for (const th of f.threads) assert.notEqual(th.work, 'compress');
    for (const c of f.chunks) {
      if (c.place.kind === 'slot' || c.place.kind === 'thread') assert.equal(c.size, 1);
      if (c.place.kind === 'sink') assert.equal(c.size, 0);
      if (c.place.kind === 'worker' && c.size > 0 && c.size < 1) shrinking += 1;
    }
  }
  assert.ok(shrinking > 0);
});

test('the I/O workers compress and send in before, and only send in after', () => {
  const work = (mode: Mode) => new Set(frames(mode).flatMap((f) => f.workers.map((w) => w.work)));
  assert.deepEqual([...work('before')].sort(), ['compress', 'send']);
  assert.ok(!work('after').has('compress'));
  assert.ok(work('after').has('send'));
  for (const mode of MODES) for (const f of frames(mode)) assert.equal(f.workers.length, WORKERS);
});

test('after: an I/O worker takes a queued chunk as soon as it is free', () => {
  for (const f of frames('after')) {
    const queued = f.depth.reduce((a, b) => a + b, 0);
    if (queued > 0) assert.ok(f.workers.every((w) => w.work !== 'idle'), 'a worker idles beside a queued chunk');
  }
});

test('after delivers more chunks per cycle than before', () => {
  assert.ok(delivered('after') > delivered('before'), `${delivered('after')} against ${delivered('before')}`);
});

test('the loop is seamless: one cycle later the drawing is the same', () => {
  const strip = (f: Frame) => ({...f, chunks: f.chunks.map(({id: _id, ...c}) => c).sort((a, b) => a.at[0] - b.at[0] || a.at[1] - b.at[1])});
  for (const mode of MODES) {
    for (const t of TIMES) {
      assert.deepEqual(strip(snapshot(mode, WARM + t, XY)), strip(snapshot(mode, WARM + CYCLE + t, XY)), `${mode} at ${t}`);
    }
    assert.deepEqual(frameAt(mode, CYCLE + 1, XY), frameAt(mode, 1, XY));
  }
});

test('the still frame shows a stalled thread and a queued chunk before, and a compressed chunk after', () => {
  const before = frameAt('before', STILL, XY);
  assert.ok(before.threads.some((th) => th.work === 'stalled'));
  assert.ok(before.chunks.some((c) => c.place.kind === 'slot' && c.size === 1 && !c.moving));
  const after = frameAt('after', STILL, XY);
  assert.ok(after.threads.every((th) => th.work !== 'stalled'));
  assert.ok(after.chunks.some((c) => c.size === 0));
});

test('playback holds the still frame until motion is known to be allowed', () => {
  assert.equal(INITIAL.time, STILL);
  assert.equal(running(INITIAL), false);
  assert.deepEqual(playback(INITIAL, {type: 'tick', time: 2}), INITIAL);
  const moving = playback(INITIAL, {type: 'motion', reduced: false});
  assert.equal(running(moving), true);
  assert.equal(moving.time, STILL);
});

test('ticks advance the clock and wrap it at the cycle', () => {
  const moving = playback(INITIAL, {type: 'motion', reduced: false});
  assert.equal(playback(moving, {type: 'tick', time: 2.5}).time, 2.5);
  assert.equal(playback(moving, {type: 'tick', time: CYCLE + 1.5}).time, 1.5);
});

test('pause stops the clock and play resumes it from the same frame', () => {
  const moving = playback(playback(INITIAL, {type: 'motion', reduced: false}), {type: 'tick', time: 3});
  const paused = playback(moving, {type: 'toggle'});
  assert.equal(running(paused), false);
  assert.deepEqual(playback(paused, {type: 'tick', time: 4}), paused);
  const resumed = playback(paused, {type: 'toggle'});
  assert.equal(running(resumed), true);
  assert.equal(resumed.time, 3);
});

test('reduced motion returns to the still frame and ignores ticks and play', () => {
  const moving = playback(playback(INITIAL, {type: 'motion', reduced: false}), {type: 'tick', time: 3});
  const reduced = playback(moving, {type: 'motion', reduced: true});
  assert.equal(reduced.time, STILL);
  assert.equal(running(reduced), false);
  assert.deepEqual(playback(reduced, {type: 'tick', time: 4}), reduced);
  assert.equal(running(playback(reduced, {type: 'toggle'})), false);
});

test('every progress bar runs from empty to full and no further', () => {
  for (const mode of MODES) {
    for (const f of frames(mode)) {
      for (const p of [...f.threads, ...f.workers]) assert.ok(p.progress >= 0 && p.progress <= 1, `${mode}: ${p.work} at ${p.progress}`);
    }
  }
});
