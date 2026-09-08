// Tests for the reveal geometry and the triggers wired around it.
//
// `./reveal.ts` is imported with its extension because `node --test` strips
// types but does not resolve the bundler's extensionless imports.

import assert from 'node:assert/strict';
import {test} from 'node:test';

import {attachReveal, revealThresholds, shouldReveal, visibleRatio} from './reveal.ts';

const VIEWPORT = 800;
const THRESHOLD = 0.15;

/** A section shorter than the viewport reveals once the viewport shows enough of it. */
test('a section the viewport shows half of reveals', () => {
  assert.equal(shouldReveal({top: 600, height: 400}, VIEWPORT, THRESHOLD), true);
});

/** The first pixels of a section are not a reveal, so a section still animates in. */
test('a section barely in view stays hidden', () => {
  assert.equal(shouldReveal({top: 780, height: 400}, VIEWPORT, THRESHOLD), false);
  assert.equal(shouldReveal({top: 900, height: 400}, VIEWPORT, THRESHOLD), false);
});

/**
 * A section taller than `1 / threshold` viewports reveals when it fills the
 * viewport, though its own visible fraction never reaches the threshold.
 */
test('a section taller than the viewport reveals', () => {
  const tall = {top: 0, height: 6000};
  assert.ok(visibleRatio(tall.top, tall.height, VIEWPORT) < THRESHOLD);
  assert.equal(shouldReveal(tall, VIEWPORT, THRESHOLD), true);
  assert.equal(shouldReveal({top: 700, height: 6000}, VIEWPORT, THRESHOLD), false);
  assert.equal(shouldReveal({top: 600, height: 6000}, VIEWPORT, THRESHOLD), true);
});

/**
 * The fallback measurement decides from a rect it reads itself, with no
 * observer entry. Off-screen stays hidden, and a target with no height reveals.
 */
test('the measurement decides from a rect alone', () => {
  assert.equal(visibleRatio(600, 400, VIEWPORT), 0.5);
  assert.equal(visibleRatio(-200, 400, VIEWPORT), 0.5);
  assert.equal(visibleRatio(2000, 400, VIEWPORT), 0);
  assert.equal(visibleRatio(0, 0, VIEWPORT), 0);
  assert.equal(shouldReveal({top: 0, height: 0}, VIEWPORT, THRESHOLD), true);
});

/** The observer reports finely enough to catch a target whose ratio stays near zero. */
test('the observer thresholds step to 1 and carry the requested one', () => {
  const steps = revealThresholds(0.17);
  assert.deepEqual([...steps].sort((a, b) => a - b), steps);
  assert.equal(new Set(steps).size, steps.length);
  assert.equal(steps[0], 0);
  assert.equal(steps.at(-1), 1);
  assert.ok(steps.includes(0.17));
  assert.ok(steps.includes(0.02), 'a 6000px target in an 800px viewport reveals at ratio 0.02');
});

/** A target and a window built from plain objects, recording what `attachReveal` does to them. */
function stub(rect: {top: number; height: number}, options: {observer?: boolean} = {}) {
  const classes: string[] = [];
  const listeners: {type: string; fn: () => void}[] = [];
  const frames = new Map<number, () => void>();
  const counts = {added: 0, removed: 0, cancelled: 0};
  const observed: {callback?: () => void; disconnects: number} = {disconnects: 0};
  let handle = 0;

  const observer = class {
    constructor(callback: () => void) {
      observed.callback = callback;
    }

    observe() {}

    disconnect() {
      observed.disconnects += 1;
    }
  };

  const el = {
    getBoundingClientRect: () => rect,
    classList: {
      add(token: string) {
        classes.push(token);
      },
    },
  };

  const win = {
    innerHeight: VIEWPORT,
    addEventListener(type: string, fn: () => void) {
      counts.added += 1;
      listeners.push({type, fn});
    },
    removeEventListener(type: string, fn: () => void) {
      counts.removed += 1;
      const at = listeners.findIndex((l) => l.type === type && l.fn === fn);
      if (at >= 0) listeners.splice(at, 1);
    },
    requestAnimationFrame(fn: () => void) {
      handle += 1;
      frames.set(handle, fn);
      return handle;
    },
    cancelAnimationFrame(pending: number) {
      counts.cancelled += 1;
      frames.delete(pending);
    },
    ...(options.observer === false ? {} : {IntersectionObserver: observer}),
  };

  return {
    el,
    win,
    rect,
    classes,
    listeners,
    counts,
    observed,
    /** Runs the handlers registered for `type`, as an event dispatch would. */
    fire(type: string) {
      for (const l of [...listeners]) if (l.type === type) l.fn();
    },
    /** Runs the callbacks the window has been asked to schedule. */
    runFrames() {
      for (const fn of [...frames.values()]) fn();
    },
  };
}

/**
 * A scroll reveals a target that comes into view while the observer holds its
 * callback without ever invoking it.
 */
test('a scroll reveals the target when the observer stays silent', () => {
  const s = stub({top: 2000, height: 400});
  attachReveal(s.el, s.win, THRESHOLD);
  assert.ok(s.observed.callback);
  assert.deepEqual(s.classes, []);

  s.rect.top = 0;
  s.fire('scroll');
  assert.deepEqual(s.classes, ['is-in']);
});

/** The scheduled frame reveals a target in view with no event and no observer notification. */
test('the frame callback reveals on its own', () => {
  const s = stub({top: 0, height: 400});
  attachReveal(s.el, s.win, THRESHOLD);
  assert.deepEqual(s.classes, []);

  s.runFrames();
  assert.deepEqual(s.classes, ['is-in']);
});

/** Revealing disconnects the observer, cancels the frame and removes both listeners. */
test('revealing tears down every trigger', () => {
  const s = stub({top: 0, height: 400});
  attachReveal(s.el, s.win, THRESHOLD);
  assert.equal(s.counts.added, 2);

  s.runFrames();
  assert.equal(s.counts.removed, s.counts.added);
  assert.deepEqual(s.listeners, []);
  assert.equal(s.observed.disconnects, 1);
  assert.equal(s.counts.cancelled, 1);
});

/** The returned teardown runs after the reveal has already torn down, leaving the target as it was. */
test('a teardown after the reveal is inert', () => {
  const s = stub({top: 0, height: 400});
  const stop = attachReveal(s.el, s.win, THRESHOLD);
  s.runFrames();

  stop();
  stop();
  assert.deepEqual(s.listeners, []);
  assert.deepEqual(s.classes, ['is-in']);
  assert.equal(s.counts.added, 2);
});

/** A window without an observer constructor reveals the target and registers nothing. */
test('a window without an observer reveals immediately', () => {
  const s = stub({top: 2000, height: 400}, {observer: false});
  const stop = attachReveal(s.el, s.win, THRESHOLD);
  assert.deepEqual(s.classes, ['is-in']);
  assert.equal(s.counts.added, 0);

  stop();
  assert.equal(s.counts.removed, 0);
});
