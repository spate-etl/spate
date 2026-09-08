// Tests for the on-screen geometry and the triggers wired around it.
//
// `./onScreen.ts` is imported with its extension because `node --test` strips
// types but does not resolve the bundler's extensionless imports.

import assert from 'node:assert/strict';
import {test} from 'node:test';

import {attachOnScreen, isOnScreen} from './onScreen.ts';

const VIEWPORT = 800;

/** A target is on screen from its first pixel to its last, and off screen either side. */
test('the measurement reads a rect against the viewport', () => {
  assert.equal(isOnScreen({top: 0, height: 400}, VIEWPORT), true);
  assert.equal(isOnScreen({top: 799, height: 400}, VIEWPORT), true);
  assert.equal(isOnScreen({top: -399, height: 400}, VIEWPORT), true);
  assert.equal(isOnScreen({top: 800, height: 400}, VIEWPORT), false);
  assert.equal(isOnScreen({top: -400, height: 400}, VIEWPORT), false);
});

/** A target and a window built from plain objects, recording what `attachOnScreen` does. */
function stub(rect: {top: number; height: number}, options: {observer?: boolean} = {}) {
  const reported: boolean[] = [];
  const listeners: {type: string; fn: () => void}[] = [];
  const frames = new Map<number, () => void>();
  const counts = {added: 0, removed: 0, cancelled: 0, frames: 0};
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
      counts.frames += 1;
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
    el: {getBoundingClientRect: () => rect},
    win,
    rect,
    reported,
    listeners,
    counts,
    observed,
    set: (onScreen: boolean) => reported.push(onScreen),
    fire(type: string) {
      for (const l of [...listeners]) if (l.type === type) l.fn();
    },
    runFrames() {
      for (const [key, fn] of [...frames]) {
        frames.delete(key);
        fn();
      }
    },
  };
}

/** A scroll reports the target that came into view while the observer stays silent. */
test('a scroll reports the target the observer never mentions', () => {
  const s = stub({top: 2000, height: 400});
  attachOnScreen(s.el, s.win, s.set);
  s.runFrames();
  assert.deepEqual(s.reported, [false]);

  s.rect.top = 100;
  s.fire('scroll');
  s.runFrames();
  assert.deepEqual(s.reported, [false, true]);
});

/** Scroll and resize between frames cost one measurement, not one each. */
test('triggers coalesce into a single frame', () => {
  const s = stub({top: 0, height: 400});
  attachOnScreen(s.el, s.win, s.set);
  s.fire('scroll');
  s.fire('resize');
  s.observed.callback?.();
  assert.equal(s.counts.frames, 1);

  s.runFrames();
  assert.deepEqual(s.reported, [true]);
});

/** The teardown disconnects the observer, drops the pending frame and both listeners. */
test('the teardown removes every trigger', () => {
  const s = stub({top: 0, height: 400});
  const stop = attachOnScreen(s.el, s.win, s.set);
  assert.equal(s.counts.added, 2);

  stop();
  assert.equal(s.counts.removed, s.counts.added);
  assert.deepEqual(s.listeners, []);
  assert.equal(s.observed.disconnects, 1);
  assert.equal(s.counts.cancelled, 1);
});

/** A window without an observer reports on screen, which is the state that keeps motion running. */
test('a window without an observer reports on screen', () => {
  const s = stub({top: 2000, height: 400}, {observer: false});
  const stop = attachOnScreen(s.el, s.win, s.set);
  assert.deepEqual(s.reported, [true]);
  assert.equal(s.counts.added, 0);

  stop();
  assert.equal(s.counts.removed, 0);
});
