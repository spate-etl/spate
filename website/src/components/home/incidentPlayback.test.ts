import assert from 'node:assert/strict';
import {test} from 'node:test';
import {DURATION, EVENTS, INITIAL, phaseAt, playback} from './incidentPlayback.ts';

test('event selection pauses at each representative state', () => {
  assert.equal(INITIAL.playing, false);
  assert.equal(phaseAt(INITIAL.time).id, 'stall');
  for (const event of EVENTS) {
    const state = playback({...INITIAL, playing: true}, {type: 'select', time: event.time});
    assert.equal(state.playing, false);
    assert.equal(phaseAt(state.time).id, event.id);
    assert.deepEqual(playback(state, {type: 'tick', time: 11}), state);
  }
});

test('playback resumes, stops at the final frame and never wraps', () => {
  const playing = playback(INITIAL, {type: 'toggle'});
  assert.equal(playing.time, INITIAL.time);
  const paused = playback(playback(playing, {type: 'tick', time: 8}), {type: 'toggle'});
  assert.equal(paused.time, 8);
  assert.equal(paused.playing, false);
  const done = playback(playback(paused, {type: 'toggle'}), {type: 'tick', time: 20});
  assert.equal(done.time, DURATION);
  assert.equal(done.playing, false);
  assert.equal(phaseAt(done.time).id, 'done');
  assert.deepEqual(playback(done, {type: 'tick', time: 21}), done);
});

test('replay restarts the clock during an existing run', () => {
  const playing = playback(INITIAL, {type: 'toggle'});
  const replay = playback(playing, {type: 'replay'});
  assert.equal(replay.time, 0);
  assert.equal(replay.playing, true);
  assert.notEqual(replay.run, playing.run);
});

test('reduced motion stops playback but preserves event inspection', () => {
  const reduced = playback({...INITIAL, playing: true}, {type: 'motion', reduced: true});
  assert.equal(reduced.playing, false);
  assert.deepEqual(playback(reduced, {type: 'toggle'}), reduced);
  assert.deepEqual(playback(reduced, {type: 'replay'}), reduced);
  const selected = playback(reduced, {type: 'select', time: EVENTS[1].time});
  assert.equal(phaseAt(selected.time).id, 'bad');
  assert.equal(selected.playing, false);
  assert.equal(playback(selected, {type: 'motion', reduced: false}).playing, false);
});
