import React from 'react';

import {
  ARROW,
  ARROW_GAP,
  ASCENT,
  DESCENT,
  Edge,
  GAP,
  HEAD_BOTTOM,
  Heads,
  LINE_BOX,
  Lifeline,
  Message,
  Node,
  QUEUE_T,
  QUEUE_W,
  Queue,
  R_NODE,
  R_STEP,
  ROW,
  Reject,
  S1,
  S2,
  SEQ_W,
  SequenceFigure,
  Step,
  Text,
  centredBaseline,
  chipW,
  linesBottom,
  styles,
  textW,
} from './parts';

const M = S1;
/** Lifeline pitch: the longest message label fits between a step disc and the next lifeline. */
const D = R_STEP + ARROW_GAP + 2 * S1 + textW('Source::resume');
const S = SEQ_W - M - textW('source') / 2;
const C = S - D;
const P = C - D;

const Y1 = HEAD_BOTTOM + GAP + LINE_BOX + S1 + QUEUE_T / 2;
const FULL_BASE = Y1 + QUEUE_T / 2 + S1 + ASCENT;
/** The other trigger sits beside step 1, between the pipeline and controller lifelines. */
const BYTES = ['or in-flight', 'bytes ≥ high', 'watermark'];
const BYTES_X = P + R_STEP + S2;
const BYTES_BASE = centredBaseline(Y1, BYTES.length);
const Y2 = Math.max(linesBottom(FULL_BASE), linesBottom(BYTES_BASE, BYTES.length)) + GAP + LINE_BOX + S1 + ARROW / 2;
const Y3 = Y2 + ROW;
const FRAME_TOP = Y3 + GAP;
const TAG = 'paused';
const TAG_H = LINE_BOX + S1;
const Y4 = FRAME_TOP + TAG_H + GAP;
const POLL_Y = Y4 + ROW;
const FRAME_BOTTOM = POLL_Y + GAP;
const Y5 = FRAME_BOTTOM + GAP + LINE_BOX + S1 + ARROW / 2;
const Y6 = Y5 + ROW;
const H = Y6 + R_STEP + M;

/**
 * The pipeline thread never blocks on a full queue: it asks the controller to pause its lanes, and
 * the controller keeps calling `poll_events` while they are paused (INV-2).
 */
function draw(id: string): React.ReactNode {
  const qx = M + QUEUE_W / 2;
  const arrowEnd = M + QUEUE_W + ARROW_GAP;
  return (
    <>
      <Heads
        heads={[
          {x: P, label: ['pipeline', 'thread']},
          {x: C, label: ['controller', 'thread']},
        ]}
      />
      <Node x={S} y={HEAD_BOTTOM - R_NODE} />
      <Text kind="label" x={S} y={HEAD_BOTTOM - 2 * R_NODE - S1 - DESCENT} lines="source" anchor="middle" />

      {[P, C, S].map((x) => (
        <Lifeline key={x} x={x} end={Y6} />
      ))}

      <Queue x={qx} y={Y1} full />
      <Text x={M} y={FULL_BASE} lines="full queue" />
      <Text x={M} y={Y1 - QUEUE_T / 2 - S1 - DESCENT} lines="try_send" />
      <Edge id={id} points={[[P - R_STEP, Y1], [arrowEnd, Y1]]} />
      <Reject x={(P - R_STEP + arrowEnd) / 2} y={Y1} />
      <Step x={P} y={Y1} n={1} />
      <Text x={BYTES_X} y={BYTES_BASE} lines={BYTES} />

      <Message id={id} kind="control" from={P} to={C} y={Y2} label="pause request" step={2} />
      <Message id={id} kind="control" from={C} to={S} y={Y3} label="Source::pause" step={3} />

      <rect className={styles.frame} x={M} y={FRAME_TOP} width={SEQ_W - 2 * M} height={FRAME_BOTTOM - FRAME_TOP} rx="3" />
      <rect className={styles.frameTag} x={M} y={FRAME_TOP} width={chipW(TAG)} height={TAG_H} rx="3" />
      <Text x={M + chipW(TAG) / 2} y={centredBaseline(FRAME_TOP + TAG_H / 2)} lines={TAG} anchor="middle" />
      <Step x={P} y={Y4} n={4} />
      <Text x={P + R_STEP + S2} y={centredBaseline(Y4)} lines={['lanes not', 'polled']} />
      <Message id={id} kind="control" from={C} to={S} y={POLL_Y} label="poll_events" />

      <Message id={id} kind="control" from={P} to={C} y={Y5} label="resume request" step={5} />
      <Message id={id} kind="control" from={C} to={S} y={Y6} label="Source::resume" step={6} />
    </>
  );
}

/** Backpressure on the Architecture concept page, as numbered steps across three participants. */
export default function BackpressureDiagram(): React.JSX.Element {
  return (
    <SequenceFigure
      name="Backpressure: a pause request goes through the controller to the source"
      viewBox={`0 0 ${SEQ_W} ${H}`}
      heading="Backpressure without blocking"
      legend={['lifeline', 'source', 'data', 'queue', 'reject', 'control', 'frame']}
      draw={draw}
      steps={[
        <>
          A full shard queue refuses <code>try_send</code>, or the in-flight byte budget crosses its
          high watermark.
        </>,
        <>The pipeline thread sends a pause request for its lanes to the controller. It never blocks on a channel send.</>,
        <>
          The controller calls <code>Source::pause</code>. Only the controller thread touches the{' '}
          <code>Source</code>.
        </>,
        <>
          While paused, the pipeline thread loops without polling its lanes, and the controller keeps
          calling <code>poll_events</code>, so the source stays live (INV-2).
        </>,
        <>
          The pipeline thread sends a resume request once three things hold: in-flight bytes are at or
          below the low watermark, every shard queue has drained below its low mark, and at least{' '}
          <code>min_pause</code> has passed since the later of the pause and the last refused{' '}
          <code>try_send</code>.
        </>,
        <>
          The controller calls <code>Source::resume</code>.
        </>,
      ]}
    />
  );
}
