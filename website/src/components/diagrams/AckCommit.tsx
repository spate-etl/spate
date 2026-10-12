import React from 'react';

import {
  ARROW_GAP,
  ASCENT,
  DESCENT,
  Edge,
  GAP,
  HEAD_BOTTOM,
  Heads,
  LINE,
  LINE_BOX,
  Lifeline,
  Message,
  Node,
  R_NODE,
  R_STEP,
  RAIL,
  ROW,
  S1,
  S2,
  SEQ_W,
  SequenceFigure,
  Step,
  STROKE,
  Text,
  centredBaseline,
  chipW,
  textW,
} from './parts';

const M = S1;
const CONTROLLER = ['controller', 'checkpointer'];
const SRC = M + textW('source') / 2;
const CK = SEQ_W - M - chipW(CONTROLLER) / 2;
/** Four lifelines, evenly spaced between the source and the controller. */
const D = (CK - SRC) / 3;
const P = SRC + D;
const W = P + D;

const Y1 = HEAD_BOTTOM + GAP + LINE_BOX + S1 + STROKE / 2;
const ACKREF = ['one AckRef,', 'cloned into', 'each record'];
const Y2 = Y1 + R_STEP + S1 + ACKREF.length * LINE + GAP + LINE_BOX;
/** The write loops back to the worker's lifeline one step below where it leaves. */
const LOOP = 2 * GAP;
const Y3 = Y2 + ROW;
const Y4 = Y3 + LOOP + ROW;
const Y5 = Y4 + LINE_BOX + 2 * GAP;
const Y6 = Y5 + ROW + GAP;
const H = Y6 + GAP + M;
const COMMIT = 'Source::commit';

/**
 * The ack and commit loop as numbered steps: nothing commits past an unresolved batch (INV-1), and
 * acks travel an unbounded channel, never behind data (INV-4).
 */
function draw(id: string): React.ReactNode {
  // The commit label is wider than a lifeline pitch, so it sits over the pipeline thread's
  // lifeline, which breaks around it.
  const labelTop = Y6 - RAIL / 2 - S1 - LINE_BOX;
  return (
    <>
      <Node x={SRC} y={HEAD_BOTTOM - R_NODE} />
      <Text kind="label" x={SRC} y={HEAD_BOTTOM - 2 * R_NODE - S1 - DESCENT} lines="source" anchor="middle" />
      <Heads
        heads={[
          {x: P, label: ['pipeline', 'thread']},
          {x: W, label: ['shard', 'worker']},
          {x: CK, label: CONTROLLER},
        ]}
      />

      <Lifeline x={SRC} spans={[[HEAD_BOTTOM, H - M]]} />
      <Lifeline
        x={P}
        spans={[
          [HEAD_BOTTOM, labelTop - S1],
          [Y6 - RAIL / 2, H - M],
        ]}
      />
      <Lifeline x={W} spans={[[HEAD_BOTTOM, H - M]]} />
      <Lifeline x={CK} spans={[[HEAD_BOTTOM, Y5], [Y6, H - M]]} />

      <Message id={id} from={SRC} to={P} y={Y1} label="batch" step={1} />
      <Text x={P + S2} y={Y1 + R_STEP + S1 + ASCENT} lines={ACKREF} />

      <Message id={id} from={P} to={W} y={Y2} label="chunk" step={2} />

      <Edge
        id={id}
        points={[
          [W + R_STEP, Y3],
          [W + R_STEP + GAP, Y3],
          [W + R_STEP + GAP, Y3 + LOOP],
          [W + ARROW_GAP, Y3 + LOOP],
        ]}
      />
      <Text x={W + R_STEP + GAP + S2} y={centredBaseline(Y3 + LOOP / 2)} lines="write" />
      <Step x={W} y={Y3} n={3} />

      <Message id={id} kind="ack" from={W} to={CK} y={Y4} label="ack" step={4} />
      <Text x={(W + R_STEP + CK) / 2} y={Y4 + STROKE / 2 + S1 + ASCENT} lines="unbounded" anchor="middle" />

      <Edge id={id} kind="mark" points={[[CK, Y5], [CK, Y6]]} head={false} />
      <Text x={CK - RAIL / 2 - S2} y={centredBaseline((Y5 + Y6) / 2)} lines="watermark" anchor="end" />
      <Step x={CK} y={Y5} n={5} />

      <Message id={id} kind="mark" from={CK} to={SRC} y={Y6} label={COMMIT} over={[SRC, W]} step={6} />
    </>
  );
}

/** The write-to-commit sequence on the Architecture concept page. */
export default function AckCommitDiagram(): React.JSX.Element {
  return (
    <SequenceFigure
      name="Acks and commits: from a poll batch to Source::commit"
      viewBox={`0 0 ${SEQ_W} ${H}`}
      heading="From write to commit"
      legend={['lifeline', 'source', 'data', 'ack', 'mark']}
      draw={draw}
      steps={[
        <>
          A lane poll returns a batch. The pipeline thread makes one <code>AckRef</code> for it and
          clones it into every record derived from the batch.
        </>,
        <>Records travel to a shard worker in chunks, each record still holding its clone.</>,
        <>The shard worker writes the batch it sealed, retrying until it succeeds or gives up.</>,
        <>
          The write outcome resolves the clones. When the last one drops, the batch&apos;s outcome
          reaches the checkpointer over an unbounded channel, so an ack never waits behind data
          (INV-4).
        </>,
        <>
          The checkpointer advances each partition&apos;s watermark over contiguous resolved batches
          only. A failed or pending batch holds it.
        </>,
        <>
          On its commit tick the controller hands the watermark to <code>Source::commit</code>, so
          nothing commits past unacknowledged data (INV-1).
        </>,
      ]}
    />
  );
}
