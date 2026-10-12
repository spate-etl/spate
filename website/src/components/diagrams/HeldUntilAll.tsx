import React from 'react';

import {
  ARROW,
  Bar,
  Edge,
  GAP,
  HEAD_BOTTOM,
  Heads,
  LINE_BOX,
  Lifeline,
  Message,
  RAIL,
  ROW,
  S1,
  S2,
  SEQ_W,
  SequenceFigure,
  Step,
  Text,
  centredBaseline,
  chipW,
} from './parts';

const M = S1;
const HEADS = [['pipeline', 'thread'], ['refunds', 'sink'], ['payments', 'sink'], ['controller', 'thread']];
const FIRST = M + chipW(HEADS[0]) / 2;
const LAST = SEQ_W - M - chipW(HEADS[3]) / 2;
/** Four lifelines, evenly spaced. */
const D = (LAST - FIRST) / 3;
const [P, REF, PAY, C] = [0, 1, 2, 3].map((i) => FIRST + i * D);

const Y1 = HEAD_BOTTOM + GAP + LINE_BOX + S1 + ARROW / 2;
const Y2 = Y1 + ROW;
const Y3 = Y2 + ROW;
/** The refunds sink lingers a row longer than the payments sink took to write. */
const Y4 = Y3 + 2 * ROW;
const Y5 = Y4 + ROW;
const Y6 = Y5 + ROW;
const H = Y6 + M;
/** The bars stop clear of the step discs at their ends. */
const WAIT: [number, number] = [Y2 + S2, Y4 - S2 - S1];
const HOLD: [number, number] = [Y3 + S2, Y4 - S2 - S1];

/**
 * One poll batch fans out to two branches, and its watermark cannot move until the slower branch
 * has written. This is the hold the checkpoint-lag trade further down the page is about.
 */
function draw(id: string): React.ReactNode {
  return (
    <>
      <Heads heads={HEADS.map((label, i) => ({x: [P, REF, PAY, C][i], label}))} />
      {[P, REF, PAY, C].map((x) => (
        <Lifeline key={x} x={x} end={Y6} />
      ))}

      <Message id={id} from={P} to={PAY} y={Y1} label="rows" over={[REF, PAY]} step={1} />
      <Message id={id} from={P} to={REF} y={Y2} label="rows" step={2} />

      <Bar kind="wait" x={REF} y0={WAIT[0]} y1={WAIT[1]} />
      <Text x={REF - S1 - S2} y={centredBaseline((WAIT[0] + WAIT[1]) / 2)} lines="linger" anchor="end" />

      <Message id={id} kind="ack" from={PAY} to={C} y={Y3} label="ack" step={3} />
      <Bar kind="hold" x={C} y0={HOLD[0]} y1={HOLD[1]} />
      <Text x={C + S1 + S2} y={centredBaseline((HOLD[0] + HOLD[1]) / 2)} lines="held" />

      <Message id={id} kind="ack" from={REF} to={C} y={Y4} label="last ack" over={[PAY, C]} step={4} />

      <Edge id={id} kind="mark" points={[[C, Y5], [C, Y6]]} head={false} />
      <Text x={C - RAIL / 2 - S2} y={centredBaseline((Y5 + Y6) / 2)} lines="commit" anchor="end" />
      <Step x={C} y={Y5} n={5} />
    </>
  );
}

/** The held-until-all hold on the Multi-sink concept page, as numbered steps. */
export default function HeldUntilAllDiagram(): React.JSX.Element {
  return (
    <SequenceFigure
      name="Held until all: a batch commits after every branch it reached has written"
      viewBox={`0 0 ${SEQ_W} ${H}`}
      heading="A batch is held until every branch it reached has written"
      legend={['lifeline', 'data', 'ack', 'wait', 'hold', 'mark']}
      draw={draw}
      steps={[
        <>
          The pipeline thread splits one poll batch. Its records for <code>&quot;payments&quot;</code>{' '}
          carry a clone of the batch&apos;s <code>AckRef</code>.
        </>,
        <>
          Its records for <code>&quot;refunds&quot;</code> carry another clone. A branch that gets no
          record from the batch holds no clone.
        </>,
        <>
          <code>&quot;payments&quot;</code> writes and resolves its clones. The batch stays held, since{' '}
          <code>&quot;refunds&quot;</code> has not written.
        </>,
        <>
          <code>&quot;refunds&quot;</code> writes when its <code>linger</code> fires, which resolves the
          last clone.
        </>,
        <>
          The watermark moves past the batch, and the controller&apos;s next commit includes it. A failed write
          in either branch fails the whole batch instead.
        </>,
      ]}
    />
  );
}
