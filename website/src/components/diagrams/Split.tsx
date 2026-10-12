import React from 'react';

import {
  ARROW_GAP,
  ASCENT,
  CHIP_H,
  Chip,
  DESCENT,
  Edge,
  GAP,
  Join,
  LINE,
  LINE_BOX,
  Node,
  OverviewFigure,
  PAD,
  STROKE,
  QUEUE_GAP,
  QUEUE_W,
  Queue,
  R_JOIN,
  R_NODE,
  Region,
  Run,
  S1,
  S2,
  Text,
  TITLE_ASCENT,
  TITLE_DESCENT,
  TITLE_BOX,
  centredBaseline,
  chipW,
  columnOf,
  linesBottom,
  rowOf,
  textW,
  type Point,
} from './parts';

const NAME = 'Split terminal: each branch acks into one batch ack, which commits only after every branch has written.';

const TRUNK = ['deserialize', 'operators', 'classify'];
const BRANCH = ['route', 'encode'];
const NAMES = ['payments', 'refunds'];
const FAIL = ['Fail: stops', 'the pipeline'];
const SKIP = ['Skip: drops', 'and counts'];
const M = S1;

/** A branch's acks leave its worker's foot this far left of the column, clear of its destination. */
const ACK_DX = R_NODE + S2;
/** Column pitch: a destination's name fits between its node and the next column's ack line. */
const PITCH = R_NODE + S2 + textW('payments') + GAP + ACK_DX;
const COL_W = chipW(BRANCH);

type Frame = {
  /** Branch column centres: payments, refunds, unmatched. */
  cols: number[];
  /** Where the trunk hands records to the branches, and the line the fan-out runs along. */
  fanFrom: Point;
  busY: number;
  srcX: number;
  srcBottom: number;
  pl: number;
  pt: number;
  skipLabel: string[];
};

/** The positions below the fan-out, derived from a frame. */
function geometry(f: Frame) {
  const [pay, ref, un] = f.cols;
  const routeY = f.busY + 2 * GAP + CHIP_H / 2;
  const failBase = routeY + CHIP_H / 2 + S1 + ASCENT;
  const textBottom = linesBottom(failBase, FAIL.length + SKIP.length);
  const pr = un + Math.max(chipW('unmatched'), ...[...FAIL, ...SKIP].map(textW)) / 2 + PAD;
  const pb = textBottom + PAD;
  const bandTop = pb + QUEUE_GAP;
  // The runtime's title sits over the gap between the two workers, clear of both columns.
  const bandH = PAD + TITLE_BOX + GAP + CHIP_H + PAD;
  const destY = bandTop + bandH + 2 * GAP + R_NODE;
  const joinY = destY + R_NODE + GAP + S2;
  const ct = joinY + S2 + GAP;
  const cpY = ct + PAD + CHIP_H / 2;
  const cpR = un - PAD - 2 * S2 - textW('unbounded');
  // The rail's label sits under the checkpointer's foot, so it can run beneath the chip.
  const commitBase = cpY + CHIP_H / 2 + S1 + ASCENT;
  const cb = linesBottom(commitBase, 2) + GAP + TITLE_BOX + PAD;
  return {
    pay,
    ref,
    un,
    routeY,
    failBase,
    textBottom,
    pr,
    pb,
    encodeBottom: routeY + CHIP_H + GAP + CHIP_H / 2,
    qy: pb + QUEUE_GAP / 2,
    bandTop,
    bandH,
    wy: bandTop + bandH - PAD - CHIP_H / 2,
    bandL: pay - COL_W / 2 - PAD,
    bandR: ref + COL_W / 2 + PAD,
    destY,
    joinY,
    ct,
    cpY,
    cpR,
    cpX: cpR - chipW('checkpointer'),
    commitBase,
    cb,
    height: cb + M,
  };
}

/** Everything from the fan-out down: branch columns, workers, destinations, the ack fan-in and the controller. */
function Branches({id, f}: {id: string; f: Frame}): React.JSX.Element {
  const g = geometry(f);
  const {pay, ref, un, routeY, failBase, textBottom, pr, pb, encodeBottom, qy, bandTop, bandH, wy, bandL, bandR} = g;
  const {destY, joinY, ct, cpY, cpR, cpX, commitBase, cb} = g;
  return (
    <>
      <Region x={f.pl} y={f.pt} w={pr - f.pl} h={pb - f.pt} />
      <Region x={bandL} y={bandTop} w={bandR - bandL} h={bandH} />
      <Region x={f.pl} y={ct} w={cpR + PAD - f.pl} h={cb - ct} />

      <Text kind="title" x={f.pl + PAD} y={f.pt + PAD + TITLE_ASCENT} lines="pipeline thread" />
      <Text kind="title" x={pay + S2} y={bandTop + PAD + TITLE_ASCENT} lines="I/O runtime" />
      <Text kind="title" x={f.pl + PAD} y={cb - PAD - TITLE_DESCENT} lines="controller thread" />

      {f.cols.map((x) => (
        <Edge key={x} id={id} points={[f.fanFrom, [f.fanFrom[0], f.busY], [x, f.busY], [x, routeY - CHIP_H / 2 - ARROW_GAP]]} />
      ))}

      {[pay, ref].map((x, i) => (
        <React.Fragment key={NAMES[i]}>
          <Text x={x + S2} y={centredBaseline(f.busY + GAP)} lines={`"${NAMES[i]}"`} />
          <Run id={id} labels={BRANCH} boxes={columnOf(2, x - COL_W / 2, routeY, COL_W)} vertical />
          <Edge id={id} points={[[x, encodeBottom], [x, qy - QUEUE_W / 2]]} head={false} />
          <Queue x={x} y={qy} vertical />
          <Edge id={id} points={[[x, qy + QUEUE_W / 2], [x, wy - CHIP_H / 2 - ARROW_GAP]]} />
          <Chip x={x - COL_W / 2} y={wy} w={COL_W} label="worker" />
          <Edge id={id} points={[[x, wy + CHIP_H / 2], [x, destY - R_NODE - ARROW_GAP]]} />
          <Node x={x} y={destY} />
          <Text kind="label" x={x + R_NODE + S2} y={centredBaseline(destY)} lines={NAMES[i]} />
        </React.Fragment>
      ))}

      <Chip x={un - chipW('unmatched') / 2} y={routeY} label="unmatched" />
      <Text kind="label" x={un} y={failBase} lines={FAIL} anchor="middle" />
      <Text x={un} y={failBase + FAIL.length * LINE} lines={SKIP} anchor="middle" />

      {/* The fan-in: each branch's worker resolves its acks, and a skipped record releases its share. */}
      {[pay, ref].map((x, i) => {
        const row = joinY + (i === 0 ? S2 : -S2);
        return (
          <Edge
            key={x}
            id={id}
            kind="ack"
            head={false}
            points={[
              [x - ACK_DX, wy + CHIP_H / 2],
              [x - ACK_DX, row],
              [un - GAP, row],
              [un - R_JOIN, joinY + (i === 0 ? R_JOIN : -R_JOIN) / 2],
            ]}
          />
        );
      })}
      <Edge id={id} kind="ack" head={false} points={[[un, textBottom + S1], [un, joinY - R_JOIN]]} />
      <Text x={un + S2} y={centredBaseline(wy, f.skipLabel.length)} lines={f.skipLabel} />
      <Join x={un} y={joinY} />
      <Text kind="label" x={un + R_JOIN + S2} y={centredBaseline(joinY, 2)} lines={['batch', 'ack']} />

      <Edge id={id} kind="ack" points={[[un, joinY + R_JOIN], [un, cpY], [cpR + ARROW_GAP, cpY]]} />
      <Text x={(cpR + PAD + un) / 2} y={cpY - STROKE / 2 - S1 - DESCENT} lines="unbounded" anchor="middle" />
      <Chip x={cpX} y={cpY} label="checkpointer" />
      <Edge id={id} kind="mark" points={[[cpX, cpY], [f.srcX, cpY], [f.srcX, f.srcBottom + ARROW_GAP]]} />
      <Text kind="label" x={f.pl + PAD} y={commitBase} lines="Source::commit" />
      <Text x={f.pl + PAD} y={commitBase + LINE} lines="watermarks" />
    </>
  );
}

// ---- landscape: the trunk runs along the top, the branches hang below its classifier ----

const W_SRC_X = M + textW('source') / 2;
const W_PL = W_SRC_X + R_NODE + 2 * GAP;
const W_PT = M;
const W_Y = W_PT + PAD + TITLE_BOX + GAP + CHIP_H / 2;
const W_TRUNK = rowOf(TRUNK, W_PL + PAD, W_Y);
const W_CLASSIFY = W_TRUNK[2];
const W_PAY = W_CLASSIFY.x + W_CLASSIFY.w / 2;
const W_COLS = [W_PAY, W_PAY + PITCH, W_PAY + 2 * PITCH];
const W_SKIP = ['skip:', 'ack share'];
const W_WIDTH = W_COLS[2] + S2 + Math.max(...W_SKIP.map(textW)) + M;

function wideFrame(): Frame {
  return {
    cols: W_COLS,
    fanFrom: [W_PAY, W_Y + CHIP_H / 2],
    busY: W_Y + CHIP_H / 2 + GAP,
    srcX: W_SRC_X,
    srcBottom: W_Y + R_NODE,
    pl: W_PL,
    pt: W_PT,
    skipLabel: W_SKIP,
  };
}

function wide(id: string): React.ReactNode {
  return (
    <>
      <Branches id={id} f={wideFrame()} />
      <Node x={W_SRC_X} y={W_Y} />
      <Text kind="label" x={W_SRC_X} y={W_Y - R_NODE - S1 - DESCENT} lines="source" anchor="middle" />
      <Edge id={id} points={[[W_SRC_X + R_NODE, W_Y], [W_TRUNK[0].x - ARROW_GAP, W_Y]]} />
      <Run id={id} labels={TRUNK} boxes={W_TRUNK} />
    </>
  );
}

// ---- portrait: the trunk runs down the middle, over the branch columns ----

const N_W = 360;
const N_SKIP = ['skip:', 'ack', 'share'];
const N_UN = N_W - M - Math.max(chipW('unmatched'), ...[...FAIL, ...SKIP].map(textW)) / 2 - PAD;
const N_COLS = [N_UN - 2 * PITCH, N_UN - PITCH, N_UN];
const N_SRC_X = M + 2 * S2;
const N_SRC_Y = M + LINE_BOX + S1 + STROKE / 2;
const N_PL = N_SRC_X + 2 * S2;
const N_PT = N_SRC_Y + 2 * GAP;
const N_TX = N_COLS[1];
const N_TRUNK_W = chipW(TRUNK);
const N_TRUNK = columnOf(3, N_TX - N_TRUNK_W / 2, N_PT + PAD + TITLE_BOX + GAP + CHIP_H / 2, N_TRUNK_W);
const N_TRUNK_END = N_TRUNK[2].y + CHIP_H / 2;

function narrowFrame(): Frame {
  return {
    cols: N_COLS,
    fanFrom: [N_TX, N_TRUNK_END],
    busY: N_TRUNK_END + GAP,
    srcX: N_SRC_X,
    srcBottom: N_SRC_Y + R_NODE,
    pl: N_PL,
    pt: N_PT,
    skipLabel: N_SKIP,
  };
}

function narrow(id: string): React.ReactNode {
  return (
    <>
      <Branches id={id} f={narrowFrame()} />
      <Node x={N_SRC_X} y={N_SRC_Y} />
      <Text kind="label" x={N_SRC_X + R_NODE + S2} y={N_SRC_Y - STROKE / 2 - S1 - DESCENT} lines="source" />
      <Edge
        id={id}
        points={[
          [N_SRC_X + R_NODE, N_SRC_Y],
          [N_TX, N_SRC_Y],
          [N_TX, N_TRUNK[0].y - CHIP_H / 2 - ARROW_GAP],
        ]}
      />
      <Run id={id} labels={TRUNK} boxes={N_TRUNK} vertical />
    </>
  );
}

/** The split terminal on the Multi-sink concept page, drawn as fan-out and the matching ack fan-in. */
export default function SplitDiagram(): React.JSX.Element {
  return (
    <OverviewFigure
      name={NAME}
      wide={{viewBox: `0 0 ${W_WIDTH} ${geometry(wideFrame()).height}`, draw: wide}}
      narrow={{viewBox: `0 0 ${N_W} ${geometry(narrowFrame()).height}`, draw: narrow}}
      legend={['data', 'ack', 'mark', 'region', 'chip', 'queue', 'node', 'join']}
      caption={
        <>
          One pipeline thread splits a stream across two destinations. After deserialize and the
          operator chain, one classifier sends each record to one branch, here payments or refunds.
          Each branch routes the record to a shard and encodes it into that sink&apos;s own queues and
          shard workers. A record that reaches no branch follows the unmatched policy: Fail stops the
          pipeline, Skip drops and counts it and releases its share of the ack. Every branch holds a
          clone of the poll batch&apos;s ack, and each branch&apos;s shard worker resolves its clones
          from the write outcome, so the acks fan back in to one batch ack. The batch commits through
          Source::commit only after every branch it reached has written.
        </>
      }
    />
  );
}
