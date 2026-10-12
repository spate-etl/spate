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
  QUEUE_GAP,
  QUEUE_W,
  Queue,
  R_JOIN,
  R_NODE,
  Region,
  Run,
  S1,
  S2,
  STROKE,
  Text,
  TITLE_ASCENT,
  TITLE_BOX,
  TITLE_LINE,
  centredBaseline,
  chipW,
  columnOf,
  linesBottom,
  rowOf,
  textW,
  titleW,
  type Point,
} from './parts';

const NAME = 'Split terminal: each branch acks into one batch ack, which commits only after every branch it reached has written.';

const TRUNK = ['deserialize', 'operators', 'classify'];
const BRANCH = ['route', 'encode'];
const NAMES = ['payments', 'refunds'];
const M = S1;
const COL_W = chipW(BRANCH);
/**
 * A worker's acks leave the middle of its right edge and turn down this far from its column: `S2`
 * past the runtime band's padding, so the last column's acks turn down outside the band.
 */
const ACK_DX = COL_W / 2 + PAD + S2;
/** Column pitch in the portrait layout: a branch's quoted name fits between its drop and the next column's. */
const PITCH = textW(`"${NAMES[0]}"`) + 2 * S2;
const CP = 'checkpointer';
const CTRL_TITLE = 'controller thread';

/** A layout's positions from the queues down, which both layouts draw the same way. */
type Lower = {
  pay: number;
  ref: number;
  joinX: number;
  /**
   * The ack that drops into the join from above; the other two arrive from the left. With `ref`,
   * `joinX` is the refunds worker's ack column.
   */
  above: 'skip' | 'ref';
  /** The skip share's path up to its last turn down, which ends at `joinX` when it drops from above. */
  skipFrom: Point[];
  joinLabel: string[];
  /**
   * The controller sits under the join, its right edge `GAP` left of the return, which carries its
   * label beside it. Otherwise the controller sits at `pl` and the return runs along its row.
   */
  ctrlUnderJoin: boolean;
  /** The pipeline region's bottom edge. */
  pb: number;
  band: {x: number; y: number; w: number; h: number};
  bandTitle: string[];
  /** The workers' centre line. */
  wy: number;
  pl: number;
  srcX: number;
  srcBottom: number;
};

/** The y of each part below the I/O runtime band. */
function lowerRows(l: Lower) {
  const destY = l.band.y + l.band.h + 2 * GAP + R_NODE;
  const destBase = destY + R_NODE + S1 + ASCENT;
  // The fan-in rows sit `S2` apart around the join, the first a gap below the destinations' names.
  const joinY = linesBottom(destBase) + GAP + S2;
  const ct = joinY + S2 + GAP;
  const cpY = ct + PAD + TITLE_BOX + GAP + CHIP_H / 2;
  const commitBase = cpY + CHIP_H / 2 + S1 + ASCENT;
  const cb = linesBottom(commitBase, 2) + PAD;
  return {destY, destBase, joinY, ct, cpY, commitBase, cb, height: cb + M};
}

/** An ack line arriving at the join from the left on the row `dy` off its centre. */
function intoJoin(l: Lower, joinY: number, dy: number): Point[] {
  return [
    [l.joinX - GAP, joinY + dy],
    [l.joinX - R_JOIN, joinY + (dy / S2) * (R_JOIN / 2)],
  ];
}

/**
 * Everything from the queues down: workers, destinations, the ack fan-in, the batch ack and the
 * controller. Each branch's records run straight down its column; its acks leave its worker's right
 * edge.
 */
function LowerPart({id, l}: {id: string; l: Lower}): React.JSX.Element {
  const {pay, ref, joinX, band, wy} = l;
  const {destY, destBase, joinY, ct, cpY, commitBase, cb} = lowerRows(l);
  const ctrlW = PAD + titleW(CTRL_TITLE) + PAD;
  const cr = l.ctrlUnderJoin ? joinX - GAP : l.pl + ctrlW;
  const cl = cr - ctrlW;
  const cpX = cr - PAD - chipW(CP);
  const qy = l.pb + QUEUE_GAP / 2;
  // Of the acks arriving from the left, the one from further right takes the upper row, so no two
  // cross.
  const rows: Record<'pay' | 'ref' | 'skip', number> =
    l.above === 'skip' ? {ref: -S2, pay: S2, skip: 0} : {ref: 0, pay: -S2, skip: S2};
  const workerAck = (x: number, dy: number, fromAbove: boolean): Point[] => [
    [x + COL_W / 2, wy],
    [x + ACK_DX, wy],
    ...(fromAbove ? [[x + ACK_DX, joinY - R_JOIN] as Point] : [[x + ACK_DX, joinY + dy] as Point, ...intoJoin(l, joinY, dy)]),
  ];
  const skipTurn = l.skipFrom[l.skipFrom.length - 1];
  return (
    <>
      <Region x={band.x} y={band.y} w={band.w} h={band.h} />
      <Region x={cl} y={ct} w={cr - cl} h={cb - ct} />
      <Text kind="title" x={band.x + PAD} y={band.y + PAD + TITLE_ASCENT} lines={l.bandTitle} />
      <Text kind="title" x={cl + PAD} y={ct + PAD + TITLE_ASCENT} lines={CTRL_TITLE} />

      {[pay, ref].map((x, i) => (
        <React.Fragment key={NAMES[i]}>
          <Queue x={x} y={qy} vertical />
          <Edge id={id} points={[[x, qy + QUEUE_W / 2], [x, wy - CHIP_H / 2 - ARROW_GAP]]} />
          <Chip x={x - COL_W / 2} y={wy} w={COL_W} label="worker" />
          <Edge id={id} points={[[x, wy + CHIP_H / 2], [x, destY - R_NODE - ARROW_GAP]]} />
          <Node x={x} y={destY} />
          <Text kind="label" x={x} y={destBase} lines={NAMES[i]} anchor="middle" />
        </React.Fragment>
      ))}

      {/* The fan-in: each worker resolves its branch's acks, and a skipped record releases its share. */}
      <Edge id={id} kind="ack" head={false} points={workerAck(pay, rows.pay, false)} />
      <Edge id={id} kind="ack" head={false} points={workerAck(ref, rows.ref, l.above === 'ref')} />
      <Edge
        id={id}
        kind="ack"
        head={false}
        points={
          l.above === 'skip'
            ? [...l.skipFrom, [joinX, joinY - R_JOIN]]
            : [...l.skipFrom, [skipTurn[0], joinY + rows.skip], ...intoJoin(l, joinY, rows.skip)]
        }
      />
      <Join x={joinX} y={joinY} />
      <Text kind="label" x={joinX + R_JOIN + S2} y={centredBaseline(joinY, l.joinLabel.length)} lines={l.joinLabel} />

      <Edge id={id} kind="ack" points={[[joinX, joinY + R_JOIN], [joinX, cpY], [cr - PAD + ARROW_GAP, cpY]]} />
      {l.ctrlUnderJoin ? (
        <Text x={joinX + STROKE / 2 + S2} y={centredBaseline((joinY + R_JOIN + cpY) / 2)} lines="unbounded" />
      ) : (
        <Text x={(cr + joinX) / 2} y={cpY - STROKE / 2 - S1 - DESCENT} lines="unbounded" anchor="middle" />
      )}
      <Chip x={cpX} y={cpY} label={CP} />
      <Edge id={id} kind="mark" points={[[cpX, cpY], [l.srcX, cpY], [l.srcX, l.srcBottom + ARROW_GAP]]} />
      {/* Under the checkpointer's foot, so the rail's labels can run beneath the chip. */}
      <Text kind="label" x={cl + PAD} y={commitBase} lines="Source::commit" />
      <Text x={cl + PAD} y={commitBase + LINE} lines="watermarks" />
    </>
  );
}

/** The branch columns' stages and the fan-out that feeds them and the unmatched box at `un`. */
function Branches({id, l, un, fanFrom, busY, routeY}: {id: string; l: Lower; un: number; fanFrom: Point; busY: number; routeY: number}) {
  const encodeBottom = routeY + CHIP_H / 2 + GAP + CHIP_H;
  return (
    <>
      {[l.pay, l.ref, un].map((x) => (
        <Edge key={x} id={id} points={[fanFrom, [fanFrom[0], busY], [x, busY], [x, routeY - CHIP_H / 2 - ARROW_GAP]]} />
      ))}
      {[l.pay, l.ref].map((x) => (
        <React.Fragment key={x}>
          <Run id={id} labels={BRANCH} boxes={columnOf(2, x - COL_W / 2, routeY, COL_W)} vertical />
          <Edge id={id} points={[[x, encodeBottom], [x, l.pb + QUEUE_GAP / 2 - QUEUE_W / 2]]} head={false} />
        </React.Fragment>
      ))}
    </>
  );
}

// ---- landscape: the trunk runs along the top, the unmatched branch fills the region's lower left
// and its skip share joins the fan-in along the bottom row. The refunds ack drops into the batch
// ack, and the controller sits under it. ----

const W_SRC_X = M + textW('source') / 2;
const W_PL = W_SRC_X + R_NODE + 2 * GAP;
const W_PT = M;
const W_Y = W_PT + PAD + TITLE_BOX + GAP + CHIP_H / 2;
const W_TRUNK = rowOf(TRUNK, W_PL + PAD, W_Y);
const W_CLASSIFY = W_TRUNK[2];
/** Payments drops straight from the classifier. */
const W_PAY = W_CLASSIFY.x + W_CLASSIFY.w / 2;
/**
 * Each quoted name sits left of its drop, `S2` from it. The pitch keeps the refunds name `2 * GAP`
 * from the payments drop, so it reads as its own drop's.
 */
const W_REF = W_PAY + 2 * GAP + textW(`"${NAMES[1]}"`) + S2;
const W_BUS = W_Y + CHIP_H / 2 + GAP;
/** The drops run long enough to hold the quoted names `GAP` clear of the bus and the route boxes. */
const W_NAME_Y = W_BUS + GAP + LINE_BOX / 2;
const W_ROUTE_Y = W_NAME_Y + LINE_BOX / 2 + GAP + CHIP_H / 2;
const W_POLICY = ['Fail: stops the pipeline', 'Skip: drops and counts'];
const W_UN_X = W_PL + PAD;
const W_UN = W_UN_X + chipW('unmatched') / 2;
const W_POLICY_BASE = W_ROUTE_Y + CHIP_H / 2 + S1 + ASCENT;
const W_POLICY_BOTTOM = linesBottom(W_POLICY_BASE, W_POLICY.length);
const W_PB = Math.max(W_POLICY_BOTTOM, W_ROUTE_Y + CHIP_H / 2 + GAP + CHIP_H) + PAD;
const W_PR = Math.max(W_CLASSIFY.x + W_CLASSIFY.w, W_REF + COL_W / 2) + PAD;
/** The runtime's title sits left of its first worker, in the band's top-left corner. */
const W_IO_TITLE = 'I/O runtime';
const W_BAND_L = W_PAY - COL_W / 2 - GAP - titleW(W_IO_TITLE) - PAD;
const W_BAND_TOP = W_PB + QUEUE_GAP;
const W_JOIN_X = W_REF + ACK_DX;
/** The skip share leaves the unmatched box's left edge and turns down as far out as a worker's ack. */
const W_SKIP_X = W_UN_X - PAD - S2;

const W_LOWER: Lower = {
  pay: W_PAY,
  ref: W_REF,
  joinX: W_JOIN_X,
  above: 'ref',
  skipFrom: [
    [W_UN_X, W_ROUTE_Y],
    [W_SKIP_X, W_ROUTE_Y],
  ],
  joinLabel: ['batch ack'],
  ctrlUnderJoin: true,
  pb: W_PB,
  band: {x: W_BAND_L, y: W_BAND_TOP, w: W_REF + COL_W / 2 + PAD - W_BAND_L, h: PAD + CHIP_H + PAD},
  bandTitle: [W_IO_TITLE],
  wy: W_BAND_TOP + PAD + CHIP_H / 2,
  pl: W_PL,
  srcX: W_SRC_X,
  srcBottom: W_Y + R_NODE,
};
const W_WIDTH = W_JOIN_X + Math.max(R_JOIN + S2 + textW('batch ack'), STROKE / 2 + S2 + textW('unbounded')) + M;

function wide(id: string): React.ReactNode {
  const l = W_LOWER;
  return (
    <>
      <Region x={W_PL} y={W_PT} w={W_PR - W_PL} h={W_PB - W_PT} />
      <Text kind="title" x={W_PL + PAD} y={W_PT + PAD + TITLE_ASCENT} lines="pipeline thread" />

      <Node x={W_SRC_X} y={W_Y} />
      <Text kind="label" x={W_SRC_X} y={W_Y - R_NODE - S1 - DESCENT} lines="source" anchor="middle" />
      <Edge id={id} points={[[W_SRC_X + R_NODE, W_Y], [W_TRUNK[0].x - ARROW_GAP, W_Y]]} />
      <Run id={id} labels={TRUNK} boxes={W_TRUNK} />

      <Branches id={id} l={l} un={W_UN} fanFrom={[W_PAY, W_Y + CHIP_H / 2]} busY={W_BUS} routeY={W_ROUTE_Y} />
      {[W_PAY, W_REF].map((x, i) => (
        <Text key={x} x={x - S2} y={centredBaseline(W_NAME_Y)} lines={`"${NAMES[i]}"`} anchor="end" />
      ))}
      <Chip x={W_UN_X} y={W_ROUTE_Y} label="unmatched" />
      <Text kind="label" x={W_UN_X} y={W_POLICY_BASE} lines={W_POLICY[0]} />
      <Text x={W_UN_X} y={W_POLICY_BASE + LINE} lines={W_POLICY[1]} />
      <Text x={W_SKIP_X + S2} y={centredBaseline(W_PB + QUEUE_GAP / 2, 2)} lines={['skip:', 'ack share']} />

      <LowerPart id={id} l={l} />
    </>
  );
}

// ---- portrait: the trunk runs down over the refunds column; the unmatched branch hangs at the
// right, its skip share dropping straight into the batch ack ----

const N_W = 360;
const N_SRC_X = M + 2 * S2;
const N_SRC_Y = M + LINE_BOX + S1 + STROKE / 2;
const N_PL = N_SRC_X + 2 * S2;
const N_PT = N_SRC_Y + 2 * GAP;
const N_IO_TITLE = ['I/O', 'runtime'];
/** Records cross the runtime's title row clear of its title, whose band is the region's left edge. */
const N_PAY = N_PL + PAD + titleW(N_IO_TITLE[1]) + S2 + STROKE / 2;
const N_REF = N_PAY + PITCH;
const N_POLICY = ['Fail: stops', 'the pipeline', 'Skip: drops', 'and counts'];
const N_UN_W = Math.max(chipW('unmatched'), ...N_POLICY.map(textW));
/** The region runs to the right margin, so every portrait drawing leaves the same margin. */
const N_PR = N_W - M;
const N_UN = N_PR - PAD - N_UN_W / 2;
const N_TRUNK_W = chipW(TRUNK);
const N_TRUNK = columnOf(3, N_REF - N_TRUNK_W / 2, N_PT + PAD + TITLE_BOX + GAP + CHIP_H / 2, N_TRUNK_W);
const N_TRUNK_END = N_TRUNK[2].y + CHIP_H / 2;
const N_BUS = N_TRUNK_END + GAP;
const N_ROUTE_Y = N_BUS + 2 * GAP + CHIP_H / 2;
const N_POLICY_BASE = N_ROUTE_Y + CHIP_H / 2 + S1 + ASCENT;
const N_POLICY_BOTTOM = linesBottom(N_POLICY_BASE, N_POLICY.length);
const N_PB = Math.max(N_POLICY_BOTTOM, N_ROUTE_Y + CHIP_H / 2 + GAP + CHIP_H) + PAD;
const N_BAND_TOP = N_PB + QUEUE_GAP;
/** The workers sit under the runtime's two-line title. */
const N_WY = N_BAND_TOP + PAD + TITLE_BOX + TITLE_LINE + GAP + CHIP_H / 2;

const N_LOWER: Lower = {
  pay: N_PAY,
  ref: N_REF,
  joinX: N_UN,
  above: 'skip',
  skipFrom: [[N_UN, N_POLICY_BOTTOM + S1]],
  joinLabel: ['batch', 'ack'],
  ctrlUnderJoin: false,
  pb: N_PB,
  band: {x: N_PL, y: N_BAND_TOP, w: N_REF + COL_W / 2 + PAD - N_PL, h: N_WY + CHIP_H / 2 + PAD - N_BAND_TOP},
  bandTitle: N_IO_TITLE,
  wy: N_WY,
  pl: N_PL,
  srcX: N_SRC_X,
  srcBottom: N_SRC_Y + R_NODE,
};

function narrow(id: string): React.ReactNode {
  const l = N_LOWER;
  const skip = ['skip:', 'ack', 'share'];
  return (
    <>
      <Region x={N_PL} y={N_PT} w={N_PR - N_PL} h={N_PB - N_PT} />
      <Text kind="title" x={N_PL + PAD} y={N_PT + PAD + TITLE_ASCENT} lines="pipeline thread" />

      <Node x={N_SRC_X} y={N_SRC_Y} />
      <Text kind="label" x={N_SRC_X + R_NODE + S2} y={N_SRC_Y - STROKE / 2 - S1 - DESCENT} lines="source" />
      <Edge
        id={id}
        points={[
          [N_SRC_X + R_NODE, N_SRC_Y],
          [N_REF, N_SRC_Y],
          [N_REF, N_TRUNK[0].y - CHIP_H / 2 - ARROW_GAP],
        ]}
      />
      <Run id={id} labels={TRUNK} boxes={N_TRUNK} vertical />

      <Branches id={id} l={l} un={N_UN} fanFrom={[N_REF, N_TRUNK_END]} busY={N_BUS} routeY={N_ROUTE_Y} />
      {[N_PAY, N_REF].map((x, i) => (
        <Text key={x} x={x + S2} y={centredBaseline(N_BUS + GAP)} lines={`"${NAMES[i]}"`} />
      ))}
      <Chip x={N_UN - chipW('unmatched') / 2} y={N_ROUTE_Y} label="unmatched" />
      <Text kind="label" x={N_UN} y={N_POLICY_BASE} lines={N_POLICY.slice(0, 2)} anchor="middle" />
      <Text x={N_UN} y={N_POLICY_BASE + 2 * LINE} lines={N_POLICY.slice(2)} anchor="middle" />
      <Text x={N_UN + S2} y={centredBaseline(N_WY, skip.length)} lines={skip} />

      <LowerPart id={id} l={l} />
    </>
  );
}

/** The split terminal on the Multi-sink concept page, drawn as fan-out and the matching ack fan-in. */
export default function SplitDiagram(): React.JSX.Element {
  return (
    <OverviewFigure
      name={NAME}
      wide={{viewBox: `0 0 ${W_WIDTH} ${lowerRows(W_LOWER).height}`, draw: wide}}
      narrow={{viewBox: `0 0 ${N_W} ${lowerRows(N_LOWER).height}`, draw: narrow}}
      legend={['data', 'ack', 'mark', 'region', 'chip', 'queue', 'node', 'join']}
      caption={
        <>
          One pipeline thread splits a stream across two destinations. After deserialize and the
          operator chain, one classifier sends each record to one branch, here payments or refunds.
          Each branch routes the record to a shard and encodes it into that sink&apos;s own queues and
          shard workers. A record that reaches no branch follows the unmatched policy: Fail stops the
          pipeline, Skip drops and counts it and releases its share of the ack. Every branch the batch
          reached holds a clone of its ack, and each branch&apos;s shard worker resolves its clones
          from the write outcome, so the acks fan back in to one batch ack. The batch commits through
          Source::commit only after every branch it reached has written.
        </>
      }
    />
  );
}
