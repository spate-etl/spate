import React from 'react';

import {
  ARROW_GAP,
  ASCENT,
  CHIP_H,
  CHIP_X,
  Chip,
  DESCENT,
  Edge,
  GAP,
  LANE_SPREAD,
  LINE,
  LINE_BOX,
  Node,
  OverviewFigure,
  PAD,
  QUEUE_GAP,
  QUEUE_W,
  Queue,
  R_NODE,
  R_STEP,
  RAIL,
  Region,
  Replicas,
  Run,
  S1,
  S2,
  STACK,
  STROKE,
  Step,
  Text,
  TITLE_ASCENT,
  TITLE_BOX,
  TITLE_DESCENT,
  TITLE_LINE,
  centredBaseline,
  chipW,
  columnOf,
  linesBottom,
  rowOf,
  textW,
  titleW,
} from './parts';

const NAME = 'Pipeline process architecture: records flow out to the destination and acks return to commit at the source.';

const STAGES = ['poll', 'deserialize', 'operators', 'route', 'encode'];

// ---- landscape ----

const M = S1;
const SRC_X = M + textW('source') / 2;
/** The control line into the source runs between the node and the pipeline region. */
const CTRL_X = SRC_X + R_NODE + GAP;
const PL = CTRL_X + GAP;

// The pipeline region runs along the top; its stack of copies reaches `STACK` above it.
const PT = M + STACK;
const Y1 = PT + PAD + TITLE_BOX + GAP + CHIP_H / 2;
const RUN = rowOf(STAGES, PL + PAD, Y1);
const RUN_END = RUN[RUN.length - 1].x + RUN[RUN.length - 1].w;
const PR = RUN_END + PAD;
const NOTE_BASE = Y1 + CHIP_H / 2 + GAP + ASCENT;
const PB = linesBottom(NOTE_BASE, 2) + PAD;

// Control lines cross the band between the pipeline and the controller, labelled outward: the
// drivers line to its left, the pause line to its right. Both cross the controller's top edge
// clear of its title.
const CTRL_TITLE = 'controller thread';
const DRIVERS_X = PL + PAD + titleW(CTRL_TITLE) + S2;
const PAUSE_X = DRIVERS_X + GAP;
const PAUSE = ['on a refused try_send, or', 'bytes in flight reaching', 'the high watermark'];
const DRIVERS = ['assign lanes,', 'FlushNow, drain,', 'shutdown'];
const CT = PB + GAP + LINE_BOX + PAUSE.length * LINE + GAP;

// The controller holds the source control plane over the checkpointer, both at its right. The
// commit rail leaves the checkpointer to the left, and its labels sit under the checkpointer.
const COMMIT = 'Source::commit';
const COMMIT_NOTE = 'watermarks';
const CP = ['checkpointer', 'tokio-free'];
const CP_W = chipW(CP);
const CP_H = CHIP_H + LINE;
const PLANE = 'source control plane';
const PLANE_NOTE = 'poll_events, pause, resume';
const CR = PL + PAD + Math.max(textW(PLANE_NOTE), textW(COMMIT_NOTE), chipW(PLANE), CP_W, titleW(CTRL_TITLE)) + PAD;
const CP_X = CR - PAD - CP_W;
const PLANE_X = CR - PAD - chipW(PLANE);
const PLANE_Y = CT + PAD + TITLE_BOX + GAP + CHIP_H / 2;
const PLANE_NOTE_BASE = PLANE_Y + CHIP_H / 2 + S1 + ASCENT;

// The I/O runtime sits beside the controller, the acks line between them. Records enter its
// worker from above, between the region's title and its thread-count note, and leave at the same
// x for the destination below, right of the admin server.
const IO_L = CR + textW('unbounded') + 2 * S2;
const IO_TITLE = 'I/O runtime';
const IO_NOTE = 'io_threads';
const WORKER_NOTES = ['one per shard;', 'batch, seal, write,', 'retry, rotate replicas'];
const ADMIN = 'admin server';
const REC_X = IO_L + PAD + Math.max(titleW(IO_TITLE), chipW(ADMIN)) + GAP;
const IOW = Math.max(chipW(WORKER_NOTES), REC_X + GAP + textW(IO_NOTE) - (IO_L + PAD));
const IO_R = IO_L + PAD + IOW + PAD;
const WORKER_TOP = CT + PAD + TITLE_BOX + GAP;

/** The acks row: the worker's last line and the checkpointer's centre, whichever sits lower. */
const Y2 = Math.max(
  WORKER_TOP + CHIP_H + (WORKER_NOTES.length + 1) * LINE - CHIP_H / 2,
  linesBottom(PLANE_NOTE_BASE) + GAP + CP_H / 2,
);
/** The commit labels clear the checkpointer's foot by `S1`. */
const COMMIT_BASE = Y2 + CP_H / 2 + S1 + ASCENT;
const CTRL_BOTTOM = linesBottom(COMMIT_BASE, 2) + PAD;

const WORKER_BOTTOM = Y2 + CHIP_H / 2;
const ADMIN_Y = WORKER_BOTTOM + GAP + CHIP_H / 2;
const IO_BOTTOM = ADMIN_Y + CHIP_H / 2 + PAD;
const DEST_Y = IO_BOTTOM + 2 * GAP + LINE_BOX + R_NODE;
const DEST_BASE = DEST_Y + R_NODE + S1 + ASCENT;
const VIEW_W = Math.max(IO_R, PR + STACK) + M;
const VIEW_H = Math.max(CTRL_BOTTOM, linesBottom(DEST_BASE)) + M;

/**
 * The landscape layout. The loop's three thread regions sit where it passes: pipeline threads on
 * the outbound run along the top, the I/O runtime at the far turn, the controller thread on the
 * return. Each hand-off between them is a channel drawn across the gap it crosses.
 */
function wide(id: string): React.ReactNode {
  const poll = RUN[0];
  const qy = (PB + CT) / 2;
  const workerLeft = IO_L + PAD;
  return (
    <>
      <Region x={PL} y={PT} w={PR - PL} h={PB - PT} stacked />
      <Region x={PL} y={CT} w={CR - PL} h={CTRL_BOTTOM - CT} />
      <Region x={IO_L} y={CT} w={IO_R - IO_L} h={IO_BOTTOM - CT} />

      <Text kind="title" x={PL + PAD} y={PT + PAD + TITLE_ASCENT} lines="pipeline threads × N" />
      <Text x={PL + PAD} y={NOTE_BASE} lines={['std threads,', 'pinning opt-in']} />
      <Text x={RUN_END} y={NOTE_BASE} lines={['try_send to a', 'per-shard queue']} anchor="end" />

      {/* The outbound run. */}
      <Node x={SRC_X} y={Y1} />
      <Text kind="label" x={SRC_X} y={Y1 - R_NODE - S1 - DESCENT} lines="source" anchor="middle" />
      {[-LANE_SPREAD, 0, LANE_SPREAD].map((dy) => (
        <Edge key={dy} id={id} points={[[SRC_X + R_NODE, Y1], [poll.x - ARROW_GAP, Y1 + dy]]} head={false} />
      ))}
      <Run id={id} labels={STAGES} boxes={RUN} />
      <Edge id={id} points={[[RUN_END, Y1], [REC_X, Y1], [REC_X, qy - QUEUE_W / 2]]} head={false} />
      <Queue x={REC_X} y={qy} vertical />
      <Edge id={id} points={[[REC_X, qy + QUEUE_W / 2], [REC_X, WORKER_TOP - ARROW_GAP]]} />

      {/* The far turn: one shard worker per shard writes, and resolves acks from the write's outcome. */}
      <Text kind="title" x={IO_L + PAD} y={CT + PAD + TITLE_ASCENT} lines={IO_TITLE} />
      <Text x={IO_R - PAD} y={CT + PAD + TITLE_ASCENT} lines={IO_NOTE} anchor="end" />
      <Chip x={workerLeft} y={(WORKER_TOP + WORKER_BOTTOM) / 2} w={IOW} h={WORKER_BOTTOM - WORKER_TOP} label={[]} />
      <Text kind="label" x={workerLeft + CHIP_X} y={centredBaseline(WORKER_TOP + CHIP_H / 2)} lines="shard worker" />
      <Text x={workerLeft + CHIP_X} y={centredBaseline(WORKER_TOP + CHIP_H / 2) + LINE} lines={WORKER_NOTES} />
      <Text kind="label" x={workerLeft + CHIP_X} y={centredBaseline(Y2)} lines="resolve acks" />
      <Chip x={workerLeft} y={ADMIN_Y} label={ADMIN} />

      <Edge id={id} points={[[REC_X, WORKER_BOTTOM], [REC_X, DEST_Y - R_NODE - ARROW_GAP]]} />
      <Text x={REC_X + S2} y={centredBaseline((IO_BOTTOM + DEST_Y - R_NODE) / 2)} lines="write" />
      <Replicas x={REC_X} y={DEST_Y} />
      <Text kind="label" x={REC_X} y={DEST_BASE} lines="destination" anchor="middle" />

      {/* The return run. */}
      <Edge id={id} kind="ack" points={[[workerLeft, Y2], [CR - PAD + ARROW_GAP, Y2]]} />
      <Text kind="label" x={(CR + IO_L) / 2} y={Y2 - STROKE / 2 - S1 - DESCENT} lines="acks" anchor="middle" />
      <Text x={(CR + IO_L) / 2} y={Y2 + STROKE / 2 + S1 + ASCENT} lines="unbounded" anchor="middle" />
      <Chip x={CP_X} y={Y2} w={CP_W} h={CP_H} label={[]} />
      <Text kind="label" x={CP_X + CP_W / 2} y={centredBaseline(Y2, 2)} lines={CP[0]} anchor="middle" />
      <Text x={CP_X + CP_W / 2} y={centredBaseline(Y2, 2) + LINE} lines={CP[1]} anchor="middle" />
      <Edge id={id} kind="mark" points={[[CP_X, Y2], [SRC_X, Y2], [SRC_X, Y1 + R_NODE + ARROW_GAP]]} />
      <Text kind="label" x={PL + PAD} y={COMMIT_BASE} lines={COMMIT} />
      <Text x={PL + PAD} y={COMMIT_BASE + LINE} lines={COMMIT_NOTE} />
      <Text kind="title" x={PL + PAD} y={CT + PAD + TITLE_ASCENT} lines={CTRL_TITLE} />

      {/* Control: the controller drives the source and the drivers; drivers ask it to pause. */}
      <Chip x={PLANE_X} y={PLANE_Y} label={PLANE} />
      <Text x={CR - PAD} y={PLANE_NOTE_BASE} lines={PLANE_NOTE} anchor="end" />
      <Edge
        id={id}
        kind="control"
        points={[
          [PLANE_X, PLANE_Y],
          [CTRL_X, PLANE_Y],
          [CTRL_X, Y1 + R_NODE + GAP],
          [SRC_X + (R_NODE + ARROW_GAP) / Math.SQRT2, Y1 + (R_NODE + ARROW_GAP) / Math.SQRT2],
        ]}
      />
      <Edge id={id} kind="control" points={[[PAUSE_X, PB], [PAUSE_X, PLANE_Y - CHIP_H / 2 - ARROW_GAP]]} />
      <Text kind="label" x={PAUSE_X + S2} y={PB + GAP + ASCENT} lines="pause or resume request" />
      <Text x={PAUSE_X + S2} y={PB + GAP + ASCENT + LINE} lines={PAUSE} />
      <Edge id={id} kind="control" points={[[DRIVERS_X, CT], [DRIVERS_X, PB + ARROW_GAP]]} />
      <Text kind="label" x={DRIVERS_X - S2} y={PB + GAP + ASCENT} lines="to drivers" anchor="end" />
      <Text x={DRIVERS_X - S2} y={PB + GAP + ASCENT + LINE} lines={DRIVERS} anchor="end" />
    </>
  );
}

// ---- portrait ----

const N_W = 360;
const N_SRC_X = M + 2 * S2;
/** The rail along the top carries step disc 5, which must clear the viewBox edge. */
const RAIL_Y = M + R_STEP;
const N_SRC_Y = RAIL_Y + RAIL / 2 + 2 * GAP + R_NODE;
const N_RT = linesBottom(N_SRC_Y + R_NODE + S1 + ASCENT) + GAP + STACK;

const N_PL = N_SRC_X + 2 * S2;
const N_PIPE_TEXT = ['std threads,', 'pinning opt-in'];
const N_PW = Math.max(chipW('deserialize'), ...N_PIPE_TEXT.map(textW), titleW('threads × N'));
const N_PR = N_PL + PAD + N_PW + PAD;
const N_NX = N_PL + PAD + N_PW / 2;
const N_COL_Y = N_RT + PAD + TITLE_ASCENT + TITLE_LINE + 2 * LINE + DESCENT + GAP + CHIP_H / 2;
const N_COL = columnOf(STAGES.length, N_NX - chipW('deserialize') / 2, N_COL_Y, chipW('deserialize'));
const N_COL_END = N_COL[N_COL.length - 1].y + CHIP_H / 2;
const N_RB = N_COL_END + PAD;

const N_PLANE = ['source', 'control plane'];
const N_CW = Math.max(chipW(N_PLANE), chipW('checkpointer'), titleW('controller'));
const N_CR = N_W - M;
const N_CL = N_CR - PAD - N_CW - PAD;
const N_CX = N_CL + PAD + N_CW / 2;
const N_PLANE_H = CHIP_H + LINE;
const N_PLANE_Y = N_RT + PAD + TITLE_ASCENT + TITLE_LINE + TITLE_DESCENT + GAP + N_PLANE_H / 2;
const N_CP_Y = N_RB - PAD - CHIP_H / 2;
/** The rail and the control line leave the control plane past the region title's right end. */
const N_RAIL_X = N_CR - PAD - S2;
const N_CTRL_X = N_RAIL_X - GAP;
/** The pipeline's control edge is the outer copy of its stack. */
const N_STACK_R = N_PR + STACK;

// The I/O runtime starts right of the records column, so records enter its worker from the side
// and the region's title keeps its top-left corner.
const N_IO_TOP = N_RB + QUEUE_GAP;
const N_IO_L = N_NX + GAP;
const N_IO_R = N_CR;
const N_WORKER_X = N_IO_L + PAD;
const N_WORKER_W = N_IO_R - PAD - N_WORKER_X;
const N_WORKER_TOP = N_IO_TOP + PAD + TITLE_BOX + GAP;
const N_WORKER_ROW = N_WORKER_TOP + CHIP_H / 2;
const N_WORKER_NOTES = ['one per shard;', 'batch, seal, write, retry,', 'rotate replicas'];
const N_WORKER_H = CHIP_H + N_WORKER_NOTES.length * LINE;
const N_ADMIN_Y = N_WORKER_TOP + N_WORKER_H + GAP + CHIP_H / 2;
const N_IO_B = N_ADMIN_Y + CHIP_H / 2 + PAD;
/** The write leaves the worker beside the admin server. */
const N_WRITE_X = N_WORKER_X + chipW('admin server') + GAP;
const N_DEST_Y = N_IO_B + 2 * GAP + LINE_BOX + R_NODE;
const N_H = N_DEST_Y + R_NODE + S1 + LINE_BOX + M;

/**
 * The portrait layout: the same loop turned a quarter, records down the left column, acks up the
 * right. Line labels move to the numbered key under the figure.
 */
function narrow(id: string): React.ReactNode {
  const gapMid = (N_STACK_R + N_CL) / 2;
  const pauseY = N_PLANE_Y - N_PLANE_H / 2 + CHIP_H / 2;
  const driversY = N_PLANE_Y + N_PLANE_H / 2 + GAP + CHIP_H / 2;
  const qy = N_RB + QUEUE_GAP / 2;
  const ackX = N_CX;
  return (
    <>
      <Region x={N_PL} y={N_RT} w={N_PR - N_PL} h={N_RB - N_RT} stacked />
      <Region x={N_CL} y={N_RT} w={N_CR - N_CL} h={N_RB - N_RT} />
      <Region x={N_IO_L} y={N_IO_TOP} w={N_IO_R - N_IO_L} h={N_IO_B - N_IO_TOP} />

      <Text kind="title" x={N_PL + PAD} y={N_RT + PAD + TITLE_ASCENT} lines={['pipeline', 'threads × N']} />
      <Text x={N_PL + PAD} y={N_RT + PAD + TITLE_ASCENT + TITLE_LINE + LINE} lines={N_PIPE_TEXT} />
      <Text kind="title" x={N_CL + PAD} y={N_RT + PAD + TITLE_ASCENT} lines={['controller', 'thread']} />
      <Text kind="title" x={N_IO_L + PAD} y={N_IO_TOP + PAD + TITLE_ASCENT} lines="I/O runtime" />

      <Node x={N_SRC_X} y={N_SRC_Y} />
      <Text kind="label" x={N_SRC_X + S2} y={N_SRC_Y + R_NODE + S1 + ASCENT} lines="source" />
      <Edge
        id={id}
        points={[
          [N_SRC_X, N_SRC_Y + R_NODE],
          [N_SRC_X, N_COL[0].y],
          [N_COL[0].x - ARROW_GAP, N_COL[0].y],
        ]}
      />
      <Run id={id} labels={STAGES} boxes={N_COL} vertical />
      <Edge id={id} points={[[N_NX, N_COL_END], [N_NX, qy - QUEUE_GAP / 2 + S2]]} head={false} />
      <Queue x={N_NX} y={qy} vertical />
      <Edge
        id={id}
        points={[
          [N_NX, qy + QUEUE_GAP / 2 - S2],
          [N_NX, N_WORKER_ROW],
          [N_WORKER_X - ARROW_GAP, N_WORKER_ROW],
        ]}
      />

      <Chip x={N_WORKER_X} y={N_WORKER_TOP + N_WORKER_H / 2} w={N_WORKER_W} h={N_WORKER_H} label={[]} />
      <Text kind="label" x={N_WORKER_X + CHIP_X} y={centredBaseline(N_WORKER_ROW)} lines="shard worker" />
      <Text kind="label" x={N_WORKER_X + N_WORKER_W - CHIP_X} y={centredBaseline(N_WORKER_ROW)} lines="resolve acks" anchor="end" />
      <Text x={N_WORKER_X + CHIP_X} y={centredBaseline(N_WORKER_ROW) + LINE} lines={N_WORKER_NOTES} />
      <Chip x={N_WORKER_X} y={N_ADMIN_Y} label="admin server" />

      <Edge id={id} points={[[N_WRITE_X, N_WORKER_TOP + N_WORKER_H], [N_WRITE_X, N_DEST_Y - R_NODE - ARROW_GAP]]} />
      <Text x={N_WRITE_X + S2} y={centredBaseline((N_IO_B + N_DEST_Y - R_NODE) / 2)} lines="write" />
      <Replicas x={N_WRITE_X} y={N_DEST_Y} />
      <Text kind="label" x={N_WRITE_X} y={N_DEST_Y + R_NODE + S1 + ASCENT} lines="destination" anchor="middle" />

      <Edge id={id} kind="ack" points={[[ackX, N_WORKER_TOP], [ackX, N_CP_Y + CHIP_H / 2 + ARROW_GAP]]} />
      <Chip x={N_CX - chipW('checkpointer') / 2} y={N_CP_Y} label="checkpointer" />
      <Edge id={id} kind="mark" points={[[N_CX, N_CP_Y - CHIP_H / 2], [N_CX, N_PLANE_Y + N_PLANE_H / 2 + ARROW_GAP]]} />
      <Chip x={N_CL + PAD} y={N_PLANE_Y} w={N_CW} h={N_PLANE_H} label={N_PLANE} />
      <Edge
        id={id}
        kind="mark"
        points={[
          [N_RAIL_X, N_PLANE_Y - N_PLANE_H / 2],
          [N_RAIL_X, RAIL_Y],
          [N_SRC_X, RAIL_Y],
          [N_SRC_X, N_SRC_Y - R_NODE - ARROW_GAP],
        ]}
      />
      <Edge
        id={id}
        kind="control"
        points={[
          [N_CTRL_X, N_PLANE_Y - N_PLANE_H / 2],
          [N_CTRL_X, N_SRC_Y],
          [N_SRC_X + R_NODE + ARROW_GAP, N_SRC_Y],
        ]}
      />
      <Edge id={id} kind="control" points={[[N_STACK_R, pauseY], [N_CL + PAD - ARROW_GAP, pauseY]]} />
      <Edge id={id} kind="control" points={[[N_CL, driversY], [N_STACK_R + ARROW_GAP, driversY]]} />

      <Step x={gapMid} y={pauseY} n={1} />
      <Step x={gapMid} y={driversY} n={2} />
      <Step x={ackX} y={(N_RB + N_IO_TOP) / 2} n={3} />
      <Step x={N_CX} y={(N_CP_Y - CHIP_H / 2 + N_PLANE_Y + N_PLANE_H / 2) / 2} n={4} />
      <Step x={(N_SRC_X + N_CL) / 2} y={RAIL_Y} n={5} />
      <Step x={(N_SRC_X + N_CL) / 2} y={N_SRC_Y} n={6} />
    </>
  );
}

/** The process layout on the Architecture concept page, drawn as the at-least-once loop. */
export default function ArchitectureDiagram(): React.JSX.Element {
  return (
    <OverviewFigure
      name={NAME}
      wide={{viewBox: `0 0 ${VIEW_W} ${VIEW_H}`, draw: wide}}
      narrow={{viewBox: `0 0 ${N_W} ${N_H}`, draw: narrow}}
      legend={['data', 'ack', 'mark', 'control', 'region', 'chip', 'queue', 'source', 'replicas']}
      steps={[
        'A driver asks the controller to pause its lanes when a try_send is refused or bytes in flight reach the high watermark, and to resume them once they drain.',
        'The controller sends drivers lane assignments, FlushNow on the commit tick, drain barriers and shutdown.',
        'Acks travel over an unbounded channel, so they never wait behind data.',
        'The checkpointer turns acks into per-partition watermarks.',
        'On the commit tick, and at a rebalance and at shutdown, the controller commits the watermarks through Source::commit.',
        'The controller keeps calling poll_events, and applies pause and resume to the source.',
      ]}
      caption={
        <>
          One process, drawn as the loop a record&apos;s delivery closes. Pipeline threads poll the
          source&apos;s lanes, deserialize, run the operator chain, route each record to a shard and
          encode it, then try_send the chunk to that shard&apos;s bounded queue. On the I/O runtime one
          shard worker per shard batches, writes to the destination and resolves acks from the
          outcome. The acks return over an unbounded channel to the checkpointer on the controller
          thread, which commits the watermarks to the source through Source::commit. A refused
          try_send or the in-flight byte budget sends a pause request to the controller, which pauses
          the source and keeps polling its events.
        </>
      }
    />
  );
}
