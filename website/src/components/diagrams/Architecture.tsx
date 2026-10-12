import React from 'react';

import {
  ARROW_GAP,
  ASCENT,
  CHIP_H,
  DESCENT,
  Edge,
  GAP,
  LANE_SPREAD,
  LINE,
  LINE_BOX,
  Node,
  OverviewFigure,
  PAD,
  RAIL,
  STROKE,
  QUEUE_GAP,
  Queue,
  R_NODE,
  R_STEP,
  Region,
  Replicas,
  Run,
  S1,
  S2,
  Step,
  Text,
  TITLE_ASCENT,
  TITLE_DESCENT,
  TITLE_LINE,
  TITLE_BOX,
  TITLE_CHAR,
  centredBaseline,
  chipW,
  columnOf,
  linesBottom,
  rowOf,
  textW,
  Chip,
} from './parts';

const NAME = 'Pipeline process architecture: records flow out to the destination and acks return to commit at the source.';

const STAGES = ['poll', 'deserialize', 'operators', 'route', 'encode'];

// ---- landscape ----

const M = S1;
const SRC_X = M + textW('source') / 2;
/** The control line into the source runs between the node and the pipeline region. */
const CTRL_X = SRC_X + R_NODE + GAP;
const PL = CTRL_X + GAP;

const WORKER = ['shard', 'worker'];
const WORKER_NOTES = ['one per', 'shard'];
const WORKER_STEPS = ['batch,', 'seal,', 'write,', 'retry,', 'rotate', 'replicas'];
const RESOLVE = ['resolve', 'acks'];
/** The I/O column holds the worker, the admin server and the runtime's note, all one width. */
const IOW = Math.max(chipW([...WORKER, ...WORKER_NOTES, ...WORKER_STEPS, ...RESOLVE]), textW('io_threads'));

// The destination sits above the I/O column; the write climbs to it past the region's top.
const DEST_Y = M + LINE_BOX + S1 + R_NODE;
const IO_TOP = DEST_Y + R_NODE + LINE_BOX + 2 * GAP;
const Y1 = IO_TOP + PAD + CHIP_H / 2;
const PT = Y1 - CHIP_H / 2 - GAP - TITLE_BOX - PAD;

const RUN = rowOf(STAGES, PL + PAD, Y1);
const RUN_END = RUN[RUN.length - 1].x + RUN[RUN.length - 1].w;
const PR = RUN_END + PAD;
const IO_L = PR + QUEUE_GAP;
const IO_C = IO_L + PAD + IOW / 2;
const IO_R = IO_L + PAD + IOW + PAD;

const NOTE_BASE = Y1 + CHIP_H / 2 + GAP + ASCENT;
const PB = linesBottom(NOTE_BASE, 2) + PAD;

// Control lines between the pipeline and controller regions, each labelled to its right.
const PAUSE = ['on a refused try_send, or', 'bytes in flight past the', 'high watermark'];
const DRIVERS = ['assign lanes,', 'FlushNow, drain,', 'shutdown'];
const CT = PB + GAP + LINE_BOX + 3 * LINE + GAP;

const PLANE = 'source control plane';
const PLANE_Y = CT + PAD + CHIP_H / 2;
const CA = PL + PAD + chipW(PLANE) / 2;
const CB = CA + S2 + Math.max(...PAUSE.map(textW)) + GAP;
const PLANE_NOTE_BASE = PLANE_Y + CHIP_H / 2 + S1 + ASCENT;

const COMMIT_BASE = linesBottom(PLANE_NOTE_BASE) + GAP + ASCENT;
const Y2 = linesBottom(COMMIT_BASE) + S1 + RAIL / 2;
/** The controller region stops where the ack line's label needs the space before the I/O column. */
const CR = IO_L - textW('unbounded') - 2 * S2;
const CP_W = chipW('checkpointer');
const CP_X = CR - PAD - CP_W;

const WORKER_BOTTOM = Y2 + CHIP_H / 2;
const ADMIN_H = CHIP_H + LINE;
const ADMIN_Y = WORKER_BOTTOM + GAP + ADMIN_H / 2;
// Each region's title sits at its foot.
const CTRL_BOTTOM = linesBottom(Y2 + CHIP_H / 2 + S1 + ASCENT) + GAP + TITLE_BOX + PAD;
const IO_BOTTOM = ADMIN_Y + ADMIN_H / 2 + GAP + TITLE_ASCENT + TITLE_LINE + LINE + DESCENT + PAD;
const VIEW_W = IO_R + M;
const VIEW_H = Math.max(CTRL_BOTTOM, IO_BOTTOM) + M;

/**
 * The landscape layout. The loop's three thread regions sit where it passes: pipeline threads on
 * the outbound run, the I/O runtime at the far turn, the controller thread on the return. Each
 * hand-off between them is a channel drawn on the boundary it crosses.
 */
function wide(id: string): React.ReactNode {
  const poll = RUN[0];
  const qx = PR + QUEUE_GAP / 2;
  return (
    <>
      <Region x={PL} y={PT} w={PR - PL} h={PB - PT} stacked />
      <Region x={IO_L} y={IO_TOP} w={IO_R - IO_L} h={IO_BOTTOM - IO_TOP} />
      <Region x={PL} y={CT} w={CR - PL} h={CTRL_BOTTOM - CT} />

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
      <Edge id={id} points={[[RUN_END, Y1], [qx - QUEUE_GAP / 2 + S2, Y1]]} head={false} />
      <Queue x={qx} y={Y1} />
      <Edge id={id} points={[[qx + QUEUE_GAP / 2 - S2, Y1], [IO_L + PAD - ARROW_GAP, Y1]]} />

      {/* The far turn: one shard worker per shard writes, and resolves acks from the write's outcome. */}
      <Chip x={IO_L + PAD} y={(Y1 + Y2) / 2} w={IOW} h={Y2 - Y1 + CHIP_H} label={[]} />
      <Text kind="label" x={IO_C} y={centredBaseline(Y1)} lines={WORKER} anchor="middle" />
      <Text x={IO_C} y={centredBaseline(Y1) + 2 * LINE} lines={WORKER_NOTES} anchor="middle" />
      <Text x={IO_C} y={centredBaseline((Y1 + Y2) / 2, WORKER_STEPS.length)} lines={WORKER_STEPS} anchor="middle" />
      <Text kind="label" x={IO_C} y={centredBaseline(Y2) - LINE} lines={RESOLVE} anchor="middle" />
      <Chip x={IO_L + PAD} y={ADMIN_Y} w={IOW} h={ADMIN_H} label={['admin', 'server']} />
      <Text kind="title" x={IO_L + PAD} y={IO_BOTTOM - PAD - DESCENT - LINE - TITLE_LINE} lines={['I/O', 'runtime']} />
      <Text x={IO_L + PAD} y={IO_BOTTOM - PAD - DESCENT} lines="io_threads" />

      <Replicas x={IO_C} y={DEST_Y} />
      <Text kind="label" x={IO_C} y={M + ASCENT} lines="destination" anchor="middle" />
      <Edge id={id} points={[[IO_C, Y1 - CHIP_H / 2], [IO_C, DEST_Y + R_NODE + ARROW_GAP]]} />
      <Text x={IO_C + S2} y={centredBaseline((DEST_Y + R_NODE + IO_TOP) / 2)} lines="write" />

      {/* The return run. */}
      <Edge id={id} kind="ack" points={[[IO_L + PAD, Y2], [CR - PAD + ARROW_GAP, Y2]]} />
      <Text kind="label" x={(CR + IO_L) / 2} y={Y2 - STROKE / 2 - S1 - DESCENT} lines="acks" anchor="middle" />
      <Text x={(CR + IO_L) / 2} y={Y2 + STROKE / 2 + S1 + ASCENT} lines="unbounded" anchor="middle" />
      <Chip x={CP_X} y={Y2} label="checkpointer" />
      <Text x={CP_X + CP_W / 2} y={Y2 + CHIP_H / 2 + S1 + ASCENT} lines="tokio-free" anchor="middle" />
      <Edge id={id} kind="mark" points={[[CP_X, Y2], [SRC_X, Y2], [SRC_X, Y1 + R_NODE + ARROW_GAP]]} />
      <Text kind="label" x={PL + PAD} y={COMMIT_BASE} lines="Source::commit" />
      <Text x={PL + PAD} y={Y2 + RAIL / 2 + S1 + ASCENT} lines="watermarks, on the tick" />
      <Text kind="title" x={PL + PAD} y={CTRL_BOTTOM - PAD - TITLE_DESCENT} lines="controller thread" />

      {/* Control: the controller drives the source and the drivers; drivers ask it to pause. */}
      <Chip x={PL + PAD} y={PLANE_Y} label={PLANE} />
      <Text x={PL + PAD} y={PLANE_NOTE_BASE} lines="poll_events, pause, resume" />
      <Edge
        id={id}
        kind="control"
        points={[
          [PL + PAD, PLANE_Y],
          [CTRL_X, PLANE_Y],
          [CTRL_X, Y1 + R_NODE + GAP],
          [SRC_X + (R_NODE + ARROW_GAP) / Math.SQRT2, Y1 + (R_NODE + ARROW_GAP) / Math.SQRT2],
        ]}
      />
      <Edge id={id} kind="control" points={[[CA, PB], [CA, PLANE_Y - CHIP_H / 2 - ARROW_GAP]]} />
      <Text kind="label" x={CA + S2} y={PB + GAP + ASCENT} lines="pause or resume request" />
      <Text x={CA + S2} y={PB + GAP + ASCENT + LINE} lines={PAUSE} />
      <Edge id={id} kind="control" points={[[CB, CT], [CB, PB + ARROW_GAP]]} />
      <Text kind="label" x={CB + S2} y={PB + GAP + ASCENT} lines="to drivers" />
      <Text x={CB + S2} y={PB + GAP + ASCENT + LINE} lines={DRIVERS} />
    </>
  );
}

// ---- portrait ----

const N_W = 360;
const N_SRC_X = M + 2 * S2;
/** The rail along the top carries step disc 5, which must clear the viewBox edge. */
const RAIL_Y = M + R_STEP;
const N_SRC_Y = RAIL_Y + RAIL / 2 + 2 * GAP + R_NODE;
const N_RT = linesBottom(N_SRC_Y + R_NODE + S1 + ASCENT) + GAP;

const N_PL = N_SRC_X + 2 * S2;
const N_PIPE_TEXT = ['std threads,', 'pinning opt-in'];
const N_PW = Math.max(chipW('deserialize'), ...N_PIPE_TEXT.map(textW), 'threads × N'.length * TITLE_CHAR);
const N_PR = N_PL + PAD + N_PW + PAD;
const N_NX = N_PL + PAD + N_PW / 2;
const N_COL_Y = N_RT + PAD + TITLE_ASCENT + TITLE_LINE + 2 * LINE + DESCENT + GAP + CHIP_H / 2;
const N_COL = columnOf(STAGES.length, N_NX - chipW('deserialize') / 2, N_COL_Y, chipW('deserialize'));
const N_COL_END = N_COL[N_COL.length - 1].y + CHIP_H / 2;
const N_RB = N_COL_END + PAD;

const N_PLANE = ['source', 'control plane'];
const N_CW = Math.max(chipW(N_PLANE), chipW('checkpointer'), 'controller'.length * TITLE_CHAR);
const N_CR = N_W - M;
const N_CL = N_CR - PAD - N_CW - PAD;
const N_CX = N_CL + PAD + N_CW / 2;
const N_PLANE_H = CHIP_H + LINE;
const N_PLANE_Y = N_RT + PAD + TITLE_ASCENT + TITLE_LINE + TITLE_DESCENT + GAP + N_PLANE_H / 2;
const N_CP_Y = N_RB - PAD - CHIP_H / 2;
/** The rail and the control line leave the control plane past the region title's right end. */
const N_RAIL_X = N_CR - PAD - S2;
const N_CTRL_X = N_RAIL_X - GAP;

const N_IO_TOP = N_RB + QUEUE_GAP;
const N_WORKER_TOP = N_IO_TOP + PAD;
const N_WORKER_H = CHIP_H + 2 * LINE;
const N_IO_L = N_PL;
const N_IO_R = N_CR;
const N_ADMIN_Y = N_WORKER_TOP + N_WORKER_H + GAP + CHIP_H / 2;
const N_IO_B = N_ADMIN_Y + CHIP_H / 2 + PAD;
const N_ADMIN_X = N_IO_R - PAD - chipW('admin server');
/** The write leaves the worker between the runtime's title and the admin server. */
const N_WRITE_X = (N_IO_L + PAD + 'I/O runtime'.length * TITLE_CHAR + N_ADMIN_X) / 2;
const N_DEST_Y = N_IO_B + 2 * GAP + LINE_BOX + R_NODE;
const N_H = N_DEST_Y + R_NODE + S1 + LINE_BOX + M;

/**
 * The portrait layout: the same loop turned a quarter, records down the left column, acks up the
 * right. Line labels move to the numbered key under the figure.
 */
function narrow(id: string): React.ReactNode {
  const gapMid = (N_PR + N_CL) / 2;
  const pauseY = N_PLANE_Y - N_PLANE_H / 2 + CHIP_H / 2;
  const driversY = N_PLANE_Y + N_PLANE_H / 2 + GAP + CHIP_H / 2;
  const workerLeft = N_IO_L + PAD;
  const workerW = N_IO_R - PAD - workerLeft;
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
      <Text kind="title" x={N_IO_L + PAD} y={centredBaseline(N_ADMIN_Y)} lines="I/O runtime" />

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
      <Edge id={id} points={[[N_NX, qy + QUEUE_GAP / 2 - S2], [N_NX, N_WORKER_TOP - ARROW_GAP]]} />

      <Chip x={workerLeft} y={N_WORKER_TOP + N_WORKER_H / 2} w={workerW} h={N_WORKER_H} label={[]} />
      <Text kind="label" x={workerLeft + GAP} y={centredBaseline(N_WORKER_TOP + CHIP_H / 2)} lines="shard worker" />
      <Text kind="label" x={workerLeft + workerW - GAP} y={centredBaseline(N_WORKER_TOP + CHIP_H / 2)} lines="resolve acks" anchor="end" />
      <Text
        x={workerLeft + GAP}
        y={centredBaseline(N_WORKER_TOP + CHIP_H / 2) + LINE}
        lines={['one per shard; batch, seal,', 'write, retry, rotate replicas']}
      />
      <Chip x={N_ADMIN_X} y={N_ADMIN_Y} label="admin server" />

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
      <Edge id={id} kind="control" points={[[N_PR, pauseY], [N_CL + PAD - ARROW_GAP, pauseY]]} />
      <Edge id={id} kind="control" points={[[N_CL, driversY], [N_PR + ARROW_GAP, driversY]]} />

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
      legend={['data', 'ack', 'mark', 'control', 'region', 'chip', 'queue', 'node']}
      steps={[
        'A driver asks the controller to pause its lanes when a try_send is refused or bytes in flight pass the high watermark, and to resume them once they drain.',
        'The controller sends drivers lane assignments, FlushNow on the commit tick, drain barriers and shutdown.',
        'Acks travel over an unbounded channel, so they never wait behind data.',
        'The checkpointer turns acks into per-partition watermarks.',
        'On the commit tick the controller commits the watermarks through Source::commit.',
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
