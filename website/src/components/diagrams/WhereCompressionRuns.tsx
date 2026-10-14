/**
 * Where the ClickHouse sink compresses an insert, before and after the change that moved it: two
 * drawings on one clock, side by side where the column allows and stacked where it does not.
 * `compressionPlayback.ts` holds the timeline; this file draws it.
 */
import React, {useEffect, useId, useReducer} from 'react';

import {useOnScreen} from '../motion/useOnScreen';
import {
  ARROW_GAP,
  CHIP_H,
  CHIP_X,
  DESCENT,
  Edge,
  GAP,
  Markers,
  PAD,
  Region,
  S1,
  S2,
  TITLE_ASCENT,
  TITLE_BOX,
  TITLE_LINE,
  Text,
  atMostOneToOne,
  centredBaseline,
  styles as frame,
  textW,
  titleW,
  type Point,
} from './parts';
import {CYCLE, INITIAL, SLOTS, THREADS, WORKERS, frameAt, playback, running, type Mode, type Place} from './compressionPlayback';
import styles from './WhereCompressionRuns.module.css';

const M = S1;
/** A thread box: wide enough for "stalled" inside it. */
const THREAD_W = Math.ceil(textW('stalled') + 2 * CHIP_X);
const LANE_PITCH = THREAD_W + GAP;
const W = 2 * M + 2 * PAD + THREADS * THREAD_W + (THREADS - 1) * GAP;
const laneX = (lane: number) => M + PAD + THREAD_W / 2 + lane * LANE_PITCH;

/** A chunk's height, and its width uncompressed and compressed. */
const CHUNK_H = 8;
const CHUNK_W = 18;
const PACKED_W = 8;
const CELL_W = 24;
const CELL_H = 12;
const CELL_PITCH = CELL_H + 2;
/** The progress bar along the foot of a box. */
const BAR_H = 3;
const BAR_INSET = 3;

const A_TOP = M + TITLE_BOX + GAP;
const A_TITLE = A_TOP + PAD + TITLE_ASCENT;
const A_NOTE = A_TITLE + TITLE_LINE;
const THREAD_Y = A_NOTE + DESCENT + GAP + CHIP_H / 2;
const A_BOTTOM = THREAD_Y + CHIP_H / 2 + PAD;
const HELD_Y = A_BOTTOM + S2 + CHUNK_H / 2;
const Q_TOP = HELD_Y + CHUNK_H / 2 + S2;
const slotY = (k: number) => Q_TOP + (SLOTS - 1 - k) * CELL_PITCH + CELL_H / 2;
const Q_BOTTOM = Q_TOP + SLOTS * CELL_PITCH - (CELL_PITCH - CELL_H);
const BUS_Y = Q_BOTTOM + GAP;
const B_TOP = BUS_Y + GAP;
const B_TITLE = B_TOP + PAD + TITLE_ASCENT;
const B_NOTE = B_TITLE + TITLE_LINE;
const B_BOTTOM = B_NOTE + DESCENT + PAD;
const WORKER_Y = (B_TOP + B_BOTTOM) / 2;
/** The I/O workers sit right of the runtime's title and note, so the arrows into them miss the text. */
const WORKER_LEFT = M + PAD + Math.max(textW('compress + send'), titleW('2 I/O workers')) + GAP;
const WORKER_W = (W - M - PAD - WORKER_LEFT - (WORKERS - 1) * GAP) / WORKERS;
const workerX = (w: number) => WORKER_LEFT + WORKER_W / 2 + w * (WORKER_W + GAP);
const SINK_Y = B_BOTTOM + GAP + CHIP_H / 2;
const H = Math.ceil(SINK_Y + CHIP_H / 2 + M);
const VIEW = `0 0 ${W} ${H}`;

const xy = (p: Place): Point => {
  switch (p.kind) {
    case 'thread':
      return [laneX(p.lane), HELD_Y];
    case 'slot':
      return [laneX(p.lane), slotY(p.k)];
    case 'worker':
      return [workerX(p.w), WORKER_Y - 1];
    case 'sink':
      return [workerX(p.w), SINK_Y];
  }
};

const TEXT: Record<Mode, {title: string; thread: string; worker: string; name: string}> = {
  before: {
    title: 'Before · 0.3.0',
    thread: 'encode',
    worker: 'compress + send',
    name: 'Before, on 0.3.0: pipeline threads hand uncompressed chunks to shard queues, two I/O workers compress and send each chunk, and a thread whose queue is full waits.',
  },
  after: {
    title: 'After · 799d3640',
    thread: 'encode + compress',
    worker: 'send',
    name: 'After, on 799d3640: each pipeline thread compresses the chunks it seals, the queues carry them compressed, and the two I/O workers only send them.',
  },
};

/** A bar along the foot of a box `w` wide centred on `x`, `f` of it filled. */
function Progress({x, y, w, f}: {x: number; y: number; w: number; f: number}): React.JSX.Element {
  const full = w - 2 * BAR_INSET;
  return <rect className={styles.progress} x={x - w / 2 + BAR_INSET} y={y + CHIP_H / 2 - BAR_INSET - BAR_H} width={Math.max(0, f * full)} height={BAR_H} />;
}

function Drawing({mode, time, id}: {mode: Mode; time: number; id: string}): React.ReactNode {
  const text = TEXT[mode];
  const f = frameAt(mode, time, xy);
  const runtimeRight = W - M;
  return (
    <>
      <Text kind="title" x={M} y={M + TITLE_ASCENT} lines={text.title} />

      <Region x={M} y={A_TOP} w={W - 2 * M} h={A_BOTTOM - A_TOP} />
      <Text kind="title" x={M + PAD} y={A_TITLE} lines="32 pipeline threads" />
      <Text x={M + PAD} y={A_NOTE} lines={text.thread} />
      {f.threads.map((th, lane) => {
        const x = laneX(lane);
        const stalled = th.work === 'stalled';
        return (
          <g key={lane} data-thread={th.work}>
            <rect
              className={stalled ? styles.stalled : frame.chip}
              x={x - THREAD_W / 2}
              y={THREAD_Y - CHIP_H / 2}
              width={THREAD_W}
              height={CHIP_H}
              rx="4"
            />
            {stalled ? (
              <Text x={x} y={centredBaseline(THREAD_Y)} lines="stalled" anchor="middle" />
            ) : (
              <Progress x={x} y={THREAD_Y} w={THREAD_W} f={th.progress} />
            )}
            <Edge id={id} points={[[x, THREAD_Y + CHIP_H / 2], [x, Q_TOP - ARROW_GAP]]} />
          </g>
        );
      })}

      {Array.from({length: THREADS}, (_, lane) =>
        Array.from({length: SLOTS}, (_, k) => (
          <rect
            key={`${lane}-${k}`}
            className={frame.cell}
            x={laneX(lane) - CELL_W / 2}
            y={slotY(k) - CELL_H / 2}
            width={CELL_W}
            height={CELL_H}
          />
        )),
      )}
      <Edge
        id={id}
        head={false}
        points={[
          [laneX(0), BUS_Y],
          [laneX(THREADS - 1), BUS_Y],
        ]}
      />
      {Array.from({length: THREADS}, (_, lane) => (
        <Edge key={lane} id={id} head={false} points={[[laneX(lane), Q_BOTTOM], [laneX(lane), BUS_Y]]} />
      ))}

      <Region x={M} y={B_TOP} w={runtimeRight - M} h={B_BOTTOM - B_TOP} />
      <Text kind="title" x={M + PAD} y={B_TITLE} lines="2 I/O workers" />
      <Text x={M + PAD} y={B_NOTE} lines={text.worker} />
      {f.workers.map((wk, w) => {
        const x = workerX(w);
        return (
          <g key={w} data-worker={wk.work}>
            <Edge id={id} points={[[x, BUS_Y], [x, WORKER_Y - CHIP_H / 2 - ARROW_GAP]]} />
            <rect className={frame.chip} x={x - WORKER_W / 2} y={WORKER_Y - CHIP_H / 2} width={WORKER_W} height={CHIP_H} rx="4" />
            <Progress x={x} y={WORKER_Y} w={WORKER_W} f={wk.work === 'idle' ? 0 : wk.progress} />
            <Edge id={id} points={[[x, WORKER_Y + CHIP_H / 2], [x, SINK_Y - CHIP_H / 2 - ARROW_GAP]]} />
          </g>
        );
      })}

      <rect className={frame.chip} x={WORKER_LEFT} y={SINK_Y - CHIP_H / 2} width={runtimeRight - PAD - WORKER_LEFT} height={CHIP_H} rx="4" />
      <Text kind="label" x={(WORKER_LEFT + runtimeRight - PAD) / 2} y={centredBaseline(SINK_Y)} lines="ClickHouse" anchor="middle" />

      {f.chunks.map((c) => {
        const w = PACKED_W + (CHUNK_W - PACKED_W) * c.size;
        return (
          <rect
            key={c.id}
            data-chunk={c.size === 0 ? 'compressed' : 'uncompressed'}
            className={c.size === 0 ? styles.packed : styles.raw}
            x={Number((c.at[0] - w / 2).toFixed(2))}
            y={Number((c.at[1] - CHUNK_H / 2).toFixed(2))}
            width={Number(w.toFixed(2))}
            height={CHUNK_H}
            rx="1"
          />
        );
      })}
    </>
  );
}

function Swatch({className, w, h, rx = 1}: {className: string; w: number; h: number; rx?: number}): React.JSX.Element {
  return (
    <svg className={frame.swatch} viewBox="0 0 33 20" width={33} height={20} aria-hidden="true">
      <rect className={className} x={(33 - w) / 2} y={10 - h / 2} width={w} height={h} rx={rx} />
    </svg>
  );
}

const CAPTION =
  'In 0.3.0, two I/O workers compressed every insert body before sending it, and the pipeline threads waited for queue space. On 799d3640, each pipeline thread compresses the chunks it seals, and the I/O workers only send them. The drawing shows four of the 32 threads and gives each one queue. In Spate a thread can route rows to several shard queues, and one full queue stalls every thread with a chunk for it.';

/**
 * Before and after moving insert compression onto the pipeline threads. Under reduced motion both
 * drawings hold one frame that still shows the stalled threads and the compressed chunks.
 */
export default function WhereCompressionRuns(): React.JSX.Element {
  const id = useId();
  const captionId = `${id}-caption`;
  const [state, dispatch] = useReducer(playback, INITIAL);
  const [ref, onScreen] = useOnScreen<HTMLElement>();
  useEffect(() => {
    const media = window.matchMedia('(prefers-reduced-motion: reduce)');
    const update = () => dispatch({type: 'motion', reduced: media.matches});
    update();
    media.addEventListener('change', update);
    return () => media.removeEventListener('change', update);
  }, []);
  const live = running(state) && onScreen;
  useEffect(() => {
    if (!live) return undefined;
    const start = performance.now() - state.time * 1000;
    let handle: number;
    const tick = (now: number) => {
      dispatch({type: 'tick', time: ((now - start) / 1000) % CYCLE});
      handle = requestAnimationFrame(tick);
    };
    handle = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(handle);
    // The clock restarts from the frame shown whenever it starts again, so `state.time` is read
    // only at start.
  }, [live]);
  return (
    <figure ref={ref} className={frame.figure} data-motion={state.reduced ? 'reduced' : 'full'}>
      <div className={styles.panels}>
        {(['before', 'after'] as const).map((mode) => (
          <svg
            key={mode}
            data-mode={mode}
            viewBox={VIEW}
            style={atMostOneToOne(VIEW)}
            role="img"
            aria-label={TEXT[mode].name}
            aria-describedby={captionId}>
            <Markers id={`${id}${mode}`} />
            <Drawing mode={mode} time={state.time} id={`${id}${mode}`} />
          </svg>
        ))}
      </div>
      <ul className={frame.legend} aria-label="Legend">
        <li>
          <Swatch className={styles.raw} w={CHUNK_W} h={CHUNK_H} />
          <span>uncompressed chunk</span>
        </li>
        <li>
          <Swatch className={styles.packed} w={PACKED_W} h={CHUNK_H} />
          <span>compressed chunk</span>
        </li>
        <li>
          <Swatch className={frame.cell} w={CELL_W} h={CELL_H} rx={0} />
          <span>queue slot</span>
        </li>
        <li>
          <Swatch className={styles.stalled} w={26} h={14} rx={3} />
          <span>thread waiting for a slot</span>
        </li>
      </ul>
      {!state.reduced && (
        <button type="button" className={styles.toggle} onClick={() => dispatch({type: 'toggle'})}>
          {state.paused ? 'Play' : 'Pause'}
        </button>
      )}
      <figcaption id={captionId} className={frame.caption}>
        {CAPTION}
      </figcaption>
    </figure>
  );
}
