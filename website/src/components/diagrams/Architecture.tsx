import React, {useId} from 'react';

import {Callout, Chip, Edge, Markers, Queue, Region, styles} from './parts';

const SUMMARY =
  'One pipeline process. Records flow from the source over lanes into N pipeline threads, each of ' +
  'which polls a lane, deserializes, runs the operator chain, routes each record to a shard and ' +
  'encodes it, then try_sends the chunk to a bounded per-shard queue. On the I/O runtime one shard ' +
  'worker per shard batches, seals and writes to the destination, rotating replicas and retrying; ' +
  'the runtime also hosts the admin server. When a queue fills, the pipeline thread sends a pause ' +
  'request to the controller thread. Shard workers send acks over an unbounded channel to the ' +
  "controller's checkpointer, which turns them into watermarks; the controller commits them to the " +
  'source, pauses and resumes it, and keeps polling its events while lanes are paused.';

const STAGES = ['poll a lane', 'deserialize', 'operator chain', 'route to a shard', 'encode, chunk'];
const SHARDS = [215, 247, 279];

function Wide({id}: {id: string}): React.JSX.Element {
  return (
    <svg className={styles.wide} viewBox="0 0 740 440" role="img" aria-label={SUMMARY}>
      <Markers id={id} />

      <Region x={96} y={48} w={210} h={248} title="pipeline threads × N" note="std threads, opt-in pinning" stacked />
      <Region x={410} y={48} w={220} h={248} title="I/O runtime" note="one tokio runtime, io_threads" />
      <Region x={96} y={334} w={534} h={90} title="controller thread" />

      <circle className={styles.node} cx={44} cy={111} r={13} />
      <text className={styles.label} x={44} y={88} textAnchor="middle">
        source
      </text>
      {[103, 111, 119].map((y) => (
        <Edge key={y} id={id} points={[[57, 111], [114, y]]} head={false} />
      ))}
      <text className={styles.note} x={76} y={140} textAnchor="middle">
        lanes
      </text>

      {STAGES.map((label, i) => (
        <Chip key={label} x={116} y={111 + i * 34} w={156} label={label} />
      ))}
      {STAGES.slice(1).map((label, i) => (
        <Edge key={label} id={id} points={[[194, 123 + i * 34], [194, 133 + i * 34]]} />
      ))}

      <text className={styles.note} x={360} y={174} textAnchor="middle">
        try_send to
      </text>
      <text className={styles.note} x={360} y={190} textAnchor="middle">
        shard queues
      </text>
      {SHARDS.map((y) => (
        <React.Fragment key={y}>
          <Edge id={id} points={[[272, 247], [314, 247], [334, y]]} head={false} />
          <Queue x={338} y={y} />
          <Edge id={id} points={[[376, y], [420, y]]} />
          <circle className={styles.node} cx={430} cy={y} r={8} />
          <Edge id={id} points={[[438, y], [678, 247 + (y - 247) / 5]]} />
        </React.Fragment>
      ))}

      <Chip x={424} y={113} w={124} label="admin server" />
      <text className={styles.chipLabel} x={424} y={156}>
        one shard worker per shard:
      </text>
      <text className={styles.note} x={424} y={174}>
        batch, seal, write,
      </text>
      <text className={styles.note} x={424} y={190}>
        rotate replicas, retry
      </text>

      <circle className={styles.node} cx={692} cy={247} r={13} />
      <text className={styles.label} x={692} y={224} textAnchor="middle">
        destination
      </text>

      <Edge id={id} kind="signal" points={[[270, 296], [270, 360]]} />
      <text className={styles.label} x={278} y={320}>
        pause request
      </text>
      <Edge id={id} kind="signal" points={[[430, 287], [430, 360]]} />
      <text className={styles.label} x={438} y={320}>
        acks, unbounded
      </text>
      <Edge id={id} kind="signal" points={[[116, 375], [44, 375], [44, 126]]} />
      <text className={styles.label} x={52} y={320}>
        commit, pause, resume
      </text>

      <Chip x={116} y={375} w={180} h={24} label="source control plane" />
      <Chip x={390} y={375} w={140} h={24} label="checkpointer" />
      <Edge id={id} kind="mark" points={[[390, 375], [298, 375]]} />
      <text className={styles.note} x={343} y={366} textAnchor="middle">
        watermarks
      </text>
      <text className={styles.note} x={116} y={410}>
        polls events while lanes pause
      </text>
      <text className={styles.note} x={390} y={410}>
        synchronous, tokio-free
      </text>
    </svg>
  );
}

const COLUMNS = [140, 180, 220];

/**
 * The portrait layout for narrow screens. The data path runs down the left with the controller
 * thread beside the pipeline threads, so the pause request crosses one short gap. Line labels move to the numbered key under the figure.
 */
function Narrow({id}: {id: string}): React.JSX.Element {
  return (
    <svg className={styles.narrow} viewBox="0 0 360 580" role="img" aria-label={SUMMARY}>
      <Markers id={id} />

      <Region
        x={34}
        y={80}
        w={190}
        h={220}
        title="pipeline threads × N"
        note={['std threads,', 'opt-in pinning']}
        stacked
      />
      <Region x={34} y={368} w={320} h={156} title="I/O runtime" />
      <Region x={256} y={80} w={98} h={272} title={['controller', 'thread']} />

      <circle className={styles.node} cx={17} cy={40} r={10} />
      <text className={styles.label} x={32} y={44}>
        source
      </text>
      {[
        [21, 146],
        [17, 152],
        [13, 158],
      ].map(([x, y]) => (
        <Edge key={y} id={id} points={[[x, 50], [x, y], [50, y]]} head={false} />
      ))}
      <text className={styles.note} x={28} y={62}>
        lanes
      </text>

      {STAGES.map((label, i) => (
        <Chip key={label} x={50} y={152 + i * 30} w={156} label={label} />
      ))}
      {STAGES.slice(1).map((label, i) => (
        <Edge key={label} id={id} points={[[128, 163 + i * 30], [128, 171 + i * 30]]} />
      ))}

      <text className={styles.note} x={120} y={322} textAnchor="end">
        try_send
      </text>
      {COLUMNS.map((x) => (
        <React.Fragment key={x}>
          <Edge id={id} points={[[128, 283], [128, 318], [x, 330], [x, 336]]} head={false} />
          <Queue x={x - 17} y={344} />
          <Edge id={id} points={[[x, 351], [x, 410]]} />
          <circle className={styles.node} cx={x} cy={420} r={8} />
          <Edge id={id} points={[[x, 428], [180 + (x - 180) / 4, 543]]} />
        </React.Fragment>
      ))}
      <text className={styles.note} x={48} y={440}>
        shard
      </text>
      <text className={styles.note} x={48} y={455}>
        workers
      </text>
      <Chip x={250} y={494} w={96} label="admin server" />

      <circle className={styles.node} cx={180} cy={556} r={12} />
      <text className={styles.label} x={160} y={560} textAnchor="end">
        destination
      </text>

      <Chip x={258} y={156} w={94} h={40} small label={['source', 'control plane']} />
      <Chip x={258} y={320} w={94} h={24} small label="checkpointer" />

      <Edge id={id} kind="signal" points={[[264, 136], [264, 20], [17, 20], [17, 28]]} />
      <Edge id={id} kind="signal" points={[[224, 156], [258, 156]]} />
      <Edge id={id} kind="signal" points={[[228, 420], [305, 420], [305, 334]]} />
      <Edge id={id} kind="mark" points={[[305, 308], [305, 178]]} />
      <Callout x={180} y={20} n={1} />
      <Callout x={246} y={156} n={2} />
      <Callout x={266} y={420} n={3} />
      <Callout x={305} y={243} n={4} />
    </svg>
  );
}

/** The process layout on the Architecture concept page: pipeline threads, I/O runtime, controller. */
export default function ArchitectureDiagram(): React.JSX.Element {
  const id = useId().replace(/:/g, '');
  return (
    <figure className={styles.figure}>
      <Wide id={`${id}w`} />
      <Narrow id={`${id}n`} />
      <ol className={styles.key}>
        <li>
          The controller commits watermarks to the source and pauses or resumes its lanes. It keeps
          polling the source&apos;s events while lanes are paused.
        </li>
        <li>A pipeline thread whose shard queue is full asks the controller to pause its lanes.</li>
        <li>
          Shard workers send acks over an unbounded channel to the checkpointer, which is synchronous
          and runs without an async runtime.
        </li>
        <li>The checkpointer turns acks into watermarks.</li>
      </ol>
    </figure>
  );
}
