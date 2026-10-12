import React, {useId} from 'react';

import {Chip, Edge, Markers, Queue, Region, styles} from './parts';

const SUMMARY =
  'A split terminal on one pipeline thread. Records from the source are deserialized and pass the ' +
  'operator chain to one classifier, which sends each record to one branch. Each branch, here ' +
  '"payments" and "refunds", routes the record to a shard and encodes it, then hands it to that ' +
  "sink's own shard queues and workers, which write to its destination. A record that reaches no " +
  'branch follows the unmatched policy: Fail stops the pipeline, Skip drops the record and counts it.';

const BRANCHES = [
  {name: '"payments"', y: 96},
  {name: '"refunds"', y: 152},
];

function Wide({id}: {id: string}): React.JSX.Element {
  return (
    <svg className={styles.wide} viewBox="0 0 740 252" role="img" aria-label={SUMMARY}>
      <Markers id={id} />
      <Region x={66} y={40} w={486} h={202} title="pipeline thread" />

      <circle className={styles.node} cx={36} cy={152} r={12} />
      <text className={styles.label} x={36} y={130} textAnchor="middle">
        source
      </text>
      <Edge id={id} points={[[48, 152], [80, 152]]} />
      <Chip x={82} y={152} w={100} label="deserialize" />
      <Edge id={id} points={[[182, 152], [194, 152]]} />
      <Chip x={196} y={152} w={88} label="operators" />
      <Edge id={id} points={[[284, 152], [296, 152]]} />
      <Chip x={298} y={152} w={84} label="classify" />

      {[...BRANCHES.map((b) => b.y), 214].map((y) => (
        <Edge key={y} id={id} points={[[382, 152], [392, 152], [404, y]]} />
      ))}

      {BRANCHES.map(({name, y}) => (
        <React.Fragment key={name}>
          <text className={styles.note} x={406} y={y - 18}>
            branch {name}
          </text>
          <Chip x={406} y={y} w={58} label="route" />
          <Edge id={id} points={[[464, y], [476, y]]} />
          <Chip x={478} y={y} w={62} label="encode" />
          <Edge id={id} points={[[540, y], [560, y]]} head={false} />
          <Queue x={562} y={y} />
          <Edge id={id} points={[[600, y], [631, y]]} />
          <circle className={styles.node} cx={640} cy={y} r={7} />
          <Edge id={id} points={[[647, y], [687, y]]} />
          <circle className={styles.node} cx={700} cy={y} r={11} />
          <text className={styles.label} x={700} y={y + 28} textAnchor="middle">
            {name.replace(/"/g, '')}
          </text>
        </React.Fragment>
      ))}
      <text className={styles.note} x={562} y={56}>
        each sink&apos;s own
      </text>
      <text className={styles.note} x={562} y={72}>
        queues and workers
      </text>

      <Chip x={406} y={214} w={134} label="unmatched" />
      <Edge id={id} kind="signal" points={[[540, 214], [560, 214]]} />
      <text className={styles.chipLabel} x={566} y={210}>
        Fail: stop the pipeline
      </text>
      <text className={styles.note} x={566} y={228}>
        Skip: drop and count
      </text>
    </svg>
  );
}

const COLUMNS = [
  {name: '"payments"', x: 70},
  {name: '"refunds"', x: 180},
];

/** The portrait layout for narrow screens: the branches become columns under the classifier. */
function Narrow({id}: {id: string}): React.JSX.Element {
  return (
    <svg className={styles.narrow} viewBox="0 0 360 440" role="img" aria-label={SUMMARY}>
      <Markers id={id} />
      <Region x={20} y={50} w={320} h={252} title="pipeline thread" />

      <circle className={styles.node} cx={180} cy={22} r={10} />
      <text className={styles.label} x={196} y={26}>
        source
      </text>
      <Edge id={id} points={[[180, 32], [180, 87]]} />
      {['deserialize', 'operators', 'classify'].map((label, i) => (
        <Chip key={label} x={110} y={100 + i * 34} w={140} label={label} />
      ))}
      {[0, 1].map((i) => (
        <Edge key={i} id={id} points={[[180, 111 + i * 34], [180, 121 + i * 34]]} />
      ))}

      {[...COLUMNS.map((c) => c.x), 290].map((x) => (
        <Edge key={x} id={id} points={[[180, 179], [180, 188], [x, 195], [x, 200]]} />
      ))}

      {COLUMNS.map(({name, x}) => (
        <React.Fragment key={name}>
          <text className={styles.note} x={x} y={216} textAnchor="middle">
            {name}
          </text>
          <Edge id={id} points={[[x, 223], [x, 228]]} />
          <Chip x={x - 40} y={240} w={80} label="route" />
          <Edge id={id} points={[[x, 251], [x, 262]]} />
          <Chip x={x - 40} y={274} w={80} label="encode" />
          <Edge id={id} points={[[x, 285], [x, 312]]} head={false} />
          <Queue x={x - 17} y={320} />
          <Edge id={id} points={[[x, 327], [x, 346]]} />
          <circle className={styles.node} cx={x} cy={354} r={7} />
          <Edge id={id} points={[[x, 361], [x, 384]]} />
          <circle className={styles.node} cx={x} cy={396} r={10} />
          <text className={styles.label} x={x} y={426} textAnchor="middle">
            destination
          </text>
        </React.Fragment>
      ))}

      <text className={styles.note} x={290} y={216} textAnchor="middle">
        unmatched
      </text>
      <text className={styles.chipLabel} x={290} y={244} textAnchor="middle">
        Fail: stops
      </text>
      <text className={styles.chipLabel} x={290} y={261} textAnchor="middle">
        the pipeline
      </text>
      <text className={styles.note} x={290} y={280} textAnchor="middle">
        Skip: drops
      </text>
      <text className={styles.note} x={290} y={295} textAnchor="middle">
        and counts
      </text>
      <text className={styles.note} x={240} y={340}>
        each sink&apos;s own
      </text>
      <text className={styles.note} x={240} y={355}>
        queues, workers
      </text>
    </svg>
  );
}

/** The split terminal on the Multi-sink concept page: one classifier, typed branches, the unmatched policy. */
export default function SplitDiagram(): React.JSX.Element {
  const id = useId().replace(/:/g, '');
  return (
    <figure className={styles.figure}>
      <Wide id={`${id}w`} />
      <Narrow id={`${id}n`} />
    </figure>
  );
}
