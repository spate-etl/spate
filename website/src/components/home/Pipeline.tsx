import React from 'react';

type Props = {className?: string};

type Point = [number, number];

// The lane that stalls, and the fraction of it the stalled record holds at.
const PAUSED = 3;
const PAUSED_AT = 0.62;

const RING = {cx: 60, cy: 150, r: 30};
const RING_C = 2 * Math.PI * RING.r;

const SHARD_Y = [98, 150, 202];
const CELL_X = [400, 409, 418, 427];

// The shard whose queue fills, and the cycle fractions its cells fill and drain on.
const FULL_SHARD = 2;
const CELL_FILL = [0.18, 0.26, 0.33];
const CELL_DRAIN = [0.66, 0.6, 0.54];

/**
 * Every data edge as a polyline. The drawn edge, the records riding it and the still frame read
 * the same points, so moving an edge moves all three.
 */
const EDGES: Point[][] = [
  [
    [78, 150],
    [106, 150],
    [148, 108],
    [196, 108],
  ],
  [
    [78, 150],
    [106, 150],
    [120, 136],
    [196, 136],
  ],
  [
    [78, 150],
    [106, 150],
    [120, 164],
    [196, 164],
  ],
  [
    [78, 150],
    [106, 150],
    [148, 192],
    [196, 192],
  ],
  [
    [326, 118],
    [400, 98],
  ],
  [
    [326, 150],
    [400, 150],
  ],
  [
    [326, 182],
    [400, 202],
  ],
  [
    [462, 98],
    [478, 98],
  ],
  [
    [462, 150],
    [478, 150],
  ],
  [
    [462, 202],
    [478, 202],
  ],
];

/** The stalled lane past the trunk, so the dim overlay covers one lane rather than the shared run. */
const PAUSED_EDGE: Point[] = [
  [106, 150],
  [148, 192],
  [196, 192],
];

/** The acknowledgement return and the watermark commit, drawn in the instrument-signal weight. */
const SIGNALS: Point[][] = [
  [
    [454, 210],
    [454, 262],
    [142, 262],
  ],
  [
    [124, 262],
    [60, 262],
    [60, 186],
  ],
];

/** Where the still frame parks a record, as an edge and a fraction of that edge's length. */
const STILL: Array<[number, number]> = [
  [0, 0.45],
  [0, 0.85],
  [1, 0.3],
  [1, 0.7],
  [2, 0.55],
  [2, 0.15],
  [PAUSED, PAUSED_AT],
  [4, 0.5],
  [5, 0.3],
  [5, 0.8],
  [6, 0.6],
  [7, 0.5],
  [8, 0.3],
  [9, 0.7],
];

const STILL_SIGNAL: Array<[number, number]> = [
  [0, 0.4],
  [0, 0.85],
  [1, 0.6],
];

function pathData(edge: Point[]): string {
  return edge.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x} ${y}`).join(' ');
}

/** The point a fraction `t` along an edge's total length. */
function pointAt(edge: Point[], t: number): Point {
  const spans = edge.slice(1).map(([x, y], i) => Math.hypot(x - edge[i][0], y - edge[i][1]));
  let left = t * spans.reduce((a, b) => a + b, 0);
  let i = 0;
  while (i < spans.length - 1 && left > spans[i]) {
    left -= spans[i];
    i += 1;
  }
  const k = left / spans[i];
  const [x0, y0] = edge[i];
  const [x1, y1] = edge[i + 1];
  return [Number((x0 + (x1 - x0) * k).toFixed(2)), Number((y0 + (y1 - y0) * k).toFixed(2))];
}

/** The dash pattern showing `step` of four watermark steps committed. */
function ringDash(step: number): string {
  return `${((RING_C * step) / 4).toFixed(1)} ${RING_C.toFixed(1)}`;
}

/**
 * The pipeline as a closed circuit. One source's partitions ride lanes into a stack of pinned
 * threads, encoded chunks cross bounded per-shard queues to sharded replicas, and acknowledgements
 * return along the bottom to a watermark that advances one step at a time.
 *
 * Motion is SMIL, so it needs no script. Every `begin` is zero or negative, which puts each record
 * mid-flight on the first frame. `prefers-reduced-motion: reduce` hides the moving parts and shows
 * `.pipeline__still`, whose dots are computed from the polylines the edges are drawn from.
 */
export default function Pipeline({className}: Props): React.JSX.Element {
  const record = (edge: number, r: number, dur: string, begin: string, extra: Record<string, string> = {}) => (
    <circle key={`${edge}-${begin}`} r={r} className={edge === PAUSED ? 'pipeline__paused' : undefined}>
      <animateMotion dur={dur} begin={begin} repeatCount="indefinite" path={pathData(EDGES[edge])} {...extra} />
    </circle>
  );
  const signalDot = (signal: number, dur: string, begin: string) => (
    <circle key={`${signal}-${begin}`} r="2.6">
      <animateMotion dur={dur} begin={begin} repeatCount="indefinite" path={pathData(SIGNALS[signal])} />
    </circle>
  );
  const cell = (x: number, y: number, key: string, fill: boolean, children?: React.ReactNode) => (
    <rect
      key={key}
      x={x}
      y={y - 7}
      width="8"
      height="14"
      className={fill ? 'fill' : undefined}
      opacity={children ? 0 : undefined}>
      {children}
    </rect>
  );
  return (
    <figure className="pipeline-figure">
    <svg
      className={className}
      viewBox="0 0 520 300"
      role="img"
      aria-label="One source fans four partition lanes into a stack of pinned threads, each running one loop over a chain of operators; the loop routes encoded chunks through bounded per-shard queues to sharded replicas, and acknowledgements return along the bottom through the checkpointer to the source, whose committed-watermark ring advances one step per acknowledgement. When a shard's queue fills, the lanes on the thread that filled it pause and keep polling while the acknowledgements keep arriving.">
      <defs>
        <marker id="pipeline-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
          <path className="pipeline__marker" d="M0 0 L8 4 L0 8 Z" />
        </marker>
        <marker
          id="pipeline-chain-arrow"
          viewBox="0 0 8 8"
          refX="6"
          refY="4"
          markerWidth="5"
          markerHeight="5"
          orient="auto">
          <path className="pipeline__chain-arrow" d="M0 0 L8 4 L0 8 Z" />
        </marker>
      </defs>

      <g className="pipeline__edges" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        {EDGES.map((edge) => (
          <path key={pathData(edge)} d={pathData(edge)} />
        ))}
        {SHARD_Y.map((y) => (
          <line key={y} x1="436" y1={y} x2="446" y2={y} />
        ))}
      </g>
      <g
        className="pipeline__paused-edge pipeline__anim"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0">
        <path d={pathData(PAUSED_EDGE)} />
        <animate
          attributeName="opacity"
          values="0;1;0"
          keyTimes="0;0.35;0.7"
          dur="6s"
          repeatCount="indefinite"
          calcMode="discrete"
        />
      </g>
      <g
        className="pipeline__paused-edge pipeline__still"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round">
        <path d={pathData(PAUSED_EDGE)} />
      </g>
      <g className="pipeline__signal" strokeWidth="1.4">
        <path d={pathData(SIGNALS[0])} />
        <path d={pathData(SIGNALS[1])} markerEnd="url(#pipeline-arrow)" />
      </g>

      <circle className="pipeline__gauge-track" cx={RING.cx} cy={RING.cy} r={RING.r} />
      <circle
        className="pipeline__gauge pipeline__anim"
        cx={RING.cx}
        cy={RING.cy}
        r={RING.r}
        transform={`rotate(-90 ${RING.cx} ${RING.cy})`}
        strokeDasharray={ringDash(0)}>
        <animate
          attributeName="stroke-dasharray"
          values={[0, 1, 2, 3, 4].map(ringDash).join(';')}
          keyTimes="0;0.25;0.5;0.75;0.97"
          dur="8s"
          repeatCount="indefinite"
          calcMode="discrete"
        />
      </circle>
      <circle
        className="pipeline__gauge pipeline__still"
        cx={RING.cx}
        cy={RING.cy}
        r={RING.r}
        transform={`rotate(-90 ${RING.cx} ${RING.cy})`}
        strokeDasharray={ringDash(2)}
      />

      <g className="pipeline__nodes">
        <circle cx="60" cy="150" r="18" />
        {SHARD_Y.map((y) => (
          <circle key={y} cx="454" cy={y} r="8" />
        ))}
        <rect x="124" y="255" width="14" height="14" rx="2" />
      </g>
      <g className="pipeline__stack">
        {SHARD_Y.map((y) => [500, 492, 484].map((x) => <circle key={`${x}-${y}`} cx={x} cy={y} r="8" />))}
      </g>

      <g className="pipeline__core">
        <rect className="behind" x="208" y="78" width="130" height="120" rx="8" />
        <rect className="behind" x="202" y="84" width="130" height="120" rx="8" />
        <rect x="196" y="90" width="130" height="120" rx="8" />
        <g className="pipeline__chain" strokeWidth="2" strokeLinecap="round">
          <line x1="224" y1="134" x2="298" y2="134" />
          {[219, 243, 267, 291].map((x) => (
            <rect key={x} x={x} y="129" width="10" height="10" rx="2" />
          ))}
          <path
            d="M296 141 L296 158 L224 158 L224 142"
            strokeLinejoin="round"
            markerEnd="url(#pipeline-chain-arrow)"
          />
        </g>
      </g>

      <g className="pipeline__queue" strokeWidth="1.2">
        {SHARD_Y.map((y) => CELL_X.map((x, i) => cell(x, y, `${x}-${y}`, i === 0)))}
        <g className="pipeline__anim">
          {CELL_FILL.map((at, i) =>
            cell(
              CELL_X[i + 1],
              SHARD_Y[FULL_SHARD],
              `anim-${i}`,
              true,
              <animate
                attributeName="opacity"
                values="0;1;0"
                keyTimes={`0;${at};${CELL_DRAIN[i]}`}
                dur="6s"
                repeatCount="indefinite"
                calcMode="discrete"
              />,
            ),
          )}
        </g>
        <g className="pipeline__still">
          {CELL_FILL.map((_, i) => cell(CELL_X[i + 1], SHARD_Y[FULL_SHARD], `still-${i}`, true))}
        </g>
      </g>

      <g className="pipeline__records">
        {record(0, 3.5, '2.4s', '-0s')}
        {record(0, 3.5, '2.4s', '-1.2s')}
        {record(1, 3.5, '2.4s', '-0.5s')}
        {record(1, 3.5, '2.4s', '-1.7s')}
        {record(2, 3.5, '2.4s', '-0.9s')}
        {record(2, 3.5, '2.4s', '-2.1s')}
        {record(PAUSED, 3.5, '6s', '0s', {
          keyTimes: '0;0.35;0.7;1',
          keyPoints: `0;${PAUSED_AT};${PAUSED_AT};1`,
          calcMode: 'linear',
        })}
        {record(4, 3, '1.3s', '-0s')}
        {record(4, 3, '1.3s', '-0.7s')}
        {record(5, 3, '1.3s', '-0.3s')}
        {record(5, 3, '1.3s', '-0.9s')}
        {record(6, 3, '1.3s', '-0.5s')}
        {record(7, 3, '1.6s', '-0s')}
        {record(8, 3, '1.6s', '-0.6s')}
        {record(9, 3, '1.6s', '-1.1s')}
      </g>
      <g className="pipeline__signal-dots">
        {signalDot(0, '4s', '0s')}
        {signalDot(0, '4s', '-2s')}
        {signalDot(1, '2s', '-1s')}
      </g>

      <g className="pipeline__still">
        {STILL.map(([edge, t]) => {
          const [cx, cy] = pointAt(EDGES[edge], t);
          return (
            <circle
              key={`${edge}-${t}`}
              cx={cx}
              cy={cy}
              r={edge < 4 ? 3.5 : 3}
              className={edge === PAUSED ? 'pipeline__paused' : undefined}
            />
          );
        })}
      </g>
      <g className="pipeline__signal-dots pipeline__still">
        {STILL_SIGNAL.map(([signal, t]) => {
          const [cx, cy] = pointAt(SIGNALS[signal], t);
          return <circle key={`${signal}-${t}`} cx={cx} cy={cy} r="2.6" />;
        })}
      </g>

      <g className="pipeline__labels" fontFamily="var(--ifm-font-family-monospace)" fontSize="14" textAnchor="middle">
        <text x="60" y="96">one source</text>
        <text x="60" y="112" fontSize="12">
          watermark
        </text>
        <text x="196" y="78" textAnchor="end">
          partitions
        </text>
        <text x="271" y="66">pinned threads</text>
        <text x="261" y="232">operators</text>
        <text x="417" y="76">bounded queues</text>
        <text x="518" y="234" fontSize="12" textAnchor="end">
          replicas
        </text>
        <text x="300" y="284" fontSize="12">
          acks, unbounded
        </text>
        <text x="131" y="288" fontSize="12">
          checkpointer
        </text>
        <g className="pipeline__paused" fontSize="12" textAnchor="end">
          <text x="188" y="212" className="pipeline__anim" opacity="0">
            paused · polling
            <animate
              attributeName="opacity"
              values="0;1;0"
              keyTimes="0;0.35;0.7"
              dur="6s"
              repeatCount="indefinite"
              calcMode="discrete"
            />
          </text>
          <text x="188" y="212" className="pipeline__still">
            paused · polling
          </text>
        </g>
      </g>
      <g className="pipeline__key-markers" fontFamily="var(--ifm-font-family-monospace)" fontSize="24" textAnchor="middle" aria-hidden="true">
        <text x="60" y="110">1</text>
        <text x="261" y="238">2</text>
        <text x="417" y="76">3</text>
        <text x="492" y="238">4</text>
        <text x="131" y="294">5</text>
      </g>
    </svg>
    <ol className="pipeline__key" aria-label="Pipeline diagram key">
      <li><b>Source.</b> Partition lanes carry records; paused lanes keep polling.</li>
      <li><b>Operators.</b> Each pinned thread runs one operator chain.</li>
      <li><b>Bounded queues.</b> Encoded chunks wait for their sink shard.</li>
      <li><b>Replicas.</b> Sink writes return acknowledgements.</li>
      <li><b>Checkpointer.</b> Acknowledgements return through an unbounded channel; committed watermarks advance behind them.</li>
    </ol>
    </figure>
  );
}
