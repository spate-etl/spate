import React from 'react';

type Props = {className?: string};

const EDGES = ['M5 3.5 L13.6 9.6', 'M5 10 L13.6 10', 'M5 16.5 L13.6 10.4', 'M18.4 10 L27 10'];

/**
 * The confluence, animated: records ride the source edges into the core and
 * leave through the sink, and the third lane pauses and resumes, which is
 * backpressure as the framework applies it. The moving records are SMIL, so
 * they need no script; under `prefers-reduced-motion: reduce` the stylesheet
 * hides them and shows the composed frame instead.
 */
export default function Pipeline({className}: Props): React.JSX.Element {
  const record = (path: string, dur: string, begin: string, extra: Record<string, string> = {}) => (
    <circle r="0.45" key={`${path}-${begin}`}>
      <animateMotion dur={dur} begin={begin} repeatCount="indefinite" path={path} {...extra} />
    </circle>
  );
  return (
    <svg
      className={className}
      viewBox="0 0 32 20"
      fill="none"
      role="img"
      aria-label="Records flow from three sources into one core and out through one sink; one lane pauses under backpressure and resumes">
      <g className="pipeline__edges" strokeWidth="0.5" strokeLinecap="round" fill="none">
        {EDGES.map((d) => (
          <path key={d} d={d} />
        ))}
      </g>
      <g className="pipeline__nodes">
        <circle cx="5" cy="3.5" r="1.3" />
        <circle cx="5" cy="10" r="1.3" />
        <circle cx="5" cy="16.5" r="1.3" />
        <circle cx="27" cy="10" r="1.6" />
      </g>
      <rect className="pipeline__core" x="13.6" y="7.2" width="4.8" height="5.6" rx="1" />
      <g className="pipeline__records">
        {record(EDGES[0], '2.2s', '0s')}
        {record(EDGES[0], '2.2s', '0.7s')}
        {record(EDGES[0], '2.2s', '1.4s')}
        {record(EDGES[1], '2.2s', '0.3s')}
        {record(EDGES[1], '2.2s', '1.0s')}
        {record(EDGES[2], '4.4s', '0.5s', {keyTimes: '0;0.35;0.7;1', keyPoints: '0;0.4;0.4;1', calcMode: 'linear'})}
        {record(EDGES[3], '1.4s', '0s')}
        {record(EDGES[3], '1.4s', '0.35s')}
        {record(EDGES[3], '1.4s', '0.7s')}
        {record(EDGES[3], '1.4s', '1.05s')}
      </g>
      <g className="pipeline__still">
        <circle cx="8.5" cy="6" r="0.45" />
        <circle cx="11.5" cy="8.1" r="0.45" />
        <circle cx="9" cy="10" r="0.45" />
        <circle cx="8.4" cy="14.1" r="0.45" />
        <circle cx="21" cy="10" r="0.45" />
        <circle cx="24" cy="10" r="0.45" />
      </g>
      <g className="pipeline__labels" fontFamily="var(--ifm-font-family-monospace)" fontSize="0.9" textAnchor="middle">
        <text x="5" y="19.4">sources</text>
        <text x="16" y="15.2">one loop</text>
        <text x="27" y="13.6">sink</text>
        <text x="9.3" y="15.4" className="pipeline__paused" fontSize="0.75">
          paused
        </text>
      </g>
    </svg>
  );
}
