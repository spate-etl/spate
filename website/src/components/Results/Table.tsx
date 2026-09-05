import Link from '@docusaurus/Link';
import React from 'react';

import {
  laneRank,
  scaleFor,
  tiedWithLeader,
  type Env,
  type Group,
  type Entrant,
  type Row as BenchRow,
} from './data';
import {columnsFor, defaultColumnsFor, detailFor} from './columns';
import {fmt, unitLabel, unrankedNote} from './format';
import Row, {type Column} from './Row';
import {metricsPresent, modeOf} from './model';

/**
 * One comparability group, as a dense ranked table.
 *
 * WHY ONE ROW PER ARM RATHER THAN A SYSTEMS x METRICS MATRIX
 *
 * A systems x metrics matrix answers "how do these systems compare" beautifully
 * at eight arms: the row order is fixed across every column, so a system's
 * profile is one horizontal scan and an unfavourable column cannot be given less
 * prominence than a favourable one without moving it. At sixty arms that same
 * form is five thousand pixels of table per group, times six groups.
 *
 * The dense table keeps the property that mattered and pays for it differently:
 * the column order is fixed, every arm is one ~30px row, and the ordering claim
 * the page makes is only ever "sorted by this column" — a fact about a column,
 * not a verdict about a system. There is no composite score, deliberately. A
 * weighted index would be an editorial act on a page whose author is one of the
 * rows being weighted.
 *
 * EVERY GROUP IS ITS OWN SCALE
 *
 * Scales belong to one column of one group and are never shared. Two groups that
 * look alike are exactly what invites the comparison the contract forbids, so
 * each column prints its real end values and each group states its own
 * provenance and its own environment caveat.
 */

const WORKLOAD_GLOSS = 'decode, flatten, filter, derive — one row per surviving event';

const MODE_GLOSS: Record<string, string> = {
  drain: 'drain — how fast the system can go through a fixed corpus',
  sustained: 'sustained — the system held at a requested rate',
};

export function columnsOf(rows: BenchRow[]): Column[] {
  const present = metricsPresent(rows);
  const on = new Set(defaultColumnsFor(present));
  return columnsFor(present).map((spec) => {
    const proto = rows.find((r) => r.metrics[spec.id])?.metrics[spec.id];
    const scale = scaleFor(rows, spec.id);
    return {
      ...spec,
      unit: proto?.unit ?? '',
      // Direction is carried per metric on every record, so sorting a
      // lower-is-better column puts the cheapest arm first without the site
      // holding an opinion about which metrics are good.
      higherIsBetter: proto?.higher_is_better ?? true,
      scaleMax: scale?.max ?? null,
      tied: tiedWithLeader(rows, spec.id).tied,
      on: on.has(spec.id),
    };
  });
}

export default function Table({
  group,
  rows,
  entrants,
  byId,
  env,
  baseUrl,
}: {
  group: Group;
  rows: BenchRow[];
  entrants: Entrant[];
  byId: Map<string, Entrant>;
  env: Env | undefined;
  baseUrl: string;
}): React.JSX.Element {
  const {order, ranked} = laneRank(rows, byId);
  const columns = columnsOf(rows);
  const detail = detailFor(metricsPresent(rows));
  // rank + arm + metrics + the disclosure column.
  const colSpan = 3 + columns.length;
  const mode = modeOf(group.key);
  const anyRanked = ranked.size > 0;

  return (
    <div className="bench-block">
      {/* Rendered from the environment's declared `class`, per group rather than
          once at the top of the page. With one environment a page-level banner
          was right; with an authoritative environment beside an indicative one
          it would be wrong for half the content — and `methodology` requires the
          caveat "prominently and in its own right rather than in a footnote". */}
      {env?.class === 'indicative' && (
        <p className="bench-standing">
          <strong>Indicative, not authoritative.</strong>{' '}
          {env.host?.description ?? env.id}. Every figure carries the spread across its
          repetitions for that reason.
        </p>
      )}
      {env?.class === 'fixture' && (
        <p className="bench-standing bench-standing--critical">
          <strong>Fixture environment.</strong> Synthetic development data, never published
          as a result.
        </p>
      )}

      {/* What the summary immediately above this cannot carry, and nothing it
          already says. The environment and the mode are the group's identity
          and belong on the disclosure a reader clicked; repeating them forty
          pixels lower taught nobody anything. What is left is what the identity
          does not tell you: what the workload asks of a system, what the mode
          means for the throughput figure, and the protocol the numbers were
          taken under. */}
      <p className="bench-prov bench-note">
        {WORKLOAD_GLOSS}
        {mode && <> · {MODE_GLOSS[mode] ?? mode}</>} · harness v
        {group.harness_version}
        {group.dataset_version && <> · corpus {group.dataset_version}</>}
      </p>

      {/* Every arm's newest sitting here produced no number, so there is no table
          to draw. Rendering the empty one instead would caption it with the
          not-headline-eligible footer below, which blames rule 3 for an absence
          that is nothing of the kind: these arms were run and they broke. The
          gaps themselves are listed by the caller. */}
      {!rows.length ? (
        <p className="bench-note">
          Nothing in this group has a current measurement: every arm&rsquo;s most recent
          run produced no number. The runs are listed below.
        </p>
      ) : (
      <>
      <div className="bench-scroll">
        <table className="bench-table">
          <caption className="bench-sr-only">
            Arms measured on {group.env_id} under harness v
            {group.harness_version}.{' '}
            {anyRanked
              ? `Ordered by ${(columns[0]?.label ?? 'the primary metric').toLowerCase()}.`
              : 'Nothing here is headline-eligible, so nothing is ranked and the order is the descriptors’ own.'}
          </caption>
          <thead>
            <tr>
              <th scope="col" className="bench-rank">
                #
              </th>
              <th scope="col" className="bench-arm__col">
                System · arm
              </th>
              {columns.map((c) => (
                <th
                  key={c.id}
                  scope="col"
                  className="bench-mh"
                  data-m={c.id}
                  data-unit={c.unit}
                  data-hib={c.higherIsBetter ? '1' : '0'}
                  hidden={!c.on}
                  // Every mark in the column divides by this, so rescaling to a
                  // filtered selection is one property write rather than a
                  // re-render — and with JavaScript off it stays exactly what
                  // the server decided.
                  style={
                    c.scaleMax != null
                      ? ({'--scale-max': c.scaleMax} as React.CSSProperties)
                      : undefined
                  }
                >
                  {/* Sorting needs JavaScript, so the control is revealed by the
                      enhancer rather than shipped as a button that does nothing.
                      With scripting off the server's order stands and the header
                      is plain text. */}
                  <button type="button" className="bench-sort" data-sort={c.id}>
                    <span className="bench-mh__name">{c.label}</span>
                    <span className="bench-sort__ind" aria-hidden="true" />
                  </button>
                  <span className="bench-mh__name bench-mh__static">{c.label}</span>
                  <span className="bench-mh__dir">
                    {c.higherIsBetter ? <>higher is better&nbsp;→</> : <>←&nbsp;lower is better</>}
                  </span>
                  {c.scaleMax != null ? (
                    <span className="bench-axis">
                      <span className="bench-axis__t">0</span>
                      <span className="bench-axis__t">{fmt(c.scaleMax, c.unit)}</span>
                    </span>
                  ) : (
                    <span className="bench-axis bench-axis--none">
                      <span className="bench-note">no axis</span>
                    </span>
                  )}
                  {/* The catalogue overrides the unit where the metric's own
                      unit string is not the whole truth — `records/s` over a
                      column that has already divided by cores. */}
                  <span className="bench-mh__unit">{c.unitLabel ?? unitLabel(c.unit)}</span>
                </th>
              ))}
              <th scope="col" className="bench-disclose__cell">
                <span className="bench-mh__dir">detail</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {order.map((r) => (
              <Row
                key={r.key}
                row={r}
                entrant={byId.get(r.entrant)}
                entrants={entrants}
                columns={columns}
                place={ranked.get(r.key)}
                colSpan={colSpan}
                detail={detail}
                baseUrl={baseUrl}
              />
            ))}
          </tbody>
        </table>
      </div>

      <p className="bench-note bench-block__foot">
        {anyRanked ? (
          <>
            Ranked positions are given only to <code>realistic</code> arms that passed the
            infrastructure-headroom limit.{' '}
            {/* Rewritten by the enhancer against the rows actually visible. The
                server's count is the whole group; the Show control can hide some
                of them. */}
            <span data-bench-unranked="">{unrankedNote(rows.length - ranked.size)}</span>
          </>
        ) : (
          <>
            Nothing in this group is headline-eligible under rule 3 of{' '}
            <Link to={`${baseUrl}contract/rules`}>the fairness contract</Link>, so nothing is
            ranked and the order is the descriptors&rsquo; own. It is not a result.{' '}
          </>
        )}
        Every column has its own scale and no scale is shared with any other group.
      </p>
      </>
      )}
    </div>
  );
}
