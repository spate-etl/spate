import Link from '@docusaurus/Link';
import {usePluginData} from '@docusaurus/useGlobalData';
import React from 'react';

import {EMPTY, type Attempt, type Data, type Entrant, type Row} from './data';
import Controls from './Controls';
import Table, {columnsOf} from './Table';
import {columnsFor, facetsOf, showClassesFor} from './columns';
import {metricsPresent, modeOf, showClassOf} from './model';
import {enhance} from './enhance';

/**
 * The results surface.
 *
 * WHAT THIS PAGE IS SHAPED BY
 *
 * The benchmark grows along three axes at once: systems toward twenty-plus,
 * variants per system, and environments. The comparability key is a
 * five-tuple, so three environments times two modes is six groups — and group
 * count, not row count, is what breaks a results page first. A design that
 * stacks every group as its own block with its own legend and prose is
 * thousands of pixels deep before anyone has filtered anything.
 *
 * So: one dense ranked table per group, every group on one page, and the group a
 * reader is looking at chosen by a native disclosure rather than by script.
 *
 * FULLY PRERENDERED — NO CLIENT-SIDE STATE
 *
 * Every number is in the HTML that leaves the server, so a reader can check by
 * viewing source that the figures were not assembled in their browser out of
 * something else. On a benchmark published by the author of one of the systems
 * in it, that property is the point. `enhance.ts` only ever hides, reorders and
 * rescales what is already there.
 *
 * THE BRIEF
 *
 * Here are the results; make your own determination. There is no headline
 * sentence naming a winner and no composite score, because both are the page
 * making a claim on a reader's behalf. What sits above the table is what was
 * measured, on what, under which protocol, and who runs this.
 */

function useData(): Data {
  return (usePluginData('bench-data') as Data | undefined) ?? EMPTY;
}

function NoResults({data}: {data: Data}) {
  const active = data.entrants.filter((e) => e.entrant.status === 'active');
  const planned = data.entrants.filter((e) => e.entrant.status === 'planned');
  return (
    <div className="bench-empty">
      <h3>No measurements published yet</h3>
      <p>
        The harness, the workload and the fairness contract are in place, but no run has
        been recorded against them. Rather than show a number that does not exist, this
        page says so.
      </p>
      <p className="bench-note">
        {active.length} system{active.length === 1 ? '' : 's'} implemented, {planned.length}{' '}
        planned.
      </p>
    </div>
  );
}

function Attempts({attempts}: {attempts: Attempt[]}) {
  if (!attempts.length) return null;
  return (
    <details className="bench-attempts">
      <summary>
        {attempts.length} run{attempts.length === 1 ? '' : 's'} here produced no number
      </summary>
      <ul>
        {attempts.map((a) => (
          <li key={`${a.entrant}-${a.variant_id}-${a.ts_ms}`}>
            {a.entrant} <code>{a.variant_id}</code> — {a.status.replace(/_/g, ' ')}
            {/* One entry is one sweep, so how many repetitions it took down is
                worth saying: failing every one of three is not the same event as
                failing one. */}
            {a.reps_counted > 1 && ` (${a.reps_counted} repetitions)`}
            {a.note && <span className="bench-note"> · {a.note}</span>}
          </li>
        ))}
      </ul>
    </details>
  );
}

/**
 * The widest A/A spread any displayed sweep measured, or `null` if none did.
 *
 * The widest rather than the mean: it is the floor a reader is being warned
 * about, and the warning has to hold for every number on the page.
 */
function measuredFloor(rows: Row[]): number | null {
  const seen = rows.map((r) => r.aa_spread).filter((n): n is number => typeof n === 'number');
  return seen.length ? Math.max(...seen) : null;
}

/** How to read a mark, drawn rather than described. */
function Legend({floor}: {floor: number | null}) {
  return (
    <details className="bench-legend">
      <summary>How to read this</summary>
      <div className="bench-legend__grid">
        <div>
          <span className="bench-trk bench-legend__demo" aria-hidden="true">
            <span className="bench-rg" style={{left: '22%', width: '38%'}} />
            <span className="bench-md" style={{left: '46%'}} />
          </span>
          <p>
            <strong>The mark is the uncertainty.</strong> The capsule spans the smallest to
            the largest repetition and the notch is the median — at three repetitions those
            are the three measurements, and nothing is modelled.{' '}
            {floor === null ? (
              <>
                No sweep here has measured what this rig does when nothing changes, so
                treat every difference narrower than the capsules as unresolved.
              </>
            ) : (
              <>
                Measuring one arm twice under two labels in these sweeps moved it by{' '}
                {(floor * 100).toFixed(1)}%, so a difference narrower than that is the rig
                rather than the system.
              </>
            )}
          </p>
        </div>
        <div>
          <span className="bench-trk bench-legend__demo" aria-hidden="true">
            <span className="bench-rg bench-rg--ctx" style={{left: '48%', width: '30%'}} />
            <span className="bench-md bench-md--ctx" style={{left: '62%'}} />
          </span>
          <p>
            <strong>Grey is shown, not ranked.</strong> A <code>tuned</code> or{' '}
            <code>stripped</code> arm is drawn on the same axis, because quantifying its
            difference is the only reason it exists — but it gets no rank and it never sets
            the scale.
          </p>
        </div>
        <div>
          <span className="bench-trk bench-trk--void bench-legend__demo" aria-hidden="true">
            <span className="bench-void" />
          </span>
          <p>
            <strong>An empty lane is a disowned number.</strong> An infra-bound figure keeps
            its digits and its reason but not its position — a position on a shared axis is
            itself a claim, and that number describes ClickHouse rather than the system.
          </p>
        </div>
        <div>
          <p>
            <strong>
              A dagger <span className="bench-vendor">†</span> is a conflict of interest.
            </strong>{' '}
            It marks a system run by the author of this benchmark, on every row that system
            has, and it renders from the descriptor rather than from anything the site knows
            about that name. No published number is reported by the system that produced it.
          </p>
        </div>
        <div>
          <p>
            <strong>Every axis starts at zero</strong> and prints its real end value, so no
            difference is magnified by a cropped scale. Scales belong to one column of one
            group and are never shared across groups. No system has a colour, and nothing is
            coloured by whether its number is good.
          </p>
        </div>
      </div>
    </details>
  );
}

export default function Results(): React.JSX.Element {
  const data = useData();
  const baseUrl = data.basePath;

  const entrants = data.entrants as Entrant[];
  const rows = data.rows as Row[];
  const byId = new Map(entrants.map((e) => [e.entrant.id, e]));
  const envById = new Map(data.environments.map((e) => [e.id, e]));

  React.useEffect(() => enhance(), [data]);

  // "Nothing has run" and "everything that ran failed" are different claims, and
  // only the first belongs here. Since only an arm's newest sitting is published,
  // one bad sweep across every arm empties `rows` while leaving the gaps that
  // explain it — and reporting that as "no run has been recorded" would be the
  // page at its least honest exactly when it has the most to admit.
  if (!rows.length && !data.attempts.length) {
    return (
      <div className="bench-root bench-wide">
        <NoResults data={data} />
      </div>
    );
  }

  // Groups that actually carry something, richest first — the group a reader
  // meets is the one with the most to compare, and it is the same group with
  // scripting on or off because nothing here is chosen in the browser.
  const groups = data.groups
    .map((g) => ({
      group: g,
      rows: rows.filter((r) => r.group === g.key),
      attempts: data.attempts.filter((a) => a.group === g.key),
    }))
    .filter((g) => g.rows.length || g.attempts.length)
    .sort((a, b) => b.rows.length - a.rows.length);

  const present = new Set(rows.map((r) => r.entrant));
  const inPlay = entrants.filter((e) => present.has(e.entrant.id));
  const facets = facetsOf(inPlay);
  const columns = columnsFor(metricsPresent(rows));
  const showClasses = showClassesFor(rows.map(showClassOf));
  const ours = entrants.filter((e) => e.entrant.vendor === 'self');
  const envs = [...new Set(rows.map((r) => r.env_id))];

  // `bench-wide` is the page-width opt-in and is deliberately not this
  // component's own class. It says "this subtree is data, give it the viewport";
  // `custom.css` keys the frame off it, so any other page can ask for the same
  // treatment without the stylesheet having to learn that page's name.
  return (
    <div className="bench-root bench-wide">
      {/* The context strip. What was measured, on what, and who runs it — and
          then straight into the table. No verdict: the reader makes their own. */}
      <div className="bench-strip">
        <p className="bench-strip__what">
          <strong>Kafka → Avro → ClickHouse.</strong> 32 CPU and 96 GiB of data plane per
          system, at-least-once. Every system consumes the same topic, decodes and flattens
          each message, applies the same two filters and two derived columns, and lands the
          surviving rows in ClickHouse.
        </p>
        <p className="bench-note">
          {rows.length} arm{rows.length === 1 ? '' : 's'} · {inPlay.length} system
          {inPlay.length === 1 ? '' : 's'} · {groups.length} measurement context
          {groups.length === 1 ? '' : 's'} across {envs.length} environment
          {envs.length === 1 ? '' : 's'}. Contexts are never compared with one another —{' '}
          <Link to={`${baseUrl}contract/comparability`}>what invalidates a comparison</Link>.
        </p>
        {/* Rendered from `vendor = "self"` in the descriptor. Nothing here
            branches on the literal id — a CI lint enforces that, which is what
            makes the neutrality claim checkable rather than asserted. */}
        {ours.length > 0 && (
          <p className="bench-strip__coi">
            {ours.map((e) => e.entrant.name).join(', ')}{' '}
            {ours.length === 1 ? 'is' : 'are'} run by the author of this benchmark. Every row
            of {ours.length === 1 ? 'that system' : 'those systems'} carries a dagger{' '}
            <span className="bench-vendor">†</span> saying so, and no published number is
            reported by the system that produced it.
          </p>
        )}
      </div>

      <Legend floor={measuredFloor(rows)} />

      <Controls
        facets={facets}
        columns={columns}
        showClasses={showClasses}
        systemCount={inPlay.length}
      />

      {/* Each group is a native disclosure with a shared `name`, which makes them
          mutually exclusive with no script at all: opening one closes the last.
          The alternative — render eighteen, then hide seventeen from JavaScript
          and drive them from a <select> — costs a layout jump on hydration, loses
          focus for anyone inside a disclosure being hidden, and gives a reader
          with scripting off a different page. This gives everyone the same one,
          and a closed disclosure is not laid out, so a reader only ever pays for
          the group they are looking at. */}
      <div className="bench-groups">
        {groups.map((g, i) => {
          const mode = modeOf(g.group.key);
          const env = envById.get(g.group.env_id);
          return (
            <details
              key={g.group.key}
              className="bench-group"
              name="bench-group"
              open={i === 0}
            >
              <summary className="bench-group__sum">
                <span className="bench-group__where">{g.group.env_id}</span>
                {mode && <span className="bench-group__what">{mode}</span>}
                <span className="bench-note">
                  {g.rows.length} arm{g.rows.length === 1 ? '' : 's'}
                  {env?.class && env.class !== 'authoritative' && <> · {env.class}</>}
                </span>
              </summary>
              <Table
                group={g.group}
                rows={g.rows}
                entrants={entrants}
                byId={byId}
                env={env}
                baseUrl={baseUrl}
              />
              <Attempts attempts={g.attempts} />
            </details>
          );
        })}
      </div>

      <h2>The systems</h2>
      <ul className="bench-roster">
        {entrants.map((e) => (
          <li key={e.entrant.id}>
            <Link to={`${baseUrl}systems/${e.entrant.id}`}>{e.entrant.name}</Link>{' '}
            <span className="bench-note">
              {e.entrant.runtime} · {e.entrant.licence} · {e.entrant.status}
            </span>
            {e.entrant.vendor === 'self' && (
              <span className="bench-pill bench-pill--vendor">run by the vendor</span>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}

export {columnsOf};
