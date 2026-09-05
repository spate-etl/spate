import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import {usePluginData} from '@docusaurus/useGlobalData';
import React from 'react';

import {
  EMPTY,
  armLabel,
  iso,
  isRanked,
  laneRank,
  unrankedBecause,
  variantOf,
  type Data,
  type Entrant,
  type Row,
} from './data';
import {CATALOGUE, specOf} from './columns';
import {fmtReps, fmt} from './format';
import {modeOf} from './model';

/**
 * One system's profile. The same template for every entrant, forever.
 *
 * WHO THIS IS FOR
 *
 * A competitor's maintainer, arriving to find out how their system was
 * configured and whether it was handicapped. That reader needs one URL, not a
 * table row — so this page carries everything that is true of the SYSTEM
 * (identity, licence, guarantees, envelope, every arm, upstream review status)
 * while the results table carries what is true of a MEASUREMENT (this arm, in
 * this context, with these knobs and this binding constraint).
 *
 * The two overlap in nothing, which is the only arrangement with nothing to keep
 * in sync.
 *
 * WHY IT IS ONE TEMPLATE
 *
 * `CONTRIBUTING.md` promises that "adding entrant N+1 touches exactly one new
 * directory. There is no central registry to update." A hand-written page per
 * system would quietly break that promise: the twenty-first vendor would owe the
 * site a page as well as a descriptor, and pages written at different times drift
 * into flattering some systems more carefully than others.
 *
 * So this branches on descriptor FIELDS and never on identity. A `planned`
 * entrant renders the same shape with its blockers where its numbers will go, and
 * the route for it is generated from the descriptor the moment the directory
 * exists.
 */

type Props = {profile?: {id?: string}};

function Missing({id}: {id: string}) {
  return (
    <Layout title="System not found">
      <main className="container margin-vert--lg">
        <h1>No such system</h1>
        <p>
          Nothing in this repository declares an entrant with the id <code>{id}</code>.
        </p>
      </main>
    </Layout>
  );
}

export default function SystemPage({profile}: Props): React.JSX.Element {
  const id = profile?.id ?? '';
  const data = (usePluginData('bench-data') as Data | undefined) ?? EMPTY;
  const baseUrl = data.basePath;
  const repo = data.repoUrl;

  const entrants = data.entrants as Entrant[];
  const e = entrants.find((x) => x.entrant.id === id);
  if (!e) return <Missing id={id} />;

  const rows = (data.rows as Row[]).filter((r) => r.entrant === id);
  const attempts = data.attempts.filter((a) => a.entrant === id);
  const byId = new Map(entrants.map((x) => [x.entrant.id, x]));
  const ours = e.entrant.vendor === 'self';
  const variants = e.variants ?? [];

  // Every context this system was measured in, with its place among ALL arms in
  // that context — a rank against only its own arms would be meaningless.
  const contexts = data.groups
    .map((g) => {
      const all = (data.rows as Row[]).filter((r) => r.group === g.key);
      const {order, ranked} = laneRank(all, byId);
      // `order` is the same ordering the results table uses, so this system's
      // arms appear here in the order a reader just saw them in — ranked first,
      // by the primary metric. Filtering `data.rows` directly would list them by
      // timestamp, which puts an unranked arm above a ranked one for no reason
      // a reader could see.
      const mine = order.filter((r) => r.entrant === id) as Row[];
      return {group: g, all, mine, ranked};
    })
    .filter((c) => c.mine.length);

  const headline = CATALOGUE.filter((m) => m.placement === 'default').slice(0, 3);

  return (
    <Layout
      title={e.entrant.name}
      description={`How ${e.entrant.name} is configured and measured in the Spate Benchmark.`}
    >
      <main className="container margin-vert--lg bench-profile">
        <p className="bench-note">
          <Link to={baseUrl}>← All results</Link>
        </p>

        <h1>
          {e.entrant.name}{' '}
          {ours && <span className="bench-pill bench-pill--vendor">run by the vendor</span>}
        </h1>

        <p className="bench-note">
          {[e.entrant.runtime, e.entrant.kind, e.entrant.licence, e.entrant.status]
            .filter(Boolean)
            .join(' · ')}
          {e.entrant.language?.length ? ` · ${e.entrant.language.join(', ')}` : ''}
        </p>

        <p>
          {e.entrant.homepage && <a href={e.entrant.homepage}>Homepage</a>}
          {e.entrant.repo && e.entrant.repo !== e.entrant.homepage && (
            <>
              {e.entrant.homepage && ' · '}
              <a href={e.entrant.repo}>Source</a>
            </>
          )}
          {(e.entrant.homepage || e.entrant.repo) && ' · '}
          <a href={`${repo}/tree/main/entrants/${id}`}>
            Its configuration in this repository
          </a>
        </p>

        {/* Rule 7. `self` is not external validation, so it renders as n/a
            rather than as a tick this project has not earned. */}
        <h2>Has this configuration been reviewed upstream?</h2>
        <p>
          {ours ? (
            <span className="bench-note">n/a — the vendor of this system is upstream.</span>
          ) : e.maintainer?.reviewed_upstream ? (
            e.maintainer.review_url ? (
              <a href={e.maintainer.review_url}>Yes — the review is here.</a>
            ) : (
              <>Yes.</>
            )
          ) : (
            <span className="bench-note">
              Not yet. This configuration has not been checked by the people who maintain
              this system, and until it has, treat its numbers as ours rather than theirs.
            </span>
          )}
        </p>

        {e.guarantees && (
          <>
            <h2>What it was asked to guarantee</h2>
            <p>
              {e.guarantees.delivery ?? 'delivery not declared'}
              {e.guarantees.durability && <> · {e.guarantees.durability}</>}
              {e.guarantees.interval_ms != null && (
                <> every {e.guarantees.interval_ms / 1000}s</>
              )}
            </p>
          </>
        )}

        {e.envelope && (
          <>
            <h2>The envelope it ran in</h2>
            <p>
              {e.envelope.cpus} CPU · {e.envelope.memory} memory
            </p>
            {e.envelope.container?.length ? (
              <ul>
                {e.envelope.container.map((c) => (
                  <li key={c.name ?? c.role}>
                    <code>{c.name}</code> — {c.role} · {c.cpus} CPU · {c.memory}
                  </li>
                ))}
              </ul>
            ) : null}
          </>
        )}

        {e.entrant.status === 'planned' && (
          <>
            <h2>Why this is not measured yet</h2>
            <p>
              {e.planned?.blockers ?? (
                <span className="bench-note">No blockers recorded.</span>
              )}
            </p>
            {e.planned?.licence_gate && (
              <p className="bench-note">Licence gate: {e.planned.licence_gate}</p>
            )}
          </>
        )}

        <h2>Its arms</h2>
        {variants.length ? (
          <ul className="bench-arms">
            {variants.map((v) => (
              <li key={v.id}>
                <strong>{armLabel(e, v, v.id)}</strong> <code>{v.id}</code>
                <span className="bench-note">
                  {' '}
                  · {v.approach ?? 'undeclared'}
                  {v.reports?.wire_format && <> · {v.reports.wire_format}</>}
                  {v.default && <> · default</>}
                </span>
                {v.knobs && Object.keys(v.knobs).length > 0 && (
                  <div className="bench-note">
                    {Object.entries(v.knobs)
                      .map(([k, val]) => `${k} ${String(val)}`)
                      .join(' · ')}
                  </div>
                )}
                {v.unshipped?.length ? (
                  <div className="bench-note">
                    not shipped by the system: {v.unshipped.join(', ')} — which is why this
                    arm is labelled <code>stripped</code> and can never be the headline.
                  </div>
                ) : null}
              </li>
            ))}
          </ul>
        ) : (
          <p className="bench-note">No arms are implemented yet.</p>
        )}

        <h2>Where it has been measured</h2>
        {contexts.length ? (
          contexts.map((c) => {
            const mode = modeOf(c.group.key);
            return (
              <div key={c.group.key} className="bench-ctx">
                <h3>
                  {c.group.env_id}
                  {mode && <> · {mode}</>}
                </h3>
                <table className="bench-profile__t">
                  <thead>
                    <tr>
                      <th scope="col">Arm</th>
                      <th scope="col">Place</th>
                      {headline.map((m) => (
                        <th key={m.id} scope="col">
                          {m.label}
                        </th>
                      ))}
                      <th scope="col">Measured</th>
                    </tr>
                  </thead>
                  <tbody>
                    {c.mine.map((r) => {
                      const why = unrankedBecause(r);
                      return (
                        <tr key={r.key}>
                          <th scope="row">
                            {armLabel(e, variantOf(e, r.variant_id), r.variant_id)}
                            {why && (
                              <span className="bench-note"> · shown, not ranked: {why}</span>
                            )}
                          </th>
                          <td>
                            {isRanked(r) ? (
                              (c.ranked.get(r.key) ?? '—')
                            ) : (
                              <span className="bench-note">—</span>
                            )}
                            <span className="bench-note"> of {c.all.length}</span>
                          </td>
                          {headline.map((m) => {
                            const v = r.metrics[m.id];
                            return (
                              <td key={m.id}>
                                {v ? (
                                  <>
                                    {fmt(v.value, v.unit)}
                                    <span className="bench-note"> {fmtReps(v)}</span>
                                  </>
                                ) : (
                                  <span className="bench-note">not measured</span>
                                )}
                              </td>
                            );
                          })}
                          <td className="bench-note">
                            {iso(r.ts_ms)} · {r.reps_counted} rep
                            {r.reps_counted === 1 ? '' : 's'}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            );
          })
        ) : (
          <p className="bench-note">
            No published measurement carries this system yet.
          </p>
        )}

        {attempts.length > 0 && (
          <>
            <h2>Runs that produced no number</h2>
            <ul>
              {attempts.map((a) => (
                <li key={`${a.variant_id}-${a.ts_ms}`}>
                  <code>{a.variant_id}</code> — {a.status.replace(/_/g, ' ')}
                  {a.reps_counted > 1 && ` (${a.reps_counted} repetitions)`}
                  {a.note && <span className="bench-note"> · {a.note}</span>}
                </li>
              ))}
            </ul>
          </>
        )}

        {e.constraints?.length ? (
          <>
            <h2>Configurations it refuses</h2>
            <ul>
              {e.constraints.map((c) => (
                <li key={`${c.knob}-${c.exceeds}`}>
                  <code>{c.knob}</code> must exceed <code>{c.exceeds}</code>.{' '}
                  <span className="bench-note">{c.why}</span>
                </li>
              ))}
            </ul>
          </>
        ) : null}

        {e.deviations?.length ? (
          <>
            <h2>Declared deviations</h2>
            <ul>
              {e.deviations.map((d, i) => (
                <li key={i}>
                  {d.what}
                  {d.why && <span className="bench-note"> — {d.why}</span>}
                  {d.affects?.length ? (
                    <span className="bench-note"> · affects {d.affects.join(', ')}</span>
                  ) : null}
                </li>
              ))}
            </ul>
          </>
        ) : null}

        {/* The invitation, on every system's page including the vendor's own,
            because an invitation extended only to competitors is a courtesy and
            an invitation extended to everyone is a rule. */}
        <h2>Tell us we got this wrong</h2>
        <p>
          If this system is configured badly here, that is a bug in this benchmark rather
          than a result about {e.entrant.name}, and the pull request that fixes it is the
          most valuable one this repository can receive.{' '}
          <a href={`${repo}/blob/main/CONTRIBUTING.md`}>How to send one</a>. The whole
          configuration is at{' '}
          <a href={`${repo}/tree/main/entrants/${id}`}>
            <code>entrants/{id}</code>
          </a>
          .
        </p>

        {(() => {
          const detail = CATALOGUE.filter((m) => m.placement !== 'default');
          if (!rows.length || !detail.length) return null;
          const measured = detail.filter((m) => rows.some((r) => r.metrics[m.id]));
          if (!measured.length) return null;
          return (
            <p className="bench-note">
              This system also reports {measured.map((m) => specOf(m.id).label).join(', ')}.
              Those figures are on each arm&rsquo;s own disclosure in{' '}
              <Link to={baseUrl}>the results table</Link>.
            </p>
          );
        })()}
      </main>
    </Layout>
  );
}
