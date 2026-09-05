import React from 'react';

import {type MetricSpec} from './columns';
import {type Facet, type ShowClass} from './columns';

/**
 * The controls, sized for twenty vendors rather than for six.
 *
 * WHY THERE IS NO CHECKBOX PER SYSTEM
 *
 * A checkbox per system is right at six and unusable well before twenty. The
 * reference implementation for this kind of page — ClickBench — takes that
 * design to about a hundred and fifty systems and spends its entire first screen
 * on filter chips before a reader reaches a number. That is the failure being
 * designed against here.
 *
 * So systems are reachable two ways, neither of which grows with the roster: a
 * text filter for "I know what I am looking for", and facets built from
 * descriptor fields — runtime, kind, licence, delivery guarantee — for "show me
 * the ones that are not on a garbage-collected runtime". Twenty systems still
 * have three or four runtimes between them, so those chips stay a handful at any
 * size. Facets with a single value are dropped by `facetsOf`, because a control
 * that cannot change the page is furniture.
 *
 * WHAT THE CONTROLS MAY DO
 *
 * The rule the whole surface is arranged around: every number ships in the HTML, and the
 * browser only ever hides, reorders and rescales what is already there. The
 * column picker is a visibility switch over columns the server already rendered,
 * not a request for more data. With scripting off these controls are inert and
 * every figure is still on the page.
 *
 * There is deliberately no control for choosing a comparability group. The
 * group disclosures do that natively — see `index.tsx`.
 */
export default function Controls({
  facets,
  columns,
  showClasses,
  systemCount,
}: {
  facets: Facet[];
  columns: MetricSpec[];
  showClasses: ShowClass[];
  systemCount: number;
}): React.JSX.Element | null {
  const canFilter = systemCount > 1 || facets.length > 0 || showClasses.length > 1;
  if (!canFilter && columns.length < 2) return null;

  return (
    <form
      className="bench-controls"
      id="bench-controls"
      aria-label="Filter, sort and choose columns"
      onSubmit={(e) => e.preventDefault()}
    >
      <noscript>
        <p className="bench-note bench-controls__noscript">
          These controls need JavaScript. Every group, every arm and every figure is on
          this page regardless — filtering only ever hides rows that are already here, and
          every measurement context can be opened without scripting.
        </p>
      </noscript>

      {systemCount > 1 && (
        <fieldset className="bench-controls__g">
          <legend>Find a system</legend>
          <input
            type="search"
            className="bench-search"
            placeholder="name or arm…"
            aria-label="Filter rows by system or arm name"
            data-bench-name
          />
        </fieldset>
      )}

      {facets.map((f) => (
        <fieldset key={f.id} className="bench-controls__g">
          <legend>{f.label}</legend>
          <div className="bench-checks">
            {f.options.map((o) => (
              <label key={o.value} className="bench-check">
                <input
                  type="checkbox"
                  defaultChecked
                  data-bench-facet={f.id}
                  data-bench-facet-value={o.value}
                />
                <span>
                  {o.value} <span className="bench-note">{o.count}</span>
                </span>
              </label>
            ))}
          </div>
        </fieldset>
      ))}

      {showClasses.length > 1 && (
        <fieldset className="bench-controls__g">
          <legend>Show</legend>
          <div className="bench-checks">
            {showClasses.map((s) => (
              <label key={s.id} className="bench-check" title={s.gloss}>
                <input type="checkbox" defaultChecked={s.on} data-bench-show={s.id} />
                <span>{s.id}</span>
              </label>
            ))}
          </div>
          {/* Rule 3 sets this default, not a preference. */}
          <p className="bench-note bench-controls__hint">
            Only <code>realistic</code> arms are ranked. The rest are shown when asked for,
            labelled, and never given a position they have not earned. An arm whose number
            was disowned as infra-bound is not filtered here at all — it stays on the page,
            with its reason, and loses only its position.
          </p>
        </fieldset>
      )}

      {columns.length > 1 && (
        <fieldset className="bench-controls__g bench-controls__g--cols">
          <legend>Columns</legend>
          <div className="bench-checks">
            {columns.map((c) => (
              <label key={c.id} className="bench-check" title={c.gloss}>
                <input
                  type="checkbox"
                  defaultChecked={c.placement === 'default'}
                  data-bench-col={c.id}
                />
                <span>{c.label}</span>
              </label>
            ))}
          </div>
        </fieldset>
      )}

      <p className="bench-note bench-controls__rescale">
        Each column&rsquo;s axis rescales to what is visible, so hiding a much faster arm
        does not leave the rest crushed against zero — which matters more the wider the
        field gets. The end value printed on every axis is what it currently means.
      </p>
    </form>
  );
}
