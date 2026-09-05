import Link from '@docusaurus/Link';
import React from 'react';

import {
  isPlotted,
  isRanked,
  iso,
  unrankedBecause,
  armLabel,
  type Metric,
  type Row as BenchRow,
  variantOf,
  type Entrant,
} from './data';
import {specOf, type MetricSpec} from './columns';
import {displayLabel, fmtReps, fmt} from './format';
import {
  armAnchor,
  descriptorIndex,
  searchTextOf,
  showClassOf,
} from './model';

/**
 * One arm, as two table rows: the measurement, and its disclosure.
 *
 * THE MARK SURVIVES THE DENSITY
 *
 * The mark is the interval — a capsule from the smallest repetition to the
 * largest, notched at the median — and that is not decoration to be dropped when
 * the table gets long. Run-to-run spread is routinely wider than the differences
 * on this page, so a bare median invites a reader to believe a ranking the data
 * does not support. The capsule shrinks
 * from 18px to 10px and stays.
 *
 * WHY THE DISCLOSURE IS A `:target` ROW
 *
 * Rule 6 requires the binding-constraint sentence in the published results
 * table, and an arm's knobs, flags and provenance belong beside its number
 * rather than a page away. That needs a full-width region inside a `<table>`,
 * which `<details>` cannot provide from within a cell.
 *
 * So each arm gets a second `<tr>`, hidden by CSS and revealed by `:target`.
 * That is a native mechanism: it needs no JavaScript, it survives with scripting
 * off, find-in-page still reaches it, and it makes every arm's detail a real URL
 * that can be linked to in an argument about a number.
 *
 * MARK GEOMETRY LIVES IN CSS, NOT IN `style`
 *
 * Measured, not assumed: the same page with mark geometry inline rather than in
 * a class is 6.04 MB instead of 2.61 MB. The only things inline on a mark are
 * `--v`, `--lo` and `--hi` — the measurements themselves, which have to be in
 * the markup so a reader can check the position against the printed figure.
 */

export type Column = MetricSpec & {
  unit: string;
  higherIsBetter: boolean;
  scaleMax: number | null;
  tied: Set<string>;
  on: boolean;
};

const at = (v: number) => ({'--v': v}) as React.CSSProperties;

function Mark({m, emphasis}: {m: Metric; emphasis: boolean}) {
  return (
    <span className="bench-trk" aria-hidden="true">
      {/* No minimum width. A floor would draw a spread on an arm whose
          repetitions agreed, anchored at `lo` and extending right, which reads
          as "the range is above the median". Arms that agreed get a hairline. */}
      <span
        className={emphasis ? 'bench-rg' : 'bench-rg bench-rg--ctx'}
        style={{'--lo': m.lo, '--hi': Math.max(m.lo, m.hi)} as React.CSSProperties}
      />
      <span
        className={emphasis ? 'bench-md' : 'bench-md bench-md--ctx'}
        style={at(m.value)}
      />
    </span>
  );
}

function Cell({row, col}: {row: BenchRow; col: Column}) {
  const m = row.metrics[col.id];
  if (!m) {
    // Never a zero. `methodology/measurement.md` is explicit that a missing GC
    // figure is not a zero — "a chart drawing a missing pause total as a
    // zero-length bar would be asserting a measurement nobody made".
    return (
      <td className="bench-c bench-c--none" data-m={col.id} hidden={!col.on}>
        <span className="bench-note">not measured</span>
      </td>
    );
  }
  const plotted = isPlotted(row);
  return (
    <td
      className="bench-c"
      data-m={col.id}
      data-v={m.value}
      data-hi={plotted ? Math.max(m.lo, m.hi) : undefined}
      data-plotted={plotted ? '1' : '0'}
      hidden={!col.on}
    >
      <span className="bench-n">
        {fmt(m.value, m.unit)}
        {col.tied.has(row.key) && (
          <span
            className="bench-chip bench-chip--tie"
            title="This arm's measured interval overlaps the leading arm's. The repetitions do not separate them."
          >
            ties
          </span>
        )}
      </span>
      {col.scaleMax != null &&
        (plotted ? (
          <Mark m={m} emphasis={isRanked(row)} />
        ) : (
          // A position on a shared axis is itself a claim about the comparison.
          // A number this project has disowned does not get one, and the empty
          // lane says so rather than silently closing the row up.
          <span className="bench-trk bench-trk--void" aria-hidden="true">
            <span className="bench-void" />
          </span>
        ))}
      <span className="bench-sub">
        {fmtReps(m)}
      </span>
    </td>
  );
}

export default function Row({
  row,
  entrant,
  entrants,
  columns,
  place,
  colSpan,
  detail,
  baseUrl,
}: {
  row: BenchRow;
  entrant: Entrant | undefined;
  entrants: Entrant[];
  columns: Column[];
  place: number | undefined;
  colSpan: number;
  detail: MetricSpec[];
  baseUrl: string;
}): React.JSX.Element {
  const v = variantOf(entrant, row.variant_id);
  const label = displayLabel(armLabel(entrant, v, row.variant_id), row);
  const name = entrant?.entrant.name ?? row.entrant;
  // Read straight off the descriptor. Nothing here knows which system this is,
  // which is what makes the disclosure a property of the data rather than a
  // courtesy the site extends.
  const ours = entrant?.entrant.vendor === 'self';
  const why = unrankedBecause(row);
  const anchor = armAnchor(row.key);
  const knobs = Object.entries(v?.knobs ?? {});

  return (
    <>
      <tr
        className={isRanked(row) ? 'bench-arm' : 'bench-arm is-context'}
        data-arm=""
        data-system={row.entrant}
        data-show={showClassOf(row)}
        data-ranked={isRanked(row) ? '1' : '0'}
        data-name={searchTextOf(name, label, row.variant_id)}
        data-index={descriptorIndex(entrants, row.entrant, row.variant_id)}
        data-runtime={entrant?.entrant.runtime}
        data-kind={entrant?.entrant.kind}
        data-licence={entrant?.entrant.licence}
        data-delivery={entrant?.guarantees?.delivery}
      >
        <td className="bench-rank">{place ?? <span aria-hidden="true">—</span>}</td>
        <th scope="row" className="bench-arm__h">
          {/* Three lines, three kinds of thing: who, which configuration, and
              where the number came from. They were previously one run-on line
              with the identity, a rule-5 fact and a control all rendered as
              matching pills, which made none of them legible as what it is. */}
          <span className="bench-arm__who">
            <Link className="bench-sys" to={`${baseUrl}systems/${row.entrant}`}>
              {name}
            </Link>
            {/* The conflict-of-interest marker, from `vendor = "self"` on the
                descriptor — nothing here branches on an id. A glyph rather than
                a badge because it repeats on every row of that system, and the
                sighted shorthand is expanded in full for assistive technology
                rather than left to a `title` nobody can focus. */}
            {ours && (
              <span className="bench-vendor" title="Run by the vendor of this benchmark">
                <span aria-hidden="true">†</span>
                <span className="bench-sr-only"> — run by the vendor of this benchmark</span>
              </span>
            )}
          </span>

          <span className="bench-arm__label">{label}</span>

          {(why || (row.approach !== 'realistic' && row.approach !== why)) && (
            <span className="bench-arm__badges">
              {why && <span className="bench-chip bench-chip--muted">{why}</span>}
              {/* A `stripped` arm does not stop being stripped because it was also
                  infra-bound, and that fact is rule 1 rather than bookkeeping. */}
              {row.approach !== 'realistic' && row.approach !== why && (
                <span className="bench-chip bench-chip--muted">{row.approach}</span>
              )}
            </span>
          )}

          {/* Rule 5's wire format sits here as plain text rather than as a chip.
              Native, RowBinary and JSONEachRow are not the same server-side work,
              so the format has to travel with the number — but it is a FACT about
              the measurement, exactly like the version and the date beside it,
              and giving it the same shape as a control is what made the control
              unreadable. */}
          {/* Two things are deliberately NOT here.
              The variant id, because the label and the wire format beside it
              already state everything the id says. It is in the arm's
              disclosure, and it is still searchable from the name filter.
              The repetition count, because every metric cell prints it in its
              own sub-line, and repeating it wrapped this line and left rows at
              uneven heights. */}
          <span className="bench-arm__meta">
            {row.wire_format ?? 'format not declared'} ·{' '}
            {row.version ?? row.commit ?? 'version unknown'} · {iso(row.ts_ms)}
          </span>
        </th>
        {columns.map((c) => (
          <Cell key={c.id} row={row} col={c} />
        ))}
        {/* The one control in the row, in a column of its own so that nothing
            else can be mistaken for it. Server-rendered as an anchor, which is
            what makes it work with scripting off — `:target` reveals the
            disclosure — and upgraded by the enhancer into a real expander that
            reports its state and leaves the URL alone. */}
        <td className="bench-disclose__cell">
          <a className="bench-disclose" href={`#${anchor}`} data-disclose={anchor}>
            <svg width="9" height="9" viewBox="0 0 9 9" aria-hidden="true" focusable="false">
              <path
                d="M2.5 1 L6.5 4.5 L2.5 8"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.6"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            <span className="bench-sr-only">
              Detail for {name} {label}
            </span>
          </a>
        </td>
      </tr>

      <tr className="bench-detail" id={anchor}>
        <td colSpan={colSpan}>
          <div className="bench-detail__box">
            <div className="bench-detail__head">
              <strong>
                {name} · {label}
              </strong>
              <a className="bench-detail__close" href="#">
                close
              </a>
            </div>

            <dl className="bench-dl">
              {/* Rule 6 of the fairness contract: one sentence per configuration
                  saying why throughput was what it was and not twice that, and
                  the contract says it "goes in the published results table". No
                  descriptor carries the field yet, so the absence is rendered
                  rather than hidden — see `Variant.binding_constraint`. */}
              <dt>Why this number and not twice it</dt>
              <dd>
                {v?.binding_constraint ?? (
                  <span className="bench-note">
                    Binding constraint not yet stated. Rule 6 of{' '}
                    <Link to={`${baseUrl}contract/rules`}>the fairness contract</Link> requires
                    this sentence and no descriptor currently carries it.
                  </span>
                )}
              </dd>

              <dt>Configuration</dt>
              <dd>
                <code>{row.variant_id}</code>
                {knobs.length > 0 && (
                  <>
                    {' · '}
                    {knobs.map(([k, val], i) => (
                      <React.Fragment key={k}>
                        {i > 0 && ' · '}
                        {k} {String(val)}
                      </React.Fragment>
                    ))}
                  </>
                )}
                {v?.unshipped?.length ? (
                  <div className="bench-note">
                    not shipped by the system: {v.unshipped.join(', ')}
                  </div>
                ) : null}
              </dd>

              <dt>Provenance</dt>
              <dd className="bench-prov">
                {row.version ?? row.commit ?? 'version unknown'} · {iso(row.ts_ms)} ·{' '}
                {row.reps_counted} rep{row.reps_counted === 1 ? '' : 's'} · harness v
                {row.harness_version} · corpus {row.dataset_version}
                {row.image_digest && (
                  <>
                    {' · '}
                    <code>{row.image_digest}</code>
                  </>
                )}
              </dd>

              {/* The reason, which the legend promises a disowned number keeps.
                  Before this the plugin dropped the record's note entirely, so
                  an infra-bound arm's whole account of itself was a two-word
                  chip — the measured share it blew, and which ceiling it blew,
                  reached nobody. Printed verbatim: it is the harness's sentence
                  about its own reading, and picking clauses out of it here would
                  be the site re-deriving a published figure by another route. */}
              {row.note && (
                <>
                  <dt>{why === 'infra-bound' ? 'Why this number is disowned' : 'This reading'}</dt>
                  <dd className="bench-note">{row.note}</dd>
                </>
              )}

              {row.flags.length > 0 && (
                <>
                  <dt>Flags</dt>
                  <dd>{row.flags.map((f) => f.replace(/_/g, ' ')).join(' · ')}</dd>
                </>
              )}

              {detail.length > 0 && (
                <>
                  <dt>Other measurements</dt>
                  <dd>
                    <ul className="bench-detail__metrics">
                      {detail.map((d) => {
                        const m = row.metrics[d.id];
                        return (
                          <li key={d.id}>
                            <span className="bench-note">{specOf(d.id).label}</span>{' '}
                            {m ? fmt(m.value, m.unit) : (
                              <span className="bench-note">not measured</span>
                            )}
                          </li>
                        );
                      })}
                    </ul>
                  </dd>
                </>
              )}
            </dl>

            <p className="bench-note">
              <Link to={`${baseUrl}systems/${row.entrant}`}>
                Full profile for {name}
              </Link>{' '}
              — every arm, its configuration, and how to tell us we got it wrong.
            </p>
          </div>
        </td>
      </tr>
    </>
  );
}
