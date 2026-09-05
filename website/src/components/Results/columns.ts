// Which metrics become columns, which become detail, and what a reader can
// filter by. Nothing else.
//
// This module imports nothing, for the same reason `Results/filter.ts` imports
// nothing: it has to be loadable directly by `node --test`, which cannot follow
// the extensionless imports the bundler resolves. The types below are therefore
// structural minimums rather than the full shapes in `Results/data.ts` — a
// component passes the real object and TypeScript checks it fits.
//
// NOTHING IN THIS FILE MAY BRANCH ON AN ENTRANT ID
//
// Placement is a property of a METRIC, and every facet is derived from a
// descriptor FIELD. `website/plugins/neutrality.test.js` enforces the absence of
// entrant ids here, which is what makes the neutrality claim checkable.

export type Placement = 'default' | 'available' | 'detail';

export type MetricSpec = {
  id: string;
  label: string;
  gloss: string;
  placement: Placement;
  /**
   * Header unit, where the metric's own unit string is not the whole truth.
   *
   * `rows_per_s_per_core` is emitted as `records/s` because that is what the
   * numerator is, and `unitLabel` renders that as `rows/s` — correct over
   * throughput and wrong over a column that has already divided by cores. The
   * override lives here rather than in the harness because the record is right
   * and only the header is at issue.
   */
  unitLabel?: string;
};

/**
 * The metric catalogue.
 *
 * The harness emits 31 distinct metric names today and will emit more. Making
 * every one of them a switchable column is not a kindness: it is a 31-item menu
 * over a table where five columns answer the question, and — because the browser
 * may only ever hide what the server already sent — it is also 31 columns of
 * markup shipped to every reader for every arm of every comparability group.
 *
 * So placement is declared, in three placements:
 *
 *   `default`    on when the page loads.
 *   `available`  prerendered as a column, off until a reader asks for it.
 *   `detail`     not a column at all; rendered in the arm's own disclosure.
 *
 * A metric NOT LISTED HERE is `detail`. That is the important property: a new
 * counter added to the harness appears in the per-arm detail on its own, without
 * this file changing, and is promoted to a column by adding one line. The
 * failure mode of forgetting is a metric that is present but quiet, never a
 * metric that vanishes.
 *
 * Order within the list is the order a reader meets the columns.
 */
const CATALOGUE_LIST: MetricSpec[] = [
  // ---- default -----------------------------------------------------------
  {
    id: 'rows_per_s_per_core',
    label: 'Throughput per core',
    gloss:
      'Rows landed per second per mean core occupied — the one-sentence goal of the fairness contract, read literally. Exactly 1e6 / CPU per row.',
    placement: 'default',
    unitLabel: 'rows/s per core',
  },
  {
    id: 'rows_per_s',
    label: 'Throughput',
    gloss: 'Rows landed in ClickHouse per second, from SELECT count() outside the system.',
    placement: 'default',
  },
  {
    id: 'cores_used',
    label: 'Cores used',
    gloss: 'Mean cores over the measured window, against a 6-core data-plane envelope.',
    placement: 'default',
  },
  {
    id: 'peak_anon_bytes',
    label: 'Peak memory',
    gloss:
      'Peak anonymous memory under a deliberately generous 96 GiB cap — what a system chooses to use, not a minimum footprint.',
    placement: 'default',
  },
  {
    id: 'duplicate_rows',
    label: 'Duplicate rows',
    gloss:
      'Exact duplicates of (batch_id, event_seq) within a bounded window of the corpus — a rate indicator, not a total.',
    placement: 'default',
  },

  // ---- available ---------------------------------------------------------
  {
    id: 'cpu_us_per_row',
    label: 'CPU per row',
    gloss:
      'cgroup v2 CPU microseconds per landed row. The same measurement as throughput per core, inverted — not a second reading of it.',
    placement: 'available',
  },
  {
    id: 'data_plane_cores_used',
    label: 'Cores, data plane',
    gloss:
      'Mean cores excluding any control plane. The stricter figure, for a reader who rejects the control-plane accounting.',
    placement: 'available',
  },
  {
    id: 'data_plane_peak_anon_bytes',
    label: 'Peak memory, data plane',
    gloss: 'Peak anonymous memory excluding any control plane.',
    placement: 'available',
  },
  {
    id: 'peak_charged_bytes',
    label: 'Peak charged memory',
    gloss: 'cgroup memory.peak — everything the kernel charged, page cache included.',
    placement: 'available',
  },
  {
    id: 'throttled_us',
    label: 'Throttled',
    gloss: 'Microseconds the CPU quota stopped the system from running.',
    placement: 'available',
  },
  {
    id: 'ch_cpu_us_per_row',
    label: 'ClickHouse CPU per row',
    gloss:
      'Server-side CPU per landed row — the work a wire format moved into the database rather than out of it.',
    placement: 'available',
  },
  {
    id: 'gc_pause_p99_us',
    label: 'GC pause p99',
    gloss:
      ' 99th-percentile stop-the-world pause. Absent on a runtime with no collector, and an absence is not a zero.',
    placement: 'available',
  },

  // ---- detail ------------------------------------------------------------
  {id: 'ch_cpu_us', label: 'ClickHouse CPU', gloss: 'Total server-side CPU over the window.', placement: 'detail'},
  {
    id: 'ch_cpu_us_per_written_row',
    label: 'ClickHouse CPU per written row',
    gloss: 'Server-side CPU per row the server itself wrote, before deduplication.',
    placement: 'detail',
  },
  {id: 'ch_cpu_wait_us', label: 'ClickHouse CPU wait', gloss: 'Server-side time spent waiting rather than running.', placement: 'detail'},
  {id: 'ch_inserted_bytes', label: 'Bytes inserted', gloss: 'Bytes the server received on the insert path.', placement: 'detail'},
  {id: 'ch_rows_per_insert', label: 'Rows per insert', gloss: 'Mean batch size as the server saw it.', placement: 'detail'},
  {id: 'ch_written_rows', label: 'Rows written server-side', gloss: 'Rows the server wrote, excluding background merges.', placement: 'detail'},
  {id: 'gc_pause_max_us', label: 'GC pause max', gloss: 'Longest stop-the-world pause in the window.', placement: 'detail'},
  {id: 'gc_pause_p999_us', label: 'GC pause p99.9', gloss: '99.9th-percentile stop-the-world pause.', placement: 'detail'},
  {id: 'gc_pause_total_us', label: 'GC pause total', gloss: 'Total stop-the-world time in the window.', placement: 'detail'},
  {id: 'jvm_heap_committed_peak_bytes', label: 'JVM heap committed peak', gloss: 'Peak heap the runtime committed from the OS.', placement: 'detail'},
  {id: 'jvm_heap_configured_bytes', label: 'JVM heap configured', gloss: 'Configured maximum heap.', placement: 'detail'},
  {id: 'jvm_heap_live_peak_bytes', label: 'JVM heap live peak', gloss: 'Peak live set after collection.', placement: 'detail'},
];

const BY_ID = new Map(CATALOGUE_LIST.map((m) => [m.id, m]));

export const CATALOGUE: readonly MetricSpec[] = CATALOGUE_LIST;

/** The metric the table is ordered by until a reader says otherwise. */
export const PRIMARY_COLUMN = CATALOGUE_LIST[0].id;

/**
 * A readable label for a metric the catalogue has never heard of.
 *
 * Deliberately mechanical. A guessed gloss would be the site asserting something
 * about a measurement it does not know, which is exactly the failure this whole
 * surface is arranged to avoid — so an unknown metric gets its name tidied and
 * no explanation at all.
 */
export function humaniseMetric(id: string): string {
  const words = id.replace(/_(us|bytes|ms|s)$/, '').split('_').filter(Boolean);
  if (!words.length) return id;
  return words.join(' ').replace(/^./, (c) => c.toUpperCase());
}

/** The catalogue entry for a metric, synthesising one for anything unlisted. */
export function specOf(id: string): MetricSpec {
  return BY_ID.get(id) ?? {id, label: humaniseMetric(id), gloss: '', placement: 'detail'};
}

export const placementOf = (id: string): Placement => specOf(id).placement;

/**
 * The switchable columns, in catalogue order, restricted to what was measured.
 *
 * A column no arm in the data carries would be a column of "not measured", which
 * teaches a reader that the page is padded rather than that a metric is missing.
 */
export function columnsFor(present: Iterable<string>): MetricSpec[] {
  const has = new Set(present);
  return CATALOGUE_LIST.filter((m) => m.placement !== 'detail' && has.has(m.id));
}

/** Column ids on when the page loads, restricted to what was measured. */
export function defaultColumnsFor(present: Iterable<string>): string[] {
  const has = new Set(present);
  return CATALOGUE_LIST.filter((m) => m.placement === 'default' && has.has(m.id)).map((m) => m.id);
}

/**
 * Metrics that belong in an arm's own disclosure rather than in the table.
 *
 * Catalogue-declared detail first, in declaration order; then anything the
 * catalogue does not know, alphabetically, so a new harness counter has a stable
 * place rather than appearing wherever the JSON happened to put it.
 */
export function detailFor(present: Iterable<string>): MetricSpec[] {
  const has = [...new Set(present)];
  const known = CATALOGUE_LIST.filter((m) => m.placement === 'detail' && has.includes(m.id));
  const unknown = has
    .filter((id) => !BY_ID.has(id))
    .sort()
    .map(specOf);
  return [...known, ...unknown];
}

// ---------------------------------------------------------------------------
// Facets
// ---------------------------------------------------------------------------

/** The descriptor fields a facet may be built from. */
export type FacetSource = {
  entrant: {
    id: string;
    runtime?: string;
    licence?: string;
    kind?: string;
  };
  guarantees?: {delivery?: string};
};

export type FacetOption = {value: string; count: number};
export type Facet = {id: string; label: string; options: FacetOption[]};

/**
 * The facets, and why they are these and not a list of system names.
 *
 * The obvious control is a checkbox per system. It works at six and collapses at
 * twenty: the reference implementation for this kind of page renders about a
 * hundred and fifty system chips and spends its entire first screen on them
 * before a reader reaches a number.
 *
 * These four are properties every descriptor already declares, and their
 * cardinality does not grow with the number of entrants — twenty systems still
 * have three or four runtimes between them. "Show me the systems that are not on
 * a garbage-collected runtime" stays one click at any size, which a name list
 * never does. Systems are reachable by name too, through a text filter.
 */
const FACET_FIELDS: {id: string; label: string; of: (e: FacetSource) => string | undefined}[] = [
  {id: 'runtime', label: 'Runtime', of: (e) => e.entrant.runtime},
  {id: 'kind', label: 'Kind', of: (e) => e.entrant.kind},
  {id: 'licence', label: 'Licence', of: (e) => e.entrant.licence},
  {id: 'delivery', label: 'Delivery', of: (e) => e.guarantees?.delivery},
];

/** The facet values one entrant carries, for the markup to hold as attributes. */
export function facetValuesOf(e: FacetSource): Record<string, string> {
  const out: Record<string, string> = {};
  for (const f of FACET_FIELDS) {
    const v = f.of(e);
    if (v) out[f.id] = v;
  }
  return out;
}

/**
 * The facets worth offering, given who is actually in the table.
 *
 * A facet whose values are all the same cannot change the page, and a control
 * that cannot change the page is furniture. So single-valued facets are dropped
 * — which means this page shows almost no facets today and grows them on its own
 * as entrants with different runtimes and licences arrive.
 */
export function facetsOf(entrants: FacetSource[]): Facet[] {
  const facets: Facet[] = [];
  for (const f of FACET_FIELDS) {
    const counts = new Map<string, number>();
    for (const e of entrants) {
      const v = f.of(e);
      if (v) counts.set(v, (counts.get(v) ?? 0) + 1);
    }
    if (counts.size < 2) continue;
    facets.push({
      id: f.id,
      label: f.label,
      options: [...counts.entries()]
        .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
        .map(([value, count]) => ({value, count})),
    });
  }
  return facets;
}

// ---------------------------------------------------------------------------
// What a reader may show
// ---------------------------------------------------------------------------

/** The class that decides whether an arm is shown: its declared approach. */
export const REALISTIC = 'realistic';

/**
 * Rule 3 of the fairness contract states that the site "defaults every chart to
 * realistic". That is the default below because the contract says so, not
 * because it flatters anything — and everything else is one click away and
 * never hidden from a reader who asks. `tuned` and `stripped` exist precisely to
 * quantify an effect, so a reader who wants the delta can have it.
 *
 * These are APPROACHES — what the descriptor declared the configuration to be —
 * and nothing else belongs here. `infra-bound` used to, and being an unticked
 * class by default is what hid a realistic arm from every reader with scripting
 * on. It is a verdict on a number rather than a kind of configuration, and it
 * reaches the reader through the chip, the void lane and the disclosure.
 * `model.ts`'s `showClassOf` carries the argument.
 */
const SHOW_ORDER = ['realistic', 'tuned', 'stripped'];

const SHOW_GLOSS: Record<string, string> = {
  realistic: 'headline-eligible',
  tuned: 'rule-1 compliant, but not a configuration a typical user would deploy',
  stripped: 'uses code the system does not ship, or drops a guarantee',
  undeclared: 'the descriptor did not say, which validation should have caught',
};

export type ShowClass = {id: string; gloss: string; on: boolean};

/**
 * The show-classes worth offering, derived from the rows actually present.
 *
 * Derived rather than hardcoded so that a class the contract gains later — or
 * one a descriptor produces by accident, like `undeclared` — reaches the control
 * instead of silently becoming unreachable. Known classes keep their documented
 * order; anything else follows, alphabetically.
 */
export function showClassesFor(present: Iterable<string>): ShowClass[] {
  const has = [...new Set(present)];
  const known = SHOW_ORDER.filter((id) => has.includes(id));
  const extra = has.filter((id) => !SHOW_ORDER.includes(id)).sort();
  return [...known, ...extra].map((id) => ({
    id,
    gloss: SHOW_GLOSS[id] ?? 'not a class this site has a description for',
    on: id === REALISTIC,
  }));
}
