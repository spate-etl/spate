/**
 * The client-side half of the results explorer.
 *
 * Everything here operates on markup the server already produced, and it reads
 * every number it needs off `data-` attributes rather than recomputing any of
 * them. A figure on this page can only ever have come from the build.
 *
 * Four operations, in this order:
 *
 *   1. **Hide rows.** An arm is two `<tr>`s — the measurement and its `:target`
 *      disclosure — so both move and hide together, or a filtered-out arm leaves
 *      its detail behind.
 *   2. **Hide columns.** The picker is a visibility switch over columns the
 *      server already rendered. It never asks for data.
 *   3. **Rescale.** Every mark positions against `--scale-max` on its column;
 *      recomputing it over the visible rows moves the whole column at once, with
 *      no re-render and no second copy of the scale logic.
 *   4. **Sort.** By the metric whose header was clicked, using the direction the
 *      measurement itself declares.
 *   5. **Restate the footer's unranked count**, which the server computed over
 *      the whole group and step 1 may have just invalidated.
 *
 * WHAT IT DELIBERATELY DOES NOT TOUCH
 *
 * Group disclosure. `<details name>` gives mutually-exclusive groups natively,
 * so there is nothing to script — and consequently no way for the scripted page
 * and the unscripted page to show different things.
 *
 * TWO THINGS THAT ARE EASY TO GET WRONG
 *
 * Axis tick labels are rewritten UNCONDITIONALLY. Guarding the rewrite on
 * `scale > 1` looks harmless and leaves stale printed end values on any column
 * whose visible maximum rounds below 1 — reachable today for CPU-per-row, which
 * runs about 0.19–1.1 µs.
 *
 * The sort tie-break reads `data-index`, which the build writes from the
 * descriptor's own order. Passing the arm's index in the CURRENT DOM order
 * instead would make ties resolve against whatever the last sort produced, so
 * the "stable" tie-break would not be stable across control changes.
 */

import {compareLanes, visibleMax, type Sortable} from './filter';
import {fmt, unrankedNote} from './format';

const q = <T extends Element>(root: ParentNode, sel: string) =>
  Array.from(root.querySelectorAll<T>(sel));

function num(el: Element | null, attr: string): number | null {
  const raw = el?.getAttribute(attr);
  if (raw == null) return null;
  const v = Number(raw);
  return Number.isFinite(v) ? v : null;
}

/** An arm: its measurement row, and the disclosure row that belongs to it. */
type Arm = {row: HTMLTableRowElement; detail: HTMLTableRowElement | null};

function armsOf(body: HTMLTableSectionElement): Arm[] {
  return q<HTMLTableRowElement>(body, 'tr[data-arm]').map((row) => {
    const next = row.nextElementSibling;
    return {
      row,
      detail:
        next instanceof HTMLTableRowElement && next.classList.contains('bench-detail')
          ? next
          : null,
    };
  });
}

type State = {
  name: string;
  facets: Map<string, Set<string>>;
  show: Set<string>;
  columns: Set<string>;
  sort: string;
};

function readState(form: HTMLFormElement, sort: string): State {
  const facets = new Map<string, Set<string>>();
  for (const input of q<HTMLInputElement>(form, 'input[data-bench-facet]')) {
    const id = input.dataset.benchFacet ?? '';
    if (!facets.has(id)) facets.set(id, new Set());
    if (input.checked) facets.get(id)!.add(input.dataset.benchFacetValue ?? '');
  }
  // An entirely unticked facet would empty the page and read as a broken site
  // rather than as a choice, so it is treated as "no opinion" instead.
  for (const [id, chosen] of facets) {
    if (chosen.size) continue;
    for (const input of q<HTMLInputElement>(form, `input[data-bench-facet="${id}"]`)) {
      chosen.add(input.dataset.benchFacetValue ?? '');
    }
  }
  return {
    name: (form.querySelector<HTMLInputElement>('input[data-bench-name]')?.value ?? '')
      .trim()
      .toLowerCase(),
    facets,
    show: new Set(
      q<HTMLInputElement>(form, 'input[data-bench-show]')
        .filter((i) => i.checked)
        .map((i) => i.dataset.benchShow ?? ''),
    ),
    columns: new Set(
      q<HTMLInputElement>(form, 'input[data-bench-col]')
        .filter((i) => i.checked)
        .map((i) => i.dataset.benchCol ?? ''),
    ),
    sort,
  };
}

function rowVisible(row: HTMLTableRowElement, s: State): boolean {
  if (s.show.size && !s.show.has(row.dataset.show ?? '')) return false;
  if (s.name && !(row.dataset.name ?? '').includes(s.name)) return false;
  for (const [id, chosen] of s.facets) {
    const v = row.dataset[id as keyof DOMStringMap] as string | undefined;
    // An arm whose descriptor does not declare the field is never hidden by a
    // facet built from it: a missing value is not a value a reader deselected.
    if (v && !chosen.has(v)) return false;
  }
  return true;
}

function sortBody(body: HTMLTableSectionElement, arms: Arm[], metric: string, hib: boolean) {
  const key = (a: Arm, i: number): Sortable => ({
    value: num(a.row.querySelector(`td[data-m="${metric}"]`), 'data-v'),
    ranked: a.row.dataset.ranked === '1',
    index: num(a.row, 'data-index') ?? i,
  });
  const visible = arms.filter((a) => !a.row.hidden);
  const hidden = arms.filter((a) => a.row.hidden);
  const ordered = visible
    .map((a, i) => ({a, k: key(a, i)}))
    .sort((x, y) => compareLanes(x.k, y.k, hib));
  for (const {a} of ordered) {
    body.appendChild(a.row);
    if (a.detail) body.appendChild(a.detail);
  }
  // Hidden arms go to the end, so unhiding one does not drop it mid-order.
  for (const a of hidden) {
    body.appendChild(a.row);
    if (a.detail) body.appendChild(a.detail);
  }
}

function applyToBlock(block: HTMLElement, s: State) {
  const body = block.querySelector('tbody');
  if (!body) return;
  const arms = armsOf(body);

  for (const a of arms) {
    const visible = rowVisible(a.row, s);
    a.row.hidden = !visible;
    if (a.detail) a.detail.hidden = !visible;
  }

  // Unconditionally, for the reason the axis ticks are: the server counted every
  // unranked arm in the group, and the loop above may have just hidden some of
  // them. A footer claiming arms the page is concurrently hiding is a printed
  // number describing a page the reader is not looking at.
  const foot = block.querySelector<HTMLElement>('[data-bench-unranked]');
  if (foot) {
    foot.textContent = unrankedNote(
      arms.filter((a) => !a.row.hidden && a.row.dataset.ranked === '0').length,
    );
  }

  const heads = q<HTMLTableCellElement>(block, 'th[data-m]');
  for (const head of heads) {
    const metric = head.dataset.m ?? '';
    const on = s.columns.has(metric);
    head.hidden = !on;

    const cells = q<HTMLElement>(block, `td[data-m="${metric}"]`);
    for (const cell of cells) cell.hidden = !on;
    if (!on) continue;

    const scale = visibleMax(
      cells.map((cell) => ({
        hi: num(cell, 'data-hi') ?? 0,
        plotted: cell.dataset.plotted === '1',
        visible: !(cell.closest('tr') as HTMLTableRowElement | null)?.hidden,
      })),
    );
    head.style.setProperty('--scale-max', String(scale));
    for (const cell of cells) cell.style.setProperty('--scale-max', String(scale));

    // Unconditionally. An axis whose printed end value is stale is worse than no
    // axis, and a `scale > 1` guard leaves exactly that on sub-unit columns.
    const unit = head.dataset.unit ?? '';
    const ticks = q<HTMLElement>(head, '.bench-axis__t');
    if (ticks.length) ticks[ticks.length - 1].textContent = fmt(scale, unit);
  }

  if (s.sort) {
    const head = block.querySelector<HTMLElement>(`th[data-m="${s.sort}"]`);
    if (head && !head.hidden) sortBody(body, arms, s.sort, head.dataset.hib !== '0');
    for (const h of heads) {
      h.dataset.sorted = h.dataset.m === s.sort && !h.hidden ? '1' : '0';
    }
  }
}

/**
 * Turns each arm's disclosure link into an expander that reports its state.
 *
 * The control ships as an `<a href="#arm-…">` so that a reader without scripting
 * still gets the disclosure — `:target` reveals it — and so that an arm's detail
 * stays a real URL somebody can paste into an argument about a number. What it
 * does NOT ship with is any way to know it is a disclosure: a screen reader
 * announces "link", and `:target` allows exactly one open row and pushes a
 * history entry every time.
 *
 * So the enhancer adds what only script can add. `aria-expanded` on a link is
 * valid ARIA and is announced as collapsed/expanded, and toggling a class
 * instead of the fragment means several arms can be open at once, the back
 * button still does what a reader expects, and the URL stops churning.
 *
 * Deep links keep working: an incoming `#arm-…` is adopted on load, so the row
 * opens and its control agrees that it is open.
 */
function bindDisclosures(root: HTMLElement): () => void {
  const controls = q<HTMLAnchorElement>(root, 'a[data-disclose]');
  const rowFor = (a: HTMLAnchorElement) =>
    document.getElementById(a.dataset.disclose ?? '');

  const setOpen = (a: HTMLAnchorElement, open: boolean) => {
    a.setAttribute('aria-expanded', String(open));
    rowFor(a)?.classList.toggle('is-open', open);
  };

  for (const a of controls) {
    a.setAttribute('role', 'button');
    a.setAttribute('aria-controls', a.dataset.disclose ?? '');
    a.setAttribute('aria-expanded', 'false');
  }

  /**
   * Adopt a deep-linked arm, so the control agrees with what the page shows.
   *
   * Asked of `:target` rather than of `location.hash`, which is the same
   * question but not the same answer: this effect runs during hydration and
   * Docusaurus's router settles the URL afterwards, so comparing the hash here
   * reads empty and leaves an open row under a control claiming it is closed.
   * `:target` is the exact condition the stylesheet used to open the row, so
   * matching on it cannot disagree with what a reader is looking at.
   */
  const syncFromTarget = () => {
    for (const a of controls) {
      if (rowFor(a)?.matches(':target')) setOpen(a, true);
    }
  };
  syncFromTarget();
  // Once more after the router has settled, for the load-with-a-fragment case.
  const raf = requestAnimationFrame(syncFromTarget);

  const onClick = (e: Event) => {
    const a = (e.target as Element | null)?.closest<HTMLAnchorElement>('a[data-disclose]');
    if (!a || !root.contains(a)) return;
    e.preventDefault();
    setOpen(a, a.getAttribute('aria-expanded') !== 'true');
  };

  // Space activates a button but scrolls a link, so the role we just claimed has
  // to be honoured by the keyboard too. Enter already works as a link.
  const onKey = (e: KeyboardEvent) => {
    if (e.key !== ' ' && e.key !== 'Spacebar') return;
    const a = (e.target as Element | null)?.closest<HTMLAnchorElement>('a[data-disclose]');
    if (!a || !root.contains(a)) return;
    e.preventDefault();
    setOpen(a, a.getAttribute('aria-expanded') !== 'true');
  };

  root.addEventListener('click', onClick);
  root.addEventListener('keydown', onKey);
  window.addEventListener('hashchange', syncFromTarget);
  return () => {
    cancelAnimationFrame(raf);
    root.removeEventListener('click', onClick);
    root.removeEventListener('keydown', onKey);
    window.removeEventListener('hashchange', syncFromTarget);
    for (const a of controls) {
      a.removeAttribute('aria-expanded');
      a.removeAttribute('aria-controls');
      a.removeAttribute('role');
      rowFor(a)?.classList.remove('is-open');
    }
  };
}

export function enhance(): () => void {
  const root = document.querySelector<HTMLElement>('.bench-root');
  const form = document.getElementById('bench-controls') as HTMLFormElement | null;
  if (!root) return () => {};

  // The server's order is by the primary metric, so that is what the page is
  // already sorted by before anyone clicks anything.
  let sort =
    root.querySelector<HTMLElement>('th[data-m]')?.dataset.m ?? '';

  const apply = () => {
    if (!form) return;
    const s = readState(form, sort);
    for (const block of q<HTMLElement>(root, '.bench-block')) applyToBlock(block, s);
  };

  const onSort = (e: Event) => {
    const button = (e.target as Element | null)?.closest<HTMLElement>('.bench-sort');
    if (!button) return;
    e.preventDefault();
    sort = button.dataset.sort ?? sort;
    apply();
  };

  const releaseDisclosures = bindDisclosures(root);

  form?.addEventListener('change', apply);
  form?.addEventListener('input', apply);
  root.addEventListener('click', onSort);
  // Only now do the sort controls become real. Shipping them enabled would offer
  // a reader without scripting a button that does nothing.
  root.classList.add('is-enhanced');
  apply();

  return () => {
    form?.removeEventListener('change', apply);
    form?.removeEventListener('input', apply);
    root.removeEventListener('click', onSort);
    releaseDisclosures();
    root.classList.remove('is-enhanced');
  };
}
