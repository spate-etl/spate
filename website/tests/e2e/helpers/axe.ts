import AxeBuilder from '@axe-core/playwright';
import type {ElementHandle, Page, TestInfo} from '@playwright/test';
import {expect} from '@playwright/test';

/**
 * The WCAG success criteria the sweep enforces. `best-practice` is excluded
 * on purpose: it is axe-core's own heuristic tag, not a WCAG level, so a
 * future axe-core minor can add a rule to it and redden this gate with
 * nothing changed here. `@axe-core/playwright`'s `~4.13.0` dependency floats
 * patches only, which already guards the WCAG tags against the same thing.
 */
export const WCAG_TAGS = ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'];

type AxeResults = Awaited<ReturnType<AxeBuilder['analyze']>>;
type AxeTarget = AxeResults['violations'][number]['nodes'][number]['target'];

/** One line per axe violation: rule id, impact, help text, help URL and target selectors. */
export function formatViolations(violations: AxeResults['violations']): string {
  return violations
    .map((violation) => {
      const targets = violation.nodes.map((node) => node.target.join(' ')).join(', ');
      return `${violation.id} (${violation.impact}): ${violation.help}\n  ${violation.helpUrl}\n  targets: ${targets}`;
    })
    .join('\n\n');
}

/**
 * A known violation to exclude from the assertion: a rule id, and a CSS
 * selector that stays pinned to the offending element while the rest of the
 * page changes around it (a heading id, an authored class name). axe's own
 * `node.target` is a selector it regenerates from the DOM it just scanned,
 * not a stable identifier — an unrelated edit anywhere earlier in the
 * document can renumber an `:nth-child()` axe writes into `node.target`, so
 * matching against that string is not matching against the element the entry
 * was written for.
 */
export type KnownViolation = {
  ruleId: string;
  selector: string;
};

/**
 * The selector addressing a violation node in this document, or `null` for
 * one inside an iframe. axe gives one entry per frame level, and an entry is
 * itself an array of selectors inside a shadow root, which Playwright's CSS
 * engine pierces.
 */
function targetSelector(target: AxeTarget): string | null {
  if (target.length !== 1) return null;
  const [entry] = target;
  return typeof entry === 'string' ? entry : entry.join(' ');
}

/** Whether `a` and `b` are the same DOM node, checked in-page rather than by comparing selector strings. */
async function sameElement(page: Page, a: ElementHandle, b: ElementHandle): Promise<boolean> {
  return page.evaluate(([x, y]) => x === y, [a, b] as const);
}

/**
 * Runs axe-core against the current page restricted to `WCAG_TAGS`, attaches
 * the full JSON results to the test report, and fails with a readable
 * violation list. `knownViolations` excludes a violation node only when its
 * live DOM element is the same one `selector` resolves to right now, so a
 * tracked defect on one element does not stop the rule checking the rest of
 * the page. Every entry must carry a comment naming the tracking issue at
 * the call site, and must match at least one violation node, or the
 * assertion fails: a selector that stops resolving to the offending element
 * (the defect was fixed, or the page changed under it) is a signal, not
 * something to pass silently.
 */
export async function expectNoAxeViolations(
  page: Page,
  testInfo: TestInfo,
  options?: {knownViolations?: KnownViolation[]},
): Promise<void> {
  const results = await new AxeBuilder({page}).withTags(WCAG_TAGS).analyze();

  await testInfo.attach('axe-results', {
    body: JSON.stringify(results, null, 2),
    contentType: 'application/json',
  });

  const known = options?.knownViolations ?? [];
  const knownHandles = await Promise.all(known.map((k) => page.$(k.selector)));
  const matched = known.map(() => false);

  const violations = [];
  for (const violation of results.violations) {
    const remainingNodes = [];
    for (const node of violation.nodes) {
      const selector = targetSelector(node.target);
      const targetHandle = selector === null ? null : await page.$(selector);
      let isKnown = false;
      if (targetHandle) {
        for (let i = 0; i < known.length; i++) {
          const knownHandle = knownHandles[i];
          if (known[i].ruleId !== violation.id || !knownHandle) continue;
          if (await sameElement(page, targetHandle, knownHandle)) {
            isKnown = true;
            matched[i] = true;
          }
        }
      }
      if (!isKnown) remainingNodes.push(node);
    }
    if (remainingNodes.length > 0) violations.push({...violation, nodes: remainingNodes});
  }

  known.forEach((k, i) => {
    expect(matched[i], `knownViolations entry did not match a violation: ${k.ruleId} ${k.selector}`).toBe(true);
  });

  expect(violations, formatViolations(violations)).toEqual([]);
}
