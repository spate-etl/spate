/** The fields of a doc's metadata that decide its page ordinal. */
export type PageFields = {id: string; sidebarPosition?: number};

/**
 * The page's place in its sidebar category, or null when the page takes no
 * header carrier. Only a user-guide page whose file name carries an `NN-`
 * prefix has an authored place; the prefixed decision records are excluded.
 */
export function pageOrdinal({id, sidebarPosition}: PageFields): number | null {
  return id.startsWith('user-guide/') && sidebarPosition !== undefined ? sidebarPosition : null;
}

/** A heading as the doc's table of contents lists it. */
export type TocEntry = {id: string; level: number};

/**
 * The place of the H2 `id` among the page's H2s. Throws, failing the build,
 * when `id` names no H2, or names the first one on a page whose header already
 * carries an ordinal.
 */
export function sectionOrdinal(toc: readonly TocEntry[], id: string, pageHasCarrier: boolean): number {
  const sections = toc.filter((entry) => entry.level === 2);
  const index = sections.findIndex((entry) => entry.id === id);
  if (index === -1) {
    throw new Error(`SectionCarrier: no H2 with id "${id}" on this page`);
  }
  if (index === 0 && pageHasCarrier) {
    throw new Error(`SectionCarrier: "${id}" is the first section, and the page header already carries an ordinal`);
  }
  return index + 1;
}

/** The two-digit numeral a carrier shows, which also names its image. */
export function numeral(ordinal: number): string {
  return String(ordinal).padStart(2, '0');
}
