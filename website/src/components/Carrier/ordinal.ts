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

/** The two-digit numeral a carrier shows, which also names its image. */
export function numeral(ordinal: number): string {
  return String(ordinal).padStart(2, '0');
}
