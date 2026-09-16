import React, {useEffect, useRef, useState} from 'react';

import styles from './styles.module.css';

const HEADING_SELECTOR = 'h1, h2, h3, h4, h5, h6';

/** The nearest heading before `element` in document order, walking sibling by sibling rather than assuming adjacency. */
function precedingHeading(element: Element): Element | null {
  let node = element.previousElementSibling;
  while (node && !node.matches(HEADING_SELECTOR)) {
    node = node.previousElementSibling;
  }
  return node;
}

/** The heading's text with its Docusaurus `.hash-link` icon excluded, so the label reads "Metrics" and not "Metrics Direct link to Metrics". */
function headingLabel(heading: Element): string | undefined {
  const clone = heading.cloneNode(true) as Element;
  clone.querySelector('.hash-link')?.remove();
  return clone.textContent?.trim() || undefined;
}

/**
 * Wraps a Markdown table in a scrollable frame a keyboard user can reach.
 * Infima renders `table` as `display: block; overflow: auto`, which makes
 * the table itself the scroll container with no `tabindex`; this component
 * moves the overflow to a wrapper `div` and puts the table back to a normal
 * table box, so the frame that scrolls is the frame that can be focused.
 *
 * The wrapper takes `tabindex`, `role="region"` and an `aria-label` cloned
 * from the nearest preceding heading only while the table actually overflows
 * it, so a table that fits gets no tab stop. Re-measured through a
 * `ResizeObserver` on both the wrapper and the table: a sidebar collapse
 * changes the wrapper's available width with no window resize, and a webfont
 * swap changes the table's intrinsic width with neither. Colour mode is not
 * a dependency here — switching `data-theme` changes colours, not box sizes,
 * and the observer still fires if that ever stops being true.
 */
export default function MarkdownTable(props: React.ComponentPropsWithoutRef<'table'>): React.JSX.Element {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const tableRef = useRef<HTMLTableElement>(null);
  const [overflowing, setOverflowing] = useState(false);
  const [label, setLabel] = useState<string | undefined>(undefined);

  useEffect(() => {
    const wrapper = wrapperRef.current;
    const table = tableRef.current;
    if (!wrapper || !table) return undefined;

    const measure = () => {
      const isOverflowing = wrapper.scrollWidth - wrapper.clientWidth > 1;
      setOverflowing(isOverflowing);
      const heading = isOverflowing ? precedingHeading(wrapper) : null;
      setLabel(heading ? headingLabel(heading) : undefined);
    };

    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(wrapper);
    observer.observe(table);
    return () => observer.disconnect();
  }, []);

  return (
    <div
      ref={wrapperRef}
      className={styles.wrapper}
      data-table-overflow={overflowing ? 'true' : 'false'}
      tabIndex={overflowing ? 0 : undefined}
      role={overflowing && label ? 'region' : undefined}
      aria-label={overflowing && label ? label : undefined}
    >
      <table ref={tableRef} className={styles.table} {...props} />
    </div>
  );
}
