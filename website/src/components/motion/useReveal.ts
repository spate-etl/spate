import {useEffect, useRef} from 'react';

import {attachReveal} from './reveal';

/**
 * Marks an element `is-in` once it scrolls into view, for the `reveal` styles
 * in site.css. Without an observer, or without script, the element is simply
 * visible: the hidden state exists only under `html[data-motion]`.
 */
export function useReveal<T extends HTMLElement>(threshold = 0.15) {
  const ref = useRef<T>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el) return undefined;
    return attachReveal(el, window, threshold);
  }, [threshold]);
  return ref;
}
