import {useEffect, useRef} from 'react';

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
    if (!('IntersectionObserver' in window)) {
      el.classList.add('is-in');
      return undefined;
    }
    const io = new IntersectionObserver(
      (entries) => {
        for (const e of entries) {
          if (e.isIntersecting) {
            el.classList.add('is-in');
            io.disconnect();
          }
        }
      },
      {threshold},
    );
    io.observe(el);
    return () => io.disconnect();
  }, [threshold]);
  return ref;
}
