import {useEffect, useRef, useState} from 'react';

import {attachOnScreen} from './onScreen';

/**
 * A ref for an element and whether the viewport shows any of it. The answer is
 * true until the first measurement, and without an observer it stays true, so a
 * caller gated on it is never left switched off.
 */
export function useOnScreen<T extends HTMLElement>(): [React.RefObject<T | null>, boolean] {
  const ref = useRef<T>(null);
  const [onScreen, setOnScreen] = useState(true);
  useEffect(() => {
    const el = ref.current;
    if (!el) return undefined;
    return attachOnScreen(el, window, setOnScreen);
  }, []);
  return [ref, onScreen];
}
