import {useEffect} from 'react';

/**
 * Stamps `data-motion` on the document once React has mounted. The stylesheet
 * hides pre-reveal elements only under that attribute, so the server-rendered
 * page and the first client render agree, and a reader without script sees
 * every section.
 */
export default function MotionRoot(): null {
  useEffect(() => {
    document.documentElement.setAttribute('data-motion', '');
    return () => document.documentElement.removeAttribute('data-motion');
  }, []);
  return null;
}
