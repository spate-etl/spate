/** Viewport geometry and element wiring for the scroll reveal styles in site.css. */

/** The fraction of a target `height` tall, `top` from the viewport top, that the viewport shows. */
export function visibleRatio(top: number, height: number, viewportHeight: number): number {
  if (height <= 0) return 0;
  const visible = Math.min(top + height, viewportHeight) - Math.max(top, 0);
  return Math.min(Math.max(visible, 0), height) / height;
}

/**
 * Whether the viewport shows `threshold` of as much of the target as it can
 * ever show at once.
 *
 * The fraction is taken against `min(height, viewportHeight)`, so a target
 * taller than the viewport reveals when the viewport is that much filled.
 */
export function shouldReveal(
  rect: {top: number; height: number},
  viewportHeight: number,
  threshold: number,
): boolean {
  const reach = Math.min(rect.height, viewportHeight);
  if (reach <= 0) return true;
  return visibleRatio(rect.top, rect.height, viewportHeight) * rect.height >= threshold * reach;
}

/**
 * Ratios at which an observer for `threshold` reports a target.
 *
 * The list must stay fine-grained: collapsing it to `threshold` alone stops
 * a target far taller than the viewport being reported at all.
 */
export function revealThresholds(threshold: number): number[] {
  const steps = Array.from({length: 51}, (_, i) => i / 50);
  return [...new Set([...steps, threshold])].sort((a, b) => a - b);
}

interface RevealTarget {
  getBoundingClientRect(): {top: number; height: number};
  classList: {add(token: string): void};
}

interface RevealObserver {
  observe(target: RevealTarget): void;
  disconnect(): void;
}

interface RevealWindow {
  innerHeight: number;
  addEventListener(type: string, listener: () => void, options?: {passive?: boolean}): void;
  removeEventListener(type: string, listener: () => void): void;
  requestAnimationFrame(callback: () => void): number;
  cancelAnimationFrame(handle: number): void;
  IntersectionObserver?: new (
    callback: () => void,
    options: {threshold: number[]},
  ) => RevealObserver;
}

/**
 * Marks `el` with `is-in` once `win` shows `threshold` of it, and returns a
 * teardown that is safe to call after the reveal.
 *
 * Every trigger runs the same measurement, so one arriving is enough.
 */
export function attachReveal(
  el: RevealTarget,
  win: RevealWindow,
  threshold: number,
): () => void {
  const Observer = win.IntersectionObserver;
  if (!Observer) {
    el.classList.add('is-in');
    return () => {};
  }

  let frame = 0;
  let io: RevealObserver | undefined;

  const stop = () => {
    io?.disconnect();
    win.cancelAnimationFrame(frame);
    win.removeEventListener('scroll', measure);
    win.removeEventListener('resize', measure);
  };

  const measure = () => {
    if (shouldReveal(el.getBoundingClientRect(), win.innerHeight, threshold)) {
      el.classList.add('is-in');
      stop();
    }
  };

  io = new Observer(measure, {threshold: revealThresholds(threshold)});
  io.observe(el);
  frame = win.requestAnimationFrame(measure);
  win.addEventListener('scroll', measure, {passive: true});
  win.addEventListener('resize', measure, {passive: true});

  return stop;
}
