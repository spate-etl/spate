/** Viewport geometry for work that only matters while its element is on screen. */

/** Whether any part of a target `top` from the viewport top and `height` tall is in view. */
export function isOnScreen(rect: {top: number; height: number}, viewportHeight: number): boolean {
  return rect.top < viewportHeight && rect.top + rect.height > 0;
}

interface ScreenTarget {
  getBoundingClientRect(): {top: number; height: number};
}

interface ScreenObserver {
  observe(target: ScreenTarget): void;
  disconnect(): void;
}

interface ScreenWindow {
  innerHeight: number;
  addEventListener(type: string, listener: () => void, options?: {passive?: boolean}): void;
  removeEventListener(type: string, listener: () => void): void;
  requestAnimationFrame(callback: () => void): number;
  cancelAnimationFrame(handle: number): void;
  IntersectionObserver?: new (callback: () => void, options: {threshold: number[]}) => ScreenObserver;
}

/**
 * Reports whether `win` shows any of `el` to `set`, and returns a teardown.
 *
 * Every trigger runs the same measurement from a rect, so an observer
 * notification is one of several and none of them has to arrive. A window
 * without an observer reports on screen once, which is the answer that leaves
 * the caller running rather than stopped.
 */
export function attachOnScreen(
  el: ScreenTarget,
  win: ScreenWindow,
  set: (onScreen: boolean) => void,
): () => void {
  const Observer = win.IntersectionObserver;
  if (!Observer) {
    set(true);
    return () => {};
  }

  let frame = 0;
  const measure = () => {
    frame = 0;
    set(isOnScreen(el.getBoundingClientRect(), win.innerHeight));
  };
  // Scroll and resize arrive far faster than a frame, and the measurement reads
  // layout, so they coalesce into the frame the first of them schedules.
  const schedule = () => {
    if (!frame) frame = win.requestAnimationFrame(measure);
  };

  const io = new Observer(schedule, {threshold: [0, 1]});
  io.observe(el);
  schedule();
  win.addEventListener('scroll', schedule, {passive: true});
  win.addEventListener('resize', schedule, {passive: true});

  return () => {
    io.disconnect();
    if (frame) win.cancelAnimationFrame(frame);
    win.removeEventListener('scroll', schedule);
    win.removeEventListener('resize', schedule);
  };
}
