import {test as base} from '@playwright/test';

export type ColorMode = 'light' | 'dark';

type ColorModeFixtures = {
  colorMode: ColorMode;
};

/**
 * Adds a `colorMode` test option, pinned per file or describe block with
 * `test.use({colorMode: 'light'})`. It drives the built-in `colorScheme`
 * context option and seeds the `localStorage` key Docusaurus's inline theme
 * script reads before first paint, so the page renders in the requested mode
 * instead of flashing the default and swapping.
 *
 * Also sets `reducedMotion: 'reduce'`, so a scan does not land mid-transition
 * on the homepage's scroll-reveal fade (`site.css`'s `.reveal` rule, gated on
 * `prefers-reduced-motion: no-preference`) and report a false contrast
 * violation against a colour pair that was only ever passing through. That
 * setting also means this suite never observes a `.reveal` element stuck at
 * `opacity: 0` for a visitor who does not have reduced motion on — see
 * `specs/reveal.spec.ts`, which runs without it and checks that case.
 */
export const test = base.extend<ColorModeFixtures>({
  colorMode: ['dark', {option: true}],
  colorScheme: ({colorMode}, use) => use(colorMode),
  reducedMotion: 'reduce',
  context: async ({context, colorMode}, use) => {
    await context.addInitScript((mode: ColorMode) => {
      window.localStorage.setItem('theme', mode);
    }, colorMode);
    await use(context);
  },
});
