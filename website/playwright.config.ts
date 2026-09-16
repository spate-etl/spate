import {defineConfig, devices} from '@playwright/test';

const PORT = 4173;
const isCI = !!process.env.CI;
// `docusaurus serve`'s default host, `localhost`, resolves to the IPv6
// loopback only on this toolchain and never answers on 127.0.0.1, so
// `baseURL` and the `webServer` command below must agree on an explicit
// address — swapping one to `localhost` without the other hangs the
// `webServer` wait until its timeout.
const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? `http://127.0.0.1:${PORT}`;

export default defineConfig({
  testDir: './tests/e2e/specs',
  fullyParallel: true,
  forbidOnly: isCI,
  retries: isCI ? 2 : 0,
  reporter: isCI ? [['github'], ['list'], ['html', {open: 'never'}]] : [['list'], ['html', {open: 'never'}]],
  use: {
    baseURL,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  projects: [
    {name: 'chromium', use: {...devices['Desktop Chrome']}},
    {name: 'firefox', use: {...devices['Desktop Firefox']}},
    {name: 'webkit', use: {...devices['Desktop Safari']}},
  ],
  // Serves the build already on disk; nothing here rebuilds it. Set
  // PLAYWRIGHT_BASE_URL to point at `npm start` instead, which also disables
  // this so the config does not fight a server you started yourself.
  webServer: process.env.PLAYWRIGHT_BASE_URL
    ? undefined
    : {
        command: `npx docusaurus serve --no-open --host 127.0.0.1 --port ${PORT}`,
        url: baseURL,
        reuseExistingServer: !isCI,
        timeout: 60_000,
      },
});
