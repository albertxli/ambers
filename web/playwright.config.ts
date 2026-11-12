import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { defineConfig } from "@playwright/test";

// End-to-end tests: the real `am open` server on the committed fixtures,
// and the real page in a browser. Build the front end first
// (`bun run build`): the server serves the page embedded in the binary.
//
// The server gets a fixed port and token and its own state folder, so it
// never joins a server the developer has running and leaves no trace.
export const PORT = 26271;
export const TOKEN = "e2e";
const state = mkdtempSync(join(tmpdir(), "ambers-e2e-"));

export default defineConfig({
  testDir: "e2e",
  timeout: 30_000,
  // One server, and tests that star and open files in it: one at a time.
  workers: 1,
  fullyParallel: false,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    // Locally the installed Edge is used, so no browser is downloaded.
    // CI installs Chromium (`bunx playwright install chromium`).
    channel: process.env.CI ? undefined : "msedge",
    // Small on purpose: not every row and column fits, so the tests see
    // that the grid only holds what is near the screen.
    viewport: { width: 900, height: 520 },
  },
  webServer: {
    command: `cargo run --quiet -p ambers-cli --bin am -- open tests/data --no-browser --port ${PORT} --keep-alive`,
    cwd: "..",
    // Without the token the server answers 401: it is up.
    url: `http://127.0.0.1:${PORT}/`,
    env: { AMBERS_DEV_TOKEN: TOKEN, AMBERS_STATE_DIR: state },
    reuseExistingServer: false,
    timeout: 600_000,
  },
});
