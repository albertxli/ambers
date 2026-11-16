import { type Page, expect } from "@playwright/test";

import { TOKEN } from "../playwright.config";

/** Fail a test when the page asks anything outside this machine. */
export function stayLocal(page: Page) {
  page.on("request", (request) => {
    expect(request.url(), "the page must not load anything from outside").toMatch(
      /^http:\/\/127\.0\.0\.1:/,
    );
  });
}

/** From the launch page into the Data View of `survey.sav`. */
export async function openSurvey(page: Page) {
  await page.goto(`/?token=${TOKEN}`);
  await page.getByRole("button", { name: "Open survey.sav", exact: true }).click();
  await expect(page.getByRole("columnheader", { name: /^id/ })).toBeVisible();
}
