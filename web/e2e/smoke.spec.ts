import { type Page, expect, test } from "@playwright/test";

import { TOKEN } from "../playwright.config";

// The whole path once: the command starts a server, the server lists and
// loads files, the API answers, the page shows it. Files are the
// committed fixtures in tests/data.

/** Fail a test when the page asks anything outside this machine. */
function stayLocal(page: Page) {
  page.on("request", (request) => {
    expect(request.url(), "the page must not load anything from outside").toMatch(
      /^http:\/\/127\.0\.0\.1:/,
    );
  });
}

async function openSurvey(page: Page) {
  await page.goto(`/?token=${TOKEN}`);
  await page.getByRole("button", { name: "Open survey.sav", exact: true }).click();
  await expect(page.getByRole("columnheader", { name: /^id/ })).toBeVisible();
}

test("the launch page lists the folder's data files", async ({ page }) => {
  stayLocal(page);
  await page.goto(`/?token=${TOKEN}`);
  // The token is traded for a cookie and leaves the address.
  await expect(page).toHaveURL(/^http:\/\/127\.0\.0\.1:\d+\/$/);
  await expect(page).toHaveTitle("Data files · ambers");

  const files = page.getByRole("listitem");
  await expect(files.filter({ hasText: "survey.zsav" })).toBeVisible();
  await expect(files.filter({ hasText: "variable_types.sav" })).toBeVisible();
  // Rows and variables arrive from each file's dictionary.
  const survey = page.getByRole("button", { name: "Open survey.sav", exact: true });
  await expect(survey).toContainText("20");
  await expect(survey).toContainText("13");

  await page.getByLabel("Search data files").fill("zsav");
  await expect(files).toHaveCount(1);
  await page.getByRole("button", { name: "Clear filters" }).click();
  await expect(files).not.toHaveCount(1);
});

test("a file opens into the grid, and its last row and last variable are reachable", async ({
  page,
}) => {
  stayLocal(page);
  await openSurvey(page);
  await expect(page).toHaveURL(/#\/data\/d\d+$/);
  await expect(page).toHaveTitle("survey.sav · ambers");
  await expect(page.getByRole("gridcell", { name: "Lisbon" }).first()).toBeVisible();

  // Only what is near the screen is in the page: the last variable is not.
  const last = page.getByRole("columnheader", { name: /^weight/ });
  await expect(last).toHaveCount(0);

  await page.getByRole("grid").focus();
  await page.keyboard.press("Control+End");
  await page.keyboard.press("End");
  await expect(last).toBeVisible();
  const selected = page.getByRole("gridcell", { selected: true });
  await expect(selected).toHaveText("1.25");
  const status = page.locator(".statusbar");
  await expect(status).toContainText("Row 20 of 20");
  await expect(status).toContainText("Variable 13 of 13");
  await expect(status).toContainText("Ready");
});

test("the mode changes what cells show, and the selected cell is shown in full", async ({
  page,
}) => {
  stayLocal(page);
  await openSurvey(page);
  await page.getByRole("gridcell", { name: "Satisfied", exact: true }).first().click();
  const selected = page.getByRole("gridcell", { selected: true });
  const full = page.locator(".value-card code");
  await expect(selected).toHaveText("Satisfied");
  await expect(full).toHaveText("Satisfied");

  await page.getByRole("button", { name: "Codes" }).click();
  await expect(selected).toHaveText("4.00");
  await expect(full).toHaveText("4.00");
  await page.getByRole("button", { name: "Both" }).click();
  await expect(selected).toHaveText(/^4\.00\s+Satisfied$/);
  await page.getByRole("button", { name: "Labels" }).click();
  await expect(selected).toHaveText("Satisfied");

  // The arrow keys move the selection.
  await page.keyboard.press("ArrowDown");
  await expect(page.locator(".statusbar")).toContainText("Row 5 of 20");
});

test("Files goes back to the launch page, which remembers what was opened", async ({ page }) => {
  stayLocal(page);
  await openSurvey(page);
  await page.getByRole("link", { name: "Back to the data files" }).click();
  await expect(page).toHaveURL(/#\/files$/);
  const recent = page.getByRole("region", { name: "Recently opened files" });
  await expect(recent.getByRole("button", { name: /survey\.sav/ })).toBeVisible();

  // Starring puts a file under Starred.
  await page.getByRole("button", { name: "Star survey.zsav", exact: true }).click();
  await page.getByRole("button", { name: /^Starred/ }).click();
  await expect(page.getByRole("listitem")).toHaveCount(1);
  await expect(page.getByRole("listitem")).toContainText("survey.zsav");
  await page.getByRole("button", { name: "Unstar survey.zsav", exact: true }).click();
  await expect(page.getByRole("listitem")).toHaveCount(0);
});
