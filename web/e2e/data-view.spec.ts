import { type Page, expect, test } from "@playwright/test";

import { openSurvey, stayLocal } from "./helpers";

// The Data View's filter bar, columns menu, selection and notices, on
// `survey.sav`: 20 rows; city is Lisbon, Osaka, Lima, N/A in turn; gender
// is Male on odd rows; satisfaction is missing in rows 7 and 14 and
// "Refused" (99) in rows 5, 10, 15 and 20.

const context = (page: Page) => page.locator(".filter-context");
const status = (page: Page) => page.locator(".statusbar");

/** Add a condition through the builder: column, condition, then a value or none. */
async function addFilter(page: Page, column: string, condition: RegExp, value?: string) {
  await page.getByRole("button", { name: "Add filter" }).click();
  const builder = page.getByRole("dialog", { name: "Build filter" });
  await builder.getByLabel("Find a column").fill(column);
  await page.keyboard.press("Enter");
  await builder.getByRole("option", { name: condition }).click();
  if (value !== undefined) {
    await builder.getByLabel("Filter value").fill(value);
    await builder.getByRole("button", { name: "Apply filter" }).click();
  }
  await expect(builder).toHaveCount(0);
}

test("a filter is built in three steps and narrows the rows", async ({ page }) => {
  stayLocal(page);
  await openSurvey(page);

  // The keyboard alone: open, find the column, choose, type, apply.
  await page.keyboard.press("Control+k");
  const builder = page.getByRole("dialog", { name: "Build filter" });
  await builder.getByLabel("Find a column").fill("resid");
  // Found by its label, "City of residence".
  await expect(builder.getByRole("option")).toHaveCount(1);
  await expect(builder.getByRole("option")).toContainText("city");
  await page.keyboard.press("Enter");
  // A text variable offers no comparisons.
  await expect(builder.getByRole("option", { name: /^Contains/ })).toBeVisible();
  await expect(builder.getByRole("option", { name: /^Greater than/ })).toHaveCount(0);
  await builder.getByRole("option", { name: /^Starts with/ }).click();
  await builder.getByLabel("Filter value").fill("li");
  // The values of the column that fit, with their counts.
  await expect(builder.locator(".filter-suggestions button")).toHaveText([
    /Lima\s*5 rows/,
    /Lisbon\s*5 rows/,
  ]);
  await page.keyboard.press("Enter");

  await expect(page.getByRole("button", { name: "Edit filter city Starts with li" })).toBeVisible();
  await expect(context(page)).toContainText("1 condition");
  await expect(context(page)).toContainText("10 of 20 rows");
  await expect(status(page)).toContainText("Row 1 of 10 filtered");
  // The rows keep their numbers in the file.
  await expect(page.locator(".grid-number").nth(1)).toHaveText("3");

  // A second condition: every one of them, or any of them.
  await addFilter(page, "id", /^Greater than/, "15");
  await expect(context(page)).toContainText("2 of 20 rows");
  await expect(page.locator(".filter-connector")).toHaveText("AND");
  await page.getByLabel("Filter match mode").selectOption("any");
  await expect(context(page)).toContainText("Matching any");
  await expect(context(page)).toContainText("13 of 20 rows");
  await expect(page.locator(".filter-connector")).toHaveText("OR");

  // A chip opens its condition for editing.
  await page.getByRole("button", { name: "Edit filter id Greater than 15" }).click();
  const editor = page.getByRole("dialog", { name: "Edit filter" });
  await editor.getByLabel("Filter value").fill("18");
  await editor.getByRole("button", { name: "Save filter" }).click();
  await expect(context(page)).toContainText("11 of 20 rows");

  await page.getByRole("button", { name: "Remove filter id Greater than 18" }).click();
  await expect(context(page)).toContainText("10 of 20 rows");
  await page.getByRole("button", { name: "Clear all" }).click();
  await expect(context(page)).toHaveCount(0);
  await expect(status(page)).toContainText("Row 1 of 20");
});

test("a missing value passes only a condition about missing values", async ({ page }) => {
  stayLocal(page);
  await openSurvey(page);

  // A labelled number is found by its label, picked from the suggestions.
  await page.getByRole("button", { name: "Add filter" }).click();
  const builder = page.getByRole("dialog", { name: "Build filter" });
  await builder.getByLabel("Find a column").fill("satisfaction");
  await page.keyboard.press("Enter");
  // It offers the comparisons and the text conditions.
  await expect(builder.getByRole("option", { name: /^Greater than/ })).toBeVisible();
  await builder.getByRole("option", { name: /^Not equal/ }).click();
  await builder.getByLabel("Filter value").fill("ref");
  await builder.locator(".filter-suggestions button", { hasText: "Refused" }).click();
  await expect(builder.getByLabel("Filter value")).toHaveValue("Refused");
  await builder.getByRole("button", { name: "Apply filter" }).click();
  // 20 rows, less the 4 refused and the 2 with no answer.
  await expect(context(page)).toContainText("14 of 20 rows");

  await page.getByRole("button", { name: "Clear all" }).click();
  // No value to enter: the condition applies when chosen.
  await addFilter(page, "satisfaction", /^Is empty/);
  await expect(page.locator(".filter-chip")).toHaveText(/satisfaction\s*is empty/);
  await expect(context(page)).toContainText("2 of 20 rows");
  await expect(page.locator(".grid-number")).toHaveText(["7", "14"]);
});

test("a condition that cannot be applied is said, and no match is not an error", async ({
  page,
}) => {
  stayLocal(page);
  await openSurvey(page);

  await page.getByRole("button", { name: "Add filter" }).click();
  const builder = page.getByRole("dialog", { name: "Build filter" });
  await builder.getByLabel("Find a column").fill("id");
  await page.keyboard.press("Enter");
  await builder.getByRole("option", { name: /^Greater than/ }).click();
  await builder.getByLabel("Filter value").fill("abc");
  await expect(builder.locator(".filter-value-help")).toHaveText("Enter a valid number.");
  await expect(builder.getByRole("button", { name: "Apply filter" })).toBeDisabled();
  await builder.getByLabel("Filter value").fill("500");
  await builder.getByRole("button", { name: "Apply filter" }).click();

  const none = page.getByRole("status").filter({ hasText: "No matching rows" });
  await expect(none).toBeVisible();
  await expect(context(page)).toContainText("0 of 20 rows");
  await none.getByRole("button", { name: "Clear filters" }).click();
  await expect(page.getByRole("gridcell", { name: "Lisbon" }).first()).toBeVisible();
  await expect(page.locator(".filter-chip")).toHaveCount(0);
});

test("the Columns menu hides and shows variables", async ({ page }) => {
  stayLocal(page);
  await openSurvey(page);
  const columns = page.getByRole("button", { name: /^Columns/ });
  await columns.click();
  const menu = page.getByRole("dialog", { name: "Choose columns" });
  await expect(menu).toContainText("13 of 13");
  await menu.getByLabel("Find a column").fill("brand");
  await expect(menu.getByRole("checkbox")).toHaveCount(3);
  await menu.getByRole("button", { name: "Hide these 3" }).click();
  await expect(menu).toContainText("10 of 13");
  await page.keyboard.press("Escape");
  await expect(columns).toContainText("10/13");

  // city is the 6th variable; its neighbour is now the 10th.
  await page.getByRole("gridcell", { name: "Lisbon" }).first().click();
  await expect(status(page)).toContainText("Variable 6 of 13");
  await page.keyboard.press("ArrowRight");
  await expect(status(page)).toContainText("Variable 10 of 13");

  await columns.click();
  await menu.getByRole("button", { name: "Show all" }).click();
  await expect(menu).toContainText("13 of 13");
  await page.keyboard.press("Escape");
  await expect(columns).not.toContainText("/");
});

test("a block of cells is selected and copied for a spreadsheet", async ({ page }) => {
  stayLocal(page);
  await openSurvey(page);
  const selected = page.getByRole("gridcell", { selected: true });
  const summary = page.locator(".selection-summary");
  const clipboard = async () =>
    (await page.evaluate(() => navigator.clipboard.readText())).replaceAll("\r\n", "\n");

  // With the keyboard: Shift and the arrows extend the selection.
  await page.getByRole("gridcell", { name: "Female", exact: true }).first().click();
  await page.keyboard.press("Shift+ArrowDown");
  await page.keyboard.press("Shift+ArrowRight");
  await expect(selected).toHaveCount(4);
  await expect(summary).toContainText("gender:satisfaction · rows 2–3");
  await expect(summary).toContainText("4 cells");
  await expect(page.locator(".inspector")).toContainText("4 cells · 2 rows × 2 variables");
  await page.keyboard.press("Control+c");
  await expect(page.getByRole("button", { name: "Copied" })).toBeVisible();
  expect(await clipboard()).toBe("Female\tDissatisfied\nMale\tNeutral");

  // As the mode shows them.
  await page.getByRole("radio", { name: "Codes" }).click();
  await page.getByRole("button", { name: "Copy range" }).click();
  await expect(page.getByRole("button", { name: "Copied" })).toBeVisible();
  expect(await clipboard()).toBe("2.00\t2.00\n1.00\t3.00");
  await page.getByRole("radio", { name: "Both" }).click();
  await page.getByRole("button", { name: "Copy range" }).click();
  await expect(page.getByRole("button", { name: "Copied" })).toBeVisible();
  expect(await clipboard()).toBe(
    "Female (2.00)\tDissatisfied (2.00)\nMale (1.00)\tNeutral (3.00)",
  );
  await page.getByRole("radio", { name: "Labels" }).click();

  // Esc goes back to the cell the selection started from.
  await page.keyboard.press("Escape");
  await expect(selected).toHaveCount(1);
  await expect(selected).toHaveText("Female");

  // With the mouse: Shift + click extends, a drag selects.
  await page.getByRole("gridcell", { name: "Neutral", exact: true }).first().click({
    modifiers: ["Shift"],
  });
  await expect(selected).toHaveCount(4);
  const from = await page.getByRole("gridcell", { name: "Lisbon" }).first().boundingBox();
  const to = await page.getByRole("gridcell", { name: "Lima" }).first().boundingBox();
  if (!from || !to) throw new Error("the cells to drag between are not on screen");
  await page.mouse.move(from.x + 6, from.y + 6);
  await page.mouse.down();
  await page.mouse.move(to.x + 6, to.y + 6, { steps: 5 });
  await page.mouse.up();
  await expect(summary).toContainText("city · rows 1–3");
  await expect(selected).toHaveText(["Lisbon", "Osaka", "Lima"]);
});

test("the keys are listed on request", async ({ page }) => {
  stayLocal(page);
  await openSurvey(page);
  await page.getByRole("button", { name: "Keyboard shortcuts" }).click();
  const panel = page.getByRole("dialog", { name: "Keyboard shortcuts" });
  await expect(panel).toContainText("Copy the selection");
  await expect(panel).toContainText("add a filter");
  await expect(panel).toContainText("Resize a column");
  await page.keyboard.press("Escape");
  await expect(panel).toHaveCount(0);
  // `?` opens it too, from the grid.
  await page.getByRole("grid").focus();
  await page.keyboard.press("?");
  await expect(panel).toBeVisible();
  await page.keyboard.press("?");
  await expect(panel).toHaveCount(0);
});

test("a stopped server is said, with the command that brings it back", async ({ page }) => {
  // The page asks the server every five seconds; this test waits for that
  // three times.
  test.setTimeout(60_000);
  stayLocal(page);
  await openSurvey(page);
  // The page hears nothing from the server any more.
  await page.route("**/api/v1/heartbeat", (route) => route.abort());
  const notice = page.getByRole("alert");
  await expect(notice).toContainText("Server stopped", { timeout: 15_000 });
  // The command names the file that is open.
  await expect(notice.locator("code")).toHaveText(/^am open ".*survey\.sav"$/);
  await notice.getByRole("button", { name: "Copy restart command" }).click();
  await expect(notice.getByRole("button", { name: "Copy restart command" })).toHaveText("Copied");
  const copied = await page.evaluate(() => navigator.clipboard.readText());
  expect(copied).toMatch(/^am open ".*survey\.sav"$/);

  // It goes by itself when the server answers again.
  await page.unroute("**/api/v1/heartbeat");
  await expect(notice).toHaveCount(0, { timeout: 15_000 });
  // And it can be put away.
  await page.route("**/api/v1/heartbeat", (route) => route.abort());
  await expect(notice).toBeVisible({ timeout: 15_000 });
  await notice.getByRole("button", { name: "Dismiss server notice" }).click();
  await expect(notice).toHaveCount(0);
  await page.unroute("**/api/v1/heartbeat");
});
