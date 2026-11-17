// The two views of a file, the Variable View and what hangs off it (the
// card on a variable's name, the value distribution, sizing columns),
// driven in a real browser against the real server.

import { type Page, expect, test } from "@playwright/test";

import { openFile, stayLocal } from "./helpers";

/** The view that is shown (the other one stays in the page, hidden). */
function panel(page: Page) {
  return page.getByRole("tabpanel");
}

/** The file positions of the variables listed, top to bottom. */
async function listed(page: Page): Promise<string[]> {
  return panel(page).locator(".grid-number").allTextContents();
}

async function clipboard(page: Page): Promise<string> {
  return (await page.evaluate(() => navigator.clipboard.readText())).replaceAll("\r\n", "\n");
}

test("Data and Variables are two views of the file, each kept as it was left", async ({ page }) => {
  stayLocal(page);
  await openFile(page, "survey.sav");
  await page.getByRole("gridcell", { name: "Satisfied", exact: true }).first().click();
  await page.getByRole("radio", { name: "Codes" }).click();
  const selected = page.getByRole("gridcell", { selected: true });
  await expect(selected).toHaveText("4.00");

  await page.getByRole("tab", { name: "Variables" }).click();
  await expect(page).toHaveURL(/#\/variables\/d\d+$/);
  await expect(page.getByRole("grid", { name: "Variables" })).toBeVisible();
  await expect(page.getByRole("grid", { name: "Data" })).toBeHidden();
  await expect(panel(page).locator(".variables-loaded")).toHaveText("13 / 13 variables");
  await expect(panel(page).locator(".statusbar")).toContainText("Counts: 20 rows");
  // One row per variable, with what the codebook says of it.
  await expect(page.getByRole("gridcell", { name: "Categorical" }).first()).toBeVisible();
  await page.getByRole("textbox", { name: "Search variables" }).fill("satis");
  await expect(panel(page).locator(".variables-loaded")).toHaveText("1 / 13 variables");

  // Back: the mode and the selected cell are as they were.
  await page.getByRole("tab", { name: "Data" }).click();
  await expect(page).toHaveURL(/#\/data\/d\d+$/);
  await expect(page.getByRole("radio", { name: "Codes" })).toBeChecked();
  await expect(selected).toHaveText("4.00");
  // And the search of the Variable View is still there.
  await page.getByRole("tab", { name: "Variables" }).click();
  await expect(page.getByRole("textbox", { name: "Search variables" })).toHaveValue("satis");

  // The address alone leads to the view.
  await page.reload();
  await expect(page.getByRole("grid", { name: "Variables" })).toBeVisible();
  await expect(page.getByRole("tab", { name: "Variables" })).toHaveAttribute("aria-selected", "true");
});

test("variables are searched, and listed by type or by what the label check found", async ({
  page,
}) => {
  stayLocal(page);
  await openFile(page, "imperfect_labels.sav");
  await page.getByRole("tab", { name: "Variables" }).click();
  const count = panel(page).locator(".variables-loaded");
  await expect(count).toHaveText("6 / 6 variables");
  expect(await listed(page)).toEqual(["1", "2", "3", "4", "5", "6"]);

  // A name, a variable label or a value label.
  const search = page.getByRole("textbox", { name: "Search variables" });
  await search.fill("brand b");
  await expect(count).toHaveText("1 / 6 variables");
  expect(await listed(page)).toEqual(["4"]);
  // Ctrl + K goes to the search.
  await page.getByRole("grid", { name: "Variables" }).focus();
  await page.keyboard.press("Control+k");
  await expect(search).toBeFocused();
  await search.fill("");

  await page.getByRole("combobox", { name: "Variable type" }).selectOption("text");
  expect(await listed(page)).toEqual(["5"]);
  await page.getByRole("combobox", { name: "Variable type" }).selectOption("all");

  const check = page.getByRole("combobox", { name: "Label check" });
  await check.selectOption("unlabelled");
  expect(await listed(page)).toEqual(["2", "3"]);
  await expect(page.getByRole("gridcell", { name: "2 unlabelled" })).toBeVisible();
  await check.selectOption("duplicates");
  expect(await listed(page)).toEqual(["4"]);
  await expect(page.getByRole("gridcell", { name: "1 duplicate" })).toBeVisible();

  // Nothing matches: said, with the way back.
  await search.fill("zzz");
  await expect(page.getByText("No matching variables")).toBeVisible();
  await page.getByRole("button", { name: "Clear filters" }).last().click();
  await expect(count).toHaveText("6 / 6 variables");
  await expect(check).toHaveValue("all");
});

test("a column sorts from its header and is sized from its edge", async ({ page }) => {
  stayLocal(page);
  await openFile(page, "imperfect_labels.sav");
  await page.getByRole("tab", { name: "Variables" }).click();
  const header = page.getByRole("columnheader", { name: "Name" });
  await expect(header).toHaveAttribute("aria-sort", "none");

  // age, brand, city, id, q1, scale.
  await page.getByRole("button", { name: "Sort by Name" }).click();
  await expect(header).toHaveAttribute("aria-sort", "ascending");
  expect(await listed(page)).toEqual(["6", "4", "5", "1", "2", "3"]);
  await page.getByRole("button", { name: "Sort by Name" }).click();
  await expect(header).toHaveAttribute("aria-sort", "descending");
  expect(await listed(page)).toEqual(["3", "2", "1", "5", "4", "6"]);
  // The arrow keys still move in the grid after a click on a header.
  await page.getByRole("gridcell", { name: "Agreement" }).click();
  await page.getByRole("button", { name: "Sort by Name" }).click();
  await expect(header).toHaveAttribute("aria-sort", "none");
  expect(await listed(page)).toEqual(["1", "2", "3", "4", "5", "6"]);
  await page.keyboard.press("ArrowDown");
  await expect(page.getByRole("gridcell", { selected: true })).toHaveText("Agreement");

  // The edge of a header: arrow keys resize, Enter puts the width back.
  const edge = page.getByRole("separator", { name: "Resize Label column" });
  const cell = page.getByRole("gridcell", { name: "Agreement" });
  const width = async () => Math.round((await cell.boundingBox())?.width ?? 0);
  await expect(edge).toHaveAttribute("aria-valuenow", "330");
  expect(await width()).toBe(330);
  await edge.focus();
  await page.keyboard.press("ArrowRight");
  await page.keyboard.press("Shift+ArrowRight");
  await expect(edge).toHaveAttribute("aria-valuenow", "390");
  expect(await width()).toBe(390);
  await page.keyboard.press("Enter");
  await expect(edge).toHaveAttribute("aria-valuenow", "330");

  // Dragged with the mouse, and back with a double-click.
  const box = await edge.boundingBox();
  if (!box) throw new Error("the edge of the Label column is not on screen");
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(box.x + box.width / 2 - 100, box.y + 5, { steps: 4 });
  await page.mouse.up();
  await expect(edge).toHaveAttribute("aria-valuenow", "230");
  expect(await width()).toBe(230);
  await edge.dblclick();
  await expect(edge).toHaveAttribute("aria-valuenow", "330");
});

test("pointing at a variable's name shows what it is", async ({ page }) => {
  stayLocal(page);
  await openFile(page, "imperfect_labels.sav");
  const card = page.getByRole("tooltip");

  // In the Data View: a column's header.
  await page.getByRole("columnheader", { name: /^q1/ }).hover();
  await expect(card).toBeVisible();
  await expect(card).toContainText("Agreement");
  await expect(card).toContainText("Numeric · F8.0");
  await expect(card).toContainText("4 defined");
  await expect(card).toContainText("System missing");
  await expect(card).toContainText("+1 more value label");
  await expect(card).toContainText("2 unlabelled: 7, 8");
  await expect(card).toContainText("Counts cover 12 rows");
  await page.keyboard.press("Escape");
  await expect(card).toHaveCount(0);
  // A click on the header shows it at once, and selects the column.
  await page.getByRole("columnheader", { name: /^city/ }).click();
  await expect(card).toContainText("String · A20");
  await expect(card).toContainText("New York");
  await expect(page.locator(".statusbar")).toContainText("Variable 5 of 6");
  await page.mouse.move(600, 600);
  await expect(card).toHaveCount(0);

  // In the Variable View: the name in the first column.
  await page.getByRole("tab", { name: "Variables" }).click();
  await panel(page).locator(".variable-name", { hasText: "brand" }).hover();
  await expect(card).toContainText("Brand used most");
  await expect(card).toContainText("duplicate: Other (97, 98)");
});

test("the value distribution counts every value of a variable", async ({ page }) => {
  stayLocal(page);
  await openFile(page, "imperfect_labels.sav");
  await page.getByRole("tab", { name: "Variables" }).click();
  await page.getByRole("button", { name: "View values for q1" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toContainText("VALUE DISTRIBUTION");
  await expect(dialog).toContainText("Agreement");
  await expect(dialog).toContainText("12 rows counted");
  const tile = (name: string) => dialog.locator(".frequency-summary > div", { hasText: name });
  await expect(tile("VALID")).toContainText("11");
  await expect(tile("MISSING")).toContainText("1");
  await expect(tile("UNLABELLED")).toContainText("2");

  // The missing row first, then the codes from low to high.
  const rows = dialog.locator("tbody tr");
  const codes = () => rows.locator("td:first-child").allTextContents();
  expect(await codes()).toEqual([".", "1", "2", "3", "7", "8", "99"]);
  await expect(rows.nth(1)).toContainText("Agree");
  await expect(rows.nth(1)).toContainText("27.3%");
  await expect(rows.nth(4)).toContainText("Unlabelled code");
  await expect(rows.nth(6)).toContainText("user-missing");

  // Sorted and searched on the server; the missing row stays.
  await dialog.getByRole("combobox", { name: "Sort the values" }).selectOption("value_desc");
  await expect(rows.nth(1)).toContainText("Refused");
  expect(await codes()).toEqual([".", "99", "8", "7", "3", "2", "1"]);
  await dialog.getByRole("combobox", { name: "Sort the values" }).selectOption("count");
  await expect(rows.nth(1)).toContainText("Agree");
  await dialog.getByRole("textbox", { name: "Search the values" }).fill("ref");
  await expect(dialog).toContainText("1–1 of 1 codes");
  expect(await codes()).toEqual([".", "99"]);

  await dialog.getByRole("button", { name: "Copy table" }).click();
  await expect(dialog.getByRole("button", { name: "Copied" })).toBeVisible();
  expect(await clipboard(page)).toBe(
    "Code\tLabel\tN\t% valid\t% total\n" +
      ".\tSystem missing\t1\t—\t8.3%\n" +
      "99\tRefused (user-missing)\t1\t9.1%\t8.3%",
  );
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  // The keyboard is back in the grid.
  await page.keyboard.press("ArrowDown");
  await expect(page.getByRole("gridcell", { selected: true })).toBeVisible();

  // A text variable: its distinct values, counted when asked for.
  await page.getByRole("button", { name: "View values for city" }).click();
  await expect(dialog.getByRole("columnheader", { name: "Value" })).toBeVisible();
  await expect(dialog.locator(".frequency-summary")).toContainText("DISTINCT");
  await expect(dialog).toContainText("New York");
  await expect(dialog).toContainText("Springfield");
  await dialog.getByRole("button", { name: "Close value distribution" }).click();
  await expect(dialog).toHaveCount(0);
});

test("cells of the Variable View are read in full, selected and copied", async ({ page }) => {
  stayLocal(page);
  await openFile(page, "imperfect_labels.sav");
  await page.getByRole("tab", { name: "Variables" }).click();
  const inspector = panel(page).locator(".inspector");
  // The first variable's label is the cell selected at first.
  await expect(inspector).toContainText("Respondent ID");

  // A variable's value labels, all of them.
  await page.getByRole("gridcell", { name: /Agree.*Neutral/ }).click();
  await expect(inspector).toContainText("Value labels · 4 defined");
  await expect(inspector).toContainText("Disagree");
  await expect(inspector).toContainText("Refused");
  await expect(inspector).toContainText("user-missing");
  // What the label check found.
  await page.getByRole("gridcell", { name: "2 unlabelled" }).click();
  await expect(inspector).toContainText("2 unlabelled: 7, 8");

  // A block, copied for a spreadsheet: counts are plain numbers.
  await page.getByRole("gridcell", { name: "Agreement" }).click();
  await page.keyboard.press("Shift+ArrowDown");
  await page.keyboard.press("Shift+ArrowRight");
  await expect(page.getByRole("gridcell", { selected: true })).toHaveCount(4);
  await expect(inspector).toContainText("4 cells · 2 variables × 2 columns");
  await page.keyboard.press("Control+c");
  await expect(inspector.getByRole("button", { name: "Copied" })).toBeVisible();
  expect(await clipboard(page)).toBe(
    "Agreement\t1 = Agree; 2 = Neutral; 3 = Disagree; 99 = Refused\n" +
      "Likelihood to recommend\t0 = Not at all likely; 10 = Extremely likely",
  );
  await page.keyboard.press("Escape");
  await expect(page.getByRole("gridcell", { selected: true })).toHaveText("Agreement");

  // The columns Figma does not have: format, measure, user-missing values.
  await page.keyboard.press("End");
  await expect(page.getByRole("columnheader", { name: "User-missing" })).toBeVisible();
  await expect(page.getByRole("columnheader", { name: "Format" })).toBeVisible();
  await expect(page.getByRole("gridcell", { selected: true })).toHaveText("99");
});
