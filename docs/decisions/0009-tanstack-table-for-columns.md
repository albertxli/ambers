# 0009: TanStack Table holds the columns and their state; rows stay on the server

Date: 2026-11-16
Status: accepted

## Decision

The Data View's grid is built on TanStack Table (version 9) and TanStack Virtual, with a division of work:

- **TanStack Table** holds the columns, one per variable, and what the user did to them: sizes and resizing, which are shown. Later: order, pinning, sort state. Headers are drawn from it.
- **TanStack Virtual** decides which rows and columns are near the screen.
- **The server** holds the rows. The page asks for the cells near the screen in blocks, sends the filter with every request, and is told how many rows it keeps. The table is given no rows at all.

Range selection (drag, Shift + click, Shift + arrows) and copying are the page's own code (`selection.ts`, `copy.ts`), working on positions: a row's place among the rows shown, a column's place among the variables shown.

## Alternatives considered

- **Give the table the rows**, as its examples and the design mock-up do, and let it filter and select. A file can have 395,330 rows by 916 variables. The table builds an object per row and, when asked for a row's cells, one per column; even a window of rows would rebuild tens of thousands of objects while scrolling, and the whole file cannot be in the page at all (decision 0005: the data stays behind the local server).
- **TanStack Table 9's cell selection feature.** It names a selection by row id and column id and resolves it through the table's row model, which this grid does not have.
- **No table library**, as the first version of the grid: columns as an array, widths in a map. It worked for one feature. With hiding, and ordering and pinning to come, it would grow into a poorer copy of what the library already has.
- **TanStack Table 8**, which the mock-up uses. Version 9 was current when the dependency was added; it registers only the features used (sizing, resizing, visibility), and its types carry the variable on each column without a global declaration.

## Why

The column side of a data grid is where a table library earns its place: state that several controls share (the header's drag handle, the Columns menu, later a pin or a sort arrow). The row side of this grid is a window onto data that lives elsewhere, which is what a virtualiser and a block cache are for.

## Consequences

- A feature of the library that needs rows (its filtering, sorting, grouping, cell selection) is not available as such: the server does the work and the table holds the state (`manual…` options) when such a feature is added.
- Cells are drawn only for the columns on screen, with no per-cell objects: measured on a 916-variable file, a frame while scrolling sideways takes about 10 ms.
- Copying a selection fetches its cells from the server, so it is capped (50,000 cells).
