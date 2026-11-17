# The `ambers` command line

`ambers` inspects SPSS `.sav` and `.zsav` files from a terminal: a quick summary of a file, and a full-screen viewer for its data and variables. It is a single program that needs no Python, no Rust and no SPSS.

`am` is a short alias. Every example below works with either name.

- [Install](#install)
- [Commands at a glance](#commands-at-a-glance)
- [`am info`: summary and variables](#am-info-summary-and-variables)
- [`am view`: browse the data](#am-view-browse-the-data)
  - [Data View](#data-view)
  - [Filtering rows](#filtering-rows)
  - [Variable View](#variable-view)
  - [Copying](#copying)
  - [Incomplete labels](#incomplete-labels)
  - [Terminal notes](#terminal-notes)
- [`am open`: the file in your browser](#am-open-the-file-in-your-browser)
- [Limits](#limits)
- [Recipes](#recipes)

## Install

**Standalone program.** One line installs it, with no administrator rights:

```bash
# macOS and Linux
curl -LsSf https://raw.githubusercontent.com/albertxli/ambers/main/install.sh | sh
```

```powershell
# Windows
powershell -NoProfile -ExecutionPolicy ByPass -c "irm https://raw.githubusercontent.com/albertxli/ambers/main/install.ps1 | iex"
```

The script downloads the program for your machine from the [latest release](https://github.com/albertxli/ambers/releases/latest) and puts `ambers` and `am` in one folder:

| | macOS and Linux | Windows |
|---|---|---|
| Installed to | `~/.local/bin` | `%LOCALAPPDATA%\ambers\bin` |
| PATH | The script tells you the line to add if the folder is not on it | Added to your user PATH (new terminal windows see it) |
| Update | Run the same line again | Run the same line again (close any open `am view` first) |
| Remove | Delete `ambers` and `am` from that folder | Delete the folder |

Settings, as environment variables: `AMBERS_VERSION` (a version instead of the latest, e.g. `0.5.0`), `AMBERS_INSTALL_DIR` (another folder), and on Windows `AMBERS_NO_MODIFY_PATH=1` (leave PATH alone).

Ready-made programs exist for these machines. On others (Windows on ARM, for one) the script says so; use the Python package there.

| Platform | Archive on the release page |
|---|---|
| Windows, 64-bit | `ambers-<version>-x86_64-pc-windows-msvc.zip` |
| Linux, x86-64 | `ambers-<version>-x86_64-unknown-linux-musl.tar.gz` |
| Linux, ARM 64-bit | `ambers-<version>-aarch64-unknown-linux-musl.tar.gz` |
| macOS, Apple Silicon | `ambers-<version>-aarch64-apple-darwin.tar.gz` |
| macOS, Intel | `ambers-<version>-x86_64-apple-darwin.tar.gz` |

You can also download an archive yourself, unpack it, and run the program inside.

**With the Python package.** The same commands come with it, run through Python:

```bash
uv tool install ambers     # puts `ambers` and `am` on your PATH
uv run am info file.sav    # in a project that has `uv add ambers`
uvx --from ambers am info file.sav   # once, without installing
```

Check that it works:

```bash
am --version               # ambers 0.6.0
am --help                  # the commands
am info --help             # one command's options
```

## Commands at a glance

| Command | What it does | Reads the data? |
|---|---|---|
| `am info <FILE>` | Prints a summary of the file and a table of its variables | Only with `--counts` or `--rows` |
| `am view <FILE>` | Opens a full-screen grid of the data, with a variable list, filters and copy | Yes, in the background |
| `am open <FILE>` | Opens the file in your browser, served from this machine only (in development) | Yes, in the background |

`<FILE>` is a path to a `.sav` or `.zsav` file. Quote it if it contains spaces.

## `am info`: summary and variables

```bash
am info survey.sav                     # summary, then the first 10 and last 5 variables
am info survey.sav --head 20           # only the first 20 variables
am info survey.sav --tail 5            # only the last 5
am info survey.sav --head 5 --tail 5   # the first 5 and the last 5
am info survey.sav --head 0            # the summary only
am info survey.sav --counts            # adds N and Missing per variable
am info survey.sav --rows 10           # also the first 10 data rows
```

| Option | Meaning |
|---|---|
| `--head N` | List only the first N variables |
| `--tail N` | List only the last N variables. With `--head`: the first and the last |
| `--counts` | Read the data once and add `N` (valid cases) and `Missing` per listed variable |
| `--rows N` | Also print the first N data rows of the first few columns |

With no option, the first 10 and the last 5 variables are listed.

```text
$ am info survey.sav --head 3 --tail 2 --counts --rows 3
╭─────────────┬─────────────────────────────────╮
│ File        │ survey.sav                      │
│ Label       │ ambers synthetic survey fixture │
│ Shape       │ 20 rows × 13 columns            │
│ Size        │ 3.6 KB (3,640 bytes)            │
│ Compression │ bytecode                        │
│ Encoding    │ UTF-8                           │
│ Created     │ 2026-11-11 23:00:15             │
│ Counted     │ 20 rows                         │
│ Status      │ ok                              │
╰─────────────┴─────────────────────────────────╯
┌─────┬──────────────┬─────────────┬──────────────────────┬─────┬─────────┐
│ #   ┆ Name         ┆ Type        ┆ Label                ┆   N ┆ Missing │
╞═════╪══════════════╪═════════════╪══════════════════════╪═════╪═════════╡
│ 1   ┆ id           ┆ numeric     ┆ Respondent ID        ┆  20 ┆       0 │
│ 2   ┆ gender       ┆ categorical ┆ Gender               ┆  20 ┆       0 │
│ 3   ┆ satisfaction ┆ categorical ┆ Overall satisfaction ┆  18 ┆       2 │
│ ... ┆ ...          ┆ ...         ┆ ...                  ┆ ... ┆     ... │
│ 12  ┆ duration     ┆ date        ┆ Interview length     ┆  20 ┆       0 │
│ 13  ┆ weight       ┆ numeric     ┆ Sampling weight      ┆  20 ┆       0 │
└─────┴──────────────┴─────────────┴──────────────────────┴─────┴─────────┘
┌─────┬────────┬──────────────┬──────┬─────────┬────────┬─────────┬─────────┐
│ id  ┆ gender ┆ satisfaction ┆ age  ┆ income  ┆ city   ┆ brand_a ┆ brand_b │
╞═════╪════════╪══════════════╪══════╪═════════╪════════╪═════════╪═════════╡
│ 1.0 ┆ 1.0    ┆ 1.0          ┆ 18.0 ┆ 20000.0 ┆ Lisbon ┆ 0.0     ┆ 1.0     │
│ 2.0 ┆ 2.0    ┆ 2.0          ┆ 21.0 ┆ 21500.5 ┆ Osaka  ┆ 1.0     ┆ 0.0     │
│ 3.0 ┆ 1.0    ┆ 3.0          ┆ 24.0 ┆ 23001.0 ┆ Lima   ┆ 0.0     ┆ 0.0     │
└─────┴────────┴──────────────┴──────┴─────────┴────────┴─────────┴─────────┘
8 of 13 columns shown
```

### The summary

| Row | Meaning |
|---|---|
| File, Label | The path given and the file label set in SPSS |
| Shape | Rows × variables, from the file's header |
| Size, Compression, Encoding, Created | As stored in the file |
| Counted | With `--counts`: how many rows were read |
| Status | `ok`, the number of warnings, or `error` |
| Warnings | When the reader found damage (header and contents disagree), one line each. See [warnings_and_errors.md](warnings_and_errors.md) |

In a terminal the summary's border is green for a clean read, yellow when there are warnings, red when the file cannot be read. The `Status` row says the same in words, so nothing is lost without colour.

### The variables table

| Column | Meaning |
|---|---|
| `#` | The variable's position in the file |
| Name | Always shown in full |
| Type | `categorical`, `numeric`, `text` or `date`, as `ambers.codebook()` defines them |
| Label | The variable label; a long one keeps its beginning and end |
| N | With `--counts`: valid cases |
| Missing | With `--counts`: system-missing cases (for text, empty strings too) |

How the type is decided:

- A string variable is `text`, even with value labels.
- A date, date-time or time variable is `date`.
- A numeric variable is `categorical` when it has value labels, belongs to a multiple-response set, or holds only 0 and 1. Otherwise it is `numeric`.

Without `--counts`, `info` reads only the file's dictionary, so it is instant on a file of any size. The type then comes from the metadata alone, which is exact with one exception: an unlabelled variable that holds only 0 and 1 shows as `numeric`. With `--counts` the data is read once, for the listed variables only, and the type is exact. On a 5.4 GB file with 916 variables that took 1.6 seconds.

Values declared user-missing in SPSS count as valid, as in the codebook.

### In scripts

- The report goes to standard output; errors go to standard error.
- Exit code `0` on success, `1` when the file cannot be read, `2` for a wrong option.
- Colour is used only when the output is a terminal. It is dropped when piped or redirected, and when `NO_COLOR` is set.
- The tables never depend on the terminal's width, so the output is the same in a terminal and in a file.

```text
$ am info nope.sav
╭────────┬─────────────────────────────────────────────────────────╮
│ File   │ nope.sav                                                │
│ Status │ error                                                   │
│ Error  │ The system cannot find the file specified. (os error 2) │
╰────────┴─────────────────────────────────────────────────────────╯
$ echo $?
1
```

## `am view`: browse the data

```bash
am view survey.sav                  # opens on value labels
am view survey.sav --mode codes     # opens on stored codes
am view big.sav --limit 200000      # load at most 200,000 rows
am view big.sav --memory 8          # load at most 8 GB of data
am view survey.sav --theme terminal # use your terminal's colours
```

| Option | Values | Default | Meaning |
|---|---|---|---|
| `--mode` | `labels`, `codes`, `both` | `labels` | What cells show at the start. Tab switches while viewing |
| `--limit N` | a row count | none | Load at most N rows; the rest of the file is not read |
| `--memory GB` | a number, e.g. `8` or `0.5` | half of the free memory | Stop loading when the data takes this much memory |
| `--theme` | `dracula`, `terminal` | `dracula` | The Dracula palette, or your terminal's own colours |

A full-screen grid in the style of SPSS's Data View, with a second screen in the style of its Variable View. It is read-only: nothing is ever written to the file.

**Loading.** The first screen appears at once and the rest of the file loads in the background; you can move, filter and copy while it loads. Loading stops by itself when the data would take more than half of the memory the machine has free at that moment (or the `--memory` you give). The bottom border then says how many rows were loaded and names the option that loads more. The rows already loaded stay usable.

Press `?` at any time for the list of keys, and `q` or Esc to leave.

### Data View

One row per respondent, one column per variable. The row numbers at the left and, unless you turn it off, the first variable stay in place while you scroll.

| Key | Action |
|---|---|
| arrows, or `h` `j` `k` `l` | move one cell |
| PgUp / PgDn | one screen up / down |
| Ctrl+u / Ctrl+d | half a screen up / down |
| Shift+arrows, or `{` `}` `<` `>` | 50 rows or columns at once |
| Ctrl+↑ / Ctrl+↓, or `g` / `G` | first / last loaded row |
| Ctrl+← / Ctrl+→, or `0` / `$`, or Home / End | far left / far right |
| digits, then Enter | go to that row; the grid moves as you type, Esc goes back |
| Tab | switch what cells show: **Labels**, **Codes**, **Both** (`1  Male`) |
| `f` | freeze / unfreeze the first variable |
| Enter | side panel with the current respondent: every variable, its value and its label |
| `J` / `K` | scroll that panel |
| `[` / `]` | make the current column narrower / wider |
| `w` | make the current column as wide as the window allows; again: back to its own width |
| `-` / `+` | make the side panel narrower / wider |
| `v` | hide / show the full-text bar under the grid |
| `/` | filter the rows by the current column |
| `=` | keep only the rows with the current cell's value |
| `c` | copy the current cell |
| `C` or `t` | with the panel open: copy that respondent |
| `i` | Variable View |
| `?` | list of keys |
| `q`, Esc | close the panel, then clear the filters, then quit |

**Mouse.** The wheel scrolls up and down. Shift+wheel, Alt+wheel or a tilt wheel scrolls sideways. A click selects the cell under it. Drag the mark after a column's name in the header line to resize the column, and the side panel's left border to resize the panel.

**Reading long text.** Columns are at most 30 characters wide at first (40 in Both mode), so a long answer is cut in the grid. Two ways to read it whole:

- The bar under the grid shows the highlighted cell in full, wrapped over up to three lines, under the variable's name and label. `v` hides it when you want the lines for rows.
- `w` widens the current column to the room the window has; `[` and `]` adjust it a few characters at a time. A width you set stays until you change it, in all three modes.

The bar is left out when the window is shorter than 14 lines.

**What cells show.** Numbers are written in the variable's SPSS print format (`3.00` for F8.2). System-missing is `.`. In Labels mode a value with a label shows the label and a value without one shows itself.

The bottom border shows the mode and the position (`Row 12/1,500 ─ Var 3/75`); the current variable's name and label are on the full-text bar, or on the border when the bar is hidden.

### Filtering rows

Put the cursor on a column and press `/`. A bar opens under the top border and the grid narrows as you type. Enter keeps the filter; Esc drops what you typed.

| Typed | Rows kept |
|---|---|
| `jap` | the value or its label contains "jap", ignoring case |
| `=Japan`, `=3` | the value or its label is exactly that |
| `!=Japan` | everything else, missing rows included |
| `>30`, `>=30`, `<30`, `<=30` | numeric variables only |
| `=.` | system-missing (`!=.`: not missing) |
| `japan\|france` | either: `\|` separates alternatives |

- **Labels and codes both match.** `jap` finds the label "Japan" and `=3` finds its code, whichever of Labels / Codes / Both is on screen.
- **Alternatives** can each use any of the forms: `=1|=2`, `<18|>65`, `refused|=.`.
- **One key for the current cell:** `=` keeps the rows that have the value under the cursor.
- **Filters on several columns stack.** Filter `country`, move to `gender`, press `/` again: a row must pass both. So `japan|france` on `country` plus `=female` on `gender` shows women in Japan or France.
- **Editing and removing.** `/` on a filtered column opens its filter again; emptying it and pressing Enter removes it. Esc, with no panel open, clears all filters.
- **The bar** lists the filters and the count: `Filter gender: =female │ city: osa   5 of 20 rows`.
- **Row numbers** at the left stay the file's own. Going to a row by number lands on that row, or on the next one the filter shows.
- Moving, the respondent panel and copying all work on the filtered rows.

A half-typed filter such as `>` keeps every row and the bar says what is missing (`type a number after >`).

Filtering is fast on large files: it compares the stored values of one column, not formatted text, and typing more only re-tests the rows still shown. On a 5.4 GB file with 395,330 rows a keystroke took at most 14 ms.

While the file is still loading, a filter covers the rows loaded so far and picks up new ones as they arrive.

### Variable View

Press `i`. One row per variable, described as `ambers.codebook()` describes it, with the same rules and the same numbers:

| Column | Meaning |
|---|---|
| `#`, Name | Position in the file, and the name; both stay in place when the list scrolls sideways |
| Type | `categorical`, `numeric`, `text` or `date` (see [the rules](#the-variables-table)) |
| Label | The variable label |
| Values | The labelled values, `1=Male, 2=Female` |
| N, Missing | Valid and system-missing cases, for the rows loaded so far |
| Label check | `ok`, or what is wrong with the labels (see [Incomplete labels](#incomplete-labels)); blank when the variable has no value labels to check |

| Key | Action |
|---|---|
| arrows, click | move the highlighted cell; the fast and edge keys of Data View work here too |
| Enter | side panel with the variable's value table |
| `J` / `K`, or the wheel over the panel | scroll the value table |
| `-` / `+`, or drag the panel's left border | make the value table narrower / wider |
| `v` | hide / show the full-text bar under the list |
| `/` | search the variables by name or label |
| `d` | go to this variable's column in Data View |
| `c` | copy the highlighted cell in full |
| `C` or `t` | with the value table open: copy the table |
| `i`, `q`, Esc | close the panel, then clear the search, then back to Data View |

**The full text of a cell.** Under the list, a bar shows the highlighted cell uncut and wrapped: the whole label, or the whole list of values.

**The value table** (Enter) shows the variable's full label, its format and missing-value definition, and every value with its count and its share of valid and of all cases.

- For a categorical variable: every code, including labelled codes that never occur (count 0), codes found in the data without a label, and the system-missing row.
- For a text, numeric or date variable: every distinct value with its count. The missing row comes first, then text from A to Z (ignoring case; symbols and digits before letters), numbers and dates in ascending order.

Counts cover the rows loaded so far and update while the file loads; the border always says how many rows that is. They do not follow the row filters of Data View.

**Searching.** `/` opens `Search variables:` and the list narrows as you type to the variables whose name or label contains the text, ignoring case. Enter keeps the search, Esc clears it. To find a column among hundreds: `i`, `/`, part of its name or label, Enter, `d`.

### Copying

| Key | Where | What is copied |
|---|---|---|
| `c` | Data View | The current cell, as shown in the current mode, in full |
| `c` | Variable View | The highlighted cell in full: the whole label, or the values one per line |
| `C` or `t` | Data View, respondent panel open | That respondent: one line per variable with `Variable`, `Label`, `Value` |
| `C` or `t` | Variable View, value table open | The table: `Code`, `Label`, `N`, `% valid`, `% total` |

Panels are copied as tab-separated columns with a header row, so pasting into a spreadsheet fills one column per field. A respondent is copied with every variable in the file, not only those the panel has room for.

Copying uses the system clipboard: `clip` on Windows, `pbcopy` on macOS, `wl-copy`, `xclip` or `xsel` on Linux. If none is available, the terminal's own clipboard sequence is used, which some terminals ignore.

### Incomplete labels

Files with incomplete labels are shown as they are, and the gaps are made visible:

- A value with no label, in a numeric variable that has labels, shows the value itself in the warning colour.
- When two codes share a label, Labels mode shows `Other [97]` and `Other [98]`.
- Values declared user-missing are dimmed and stay in the data.
- The bottom border counts the variables that hold unlabelled values.
- In Variable View the **Label check** column says `ok`, `2 unlabelled: 7, 8` or `duplicate: Other (97, 98)`.

These are the checks of `ambers.validate()` (see [validate.md](validate.md)), run on the rows loaded so far. String variables are not checked: free text in a labelled string variable is ordinary data.

### Terminal notes

- `am view` needs an interactive terminal; it refuses to start when its input or output is piped.
- The viewer captures the mouse. To select text with the mouse, hold Shift while dragging (in most terminals).
- Some terminals and multiplexers keep key combinations for themselves: VS Code's terminal keeps Ctrl+↑ / Ctrl+↓, and Herdr keeps Ctrl+← / Ctrl+→. The letter forms always work: `g` `G` `0` `$` for the edges, `{` `}` `<` `>` for 50 at a time.
- If a terminal does not report mouse drags, resize with the keys: `[` `]` `w` for a column, `-` `+` for the side panel.
- The frame keeps a small margin from the window's edge, which shows your terminal's own background; the Dracula background is painted inside the frames only. A window narrower than 60 columns or shorter than 16 lines has no margin, so a small pane loses nothing.
- `NO_COLOR` turns colour off; highlights then use bold and reverse video.
- Colours: `--theme dracula` looks the same everywhere; `--theme terminal` follows your terminal's palette.

## `am open`: the file in your browser

> In development. Today the page has a list of your data files and two views of a file. The Data View is a grid of every row and variable that scrolls smoothly on files of any size, with value labels / codes / both, a filter bar, a chooser for the variables shown, resizable columns, and selecting and copying blocks of cells. The Variable View lists every variable with its type, labels, counts and label check, and shows how the values of a variable are spread. Exporting is not there yet. A binary built without the front end shows a basic page with a file's first rows instead, and says so when it starts.

```bash
am open survey.sav                 # serve the file and open it in the browser
am open                            # list the data files of this folder to pick from
am open ~/surveys                  # the same, for another folder
am open survey.sav --no-browser    # only print the address
am open big.sav --memory 8         # load at most 8 GB of data
```

| Option | Default | Meaning |
|---|---|---|
| `--port PORT` | 2627, or the next free port after it | The port to listen on |
| `--no-browser` | off | Print the address without opening the browser |
| `--keep-alive` | off | Keep serving after the last page is closed |
| `--limit N` | none | Load at most N rows; the rest of the file is not read |
| `--memory GB` | half of the free memory | Stop loading when the data takes this much memory |
| `--check` | | Say whether this build includes the browser interface, and exit (code 0 if it does, 1 if not) |

`am open` starts a small server on your machine and prints an address like `http://127.0.0.1:2627/?token=…`. The page asks the server for the rows it shows, so a file of any size opens at once and the data never leaves the machine.

**The files page.** Without a file, the page lists the `.sav` and `.zsav` files of the folder (and its sub-folders) with their rows and variables, and remembers the files you opened and starred. Search by name, label or folder, sort by name, rows or date, and click a file to open it; "Files" at the top of a file's view comes back. The page can only list and open files in that folder, files you named on the command line, and files you opened before: it cannot look elsewhere on the disk.

**In the page.** "Data" and "Variables" at the top switch between the two views of the file; each stays as you left it (filters, selection, column widths, where you had scrolled to). In both, click a cell or move with the arrow keys; PgUp / PgDn, Home / End and Ctrl+arrows work as in a spreadsheet. The strip under the grid shows the selected cell in full. While a file loads, both views already work and the status bar shows the progress. "Keyboard shortcuts" in the status bar, or `?`, lists every key.

**Column widths.** Drag the right edge of a column's header to resize it; double-click the edge to put the column back to its own width. From the keyboard, Tab to an edge: ← and → resize by 10 pixels (50 with Shift), Home and End go to the narrowest and widest, Enter resets.

**Labels, Codes, Both.** The switch right of the filter bar chooses what cells show: value labels, the stored values, or the label with its stored value beside it as a small tag. It is greyed out for a file without value labels. Under the grid, "Stored code" says which value a label stands for.

**What a variable is.** Rest the pointer on a column's header (or click it) for a card with the variable's label, its first value labels, how it is stored and formatted, its measure, valid and missing cases, any user-missing values the file declares, and what the label check found.

**Filtering rows.** The bar above the grid holds the conditions rows must meet. Click it, or press Ctrl+K (⌘K on a Mac), and build a condition in three steps: a column (found by name or label), a condition, a value. While you type a value, the most frequent values of the column that contain it are listed with their counts; click one to take it. Each condition becomes a chip: click it to change it, × to remove it. With more than one, choose "Match all" or "Match any". The line under the bar says how many rows match.

| Condition | For | Rows kept |
|---|---|---|
| Equals, Not equal | every variable | the value is, or is not, exactly that |
| Greater than, Less than, Greater or equal, Less or equal | numbers | the value compares so with a number |
| Contains, Does not contain, Starts with, Ends with | text, labelled numbers, dates | the value, or its label, does |
| Like, iLike | text, labelled numbers, dates | the whole value fits a pattern: `%` is any text, `_` one character. Like minds upper and lower case, iLike does not |
| Is empty, Is not empty | every variable | the value is missing (system-missing, or an empty text), or is not |

- Text is compared without regard to upper and lower case, except by Like.
- A labelled number is found by its label or by its code: "Equals Female" and "Equals 2" keep the same rows, whatever the grid shows.
- A missing value matches only "Is empty". "Not equal 99" does not keep the rows with no answer; add "Is empty" with "Match any" to keep them. (`am view` differs: its typed `!=99` keeps them.) A value that the file declares user-missing, such as 99 = Refused, is a value like any other.
- Dates and times are matched as they are written (`2024-01-15`); comparing them is not there yet.
- Filters cover the rows loaded so far; while a file loads, the count grows.

**Choosing columns.** "Columns" lists the variables with a search box; untick the ones to hide, or show and hide everything the search finds at once. Hidden variables are still in the file, and can still be filtered on.

**Selecting and copying.** Drag over cells to select a block; Shift+click or Shift+arrows extend it, Ctrl+A selects everything, Esc goes back to one cell. Ctrl+C, or "Copy range" under the grid, copies the block as the grid shows it (labels, codes, or both as `Male (1)`), ready to paste into a spreadsheet. At most 50,000 cells are copied at once.

**The Variable View.** One row per variable:

| Column | Shows |
|---|---|
| Name, Label | The variable's name and label. Rest the pointer on a name for its card |
| Type | `Categorical`, `Numeric`, `Text` or `Date`, as the codebook has it. A numeric variable is categorical if it has value labels, is in a multiple-response set, or holds only 0 and 1; the type can change while a file loads |
| Values | The first value labels, and `⋯` to see every value with its count |
| N, Missing | Valid and missing cases, over the rows loaded |
| Label check | `OK`, or what is wrong with the value labels: `2 unlabelled` (values in the data without a label), `1 duplicate` (a label carried by several codes). `—` for a variable that is not checked: the check is for numeric variables with value labels |
| Format, Measure, User-missing | The print format (`F8.2`), the measurement level, and the values the file declares missing (`99`, `90 thru 99`) |

- Search finds variables by name, label or value label (Ctrl+K goes to the search box); the two lists beside it keep one type, or the variables where the label check found unlabelled values, duplicate labels or no variable label.
- Click a header to sort by that column (again to turn the order round, a third time for file order). The number at the left is the variable's position in the file, whatever the order.
- Cells are selected and copied as in the Data View; a Values cell copies as `1 = Male; 2 = Female`.
- Missing means system-missing, or an empty text. A value the file declares user-missing (99 = Refused) is a value: it counts as valid and is marked `user-missing`.
- Counts cover the rows loaded so far; the status bar says how many.

**How a variable's values are spread.** `⋯` in a Values cell opens the value distribution: valid, missing and total cases, then every code (with its label) or every distinct value with its count, its share of the valid cases and of all cases. Search the values, sort them (low to high, high to low, most frequent first), page through them 50 at a time, and "Copy table" for a spreadsheet (at most 50,000 values). A code found in the data without a label reads "Unlabelled code". For a text or number variable the values are counted when you open it, which takes a moment on a large file. While a file is still loading, the counts are of the rows loaded when you opened it; "Refresh" counts again.

**If the server has stopped.** The page says so at the top and shows the command that brings it back, with a button to copy it. The rows already on the screen stay; nothing new can be shown until ambers runs again. When it does, the page reconnects by itself, on the same file when the new server has it open.

**One server.** If ambers is already serving, a second `am open other.sav` opens that file in the running server and returns at once. A server from a different ambers version is left alone, and a new one starts on another port.

**Stopping.** Ctrl+C stops the server. It also stops by itself about three minutes after the last page is closed, unless you pass `--keep-alive`.

**The file is not kept open.** Once a file has loaded, ambers closes it. You can move, replace or delete it while its rows are still shown.

**Who can reach it.** Only you, on this machine:

- The server listens on `127.0.0.1`, never on the network.
- The address carries a random token. Without it, or the cookie the first visit trades it for, every request is refused.
- Requests that arrive under another host name are refused, so a web page elsewhere cannot reach the server through your browser.
- The page loads nothing from the internet.

## Limits

- The CLI is read-only. Writing and editing files is done from Python or Rust.
- A filter looks at one column. Stack filters for more columns; there is no search across all columns at once.
- Alternatives (`a|b`) are "or" within one column; filters on different columns are always "and". A literal `|` in a value cannot be searched for.
- No regular expressions.
- Comparisons (`>`, `<`) work on numeric variables only, not on dates or text.
- Counts in Variable View and in value tables cover all loaded rows, not the filtered ones.
- When loading stops at the memory budget or `--limit`, everything in the viewer covers the loaded rows only. `am info --counts` always reads the whole file.

## Recipes

**Is this file damaged?**

```bash
am info file.sav --head 0
```

Look at `Status`. Each warning is explained in [warnings_and_errors.md](warnings_and_errors.md).

**How many valid answers does each variable have?**

```bash
am info file.sav --counts --head 1000
```

`--head` larger than the number of variables lists them all.

**Save a report to a file.**

```bash
am info file.sav --counts --head 1000 > report.txt
```

**Find a variable by part of its name or label.** `am view file.sav`, then `i`, `/`, type the text, Enter. `d` jumps to its column.

**Look up one respondent.** Cursor on the id column, `/`, `=1042`, Enter. Enter again opens the panel with all of that respondent's answers.

**Read one country's open-ended answers.** Cursor on `country`, `/`, `jap`, Enter. Move to the open-ended column and read down; `c` copies an answer.

**Compare two groups.** Cursor on `country`, `/`, `japan|france`, Enter.

**Find the rows where a question was not answered.** Cursor on that column, `/`, `=.`, Enter.

**Copy a variable's frequency table into Excel.** `i`, move to the variable, Enter, `C`. Paste into a sheet.

**Copy one respondent into Excel.** In Data View, Enter on the row, `C`. Paste into a sheet.
