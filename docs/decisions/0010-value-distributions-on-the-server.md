# 0010: A variable's value distribution is searched, sorted and paged on the server

Date: 2026-11-16
Status: accepted

## Decision

The value distribution of a variable (every code or distinct value with its count and shares) is built in `ambers-dataset` (`detail.rs`) and stays on the server. The page asks for what it shows:

`GET /api/v1/datasets/{id}/variables/{index}/values?offset=&limit=&search=&order=`

- `order` is `value`, `value_desc` or `count`; `search` keeps the values whose code or label contains the text.
- Every answer carries `totals` (valid, missing, total, values listed, labelled, unlabelled), whatever page or search was asked for. With an order, the row that counts missing cases is left out of the rows: the page pins it above them from the totals.
- For a categorical variable the rows come from the codebook's counts, which the loader keeps anyway. For a text, number or date variable the distinct values are counted when the table is first asked for, from the loaded rows of that one column, and kept for the last four variables until more rows load.
- Copying the table fetches it in parts, at most 50,000 values.

What a table holds is decided in `ambers-dataset` only, as for the terminal viewer's value panel: a user-missing value (99 = Refused) is a value and counts as valid; a code found in the data without a label is listed and marked.

## Alternatives considered

- **Build the distribution in the page**, as the design mock-up does from its 21 rows. The page does not have the rows (decision 0005), and a text variable of a real file has tens of thousands of distinct values: 79,065 for an id variable of the largest file tried, far more for open-ended answers.
- **Send the whole table once** and let the page search, sort and page it. Fine for a labelled variable with five codes; megabytes of JSON for a text variable, sent again whenever more rows have loaded.
- **Count distinct values for every variable while loading**, so that every table is ready. It slows loading for all files to serve a table that is opened for a few variables; the codebook deliberately keeps only a summary for variables without labels.
- **Send the labelled values with the list of variables and count nothing.** That is what the Variable View's Values column and the cards show, and it needs no request. But the question a distribution answers is how often each value occurs, and which codes occur without a label.

## Why

The server has the column and the counts; a request for one page of a sorted, searched table is answered in tens of milliseconds, and the page stays small whatever the variable. It is the same division as for the rows (decision 0009).

## Consequences

- Measured on a 5.4 GB file (395,330 rows), debug build: opening the distribution of a text variable with 79,065 distinct values takes 0.8 s (counting), then a sort by frequency 40 ms and a page 22 ms; a categorical variable opens in 34 ms.
- While a file loads, an open distribution shows the counts of the rows loaded when it was asked for, says how many rows have arrived since, and counts again on "Refresh": it does not recount by itself, which would restart a 0.8 s count several times a second.
- The filter bar's value suggestions (`?top=N`) use the same table and cache.
