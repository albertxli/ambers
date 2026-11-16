# 0008: The browser filter sends conditions, and a missing value passes only "is empty"

Date: 2026-11-16
Status: accepted

## Decision

The browser viewer's filter bar does not send typed text. It sends a list of conditions, each a variable, one of fourteen operators (equals, not equals, four comparisons, contains, does not contain, starts with, ends with, LIKE, ILIKE, is empty, is not empty) and a value, with a mode: a row must pass all of them, or any.

The matching lives in `ambers-dataset` (`filter.rs`), beside the typed grammar of the terminal viewer and on the same code. Three rules are fixed there:

- **A missing value passes only "is empty".** System-missing, or an empty string. "Not equal 99" and "does not contain x" do not keep rows with no answer. A value the file declares user-missing (99 = Refused) is a value like any other, as everywhere in ambers.
- **A labelled number is found by its label or by its code**, by every text condition; comparisons use the code. The rows kept never depend on whether the grid shows labels or codes.
- **The variable decides which operators apply** (`filter::Kind`): comparisons for numbers, text matching for strings, both for labelled numbers, text matching on the written value for dates and times. The server sends this table to the page and refuses a condition that does not fit; the page keeps no copy of the rule.

## Alternatives considered

- **Reuse the terminal's grammar** (`>30`, `!=x`, `a|b`) and build the text in the page. Nothing new on the server, but the grammar cannot say "starts with", "ends with" or LIKE, a `|` inside a value cannot be searched for, and "match any" across variables has no place in it.
- **Keep missing rows under a negation**, as the terminal's `!=99` does ("everything but 99"). One rule in both viewers. But SPSS's `SELECT IF (q1 NE 99)` and SQL's `q1 <> 99` both drop the missing rows, and the filter bar speaks their language (LIKE, ILIKE, "is empty"). People who know either would be given rows they did not ask for. The terminal viewer is frozen and keeps its behaviour; the difference is stated in `docs/cli.md`.
- **Let the view mode decide what a labelled number is** (text in Labels, a number in Codes). The same filter would keep different rows after pressing a button, and a filter saved or shared later would mean two things.
- **Filter in the page**, as the design mock-up does with TanStack Table's row model. The mock-up has 21 rows; a real file has hundreds of thousands, which stay on the server (decision 0009).

## Why

A fixed list of operators is what a three-step builder can offer, and each one has a plain meaning that can be tested and documented. Putting the rules in `ambers-dataset` keeps one place that decides which rows a filter keeps, whoever asks: the browser today, a command or the Python API later.

## Consequences

- The typed grammar and the conditions differ on missing rows. Tests assert both, side by side.
- `POST /api/v1/datasets/{id}/rows` takes `filters: [{ variable, operator, value }]` and `filter_mode`; a condition that cannot be used is reported by its position and left out, the others still apply.
- Comparing dates (`interview_date > 2024-03-01`) is not possible yet: dates are matched as written. It needs its own design (which formats to accept).
