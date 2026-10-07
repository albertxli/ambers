# Warnings and Errors

What ambers tells you when a `.sav` / `.zsav` file is not what it claims to be, and what to do
about it.

## The rule

ambers reads a file exactly as stored and never silently "corrects" values. When something in
the file is wrong, one of two things happens:

- **Warning, data returned.** The file contradicts itself, but a more reliable part of the file
  (usually the variable list or the data itself) says what the bytes mean. ambers reads from
  that source and reports the contradiction as a *finding*. SPSS behaves the same way on these
  files: it ignores the header fields it does not trust.
- **Error, nothing returned.** The corruption sits in the structure the reader needs to navigate
  the file, and there is no second source to fall back on. Guessing would not give "some
  results", it would give wrong results that look right. Such a file cannot be read by SPSS
  either. Get a fresh export from the source.

A warning never changes the data. The values you get are the values in the file.

## How warnings reach you

**Python.** `read_sav`, `scan_sav` and `read_sav_meta` emit one `ambers.CorruptFileWarning`
per call, listing every finding:

```
CorruptFileWarning: survey.sav appears damaged or corrupted; please double-check the source.
Findings: (1) header declares 0 rows but 5 rows were read; (2) compression bias in the header
is 9.597803938502254e-308 instead of the standard 100; compressed integer values may be
shifted by 100 (SPSS reads such a file assuming 100). The data was read exactly as stored in
the file.
```

The findings are also available programmatically:

```python
sav = am.read_sav("survey.sav")
sav.warnings            # list[str], empty for a healthy file
sav.meta.warnings       # same list on the metadata
meta.schema["warnings"] # included in the schema dict
```

The `SavFile` repr shows a `Warnings` line, and `meta.summary()` prints the findings.
`scan_sav` reports header-level findings only; the row-count finding needs the data and is
produced by `read_sav`. A row-count finding is skipped when you pass `n_rows=`, since a
mismatch is then expected.

To silence the warning (the findings stay in `sav.warnings`):

```python
import warnings
warnings.filterwarnings("ignore", category=am.CorruptFileWarning)
```

To treat any damaged file as fatal in a pipeline:

```python
warnings.simplefilter("error", am.CorruptFileWarning)
```

**Rust.** The same strings are in `SpssMetadata::warnings`. `read_sav` returns metadata cloned
after the data was read, so it includes data-time findings; `SavScanner::metadata()` carries
them once the scanner has been drained.

## Warning catalogue

| Message | What it means | What ambers did | What to do |
|---|---|---|---|
| `header declares N values per row but the variable list defines M; used M` | The header's row width disagrees with the variable records. SPSS ignores this header field. | Used the variable list. Data is read correctly. | Nothing, unless other findings appear too. |
| `header does not declare the number of values per row (case size V); used the M defined by the variable list` | The header holds 0 or -1 for the row width. Some writers do this. | Used the variable list. | Nothing. |
| `header declares N rows but M rows were read` | The header's case count disagrees with the rows actually present. | Read every row in the data section, as SPSS does. `meta.number_rows` still reports the header value; `sav.shape` is the truth. | If M is far from N the file may be truncated or padded; compare with the source. |
| `compression bias in the header is B instead of the standard 100; compressed integer values may be shifted by D (SPSS reads such a file assuming 100)` | Compressed files store small integers as `code - bias`; every real file has bias 100. A different value shifts every compressed integer. | Decoded with the bias in the file, literally. SPSS would show values shifted by D. | Treat numeric columns with suspicion; value labels will not match the data. Re-export the file. |
| `zsav trailer declares a compression bias of B instead of the standard 100` | The `.zsav` block trailer carries its own copy of the bias (written as +100 or -100 depending on the tool). | Nothing; decoding uses the main header. | Usually accompanies other damage. |
| `N date/time values were outside the representable range (years 1-9999) and were set to null` | Date, datetime or time cells held values no calendar can represent (SPSS itself cannot store dates outside 1582-9999). Stored as-is they make Polars panic on display. | Set those cells to null. All other cells unchanged. | Check the source for garbage dates; nulls mark the affected rows. |
| `variable record I ("NAME") declares type T; treated as a continuation slot` | A variable record's type field is a negative number other than -1. SPSS opens such files. | Treated the record as a continuation slot of a wide string (no column produced). | Compare the column list with the source. |
| `very long string record declares width W for "NAME"; ignored (valid range 256-32767)` | The very-long-string declaration for a variable is impossible. | Ignored the declaration; the variable keeps the width of its variable record (at most 255). | Long text in that column may be cut at 255 bytes; re-export. |
| `very long string record declares width W for "NAME" needing S segments but only A variables remain; ignored` | Same declaration, but it would need more variables than the file has. | Ignored the declaration. | As above. |

## Error catalogue

In Python every reader error is an `OSError`; in Rust it is an `SpssError` variant. The message
starts with the category.

| Message | What it means | What to do |
|---|---|---|
| `invalid magic number: expected "$FL2" or "$FL3", found ...` | Not a `.sav` / `.zsav` file (or the first bytes are destroyed). | Check the file; a `.por` or a renamed CSV will fail here. |
| `unsupported compression type: N` | The header's compression code is not 0 (none), 1 (bytecode) or 2 (zlib). | File is damaged; re-export. |
| `invalid dictionary: dictionary defines no variables` | No variable records before the end of the dictionary. | Re-export. |
| `invalid dictionary: duplicate variable name "x"; SPSS also rejects this file` | Two variables resolve to the same name. A DataFrame cannot hold two columns with one name, and renaming one would invent data. | Re-export; if the file was produced by a script, fix the names at the source. |
| `invalid variable record: variable "X" declares type T; valid values are 0 (numeric), 1-255 (string width) or -1 (continuation)` | A variable record's type field is impossible (for example a string width of 905,969,664). Everything after it would be read from the wrong bytes. | Re-export. |
| `invalid variable record: variable record declares N missing values (format allows at most 3)` | A variable claims more missing values than the format can hold; the records after it cannot be located. | Re-export. |
| `invalid variable record: column not found: "x"` | You passed a `columns=` name that is not in the file. | Check `meta.variable_names`. |
| `invalid variable record: cannot determine endianness from layout_code bytes: ...` | The header's layout code is not one of the known values. | File is damaged; re-export. |
| `invalid value label record: ...` | A value-label record is malformed (missing partner record, zero variables, bad variable index). | Re-export. |
| `unexpected record type N at offset 0` | The dictionary contains a record type the format does not define. | Re-export. |
| `truncated file: expected N bytes, got M` | A record claims more bytes than the whole file holds, or the file ends in the middle of a record. | The file is cut short or damaged; obtain a complete copy. |
| `I/O error: failed to fill whole buffer` | The file ended while a record was being read. | As above. |
| `zlib decompression failed: ...` | A `.zsav` block could not be inflated. | Damaged compression block; re-export. |
| `encoding error: ...` | Text could not be decoded with the file's declared encoding. | Report it with a sample file; ambers normally decodes leniently. |
| `internal error: output buffer too small ...` | A safety check that should never trigger from a file. | Please report it as a bug with the file. |

## Why a file with an absurd header can read while another errors

Severity is not the criterion; *recoverability* is. A header that claims 1.9 billion values
per row reads fine, because the variable list overrides it. A file with two variables named
`mydate` fails, because nothing in the file says which one is which. The first is a wrong
claim about a readable structure; the second is an unreadable structure.
