"""Record what ``am.codebook()`` returns, as the reference for the Rust port.

The Rust codebook (``crates/ambers-codebook``) must give the same types and
counts as the Python implementation. This script runs the Python one and
writes its output as JSON; the Rust test
``crates/ambers-codebook/tests/parity.rs`` compares against it.

Usage:
    uv run python/tests/make_codebook_expected.py            # the committed fixtures in tests/data/
    uv run python/tests/make_codebook_expected.py --private  # also test_data/ files (gitignored output)

Run it again whenever a fixture or the Python codebook changes, and review
the diff: a change here is a change in what the codebook reports.
"""

import json
import sys
from pathlib import Path

import ambers as am
from ambers.codebook._types import detect_types

ROOT = Path(__file__).resolve().parent.parent.parent
# Private files worth checking locally: real surveys, one with MR sets.
PRIVATE = ["test_1_small.sav", "data500-mrsets.sav"]


def expected(path: Path) -> dict:
    sav = am.read_sav(path)
    df, meta = sav.data, sav.meta
    variables = am.codebook(df, meta, view="variables", values_format="struct")
    values = am.codebook(df, meta, view="values")
    return {
        "file": path.name,
        "rows": df.height,
        # The five-way type; the codebook's own column merges the two selects.
        "internal_types": detect_types(df, meta),
        "variables": variables.to_dicts(),
        "values": values.to_dicts(),
    }


def write(source_dir: Path, names: list[str]) -> None:
    out = source_dir / "expected"
    out.mkdir(exist_ok=True)
    for name in names:
        path = source_dir / name
        if not path.exists():
            print(f"skipped (not found): {path}")
            continue
        target = out / f"{name}.codebook.json"
        text = json.dumps(expected(path), indent=1, ensure_ascii=False, default=str)
        target.write_text(text + "\n", encoding="utf-8", newline="\n")
        print(f"wrote {target.relative_to(ROOT)}")


def main() -> None:
    fixtures = ROOT / "tests" / "data"
    write(fixtures, sorted(p.name for p in fixtures.glob("*sav")))
    if "--private" in sys.argv[1:]:
        write(ROOT / "test_data", PRIVATE)


if __name__ == "__main__":
    main()
