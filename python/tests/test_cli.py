"""The ``ambers`` console script shipped in the wheel.

It calls the same Rust code as the standalone binary; the output itself is
covered by the snapshot tests in crates/ambers-cli/tests/cli.rs. These tests
check the Python wiring: arguments in, exit code out, streams untouched.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def run_cli(*args: str) -> subprocess.CompletedProcess:
    code = "import sys; from ambers._cli import main; sys.argv = ['ambers', *sys.argv[1:]]; main()"
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def test_info_prints_a_summary():
    result = run_cli("info", "tests/data/survey.sav")
    assert result.returncode == 0
    assert result.stderr == ""
    assert "File:         tests/data/survey.sav" in result.stdout
    assert "Rows:         20" in result.stdout
    assert "Columns:      13" in result.stdout


def test_head_prints_rows():
    result = run_cli("info", "tests/data/survey.zsav", "--head", "2")
    assert result.returncode == 0
    assert "First 2 rows (first 8 of 13 columns):" in result.stdout
    assert "Lisbon" in result.stdout


def test_missing_file_exits_1_on_stderr():
    result = run_cli("info", "tests/data/does_not_exist.sav")
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("error: cannot read tests/data/does_not_exist.sav: ")


def test_usage_error_exits_2():
    result = run_cli("bogus")
    assert result.returncode == 2
    assert result.stdout == ""
    assert "Usage: ambers <COMMAND>" in result.stderr
