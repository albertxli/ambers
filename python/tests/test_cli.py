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
    assert "│ File        │ tests/data/survey.sav" in result.stdout
    assert "│ Shape       │ 20 rows × 13 columns" in result.stdout
    assert "│ Status      │ ok" in result.stdout
    # Captured output is not a terminal: no colour codes.
    assert "\x1b" not in result.stdout


def test_head_and_tail_choose_variables():
    result = run_cli("info", "tests/data/survey.sav", "--head", "1", "--tail", "1")
    assert result.returncode == 0
    assert "┆ id " in result.stdout
    assert "┆ weight " in result.stdout
    assert "┆ gender " not in result.stdout


def test_rows_prints_data():
    result = run_cli("info", "tests/data/survey.zsav", "--rows", "2")
    assert result.returncode == 0
    assert "Lisbon" in result.stdout
    assert "8 of 13 columns shown" in result.stdout


def test_missing_file_exits_1_on_stderr():
    result = run_cli("info", "tests/data/does_not_exist.sav")
    assert result.returncode == 1
    assert result.stdout == ""
    assert "│ File   │ tests/data/does_not_exist.sav" in result.stderr
    assert "│ Status │ error" in result.stderr


def test_usage_error_exits_2():
    result = run_cli("bogus")
    assert result.returncode == 2
    assert result.stdout == ""
    assert "Usage: ambers <COMMAND>" in result.stderr
