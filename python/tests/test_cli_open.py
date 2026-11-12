"""``ambers open`` through the console script shipped in the wheel.

The command itself is Rust and is tested there. These tests check what
only the wheel can get wrong: that the server stops on Ctrl+C although it
runs inside a Python process, and that it stops by itself when no page is
in touch. Each run has its own state folder and never opens a browser.
"""

import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
WINDOWS = sys.platform == "win32"


def start_open(state: Path, *args: str) -> subprocess.Popen:
    """Start ``ambers open …`` as the console script does."""
    code = "import sys; from ambers._cli import main; sys.argv = ['ambers', *sys.argv[1:]]; main()"
    return subprocess.Popen(
        [sys.executable, "-c", code, "open", *args, "--no-browser", "--port", "0"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env={**os.environ, "AMBERS_STATE_DIR": str(state)},
        # On Windows a console signal goes to a whole process group: give
        # the server its own, so the signal does not reach pytest.
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if WINDOWS else 0,
    )


def read_until_serving(process: subprocess.Popen) -> str:
    """What the server prints before it starts waiting."""
    printed = ""
    while "Ctrl+C" not in printed:
        line = process.stdout.readline()
        assert line, f"the server ended early: {printed}{process.stderr.read()}"
        printed += line
    return printed


def stop(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.kill()
    process.wait(timeout=20)


def test_open_stops_on_ctrl_c(tmp_path):
    process = start_open(tmp_path, "tests/data/survey.sav", "--keep-alive")
    try:
        printed = read_until_serving(process)
        assert "is serving on this machine only" in printed
        assert (tmp_path / "server.json").exists()

        # Ctrl+C. A test cannot send that to another process group on
        # Windows; Ctrl+Break can be sent, and the server treats it the same.
        try:
            process.send_signal(signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT)
        except OSError as error:
            pytest.skip(f"this environment cannot deliver a console signal: {error}")
        assert process.wait(timeout=30) == 0, process.stderr.read()
        # It stopped in an orderly way: Rust handled the signal and took
        # its record with it. A process that was simply killed leaves it.
        assert not (tmp_path / "server.json").exists()
    finally:
        stop(process)


def test_open_stops_when_no_page_is_in_touch(tmp_path):
    process = start_open(tmp_path, "tests/data/survey.sav", "--idle-seconds", "0.5")
    try:
        assert process.wait(timeout=60) == 0, process.stderr.read()
        assert "is serving on this machine only" in process.stdout.read()
        assert not (tmp_path / "server.json").exists()
    finally:
        stop(process)


def test_open_reports_an_unreadable_file(tmp_path):
    process = start_open(tmp_path, "tests/data/does_not_exist.sav")
    try:
        assert process.wait(timeout=60) == 1
        assert process.stdout.read() == ""
        assert "│ Status │ error" in process.stderr.read()
    finally:
        stop(process)
