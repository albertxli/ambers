"""Shared fixtures for ambers Python test suite."""

import pytest


# Default test files — override with --sav-file on the command line
DEFAULT_FILES = [
    r"C:\Users\lipov\SynologyDrive\_PMI\Multi-Wave\RPM\2025\Data\rpm_2025_data_tracking_partial_uam_2026_02_16.sav",
    r"C:\Users\lipov\SynologyDrive\_PMI\Multi-Wave\RPM\2025\Data\251001.sav",
]


def pytest_addoption(parser):
    parser.addoption(
        "--sav-file",
        action="append",
        default=None,
        help="Path to .sav file(s) for testing. Can be specified multiple times.",
    )


def pytest_generate_tests(metafunc):
    """Parametrize tests that use the sav_file fixture."""
    if "sav_file" in metafunc.fixturenames:
        files = metafunc.config.getoption("sav_file") or DEFAULT_FILES
        metafunc.parametrize("sav_file", files, ids=[f.split("\\")[-1] for f in files])


@pytest.fixture(scope="session")
def ambers_mod():
    import ambers
    return ambers


@pytest.fixture(scope="session")
def pyreadstat_mod():
    import pyreadstat
    return pyreadstat
