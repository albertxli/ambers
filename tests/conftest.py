"""Shared fixtures for ambers Python test suite."""

import pytest

from test_paths import DEFAULT_FILES


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
