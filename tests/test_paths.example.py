"""Private test file paths — NOT checked into git.

Copy this file to test_paths.py and fill in your own paths.
Place .sav files in the test_data/ directory (also gitignored).
"""

import os

_TEST_DATA = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_data')

# Default files for pytest (conftest.py)
DEFAULT_FILES = [
    os.path.join(_TEST_DATA, 'file1.sav'),
    os.path.join(_TEST_DATA, 'file2.sav'),
]

# Default file for bench_read.py
BENCH_READ_FILE = os.path.join(_TEST_DATA, 'file1.sav')

# Files for bench_v021.py: (label, filename_in_test_data, description)
BENCH_V021_FILES = [
    ('test_1', 'file1.sav', 'description of file'),
]
