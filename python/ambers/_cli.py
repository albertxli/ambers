"""The ``ambers`` console script installed with the wheel.

It runs the same Rust code as the standalone ``ambers`` binary, so the two
print exactly the same output.
"""

import signal
import sys

from ambers._ambers import _run_cli


def main() -> None:
    # The command runs in Rust, where Python's KeyboardInterrupt handler
    # would only fire after it returns. Restore the default so Ctrl-C stops
    # the process at once, as it does for the standalone binary.
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    sys.exit(_run_cli(sys.argv))
