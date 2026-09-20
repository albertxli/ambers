"""File-damage warnings: tell the caller when a .sav file looks corrupted.

ambers always reads a file exactly as stored and never "corrects" values to
what SPSS would assume. When the file's header disagrees with its contents
(row count, compression bias, row width), the Rust reader records a message in
``SpssMetadata.warnings``; this module turns those into one Python warning so
notebooks and scripts see it immediately, and returns the list for
``SavFile.warnings``.
"""

from __future__ import annotations

import os
import warnings

from ambers._ambers import SpssMetadata


class CorruptFileWarning(UserWarning):
    """The .sav file appears damaged or corrupted.

    Its header disagrees with its contents, so the values may not be what the
    file's author stored. The data was read exactly as found in the file.
    Silence with ``warnings.filterwarnings("ignore", category=ambers.CorruptFileWarning)``.
    """


def warn_if_damaged(meta: SpssMetadata, source: str | os.PathLike | None, *, stacklevel: int = 3) -> list[str]:
    """Emit one ``CorruptFileWarning`` summarising ``meta.warnings`` and return them.

    ``stacklevel=3`` points the warning at the caller of the public ambers
    function (``warn_if_damaged`` -> ``read_sav`` -> user code).
    """
    found = list(meta.warnings)
    if found:
        name = os.path.basename(str(source)) if source else "SPSS file"
        details = "; ".join(f"({i}) {m}" for i, m in enumerate(found, 1))
        warnings.warn(
            f"{name} appears damaged or corrupted; please double-check the source. "
            f"Findings: {details}. The data was read exactly as stored in the file.",
            CorruptFileWarning,
            stacklevel=stacklevel,
        )
    return found
