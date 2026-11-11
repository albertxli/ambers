"""Install-and-run smoke test for a built wheel (not collected by pytest).

Run by CI and the release gate on every OS / Python combination, against the wheel that
will be published. It exercises the native extension broadly enough that code compiled
for the wrong CPU (GitHub issue #6) is executed: import, all three compression modes,
eager and lazy reads, strings, dates, metadata.

    python tests/smoke_wheel.py
"""

import datetime as dt
import platform
import sys
import tempfile
from importlib.metadata import version
from pathlib import Path

import polars as pl

import ambers

print(f"ambers {version('ambers')} | Python {sys.version.split()[0]} | "
      f"{platform.system()} {platform.machine()} | polars {pl.__version__}")
print(f"extension: {ambers._ambers.__file__}")

n = 5_000
df = pl.DataFrame({
    "id": pl.Series(range(n), dtype=pl.Float64),
    "score": [i * 0.25 if i % 7 else None for i in range(n)],
    "city": [["Oslo", "Lima", "Pécs", "Zürich"][i % 4] for i in range(n)],
    "born": [dt.date(1950, 1, 1) + dt.timedelta(days=i) for i in range(n)],
})

with tempfile.TemporaryDirectory() as tmp:
    for name, compression in [("raw.sav", "uncompressed"), ("bc.sav", "bytecode"), ("z.zsav", "zlib")]:
        path = str(Path(tmp, name))
        ambers.write_sav(df, path, compression=compression)

        sav = ambers.read_sav(path)
        out, meta = sav.data, sav.meta
        assert out.shape == df.shape, (name, out.shape)
        assert out.equals(df), f"{name}: eager roundtrip differs"
        assert meta.compression == compression, (name, meta.compression)
        assert meta.number_rows == n

        lazy = ambers.scan_sav(path).data
        part = lazy.select("city", "score").filter(pl.col("city") == "Pécs").head(100).collect()
        assert part.height == 100 and part["city"].unique().to_list() == ["Pécs"], name

        assert ambers.read_sav_meta(path).variable_names == df.columns
        print(f"  {name:8} ok ({compression})")

print("SMOKE OK")
