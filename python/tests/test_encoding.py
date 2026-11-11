"""Code-page encoded files: variable names, labels and MR sets must decode with the
file's declared encoding (GitHub issue #5). Uses the reporter's windows-1250 sample when
present; the synthetic coverage lives in tests/test_encoding.rs."""

from __future__ import annotations

import os
import warnings

import pytest

import ambers as am

SAMPLE = "test_data/github_issues/issue5_mrset_parsing_panic_non_utf8_encoding.sav"
NAMES = ["KORKVÓTA", "ISKOLAKVÓTA", "RÉGIÓKVÓTA"]
MR_LABEL = "Mikor szokott leggyakrabban olvasni(őŐűŰ)?"

needs_sample = pytest.mark.skipif(not os.path.exists(SAMPLE), reason="reporter sample not present")


@needs_sample
class TestWindows1250Sample:
    def test_names_labels_and_mr_set(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # a healthy code-page file must not warn
            sav = am.read_sav(SAMPLE)
        meta = sav.meta
        assert meta.file_encoding == "windows-1250"
        assert sav.shape == (10, 50)
        for n in NAMES:
            assert n in meta.variable_names
            assert n in sav.data.columns
        assert meta.variable_labels["KORKVÓTA"] == "Korcsoport kvóta"
        mr = meta.mr_sets["Q15M"]
        assert mr["label"] == MR_LABEL
        assert len(mr["variables"]) == 10
        texts = list(meta.variable_names) + list(meta.variable_labels.values())
        texts += [l for d in meta.variable_value_labels.values() for l in d.values()]
        assert not any("�" in t for t in texts if t)

    def test_scan_and_meta_only_agree(self):
        meta = am.read_sav_meta(SAMPLE)
        assert all(n in meta.variable_names for n in NAMES)
        lf = am.scan_sav(SAMPLE)
        assert all(n in lf.data.collect_schema().names() for n in NAMES)

    def test_roundtrip_to_utf8_keeps_text(self, tmp_path):
        sav = am.read_sav(SAMPLE)
        out = tmp_path / "roundtrip.sav"
        am.write_sav(sav.data, str(out), meta=sav.meta)
        back = am.read_sav(str(out))
        assert back.meta.file_encoding == "UTF-8"
        assert back.meta.variable_names == sav.meta.variable_names
        assert back.meta.variable_labels["KORKVÓTA"] == "Korcsoport kvóta"
        assert back.meta.mr_sets["Q15M"]["label"] == MR_LABEL

    def test_matches_pyreadstat(self):
        pyreadstat = pytest.importorskip("pyreadstat")
        _, ref = pyreadstat.read_sav(SAMPLE)
        meta = am.read_sav_meta(SAMPLE)
        assert meta.variable_names == ref.column_names
        assert meta.variable_labels["KORKVÓTA"] == ref.column_names_to_labels["KORKVÓTA"]
        if getattr(ref, "mr_sets", None):
            assert meta.mr_sets["Q15M"]["label"] == ref.mr_sets["Q15M"]["label"]
