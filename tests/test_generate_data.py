"""Smoke test for the sample data generator.

Runs the generator into a tmp dir and asserts we got the expected number
of PDFs and transcripts. Does not exercise anything in the workspace.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_generate_data(tmp_path: Path) -> None:
    out = tmp_path / "sample_data"
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "src.sample_data.generate_data",
            "--out",
            str(out),
            "--n-substations",
            "3",
            "--n-debriefs",
            "2",
        ],
        cwd=Path(__file__).resolve().parent.parent,
    )
    assert (out / "manifest.json").exists()
    onelines = list((out / "onelines").glob("*.pdf"))
    studies = list((out / "studies").glob("*.pdf"))
    debriefs = list((out / "debriefs").glob("*.txt"))
    assert len(onelines) == 3
    assert len(studies) == 3
    assert len(debriefs) == 2
    for pdf in onelines + studies:
        assert pdf.stat().st_size > 1000, f"{pdf} looks suspiciously small"
