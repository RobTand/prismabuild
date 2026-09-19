"""The design says the relay slides on both tiers, in the same commit.

The two-tier streaming relay is HDD -> SSD -> RAM: the hard disks fill the
SSD while the reader reads it, and the SSD promotes to the tmpfs while the
reader reads that.  Since #673 only the second leg refills as it frees; the
SSD leg still sawtooths a whole phase at a time.  This change cuts the
stage leg into the same chunk family -- the stage record announces the
same ``promotion_chunk_gib``, the sealer seals ``stage_chunks`` beside the
phase's own rows, and the stage window publishes and evicts per chunk --
so every tier refills as it frees.

The doc follows the code in the same commit (repo rule, and
``test_design_doc_line_references`` keeps quoted lines honest): these pins
fail if the relay paragraphs are dropped from ``docs/design.md``.
"""
from __future__ import annotations

from pathlib import Path

DOC = (Path(__file__).resolve().parents[1] / "docs" / "design.md").read_text()


def test_the_design_names_the_two_tier_sliding_relay() -> None:
    assert "every tier refills as it frees" in DOC


def test_the_design_names_the_stage_chunk_announcement() -> None:
    assert "promotion_chunk_gib" in DOC
    assert "stage_chunks" in DOC


def test_the_design_names_the_one_chunk_family() -> None:
    assert "one chunk family across tiers" in DOC
