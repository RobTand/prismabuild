"""Every published policy source must state the same vLLM exemption.

They drifted once: the skill was narrowed to inference serving only while
`docs/agent_execution_policy.md` still carried the universal ruling, so the two
published sources disagreed about whether a benchmark against a live endpoint
submits. An agent reads whichever it reaches first, so the drift routed the
same work two ways depending on which file was open. The exemption is Rob's
ruling, not a capability claim: a bounded action that starts vLLM can run to
completion under PrismaBuild (action ``a475cb59`` did) and is exempt all the
same.

This is a prose test on purpose, and it is narrow on purpose: it does not check
wording, it checks that neither document has quietly lost the two claims that
distinguish the ruling from its plausible narrowing. Changing the ruling means
changing both documents and this test, which is the intent.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "prismabuild" / "SKILL.md"
POLICY = ROOT / "docs" / "agent_execution_policy.md"
#: ``AGENTS.md`` is the source an agent reaches FIRST in this repo, and it
#: carried the routing rule as an absolute while the other two carried the
#: exemption.  A binding that covers the documents an agent reads second is
#: not a binding on what it does.
AGENTS = ROOT / "AGENTS.md"
SOURCES = [SKILL, POLICY, AGENTS]
IDS = ["skill", "policy", "agents"]


def _vllm_text(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    lowered = text.lower()
    assert "vllm" in lowered, f"{path} names no vLLM exemption at all"
    return lowered


@pytest.mark.parametrize("path", SOURCES, ids=IDS)
def test_the_exemption_is_stated_as_universal(path: Path):
    text = _vllm_text(path)
    assert "universally" in text, (
        f"{path.relative_to(ROOT)} no longer states the vLLM exemption as "
        "universal. Rob, 2026-09-07: 'vllm is exempt. It can't run in "
        "prismabuild. This is not complex don't make it so.'")


@pytest.mark.parametrize("path", SOURCES, ids=IDS)
def test_a_benchmark_against_a_live_endpoint_is_named_as_exempt(path: Path):
    """The clause the narrowing removed, and the one that decides real cases.

    'Running the service is exempt' and 'everything that runs it is exempt'
    read alike until someone has a benchmark to route, which is when the two
    documents have to already agree.
    """
    text = _vllm_text(path)
    assert "benchmark against a live endpoint" in text, (
        f"{path.relative_to(ROOT)} dropped the live-endpoint benchmark from "
        "the exemption; that is the case the two documents disagreed on.")


def test_the_external_load_qualifier_survives_in_both():
    """Exempt from submission is not invisible to admission."""
    for path in SOURCES:
        assert "external load" in _vllm_text(path), (
            f"{path.relative_to(ROOT)} dropped the external-load qualifier; "
            "an exempt serve still consumes CPU, memory and GPU no "
            "reservation accounts for.")
