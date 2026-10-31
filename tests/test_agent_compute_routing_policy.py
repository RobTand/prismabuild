from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
POLICIES = [ROOT / "AGENTS.md", ROOT / "docs" / "agent_execution_policy.md"]


def policy_text(path: Path) -> str:
    """The policy as one lowercase line, so a wrapped phrase still matches."""
    return " ".join(path.read_text(encoding="utf-8").lower().split())


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_agents_route_their_tests_through_prismabuild(path: Path):
    # Rob's routing policy (2026-09-05), amended 2026-10-29: development
    # tests and trials still go through PrismaBuild; only CI does not.
    text = policy_text(path)
    assert "agents and subagents run their tests and computational work through prismabuild" in text
    assert "gpu jobs, benchmarks, and large or long-running compute" in text
    assert "run the fast relevant tests in your worktree" not in text
    assert "a local run is evidence" not in text
    # The coordinator may inspect, edit, use Git and submit; it runs no tests.
    assert "development tests and submission commands may run on the coordinator" not in text
    assert "fast relevant development tests" not in text


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_ci_runs_on_circleci_in_the_mergify_queue(path: Path):
    text = policy_text(path)
    assert "prismabuild is not the ci system" in text
    assert "ci runs on circleci" in text
    assert "mergify queue, which gates on circleci's checks of the integrated sha" in text
    assert "do not route ci through prismabuild" in text
    assert "ci and integration tests run as github checks" not in text
    assert "never bypass required checks, reviews or branch protections" in text


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_a_prismabuild_run_is_development_evidence_not_the_merge_gate(path: Path):
    text = policy_text(path)
    assert "a prismabuild run is development evidence, not the merge gate" in text
    assert "merge only through the mergify queue" in text
