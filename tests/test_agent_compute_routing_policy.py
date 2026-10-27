from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
POLICIES = [ROOT / "AGENTS.md", ROOT / "docs" / "agent_execution_policy.md"]


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_prismabuild_admission_is_scoped_to_computational_work(path: Path):
    text = path.read_text(encoding="utf-8").lower()
    assert "gpu jobs, benchmarks, and large or long-running compute" in text
    assert "all tests and batch gpu work" not in text
    assert "all agent and subagent test execution" not in text


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_ci_uses_github_checks_on_the_integrated_sha(path: Path):
    text = path.read_text(encoding="utf-8").lower()
    assert "prismabuild is not the ci system" in text
    assert "ci and integration tests run as github checks" in text
    assert "mergify merge queue on the integrated sha" in text
    assert "never bypass required checks, reviews or branch protections" in text


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_fast_local_development_tests_are_not_the_merge_gate(path: Path):
    text = path.read_text(encoding="utf-8").lower()
    assert "run the fast relevant tests in your worktree" in text
    assert "a local run is evidence, not the merge gate" in text
