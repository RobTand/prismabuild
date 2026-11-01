from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
POLICIES = [ROOT / "AGENTS.md", ROOT / "docs" / "agent_execution_policy.md"]


def policy_text(path: Path) -> str:
    """The policy as one lowercase line, so a wrapped phrase still matches."""
    return " ".join(path.read_text(encoding="utf-8").lower().split())


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_remote_tests_may_skip_prismabuild_but_fleet_work_may_not(path: Path):
    # Rob's routing policy (2026-09-05), amended 2026-10-29 and 2026-11-01:
    # tests on free remote compute need no PrismaBuild; fleet machines still do.
    text = policy_text(path)
    assert "amended 2026-10-29 and 2026-11-01" in text
    assert "tests that run on free remote compute" in text
    assert "buildbuddy" in text and "github-hosted ci" in text
    assert "reserved for exploratory work" in text
    assert "work that runs on a fleet machine still goes through prismabuild" in text
    assert "do not run test suites directly on fleet hosts outside it" in text
    assert "read the run's own verdict and logs" in text
    assert "agents and subagents run their tests and computational work through prismabuild" not in text
    assert "run the fast relevant tests in your worktree" not in text
    assert "a local run is evidence" not in text
    # The coordinator may inspect, edit, use Git and submit; it runs no tests.
    assert "development tests and submission commands may run on the coordinator" not in text
    assert "fast relevant development tests" not in text


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_ci_runs_on_github_hosted_runners_in_the_mergify_queue(path: Path):
    # Rob, 2026-10-31: CircleCI is out; CI is GitHub Actions on GitHub-hosted
    # runners, free for this public repository, and off the fleet.
    text = policy_text(path)
    assert "prismabuild is not the ci system" in text
    assert "ci runs as github actions checks on github-hosted runners" in text
    assert "mergify queue, which gates on those checks of the integrated sha" in text
    assert "do not route ci through prismabuild" in text
    assert "circleci" not in text
    assert "never bypass required checks, reviews or branch protections" in text


@pytest.mark.parametrize("path", POLICIES, ids=["agents", "execution-policy"])
def test_a_prismabuild_run_is_development_evidence_not_the_merge_gate(path: Path):
    text = policy_text(path)
    assert "a prismabuild run is development evidence, not the merge gate" in text
    assert "merge only through the mergify queue" in text


def test_the_skill_follows_the_amended_routing():
    # The skill told agents to run fast tests in their worktree, which on a
    # fleet host bypasses PrismaBuild (Rob's routing policy, amended 2026-11-01).
    text = policy_text(ROOT / "skills" / "prismabuild" / "SKILL.md")
    assert "run the fast relevant tests in your worktree" not in text
    assert "tests run in your worktree" not in text
    assert "a local run is evidence" not in text
    assert "free remote compute" in text
    assert "never run test suites directly on a fleet host outside prismabuild" in text


def test_later_passages_keep_the_remote_exception():
    policy = policy_text(ROOT / "docs" / "agent_execution_policy.md")
    assert "; tests run through prismabuild." not in policy
    assert "and so do development tests, as the start of this policy says" not in policy
    operating = policy_text(ROOT / "docs" / "operating_prismabuild.md")
    assert "five times per architecture through `pbtest` before handoff" not in operating
    assert "github-hosted runners of both architectures" in operating
    assert "buildbuddy" in operating
