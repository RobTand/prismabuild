"""Submission Git waits for exit, even beyond the former per-call caps."""
import subprocess
from types import SimpleNamespace

import pytest

from prismabuild import core, submission


@pytest.fixture
def checkout(tmp_path):
    root = tmp_path / "checkout"
    root.mkdir()
    (root / "tracked").write_text("payload\n")
    for args in (("init", "-q"), ("add", "tracked"),
                 ("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                  "commit", "-qm", "initial")):
        subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)
    return root


@pytest.mark.parametrize("verb", ("rev-parse", "diff-index", "add", "commit-tree", "pack-objects"))
@pytest.mark.parametrize("bounded", (False, True))
def test_slow_local_git_reaches_exit(monkeypatch, tmp_path, checkout, verb, bounded):
    run = subprocess.run
    seen = []
    now = 100.0
    deadline = 10000.0 if bounded else None
    monkeypatch.setattr(core, "time", SimpleNamespace(monotonic=lambda: now))

    def slow_git(argv, **kwargs):
        # Model a process exiting after 1,801 seconds without sleeping. The
        # subprocess seam enforces any timeout exactly as subprocess.run does.
        nonlocal now
        if argv[0] == "git":
            timeout = kwargs.get("timeout")
            assert timeout == (deadline - now if bounded else None)
            elapsed = 1801 if verb in argv and not seen else 1
            if timeout is not None and timeout < elapsed:
                raise subprocess.TimeoutExpired(argv, timeout)
            now += elapsed
            if verb in argv:
                seen.append(tuple(argv))
        return run(argv, **kwargs)

    monkeypatch.setattr(subprocess, "run", slow_git)
    if verb in {"rev-parse", "diff-index"}:
        assert core.git_checkout_identity(checkout, deadline=deadline)["head"] != "no-git"
    else:
        cas = core.PrismaBuildCAS(tmp_path / "cas")
        snapshot = submission.snapshot_checkout(checkout, cas=cas, deadline=deadline)
        assert snapshot["parent"] != "no-git"
    assert seen
    assert now >= 1901


@pytest.mark.parametrize("verb", ("diff-index", "add", "pack-objects"))
def test_caller_deadline_refuses_before_snapshot_publication(monkeypatch, tmp_path, checkout, verb):
    run = subprocess.run
    now = 0.0
    monkeypatch.setattr(core, "time", SimpleNamespace(monotonic=lambda: now))
    cas = core.PrismaBuildCAS(tmp_path / "cas")
    monkeypatch.setattr(cas, "ingest_input", lambda *a, **kw: pytest.fail("expired snapshot ingested"))

    def deadline_git(argv, **kwargs):
        nonlocal now
        if argv[0] == "git":
            assert kwargs["timeout"] == 100 - now
            if verb in argv:
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            now += 1
        return run(argv, **kwargs)

    monkeypatch.setattr(subprocess, "run", deadline_git)
    with pytest.raises(submission.SubmissionRefused, match="timed out after"):
        submission.snapshot_checkout(checkout, cas=cas, deadline=100)


def test_pbtest_passes_shard_deadline_to_preparation(monkeypatch):
    from contextlib import contextmanager
    from test_pbtest import pbtest
    from prismabuild.submission_types import SubmissionRequest

    captured = []

    @contextmanager
    def prepared(request, *, store, display, deadline):
        captured.append(deadline)
        yield {"template": {"params": {"command": list(request.command)}}, "cwd": "."}

    monkeypatch.setattr(submission, "preparation", prepared)
    request = SubmissionRequest(command=("python", "test.py"))
    templates = pbtest._Templates(None)
    templates.sealing("plan", request, 0, request, shared=True, deadline=456)
    assert captured == [456]
