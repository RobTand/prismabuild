"""Private CPU-only publisher IPC and authority fixtures; no live fleet access."""
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

import pytest
import pbcanary
import pbcanary_submission as handoff
import publish_runtime
from prismabuild import core as pb, publication_canary as slots
from test_publication_canary_slot import _seal, GENERATION


def _stub(tmp_path, body):
    script = tmp_path / "pbrun.py"
    script.write_text("import json, os\nfrom pathlib import Path\n" + body)
    return [sys.executable, str(script)]


def test_handoff_authorizes_exact_payload_in_parent_before_publication(tmp_path):
    published = tmp_path / "published"
    argv = _stub(tmp_path, f'''
def main(*, publication_canary_intent, authorize_canary):
    action = {{"params": {{"publication_canary": publication_canary_intent}}, "pid": os.getpid()}}
    authorize_canary(action)
    Path({str(published)!r}).write_text("published")
    print(json.dumps({{"action_key": "a" * 64}}))
    return 0
''')
    seen = []
    def authorize(action, *, deadline):
        assert deadline > time.monotonic()
        assert not published.exists()
        assert action["pid"] != os.getpid()
        seen.append((os.getpid(), action["params"]["publication_canary"]))
    intent = {"generation": GENERATION, "host": socket.gethostname()}
    result = handoff.submit(argv, intent=intent, authorize=authorize, timeout_s=5)
    assert result.returncode == 0
    assert seen == [(os.getpid(), intent)]
    assert published.exists()


def test_refused_authorization_cannot_reach_publication(tmp_path):
    published = tmp_path / "published"
    argv = _stub(tmp_path, f'''
def main(*, publication_canary_intent, authorize_canary):
    authorize_canary({{"untrusted": True}})
    Path({str(published)!r}).write_text("must not publish")
    return 0
''')
    def refuse(_, *, deadline):
        raise ValueError("no publisher authority")
    with pytest.raises(ValueError, match="no publisher authority"):
        handoff.submit(argv, intent={}, authorize=refuse, timeout_s=5)
    assert not published.exists()


def test_preparation_refusal_preserves_pbrun_diagnostic(tmp_path):
    argv = _stub(tmp_path, '''
def main(**kwargs):
    print("pbrun: unsupported worker capability", file=__import__("sys").stderr)
    return 2
''')
    result = handoff.submit(argv, intent={}, authorize=lambda _: pytest.fail("not sealed"),
                            timeout_s=5)
    assert result.returncode == 2
    assert "unsupported worker capability" in result.stderr


def test_handoff_has_a_hard_submission_deadline(tmp_path):
    argv = _stub(tmp_path, '''
def main(**kwargs):
    __import__("time").sleep(20)
''')
    with pytest.raises(subprocess.TimeoutExpired):
        handoff.submit(argv, intent={}, authorize=lambda _: None, timeout_s=0.1)


def test_handoff_rejects_success_without_authorization(tmp_path):
    argv = _stub(tmp_path, "def main(**kwargs): return 0\n")
    with pytest.raises(ValueError, match="without authorization"):
        handoff.submit(argv, intent={}, authorize=lambda _: None, timeout_s=5)


def _publisher_action(tmp_path):
    action, _, _ = _seal(tmp_path, "publisher")
    body = copy.deepcopy(action)
    body.pop("action_key")
    body["environment"]["variables"].update({
        "PBCANARY_LEG": "leg-2", "PBCANARY_RUN_ID": "private-run",
        "PBCANARY_GENERATION": GENERATION})
    return pb.seal_action(body)


def test_real_publisher_grant_requires_owned_lock_and_is_single_use(tmp_path, monkeypatch):
    action = _publisher_action(tmp_path)
    monkeypatch.setattr(publish_runtime, "MIRROR", tmp_path / "repo")
    def driver(*, generation, publication_canary_authorizer, report):
        publication_canary_authorizer(action, run_id="private-run",
                                      deadline=time.monotonic() + 5)
        return 0
    monkeypatch.setattr(publish_runtime, "_load_canary_driver", lambda: driver)
    with pytest.raises(publish_runtime._CanaryPrecondition, match="owned publication lock"):
        publish_runtime._invoke_canary_driver(GENERATION)
    root = tmp_path / "pb-queue"
    assert not (root / "publication-canaries").exists()
    with publish_runtime._publication_lock():
        assert publish_runtime._invoke_canary_driver(GENERATION)[0] == 0
        path = slots.slot_path(root, slots.intent(action))
        original = path.read_bytes()
        with pytest.raises(publish_runtime._CanaryPrecondition, match="already exists"):
            publish_runtime._invoke_canary_driver(GENERATION)
        assert path.read_bytes() == original


def test_forked_depth_is_not_publisher_ownership(tmp_path, monkeypatch):
    monkeypatch.setattr(publish_runtime, "_PUBLISH_LOCK_DEPTH", 1)
    monkeypatch.setattr(publish_runtime, "_PUBLISH_LOCK_PID", -1)
    with pytest.raises(SystemExit, match="fork cannot inherit"):
        with publish_runtime._publication_lock():
            pytest.fail("must not enter")


def test_slot_submission_has_generation_fence_and_execution_deadline(tmp_path, monkeypatch):
    source = Path(__file__).resolve().parents[1] / "src"
    seen = []
    def submit(argv, *, intent, authorize, timeout_s):
        seen.append((argv, intent, timeout_s))
        return subprocess.CompletedProcess(argv, 0, json.dumps({"action_key": "a" * 64}), "")
    monkeypatch.setattr(handoff, "submit", submit)
    paths = {"pbrun": tmp_path / "pbrun.py", "published_src": source,
             "publication_canary_authorizer": lambda *a, **k: None}
    pbcanary.submit_leg(paths, {"name": "leg-2", "argv": ["true"],
                               "demand": {"gpu": 1, "cpu": 1, "mem_gb": 8}},
                       tmp_path, "private-run", -10, GENERATION)
    argv, intent, timeout = seen[0]
    assert intent == {"generation": GENERATION, "host": socket.gethostname()}
    assert slots.CAPABILITY in argv and f"runtime-generation:{GENERATION}" in argv
    assert argv[argv.index("--timeout-s") + 1] == "32"
    assert "--exclusive" in argv and timeout == 120


def test_standalone_canary_cannot_reach_authorizing_handoff(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(handoff, "submit", lambda *a, **k: pytest.fail("not a publisher"))
    def ordinary(argv, **kw):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, json.dumps({"action_key": "b" * 64}), "")
    monkeypatch.setattr(pbcanary, "run_process", ordinary)
    pbcanary.submit_leg({"pbrun": tmp_path / "pbrun.py"},
                       {"name": "leg-2", "argv": ["true"]},
                       tmp_path, "standalone", -10, GENERATION)
    assert slots.CAPABILITY not in calls[0]
