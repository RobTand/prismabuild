"""A slow publisher-side filesystem mint cannot defeat submission's deadline."""
import json
from pathlib import Path
import time

import pytest
import pbcanary
import pbcanary_submission as handoff
import publish_runtime
from prismabuild import publication_canary as slots
from test_publication_canary_handoff import _publisher_action, _stub
from test_publication_canary_slot import GENERATION


def test_shared_filesystem_mint_stall_is_bounded_before_ready(tmp_path, monkeypatch):
    action = _publisher_action(tmp_path)
    entered = tmp_path / "mint-entered"
    published = tmp_path / "ready-marker"
    argv = _stub(tmp_path, f'''
def main(*, publication_canary_intent, authorize_canary):
    authorize_canary(json.loads({json.dumps(action)!r}))
    Path({str(published)!r}).write_text("READY")
    print(json.dumps({{"action_key": {action['action_key']!r}}}))
    return 0
''')
    def slow_mint(*args, **kwargs):
        entered.write_text("entered shared-filesystem mint")
        time.sleep(4)
        return tmp_path / "grant.json"
    monkeypatch.setattr(slots, "mint", slow_mint)
    monkeypatch.setattr(publish_runtime, "MIRROR", tmp_path / "repo")
    real_submit = handoff.submit
    def short_remaining_budget(argv, *, intent, authorize, timeout_s):
        # Model a nearly consumed preparation budget without waiting120s.
        return real_submit(argv, intent=intent, authorize=authorize, timeout_s=1.0)
    monkeypatch.setattr(handoff, "submit", short_remaining_budget)
    def driver(*, generation, publication_canary_authorizer, report):
        paths = {"pbrun": Path(argv[1]),
                 "published_src": Path(__file__).resolve().parents[1] / "src",
                 "publication_canary_authorizer": publication_canary_authorizer}
        pbcanary.submit_leg(paths, {"name": "leg-2", "argv": ["true"],
                                   "demand": {"cpu": 1, "gpu": 1, "mem_gb": 8}},
                           tmp_path, "private-run", -10, generation)
        return 0
    monkeypatch.setattr(publish_runtime, "_load_canary_driver", lambda: driver)
    started = time.monotonic()
    with publish_runtime._publication_lock():
        with pytest.raises(publish_runtime._CanaryPrecondition):
            publish_runtime._invoke_canary_driver(GENERATION)
    elapsed = time.monotonic() - started
    assert entered.exists(), "must reach the mint, not time out before exercising it"
    assert not published.exists(), "deadline failure must not authorize READY"
    assert elapsed < 3, f"one-second submission budget blocked {elapsed:.3f}s in mint"
