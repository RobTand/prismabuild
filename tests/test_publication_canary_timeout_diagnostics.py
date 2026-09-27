"""The publisher's durable refusal must retain a bounded mint helper's identity."""
import json
from pathlib import Path

import pytest
import pbcanary
import pbstatus
import publish_runtime
from test_publication_canary_handoff import _publisher_action, _stub
from test_publication_canary_slot import GENERATION


def test_mint_timeout_identity_survives_handoff_and_driver(tmp_path, monkeypatch):
    action = _publisher_action(tmp_path)
    ready = tmp_path / "ready-marker"
    argv = _stub(tmp_path, f'''
def main(*, publication_canary_intent, authorize_canary):
    authorize_canary(json.loads({json.dumps(action)!r}))
    Path({str(ready)!r}).write_text("READY")
    return 0
''')
    def retained(name, read, *, deadline, abandoned, announce_retained):
        assert name == "publication canary mint"
        abandoned.append({"pid": 987654, "start_ticks": 12345})
        return {"status": "timed_out"}
    monkeypatch.setattr(pbstatus, "bounded", retained)
    monkeypatch.setattr(publish_runtime, "MIRROR", tmp_path / "repo")
    def driver(*, generation, publication_canary_authorizer, report):
        paths = {"pbrun": Path(argv[1]),
                 "published_src": Path(__file__).resolve().parents[1] / "src",
                 "publication_canary_authorizer": publication_canary_authorizer}
        pbcanary.submit_leg(paths, {"name": "leg-2", "argv": ["true"],
                                   "demand": {"cpu": 1, "gpu": 1, "mem_gb": 8}},
                           tmp_path, "private-run", -10, generation)
        return 0
    monkeypatch.setattr(publish_runtime, "_load_canary_driver", lambda: driver)
    with publish_runtime._publication_lock():
        with pytest.raises(publish_runtime._CanaryPrecondition) as caught:
            publish_runtime._invoke_canary_driver(GENERATION)
    detail = str(caught.value)
    assert "retained helper" in detail
    assert "987654" in detail and "12345" in detail
    assert not ready.exists()
