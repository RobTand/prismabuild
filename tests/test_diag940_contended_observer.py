"""A queued observer must not sample after its intended holder retires.

These are private unit-fixture claims, not fleet holders or live profiles.
"""
import importlib.util
import json
from pathlib import Path

import pytest


KEY = "a" * 64
HOST = "observed-worker"


@pytest.fixture
def diagnostic(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "tools/maintenance/diag940_contended_observer.py"
    spec = importlib.util.spec_from_file_location("diag940_observer_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.socket, "gethostname", lambda: HOST)
    return module


def write_claim(queue, **changes):
    data = {"action_key": KEY, "claimed_host": HOST,
            "claimed_by": "unit-fixture-owner", "claimed_unix": 1.0}
    data.update(changes)
    (queue / "claimed").mkdir()
    (queue / "claimed" / f"{KEY}.json").write_text(json.dumps(data))


@pytest.mark.parametrize("state", ["retired", "unbound_host", "foreign_host"])
def test_preflight_refuses_an_absent_or_unbound_holder(diagnostic, tmp_path, state):
    if state != "retired":
        write_claim(tmp_path, claimed_host=None if state == "unbound_host" else "other-worker")
    with pytest.raises(ValueError, match="observer holder.*(claimed|host)"):
        diagnostic.holder(tmp_path, KEY)


def test_matching_live_claim_is_a_positive_control(diagnostic, tmp_path):
    write_claim(tmp_path)
    result = diagnostic.holder(tmp_path, KEY)
    assert result["action_key"] == KEY
    assert result["present"] is True
    assert result["claimed_by"] == "unit-fixture-owner"
    assert result["claimed_unix"] == 1.0


def test_claim_must_still_bind_its_key(diagnostic, tmp_path):
    write_claim(tmp_path, action_key="b" * 64)
    with pytest.raises(ValueError, match="observer holder claim does not bind its key"):
        diagnostic.holder(tmp_path, KEY)


def test_post_capture_observation_can_record_a_retired_holder(diagnostic, tmp_path):
    assert diagnostic._holder_observation(tmp_path, KEY) == {
        "action_key": KEY, "present": False,
    }
