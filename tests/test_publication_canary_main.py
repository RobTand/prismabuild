"""Real pbrun parsing/preparation/sealing/publication, wholly in a tmp store."""
import json
import socket
import sys

import pytest
import pbrun
from prismabuild import pool, publication_canary as slots
from test_pbrun_detach import _checkout
from test_publication_canary_slot import GENERATION


def _private_submission(tmp_path, monkeypatch):
    checkout = _checkout(tmp_path)
    host = socket.gethostname()
    intent = {"generation": GENERATION, "host": host}
    tags = [host, slots.CAPABILITY, f"runtime-generation:{GENERATION}"]
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.announce(host=host, tags=tags, has_gpu=True,
               capacity={"cpu": 4, "gpu": 1, "mem_gb": 16})
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(checkout), "--detach", "--transport", "pool",
        "--demand", "cpu=1,gpu=1,mem_gb=8", "--gpu-capacity", "1",
        "--priority", "-10", "--exclusive", "--timeout-s", "32",
        *[arg for tag in tags for arg in ("--tag", tag)],
        "--env", "PBCANARY_LEG=leg-2", "--env", "PBCANARY_RUN_ID=private-main",
        "--env", f"PBCANARY_GENERATION={GENERATION}",
        "--", "/bin/bash", "-c", "printf private-fixture > result",
    ])
    return q, intent


def test_real_main_authorization_refusal_precedes_cas_and_ready(tmp_path, monkeypatch):
    q, intent = _private_submission(tmp_path, monkeypatch)
    seen = []
    def refuse(action):
        seen.append(action["action_key"])
        assert slots.intent(action) == intent
        raise ValueError("publisher refused exact sealed request")
    with pytest.raises(ValueError, match="publisher refused"):
        pbrun.main(publication_canary_intent=intent, authorize_canary=refuse)
    assert len(seen) == 1
    key = seen[0]
    assert not (tmp_path / "cas" / "requests" / key[:2] / f"{key}.json").exists()
    assert not q.item_path(pool.READY, key).exists()


def test_real_main_grant_binds_exact_request_and_ready_generation(tmp_path, monkeypatch, capsys):
    q, intent = _private_submission(tmp_path, monkeypatch)
    seen = []
    def grant(action):
        key = action["action_key"]
        seen.append(key)
        assert not (tmp_path / "cas" / "requests" / key[:2] / f"{key}.json").exists()
        assert not q.item_path(pool.READY, key).exists()
        slots.mint(q.root, action, run_id="private-main")
    assert pbrun.main(publication_canary_intent=intent, authorize_canary=grant) == 0
    line = json.loads(capsys.readouterr().out)
    assert seen == [line["action_key"]]
    key = seen[0]
    request = tmp_path / "cas" / "requests" / key[:2] / f"{key}.json"
    assert json.loads(request.read_text())["action_key"] == key
    item = json.loads(q.item_path(pool.READY, key).read_text())
    bound = json.loads(slots.slot_path(q.root, intent).read_text())
    # Preserve pbrun's existing exclusive-GPU 16-GiB floor, even though the
    # ordinary leg's declared demand starts at 8 GiB.
    assert item["resources"] == {"cpu": 1, "gpu": 1, "mem_gb": 16}
    assert item["publication_canary"] == intent
    assert bound["action_key"] == key
    assert bound["published_unix"] == item["published_unix"] == line["published_unix"]
    assert slots.verified(q.root, item)
