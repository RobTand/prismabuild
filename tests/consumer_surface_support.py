"""Scratch consumer queue and real terminal result fixtures."""
import json
import pytest
import pbwait
from prismabuild import core as pb
from fleet_world import Admission, FleetWorld
from submission_support import patch_store
from verified_action_support import _standard_action, _publish_result
from pbmcp_fixture import settle_publishers

@pytest.fixture
def consumer_world(tmp_path, monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)
    world = FleetWorld(tmp_path, admission=Admission(
        capacity={"cpu": 8, "mem_gb": 16}, default_demand={"cpu": 1, "mem_gb": 1}))
    patch_store(monkeypatch, root=tmp_path)
    monkeypatch.setenv(pbwait.slurm_lane.LANE_ROOT_ENV, str(tmp_path / "slurm"))
    return world



def consumer_action(world, name, status="executed", action_exit=None, *, with_receipt=False, log_padding=None):
    base = world.root / name
    checkout = world.root / "checkout"
    checkout.mkdir(parents=True, exist_ok=True)
    action = _standard_action(checkout, command=["/bin/echo", name])
    cas = pb.PrismaBuildCAS(world.root / "cas")
    cas.publish_action_request(action)
    receipt = None
    stdout = "failed command output\n"
    if status in {"executed", "cache_hit"} or with_receipt:
        # Build the receipt in the same CAS used by the queue's sealed request.
        scratch_cas, receipt = _publish_result(world.root, action, b"result\n")
        assert scratch_cas.root == cas.root
        stdout = json.dumps({"status": "published", "receipt": receipt}) + "\n"
    streams = {"stdout": stdout, "stderr": "diagnostic\n"}
    if log_padding is not None:
        stream, padding = log_padding
        streams[stream] = padding + streams[stream]
    key = action["action_key"]
    world.actions.publish(key, cas_root=cas.root, checkout_root=checkout, tags=["x86"],
                          resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    if status != "waiting":
        world.queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
        world.queue.finish(key, status=status, detail={
            "status": status, "returncode": 0 if receipt else 1,
            "action_returncode": action_exit if action_exit is not None else 0 if receipt else 7,
            **streams})
        settle_publishers()
    return key, receipt

