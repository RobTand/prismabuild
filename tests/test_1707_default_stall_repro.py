"""Repro for prismabuild#1707: default stall supervision, not wall-clock kill."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, pool  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

CHATTY = '''
import sys, time
deadline = time.monotonic() + float(sys.argv[1])
while time.monotonic() < deadline:
    print("still working", flush=True)
    time.sleep(0.05)
open("result", "w").write("ok")
'''


def _claimed(tmp_path, *, seconds, timeout_s=None, stall_s=None):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(CHATTY)
    params = {}
    if timeout_s is not None:
        params["execution_timeout_s"] = timeout_s
    if stall_s is not None:
        params["stall_allowance_s"] = stall_s
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/1707", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py", str(seconds)],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"),
        capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout,
                  worker_script=Path(__file__).resolve().parents[1]
                  / "tools" / "prismabuild_worker.py")
    return queue, queue.claim()


def test_default_action_with_steady_output_outlives_old_ceiling(tmp_path):
    queue, item = _claimed(tmp_path, seconds=1.5)
    outcome = queue.execute(item, timeout_s=0.4, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "executed", repr(outcome)
    assert outcome["elapsed_s"] > 1.0
