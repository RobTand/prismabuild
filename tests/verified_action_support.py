"""Shared scratch CAS/action fixtures for verified results and private records."""
from __future__ import annotations
import json
from pathlib import Path
import pytest
from prismabuild import core as pb, movement_actions, pool
import pbmcp_fixture as fx
PATH_PREFIX = "/opt/pb-tools"

@pytest.fixture(autouse=True)
def _no_outer_launch_identity(monkeypatch):
    """A test that seals its own action never inherits an outer attempt's."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)



def _standard_action(
    checkout: Path,
    *,
    command: list[str],
    declared_command: list[str] | None = None,
    log_name: str = "pbrun_result.txt",
    result_path: str | None = None,
    path_prefix: str = PATH_PREFIX,
    declared_path: str | None = None,
    inputs: list[dict[str, object]] | None = None,
    determinism: str = "deterministic",
    demand: dict[str, int] | None = None,
) -> dict[str, object]:
    """A sealed action in pbrun's standard captured-log recipe."""

    code = checkout / "task_code.py"
    if not code.exists():
        code.write_text("# closure member\n", encoding="utf-8")
    argv = movement_actions.standard_capture_argv(
        command, log_name, path_prefix=path_prefix)
    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/verified-result",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": determinism,
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": argv,
            "working_directory": ".",
            "result_path": result_path or log_name,
        },
        "inputs": inputs or [],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": {"command": list(
            command if declared_command is None else declared_command),
                   **({"demand": demand} if demand is not None else {})},
        "environment": {
            "variables": {"PATH": declared_path or f"{path_prefix}:/usr/bin:/bin"},
            "toolchain": {},
        },
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })



def _publish_result(base: Path, action, payload: bytes, *, launcher=None):
    cas = pb.PrismaBuildCAS(base / "cas")
    cas.publish_action_request(action)
    attestation = pb.preflight_action(
        action, cas_root=base / "cas", checkout_root=base / "checkout",
        worker_launcher_identity=launcher)
    output = base / "output.bin"
    output.write_bytes(payload)
    receipt, _won = cas.publish_result(
        action, output, attestation=attestation, return_execution_receipt=True)
    return cas, receipt



def _finish(base: Path, action, receipt, *, status: str = "executed",
            stdout: str | None = None, returncode: int = 0,
            native=None, worker_script=None, queue_root=None):
    key = str(action["action_key"])
    cas = pb.PrismaBuildCAS(base / "cas")
    if stdout is None:
        announcement = {
            "status": "published",
            "receipt": receipt,
            "payload_path": str(cas.blob_path(str(receipt["result"]["sha256"]))),
        }
        stdout = json.dumps(announcement) + "\n"
    queue = pool.PoolQueue(queue_root if queue_root is not None else base / "queue")
    queue.records.ensure_layout()
    queue.publish(action_key=key, cas_root=base / "cas",
                  checkout_root=base / "checkout",
                  worker_script=worker_script or base / "worker.py",
                  tags=["x86"], resources={"cpu": 1, "mem_gb": 1})
    queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    if native is not None:
        claimed_path = queue.records.item_path(pool.record_store.CLAIMED, key)
        claimed = json.loads(claimed_path.read_text())
        claimed.update(native["claim"])
        claimed_path.write_bytes(pb._canonical_file_bytes(claimed))
    queue.finish(key, status=status,
                 detail={"returncode": returncode, "status": status,
                         "stdout": stdout, "stderr": "",
                         **(native["detail"] if native is not None else {})})
    fx.settle_publishers()
    record = json.loads(
        queue.records.item_path(pool.record_store.DONE, key).read_text(encoding="utf-8"),
    )
    return queue, record

