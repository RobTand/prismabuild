"""The bounded, verified action-result read and the capture-command binding (#1446).

Everything here is a private CPU fixture: a real sealed action, a real
PrismaBuildCAS receipt and execution receipt, and a real PoolQueue
``publish``/``claim``/``finish``.  No GPU, no fleet, no live queue.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import client, movement_actions, pool  # noqa: E402

import pbmcp_fixture as fx  # noqa: E402

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
            native=None, worker_script=None):
    key = str(action["action_key"])
    cas = pb.PrismaBuildCAS(base / "cas")
    if stdout is None:
        announcement = {
            "status": "published",
            "receipt": receipt,
            "payload_path": str(cas.blob_path(str(receipt["result"]["sha256"]))),
        }
        stdout = json.dumps(announcement) + "\n"
    queue = pool.PoolQueue(base / "queue")
    queue.ensure_layout()
    queue.publish(action_key=key, cas_root=base / "cas",
                  checkout_root=base / "checkout",
                  worker_script=worker_script or base / "worker.py",
                  tags=["x86"], resources={"cpu": 1, "mem_gb": 1})
    queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    if native is not None:
        claimed_path = queue.item_path(pool.CLAIMED, key)
        claimed = json.loads(claimed_path.read_text())
        claimed.update(native["claim"])
        claimed_path.write_bytes(pb._canonical_file_bytes(claimed))
    queue.finish(key, status=status,
                 detail={"returncode": returncode, "status": status,
                         "stdout": stdout, "stderr": "",
                         **(native["detail"] if native is not None else {})})
    fx.settle_publishers()
    record = json.loads(queue.item_path(pool.DONE, key).read_text(encoding="utf-8"))
    return queue, record


def _fixture(base: Path, payload: bytes = b"the verified payload\n"):
    base.mkdir(parents=True, exist_ok=True)
    (base / "checkout").mkdir(parents=True, exist_ok=True)
    action = _standard_action(base / "checkout", command=["/bin/echo", "hello"])
    _cas, receipt = _publish_result(base, action, payload)
    queue, record = _finish(base, action, receipt)
    return action, queue, record, receipt, payload


def test_reads_the_verified_payload_request_and_receipt(tmp_path: Path):
    action, queue, record, receipt, payload = _fixture(tmp_path)
    result = client.read_verified_action_result(
        queue, str(action["action_key"]),
        published_unix=float(record["published_unix"]), attempt=1,
        max_result_bytes=len(payload) + 8, max_evidence_bytes=1 << 20)
    assert result["schema"] == client.ACTION_RESULT_SCHEMA_V1
    assert result["payload"] == payload
    assert result["receipt"] == receipt
    assert result["request"] == action
    assert result["attempt"] == 1
    assert result["published_unix"] == float(record["published_unix"])
    assert result["generation"] == queue.attempt_generation(record)
    assert result["inputs"] == []
    assert result["input_payloads"] == {}
    assert isinstance(result["worker_id"], str)
    assert isinstance(result["host"], str)


def test_selects_the_execution_receipt_not_the_canonical_winner(tmp_path: Path):
    base = tmp_path / "r"
    base.mkdir()
    (base / "checkout").mkdir()
    action = _standard_action(
        base / "checkout", command=["/bin/echo", "x"], determinism="stochastic")
    cas, first = _publish_result(base, action, b"first\n")
    _cas, second = _publish_result(base, action, b"second\n")
    assert first["receipt_sha256"] != second["receipt_sha256"]
    assert cas.lookup(action) == first
    queue, record = _finish(base, action, second)
    result = client.read_verified_action_result(
        queue, str(action["action_key"]),
        published_unix=float(record["published_unix"]), attempt=1,
        max_result_bytes=64, max_evidence_bytes=1 << 20)
    assert result["payload"] == b"second\n"
    assert result["receipt"] == second


def test_wrong_publication_and_attempt_refuse(tmp_path: Path):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    key = str(action["action_key"])
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]) + 1,
            attempt=1, max_result_bytes=64)
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=2, max_result_bytes=64)
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=1, max_result_bytes=64, max_evidence_bytes=0)


def test_a_newer_failed_generation_refuses(tmp_path: Path):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    key = str(action["action_key"])
    failed = {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
              "status": "failed",
              "published_unix": float(record["published_unix"]) + 5,
              "finished_unix": 1.0, "finished_host": "box", "detail": {}}
    pb._atomic_publish(queue.item_path(pool.FAILED, key),
                       pb._canonical_file_bytes(failed))
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=1, max_result_bytes=64)


def test_a_newer_withdrawn_or_contested_ending_refuses(tmp_path: Path):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    key = str(action["action_key"])
    generation = float(record["published_unix"])
    withdrawn = {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
                 "status": "withdrawn", "published_unix": generation,
                 "withdrawn_unix": generation + 5, "finished_host": "box",
                 "detail": {}}
    pb._atomic_publish(queue.item_path(pool.WITHDRAWN, key),
                       pb._canonical_file_bytes(withdrawn))
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=generation, attempt=1,
            max_result_bytes=64, max_evidence_bytes=1 << 20)
    # A tie at the newest generation is contested: a failed row beside the
    # success cannot be ordered, so the reader must refuse rather than pick.
    queue.item_path(pool.WITHDRAWN, key).unlink()
    contested = {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
                 "status": "failed", "published_unix": generation,
                 "finished_unix": generation + 1, "finished_host": "box",
                 "detail": {}}
    pb._atomic_publish(queue.item_path(pool.FAILED, key),
                       pb._canonical_file_bytes(contested))
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=generation, attempt=1,
            max_result_bytes=64, max_evidence_bytes=1 << 20)


def test_a_poisoned_historical_attempt_is_never_read_for_a_later_success(
    tmp_path: Path, monkeypatch
):
    base = tmp_path / "two"
    base.mkdir()
    (base / "checkout").mkdir()
    action = _standard_action(base / "checkout", command=["/bin/echo", "hello"])
    cas = pb.PrismaBuildCAS(base / "cas")
    cas.publish_action_request(action)
    attestation = pb.preflight_action(
        action, cas_root=base / "cas", checkout_root=base / "checkout")
    output = base / "output.bin"
    output.write_bytes(b"the later payload\n")
    receipt, _won = cas.publish_result(
        action, output, attestation=attestation, return_execution_receipt=True)
    key = str(action["action_key"])
    queue = pool.PoolQueue(base / "queue")
    queue.ensure_layout()
    queue.publish(action_key=key, cas_root=base / "cas",
                  checkout_root=base / "checkout",
                  worker_script=base / "worker.py", tags=["x86"],
                  resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=2, retry_safe=True)
    queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    queue.finish(key, status="failed", detail={
        "returncode": 1, "status": "failed",
        "stdout": '{"receipt": {"receipt_sha256": "' + "0" * 64 + '"}}\n',
        "stderr": "poisoned historical attempt\n"})
    fx.settle_publishers()
    queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    announcement = json.dumps({
        "status": "published", "receipt": receipt,
        "payload_path": str(cas.blob_path(str(receipt["result"]["sha256"]))),
    }) + "\n"
    queue.finish(key, status="executed", detail={
        "returncode": 0, "status": "executed", "stdout": announcement,
        "stderr": ""})
    fx.settle_publishers()
    record = json.loads(queue.item_path(pool.DONE, key).read_text(encoding="utf-8"))
    assert record["attempts"] == 2
    first = json.loads(queue.attempt_path(record, 1).read_text(encoding="utf-8"))
    first_log = queue.root / str(first["logs"]["stdout"]["path"])
    seen: list[Path] = []
    original = pb._read_regular_file_nofollow

    def spy(path, **kwargs):
        seen.append(Path(path))
        return original(path, **kwargs)

    monkeypatch.setattr(pb, "_read_regular_file_nofollow", spy)
    result = client.read_verified_action_result(
        queue, key, published_unix=float(record["published_unix"]), attempt=2,
        max_result_bytes=64, max_evidence_bytes=1 << 20)
    assert result["attempt"] == 2
    assert result["payload"] == b"the later payload\n"
    assert first_log not in seen, "a failed historical attempt's log is not read"


def test_a_payload_over_its_cap_refuses_before_the_blob_is_opened(
    tmp_path: Path, monkeypatch
):
    action, queue, record, receipt, _payload = _fixture(tmp_path)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    blob = cas.blob_path(str(receipt["result"]["sha256"]))
    seen: list[Path] = []
    original = pb._read_regular_file_nofollow

    def spy(path, **kwargs):
        seen.append(Path(path))
        return original(path, **kwargs)

    monkeypatch.setattr(pb, "_read_regular_file_nofollow", spy)
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, str(action["action_key"]),
            published_unix=float(record["published_unix"]), attempt=1,
            max_result_bytes=int(receipt["result"]["bytes"]) - 1,
            max_evidence_bytes=1 << 20)
    assert blob not in seen, "an oversized payload must not be opened"


def test_a_writable_selected_log_refuses(tmp_path: Path):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    key = str(action["action_key"])
    attempt = json.loads(queue.attempt_path(record, 1).read_text(encoding="utf-8"))
    log = queue.root / str(attempt["logs"]["stdout"]["path"])
    log.chmod(0o644)
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=1, max_result_bytes=64, max_evidence_bytes=1 << 20)


def test_only_the_selected_attempt_and_log_are_opened(tmp_path: Path, monkeypatch):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    # An unrelated, poisoned file beside the immutable attempt must never be
    # opened: the reader follows the selected link; it does not list or expand
    # history.
    poison = queue.attempt_path(record, 1).parent / "poison.log"
    poison.write_text("not a result\n", encoding="utf-8")
    seen: list[Path] = []
    original = pb._read_regular_file_nofollow

    def spy(path, **kwargs):
        seen.append(Path(path))
        return original(path, **kwargs)

    monkeypatch.setattr(pb, "_read_regular_file_nofollow", spy)
    client.read_verified_action_result(
        queue, str(action["action_key"]),
        published_unix=float(record["published_unix"]), attempt=1,
        max_result_bytes=64, max_evidence_bytes=1 << 20)
    assert poison not in seen


def test_input_limits_return_owned_bytes_for_selected_inputs_only(tmp_path: Path):
    base = tmp_path / "i"
    base.mkdir()
    (base / "checkout").mkdir()
    cas = pb.PrismaBuildCAS(base / "cas")
    source = base / "control.json"
    source.write_bytes(b'{"quantum": 1}\n')
    entry, _won = cas.ingest_input(source, input_id="pq.quantum")
    action = _standard_action(
        base / "checkout", command=["/bin/echo", "hi"], inputs=[entry])
    _cas, receipt = _publish_result(base, action, b"payload\n")
    queue, record = _finish(base, action, receipt)
    result = client.read_verified_action_result(
        queue, str(action["action_key"]),
        published_unix=float(record["published_unix"]), attempt=1,
        max_result_bytes=64, max_evidence_bytes=1 << 20,
        input_limits={"pq.quantum": 4096})
    assert result["inputs"] == [entry]
    assert result["input_payloads"] == {"pq.quantum": b'{"quantum": 1}\n'}
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, str(action["action_key"]),
            published_unix=float(record["published_unix"]), attempt=1,
            max_result_bytes=64, max_evidence_bytes=1 << 20,
            input_limits={"pq.absent": 16})


def test_selected_input_tamper_and_over_cap_refuse_without_reading_unchosen(
    tmp_path: Path, monkeypatch
):
    base = tmp_path / "t"
    base.mkdir()
    (base / "checkout").mkdir()
    cas = pb.PrismaBuildCAS(base / "cas")
    source_a = base / "a.json"
    source_a.write_bytes(b'{"a": 1}\n')
    a_entry, _won = cas.ingest_input(source_a, input_id="pq.a")
    source_b = base / "b.json"
    source_b.write_bytes(b'{"b": 2}\n')
    b_entry, _won = cas.ingest_input(source_b, input_id="pq.b")
    action = _standard_action(base / "checkout", command=["/bin/echo", "hi"],
                              inputs=[a_entry, b_entry])
    _cas, receipt = _publish_result(base, action, b"payload\n")
    queue, record = _finish(base, action, receipt)
    key = str(action["action_key"])
    b_blob = cas.blob_path(str(b_entry["sha256"]))
    seen: list[Path] = []
    original = pb._read_regular_file_nofollow

    def spy(path, **kwargs):
        seen.append(Path(path))
        return original(path, **kwargs)

    monkeypatch.setattr(pb, "_read_regular_file_nofollow", spy)
    result = client.read_verified_action_result(
        queue, key, published_unix=float(record["published_unix"]), attempt=1,
        max_result_bytes=64, max_evidence_bytes=1 << 20,
        input_limits={"pq.a": 4096})
    assert result["input_payloads"] == {"pq.a": b'{"a": 1}\n'}
    assert result["inputs"] == [a_entry, b_entry]
    assert b_blob not in seen, "an unchosen declared input must never be opened"
    # Over cap: refused before the selected blob is opened.
    with pytest.raises(client.ActionResultError, match="byte cap"):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=1, max_result_bytes=64, max_evidence_bytes=1 << 20,
            input_limits={"pq.a": int(a_entry["bytes"]) - 1})
    # Tamper: the selected input's bytes no longer match its declared address,
    # refused through the shared Core owned-blob owner.
    a_blob = cas.blob_path(str(a_entry["sha256"]))
    a_blob.chmod(0o644)
    a_blob.write_bytes(b'{"a": 9}\n')
    a_blob.chmod(0o444)
    with pytest.raises(client.ActionResultError, match="declared address"):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=1, max_result_bytes=64, max_evidence_bytes=1 << 20,
            input_limits={"pq.a": 4096})


# -- every byte is bounded, and loosely-typed evidence is refused ------------

def _rewrite_readonly(path: Path, payload: bytes) -> None:
    """Write one immutable evidence file (creating it) and leave it read-only."""

    if path.exists():
        path.chmod(0o644)
    path.write_bytes(payload)
    path.chmod(0o444)


def test_an_oversized_execution_receipt_refuses_under_the_evidence_cap(
    tmp_path: Path, monkeypatch
):
    action, queue, record, receipt, _payload = _fixture(tmp_path)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    receipt_path = cas._execution_receipt_path(str(receipt["receipt_sha256"]))
    # A sparse file just over the 1 MiB evidence cap: nominally oversized, no
    # blocks allocated.  A reader without a cap would allocate and hash the
    # whole declared size before refusing.
    receipt_path.chmod(0o644)
    with receipt_path.open("r+b") as handle:
        handle.truncate(2 << 20)
    receipt_path.chmod(0o444)
    seen: list[tuple[Path, object]] = []
    original = pb._read_regular_file_nofollow

    def spy(path, **kwargs):
        seen.append((Path(path), kwargs.get("max_bytes")))
        return original(path, **kwargs)

    monkeypatch.setattr(pb, "_read_regular_file_nofollow", spy)
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, str(action["action_key"]),
            published_unix=float(record["published_unix"]), attempt=1,
            max_result_bytes=64, max_evidence_bytes=1 << 20)
    assert (receipt_path, 1 << 20) in seen, (
        "the execution receipt must be read under the caller's cap")
    assert all(cap is not None for _path, cap in seen), (
        "every read in the verified-result path must carry a byte cap")


def test_an_attempt_changed_during_the_read_refuses(tmp_path: Path, monkeypatch):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    key = str(action["action_key"])
    attempt_path = queue.attempt_path(record, 1)
    original = pb._read_regular_file_nofollow
    tampered = []

    def spy(path, **kwargs):
        payload = original(path, **kwargs)
        if not tampered and Path(path) == attempt_path:
            # Weaken the held attempt's binding in place, after the first read:
            # the completion recheck must refuse rather than return the result
            # the first read justified.
            tampered.append(True)
            value = json.loads(payload)
            value["preemption_context"] = {"forged": 1}
            _rewrite_readonly(attempt_path, pb._canonical_file_bytes(value))
        return payload

    monkeypatch.setattr(pb, "_read_regular_file_nofollow", spy)
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=1, max_result_bytes=64, max_evidence_bytes=1 << 20)


def test_a_boolean_attempt_number_refuses(tmp_path: Path):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    attempt_path = queue.attempt_path(record, 1)
    value = json.loads(attempt_path.read_text(encoding="utf-8"))
    value["attempt"] = True  # JSON true, equal to 1 under ``==``
    _rewrite_readonly(attempt_path, pb._canonical_file_bytes(value))
    with pytest.raises(client.ActionResultError, match="history link"):
        client.read_verified_action_result(
            queue, str(action["action_key"]),
            published_unix=float(record["published_unix"]), attempt=1,
            max_result_bytes=64, max_evidence_bytes=1 << 20)


def test_a_huge_attempt_generation_refuses(tmp_path: Path):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    attempt_path = queue.attempt_path(record, 1)
    value = json.loads(attempt_path.read_text(encoding="utf-8"))
    value["published_unix"] = 10 ** 400  # finite to no binary64 reader
    _rewrite_readonly(attempt_path, pb._canonical_file_bytes(value))
    with pytest.raises(client.ActionResultError, match="history link"):
        client.read_verified_action_result(
            queue, str(action["action_key"]),
            published_unix=float(record["published_unix"]), attempt=1,
            max_result_bytes=64, max_evidence_bytes=1 << 20)


def test_a_huge_publication_integer_refuses(tmp_path: Path):
    action, queue, _record, _receipt, _payload = _fixture(tmp_path)
    # A caller-supplied integer past the binary64 range is a refusal, never an
    # escaped OverflowError.
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, str(action["action_key"]), published_unix=10 ** 400,
            attempt=1, max_result_bytes=64, max_evidence_bytes=1 << 20)


def test_a_huge_terminal_generation_is_unorderable_not_an_overflow(tmp_path: Path):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    key = str(action["action_key"])
    # The terminal row itself names a generation past the binary64 range: the
    # census rule must read it as unorderable, never raise OverflowError.
    row = dict(record)
    row["published_unix"] = 10 ** 400
    ending = queue.resolve_ending({pool.DONE: (queue.item_path(pool.DONE, key), row)}, [])
    assert ending["generation"] is None
    assert pool._finite_generation(10 ** 400) is None
    assert pool._finite_generation(float("inf")) is None
    assert pool._finite_generation(3.5) == 3.5


def _a_nesting_depth_that_refuses() -> bytes | None:
    """A bounded document this interpreter cannot parse, or ``None``."""

    for depth in (1000, 4000, 16000, 64000, 256000):
        document = b'{"a":' * depth + b"0" + b"}" * depth
        try:
            pb._decode_strict_json(document, where="control")
        except pb.PrismaBuildError:
            return document
        except RecursionError:
            pytest.fail("the strict decoder let RecursionError escape")
    return None


def test_a_deeply_nested_record_refuses_in_the_action_result_vocabulary(
    tmp_path: Path,
):
    # A bounded, deeply nested document: the strict decoder owner must
    # normalize the recursion refusal, and the public reader must still refuse
    # with ActionResultError, never a bare RecursionError.
    nested = _a_nesting_depth_that_refuses()
    if nested is None:
        pytest.skip("this interpreter parses every nesting depth tried")
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    key = str(action["action_key"])
    row = (b'{"schema": "control", "action_key": "' + key.encode()
           + b'", "detail": ' + nested + b"}")
    queue.item_path(pool.DONE, key).write_bytes(row)
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=1, max_result_bytes=64, max_evidence_bytes=1 << 20)


def test_an_empty_terminal_leaf_never_reads_as_absent(tmp_path: Path):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    key = str(action["action_key"])
    # A newer generation's failed row, present but unreadable (empty).  The
    # bounded census must not skip it and answer the older success.
    failed = queue.item_path(pool.FAILED, key)
    failed.parent.mkdir(parents=True, exist_ok=True)
    failed.write_bytes(b"")
    with pytest.raises(client.ActionResultError):
        client.read_verified_action_result(
            queue, key, published_unix=float(record["published_unix"]),
            attempt=1, max_result_bytes=64, max_evidence_bytes=1 << 20)


def test_the_bounded_row_reader_is_strict_and_refuses_empty(tmp_path: Path):
    absent = tmp_path / "absent.json"
    assert pool._read_json_bounded(absent, max_bytes=1 << 20) is None
    empty = tmp_path / "empty.json"
    empty.write_bytes(b"")
    with pytest.raises(pool.PoolContractError):
        pool._read_json_bounded(empty, max_bytes=1 << 20)
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"a": 1, "a": 2}\n', encoding="utf-8")
    with pytest.raises(pool.PoolContractError):
        pool._read_json_bounded(duplicate, max_bytes=1 << 20)
    nonfinite = tmp_path / "nonfinite.json"
    nonfinite.write_text('{"a": NaN}\n', encoding="utf-8")
    with pytest.raises(pool.PoolContractError):
        pool._read_json_bounded(nonfinite, max_bytes=1 << 20)


def test_a_duplicate_key_announcement_does_not_bind(tmp_path: Path):
    action, queue, record, receipt, _payload = _fixture(tmp_path)
    attempt_path = queue.attempt_path(record, 1)
    value = json.loads(attempt_path.read_text(encoding="utf-8"))
    # Two identical ``receipt`` keys in one line: permissive ``json.loads``
    # keeps the last and would bind the authentic receipt; strict decoding
    # refuses the line, so no announcement remains.
    line = ('{"receipt": ' + json.dumps(receipt) + ', '
            '"receipt": ' + json.dumps(receipt) + "}\n").encode("utf-8")
    digest = pb.raw_sha256(line)
    log_path = queue.attempt_log_path(record, 1, "stdout", digest)
    _rewrite_readonly(log_path, line)
    value["logs"]["stdout"]["sha256"] = digest
    value["logs"]["stdout"]["bytes"] = len(line)
    value["logs"]["stdout"]["path"] = str(log_path.relative_to(queue.root))
    _rewrite_readonly(attempt_path, pb._canonical_file_bytes(value))
    with pytest.raises(client.ActionResultError, match="announcement"):
        client.read_verified_action_result(
            queue, str(action["action_key"]),
            published_unix=float(record["published_unix"]), attempt=1,
            max_result_bytes=64, max_evidence_bytes=1 << 20)


# -- the standard-capture command binding ------------------------------------

def test_the_capture_wrapper_is_one_golden_byte_for_byte():
    argv = movement_actions.standard_capture_argv(
        ["/bin/echo", "hi"], "log.txt", path_prefix="/opt/tools")
    assert argv[:4] == ["/bin/bash", "--noprofile", "--norc", "-c"]
    assert argv[4] == (
        "export PATH=/opt/tools:$PATH; "
        'if _pb_capture=$(command -v gnutee); then :; '
        'elif _pb_capture=$(command -v tee); then :; '
        'else printf "pbrun: log capture executable unavailable\\n" >&2; '
        'exit 127; fi; '
        'printf "pbrun: log capture executable=%s\\n" "$_pb_capture" >&2; '
        '/bin/echo hi 2>&1 | "$_pb_capture" log.txt; '
        '_pb_status=("${PIPESTATUS[@]}"); '
        'if (( _pb_status[1] != 0 )); then '
        'printf "pbrun: capture status=%s; producer status=%s\\n" '
        '"${_pb_status[1]}" "${_pb_status[0]}" >&2; fi; '
        'if (( _pb_status[0] != 0 )); then exit "${_pb_status[0]}"; fi; '
        'exit "${_pb_status[1]}"')


def test_the_binding_accepts_the_standard_recipe(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _standard_action(checkout, command=["/bin/echo", "hi"])
    assert client.bind_standard_capture_command(action) == list(action["task"]["argv"])


def test_the_binding_refuses_a_file_result_recipe(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _standard_action(
        checkout, command=["/bin/echo", "hi"], log_name="log.txt",
        result_path="declared.out")
    with pytest.raises(client.ActionResultError):
        client.bind_standard_capture_command(action)


def test_the_binding_refuses_a_non_recipe_and_tampered_inputs(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    bare_argv = [sys.executable, "-c", "pass"]
    code = checkout / "task_code.py"
    code.write_text("# closure member\n", encoding="utf-8")
    bare = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/verified-result",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic", "artifact_family": "generic",
                 "artifact_kind": "generic", "argv": bare_argv,
                 "working_directory": ".", "result_path": "result.bin"},
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": {"command": list(bare_argv)},
        "environment": {"variables": {"PATH": f"{PATH_PREFIX}:/bin"},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    with pytest.raises(client.ActionResultError):
        client.bind_standard_capture_command(bare)
    # A command whose metadata disagrees with the sealed wrapper refuses.
    action = _standard_action(checkout, command=["/bin/echo", "hi"])
    tampered = {**action,
                "params": {**action["params"], "command": ["/bin/echo", "bye"]}}
    with pytest.raises(client.ActionResultError):
        client.bind_standard_capture_command(tampered)
    # A PATH that does not reconstruct the sealed wrapper refuses.
    action = _standard_action(checkout, command=["/bin/echo", "hi"],
                              declared_path="/elsewhere:/bin")
    with pytest.raises(client.ActionResultError):
        client.bind_standard_capture_command(action)


def test_a_validly_resealed_command_mismatch_reaches_the_binder_and_refuses(
    tmp_path: Path,
):
    checkout = tmp_path / "resealed"
    checkout.mkdir()
    # The sealed action passes validate_action: task.argv is the standard
    # capture wrapper for /bin/echo hi, while params.command names another
    # command.  Only the binder can catch that, and it must.
    action = _standard_action(checkout, command=["/bin/echo", "hi"],
                              declared_command=["/bin/echo", "bye"])
    assert pb.validate_action(action) == action
    with pytest.raises(client.ActionResultError, match="standard captured-log"):
        client.bind_standard_capture_command(action)


# These are private synthetic native-carrier fixtures, not a fleet qualification.
# The retained real native producer is exercised separately by the PB action.
def _native_fixture(base: Path, monkeypatch):
    from prismabuild import produced_output, reader_lease

    base.mkdir(exist_ok=True)
    (base / "checkout").mkdir()
    generation = base / "generation"
    core_file = generation / "src/prismabuild/core.py"
    core_file.parent.mkdir(parents=True)
    core_file.write_bytes(Path(pb.__file__).read_bytes())
    core_file.chmod(0o444)
    launcher_file = generation / "tools/prismabuild_worker.py"
    launcher_file.parent.mkdir()
    launcher_file.write_bytes((REPOSITORY / "tools/prismabuild_worker.py").read_bytes())
    launcher_file.chmod(0o444)
    monkeypatch.setattr(pb, "_LOADED_WORKER_CORE_IDENTITY",
                        pb._identify_runtime_source(core_file, where="private fixture core"))
    launcher = pb._identify_runtime_source(launcher_file, where="private fixture launcher")
    action = _standard_action(base / "checkout", command=["/bin/echo", "native"],
                              demand={"cpu": 1, "mem_gb": 1})
    cas, receipt = _publish_result(base, action, b"native fixture\n", launcher=launcher)
    key = action["action_key"]
    host = receipt["producer"]["evidence"]["hostname"]
    nonce = "a" * 32
    scope_id = produced_output._broker_scope_id(key, nonce)
    telemetry = {"complete": True, "action_key": key, "nonce": nonce,
                 "scope_unit": scope_id, "host": host, "memory_max_bytes": 1024 ** 3}
    allocation = {"preferred": [0], "fallback": []}
    export = {"scope_id": scope_id, "stopped_unix": 42.0, "empty": True,
              "tickets_pending": False, "released": True, "retired": False,
              "settled": True}
    native = {"claim": {"resource_scope": {"action_key": key, "nonce": nonce,
                "scope_id": scope_id, "memory_max_bytes": 1024 ** 3,
                "token": "b" * 64, "socket_path": "/run/prismabuild/resources.sock",
                "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + scope_id},
                "cpu_allocation": allocation,
                "resource_scope_cleanup": {"complete": True, "nonce": nonce,
                    "telemetry": telemetry, "export": export}},
              "detail": {"resource_telemetry": telemetry, "cpu_allocation": allocation}}
    from prismabuild import resource_scope
    native["detail"]["argv"] = resource_scope._wrapped_scope_argv(
        pool._with_cpu_affinity([sys.executable] + pool.worker_argv(
            worker_script=launcher_file, action_key=key, cas_root=base / "cas",
            checkout_root=base / "checkout"), allocation["preferred"]),
        python=sys.executable, helper=generation / "tools/resource_exec.py",
        socket_path=resource_scope.BROKER_SOCKET, action_key=key, nonce=nonce,
        token=native["claim"]["resource_scope"]["token"])
    queue, record = _finish(base, action, receipt, native=native, worker_script=launcher_file)
    # finish already files this proof through the existing pool writer.
    # Reuse that carrier rather than minting a second fixture representation.
    proof_path = reader_lease.attestation_path(queue, key, nonce)
    assert proof_path.is_file()
    return action, queue, record, receipt, proof_path


def _native_read(action, queue, record, **kwargs):
    return client.read_verified_action_result(
        queue, action["action_key"], published_unix=record["published_unix"],
        attempt=1, max_result_bytes=4096, require_native_producer_context=True,
        **kwargs)


def test_selected_native_context_is_owned_and_truthfully_bound(tmp_path, monkeypatch):
    action, queue, record, receipt, proof_path = _native_fixture(tmp_path, monkeypatch)
    result = _native_read(action, queue, record)
    ctx = result["producer_context"]
    assert ctx["schema"] == client.NATIVE_PRODUCER_CONTEXT_SCHEMA_V1
    assert ctx["queue_root"] == str(queue.root.resolve())
    assert ctx["action_key"] == result["action_key"]
    assert ctx["published_unix"] == result["published_unix"]
    assert ctx["attempt"] == result["attempt"]
    assert ctx["generation"] == result["generation"]
    assert ctx["worker"] == ctx["incarnation"] == result["worker_id"]
    assert ctx["host"] == result["host"]
    assert ctx["resources"] == {"cpu": 1, "mem_gb": 1}
    assert ctx["resources_semantics"] == "selected-claim-sealed-demand"
    assert ctx["attempt_source"] == "selected-immutable-attempt"
    assert ctx["helper_root"] == str(tmp_path / "generation")
    assert ctx["receipt_sha256"] == receipt["receipt_sha256"]
    assert ctx["scope_attestation_sha256"] == pb.raw_sha256(proof_path.read_bytes())
    assert "b" * 64 not in json.dumps(ctx)
    assert "/run/prismabuild/resources.sock" not in json.dumps(ctx)
    ctx["resources"]["cpu"] = 99
    assert _native_read(action, queue, record)["producer_context"]["resources"]["cpu"] == 1


@pytest.mark.parametrize("field,value", [
    ("claimed_by", "foreign:1:incarnation"), ("claimed_host", "foreign"),
    ("resources", {"cpu": True, "mem_gb": 1}),
    ("resources", {"cpu": 2, "mem_gb": 1}),
    ("resource_scope", {}), ("resource_scope_cleanup", {}),
    ("cpu_allocation", {"preferred": [1], "fallback": []}),
    ("worker_script", "/foreign/tools/prismabuild_worker.py"),
])
def test_selected_native_claim_metadata_refuses(tmp_path, monkeypatch, field, value):
    action, queue, record, _receipt, _proof = _native_fixture(tmp_path, monkeypatch)
    row = {**record, field: value}
    queue.item_path(pool.DONE, action["action_key"]).write_bytes(pb._canonical_file_bytes(row))
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)


@pytest.mark.parametrize("field,value", [
    ("schema", "legacy"), ("action_key", "b" * 64), ("nonce", "b" * 32),
    ("scope_id", "foreign"), ("host", "foreign"), ("worker", "foreign"),
    ("incarnation", "clock-tag-only"), ("scope_empty", False), ("empty", False),
    ("tickets_pending", None), ("released", 1), ("stopped_unix", 10 ** 400),
])
def test_selected_native_scope_proof_refuses(tmp_path, monkeypatch, field, value):
    action, queue, record, _receipt, path = _native_fixture(tmp_path, monkeypatch)
    proof = json.loads(path.read_text())
    proof[field] = value
    path.write_bytes(pb._canonical_file_bytes(proof))
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)


@pytest.mark.parametrize("field,value", [
    ("complete", 1), ("action_key", "b" * 64), ("nonce", "b" * 32),
    ("scope_unit", "foreign"), ("host", "foreign"), ("memory_max_bytes", 1),
])
def test_selected_immutable_native_telemetry_refuses(tmp_path, monkeypatch, field, value):
    action, queue, record, _receipt, _proof = _native_fixture(tmp_path, monkeypatch)
    path = queue.attempt_path(record, 1)
    attempt = json.loads(path.read_text())
    attempt["detail"]["resource_telemetry"][field] = value
    _rewrite_readonly(path, pb._canonical_file_bytes(attempt))
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)


@pytest.mark.parametrize("mode", ["missing", "oversized", "symlink", "changed"])
def test_native_proof_is_bounded_nofollow_and_rechecked(tmp_path, monkeypatch, mode):
    from prismabuild import action_result

    action, queue, record, _receipt, path = _native_fixture(tmp_path, monkeypatch)
    if mode == "missing":
        path.unlink()
    elif mode == "oversized":
        path.write_bytes(b" " * (4 * 1024 * 1024 + 1))
    elif mode == "symlink":
        other = path.with_suffix(".saved")
        path.rename(other)
        path.symlink_to(other)
    else:
        original = action_result._read_bounded
        changed = False
        def read_and_replace(p, **kwargs):
            nonlocal changed
            raw = original(p, **kwargs)
            if kwargs["where"] == "native producer scope attestation" and not changed:
                changed = True
                proof = json.loads(raw)
                proof["unix"] = 99.0
                path.write_bytes(pb._canonical_file_bytes(proof))
            return raw
        monkeypatch.setattr(action_result, "_read_bounded", read_and_replace)
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)


def test_legacy_and_cache_result_generic_reads_remain_available(tmp_path):
    action, queue, record, receipt, payload = _fixture(tmp_path)
    assert client.read_verified_action_result(
        queue, action["action_key"], published_unix=record["published_unix"],
        attempt=1, max_result_bytes=4096)["payload"] == payload
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "checkout").mkdir()
    action = _standard_action(cache / "checkout", command=["/bin/echo", "cached"])
    _cas, receipt = _publish_result(cache, action, payload)
    queue, record = _finish(cache, action, receipt,
                           stdout=json.dumps({"status": "cache-hit", "receipt": receipt}))
    assert client.read_verified_action_result(
        queue, action["action_key"], published_unix=record["published_unix"],
        attempt=1, max_result_bytes=4096)["payload"] == payload
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)


@pytest.mark.parametrize("value", [1, None, "true"])
def test_native_requirement_is_exact_boolean(tmp_path, value):
    action, queue, record, _receipt, _payload = _fixture(tmp_path)
    with pytest.raises(client.ActionResultError, match="must be a boolean"):
        client.read_verified_action_result(
            queue, action["action_key"], published_unix=record["published_unix"],
            attempt=1, max_result_bytes=4096, require_native_producer_context=value)


@pytest.mark.parametrize("mode", ["tampered", "writable", "symlink", "changed"])
def test_attested_native_runtime_sources_refuse_mutation(tmp_path, monkeypatch, mode):
    from prismabuild import action_result

    action, queue, record, receipt, _proof = _native_fixture(tmp_path, monkeypatch)
    source = Path(receipt["producer"]["runtime"]["core"]["path"])
    if mode == "writable":
        source.chmod(0o644)
    elif mode == "tampered":
        source.chmod(0o644)
        source.write_bytes(b"changed source\n")
        source.chmod(0o444)
    elif mode == "symlink":
        other = source.with_suffix(".saved")
        source.rename(other)
        source.symlink_to(other)
    else:
        original = action_result._read_bounded
        changed = False
        def read_and_replace(path, **kwargs):
            nonlocal changed
            raw = original(path, **kwargs)
            if Path(path) == source and not changed:
                changed = True
                source.chmod(0o644)
                source.write_bytes(b"replaced after first bounded read\n")
                source.chmod(0o444)
            return raw
        monkeypatch.setattr(action_result, "_read_bounded", read_and_replace)
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)


def test_selected_native_ending_extra_fields_are_rechecked(tmp_path, monkeypatch):
    from prismabuild import action_result

    action, queue, record, _receipt, _proof = _native_fixture(tmp_path, monkeypatch)
    original = action_result._read_bounded
    changed = False
    def read_and_replace(path, **kwargs):
        nonlocal changed
        raw = original(path, **kwargs)
        if kwargs["where"] == "native producer scope attestation" and not changed:
            changed = True
            row = json.loads(queue.item_path(pool.DONE, action["action_key"]).read_text())
            row["resources"]["cpu"] = 2
            queue.item_path(pool.DONE, action["action_key"]).write_bytes(pb._canonical_file_bytes(row))
        return raw
    monkeypatch.setattr(action_result, "_read_bounded", read_and_replace)
    with pytest.raises(client.ActionResultError, match="selected native ending changed"):
        _native_read(action, queue, record)


@pytest.mark.parametrize("change", ["withdrawn", "replacement"])
def test_native_last_proof_read_cannot_return_before_terminal_recheck(
    tmp_path, monkeypatch, change
):
    from prismabuild import action_result

    action, queue, record, _receipt, _proof = _native_fixture(tmp_path, monkeypatch)
    original = action_result._read_bounded
    proof_reads = 0
    def read_and_change_terminal(path, **kwargs):
        nonlocal proof_reads
        raw = original(path, **kwargs)
        if kwargs["where"] == "native producer scope attestation":
            proof_reads += 1
            if proof_reads == 2:
                key = action["action_key"]
                if change == "withdrawn":
                    withdrawn = {"schema": pool.POOL_OUTCOME_SCHEMA_V1,
                        "action_key": key, "status": "withdrawn",
                        "published_unix": record["published_unix"],
                        "withdrawn_unix": record["published_unix"] + 5,
                        "finished_host": "box", "detail": {}}
                    pb._atomic_publish(queue.item_path(pool.WITHDRAWN, key),
                                       pb._canonical_file_bytes(withdrawn))
                else:
                    replacement = {**record, "published_unix": record["published_unix"] + 5}
                    queue.item_path(pool.DONE, key).write_bytes(pb._canonical_file_bytes(replacement))
        return raw
    monkeypatch.setattr(action_result, "_read_bounded", read_and_change_terminal)
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)
    assert proof_reads == 2


@pytest.mark.parametrize("field,value", [
    ("cgroup_path", None), ("cgroup_path", "/sys/fs/cgroup/foreign"),
    ("socket_path", None), ("socket_path", "/foreign/socket"),
    ("token", None), ("token", "not-a-broker-token"), ("token", "c" * 64),
])
def test_native_full_broker_control_must_join_immutable_launch(
    tmp_path, monkeypatch, field, value
):
    action, queue, record, _receipt, _proof = _native_fixture(tmp_path, monkeypatch)
    row = json.loads(queue.item_path(pool.DONE, action["action_key"]).read_text())
    if value is None:
        row["resource_scope"].pop(field)
    else:
        row["resource_scope"][field] = value
    queue.item_path(pool.DONE, action["action_key"]).write_bytes(pb._canonical_file_bytes(row))
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)


@pytest.mark.parametrize("position", [None, 1, 3, 5, 7, 9, 10, 11, 13, 15, 17, 18, 19, 20, 21, 22])
def test_native_immutable_argv_binds_proxy_scope_secret_affinity_launcher_request_cas(
    tmp_path, monkeypatch, position
):
    action, queue, record, _receipt, _proof = _native_fixture(tmp_path, monkeypatch)
    path = queue.attempt_path(record, 1)
    attempt = json.loads(path.read_text())
    if position is None:
        attempt["detail"].pop("argv")
    else:
        attempt["detail"]["argv"][position] = "foreign-value"
    _rewrite_readonly(path, pb._canonical_file_bytes(attempt))
    with pytest.raises(client.ActionResultError):
        _native_read(action, queue, record)
