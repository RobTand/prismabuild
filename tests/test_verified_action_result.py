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
    log_name: str = "pbrun_result.txt",
    result_path: str | None = None,
    path_prefix: str = PATH_PREFIX,
    declared_path: str | None = None,
    inputs: list[dict[str, object]] | None = None,
    determinism: str = "deterministic",
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
        "params": {"command": list(command)},
        "environment": {
            "variables": {"PATH": declared_path or f"{path_prefix}:/usr/bin:/bin"},
            "toolchain": {},
        },
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })


def _publish_result(base: Path, action, payload: bytes):
    cas = pb.PrismaBuildCAS(base / "cas")
    cas.publish_action_request(action)
    attestation = pb.preflight_action(
        action, cas_root=base / "cas", checkout_root=base / "checkout")
    output = base / "output.bin"
    output.write_bytes(payload)
    receipt, _won = cas.publish_result(
        action, output, attestation=attestation, return_execution_receipt=True)
    return cas, receipt


def _finish(base: Path, action, receipt, *, status: str = "executed",
            stdout: str | None = None, returncode: int = 0):
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
                  worker_script=base / "worker.py",
                  tags=["x86"], resources={"cpu": 1, "mem_gb": 1})
    queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    queue.finish(key, status=status,
                 detail={"returncode": returncode, "status": status,
                         "stdout": stdout, "stderr": ""})
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
    # A sparse file: nominally enormous, no blocks allocated.  A reader without
    # a cap would allocate and hash the whole declared size before refusing.
    receipt_path.chmod(0o644)
    with receipt_path.open("r+b") as handle:
        handle.truncate(1 << 34)
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
