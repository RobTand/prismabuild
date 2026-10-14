"""Private callers keep the real admission boundary and shared observation."""
import json
import os
import subprocess
import sys
from pathlib import Path
import time

import pytest

from prismabuild import pool
import worker_loop
import bench_tier_cycle_r13 as cycle
import test_prepaid_writer_integration as funding

_isolated_synthetic_launch_context = funding._isolated_synthetic_launch_context
ROOT = Path(__file__).resolve().parents[1]
TIERS = {"preferred": [0, 1], "fallback": []}
CAPACITY = {"cpu": 2, "mem_gb": 4}


def test_private_parameters_observe_the_actual_ledger_and_keep_admission(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    seen = []
    monkeypatch.setattr(worker_loop.cpu_topology, "inherited_tiers", lambda: TIERS)
    monkeypatch.setattr(worker_loop, "declared_host_capacity", lambda args, cores: CAPACITY)
    class Observer:
        def __init__(self, *, samples, ledger_total):
            assert samples > 0 and ledger_total == {}
            self.last_offer_external_gib = 0
        def offer(self, declared, held, **overrides):
            assert callable(held) and held() == queue.ledger().held()
            assert declared == {"gpu": 0, **CAPACITY}
            seen.append(overrides)
            return {"cpu": 2, "mem_gb": 3}
    monkeypatch.setattr(worker_loop.box_capacity, "CapacityObserver", Observer)
    parameters = worker_loop.private_claim_parameters(queue)
    assert parameters == {"capacity": {"cpu": 2, "mem_gb": 3},
                          "cpu_tiers": TIERS, "adaptive_cpu": True,
                          "observed_external_gib": 0}
    assert len(seen) == 1 and seen[0]["gpu_sample"] is None
    monkeypatch.setattr(pool.cpu_admission.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": 2, "interval_s": 1.})
    key = "a" * 64
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1})
    assert queue.claim(**parameters)["action_key"] == key
    assert queue.ledger().holder_tokens(key) == {"cpu": 1, "mem_gb": 1}
    queue.finish(key, status="executed")
    assert queue.ledger().held_keys() == []


def test_private_parameters_refuse_unknown_inherited_topology(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(worker_loop.cpu_topology, "inherited_tiers", lambda: None)
    with pytest.raises(pool.PoolContractError, match="known inherited CPU topology"):
        worker_loop.private_claim_parameters(queue)
    assert queue.ledger().held_keys() == []


def test_private_write_only_owner_claims_and_holds_real_host_tokens(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(worker_loop, "private_claim_parameters", lambda queue: {
        "capacity": CAPACITY, "cpu_tiers": TIERS, "adaptive_cpu": False})
    template = cycle._write_only_template(tmp_path / "out")
    key = "b" * 64
    instance = cycle._bind_owner(queue, template, key)
    assert instance["owner_action_key"] == key
    assert queue.item_path(pool.CLAIMED, key).exists()
    assert queue.ledger().holder_tokens(key) == {"cpu": 1, "mem_gb": 1}
    queue.finish(key, status="executed")
    assert queue.ledger().held_keys() == [key]
    assert queue.item_path(pool.CLAIMED, key).exists()
    assert queue.ledger().holder_tokens(key) == {"cpu": 1, "mem_gb": 1}

_BOX_STATE_ENV = "PRISMABUILD_BOX_STATE_ROOT"

_BENCH = ROOT / "tools" / "fleet" / "bench_claim_pass.py"

#: Fresh-subprocess driver. It installs an audit guard before runpy starts
#: the real benchmark entry point. The guard refuses mutating calls under
#: the production admission root and records them. Read-only opens pass.
#: It never imports pool or conftest first. Such an import would bind the
#: wrong root too early.
_CHILD_SOURCE = '''import contextlib
import io
import json
import os
import runpy
import sys
import traceback
from pathlib import Path

KEY = "PRISMABUILD_BOX_STATE_ROOT"
ONE_PATH = ("open", "os.open", "os.mkdir", "os.remove", "os.unlink",
            "os.rmdir", "os.chmod", "os.truncate", "os.listdir", "os.scandir")
TWO_PATH = ("os.rename", "os.replace", "os.link", "os.symlink")


def norm(raw):
    try:
        raw = os.fspath(raw)
    except TypeError:
        return None
    if isinstance(raw, bytes):
        try:
            raw = raw.decode()
        except (ValueError, OSError):
            return None
    if not isinstance(raw, str):
        return None
    if not os.path.isabs(raw):
        raw = os.path.join(os.getcwd(), raw)
    return os.path.normpath(raw)


def open_is_read_only(event, args):
    if event == "os.open":
        flags = args[1] if len(args) > 1 else None
        if not isinstance(flags, int):
            return False
        return not flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT
                            | os.O_TRUNC | os.O_APPEND)
    if event == "open":
        mode = args[1] if len(args) > 1 else None
        flags = args[2] if len(args) > 2 else 0
        if isinstance(mode, str) and any(
                char in mode for char in "wax+"):
            return False
        if isinstance(flags, int) and flags & (
                os.O_WRONLY | os.O_RDWR | os.O_CREAT
                | os.O_TRUNC | os.O_APPEND):
            return False
        if isinstance(mode, str) or isinstance(flags, int):
            return True
        return False
    return False


def main():
    bench, checkout, work, result_path = sys.argv[1:5]
    prod = os.path.normpath("/tmp/prismabuild-admission-%d" % os.getuid())
    prod_slash = prod + "/"
    attempts = []
    tmp_ops = []

    def names(event, args):
        if event in ONE_PATH:
            return args[:1]
        if event == "os.utime":
            return args[:1]
        if event in TWO_PATH:
            return args[:2]
        return []

    def hook(event, args):
        for raw in names(event, args):
            path = norm(raw)
            if path is None:
                continue
            if path == prod or path.startswith(prod_slash):
                if event in ("os.listdir", "os.scandir"):
                    continue
                if event in ("open", "os.open") and open_is_read_only(
                        event, args):
                    continue
                attempts.append([event, path])
                raise RuntimeError(
                    "refused production-root write: %s %s" % (event, path))
            elif (event in ("open", "os.open", "os.mkdir")
                    and path.startswith("/tmp/")):
                tmp_ops.append([event, path])

    sys.addaudithook(hook)


    record = {"attempts": attempts, "tmp_ops": tmp_ops, "prod": prod}
    error = None
    output = io.StringIO()
    sys.argv = ["bench_claim_pass.py", "--work", work, "--checkout",
                checkout, "--ready", "1", "--claimed", "0", "--passes", "0"]
    try:
        with contextlib.redirect_stdout(output):
            runpy.run_path(bench, run_name="__main__")
    except SystemExit as exc:
        if exc.code:
            error = "benchmark exited with status %r" % (exc.code,)
    except BaseException:
        error = traceback.format_exc()
    record["stdout"] = output.getvalue()
    record["error"] = error
    rows = []
    for line in record["stdout"].splitlines():
        text = line.strip()
        if text.startswith("{"):
            try:
                rows.append(json.loads(text))
            except ValueError:
                record.setdefault("unparsed", []).append(text)
    record["poll_rows"] = rows
    module = sys.modules.get("prismabuild.adaptive_cpu")
    effective = None
    if module is not None:
        effective = str(getattr(module, "BOX_STATE_ROOT", ""))
    record["effective_root"] = effective
    record["root_exists_after"] = Path(effective).exists() if effective else None
    record["env_present"] = KEY in os.environ
    record["env_value"] = os.environ.get(KEY)
    try:
        from prismabuild import pool
        queue = pool.PoolQueue(Path(work) / "pb-queue")
        record["ready_count"] = len(queue.ready_items())
        record["held_keys"] = sorted(queue.ledger().held_keys())
    except BaseException:
        record["queue_error"] = traceback.format_exc()
    Path(result_path).write_text(json.dumps(record, indent=1))
    return 1 if error else 0


raise SystemExit(main())
'''


def _run_guarded_bench(tmp_path, mode):
    child = tmp_path / ("guard-child-%s.py" % mode)
    child.write_text(_CHILD_SOURCE)
    work = tmp_path / ("bench-work-%s" % mode)
    result = tmp_path / ("bench-result-%s.json" % mode)
    env = dict(os.environ)
    explicit = None
    if mode == "absent":
        env.pop(_BOX_STATE_ENV, None)
    elif mode == "empty":
        env[_BOX_STATE_ENV] = ""
    else:
        explicit = tmp_path / "explicit-root"
        explicit.mkdir()
        (explicit / "sentinel.json").write_bytes(b'{"owner": "explicit"}')
        env[_BOX_STATE_ENV] = str(explicit)
    completed = subprocess.run(
        [sys.executable, str(child), str(_BENCH), str(ROOT),
         str(work), str(result)],
        cwd=tmp_path, env=env, capture_output=True, text=True,
        timeout=240)
    if result.exists():
        record = json.loads(result.read_text())
    else:
        record = {}
    assert completed.returncode == 0, (
        completed.stderr[-1000:]
        + "\nchild-error=" + str(record.get("error"))[-3000:]
        + "\nattempts=" + str(record.get("attempts")))
    assert record["error"] is None, record["error"]
    return record, explicit


def _assert_two_foreign_polls(record):
    assert "queue_error" not in record, record["queue_error"]
    rows = record["poll_rows"]
    assert [row["poll"] for row in rows] == [1, 2]
    for row in rows:
        assert row["snapshot"] == 1
        assert row["claimed"] is None


def test_bench_claim_pass_absent_override_uses_owned_temporary_root(tmp_path):
    record, _ = _run_guarded_bench(tmp_path, "absent")
    assert record["attempts"] == [], record["attempts"]
    _assert_two_foreign_polls(record)
    effective = record["effective_root"]
    assert effective.startswith("/tmp/")
    assert effective != record["prod"]
    assert record["root_exists_after"] is False
    assert record["held_keys"] == []
    assert record["ready_count"] == 1
    assert record["env_present"] is False
    mkdirs = [path for event, path in record["tmp_ops"] if event == "os.mkdir"]
    assert any(path == effective or path.startswith(effective + "/")
               for path in mkdirs)


def test_bench_claim_pass_empty_override_uses_owned_temporary_root(tmp_path):
    record, _ = _run_guarded_bench(tmp_path, "empty")
    assert record["attempts"] == [], record["attempts"]
    _assert_two_foreign_polls(record)
    effective = record["effective_root"]
    assert effective.startswith("/tmp/")
    assert effective != record["prod"]
    assert record["root_exists_after"] is False
    assert record["held_keys"] == []
    assert record["ready_count"] == 1
    assert record["env_present"] is True
    assert record["env_value"] == ""
    mkdirs = [path for event, path in record["tmp_ops"] if event == "os.mkdir"]
    assert any(path == effective or path.startswith(effective + "/")
               for path in mkdirs)


def test_bench_claim_pass_explicit_override_stays_intact(tmp_path):
    record, explicit = _run_guarded_bench(tmp_path, "explicit")
    assert record["attempts"] == [], record["attempts"]
    _assert_two_foreign_polls(record)
    assert record["effective_root"] == str(explicit)
    assert record["root_exists_after"] is True
    assert (explicit / "sentinel.json").read_bytes() == b'{"owner": "explicit"}'
    assert record["env_value"] == str(explicit)
    assert record["held_keys"] == []
    assert record["ready_count"] == 1
