"""pbrun retries require a contract distinct from numeric determinism."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun_retry_contract",
    Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py",
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)  # type: ignore[union-attr]


def _work_and_queue(tmp_path: Path) -> tuple[Path, pool.PoolQueue]:
    work = tmp_path / "work"
    work.mkdir()
    (work / "seed.txt").write_text("sealed\n", encoding="utf-8")
    for args in (
        ("init", "-q"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "PrismaBuild test"),
        ("add", "seed.txt"),
        ("commit", "-qm", "sealed tree"),
    ):
        completed = subprocess.run(
            ["git", "-C", str(work), *args],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky",
        tags=["sparky", "gb10"],
        has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    return work, queue


def _submit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    work: Path,
    *options: str,
) -> int:
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pbrun.py",
            "--cwd",
            str(work),
            "--wait-s",
            "0.01",
            *options,
            "--",
            "/bin/bash",
            "-lc",
            "printf artifact > \"$1\"; exit 9",
            "pbrun-test",
            str(tmp_path / "external-output.bin"),
        ],
    )
    return pbrun.main()


def test_a_stochastic_external_output_defaults_to_one_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure after a side effect must not reacquire a worker by default.

    main: first failure returns the full item to ``ready`` and ``serve_once``
    claims it again.  Branch: the default policy files it after one attempt.
    """

    work, queue = _work_and_queue(tmp_path)
    assert _submit(tmp_path, monkeypatch, work) == 75
    ready = list(queue.dir(pool.READY).glob("*.json"))
    assert len(ready) == 1
    item = json.loads(ready[0].read_text(encoding="utf-8"))
    assert item["max_attempts"] == 1

    first = queue.serve_once(
        tags=["sparky"],
        python=sys.executable,
        timeout_s=30.0,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    assert first is not None and first["status"] == "failed"
    assert (tmp_path / "external-output.bin").read_text(encoding="utf-8") == "artifact"
    assert queue.item_path(pool.FAILED, str(item["action_key"])).exists()
    assert not queue.item_path(pool.READY, str(item["action_key"])).exists()

    second = queue.serve_once(
        tags=["sparky"],
        python=sys.executable,
        timeout_s=30.0,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    assert second is None, "a failed default action reacquired fleet capacity"


def test_deterministic_does_not_authorize_external_side_effect_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work, queue = _work_and_queue(tmp_path)

    with pytest.raises(SystemExit) as caught:
        _submit(
            tmp_path,
            monkeypatch,
            work,
            "--deterministic",
            "--max-attempts",
            "2",
        )

    assert "--retry-safe" in str(caught.value)
    assert list(queue.dir(pool.READY).glob("*.json")) == []


def test_pool_refuses_an_explicitly_unsafe_retry_bound(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    with pytest.raises(pool.PoolContractError, match="retry_safe"):
        queue.publish(
            action_key="f" * 64,
            cas_root="/cas",
            checkout_root="/checkout",
            worker_script="/worker.py",
            max_attempts=2,
            retry_safe=False,
        )


def test_retry_safe_policy_is_explicit_bounded_and_sealed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work, queue = _work_and_queue(tmp_path)
    assert _submit(
        tmp_path,
        monkeypatch,
        work,
        "--retry-safe",
        "--max-attempts",
        "2",
    ) == 75

    ready_path = next(queue.dir(pool.READY).glob("*.json"))
    item = json.loads(ready_path.read_text(encoding="utf-8"))
    assert item["max_attempts"] == 2
    request = json.loads(
        (
            tmp_path
            / "cas"
            / "requests"
            / ready_path.stem[:2]
            / f"{ready_path.stem}.json"
        ).read_text(encoding="utf-8")
    )
    assert request["task"]["determinism"] == "stochastic"
    assert request["params"]["retry_policy"] == {
        "max_attempts": 2,
        "retry_safe": True,
    }


def test_new_submitter_keeps_one_attempt_during_an_old_pool_runtime_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Publishing the repo checkout can precede the atomic worker rollout."""

    current_publish = pool.PoolQueue.publish

    def legacy_publish(
        self,
        *,
        action_key,
        cas_root,
        worker_script,
        checkout_root=None,
        checkout_snapshot=None,
        tags=(),
        needs_gpu=False,
        priority=0,
        resources=None,
        max_attempts=pool.DEFAULT_MAX_ATTEMPTS,
        container_owner=None,
    ):
        return current_publish(
            self,
            action_key=action_key,
            cas_root=cas_root,
            checkout_root=checkout_root,
            checkout_snapshot=checkout_snapshot,
            worker_script=worker_script,
            tags=tags,
            needs_gpu=needs_gpu,
            priority=priority,
            resources=resources,
            max_attempts=max_attempts,
            container_owner=container_owner,
        )

    monkeypatch.setattr(pool.PoolQueue, "publish", legacy_publish)
    work, queue = _work_and_queue(tmp_path)
    assert _submit(tmp_path, monkeypatch, work) == 75
    item = json.loads(
        next(queue.dir(pool.READY).glob("*.json")).read_text(encoding="utf-8")
    )
    assert item["max_attempts"] == 1
    assert "retry_safe" not in item
