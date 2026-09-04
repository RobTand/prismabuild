"""pbrun owns Docker payloads after they leave the process tree."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402


def _submission_variables(argv, monkeypatch, tmp_path):
    captured = []

    class Stop(Exception):
        pass

    def stop(body, *_args, **_kwargs):
        captured.append(body)
        raise Stop()

    monkeypatch.setattr(pbrun.pb, "seal_action", stop)
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--cwd", str(tmp_path), *argv])
    with pytest.raises(Stop):
        pbrun.main()
    return captured[0]["environment"]["variables"]


def test_every_submission_gets_a_sealed_container_owner(
    monkeypatch, tmp_path: Path
) -> None:
    variables = _submission_variables(["--", "true"], monkeypatch, tmp_path)
    owner = variables["PRISMABUILD_CONTAINER_OWNER"]
    marker = variables["PRISMABUILD_CONTAINER_MARKER"]
    assert len(owner) == 64 and set(owner) <= set("0123456789abcdef")
    assert marker.endswith(f"/container-owners/{owner}.used")
    assert variables["PATH"].split(":")[0] == str(pbrun.RUNTIME_ROOT / "tools")


def test_pool_item_carries_the_same_container_owner(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    owner = "a" * 64
    queue.publish(
        action_key="b" * 64,
        cas_root="/cas",
        checkout_root="/checkout",
        worker_script="/worker.py",
        container_owner=owner,
    )
    item = json.loads(queue.item_path(pool.READY, "b" * 64).read_text())
    assert item["container_owner"] == owner


def test_docker_shim_marks_and_labels_a_created_container(tmp_path: Path) -> None:
    shim = ROOT / "tools" / "fleet" / "docker"
    called = tmp_path / "called.json"
    real = tmp_path / "real-docker"
    real.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib, sys\n"
        "pathlib.Path(os.environ['CALLED']).write_text(json.dumps(sys.argv[1:]))\n"
    )
    real.chmod(0o755)
    owner = "1" * 64
    marker = tmp_path / "owner.used"
    environment = dict(os.environ)
    environment.update({
        "CALLED": str(called),
        "PRISMABUILD_CONTAINER_OWNER": owner,
        "PRISMABUILD_CONTAINER_MARKER": str(marker),
        "PRISMABUILD_DOCKER_REAL": str(real),
        "PRISMABUILD_DOCKER_TESTING": "1",
    })

    result = subprocess.run(
        [str(shim), "run", "--rm", "example:image"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert marker.read_text().strip() == owner
    argv = json.loads(called.read_text())
    assert argv[:3] == ["run", "--label", f"prismabuild.action={owner}"]
