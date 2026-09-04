"""A multiline shell payload is an ordinary submission (issue #21)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun_multiline_argv",
    Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py",
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)  # type: ignore[union-attr]

SCRIPT = "set -e\nprintf 'line1\\nline2\\n'\n\techo tabbed\n"


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
            capture_output=True, text=True, check=False,
        )
        assert completed.returncode == 0, completed.stderr
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky", tags=["sparky", "gb10"], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    return work, queue


def _submit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, work: Path) -> int:
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(
        sys, "argv",
        ["pbrun.py", "--cwd", str(work), "--wait-s", "0.01",
         "--", "/bin/bash", "-lc", SCRIPT],
    )
    return pbrun.main()


def test_a_multiline_shell_script_is_queued_verbatim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work, queue = _work_and_queue(tmp_path)

    assert _submit(tmp_path, monkeypatch, work) == 75   # queued, nobody served it

    ready = list(queue.dir(pool.READY).glob("*.json"))
    assert len(ready) == 1
    item = json.loads(ready[0].read_text(encoding="utf-8"))
    key = str(item["action_key"])
    request = Path(str(item["cas_root"])) / "requests" / key[:2] / f"{key}.json"
    action = json.loads(request.read_text(encoding="utf-8"))
    # pbrun wraps the command in its own ``bash -lc`` line (PATH, tee of the
    # result); the script must survive inside it, shell-quoted, byte for byte.
    assert shlex.quote(SCRIPT) in action["task"]["argv"][-1]


def test_a_refused_contract_is_a_named_refusal_not_a_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work, queue = _work_and_queue(tmp_path)

    def refuse(_body):
        raise pb.ActionContractError("action.task.argv[2] is not acceptable")

    monkeypatch.setattr(pbrun.pb, "seal_action", refuse)
    with pytest.raises(SystemExit, match=r"^pbrun: refusing to seal the action: "):
        _submit(tmp_path, monkeypatch, work)
    assert list(queue.dir(pool.READY).glob("*.json")) == []
