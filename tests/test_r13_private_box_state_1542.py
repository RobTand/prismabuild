"""The R13 bench keeps its admission state in its work directory (#1542).

``bind_private_box_state`` points ``PRISMABUILD_BOX_STATE_ROOT`` at
``<work>/box-state`` before the first queue use, in parent and child, and
keeps an explicit override untouched. The run mints its digest set there,
never in the fleet's own directory. Unresolved claims and census fences
survive the run inside the work directory.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "bench_tier_cycle_r13", ROOT / "tools" / "fleet" / "bench_tier_cycle_r13.py")
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)


def test_bind_keeps_an_explicit_root(tmp_path, monkeypatch):
    explicit = tmp_path / "explicit"
    explicit.mkdir()
    monkeypatch.setenv("PRISMABUILD_BOX_STATE_ROOT", str(explicit))
    assert bench.bind_private_box_state(tmp_path / "work") == str(explicit)
    assert os.environ["PRISMABUILD_BOX_STATE_ROOT"] == str(explicit)


def test_bind_points_a_bare_process_at_the_work_directory(tmp_path, monkeypatch):
    monkeypatch.delenv("PRISMABUILD_BOX_STATE_ROOT", raising=False)
    bound = bench.bind_private_box_state(tmp_path / "work")
    assert bound == str((tmp_path / "work").resolve() / "box-state")
    assert Path(bound).is_dir()
    assert os.environ["PRISMABUILD_BOX_STATE_ROOT"] == bound


def test_setup_and_cycles_touch_only_the_private_root(tmp_path):
    child = tmp_path / "guard-child.py"
    result_path = tmp_path / "child-result.json"
    child.write_text(
        "import json, os, runpy, sys\n"
        "from pathlib import Path\n"
        "bench, work, result = sys.argv[1:4]\n"
        "prod = os.path.normpath('/tmp/prismabuild-admission-%d' % os.getuid())\n"
        "attempts = []\n"
        "def hook(event, args):\n"
        "    for raw in (args[:1] if event in ('open', 'os.open', 'os.mkdir', 'os.listdir', 'os.scandir') else []):\n"
        "        try:\n"
        "            path = os.path.normpath(raw if isinstance(raw, str) else os.fspath(raw))\n"
        "        except TypeError:\n"
        "            continue\n"
        "        if not os.path.isabs(path):\n"
        "            path = os.path.normpath(os.path.join(os.getcwd(), path))\n"
        "        if path == prod or path.startswith(prod + '/'):\n"
        "            if event in ('os.listdir', 'os.scandir'):\n"
        "                continue\n"
        "            attempts.append([event, path])\n"
        "            raise RuntimeError('production-root write: %s %s' % (event, path))\n"
        "sys.addaudithook(hook)\n"
        "box = str(Path(work) / 'box-state')\n"
        "os.environ['PRISMABUILD_BOX_STATE_ROOT'] = box\n"
        "import worker_loop\n"
        "worker_loop.private_claim_parameters = lambda queue: {\n"
        "    'capacity': {'cpu': 2, 'mem_gb': 4},\n"
        "    'cpu_tiers': {'preferred': [0, 1], 'fallback': []},\n"
        "    'adaptive_cpu': False}\n"
        "sys.argv = ['bench_tier_cycle_r13.py', '--work', work, '--out', str(Path(work).parent / 'out'),\n"
        "            '--cycles', '1', '--instances', '0', '--dead-owner-pairs', '0',\n"
        "            '--dead-entries', '2', '--write-only-scopes', '1',\n"
        "            '--write-only-batches', '1', '--write-only-paths', '1', '--tiny-shape']\n"
        "try:\n"
        "    runpy.run_path(bench, run_name='__main__')\n"
        "except SystemExit as exc:\n"
        "    code = exc.code\n"
        "else:\n"
        "    code = 0\n"
        "record = {'code': code, 'attempts': attempts, 'box': box,\n"
        "          'box_files': sorted(p.name for p in Path(box).iterdir()) if Path(box).is_dir() else []}\n"
        "Path(result).write_text(json.dumps(record))\n")
    env = dict(os.environ)
    env.pop("PRISMABUILD_BOX_STATE_ROOT", None)
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + str(ROOT / "tools" / "fleet") + os.pathsep + str(ROOT / "tests")
    # The bench refuses /tmp; keep its work and output in private RAM scratch.
    with tempfile.TemporaryDirectory(prefix="pb1542-r13-test-", dir="/dev/shm") as parent:
        work = Path(parent) / "work"
        work.mkdir()
        completed = subprocess.run([sys.executable, str(child), str(ROOT / "tools" / "fleet" / "bench_tier_cycle_r13.py"),
                                    str(work), str(result_path)], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=600)
        assert result_path.exists(), completed.stdout[-2000:] + completed.stderr[-2000:]
        record = json.loads(result_path.read_text())
        assert record["code"] == 0, completed.stderr[-2000:]
        assert record["attempts"] == [], record["attempts"]
        assert record["box_files"], "the bench filed no admission state in its work directory"
        assert any(name.endswith(".lock") for name in record["box_files"]), record["box_files"]
    assert not work.exists(), "the R13 test left its work directory behind"
    assert not Path(parent).exists(), "the R13 test left its temporary parent behind"


@pytest.fixture
def reused_work():
    # The bench refuses /tmp; /dev/shm provides RAM scratch for this gate test.
    with tempfile.TemporaryDirectory(prefix="pb1542-r13-reuse-", dir="/dev/shm") as parent:
        work = Path(parent) / "work"
        work.mkdir()
        yield work


@pytest.mark.parametrize("relative", [
    "pb-queue/claimed/" + "ab12" * 16 + ".json",
    "pb-queue/claimed/" + "ab12" * 16 + ".lease",
    "box-state/" + "cd34" * 16 + ".measurement-reader-v1",
    "box-state/" + "cd34" * 16 + ".measurement-reader-v1.guard",
    "box-state/" + "cd34" * 16 + ".measurement-reader-v1.writing",
    "box-state/" + "cd34" * 16 + ".adaptive-cpu-v1/claim-denials.json",
    "previous-run.json",
])
def test_reuse_refuses_existing_state_without_deletion(reused_work, monkeypatch, relative):
    evidence = reused_work / relative
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text('{"custody": "unresolved"}')
    before = {path.relative_to(reused_work): path.lstat().st_ino
              for path in reused_work.rglob("*")}
    out = reused_work.parent / "out"

    def queue_must_not_start(*args, **kwargs):
        raise AssertionError("the bench used the queue before it refused retained state")

    monkeypatch.setattr(bench.pool, "PoolQueue", queue_must_not_start)
    with pytest.raises(SystemExit, match="refusing.*" + str(reused_work)):
        bench.main(["--work", str(reused_work), "--out", str(out), "--tiny-shape"])
    assert evidence.read_text() == '{"custody": "unresolved"}'
    assert {path.relative_to(reused_work): path.lstat().st_ino
            for path in reused_work.rglob("*")} == before
    assert not out.exists()


def test_reuse_refuses_an_unreadable_work_directory(reused_work, monkeypatch):
    evidence = reused_work / "retained.json"
    evidence.write_text("keep")
    inode = evidence.stat().st_ino
    real_scandir = os.scandir

    def unreadable(path):
        if path == reused_work:
            raise PermissionError("the work census is unavailable")
        return real_scandir(path)

    def queue_must_not_start(*args, **kwargs):
        raise AssertionError("the bench used the queue without a complete work census")

    monkeypatch.setattr(os, "scandir", unreadable)
    monkeypatch.setattr(bench.pool, "PoolQueue", queue_must_not_start)
    with pytest.raises(SystemExit, match="refusing.*" + str(reused_work)):
        bench.main(["--work", str(reused_work), "--out", str(reused_work.parent / "out")])
    assert evidence.read_text() == "keep"
    assert evidence.stat().st_ino == inode
