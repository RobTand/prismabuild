"""Host-local shared snapshot lifecycle; no arbitrary execution opts in here.

Private integration seam for W3/W4: _shared_source_checkout(item, *,
local_checkout_root) yields a generation-pinned lease exposing checkout_root,
generation and preflight(action). _reap_shared_sources(*, local_checkout_root,
now) does bounded housekeeping. Neither is a public SDK extension.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from copy import deepcopy
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import threading
import time

import pytest

from test_producer_snapshot_is_proven import sealed, _git  # noqa: F401
from prismabuild import core, materialize
from prismabuild.digest_primitives import canonical_sha256


def _borrow():
    function = getattr(materialize, "_shared_source_checkout", None)
    assert callable(function), "#2119 requires generation-pinned shared-source leases"
    return function


def _reap(base, *, now=None):
    function = getattr(materialize, "_reap_shared_sources", None)
    assert callable(function), "#2119 requires dead-builder/dead-reader recovery"
    function(local_checkout_root=base, now=time.time() if now is None else now)


def _record(action, cas):
    return {"action_key": action["action_key"], "cas_root": str(cas.root),
            "checkout_snapshot": action["params"]["checkout_snapshot"]}


def test_concurrent_identical_snapshots_build_once_and_release_independently(
        sealed, tmp_path, monkeypatch):
    action, cas, _ = sealed
    borrow = _borrow()
    base = tmp_path / "local"
    item = _record(action, cas)
    lock = threading.Lock()
    counts = {"init": 0, "checkout": 0, "identity": 0}
    git = materialize._run_materializer_git
    identity = core.git_checkout_identity

    def run(argv, **kwargs):
        with lock:
            if "init" in argv:
                counts["init"] += 1
            if "checkout" in argv:
                counts["checkout"] += 1
        return git(argv, **kwargs)

    def identify(*args, **kwargs):
        with lock:
            counts["identity"] += 1
        return identity(*args, **kwargs)

    monkeypatch.setattr(materialize, "_run_materializer_git", run)
    monkeypatch.setattr(core, "git_checkout_identity", identify)
    ready = threading.Barrier(3)
    leaving = threading.Event()

    def reader(index):
        sibling = {**item, "action_key": canonical_sha256({"reader": index})}
        with borrow(sibling, local_checkout_root=base) as lease:
            lease.preflight(action)
            ready.wait(timeout=30)
            assert leaving.wait(30)
            assert (lease.checkout_root / "payload.txt").is_file()
            return lease.checkout_root, lease.generation

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(reader, index) for index in range(2)]
        try:
            ready.wait(timeout=30)
            with borrow(item, local_checkout_root=base) as remaining:
                remaining.preflight(action)
                leaving.set()
                results = [future.result(timeout=30) for future in futures]
                # Their exit cannot delete the third reader's generation.
                assert results == [(remaining.checkout_root, remaining.generation)] * 2
                assert remaining.checkout_root.is_relative_to(base)
                assert (remaining.checkout_root / "payload.txt").read_text() == (
                    "sealed by a producer\n")
                assert counts == {"init": 1, "checkout": 1, "identity": 1}
                _reap(base, now=time.time() + 366 * 86400)
                assert (remaining.checkout_root / "payload.txt").is_file()
        finally:
            leaving.set()


@pytest.mark.parametrize("difference", ["bundle", "refs", "parent", "subdirectory", "cas"])
def test_full_snapshot_and_cas_context_never_alias_a_warm_generation(
        sealed, tmp_path, difference):
    action, cas, _ = sealed
    borrow = _borrow()
    item = _record(action, cas)
    other = deepcopy(item)
    snapshot = other["checkout_snapshot"]
    if difference == "bundle":
        # A structurally valid different input identity, absent from this CAS.
        snapshot["input"]["sha256"] = "0" * 64
    elif difference == "refs":
        snapshot["refs"]["not-in-the-bundle"] = snapshot["commit"]
    elif difference == "parent":
        snapshot["parent"] = snapshot["commit"]
    elif difference == "subdirectory":
        snapshot["subdirectory"] = "absent-subdirectory"
    else:
        other["cas_root"] = str(tmp_path / "untrusted-empty-cas")
    with borrow(item, local_checkout_root=tmp_path / "local") as valid:
        valid.preflight(action)
        with pytest.raises((core.ActionContractError, materialize.MaterializationContractError)):
            with borrow(other, local_checkout_root=tmp_path / "local") as invalid:
                invalid.preflight(action)
        assert valid.preflight(action)["action_key"] == action["action_key"]


def test_different_valid_subdirectories_do_not_share_a_generation(sealed, tmp_path):
    action, cas, _ = sealed
    # The Git tree already contains task.py. Add a real directory by building
    # a second temporary producer fixture, rather than naming historical Git.
    from test_producer_snapshot_is_proven import _producer_action
    from prismabuild import fleet_submit
    source = tmp_path / "another-producer"
    source.mkdir()
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Snapshot lease test")
    _git(source, "config", "user.email", "snapshot@example.invalid")
    (source / "task.py").write_text("print('snapshot')\n")
    (source / "nested").mkdir()
    (source / "nested" / "payload.txt").write_text("nested\n")
    _git(source, "add", ".")
    _git(source, "commit", "-qm", "temporary source")
    action, _ = fleet_submit.seal_checkout_into_action(
        _producer_action(source), cas=cas, checkout_root=source)
    item = _record(action, cas)
    nested = deepcopy(item)
    nested["checkout_snapshot"]["subdirectory"] = "nested"
    with ExitStack() as stack:
        a = stack.enter_context(_borrow()(item, local_checkout_root=tmp_path / "local"))
        b = stack.enter_context(_borrow()(nested, local_checkout_root=tmp_path / "local"))
        assert a.generation != b.generation
        # Distinct trees, not merely two subpaths into one HEAD-keyed tree.
        assert a.checkout_root not in b.checkout_root.parents
        assert (b.checkout_root / "payload.txt").read_text() == "nested\n"


def test_interrupted_build_is_not_published_and_retry_builds_faithfully(
        sealed, tmp_path, monkeypatch):
    action, cas, _ = sealed
    borrow = _borrow()
    item = _record(action, cas)
    base = tmp_path / "local"
    git = materialize._run_materializer_git
    damaged = []

    def fail_after_checkout(argv, **kwargs):
        result = git(argv, **kwargs)
        if "checkout" in argv:
            root = Path(argv[argv.index("-C") + 1])
            (root / "payload.txt").write_text("partial builder, not publishable\n")
            damaged.append(root)
            raise materialize.MaterializationContractError("injected interrupted builder")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(materialize, "_run_materializer_git", fail_after_checkout)
        with pytest.raises(materialize.MaterializationContractError, match="interrupted builder"):
            with borrow(item, local_checkout_root=base):
                pytest.fail("partial generation became visible")
    assert damaged, "failure injection did not reach a checkout"
    with borrow(item, local_checkout_root=base) as lease:
        assert (lease.checkout_root / "payload.txt").read_text() == "sealed by a producer\n"
        lease.preflight(action)


@pytest.mark.parametrize("mutation", ["bytes", "ancestry", "refs"])
def test_changed_generation_is_refused_even_with_same_size_and_restored_mtime(
        sealed, tmp_path, mutation):
    action, cas, _ = sealed
    item = _record(action, cas)
    borrow = _borrow()
    with borrow(item, local_checkout_root=tmp_path / "local") as lease:
        lease.preflight(action)
        root = lease.checkout_root
        if mutation == "bytes":
            path = root / "payload.txt"
            before = path.stat()
            # Exercise byte-derived identity, not just Git's stat cache.
            path.chmod(before.st_mode | 0o200)
            path.write_bytes(b"X" * before.st_size)
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        elif mutation == "ancestry":
            path = root / ".git" / "shallow"
            path.parent.chmod(path.parent.stat().st_mode | 0o700)
            path.write_text(item["checkout_snapshot"]["commit"] + "\n")
        else:
            path = root / ".git" / "HEAD"
            path.chmod(path.stat().st_mode | 0o200)
            path.write_text(item["checkout_snapshot"]["parent"] + "\n")
        # Same-UID mutation is deliberately possible: permissions are not a
        # security sandbox. A cached proof cannot bless the changed source.
        with pytest.raises((core.ActionContractError, materialize.MaterializationContractError)):
            lease.preflight(action)
        try:
            with borrow(item, local_checkout_root=tmp_path / "local") as next_lease:
                # A fresh privately validated replacement is allowed; reuse of
                # the uncertain generation is not.
                assert next_lease.generation != lease.generation
                assert next_lease.checkout_root != root
                next_lease.preflight(action)
        except (core.ActionContractError, materialize.MaterializationContractError):
            pass


CHILD = '''import json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[3])
from prismabuild import materialize
item = json.loads(sys.argv[1])
if sys.argv[4] == "builder":
    run = materialize._run_materializer_git
    def stop(argv, **kwargs):
        result = run(argv, **kwargs)
        if "checkout" in argv:
            print(json.dumps({"building": True}), flush=True)
            sys.stdin.readline()
        return result
    materialize._run_materializer_git = stop
with materialize._shared_source_checkout(item, local_checkout_root=sys.argv[2]) as lease:
    print(json.dumps({"root": str(lease.checkout_root), "generation": str(lease.generation)}), flush=True)
    sys.stdin.readline()
'''


def _child(item, base, mode):
    process = subprocess.Popen(
        [sys.executable, "-c", CHILD, json.dumps(item), str(base),
         str(Path(__file__).resolve().parents[1] / "src"), mode],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        readable, _, _ = select.select([process.stdout], [], [], 30)
        assert readable, "child did not reach the lifecycle boundary"
        line = process.stdout.readline()
        assert line, f"child failed: {process.stderr.read()}"
        return process, json.loads(line)
    except BaseException:
        process.kill()
        process.wait(timeout=30)
        raise


@pytest.mark.parametrize("mode", ["reader", "builder"])
def test_sigkill_recovery_keeps_live_readers_and_converges_after_last_release(
        sealed, tmp_path, mode):
    action, cas, _ = sealed
    borrow = _borrow()  # fail with a feature assertion, not a child import error
    item = _record(action, cas)
    base = tmp_path / "local"
    process, info = _child(item, base, mode)
    try:
        if mode == "builder":
            assert info == {"building": True}
        process.kill()
        process.wait(timeout=30)
        with borrow(item, local_checkout_root=base) as live:
            live.preflight(action)
            if mode == "reader" and live.checkout_root == Path(info["root"]):
                assert str(live.generation) == info["generation"]
            _reap(base, now=time.time() + 366 * 86400)
            assert (live.checkout_root / "payload.txt").read_text() == "sealed by a producer\n"
            # Legacy private reapers must not recognize shared generation names.
            with materialize._execution_checkout(item, local_checkout_root=base):
                assert live.checkout_root.is_dir()
            root = live.checkout_root
        _reap(base)
        _reap(base, now=time.time() + 366 * 86400)
        assert not root.exists(), "last-user/dead-lease retirement never converged"
        with borrow(item, local_checkout_root=base) as replacement:
            replacement.preflight(action)
            assert (replacement.checkout_root / "payload.txt").is_file()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)


def test_acquire_and_last_release_have_one_exclusion_boundary(sealed, tmp_path):
    action, cas, _ = sealed
    borrow = _borrow()
    base = tmp_path / "local"
    item = _record(action, cas)
    # Repeated handoff races with retirement. Every successful acquisition
    # must pin its generation until after its final verification/read.
    def read():
        for _ in range(8):
            with borrow(item, local_checkout_root=base) as lease:
                lease.preflight(action)
                assert (lease.checkout_root / "payload.txt").is_file()

    def retire():
        for _ in range(8):
            _reap(base, now=time.time() + 366 * 86400)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(read), executor.submit(read), executor.submit(retire)]
        for future in futures:
            future.result(timeout=90)
