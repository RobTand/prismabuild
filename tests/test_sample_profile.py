"""``--profile sample`` is a different action, and it must leave a profile behind.

Three claims, and they are the reason this tier exists at all.

*   **A profiled run is its own action.**  ``profile`` is sealed into
    ``params``, so the profiled and unprofiled forms of one command have
    different keys.  Without that, the first ``--profile sample`` submission of
    a command anybody had already run would be answered from the CAS with a
    receipt that has no profile in it, and a timing comparison between the two
    would be an A/B whose arms are one arm.  The other direction matters just
    as much: *omitting* the flag must leave the key byte-identical to what it
    was before this feature existed, or every cached action in the store is
    orphaned.

*   **A profiled action that produced no profile is not the action that was
    asked for.**  The backend failing is an action failure with a reason, never
    a run that quietly came back unprofiled.  The child's own result is ingested
    on the way out so the failure can be read rather than guessed at.

*   **The reference travels in the record a reader already opens.**  The blob
    goes into the CAS the way every result payload does, and its digest into
    the pool's ending under ``profile`` -- not into the CAS receipt, whose v3
    key set is an immutable interpretation domain.

The backend is a registry rather than an ``if``: Tier 2 (issue #372) adds
``torch``/``nsys`` behind the same flag, and the tests below drive a fake
backend through the same seams the real one uses.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import threading
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbstatus  # noqa: E402
import pbcampaign  # noqa: E402


REPOSITORY = Path(__file__).resolve().parents[1]


def _speedscope(name: str) -> str:
    """The smallest document a speedscope reader will open."""

    return json.dumps({
        "$schema": pb.PROFILE_SPEEDSCOPE_SCHEMA,
        "shared": {"frames": [{"name": name}]},
        "profiles": [{
            "type": "sampled",
            "name": name,
            "unit": "seconds",
            "startValue": 0.0,
            "endValue": 1.0,
            "samples": [[0], [0], [0]],
            "weights": [0.5, 0.25, 0.25],
        }],
    })


class _FakeBackend:
    """A backend that writes a fixed profile around the sealed argv.

    It goes through the same two seams the py-spy backend does -- it wraps the
    argv and it reads back a file -- so a test that drives it is testing the
    worker's contract with a backend rather than one profiler's command line.
    """

    mode = "fake"
    name = "fake-profiler"
    version = "fake-1"
    rate_hz = 7
    profile_suffix = "speedscope.json"

    def __init__(self, *, writes_profile: bool = True):
        self.writes_profile = writes_profile

    def locate(self) -> str:
        return "/bin/sh"

    def launch_argv(self, argv, *, profile_path: Path):
        # The argv handed to a backend is already relayed: the session owns
        # the exit-status guarantee so that no backend can forget it.
        if not self.writes_profile:
            return list(argv)
        return [
            "/bin/sh", "-c", 'printf %s "$1" > "$0"; shift 1; exec "$@"',
            str(profile_path), _speedscope("fake"), *argv,
        ]

    def read_profile(self, path: Path) -> dict:
        return pb.read_speedscope(path)


@pytest.fixture
def fake_backend(monkeypatch: pytest.MonkeyPatch):
    backend = _FakeBackend()
    monkeypatch.setitem(pb.PROFILE_BACKENDS, "fake", backend)
    return backend


def _closure_member(checkout: Path) -> None:
    (checkout / "task_code.py").write_text("# closure member\n", encoding="utf-8")


#: Long enough for a 100 Hz sampler to see it.  A sub-second action is not
#: profilable by a sampling profiler that has to find the interpreter first,
#: and this tier says so out loud rather than returning an empty profile.
_WORK = ("import math; "
         "print(sum(sum(math.sqrt(i) for i in range(20000)) for _ in range(160)))")


def _action(checkout: Path, *, profile: str | None, result: str = "result.txt",
            exit_code: int = 0):
    """A portable action that writes its result through the pipeline shape.

    ``| tee`` is not decoration: it is what ``pbrun`` seals, and bash forks a
    pipeline member rather than exec'ing it, which is exactly the shape that
    decides whether a profiler can see the child at all.

    ``exit_code`` makes the action fail *after* doing its work, which is what a
    failing test run looks like: the payload ran, the profiler has a complete
    report of it, and the ending is nonzero.  Zero leaves the argv, and so the
    action key, exactly as it was.
    """

    _closure_member(checkout)
    ending = "exit ${PIPESTATUS[0]}" if not exit_code else f"exit {exit_code}"
    argv = [
        "/bin/bash", "--noprofile", "--norc", "-c",
        f"{sys.executable} -c {_WORK!r} 2>&1 | tee {result}; " + ending,
    ]
    params: dict[str, object] = {"command": ["work"]}
    if profile is not None:
        params["profile"] = profile
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/profile",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": argv,
            "working_directory": ".",
            "result_path": result,
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": params,
        "environment": {"variables": {"PATH": "/usr/bin:/bin"}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    }
    return pb.seal_action(body)


# -- identity ---------------------------------------------------------------


def test_profile_is_sealed_into_the_action_key(tmp_path: Path):
    plain = _action(tmp_path, profile=None)
    profiled = _action(tmp_path, profile="sample")
    assert plain["action_key"] != profiled["action_key"]


def test_omitting_profile_leaves_the_key_untouched(tmp_path: Path):
    """No flag, no key change: every action already in the CAS stays addressable."""

    first = _action(tmp_path, profile=None)
    second = _action(tmp_path, profile=None)
    assert first["action_key"] == second["action_key"]
    assert "profile" not in first["params"]


def test_pbrun_seals_the_flag_only_when_it_is_given(tmp_path: Path):
    parser_flags = subprocess.run(
        [sys.executable, str(REPOSITORY / "tools" / "fleet" / "pbrun.py"), "--help"],
        capture_output=True, text=True, cwd=tmp_path,
    ).stdout
    assert "--profile" in parser_flags
    assert "sample" in parser_flags


# -- the worker's contract with a backend -----------------------------------


def test_a_registered_backend_wraps_the_child_and_its_output_is_ingested(
    tmp_path: Path, fake_backend: _FakeBackend
):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, profile="fake")
    result = pb.run_local_action(
        action, cas_root=tmp_path / "cas", checkout_root=checkout
    )
    profile = result["profile"]
    assert profile["mode"] == "fake"
    assert profile["backend"] == "fake-profiler"
    assert profile["rate_hz"] == 7
    assert profile["samples"] == 3
    blob = (tmp_path / "cas" / "blobs"
            / str(profile["blob_sha256"])[:2] / str(profile["blob_sha256"]))
    assert blob.exists()
    assert json.loads(blob.read_text())["$schema"] == pb.PROFILE_SPEEDSCOPE_SCHEMA
    assert blob.stat().st_size == profile["bytes"]
    assert profile["blob_path"] == str(blob)
    # The scratch file is the action's own and does not outlive it.
    assert not (checkout / pb.PROFILE_SCRATCH_DIRNAME).exists()


def test_an_unprofiled_action_carries_no_profile_key(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, profile=None)
    result = pb.run_local_action(
        action, cas_root=tmp_path / "cas", checkout_root=checkout
    )
    assert "profile" not in result


def test_the_action_exit_status_survives_the_profiler(
    tmp_path: Path, fake_backend: _FakeBackend
):
    """The profiler is the child's parent, so its own exit status is not the action's.

    py-spy returns 0 whatever the program it ran returned (measured, 0.4.2), so
    a profiled run that reported the launcher's status would call every failing
    action a pass.  The relay carries the action's own number out of band.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    _closure_member(checkout)
    action = _action(checkout, profile="fake")
    body = {key: value for key, value in action.items() if key != "action_key"}
    body["task"] = {**body["task"], "argv": [
        "/bin/bash", "--noprofile", "--norc", "-c",
        "printf x > result.txt; exit 7",
    ]}
    failing = pb.seal_action(body)
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            failing, cas_root=tmp_path / "cas", checkout_root=checkout
        )
    assert raised.value.returncode == 7


def test_a_backend_that_leaves_no_profile_fails_the_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    monkeypatch.setitem(
        pb.PROFILE_BACKENDS, "fake", _FakeBackend(writes_profile=False)
    )
    action = _action(checkout, profile="fake")
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            action, cas_root=tmp_path / "cas", checkout_root=checkout
        )
    message = str(raised.value)
    assert "fake-profiler" in message
    # The child's own result is ingested so the failure can be read.
    digest = [word for word in message.replace(",", " ").split()
              if len(word) == 64 and all(c in "0123456789abcdef" for c in word)]
    assert digest, message
    assert (tmp_path / "cas" / "blobs" / digest[0][:2] / digest[0]).exists()
    assert (tmp_path / "cas" / "actions").exists() is False


def test_a_reader_is_told_when_a_profile_did_not_survive(tmp_path: Path):
    """Printing nothing reads as "nobody asked for a profile", which is wrong."""

    assert pb.describe_profile({}) == ""
    absent = pb.describe_profile(
        {"mode": "nsys", "backend": "nsys", "produced": False,
         "reason": "ProfileUnusable: nsys wrote no report"}
    )
    assert "not produced" in absent and "nsys wrote no report" in absent
    partial = pb.describe_profile({
        "mode": "sample", "backend": "py-spy", "blob_sha256": "ab" * 32,
        "bytes": 12, "blob_path": "/blobs/ab", "partial": True,
        "backend_status_ignored": True, "backend_returncode": 1,
    })
    assert "partial" in partial and "profiled anyway" in partial


def _dying_action(checkout: Path, *, profile: str | None, script: str):
    """The same sealed argv with and without the flag, so the arms compare."""

    action = _action(checkout, profile=profile)
    body = {key: value for key, value in action.items() if key != "action_key"}
    body["task"] = {**body["task"], "argv": [
        "/bin/bash", "--noprofile", "--norc", "-c", script,
    ]}
    return pb.seal_action(body)


def _failure_of(action, *, cas_root: Path, checkout: Path):
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            action, cas_root=cas_root, checkout_root=checkout, recompute=True
        )
    return raised.value.returncode, raised.value.signal


@pytest.mark.parametrize(
    ("script", "expected"),
    [
        ("kill -KILL $$", (-9, 9)),
        ("kill -TERM $$", (-15, 15)),
        ("exit 137", (137, None)),
    ],
)
def test_the_failure_record_does_not_change_shape_under_the_flag(
    tmp_path: Path, fake_backend: _FakeBackend, script: str, expected
):
    """A profiled death and an unprofiled death are the same death.

    The first relay was ``/bin/sh -c '"$@"; printf %d $? > "$0"'``, and a shell
    reports a signalled child as 128+n with no way to tell it from a literal
    ``exit 137``.  That made a profiled OOM-kill report ``returncode=137,
    signal=None`` where the unprofiled path reports ``-9`` and ``signal 9``:
    the shape of the record changed with the diagnostic flag, so an operator
    reading a failed action could not tell whether the box had killed it.  The
    third case is here to prove the fix is not the trivial one -- an action
    that really does exit 137 must still say so, with no signal.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    cas_root = tmp_path / "cas"
    unprofiled = _failure_of(
        _dying_action(checkout, profile=None, script=script),
        cas_root=cas_root, checkout=checkout,
    )
    profiled = _failure_of(
        _dying_action(checkout, profile="fake", script=script),
        cas_root=cas_root, checkout=checkout,
    )
    assert unprofiled == expected
    assert profiled == unprofiled


def test_the_record_names_the_binary_that_did_the_profiling(
    tmp_path: Path, fake_backend: _FakeBackend
):
    """A version string is a claim about a box; a digest is a fact about a file.

    Two boxes can carry one version over different binaries, and an overhead
    number is only comparable against the binary it was measured on.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    result = pb.run_local_action(
        _action(checkout, profile="fake"),
        cas_root=tmp_path / "cas", checkout_root=checkout,
    )
    profile = result["profile"]
    assert profile["backend_path"] == "/bin/sh"
    assert len(str(profile["backend_sha256"])) == 64
    assert profile["backend_bytes"] > 0
    assert profile["backend_returncode"] == 0
    assert profile["produced"] is True


def test_a_profiler_that_ends_badly_but_profiled_anyway_is_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """py-spy's own status was overwritten by the action's and never reported.

    It is reported now -- and not acted on by itself.  py-spy 0.4.2 on sparky
    exits 1 with ``Error: No child process`` from time to time *having written
    a complete speedscope* (action ``b6f2ab795ef6``: the failing arm and the
    arm after it, whose conditions were a strict superset, left 249 and 271
    samples).  Failing the run there throws away a good action for a race
    inside the tool watching it.
    """

    class _Noisy(_FakeBackend):
        def launch_argv(self, argv, *, profile_path: Path):
            return [
                "/bin/sh", "-c",
                'printf %s "$1" > "$0"; shift 2; "$@"; exit 3',
                str(profile_path), _speedscope("fake"), "--", *argv,
            ]

    monkeypatch.setitem(pb.PROFILE_BACKENDS, "fake", _Noisy())
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    result = pb.run_local_action(
        _action(checkout, profile="fake"),
        cas_root=tmp_path / "cas", checkout_root=checkout,
    )
    profile = result["profile"]
    assert profile["backend_returncode"] == 3
    assert profile["backend_status_ignored"] is True
    assert "the run stands" in str(profile["backend_status_note"])


def test_a_profiler_that_ends_badly_and_profiled_nothing_names_both_numbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """What was lost is the rule, not what was returned."""

    class _Empty(_FakeBackend):
        def launch_argv(self, argv, *, profile_path: Path):
            return ["/bin/sh", "-c", '"$@"; exit 3', "--", *argv]

    monkeypatch.setitem(pb.PROFILE_BACKENDS, "fake", _Empty())
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            _action(checkout, profile="fake"),
            cas_root=tmp_path / "cas", checkout_root=checkout,
        )
    message = str(raised.value)
    assert "The profiler exited with status 3" in message
    assert "The action itself ended with status 0" in message


def test_a_timed_out_action_still_files_the_profile_it_reached(
    tmp_path: Path, fake_backend: _FakeBackend
):
    """The timed-out case is the one a profile is most wanted for.

    Tier 1 raised the timeout before the profile block, so the run somebody
    profiled *because* it was slow came back with nothing to look at.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    _closure_member(checkout)
    action = _action(checkout, profile="fake")
    body = {key: value for key, value in action.items() if key != "action_key"}
    body["task"] = {**body["task"], "argv": [
        "/bin/bash", "--noprofile", "--norc", "-c", "sleep 120",
    ]}
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            pb.seal_action(body),
            cas_root=tmp_path / "cas", checkout_root=checkout,
            timeout_seconds=1.0,
        )
    error = raised.value
    assert "timed out" in str(error)
    assert error.returncode is None, "a timeout is the worker's verdict"
    profile = error.profile
    assert profile["partial"] is True
    assert profile["produced"] is True
    # The relay's own record says the action never reached an ending, which is
    # what distinguishes a killed run from one that failed on its own.
    assert profile["action_phase"] == "launched"
    blob = (tmp_path / "cas" / "blobs"
            / str(profile["blob_sha256"])[:2] / str(profile["blob_sha256"]))
    assert blob.exists()
    assert str(profile["blob_sha256"]) in str(error)


def test_a_partial_profile_travels_in_the_action_status_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The launcher exits 1 whatever happened, so both facts go in a file.

    The pool kills a launcher that overran its deadline; nothing it printed
    after that is read.  ``core`` writes what it knows here, and the pool lifts
    it onto the ending.
    """

    status = tmp_path / "status.json"
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status))
    pb._write_action_status({"profile": {"mode": "fake", "partial": True}})
    pb._record_action_status(pb.LocalActionError("x", returncode=-9, signal=9))
    body = json.loads(status.read_text())
    assert body == {
        "action_returncode": -9,
        "action_signal": 9,
        "profile": {"mode": "fake", "partial": True},
    }


def test_the_pool_lifts_both_facts_off_the_status_file(tmp_path: Path):
    """And removes it, so the next attempt on this key reads its own."""

    import prismabuild.pool as pool

    path = tmp_path / "k.status"
    path.write_text(json.dumps({
        "action_returncode": 7, "action_signal": None,
        "profile": {"mode": "nsys", "partial": True},
    }), encoding="utf-8")
    outcome = pool.PoolQueue._merge_action_status(
        {"status": "timeout", "returncode": None}, path
    )
    assert outcome["action_returncode"] == 7
    assert "action_signal" not in outcome
    assert outcome["profile"]["mode"] == "nsys"
    assert not path.exists()


def test_an_ending_without_a_returncode_is_not_filed_as_one(tmp_path: Path):
    """A signal alone would state an ending the action never reached."""

    from prismabuild import slurm_lane

    path = tmp_path / "s.json"
    path.write_text(json.dumps({
        "action_signal": 9, "profile": {"mode": "sample", "partial": True},
    }), encoding="utf-8")
    status = slurm_lane.read_action_status(path)
    assert "action_returncode" not in status
    assert "action_signal" not in status
    assert status["profile"]["mode"] == "sample"


def _session(tmp_path: Path, backend=None) -> "pb._ProfileSession":
    return pb._ProfileSession(
        mode="fake",
        backend=backend if backend is not None else _FakeBackend(),
        directory=tmp_path / "scratch",
    )


def _write_status(session, body: dict) -> None:
    session.directory.mkdir(parents=True, exist_ok=True)
    temporary = session.exit_status_path.with_name(
        f".{session.exit_status_path.name}.tmp"
    )
    temporary.write_text(json.dumps(body), encoding="utf-8")
    os.replace(temporary, session.exit_status_path)


def _proc_start_ticks(pid: int) -> int:
    """Read field 22 from Linux ``/proc/<pid>/stat`` independently."""

    raw = Path(f"/proc/{pid}/stat").read_bytes()
    _, _, tail = raw.rpartition(b")")
    return int(tail.split()[19])


def _await_zombie(pid: int) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        raw = Path(f"/proc/{pid}/stat").read_bytes()
        _, _, tail = raw.rpartition(b")")
        if tail.split()[0] == b"Z":
            return
        time.sleep(.01)
    pytest.fail(f"relay fixture {pid} never became an unreaped zombie")


def test_a_relay_that_never_recorded_an_ending_is_not_a_pass(tmp_path: Path):
    """``launched`` with no ``ended`` means nobody knows how the action ended.

    It is the state a killed profiler leaves behind, and reading it as success
    would publish a receipt for an action whose status was never observed.
    """

    session = _session(tmp_path)
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launched", "child_pid": 4321,
    })
    with pytest.raises(pb.ProfileUnusable) as raised:
        session.action_returncode()
    assert "4321" in str(raised.value)


def test_a_relay_that_could_not_launch_is_a_worker_verdict(tmp_path: Path):
    """No ``returncode`` reaches the error: the action never ran to have one."""

    session = _session(tmp_path)
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launch_failed", "launch_error": "OSError: [Errno 13] denied",
    })
    with pytest.raises(pb.LocalActionError) as raised:
        session.action_returncode()
    assert raised.value.returncode is None
    assert "Errno 13" in str(raised.value)


def test_an_exit_status_from_another_era_is_refused(tmp_path: Path):
    session = _session(tmp_path)
    _write_status(session, {"phase": "ended", "returncode": 0})
    with pytest.raises(pb.ProfileUnusable):
        session.action_returncode()


def test_a_profiler_that_exits_first_waits_for_the_action(tmp_path: Path):
    """``nsys --duration`` stops tracing and exits while the action runs on.

    Measured on sparky (action ``83d2530f3eda``): a 2 s cap ended nsys at 3.3 s
    with the workload still going at 8.4 s.  A backend that can do that says
    so, and the worker waits for the relay's real ending instead of reporting
    an ending the action had not reached.
    """

    class _Early(_FakeBackend):
        exits_before_action = True

    session = _session(tmp_path, backend=_Early())
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launched", "child_pid": os.getpid(),
        "relay_pid": os.getpid(),
        "relay_start_ticks": _proc_start_ticks(os.getpid()),
    })

    def _finish() -> None:
        time.sleep(0.3)
        _write_status(session, {
            "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
            "phase": "ended", "returncode": 3, "signal": None,
        })

    thread = threading.Thread(target=_finish)
    thread.start()
    try:
        assert session.action_returncode() == 3
    finally:
        thread.join()


def test_a_windowed_profiler_does_not_cap_the_action_lifetime(tmp_path: Path):
    """A completed window leaves execution to its actual deadline policy (#514)."""

    class _Early(_FakeBackend):
        exits_before_action = True

    session = _session(tmp_path, backend=_Early())
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launched", "child_pid": os.getpid(),
        "relay_pid": os.getpid(),
        "relay_start_ticks": _proc_start_ticks(os.getpid()),
    })

    def _finish() -> None:
        time.sleep(0.15)
        _write_status(session, {
            "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
            "phase": "ended", "returncode": 0, "signal": None,
            "relay_pid": os.getpid(),
        })

    thread = threading.Thread(target=_finish)
    thread.start()
    try:
        assert session.action_returncode() == 0
    finally:
        thread.join()


def test_a_dead_window_relay_fails_closed_without_waiting_for_a_backend_cap(
    tmp_path: Path,
):
    """A departed relay cannot leave an unbounded worker wait behind (#514)."""

    class _Early(_FakeBackend):
        exits_before_action = True

    relay = subprocess.Popen([sys.executable, "-c", "pass"])
    relay.wait()
    session = _session(tmp_path, backend=_Early())
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launched", "child_pid": os.getpid(),
        "relay_pid": relay.pid,
        "relay_start_ticks": 1,
    })
    with pytest.raises(pb.ProfileUnusable, match="relay .* is no longer running"):
        session.action_returncode()


def test_an_unreaped_zombie_relay_fails_closed(tmp_path: Path):
    """``kill(pid, 0)`` is insufficient: zombies have already exited (#514)."""

    class _Early(_FakeBackend):
        exits_before_action = True

    child_pid = tmp_path / "relay.pid"
    parent = subprocess.Popen([
        sys.executable, "-c",
        (
            "import subprocess,sys,time; from pathlib import Path; "
            "child=subprocess.Popen([sys.executable, '-c', 'pass']); "
            "Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(30)"
        ),
        str(child_pid),
    ])
    try:
        deadline = time.monotonic() + 10.0
        while not child_pid.exists():
            assert time.monotonic() < deadline
            time.sleep(.01)
        relay_pid = int(child_pid.read_text())
        _await_zombie(relay_pid)
        session = _session(tmp_path, backend=_Early())
        _write_status(session, {
            "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
            "phase": "launched", "child_pid": os.getpid(),
            "relay_pid": relay_pid,
            "relay_start_ticks": _proc_start_ticks(relay_pid),
        })
        with pytest.raises(pb.ProfileUnusable, match="relay .* is no longer running"):
            session.action_returncode()
    finally:
        parent.kill()
        parent.wait()


def test_a_reused_relay_pid_identity_fails_closed(tmp_path: Path):
    """A PID alone must not turn an unrelated live process into a relay (#514)."""

    class _Early(_FakeBackend):
        exits_before_action = True

    session = _session(tmp_path, backend=_Early())
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launched", "child_pid": os.getpid(),
        "relay_pid": os.getpid(),
        "relay_start_ticks": _proc_start_ticks(os.getpid()) + 1,
    })
    with pytest.raises(pb.ProfileUnusable, match="relay .* is no longer running"):
        session.action_returncode()


def test_a_relay_that_exits_after_writing_its_ending_is_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """The liveness check re-reads a raced terminal relay record (#514)."""

    class _Early(_FakeBackend):
        exits_before_action = True

    session = _session(tmp_path, backend=_Early())
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launched", "child_pid": os.getpid(),
        "relay_pid": os.getpid(),
        "relay_start_ticks": _proc_start_ticks(os.getpid()),
    })

    def finished_then_gone(pid: int, start_ticks: int) -> bool:
        _write_status(session, {
            "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
            "phase": "ended", "returncode": 0, "signal": None,
            "relay_pid": pid, "relay_start_ticks": start_ticks,
        })
        return False

    monkeypatch.setattr(pb, "_profile_relay_is_live", finished_then_gone)
    assert session.action_returncode() == 0


def test_a_windowed_profile_without_a_relay_identity_fails_closed(
    tmp_path: Path,
):
    class _Early(_FakeBackend):
        exits_before_action = True

    session = _session(tmp_path, backend=_Early())
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launched", "child_pid": os.getpid(),
    })
    with pytest.raises(pb.ProfileUnusable, match="no live relay identity"):
        session.action_returncode()


def test_a_dead_relay_reaps_its_owned_action(tmp_path: Path):
    """A broken relay fails closed without leaving its action behind (#514)."""

    class _Early(_FakeBackend):
        exits_before_action = True

    relay = subprocess.Popen([sys.executable, "-c", "pass"])
    relay.wait()
    session = _session(tmp_path, backend=_Early())
    _write_status(session, {
        "schema": pb.PROFILE_EXIT_STATUS_SCHEMA,
        "phase": "launched", "child_pid": os.getpid(),
        "relay_pid": relay.pid,
        "relay_start_ticks": 1,
    })
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
    )
    try:
        with pytest.raises(pb.ProfileUnusable, match="relay .* is no longer running"):
            session.action_returncode(process)
        assert process.poll() is not None, "the action was left running"
    finally:
        if process.poll() is None:  # pragma: no cover - only on a failure
            process.kill()
            process.wait()


def test_an_uncreatable_scratch_directory_refuses_with_a_reason(
    tmp_path: Path, fake_backend: _FakeBackend
):
    """A read-only checkout must fail the action, not the worker.

    ``core.main`` catches ``LocalActionError`` and writes the action's status
    before re-raising.  An unwrapped ``OSError`` from the scratch ``mkdir``
    tracebacked out of ``main`` instead, skipping that write, so the claim was
    reaped and retried on the next attempt with nothing saying why.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, profile="fake")
    checkout.chmod(0o500)
    try:
        with pytest.raises(pb.LocalActionError) as raised:
            pb.run_local_action(
                action, cas_root=tmp_path / "cas", checkout_root=checkout
            )
    finally:
        checkout.chmod(0o700)
    message = str(raised.value)
    assert pb.PROFILE_SCRATCH_DIRNAME in message
    assert "before the action started" in message


def test_a_missing_backend_refuses_and_names_the_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    import socket

    class _Absent(_FakeBackend):
        def locate(self) -> str:
            raise pb.ProfileBackendUnavailable(
                "py-spy is not beside /usr/bin/python3 and not on PATH"
            )

    monkeypatch.setitem(pb.PROFILE_BACKENDS, "fake", _Absent())
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, profile="fake")
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            action, cas_root=tmp_path / "cas", checkout_root=checkout
        )
    message = str(raised.value)
    assert socket.gethostname() in message
    assert "fake-profiler" in message


def test_an_unknown_mode_is_refused_by_name(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, profile="no-such-mode")
    with pytest.raises(pb.LocalActionError, match="no-such-mode"):
        pb.run_local_action(
            action, cas_root=tmp_path / "cas", checkout_root=checkout
        )


# -- the py-spy backend on this box -----------------------------------------


def test_the_sampling_backend_profiles_the_pipeline_member(tmp_path: Path):
    """The real backend, on the real argv shape, on this box.

    Skipping when py-spy is absent would make the one test that proves the
    fleet can profile anything green on a box that cannot.  It fails instead:
    an unavailable backend is a fleet fact to fix, not a test to skip.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, profile="sample")
    result = pb.run_local_action(
        action, cas_root=tmp_path / "cas", checkout_root=checkout
    )
    profile = result["profile"]
    assert profile["mode"] == "sample"
    assert profile["backend"] == "py-spy"
    assert profile["rate_hz"] == pb.PROFILE_SAMPLE_RATE_HZ
    assert profile["samples"] > 0
    blob = (tmp_path / "cas" / "blobs"
            / str(profile["blob_sha256"])[:2] / str(profile["blob_sha256"]))
    assert json.loads(blob.read_text())["$schema"] == pb.PROFILE_SPEEDSCOPE_SCHEMA


def test_the_output_lock_still_reaches_the_child_through_the_profiler(
    tmp_path: Path
):
    """The action keeps the exclusion its worker was killed holding.

    ``_run_local_action`` passes the output-lock descriptor into the child so
    the lock outlives a killed worker.  Profiling puts two processes between
    the worker and the sealed argv, and a descriptor that stopped at either of
    them would turn a profiled action into one a second worker could run
    beside -- silently, because nothing else in the run would look different.

    The check is on the pipe's inode rather than the descriptor number, and
    only inside the launched process group: a number alone matches whatever
    that process happened to open, and this test's own ancestors carry the
    same argv text.
    """

    read_end, _write_end = os.pipe()
    os.set_inheritable(read_end, True)
    held = os.readlink(f"/proc/self/fd/{read_end}")
    sealed = [
        "/bin/bash", "--noprofile", "--norc", "-c",
        f"{sys.executable} -c {_WORK!r} 2>&1 | tee {tmp_path / 'log'}; "
        "exit ${PIPESTATUS[0]}",
    ]
    argv = pb._ProfileSession(
        mode="sample",
        backend=pb.PROFILE_BACKENDS["sample"],
        directory=tmp_path / "scratch",
    ).launch_argv(sealed)
    child = subprocess.Popen(
        argv, start_new_session=True, stdin=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, pass_fds=(read_end,))
    try:
        group = os.getpgid(child.pid)
        holders: dict[str, bool] = {}
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline and len(holders) < 2:
            time.sleep(0.1)
            for entry in Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    if os.getpgid(int(entry.name)) != group:
                        continue
                    command = (entry / "cmdline").read_bytes().split(b"\0")[0]
                    # The relay is a Python process (it needs ``waitpid`` to
                    # tell a signalled action from one that exited 128+n), so
                    # the two processes between the worker and the work are
                    # this interpreter and the sealed shell.
                    if not command.endswith(
                        (b"/sh", b"/bash", os.fsencode(Path(sys.executable).name))
                    ):
                        continue
                    holders[f"{command.decode()}:{entry.name}"] = any(
                        os.readlink(str(descriptor)) == held
                        for descriptor in (entry / "fd").iterdir()
                    )
                except (OSError, ProcessLookupError, ValueError):
                    continue
    finally:
        child.wait(timeout=180)
    assert len(holders) >= 2, f"expected the relay and the sealed shell: {holders}"
    assert all(holders.values()), holders


# -- the record a reader opens ----------------------------------------------


def _profile_record() -> dict[str, object]:
    return {
        "mode": "sample", "backend": "py-spy", "backend_version": "py-spy 0.4.2",
        "rate_hz": 100, "blob_sha256": "ab" * 32, "bytes": 4096, "samples": 900,
        "blob_path": "/mnt/shared/prismabuild-fleet/cas/blobs/ab/" + "ab" * 32,
    }


def test_the_pool_lifts_the_profile_out_of_the_launcher_result():
    stdout = "some earlier line\n" + json.dumps(
        {"status": "published", "profile": _profile_record()}, sort_keys=True) + "\n"
    assert pool.profile_from_launcher_stdout(stdout) == _profile_record()
    assert pool.profile_from_launcher_stdout("not json at all\n") is None
    assert pool.profile_from_launcher_stdout(
        json.dumps({"status": "published"})) is None


def test_the_ending_a_worker_files_carries_the_profile(tmp_path: Path):
    """Claim, run, finish -- and the reference is in the record on disk.

    The stub launcher prints exactly what ``core.main`` prints, because the
    pool's whole job here is to move one key from that line into the outcome it
    files.  A unit test of the lift alone would leave the wiring from
    ``_execute_in_checkout`` to ``finish`` untested, and that wiring is what a
    reader of ``pbstatus`` actually depends on.
    """

    import uuid

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = uuid.uuid4().hex + uuid.uuid4().hex
    stub = tmp_path / "stub_worker.py"
    stub.write_text(
        "import json\n"
        f"print(json.dumps({{'status': 'published', "
        f"'profile': {_profile_record()!r}}}))\n",
        encoding="utf-8",
    )
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script=str(stub))
    outcome = queue.serve_once()
    assert outcome is not None and outcome["status"] == "executed"
    assert outcome["profile"] == _profile_record()
    ending = json.loads(
        queue.item_path(pool.DONE, key).read_text(encoding="utf-8"))
    filed = queue.attempt_outcomes(ending)[-1]
    assert filed["detail"]["profile"] == _profile_record()


def test_pbrun_prints_the_blob_for_a_profiled_ending():
    line = pbrun.outcome_headline({
        "status": "executed", "finished_host": "dl380g10",
        "detail": {"elapsed_s": 61.0, "profile": _profile_record()},
    })
    assert "ab" * 6 in line
    assert "py-spy" in line


def test_pbstatus_names_the_profile_on_the_ending_row():
    rows = pbstatus.ending_lines([{
        "action_key": "cd" * 32, "status": "executed", "transport": "pool",
        "host": "dl380g10", "elapsed_s": 61.0, "returncode": 0,
        "action_returncode": None, "action_signal": None,
        "receipt_published": None, "slurm_state": None, "unreadable": None,
        "preempted_by": None, "profile": _profile_record(),
    }])
    assert any("ab" * 6 in line for line in rows), rows


# -- the flag travels ---------------------------------------------------------


def test_pbtest_forwards_the_flag():
    source = (REPOSITORY / "tools" / "fleet" / "pbtest.py").read_text(encoding="utf-8")
    assert '"--profile"' in source
    help_text = subprocess.run(
        [sys.executable, str(REPOSITORY / "tools" / "fleet" / "pbtest.py"), "--help"],
        capture_output=True, text=True,
    ).stdout
    assert "--profile" in help_text


def test_pbcampaign_maps_the_row_field_to_the_flag():
    assert ("profile", "--profile") in pbcampaign._VALUE_FIELDS
    assert "profile" in pbcampaign.KNOWN_FIELDS
    assert "profile" in pbcampaign._TEXT_FIELDS
