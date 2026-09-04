"""A declared ``mem_gb`` is the limit an action's host footprint runs under.

Not a promise, and not the whole footprint either: on a GB10 the cgroup charges
anonymous and pinned host pages and charges a CUDA allocation nothing, so what
is published is a *scope* and never a bare "enforced"
(``docs/memory_enforcement_2026-09-04.md``).

Before this, ``ResourceLedger`` admitted work against a declared demand and
nothing held the work to it: an action that exceeded its declaration was
admitted, ran, and finished.  On a box whose GPU and host share one 128 GB
pool that is contained nowhere, and the kernel's own OOM killer picks its
victim from the whole box -- on sparky at 2026-09-01 09:58:41 it took
``pqwork.service``, 20.6 MB peak, which was consuming nothing.

The tests below are about two things and keep them apart:

* the **launch** is the same execution it always was, plus a bound -- same
  argv, same cwd, same environment, or the action key is describing something
  other than what ran;
* a box that cannot cap says so, in the outcome and in its offer, rather than
  behaving differently in silence.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64
KEY_B = "b" * 64


@pytest.fixture(autouse=True)
def _forget_the_probe(monkeypatch):
    """The probe is memoised for the life of a process; a test is not that."""

    monkeypatch.setattr(pool, "_CAP_SUPPORT", None)


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        **kw,
    )


def _capping(monkeypatch, supported: bool, detail: str = "") -> None:
    monkeypatch.setattr(
        pool, "memory_capping_supported", lambda **_kw: (supported, detail)
    )


def _launched(monkeypatch) -> list[list[str]]:
    """Record the argv ``execute`` actually spawns, and run nothing."""

    seen: list[list[str]] = []

    class _Done:
        returncode = 0

        def communicate(self, timeout=None):
            return ("", "")

    def _popen(argv, *_a, **_kw):
        seen.append(list(argv))
        return _Done()

    monkeypatch.setattr(pool.subprocess, "Popen", _popen)
    monkeypatch.setattr(pool, "_systemctl", lambda *_a, **_kw: None)
    monkeypatch.setattr(pool, "unit_outcome", lambda _u: {})
    return seen


# -- the wrapper itself -------------------------------------------------------


def test_the_cap_is_the_declaration_and_swap_is_shut_off() -> None:
    argv = pool.capped_launch_argv(["/usr/bin/python3", "w.py"], cap_gb=16,
                                   unit="pbcap-x")
    assert argv[:2] == ["systemd-run", "--user"]
    assert "MemoryMax=16G" in argv
    # Without this an over-budget action slides into swap and thrashes
    # instead of failing, which turns a loud kill into a slow box.
    assert "MemorySwapMax=0" in argv
    assert argv[argv.index("--") + 1:] == ["/usr/bin/python3", "w.py"]


def test_the_wrapped_argv_is_untouched() -> None:
    """The wrapper bounds the execution; it must not change it."""

    inner = ["/usr/bin/python3", "/w.py", "run-local", "--action", "/a.json"]
    argv = pool.capped_launch_argv(inner, cap_gb=4, unit="u")
    assert argv[argv.index("--") + 1:] == inner


def test_the_environment_and_directory_travel_with_it() -> None:
    """``systemd-run --user`` starts from the user manager, not the caller.

    A child that quietly lost ``TRITON_CACHE_DIR`` or gained a different
    ``PATH`` is a different execution wearing the same action key.
    """

    argv = pool.capped_launch_argv(
        ["/bin/true"], cap_gb=1, unit="u", cwd="/home/rob/checkout",
        env={"PATH": "/usr/bin", "TRITON_CACHE_DIR": "/home/rob/.triton-cache"},
    )
    assert "WorkingDirectory=/home/rob/checkout" in argv
    assert "--setenv=PATH=/usr/bin" in argv
    assert "--setenv=TRITON_CACHE_DIR=/home/rob/.triton-cache" in argv


def test_the_core_pin_travels_with_it() -> None:
    """The loop pins itself and relies on children inheriting the mask.

    ``systemd-run --user`` asks the *user manager* to fork the work, so
    inheritance does not happen and a loop pinned to GB10's fast cores would
    run its actions on all twenty -- half of them at 2.8 GHz -- while its
    cpu-token offer still described ten.
    """

    argv = pool.capped_launch_argv(
        ["/bin/true"], cap_gb=1, unit="u",
        cpus=[5, 6, 7, 8, 9, 15, 16, 17, 18, 19],
    )
    assert "CPUAffinity=5-9,15-19" in argv


def test_the_fd_ceiling_travels_with_it() -> None:
    """Otherwise it falls to systemd's ``DefaultLimitNOFILE`` soft of 1024.

    Measured 2026-09-04: a launcher at 500000/500000 produced a unit at
    1024/500000.  An NFS shard reader or ``pytest -n N`` that crosses 1024
    raises ``EMFILE``, and the queue retries that ``max_attempts`` times and
    attributes it to the payload.
    """

    argv = pool.capped_launch_argv(["/bin/true"], cap_gb=1, unit="u",
                                   nofile=(500000, 500000))
    assert "LimitNOFILE=500000:500000" in argv


def test_an_infinite_rlimit_is_spelled_the_way_systemd_spells_it() -> None:
    """``LimitNOFILE=-1`` is a parse error systemd answers by ignoring it.

    Which would restore exactly the silence this property exists to end.
    """

    import resource

    argv = pool.capped_launch_argv(
        ["/bin/true"], cap_gb=1, unit="u",
        nofile=(1024, resource.RLIM_INFINITY),
    )
    assert "LimitNOFILE=1024:infinity" in argv


def test_a_caller_that_names_neither_gets_neither() -> None:
    """The builder stays pure: it bounds what it is given, not what it runs in."""

    argv = pool.capped_launch_argv(["/bin/true"], cap_gb=1, unit="u")
    assert not any(a.startswith("CPUAffinity") or a.startswith("LimitNOFILE")
                   for a in argv)


def test_the_launcher_context_is_read_from_the_launcher() -> None:
    """And it is what the call site hands the builder."""

    import os
    import resource

    context = pool.launcher_exec_context()
    assert context["cpus"] == sorted(os.sched_getaffinity(0))
    assert tuple(context["nofile"]) == resource.getrlimit(resource.RLIMIT_NOFILE)


def test_an_unset_name_is_not_forwarded_as_an_empty_one() -> None:
    """Measured 2026-09-04, and it cost both GPU arms of the first probe run.

    ``CUDA_VISIBLE_DEVICES=`` does not mean "unset", it means "no devices".
    Only names the caller actually has are carried across.
    """

    argv = pool.capped_launch_argv(["/bin/true"], cap_gb=1, unit="u",
                                   env={"PATH": "/usr/bin"})
    assert not any(a.startswith("--setenv=CUDA_VISIBLE_DEVICES") for a in argv)


def test_systemds_own_unit_variables_are_not_carried_across() -> None:
    argv = pool.capped_launch_argv(
        ["/bin/true"], cap_gb=1, unit="u",
        env={"INVOCATION_ID": "abc", "JOURNAL_STREAM": "8:123", "HOME": "/home/rob"},
    )
    assert "--setenv=HOME=/home/rob" in argv
    assert not any("INVOCATION_ID" in a or "JOURNAL_STREAM" in a for a in argv)


def test_a_zero_or_negative_cap_is_refused() -> None:
    """Nothing may ask the kernel to enforce a budget of nothing."""

    for bad in (0, -1):
        with pytest.raises(pool.PoolContractError):
            pool.capped_launch_argv(["/bin/true"], cap_gb=bad, unit="u")


def test_the_unit_name_is_per_attempt_not_per_action() -> None:
    """A reaped-but-still-running attempt must not collide with its retry.

    A lease that expires while its child runs is returned to ``ready`` and
    claimed again, possibly by another loop on the same box, so two attempts
    at one action can overlap -- and two units of one name cannot.
    """

    first = pool.cap_unit_name(KEY_A, "sparky:1001:aaaaaaaa")
    second = pool.cap_unit_name(KEY_A, "sparky:1002:bbbbbbbb")
    assert first != second
    assert first.startswith(pool.CAP_UNIT_PREFIX)
    assert all(c.isalnum() or c in ":_.-" for c in first)


def test_an_unnamed_owner_still_gets_a_unique_unit() -> None:
    assert pool.cap_unit_name(KEY_A, "") != pool.cap_unit_name(KEY_A, "")


# -- what execute() does with it ----------------------------------------------


def test_a_declared_action_runs_inside_its_own_cap(queue, monkeypatch) -> None:
    _capping(monkeypatch, True)
    seen = _launched(monkeypatch)
    _publish(queue, KEY_A, resources={"gpu": 1, "mem_gb": 16})
    item = queue.claim()
    outcome = queue.execute(item)
    assert seen[0][0] == "systemd-run"
    assert "MemoryMax=16G" in seen[0]
    assert outcome["capped"] is True
    assert outcome["declared_mem_gb"] == 16


def test_the_cap_is_the_items_figure_not_the_boxs(queue, monkeypatch) -> None:
    """The point is that the offender is the one killed, so it is its own."""

    _capping(monkeypatch, True)
    seen = _launched(monkeypatch)
    _publish(queue, KEY_A, resources={"mem_gb": 12})
    queue.execute(queue.claim())
    assert "MemoryMax=12G" in seen[0]
    assert "MemoryMax=96G" not in seen[0]


def test_the_launch_carries_the_launchers_own_pin_and_fd_ceiling(
    queue, monkeypatch
) -> None:
    """The builder can carry them and the call site can still forget to.

    So this asserts the *executed* argv, not the builder's: an execution
    context member that only the unit tests know about is one the fleet does
    not have.
    """

    import os
    import resource

    _capping(monkeypatch, True)
    seen = _launched(monkeypatch)
    _publish(queue, KEY_A, resources={"mem_gb": 4})
    queue.execute(queue.claim())
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    assert f"CPUAffinity={pool.cpu_topology.as_range(os.sched_getaffinity(0))}" \
        in seen[0]
    assert (f"LimitNOFILE={pool.rlimit_word(soft)}:{pool.rlimit_word(hard)}"
            in seen[0])


def test_an_action_that_declares_nothing_runs_exactly_as_before(
    queue, monkeypatch
) -> None:
    _capping(monkeypatch, True)
    seen = _launched(monkeypatch)
    _publish(queue, KEY_A)
    outcome = queue.execute(queue.claim())
    assert seen[0][0] != "systemd-run"
    assert outcome["capped"] is False
    assert outcome["cap_unavailable"] == ""


def test_capping_does_not_wait_for_a_worker_to_declare_capacity(
    queue, monkeypatch
) -> None:
    """The demand is the ACTION's claim about itself.

    ``worker.py`` -- the smoke worker -- passes no ``capacity`` and so runs no
    ledger, but the items it claims still declare ``mem_gb``.  A declaration
    is enforced wherever it runs, or it is enforced only where somebody
    remembered to configure it.
    """

    _capping(monkeypatch, True)
    seen = _launched(monkeypatch)
    _publish(queue, KEY_A, resources={"mem_gb": 8})
    queue.execute(queue.claim(capacity=None))
    assert "MemoryMax=8G" in seen[0]


def test_a_box_that_cannot_cap_degrades_loudly(queue, monkeypatch) -> None:
    """Today's behaviour, said out loud -- never a silent downgrade.

    Without the probe such a box would fail every capped action the instant
    it started them and the queue would requeue each one forever: a capability
    gap wearing the costume of a flaky job.
    """

    _capping(monkeypatch, False, "no cgroup delegation to the user manager")
    seen = _launched(monkeypatch)
    _publish(queue, KEY_A, resources={"mem_gb": 16})
    outcome = queue.execute(queue.claim())
    assert seen[0][0] != "systemd-run"
    assert outcome["capped"] is False
    assert outcome["declared_mem_gb"] == 16
    assert "delegation" in outcome["cap_unavailable"]


def test_the_outcome_record_carries_the_verdict_to_the_queue(
    queue, monkeypatch
) -> None:
    """``serve_once`` files ``execute``'s dict as the item's ``detail``."""

    _capping(monkeypatch, True)
    _launched(monkeypatch)
    _publish(queue, KEY_A, resources={"mem_gb": 5})
    queue.serve_once()
    import json

    filed = json.loads(queue.item_path(pool.DONE, KEY_A).read_text())
    assert filed["detail"]["capped"] is True
    assert filed["detail"]["declared_mem_gb"] == 5


# -- reading the unit back ----------------------------------------------------


def test_a_killed_child_is_reported_as_killed_not_as_exit_one(monkeypatch) -> None:
    """``systemd-run --wait`` reports how IT ended, not how the service did.

    A unit killed by its own cgroup returns 1 while recording
    ``Result=oom-kill`` and ``ExecMainStatus=9``.  Python's convention for a
    signalled child is a negative return code, so the translation is exact.
    """

    monkeypatch.setattr(
        pool, "_systemctl",
        lambda *_a, **_kw: subprocess.CompletedProcess(
            [], 0,
            stdout="Result=oom-kill\nExecMainCode=2\nExecMainStatus=9\n"
                   "MemoryPeak=4294967296\n",
            stderr="",
        ),
    )
    reported = pool.unit_outcome("u")
    assert reported["oom_killed"] is True
    assert reported["returncode"] == -9
    assert reported["memory_peak"] == 4294967296


def test_an_ordinary_exit_code_survives_the_wrapper(monkeypatch) -> None:
    monkeypatch.setattr(
        pool, "_systemctl",
        lambda *_a, **_kw: subprocess.CompletedProcess(
            [], 0, stdout="Result=exit-code\nExecMainCode=1\nExecMainStatus=3\n",
            stderr="",
        ),
    )
    reported = pool.unit_outcome("u")
    assert reported["returncode"] == 3 and reported["oom_killed"] is False


def test_an_unreadable_unit_leaves_the_launchers_answer_alone(monkeypatch) -> None:
    """systemd prints an unset 64-bit property as UINT64_MAX; not a status."""

    monkeypatch.setattr(pool, "_systemctl", lambda *_a, **_kw: None)
    reported = pool.unit_outcome("u")
    assert reported["returncode"] is None and reported["oom_killed"] is False


def test_an_oom_kill_is_named_in_the_error_the_caller_reads(
    queue, monkeypatch
) -> None:
    _capping(monkeypatch, True)
    _launched(monkeypatch)
    monkeypatch.setattr(pool, "unit_outcome", lambda _u: {
        "result": "oom-kill", "returncode": -9, "oom_killed": True,
        "memory_peak": 1073741824,
    })
    _publish(queue, KEY_A, resources={"mem_gb": 1})
    outcome = queue.execute(queue.claim())
    assert outcome["status"] == "failed" and outcome["returncode"] == -9
    assert outcome["oom_killed"] is True
    assert "declared 1 GB and exceeded it" in outcome["stderr"]


# -- what the fleet can see ---------------------------------------------------


def test_a_worker_publishes_what_its_cap_charges(queue) -> None:
    """Scope, not a bool.

    "This box enforces mem_gb" is exactly the sentence the issue said must not
    be published: on a GB10 the host half is bounded and the device half of the
    same 128 GB pool is not, so a submitter reading a bare ``true`` would take
    a limit for something it is not.
    """

    queue.announce(host="sparky", tags=["gb10"], has_gpu=True,
                   capacity={"mem_gb": 48}, mem_cap_scope=pool.MEM_CAP_SCOPE_HOST)
    assert queue.offers()[0]["mem_cap_scope"] == "host"


def test_an_offer_that_does_not_say_stays_unknown(queue) -> None:
    """Three-valued like ``placeable``: a submitter's guess is not a box's answer.

    A loop running bytes that predate this field says nothing, and nothing is
    what should be recorded -- not ``False``, which reads as "measured, and it
    does not enforce".
    """

    queue.announce(host="sparky", tags=["gb10"], has_gpu=True)
    assert queue.offers()[0]["mem_cap_scope"] is None


# -- the launcher's own preconditions -----------------------------------------


def test_a_capped_launch_never_inherits_a_closed_stdin(queue, monkeypatch) -> None:
    """``--pipe`` forwards stdin, and a closed fd 0 breaks it before the work.

    Measured 2026-09-04: ``systemd-run --user --pipe`` with fd 0 closed exits 1
    with "Failed to create bus message: Bad file descriptor" -- the work never
    starts, and the failure looks like the action's rather than the launcher's.
    A worker reads no stdin, so /dev/null costs nothing.
    """

    _capping(monkeypatch, True)
    seen: list[object] = []

    class _Done:
        returncode = 0

        def communicate(self, timeout=None):
            return ("", "")

    def _popen(_argv, *_a, **kw):
        seen.append(kw.get("stdin"))
        return _Done()

    monkeypatch.setattr(pool.subprocess, "Popen", _popen)
    monkeypatch.setattr(pool, "_systemctl", lambda *_a, **_kw: None)
    monkeypatch.setattr(pool, "unit_outcome", lambda _u: {})
    _publish(queue, KEY_A, resources={"mem_gb": 8})
    queue.execute(queue.claim())
    assert seen == [subprocess.DEVNULL]


def test_a_missing_user_bus_address_is_repaired_when_the_bus_is_there(
    monkeypatch, tmp_path
) -> None:
    """A loop spawned outside a login session has the bus but not its name.

    Answering "this box cannot cap" there would publish
    ``mem_cap_scope: none`` -- truthfully about the loop, falsely about the
    hardware -- and quietly stand every declaration back down to an honour
    system.
    """

    runtime = tmp_path / "run" / "user" / "4242"
    runtime.mkdir(parents=True)
    (runtime / "bus").write_text("")
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.setattr(pool.os, "getuid", lambda: 4242)
    monkeypatch.setattr(pool, "Path", lambda p: tmp_path / str(p).lstrip("/"))
    env = pool._bus_ready_env()
    assert env is not None and env["XDG_RUNTIME_DIR"] == str(runtime)


def test_a_box_with_no_user_manager_is_left_to_degrade_loudly(
    monkeypatch, tmp_path
) -> None:
    """No socket, no repair: a bad bus address fails later and less clearly."""

    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.setattr(pool.os, "getuid", lambda: 4242)
    monkeypatch.setattr(pool, "Path", lambda p: tmp_path / str(p).lstrip("/"))
    assert pool._bus_ready_env() is None


def test_an_environment_that_already_names_the_bus_is_inherited(monkeypatch) -> None:
    monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/1000")
    assert pool._bus_ready_env() is None


def test_the_whole_action_dies_together_not_one_process_of_it() -> None:
    """``OOMPolicy=kill`` sets ``memory.oom.group``; the default does not.

    Measured 2026-09-04 on a unit whose fat process was a grandchild: under the
    default ``stop`` the kernel takes the child and systemd then stops the
    unit, so the caller sees SIGTERM (``ExecMainStatus=15``); under ``kill``
    the tree goes together and the caller sees SIGKILL (9).  ``Result`` is
    ``oom-kill`` either way, but "terminated" is not what happened.
    """

    argv = pool.capped_launch_argv(["/bin/true"], cap_gb=4, unit="u")
    assert "OOMPolicy=kill" in argv
