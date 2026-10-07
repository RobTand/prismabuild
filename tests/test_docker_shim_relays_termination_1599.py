"""The Docker shim relays termination and stops its own container (#1599).

``tools/fleet/docker`` ran the real client with ``subprocess.run`` and no
handler, so a TERM to the shim ended the shim while the container, which
belongs to the daemon, kept running.  A guard that promises to stop a
workload then stopped only its launcher.

The shim now forwards TERM, INT and HUP to the client, waits a bounded grace,
and then stops the exact container this invocation created, after checking
that the container carries this attempt's owner label (and its scope label
when the action has one).  It never stops a container by name, and it writes
a receipt naming the real container and its final state.

Nothing here contacts a Docker daemon.  The shim's testing gate points it at a
fake CLI that plays a client, and a fake daemon behind ``inspect``, ``kill``
and ``ps``.
"""

from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
SHIM = ROOT / "tools" / "fleet" / "docker"
OWNER = "1" * 64
CID = "c" * 64

FAKE = r'''#!/usr/bin/env python3
import json, os, pathlib, signal, sys, time

state_path = pathlib.Path(os.environ["FAKE_STATE"])


def load():
    return json.loads(state_path.read_text()) if state_path.exists() else {"calls": []}


def save(data):
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(state_path)


def record(**event):
    data = load()
    data.setdefault("calls", []).append(event)
    save(data)


argv = sys.argv[1:]
if "context" in argv and "inspect" in argv:
    print(json.dumps("unix:///var/run/docker.sock"))
    sys.exit(0)
verb = next((t for t in argv if t in {"run", "create", "inspect", "kill", "ps", "rm"}), None)
if verb in ("inspect", "kill", "ps") and os.environ.get("FAKE_DAEMON_SLOW"):
    time.sleep(float(os.environ["FAKE_DAEMON_SLOW"]))

if verb in ("run", "create"):
    cidfile = argv[argv.index("--cidfile") + 1] if "--cidfile" in argv else None
    labels = {}
    for i, token in enumerate(argv):
        if token == "--label":
            key, _, value = argv[i + 1].partition("=")
            labels[key] = value
    mode = os.environ.get("FAKE_MODE", "honor")
    delay = float(os.environ.get("FAKE_DELAY_START", "0"))
    record(verb=verb, argv=argv)
    late = os.environ.get("FAKE_LATE_CREATE")
    if late:
        # The client has sent its create request; an independent process (the
        # daemon) completes it later, after the client may be gone.
        import subprocess
        code = (
            "import json, pathlib, sys, time\n"
            "time.sleep(float(sys.argv[1]))\n"
            "path = pathlib.Path(sys.argv[2])\n"
            "data = json.loads(path.read_text()) if path.exists() else {'calls': []}\n"
            "data.update(cid=sys.argv[3], labels=json.loads(sys.argv[4]), running=True, killed=False)\n"
            "tmp = path.with_suffix('.late'); tmp.write_text(json.dumps(data)); tmp.replace(path)\n")
        subprocess.Popen([sys.executable, "-c", code, late, str(state_path),
                          os.environ.get("FAKE_CID", "c" * 64), json.dumps(labels)],
                         start_new_session=True)
        pathlib.Path(os.environ["FAKE_STARTED"]).write_text("1")
        signal.signal(signal.SIGTERM, lambda *a: sys.exit(143))
        time.sleep(60)
        sys.exit(0)
    if delay:
        time.sleep(delay)
    data = load()
    data.update(cid=os.environ.get("FAKE_CID", "c" * 64), labels=labels,
                running=True, killed=False)
    save(data)
    start_later = os.environ.get("FAKE_START_LATER")
    if start_later:
        # The container exists but is only created; an independent process
        # (the daemon) starts it later, after the client may be gone.
        import subprocess
        data["running"] = False
        data["status"] = "created"
        save(data)
        code = (
            "import json, pathlib, sys, time\n"
            "time.sleep(float(sys.argv[1]))\n"
            "path = pathlib.Path(sys.argv[2])\n"
            "data = json.loads(path.read_text())\n"
            "if not data.get('removed'):\n"
            "    data.update(running=True, status='running')\n"
            "    tmp = path.with_suffix('.start'); tmp.write_text(json.dumps(data)); tmp.replace(path)\n")
        subprocess.Popen([sys.executable, "-c", code, start_later, str(state_path)],
                         start_new_session=True)
    if cidfile and os.environ.get("FAKE_NO_CIDFILE") != "1":
        pathlib.Path(cidfile).write_text(data["cid"])
    pathlib.Path(os.environ["FAKE_STARTED"]).write_text("1")

    def on_signal(signum, frame):
        record(event="signal-seen", signal=signum)
        if mode == "honor":
            current = load()
            current["running"] = False
            save(current)
            sys.exit(143)

    for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(number, on_signal)
    started = time.monotonic()
    exit_after = float(os.environ.get("FAKE_EXIT_AFTER", "0"))
    while True:
        current = load()
        if exit_after and time.monotonic() - started >= exit_after:
            sys.exit(0)
        if (not current.get("running", True) and mode != "stuck"
                and current.get("status") != "created"):
            sys.exit(137 if current.get("killed") else 143)
        time.sleep(0.02)

if verb == "rm":
    record(verb="rm", argv=argv)
    if os.environ.get("FAKE_RM_REJECTS") == "1":
        sys.stderr.write("Error response from daemon: removal denied\\n")
        sys.exit(1)
    data = load()
    if argv[-1] != data.get("cid"):
        sys.exit(1)
    data["running"] = False
    data["removed"] = True
    save(data)
    print(argv[-1])
    sys.exit(0)

if verb == "kill":
    record(verb="kill", argv=argv)
    if os.environ.get("FAKE_KILL_REJECTS") == "1":
        sys.stderr.write("Error response from daemon: permission denied\\n")
        sys.exit(1)
    data = load()
    if argv[-1] != data.get("cid"):
        sys.exit(1)
    if not data.get("running"):
        # The real daemon refuses to signal a container that is not running.
        sys.stderr.write("Error response from daemon: container is not running\\n")
        sys.exit(1)
    data["running"] = False
    data["killed"] = True
    data["status"] = "exited"
    if os.environ.get("FAKE_RM") == "1":
        data["removed"] = True          # --rm: a stopped container is gone
    save(data)
    print(argv[-1])
    sys.exit(0)

if verb == "inspect":
    record(verb="inspect", argv=argv)
    data = load()
    if os.environ.get("FAKE_INSPECT_FAILS") == "1":
        sys.stderr.write("Cannot connect to the Docker daemon at unix:///var/run/docker.sock\\n")
        sys.exit(1)
    if argv[-1] != data.get("cid") or data.get("removed"):
        sys.stderr.write("Error: No such object: " + argv[-1] + "\\n")
        sys.exit(1)
    labels = dict(data.get("labels", {}))
    labels.update(json.loads(os.environ.get("FAKE_LABEL_OVERRIDE", "{}")))
    running = bool(data.get("running"))
    status = data.get("status") or ("running" if running else "exited")
    print(json.dumps([{
        "Id": data["cid"],
        "Config": {"Labels": labels},
        "HostConfig": {"CgroupParent": ""},
        "State": {"Running": running, "Status": status,
                  "ExitCode": 137 if data.get("killed") else 143,
                  "OOMKilled": False, "FinishedAt": "2026-10-07T00:00:00Z"},
    }]))
    sys.exit(0)

if verb == "ps":
    record(verb="ps", argv=argv)
    if os.environ.get("FAKE_PS_FAILS") == "1":
        sys.exit(1)
    data = load()
    wanted = [argv[i + 1][len("label="):] for i, t in enumerate(argv)
              if t == "--filter" and argv[i + 1].startswith("label=")]
    labels = data.get("labels", {})
    if data.get("cid") and wanted and all(
            labels.get(item.partition("=")[0]) == item.partition("=")[2]
            for item in wanted):
        print(data["cid"])
    sys.exit(0)

sys.exit(0)
'''


def _start(tmp_path: Path, *, mode: str = "honor", grace: str | None = "1",
           kill_wait: str | None = "1", extra: dict | None = None,
           argv: list[str] | None = None):
    real = tmp_path / "fake-docker"
    real.write_text(FAKE)
    real.chmod(0o755)
    cgroup = tmp_path / "cgroup"
    cgroup.write_text("0::/test-unscoped\n")
    environment = dict(os.environ)
    environment.update({
        "FAKE_STATE": str(tmp_path / "state.json"),
        "FAKE_STARTED": str(tmp_path / "started"),
        "FAKE_MODE": mode,
        "PRISMABUILD_CONTAINER_OWNER": OWNER,
        "PRISMABUILD_CONTAINER_MARKER": str(tmp_path / "owner.used"),
        "PRISMABUILD_DOCKER_REAL": str(real),
        "PRISMABUILD_DOCKER_TESTING": "1",
        "PRISMABUILD_CGROUP_FILE": str(cgroup),
    })
    for name, value in (("PRISMABUILD_DOCKER_STOP_GRACE_S", grace),
                        ("PRISMABUILD_DOCKER_STOP_KILL_WAIT_S", kill_wait)):
        if value is not None:
            environment[name] = value
    environment.update(extra or {})
    out = (tmp_path / "out").open("w")
    err = (tmp_path / "err").open("w")
    process = subprocess.Popen([str(SHIM), *(argv or ["run", "example:image"])],
                               env=environment, stdout=out, stderr=err)
    return process


def _wait_started(tmp_path: Path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (tmp_path / "started").exists():
            return
        time.sleep(0.02)
    raise AssertionError("the fake client never started: "
                         + (tmp_path / "err").read_text())


def _state(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "state.json").read_text())


def _calls(tmp_path: Path, verb: str) -> list[dict]:
    return [call for call in _state(tmp_path).get("calls", [])
            if call.get("verb") == verb]


def _receipts(tmp_path: Path) -> list[dict]:
    return [json.loads(path.read_text())
            for path in sorted(tmp_path.glob("owner.used.stop-*.json"))]


def test_a_term_reaches_the_client_and_the_receipt_names_the_container(tmp_path):
    process = _start(tmp_path, mode="honor")
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    assert process.wait(timeout=15) == 128 + signal.SIGTERM
    events = [c for c in _state(tmp_path)["calls"] if c.get("event") == "signal-seen"]
    assert [e["signal"] for e in events] == [int(signal.SIGTERM)]
    assert _calls(tmp_path, "kill") == []
    receipts = _receipts(tmp_path)
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["container_id"] == CID
    assert receipt["owner"] == OWNER
    assert receipt["outcome"] == "stopped"
    assert receipt["signals"][0]["signal"] == int(signal.SIGTERM)
    assert receipt["escalation"] == {"container_killed": False, "client_killed": False, "kill_rejected": 0,
                                     "container_removed": False, "remove_rejected": 0}
    assert receipt["container_final"]["running"] is False


def test_a_term_ignoring_workload_has_its_exact_container_killed(tmp_path):
    process = _start(tmp_path, mode="ignore", grace="1")
    _wait_started(tmp_path)
    began = time.monotonic()
    process.send_signal(signal.SIGTERM)
    assert process.wait(timeout=15) == 128 + signal.SIGTERM
    elapsed = time.monotonic() - began
    assert 0.9 <= elapsed < 8, elapsed
    kills = _calls(tmp_path, "kill")
    assert len(kills) == 1
    assert kills[0]["argv"][-1] == CID
    assert "KILL" in " ".join(kills[0]["argv"]).upper()
    receipt = _receipts(tmp_path)[0]
    assert receipt["outcome"] == "killed"
    assert receipt["escalation"]["container_killed"] is True
    assert receipt["container_final"]["running"] is False


def test_a_removed_container_after_our_kill_is_reported_as_killed(tmp_path):
    """With --rm the container is gone once stopped; absent after a kill is a kill."""
    process = _start(tmp_path, mode="ignore", grace="1", extra={"FAKE_RM": "1"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=15)
    receipt = _receipts(tmp_path)[0]
    assert receipt["escalation"]["container_killed"] is True
    assert receipt["container_final"]["found"] is False
    assert receipt["outcome"] == "killed"


def test_a_removed_container_after_a_clean_stop_is_reported_as_stopped(tmp_path):
    process = _start(tmp_path, mode="honor", extra={"FAKE_RM": "1"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=15)
    receipt = _receipts(tmp_path)[0]
    assert receipt["escalation"]["container_killed"] is False
    assert receipt["outcome"] == "stopped"


def test_a_container_this_attempt_does_not_own_is_never_killed(tmp_path):
    other = {"prismabuild.action": "2" * 64}
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_LABEL_OVERRIDE": json.dumps(other)})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=15)
    assert _calls(tmp_path, "kill") == []
    receipt = _receipts(tmp_path)[0]
    assert receipt["outcome"] == "not_owned"
    assert receipt["escalation"]["container_killed"] is False
    assert receipt["escalation"]["client_killed"] is True


def test_without_a_cidfile_the_attempt_label_finds_the_container(tmp_path):
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_NO_CIDFILE": "1"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=15)
    probes = _calls(tmp_path, "ps")
    assert probes and any("prismabuild.shim=" in " ".join(p["argv"]) for p in probes)
    kills = _calls(tmp_path, "kill")
    assert len(kills) == 1 and kills[0]["argv"][-1] == CID
    receipt = _receipts(tmp_path)[0]
    assert receipt["container_id"] == CID
    assert receipt["found_by"] == "label"


def test_a_signal_before_the_container_exists_is_unresolved_and_not_a_stop(tmp_path):
    """An empty answer is not proof that no container will appear (a delayed create)."""
    process = _start(tmp_path, mode="slow", grace="1",
                     extra={"FAKE_DELAY_START": "30"})
    time.sleep(0.5)
    process.send_signal(signal.SIGTERM)
    assert process.wait(timeout=20) == 125
    assert _calls(tmp_path, "kill") == []
    receipt = _receipts(tmp_path)[0]
    assert receipt["outcome"] == "creation_unresolved"
    assert receipt["stop_proved"] is False
    assert receipt["container_id"] is None
    assert len(_calls(tmp_path, "ps")) >= 2          # it kept watching to the deadline


def test_a_container_created_after_the_client_is_gone_is_found_and_stopped(tmp_path):
    """An independent daemon request survives the client and creates the container late."""
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_LATE_CREATE": "1.2"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    code = process.wait(timeout=20)
    receipt = _receipts(tmp_path)[0]
    assert receipt["container_id"] == CID
    assert receipt["found_by"] == "label"
    assert receipt["outcome"] in {"stopped", "killed"}, receipt
    assert receipt["stop_proved"] is True
    assert code == 128 + signal.SIGTERM
    kills = _calls(tmp_path, "kill")
    assert kills and kills[-1]["argv"][-1] == CID


def test_a_created_container_is_removed_so_a_late_start_cannot_land(tmp_path):
    """Running=false is not stopped: an accepted start can complete after the client."""
    process = _start(tmp_path, mode="honor", grace="1",
                     extra={"FAKE_START_LATER": "2.5"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    code = process.wait(timeout=20)
    time.sleep(3.0)                       # past the moment the late start would land
    receipt = _receipts(tmp_path)[0]
    assert receipt["stop_proved"] is True and code == 128 + signal.SIGTERM
    assert receipt["escalation"]["container_removed"] is True
    removals = _calls(tmp_path, "rm")
    assert len(removals) >= 1 and removals[-1]["argv"][-1] == CID
    assert _state(tmp_path)["running"] is False        # the late start found it removed


def test_a_created_container_that_cannot_be_removed_is_start_unresolved(tmp_path):
    process = _start(tmp_path, mode="honor", grace="1",
                     extra={"FAKE_START_LATER": "30", "FAKE_RM_REJECTS": "1"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    code = process.wait(timeout=20)
    assert code == 125, code
    receipt = _receipts(tmp_path)[0]
    assert receipt["stop_proved"] is False
    assert receipt["outcome"] == "start_unresolved"
    assert receipt["escalation"]["remove_rejected"] >= 1
    assert receipt["escalation"]["container_removed"] is False


def test_a_creation_that_lands_after_the_deadline_is_reported_and_not_a_stop(tmp_path):
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_LATE_CREATE": "30"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    assert process.wait(timeout=20) == 125
    receipt = _receipts(tmp_path)[0]
    assert receipt["outcome"] == "creation_unresolved"
    assert receipt["stop_proved"] is False


def test_a_failed_container_query_is_unknown_and_not_an_empty_answer(tmp_path):
    """The D44 helper's rule: a failed Docker query is not 'no container'."""
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_NO_CIDFILE": "1", "FAKE_PS_FAILS": "1"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=15)
    assert _calls(tmp_path, "kill") == []
    receipt = _receipts(tmp_path)[0]
    assert receipt["outcome"] == "unknown"
    assert receipt["container_id"] is None
    assert receipt["found_by"] == "unreadable"


def test_the_wait_is_bounded_when_the_client_survives_everything_but_kill(tmp_path):
    process = _start(tmp_path, mode="stuck", grace="1", kill_wait="1")
    _wait_started(tmp_path)
    began = time.monotonic()
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=20)
    assert time.monotonic() - began < 10
    assert len(_calls(tmp_path, "kill")) == 1
    receipt = _receipts(tmp_path)[0]
    assert receipt["escalation"] == {"container_killed": True, "client_killed": True, "kill_rejected": 0,
                                     "container_removed": False, "remove_rejected": 0}


def test_without_a_signal_there_is_no_receipt_and_no_cidfile_left(tmp_path):
    process = _start(tmp_path, mode="honor", extra={"FAKE_EXIT_AFTER": "0.3"})
    assert process.wait(timeout=15) == 0
    assert _receipts(tmp_path) == []
    cidfile = _calls(tmp_path, "run")[0]["argv"]
    path = Path(cidfile[cidfile.index("--cidfile") + 1])
    assert not path.exists()


def test_the_cidfile_and_attempt_label_are_added_after_the_owner_label(tmp_path):
    process = _start(tmp_path, mode="honor", extra={"FAKE_EXIT_AFTER": "0.2"})
    assert process.wait(timeout=15) == 0
    argv = _calls(tmp_path, "run")[0]["argv"]
    created = argv.index("run") + 1
    assert argv[created:created + 2] == ["--label", f"prismabuild.action={OWNER}"]
    labels = [argv[i + 1] for i, t in enumerate(argv) if t == "--label"]
    assert sum(label.startswith("prismabuild.shim=") for label in labels) == 1
    assert argv.count("--cidfile") == 1


def test_a_caller_cannot_forge_the_attempt_label(tmp_path):
    process = _start(tmp_path, argv=["run", "--label", "prismabuild.shim=x", "example:image"])
    assert process.wait(timeout=15) == 125


def test_a_caller_cidfile_is_used_and_kept(tmp_path):
    mine = tmp_path / "mine.cid"
    process = _start(tmp_path, mode="honor", extra={"FAKE_EXIT_AFTER": "0.2"},
                     argv=["run", "--cidfile", str(mine), "example:image"])
    assert process.wait(timeout=15) == 0
    assert mine.read_text() == CID
    assert _calls(tmp_path, "run")[0]["argv"].count("--cidfile") == 1


@pytest.mark.parametrize("raw,expected", [
    (None, 1.5), ("", 1.5), ("abc", 1.5), ("-1", 1.5), ("nan", 1.5), ("0", 1.5),
    ("0.5", 0.5), ("7", 7.0), ("999", 30.0),
])
def test_the_grace_is_parsed_bounded_and_defaulted(raw, expected):
    loader = importlib.machinery.SourceFileLoader("pb_docker_shim_1599", str(SHIM))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    previous = os.environ.pop("PRISMABUILD_DOCKER_STOP_GRACE_S", None)
    try:
        if raw is not None:
            os.environ["PRISMABUILD_DOCKER_STOP_GRACE_S"] = raw
        assert module._stop_grace() == expected
    finally:
        os.environ.pop("PRISMABUILD_DOCKER_STOP_GRACE_S", None)
        if previous is not None:
            os.environ["PRISMABUILD_DOCKER_STOP_GRACE_S"] = previous


def test_a_failed_inspect_is_unknown_and_never_a_stop(tmp_path):
    """A daemon that cannot be queried proves nothing about the container."""
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_INSPECT_FAILS": "1"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    code = process.wait(timeout=15)
    assert code == 125, code            # not 143: no stop was proven
    receipt = _receipts(tmp_path)[0]
    assert receipt["outcome"] == "unknown"
    assert receipt["escalation"]["container_killed"] is False
    assert _calls(tmp_path, "kill") == []   # ownership could not be shown


def test_a_rejected_kill_is_not_a_kill_and_the_shim_does_not_claim_a_stop(tmp_path):
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_KILL_REJECTS": "1"})
    _wait_started(tmp_path)
    began = time.monotonic()
    process.send_signal(signal.SIGTERM)
    code = process.wait(timeout=20)
    assert time.monotonic() - began < 6
    assert code == 125, code
    receipt = _receipts(tmp_path)[0]
    assert receipt["outcome"] == "still_running"
    assert receipt["escalation"]["container_killed"] is False
    assert receipt["escalation"]["kill_rejected"] >= 1
    assert receipt["container_final"]["running"] is True
    assert len(_calls(tmp_path, "kill")) >= 2        # it retried within the bound


def test_the_relay_handlers_stay_armed_through_the_stop(tmp_path):
    """A second TERM during cleanup must not kill the shim before its receipt."""
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_KILL_REJECTS": "1"})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    time.sleep(1.4)                      # past the grace: the stop path is running
    process.send_signal(signal.SIGTERM)
    code = process.wait(timeout=20)
    assert code == 125, code             # not -15: the handler was still armed
    assert _receipts(tmp_path)[0]["outcome"] == "still_running"


def test_a_hung_daemon_cannot_stretch_the_stop_past_the_guard_interval(tmp_path):
    """Default settings: the whole stop fits the five seconds a guard allows."""
    process = _start(tmp_path, mode="ignore", grace=None, kill_wait=None,
                     extra={"FAKE_DAEMON_SLOW": "30"})
    _wait_started(tmp_path)
    began = time.monotonic()
    process.send_signal(signal.SIGTERM)
    code = process.wait(timeout=20)
    elapsed = time.monotonic() - began
    assert elapsed < 5.0, elapsed
    assert code == 125, code
    assert _receipts(tmp_path)[0]["outcome"] in {"unknown", "still_running"}


def test_the_default_stop_of_a_term_ignoring_workload_fits_the_guard_interval(tmp_path):
    process = _start(tmp_path, mode="ignore", grace=None, kill_wait=None)
    _wait_started(tmp_path)
    began = time.monotonic()
    process.send_signal(signal.SIGTERM)
    assert process.wait(timeout=20) == 128 + signal.SIGTERM
    assert time.monotonic() - began < 5.0
    assert _receipts(tmp_path)[0]["outcome"] == "killed"


@pytest.mark.parametrize("number", [signal.SIGINT, signal.SIGHUP])
def test_int_and_hup_are_relayed_like_term(tmp_path, number):
    process = _start(tmp_path, mode="honor")
    _wait_started(tmp_path)
    process.send_signal(number)
    assert process.wait(timeout=15) == 128 + int(number)
    seen = [c["signal"] for c in _state(tmp_path)["calls"] if c.get("event") == "signal-seen"]
    assert seen == [int(number)]
    assert _receipts(tmp_path)[0]["outcome"] == "stopped"


def test_a_nonce_mismatch_is_not_owned_and_is_never_killed(tmp_path):
    process = _start(tmp_path, mode="ignore", grace="1",
                     extra={"FAKE_LABEL_OVERRIDE": json.dumps({"prismabuild.shim": "0" * 32})})
    _wait_started(tmp_path)
    process.send_signal(signal.SIGTERM)
    code = process.wait(timeout=15)
    assert code == 125, code
    assert _calls(tmp_path, "kill") == []
    assert _receipts(tmp_path)[0]["outcome"] == "not_owned"


def _shim_module():
    loader = importlib.machinery.SourceFileLoader("pb_docker_shim_1599_own", str(SHIM))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


SCOPE = ("prismabuild-job" + "a" * 32 + ".slice", 1)


@pytest.mark.parametrize("labels,parent,scope,expected", [
    ({"prismabuild.action": OWNER, "prismabuild.shim": "n"}, "", None, True),
    ({"prismabuild.action": "2" * 64, "prismabuild.shim": "n"}, "", None, False),
    ({"prismabuild.action": OWNER, "prismabuild.shim": "other"}, "", None, False),
    ({"prismabuild.action": OWNER, "prismabuild.shim": "n",
      "prismabuild.scope": SCOPE[0]}, SCOPE[0], SCOPE, True),
    ({"prismabuild.action": OWNER, "prismabuild.shim": "n",
      "prismabuild.scope": "prismabuild-job" + "b" * 32 + ".slice"}, SCOPE[0], SCOPE, False),
    ({"prismabuild.action": OWNER, "prismabuild.shim": "n",
      "prismabuild.scope": SCOPE[0]}, "elsewhere.slice", SCOPE, False),
    ({"prismabuild.action": OWNER, "prismabuild.shim": "n"}, SCOPE[0], SCOPE, False),
])
def test_ownership_needs_owner_nonce_and_the_scope_when_scoped(labels, parent, scope, expected):
    row = {"Config": {"Labels": labels}, "HostConfig": {"CgroupParent": parent}}
    assert _shim_module()._owned_by_this_attempt(row, OWNER, scope, "n") is expected
