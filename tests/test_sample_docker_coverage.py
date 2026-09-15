"""``--profile sample`` cannot see a workload the Docker daemon starts (#562).

py-spy follows the action's own process tree, and a container's payload is a
child of ``containerd-shim`` rather than of the action.  Three claims, and all
three are needed:

*   **The route is refused early.** Under the sample guard, PB's Docker shim
    refuses run/create/exec and start/compose-start before contacting the
    daemon, the way ``nsys`` does (#513).  Metadata reads still work.
*   **The marker is a pre-armed handshake.** The worker arms the marker before
    the action starts; the shim replaces the armed record with the attempted
    route before it refuses.  A marker that is missing, unreadable, malformed,
    nonregular, a symlink, oversized or in an unknown state is *unknown
    coverage* -- never a clean profile -- so a launcher that swallows the
    ``125`` cannot make a host-only speedscope look like a covered one.
*   **Uncovered means no receipt.** A record with ``produced: false`` refuses
    receipt publication even when the launcher exits zero, while the host-side
    blob, its digest and the negative coverage metadata survive on the error,
    in the status sidecar, and in the ingested result the message names.  A
    nonzero action still reports its own returncode.

The reported defect is a real accepted run: action ``8d40d7832fda`` recorded
``produced: true`` over CAS blob ``dcbf21df...6321d``, which held 5 relay and 6
Docker-shim samples and no workload frame at all.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from test_docker_shim_global_options import _shim  # noqa: E402
from test_sample_profile import _FakeBackend  # noqa: E402


REPOSITORY = Path(__file__).resolve().parents[1]
DOCKER_SHIM = REPOSITORY / "tools" / "fleet" / "docker"

#: The contract strings, literal so a baseline checkout without the new
#: constants still exercises the same environment and marker shape.
SAMPLE_GUARD = "PRISMABUILD_PROFILE_SAMPLE"
SAMPLE_MARKER = "PRISMABUILD_PROFILE_SAMPLE_MARKER"
ROUTE_FILENAME = "container-route"
ROUTE_SCHEMA = "prismabuild.profile_container_route.v1"

#: Long enough that a 100 Hz sampler would see it; the shaped backend here does
#: not sample, so this only has to be real work, not a sampling window.
_WORK = ("import math; "
         "print(sum(sum(math.sqrt(i) for i in range(20000)) for _ in range(160)))")

#: A native workload whose frame the real sampler must record.
_NATIVE_WORKLOAD_MARKER = "prismabuild_562_native_workload"
_NATIVE_WORKLOAD = (
    "import time\n"
    "def prismabuild_562_native_workload():\n"
    "    total = 0.0\n"
    "    end = time.monotonic() + 2.5\n"
    "    while time.monotonic() < end:\n"
    "        for i in range(50000):\n"
    "            total += i * 0.5\n"
    "    return total\n"
    "print(prismabuild_562_native_workload())\n"
)


def _marker_path(session) -> Path:
    return session.profile_path.parent / ROUTE_FILENAME


def _sample_session(tmp_path: Path, *, arm: bool = True):
    session = pb._ProfileSession(
        mode="sample",
        backend=pb.PySpyProfileBackend(),
        directory=tmp_path / "profile",
    )
    session.directory.mkdir(parents=True, exist_ok=True)
    if arm:
        session._arm_container_route()
    return session


def _write_marker(session, raw: bytes) -> None:
    _marker_path(session).write_bytes(raw)


def _valid_record(state: str, **fields: object) -> bytes:
    document = {"schema": ROUTE_SCHEMA, "state": state, **fields}
    return (json.dumps(document, sort_keys=True) + "\n").encode()


# -- the shim guard ----------------------------------------------------------


@pytest.mark.parametrize(("argv", "route"), [
    (["run", "--rm", "workload:image", "python3", "x.py"], "docker run"),
    (["--context=default", "run", "workload:image"], "docker run"),
    (["-cdefault", "container", "run", "workload:image"], "docker container run"),
    (["create", "workload:image"], "docker create"),
    (["--debug", "container", "create", "workload:image"],
     "docker container create"),
    (["exec", "owned-container", "python3", "x.py"], "docker exec"),
    (["--context", "default", "container", "exec", "owned-container", "true"],
     "docker container exec"),
    (["start", "old-container"], "docker start"),
    (["--debug", "container", "start", "old-container"],
     "docker container start"),
    (["compose", "-f", "compose.yml", "up", "-d"], "docker compose up"),
])
def test_the_sample_guard_refuses_container_routes_before_the_daemon(
    tmp_path: Path, argv: list[str], route: str
):
    """The environment the worker really sets, through the real shim."""

    session = _sample_session(tmp_path)
    result, _marked, forwarded = _shim(
        tmp_path, argv, docker_env=session.environment({})
    )
    assert result.returncode == 125, result.stderr
    assert "--profile sample" in result.stderr
    assert "Docker daemon" in result.stderr
    assert "native" in result.stderr
    assert forwarded is None
    # The route is recorded before the refusal, so a launcher that swallows
    # the 125 cannot leave the profile looking like it covered the workload.
    record = json.loads(_marker_path(session).read_text(encoding="utf-8"))
    assert record == {"schema": ROUTE_SCHEMA, "state": "route", "route": route}
    # Refused before contacting the daemon, not merely failed by it.
    assert not (tmp_path / "called.json").exists()
    assert not (tmp_path / "inspected.json").exists()


def test_the_sample_guard_still_allows_docker_metadata_reads(tmp_path: Path):
    session = _sample_session(tmp_path)
    result, marked, forwarded = _shim(
        tmp_path, ["image", "inspect", "workload:image"],
        docker_env=session.environment({}),
    )
    assert result.returncode == 0, result.stderr
    assert forwarded == ["image", "inspect", "workload:image"]
    assert not marked
    # A metadata read leaves the pre-armed marker armed: nothing to record.
    assert json.loads(_marker_path(session).read_text(encoding="utf-8")) == {
        "schema": ROUTE_SCHEMA, "state": "armed",
    }


def test_the_sample_mode_does_not_take_over_a_sealed_guard(tmp_path: Path):
    session = _sample_session(tmp_path)
    with pytest.raises(pb.LocalActionError, match="sealed environment already sets"):
        session.environment({SAMPLE_GUARD: "0"})


def test_an_unarmable_marker_refuses_the_action(tmp_path: Path):
    """The handshake cannot silently become optional."""

    session = pb._ProfileSession(
        mode="sample",
        backend=pb.PySpyProfileBackend(),
        directory=tmp_path / "profile",
    )
    session.directory.mkdir()
    session.directory.chmod(0o500)
    try:
        with pytest.raises(pb.LocalActionError) as raised:
            session._arm_container_route()
    finally:
        session.directory.chmod(0o700)
    message = str(raised.value)
    assert ROUTE_FILENAME in message
    assert "before the action started" in message


# -- the marker's states -----------------------------------------------------


def test_an_armed_marker_is_a_clean_profile(tmp_path: Path):
    session = _sample_session(tmp_path)
    assert session.container_route() is None
    assert session.container_route_path == _marker_path(session)


def test_a_recorded_route_is_reported_by_name(tmp_path: Path):
    session = _sample_session(tmp_path)
    _write_marker(session, _valid_record("route", route="docker run"))
    assert session.container_route() == "docker run"


def test_an_absent_marker_after_preinit_is_unknown_coverage(tmp_path: Path):
    session = _sample_session(tmp_path)
    _marker_path(session).unlink()
    assert session.container_route() == "unrecorded"


@pytest.mark.parametrize(("name", "raw"), [
    ("empty", b""),
    ("not json", b"docker run\n"),
    ("not utf8", b"\xff\xfe\x00"),
    ("wrong schema",
     b'{"schema":"other","state":"route","route":"docker run"}'),
    ("unknown state", _valid_record("something-else")),
    ("route not a string", _valid_record("route", route=7)),
    ("route blank", _valid_record("route", route="  ")),
    ("route too long", _valid_record("route", route="d" * 129)),
    ("duplicate key", b'{"schema":"' + ROUTE_SCHEMA.encode()
     + b'","state":"armed","state":"route"}'),
    ("oversized", b"x" * 5000),
])
def test_any_state_the_schema_does_not_name_is_unknown_coverage(
    tmp_path: Path, name: str, raw: bytes
):
    session = _sample_session(tmp_path, arm=False)
    _write_marker(session, raw)
    assert session.container_route() == "unrecorded", name


def test_a_symlinked_marker_is_unknown_coverage(tmp_path: Path):
    """A read that followed the link would report whatever it pointed at."""

    session = _sample_session(tmp_path)
    target = tmp_path / "elsewhere"
    target.write_text(_valid_record("armed").decode(), encoding="utf-8")
    _marker_path(session).unlink()
    os.symlink(target, _marker_path(session))
    assert session.container_route() == "unrecorded"


def test_a_fifo_marker_neither_blocks_nor_certifies(tmp_path: Path):
    """``read_text()`` on a FIFO blocks; the bounded reader must not."""

    session = _sample_session(tmp_path, arm=False)
    os.mkfifo(_marker_path(session))
    try:
        assert stat.S_ISFIFO(_marker_path(session).lstat().st_mode)
        assert session.container_route() == "unrecorded"
    finally:
        _marker_path(session).unlink()


def test_a_directory_marker_is_unknown_coverage(tmp_path: Path):
    session = _sample_session(tmp_path, arm=False)
    _marker_path(session).mkdir()
    assert session.container_route() == "unrecorded"


def test_a_backend_that_can_see_containers_ignores_the_marker(tmp_path: Path):
    session = pb._ProfileSession(
        mode="fake", backend=_FakeBackend(), directory=tmp_path / "scratch",
    )
    session.directory.mkdir()
    _write_marker(session, _valid_record("route", route="docker run"))
    assert session.container_route() is None


# -- writer safety -----------------------------------------------------------


def test_arming_replaces_a_symlinked_marker_without_following_it(tmp_path: Path):
    """The staging inode is fresh; the destination is replaced, not written."""

    session = _sample_session(tmp_path, arm=False)
    victim = tmp_path / "victim"
    victim.write_text("keep\n", encoding="utf-8")
    os.symlink(victim, _marker_path(session))
    session._arm_container_route()
    assert victim.read_text(encoding="utf-8") == "keep\n"
    assert not _marker_path(session).is_symlink()
    assert json.loads(_marker_path(session).read_text(encoding="utf-8")) == {
        "schema": ROUTE_SCHEMA, "state": "armed",
    }
    assert not list(session.directory.glob("*.tmp"))


def test_the_shim_replaces_a_symlinked_marker_without_following_it(tmp_path: Path):
    session = _sample_session(tmp_path)
    victim = tmp_path / "victim"
    victim.write_text("keep\n", encoding="utf-8")
    _marker_path(session).unlink()
    os.symlink(victim, _marker_path(session))
    result, _marked, _forwarded = _shim(
        tmp_path, ["run", "workload:image"], docker_env=session.environment({})
    )
    assert result.returncode == 125, result.stderr
    assert victim.read_text(encoding="utf-8") == "keep\n"
    assert json.loads(_marker_path(session).read_text(encoding="utf-8")) == {
        "schema": ROUTE_SCHEMA, "state": "route", "route": "docker run",
    }
    assert not list(session.directory.glob("*.tmp"))


def test_the_shim_does_not_block_on_a_fifo_marker(tmp_path: Path):
    """A FIFO at the marker path must not stall the refusal."""

    session = _sample_session(tmp_path, arm=False)
    os.mkfifo(_marker_path(session))
    result, _marked, _forwarded = _shim(
        tmp_path, ["run", "workload:image"], docker_env=session.environment({}),
        timeout=30,
    )
    assert result.returncode == 125, result.stderr
    assert not stat.S_ISFIFO(_marker_path(session).lstat().st_mode)
    assert json.loads(_marker_path(session).read_text(encoding="utf-8")) == {
        "schema": ROUTE_SCHEMA, "state": "route", "route": "docker run",
    }
    assert not list(session.directory.glob("*.tmp"))


def test_a_marker_the_shim_cannot_write_is_named_in_the_refusal(tmp_path: Path):
    """An unrecordable route must not be presented as a recorded one.

    The scratch directory is the action's own, so this is the documented
    tampering limitation rather than a security boundary: when the shim cannot
    install the record, the refusal says so and the settlement sees unknown
    coverage if the leaf was removable.
    """

    session = _sample_session(tmp_path)
    session.directory.chmod(0o500)
    try:
        result, _marked, _forwarded = _shim(
            tmp_path, ["run", "workload:image"], docker_env=session.environment({})
        )
    finally:
        session.directory.chmod(0o700)
    assert result.returncode == 125, result.stderr
    assert "could not be written to the profile marker" in result.stderr
    assert "unprofiled" in result.stderr


# -- the coverage record -----------------------------------------------------


class _SampleShapedBackend(_FakeBackend):
    """The real sample mode's environment contract, without py-spy.

    Coverage is a property of the environment the Docker shim reads and of the
    marker it leaves, not of py-spy's capture, so this drives the real
    ``PySpyProfileBackend.environment`` through a backend that writes a valid
    speedscope.  The environment falls back to the literal contract on a
    baseline checkout whose ``PySpyProfileBackend`` predates the mode, so the
    swallowed-refusal regression reaches receipt publication and fails there
    instead of erroring on a missing attribute.  The real sampler is exercised
    by ``test_sample_profile.py`` and by the native-frames test below.
    """

    mode = "sample"
    name = "py-spy"
    rate_hz = getattr(pb, "PROFILE_SAMPLE_RATE_HZ", 100)
    container_routes_unsupported = True

    def __init__(self) -> None:
        super().__init__()
        self._real = pb.PySpyProfileBackend()

    def environment(self, *, profile_path: Path):
        method = getattr(pb.PySpyProfileBackend, "environment", None)
        if method is not None:
            return method(self._real, profile_path=profile_path)
        return {
            SAMPLE_GUARD: "1",
            SAMPLE_MARKER: str(Path(profile_path).parent / ROUTE_FILENAME),
        }


@pytest.fixture
def sample_backend(monkeypatch: pytest.MonkeyPatch) -> _SampleShapedBackend:
    backend = _SampleShapedBackend()
    monkeypatch.setitem(pb.PROFILE_BACKENDS, "sample", backend)
    return backend


def _fake_docker(tmp_path: Path) -> str:
    real = tmp_path / "fake-docker"
    real.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib, sys\n"
        "if 'context' in sys.argv and 'inspect' in sys.argv:\n"
        "    pathlib.Path(os.environ['INSPECTED']).write_text(json.dumps(sys.argv[1:]))\n"
        "    print(os.environ['FAKE_ENDPOINT'])\n"
        "    sys.exit(0)\n"
        "pathlib.Path(os.environ['CALLED']).write_text(json.dumps(sys.argv[1:]))\n"
        "sys.exit(0)\n",
        encoding="utf-8",
    )
    real.chmod(0o755)
    return str(real)


def _shim_environment(tmp_path: Path) -> dict[str, str]:
    cgroup = tmp_path / "cgroup"
    cgroup.write_text("0::/test-unscoped\n", encoding="utf-8")
    return {
        "PRISMABUILD_CONTAINER_OWNER": "1" * 64,
        "PRISMABUILD_CONTAINER_MARKER": str(tmp_path / "owner.used"),
        "PRISMABUILD_DOCKER_TESTING": "1",
        "PRISMABUILD_DOCKER_REAL": _fake_docker(tmp_path),
        "PRISMABUILD_CGROUP_FILE": str(cgroup),
        "INSPECTED": str(tmp_path / "inspected.json"),
        "CALLED": str(tmp_path / "called.json"),
        "FAKE_ENDPOINT": "unix:///var/run/docker.sock",
    }


def _sealed_action(checkout: Path, tmp_path: Path, argv: list[str],
                   files: dict[str, str] | None = None) -> dict:
    (checkout / "task_code.py").write_text("# closure member\n", encoding="utf-8")
    closure = ["task_code.py"]
    for name, text in (files or {}).items():
        (checkout / name).write_text(text, encoding="utf-8")
        closure.append(name)
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/sample-docker-coverage",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": argv,
            "working_directory": ".",
            "result_path": "result.txt",
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, closure),
        "params": {"command": ["work"], "profile": "sample"},
        "environment": {
            "variables": {"PATH": "/usr/bin:/bin", **_shim_environment(tmp_path)},
            "toolchain": {},
        },
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    }
    return pb.seal_action(body)


def _action_argv(script: str) -> list[str]:
    return ["/bin/bash", "--noprofile", "--norc", "-c", script]


def _shim_call(*args: str) -> str:
    return f"{sys.executable} {DOCKER_SHIM} {' '.join(args)}"


def _result_blob_digest(message: str) -> str | None:
    for word in message.replace(",", " ").replace(".", " ").split():
        if len(word) == 64 and all(ch in "0123456789abcdef" for ch in word):
            return word
    return None


def test_a_swallowed_refusal_publishes_no_receipt(
    tmp_path: Path, sample_backend: _SampleShapedBackend,
    monkeypatch: pytest.MonkeyPatch,
):
    """The regression: before the fix this action published ``produced: true``.

    The action invokes the real shim (with a fake docker behind its testing
    gate) and swallows the ``125`` -- exactly the launcher the review warned
    about.  The profile still must not certify coverage, and the uncovered
    record must refuse receipt publication rather than poisoning the key.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    status = tmp_path / "status.json"
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status))
    work = f"{sys.executable} -c {_WORK!r} 2>&1 | tee result.txt"
    action = _sealed_action(checkout, tmp_path, _action_argv(
        f"{_shim_call('run', '--rm', 'workload:image', 'python3', 'x.py')}"
        f" || true; {work}; exit ${{PIPESTATUS[0]}}"
    ))
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            action, cas_root=tmp_path / "cas", checkout_root=checkout
        )
    error = raised.value
    assert error.returncode is None, "a zero-exit action is a worker verdict"
    profile = error.profile
    assert profile["produced"] is False
    assert profile["workload_coverage"] == "unsupported"
    assert profile["container_route"] == "docker run"
    assert "Docker container route (docker run)" in str(profile["reason"])
    assert "No receipt was published" in str(error)
    # The host-side blob is still filed; it is evidence of what was sampled.
    assert profile["samples"] == 3
    blob = (tmp_path / "cas" / "blobs"
            / str(profile["blob_sha256"])[:2] / str(profile["blob_sha256"]))
    assert json.loads(blob.read_text())["$schema"] == pb.PROFILE_SPEEDSCOPE_SCHEMA
    # No success receipt, and the action's own result is readable anyway.
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert cas.lookup(action) is None
    assert not (tmp_path / "cas" / "actions").exists()
    digest = _result_blob_digest(str(error))
    assert digest is not None
    assert (tmp_path / "cas" / "blobs" / digest[:2] / digest).exists()
    # The refusal happened before the daemon, not after it.
    assert not (tmp_path / "called.json").exists()
    assert not (tmp_path / "inspected.json").exists()
    # The status sidecar a pool deadline reads carries the same coverage.
    sidecar = json.loads(status.read_text(encoding="utf-8"))
    assert sidecar["profile"]["produced"] is False
    assert sidecar["profile"]["workload_coverage"] == "unsupported"
    # The scratch directory, marker included, dies with the action.
    assert not (checkout / pb.PROFILE_SCRATCH_DIRNAME).exists()


def test_a_nonzero_action_keeps_its_own_code_and_the_negative_profile(
    tmp_path: Path, sample_backend: _SampleShapedBackend,
):
    """The uncovered profile must not overwrite the action's real status."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _sealed_action(checkout, tmp_path, _action_argv(
        f"{_shim_call('run', '--rm', 'workload:image', 'true')} || true; "
        "printf ok > result.txt; exit 7"
    ))
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            action, cas_root=tmp_path / "cas", checkout_root=checkout
        )
    error = raised.value
    assert error.returncode == 7
    assert error.signal is None
    assert "action argv exited with status 7" in str(error)
    assert error.profile["produced"] is False
    assert error.profile["workload_coverage"] == "unsupported"
    assert pb.PrismaBuildCAS(tmp_path / "cas").lookup(action) is None


def test_a_marker_that_disappears_makes_the_profile_unknown(
    tmp_path: Path, sample_backend: _SampleShapedBackend,
):
    """A missing armed marker cannot be read as "no container was used"."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    work = f"{sys.executable} -c {_WORK!r} 2>&1 | tee result.txt"
    action = _sealed_action(checkout, tmp_path, _action_argv(
        f'rm -f "$PRISMABUILD_PROFILE_SAMPLE_MARKER" || true; {work}; '
        "exit ${PIPESTATUS[0]}"
    ))
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            action, cas_root=tmp_path / "cas", checkout_root=checkout
        )
    error = raised.value
    assert error.profile["produced"] is False
    assert error.profile["workload_coverage"] == "unknown"
    assert error.profile["container_route"] == "unrecorded"
    assert "cannot be ruled out" in str(error.profile["reason"])
    assert pb.PrismaBuildCAS(tmp_path / "cas").lookup(action) is None


def test_a_killed_run_keeps_the_coverage_disclaimer(
    tmp_path: Path, sample_backend: _SampleShapedBackend,
):
    """The partial checkpoint shares ``ingest``, so it cannot claim coverage."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _sealed_action(checkout, tmp_path, _action_argv(
        f"{_shim_call('run', '--rm', 'workload:image', 'true')} || true; "
        "sleep 120"
    ))
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            action, cas_root=tmp_path / "cas", checkout_root=checkout,
            timeout_seconds=1.0,
        )
    profile = raised.value.profile
    assert profile["partial"] is True
    assert profile["produced"] is False
    assert profile["workload_coverage"] == "unsupported"
    assert profile["container_route"] == "docker run"
    assert "does not cover the action's workload" in str(raised.value)


# -- the native sampler on this box ------------------------------------------


def test_the_native_sampler_records_workload_frames(tmp_path: Path):
    """Native py-spy must sample a clean action's own workload, not just us.

    The fake-backend regressions above prove the coverage contract; this is
    the compatibility check the review asked for, run through PB: the real
    backend, the real py-spy, and a workload frame name that exists nowhere
    else in the process tree.  It also proves the armed marker does not turn a
    clean native action into a negative one.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _sealed_action(checkout, tmp_path, _action_argv(
        f"{sys.executable} native_workload.py 2>&1 | tee result.txt; "
        "exit ${PIPESTATUS[0]}"
    ), files={"native_workload.py": _NATIVE_WORKLOAD})
    result = pb.run_local_action(
        action, cas_root=tmp_path / "cas", checkout_root=checkout
    )
    profile = result["profile"]
    assert profile["mode"] == "sample"
    assert profile["produced"] is True
    assert "workload_coverage" not in profile
    assert profile["samples"] > 0
    blob = (tmp_path / "cas" / "blobs"
            / str(profile["blob_sha256"])[:2] / str(profile["blob_sha256"]))
    document = json.loads(blob.read_text())
    frames = [frame.get("name", "") for frame in document["shared"]["frames"]]
    matching = [name for name in frames if _NATIVE_WORKLOAD_MARKER in name]
    # The PB log carries this line, so the exact workload frames are readable
    # from the action's retained stdout as well as from this assertion.
    print(f"native sample evidence: samples={profile['samples']} "
          f"blob={profile['blob_sha256']} workload_frames={matching!r}")
    assert matching, frames


# -- the record a reader opens -----------------------------------------------


def test_a_reader_is_told_the_blob_does_not_cover_the_workload():
    described = pb.describe_profile({
        "mode": "sample", "backend": "py-spy", "blob_sha256": "ab" * 32,
        "bytes": 12, "blob_path": "/blobs/ab", "produced": False,
        "workload_coverage": "unsupported", "container_route": "docker run",
        "reason": "the action attempted a Docker container route (docker run)",
    })
    assert "NOT covering the workload" in described
    assert "docker run" in described
    assert "ab" * 6 in described
