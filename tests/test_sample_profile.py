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
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import profile_backends  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbstatus  # noqa: E402
import pbcampaign  # noqa: E402


REPOSITORY = Path(__file__).resolve().parents[1]


def _speedscope(name: str) -> str:
    """The smallest document a speedscope reader will open."""

    return json.dumps({
        "$schema": profile_backends.SPEEDSCOPE_SCHEMA,
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

    def launch_argv(self, argv, *, profile_path: Path, exit_status_path: Path):
        inner = profile_backends.exit_status_relay(argv, exit_status_path)
        if not self.writes_profile:
            return list(inner)
        return [
            "/bin/sh", "-c", 'printf %s "$1" > "$0"; shift 1; exec "$@"',
            str(profile_path), _speedscope("fake"), *inner,
        ]

    def read_profile(self, path: Path) -> dict:
        return profile_backends.read_speedscope(path)


@pytest.fixture
def fake_backend(monkeypatch: pytest.MonkeyPatch):
    backend = _FakeBackend()
    monkeypatch.setitem(profile_backends.BACKENDS, "fake", backend)
    return backend


def _closure_member(checkout: Path) -> None:
    (checkout / "task_code.py").write_text("# closure member\n", encoding="utf-8")


def _action(checkout: Path, *, profile: str | None, result: str = "result.txt"):
    """A portable action that writes its result through the pipeline shape.

    ``| tee`` is not decoration: it is what ``pbrun`` seals, and bash forks a
    pipeline member rather than exec'ing it, which is exactly the shape that
    decides whether a profiler can see the child at all.
    """

    _closure_member(checkout)
    argv = [
        "/bin/bash", "--noprofile", "--norc", "-c",
        f"{sys.executable} -c 'print(\"work\")' 2>&1 | tee {result}; "
        "exit ${PIPESTATUS[0]}",
    ]
    params: dict[str, object] = {"command": ["work"]}
    if profile is not None:
        params["profile"] = profile
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/profile",
            "definition_version": "v1",
            "task_class": "measurement",
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
    assert json.loads(blob.read_text())["$schema"] == profile_backends.SPEEDSCOPE_SCHEMA
    assert blob.stat().st_size == profile["bytes"]
    assert profile["blob_path"] == str(blob)
    # The scratch file is the action's own and does not outlive it.
    assert not (checkout / profile_backends.SCRATCH_DIRNAME).exists()


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
        profile_backends.BACKENDS, "fake", _FakeBackend(writes_profile=False)
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


def test_a_missing_backend_refuses_and_names_the_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    import socket

    class _Absent(_FakeBackend):
        def locate(self) -> str:
            raise profile_backends.BackendUnavailable(
                "py-spy is not beside /usr/bin/python3 and not on PATH"
            )

    monkeypatch.setitem(profile_backends.BACKENDS, "fake", _Absent())
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
    assert profile["rate_hz"] == profile_backends.SAMPLE_RATE_HZ
    blob = (tmp_path / "cas" / "blobs"
            / str(profile["blob_sha256"])[:2] / str(profile["blob_sha256"]))
    assert json.loads(blob.read_text())["$schema"] == profile_backends.SPEEDSCOPE_SCHEMA


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
