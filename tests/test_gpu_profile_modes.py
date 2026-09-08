"""``--profile nsys`` and ``--profile torch``: the two GPU modes of #372.

The tier's claim is narrow and worth stating: a GPU action can be asked for a
CUPTI-backed trace or a torch trace, on request only, and what comes back is
governed by the same rules as Tier 1's sampler -- the mode is sealed into the
key, a mode that produced no profile fails the action rather than publishing a
receipt that answers every later run with an unprofiled cache hit, and every
cost is bounded rather than absent.

The two modes differ in one structural way, and these tests are mostly about
it.  nsys is a process that wraps the action.  ``torch.profiler`` cannot be:
it lives inside the action's own interpreter, so the mode is a contract -- one
environment variable naming one path -- and PrismaBuild's job is to validate,
size and file what the action wrote there.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.core as pb                                       # noqa: E402

REPOSITORY = Path(__file__).resolve().parents[1]


def _trace(events: int = 3) -> bytes:
    return json.dumps({
        "schemaVersion": 1,
        "traceEvents": [
            {"ph": "X", "name": f"aten::mm{index}", "ts": index, "dur": 1}
            for index in range(events)
        ],
    }).encode("utf-8")


# -- the mode vocabulary ----------------------------------------------------


def test_a_window_is_part_of_the_action_identity(tmp_path: Path):
    """Two windows are two measurements, so they are two actions."""

    assert pb.parse_profile_mode("nsys") == "nsys"
    assert pb.parse_profile_mode("nsys:600") == "nsys:600"
    assert pb.split_profile_mode("nsys:600") == ("nsys", "600")
    assert pb.split_profile_mode("torch") == ("torch", None)


@pytest.mark.parametrize("text", [
    "nsys:abc", "nsys:0", "nsys:-1", "nsys:1000000", "sample:5", "torch:1",
    "nvprof", "ncu",
])
def test_a_mode_this_runtime_cannot_honour_is_refused_at_the_client(text: str):
    """A submission that only the worker would reject is a wasted round trip."""

    with pytest.raises(pb.ProfileBackendUnavailable):
        pb.parse_profile_mode(text)


def test_pbrun_lists_the_gpu_modes(tmp_path: Path):
    help_text = subprocess.run(
        [sys.executable, str(REPOSITORY / "tools" / "fleet" / "pbrun.py"), "--help"],
        capture_output=True, text=True, cwd=tmp_path,
    ).stdout
    assert "nsys" in help_text
    assert "torch" in help_text
    assert pb.TorchProfileBackend.OUT_ENV in help_text


# -- nsys -------------------------------------------------------------------


def test_the_nsys_launcher_names_the_report_and_traces_the_gpu(tmp_path: Path):
    backend = pb.NsysProfileBackend()
    backend._path = "/usr/local/bin/nsys"
    argv = backend.launch_argv(["/bin/true"], profile_path=tmp_path / "p.nsys-rep")
    assert argv[:2] == ["/usr/local/bin/nsys", "profile"]
    # ``-o`` names the report without the suffix nsys appends itself.
    assert str(tmp_path / "p") in argv
    assert "p.nsys-rep" not in " ".join(argv)
    assert argv[argv.index("--trace") + 1] == "cuda,nvtx"
    assert argv[argv.index("--sample") + 1] == "none"
    assert argv[-1] == "/bin/true"
    assert "--duration" not in argv, "no window unless one was asked for"


def test_a_window_must_not_kill_the_action_it_was_watching(tmp_path: Path):
    """nsys's default at the end of a window is to SIGTERM the application.

    A diagnostic that ends the action it was asked to watch is not a
    diagnostic, so the window pins ``--kill none`` and the worker waits for
    the action's own ending through the relay.
    """

    backend = pb.NsysProfileBackend().bind("600")
    backend._path = "/usr/local/bin/nsys"
    argv = backend.launch_argv(["/bin/true"], profile_path=tmp_path / "p.nsys-rep")
    assert argv[argv.index("--duration") + 1] == "600"
    assert argv[argv.index("--kill") + 1] == "none"
    assert backend.exits_before_action is True
    assert pb.NsysProfileBackend().exits_before_action is False


def test_binding_a_window_does_not_configure_the_next_action(tmp_path: Path):
    """The registry holds one backend per mode; an option must not stick."""

    registered = pb.PROFILE_BACKENDS["nsys"]
    bound = registered.bind("30")
    assert bound is not registered
    assert registered.duration_s is None


def test_a_report_over_the_fleet_budget_is_refused_with_the_remedy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Evidence that fills the disk is a cost the next action pays."""

    monkeypatch.setattr(pb, "PROFILE_BLOB_BUDGET_BYTES", 64)
    report = tmp_path / "p.nsys-rep"
    report.write_bytes(b"x" * 128)
    with pytest.raises(pb.ProfileUnusable) as raised:
        pb.NsysProfileBackend().read_profile(report)
    assert "--profile nsys:600" in str(raised.value)


def test_an_empty_or_missing_report_is_not_a_profile(tmp_path: Path):
    backend = pb.NsysProfileBackend()
    with pytest.raises(pb.ProfileUnusable):
        backend.read_profile(tmp_path / "absent.nsys-rep")
    (tmp_path / "empty.nsys-rep").write_bytes(b"")
    with pytest.raises(pb.ProfileUnusable):
        backend.read_profile(tmp_path / "empty.nsys-rep")


def test_a_windowed_report_says_it_is_a_window(tmp_path: Path):
    report = tmp_path / "p.nsys-rep"
    report.write_bytes(b"x" * 16)
    record = pb.NsysProfileBackend().bind("120").read_profile(report)
    assert record["window_s"] == 120
    assert record["partial_window"] is True
    assert pb.NsysProfileBackend().read_profile(report).get("window_s") is None


def test_nsys_refuses_an_action_whose_intermediate_would_land_in_tmp():
    """The report follows ``-o``; the ``.qdstrm`` follows ``TMPDIR`` alone."""

    backend = pb.NsysProfileBackend()
    backend.check_environment({"TMPDIR": "/home/rob/tmp", "PATH": "/usr/bin"})
    with pytest.raises(pb.LocalActionError) as raised:
        backend.check_environment({"PATH": "/usr/bin"})
    assert "TMPDIR" in str(raised.value)
    assert "--no-default-env" in str(raised.value)


# -- torch: a contract, not a wrapper ---------------------------------------


def test_the_torch_mode_wraps_nothing_and_names_a_path(tmp_path: Path):
    backend = pb.PROFILE_BACKENDS["torch"]
    out = tmp_path / "p.chrome-trace.json.gz"
    assert backend.launch_argv(["/bin/true", "x"], profile_path=out) == [
        "/bin/true", "x"
    ]
    assert backend.environment(profile_path=out) == {
        pb.TorchProfileBackend.OUT_ENV: str(out),
    }


@pytest.mark.parametrize("compress", [True, False])
def test_a_written_trace_is_read_back_and_counted(tmp_path: Path, compress: bool):
    path = tmp_path / "t.json.gz"
    path.write_bytes(gzip.compress(_trace(4)) if compress else _trace(4))
    record = pb.read_chrome_trace(path)
    assert record["events"] == 4
    assert record["compressed"] is compress


@pytest.mark.parametrize(("blob", "reason"), [
    (b"", "empty"),
    (b"not json at all", "not a JSON trace"),
    (json.dumps({"nope": 1}).encode(), "no traceEvents"),
    (b"\x1f\x8b\x08\x00truncated", "truncated gzip"),
])
def test_what_is_not_a_trace_is_not_accepted_as_one(
    tmp_path: Path, blob: bytes, reason: str
):
    path = tmp_path / "t.json.gz"
    path.write_bytes(blob)
    with pytest.raises(pb.ProfileUnusable) as raised:
        pb.read_chrome_trace(path)
    assert reason in str(raised.value)


def test_an_action_that_wrote_no_trace_fails_and_says_how_to_write_one(
    tmp_path: Path
):
    """Not ``produced: false`` with a receipt: that poisons the key.

    ``run_local_action`` answers a cache hit with no ``profile`` key at all, so
    a receipt filed for a run that produced no profile makes every later
    identical submission a profile-less hit with no reason attached.
    """

    with pytest.raises(pb.ProfileUnusable) as raised:
        pb.TorchProfileBackend().read_profile(tmp_path / "absent.json.gz")
    message = str(raised.value)
    assert pb.TorchProfileBackend.OUT_ENV in message
    assert "tools/profile_torch.py" in message
    assert "cache hit" in message


def test_the_torch_record_does_not_pretend_to_hash_a_profiler(tmp_path: Path):
    session = pb._ProfileSession(
        mode="torch",
        backend=pb.PROFILE_BACKENDS["torch"],
        directory=tmp_path / "scratch",
    )
    record = session.identity()
    assert record["backend"] == "torch.profiler"
    assert "in-process" in str(record["backend_path"])
    assert "backend_sha256" not in record


# -- the contract, end to end, with a shell standing in for torch -----------


def _closure_member(checkout: Path) -> None:
    (checkout / "task_code.py").write_text("# closure member\n", encoding="utf-8")


def _torch_action(checkout: Path, *, script: str, environment=None):
    _closure_member(checkout)
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/torch-profile",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": ["/bin/bash", "--noprofile", "--norc", "-c", script],
            "working_directory": ".",
            "result_path": "result.txt",
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": {"command": ["work"], "profile": "torch"},
        "environment": {
            "variables": environment or {"PATH": "/usr/bin:/bin"},
            "toolchain": {},
        },
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    }
    return pb.seal_action(body)


def test_the_action_is_told_where_to_write_and_its_trace_is_filed(
    tmp_path: Path
):
    """The whole torch mode, with ``python3 -c`` standing in for torch.

    The contract is a variable and a file, so a test does not need a GPU to
    prove the worker's half of it: writing the trace is the action's job, and
    what PrismaBuild does with it is what is under test here.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    result = pb.run_local_action(
        _torch_action(checkout, script=(
            "printf ok > result.txt; "
            f'/usr/bin/python3 -c "'
            "import gzip, json, os; "
            f"gzip.open(os.environ['{pb.TorchProfileBackend.OUT_ENV}'], 'wb')"
            ".write(json.dumps({'traceEvents': [{'ph': 'X'}, {'ph': 'X'}]})"
            '.encode())"'
        )),
        cas_root=tmp_path / "cas", checkout_root=checkout,
    )
    profile = result["profile"]
    assert profile["mode"] == "torch"
    assert profile["events"] == 2
    assert profile["compressed"] is True
    assert profile["produced"] is True
    blob = (tmp_path / "cas" / "blobs"
            / str(profile["blob_sha256"])[:2] / str(profile["blob_sha256"]))
    assert blob.exists()


def test_an_action_that_ignores_the_contract_fails_the_run(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            _torch_action(checkout, script="printf ok > result.txt"),
            cas_root=tmp_path / "cas", checkout_root=checkout,
        )
    assert pb.TorchProfileBackend.OUT_ENV in str(raised.value)
    # No receipt: the key stays answerable by a run that does profile.
    assert not (tmp_path / "cas" / "actions").exists()


def test_an_oversized_decoded_trace_cannot_publish_a_success_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(pb, "PROFILE_BLOB_BUDGET_BYTES", 1024)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _torch_action(checkout, script=(
        "printf ok > result.txt; "
        f'/usr/bin/python3 -c "'
        "import gzip, json, os; "
        f"gzip.open(os.environ['{pb.TorchProfileBackend.OUT_ENV}'], 'wb')"
        ".write(json.dumps({'traceEvents': [], 'padding': 'x' * 2048})"
        '.encode())"'
    ))
    with pytest.raises(pb.LocalActionError, match="decoded.*profile budget"):
        pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=checkout)
    assert (checkout / "result.txt").read_text() == "ok"
    assert pb.PrismaBuildCAS(tmp_path / "cas").lookup(action) is None


def test_a_mode_never_takes_over_a_variable_the_action_already_seals(
    tmp_path: Path
):
    """Silently replacing it would change what the action does under a flag."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _torch_action(
        checkout, script="printf ok > result.txt",
        environment={
            "PATH": "/usr/bin:/bin",
            pb.TorchProfileBackend.OUT_ENV: "/home/rob/tmp/mine.json.gz",
        },
    )
    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(
            action, cas_root=tmp_path / "cas", checkout_root=checkout
        )
    assert "already sets" in str(raised.value)


def _fake_nsys(tmp_path: Path, body: str, rc: int = 0) -> Path:
    """An ``nsys`` whose ``stats`` writes exactly ``body`` and exits ``rc``."""

    script = tmp_path / "fake-nsys"
    script.write_text(
        "#!/bin/sh\n"
        "for arg in \"$@\"; do\n"
        "  case \"$prev\" in --output) base=$arg;; esac\n"
        "  prev=$arg\n"
        "done\n"
        # %b so the escapes in the parametrized bodies become real lines.
        f"printf %b '{body}' > \"$base\"_cuda_gpu_kern_sum.csv\n"
        f"exit {rc}\n"
    )
    script.chmod(0o755)
    return script


@pytest.mark.parametrize(
    "body, rc, filed, reason",
    [
        ("Time,Calls,Name\\n50.0,10,gemm\\n", 0, True, None),
        ("", 0, False, "no rows"),
        ("Time,Calls,Name\\n", 0, False, "no rows"),
        ("Time,Calls,Name\\n50.0,10,gemm\\n", 3, False, "exited 3"),
    ],
)
def test_a_kernel_summary_with_nothing_in_it_is_not_filed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    body: str, rc: int, filed: bool, reason: str | None
):
    """A partial report can hold no completed kernel, and the CSV says so.

    Measured: the timeout arm of action ``2defaf9735ed`` filed a
    ``kernel_summary`` of 0 bytes, digest ``e3b0c442`` -- the hash of no bytes
    at all -- next to a real 114 kB report, which reads as a table that was
    produced and had nothing in it rather than a table that does not exist.
    """

    backend = pb.NsysProfileBackend()
    monkeypatch.setattr(
        backend, "locate", lambda: str(_fake_nsys(tmp_path, body, rc))
    )
    report = tmp_path / "p.nsys-rep"
    report.write_bytes(b"x" * 16)

    blobs = backend.extra_blobs(report)
    notes = backend.extra_blob_notes()
    if filed:
        assert [name for name, _ in blobs] == ["kernel_summary"]
        assert notes == {}
        return
    assert blobs == []
    assert reason in str(notes["kernel_summary_absent"])
    assert reason in pb.describe_profile({
        "mode": "nsys", "backend": "nsys", "blob_sha256": "a" * 64,
        "bytes": 16, **notes,
    })
