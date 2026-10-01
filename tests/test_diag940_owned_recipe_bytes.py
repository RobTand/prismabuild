"""Exact observer bytes against the archived pre-Core implementation (#1413).

Private filesystem/process/server facts only: both identity() and main() run
unchanged, coverage uses the real sibling analyzer, and CAS publication is real.
The .txt baseline is archival source, not an alternate production module.
"""
from __future__ import annotations

from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import shutil
import sys
import tarfile
import time
from types import ModuleType, SimpleNamespace
import urllib.parse

import pytest


REPO = Path(__file__).resolve().parents[1]
TOOL = REPO / "tools/maintenance/diag940_contended_observer.py"
BASELINE = REPO / "tests/fixtures/diag940_contended_observer_1408.txt"
BASELINE_SHA256 = "074564ba76e9e6f9a299f2c6174568bca3fc298f625ec1467cc35e4bfbab4f74"
PID = 123
TICKS = 987654
KEY = "a" * 64
HOST = "private-observer-\u00e9"
EPOCH = 1700000000
CHARTS = ("system.cpu", "system.load", "system.io", "nfsd.io", "nfsd.rpc", "nfsd.proc4ops")
PYSPY_BYTES = b"private py-spy\x00\xff\r\n" + "\u03bb".encode("utf-8")
POOL_BYTES = b"# private pool source\r\n\x00\xff" + "\u03bb".encode("utf-8")


def digest(raw):
    # Independent oracle, never the new Core byte owner.
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture
def variants(monkeypatch):
    raw = BASELINE.read_bytes()
    assert len(raw) == 8381
    assert digest(raw) == BASELINE_SHA256
    # Give the archived script its ORIGINAL tools/maintenance layout, so its
    # late src bootstrap and sibling analyzer resolve to this checkout.
    old = ModuleType("diag940_archived_bytes_test")
    old.__file__ = str(TOOL.with_name("diag940_contended_observer_1408.py"))
    exec(compile(raw, old.__file__, "exec"), old.__dict__)
    monkeypatch.setattr(sys, "path", sys.path.copy())
    spec = importlib.util.spec_from_file_location("diag940_candidate_bytes_test", TOOL)
    assert spec is not None and spec.loader is not None
    new = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(new)
    assert Path(new.core.__file__).resolve() == REPO / "src/prismabuild/core.py"
    assert sys.path[0] == str(REPO / "src")
    return old, new


def private_facts(root):
    generation = root / "published/runtime-generations/g\u00e9n\u03bb"
    script = generation / "tools/worker_loop.py"
    script.parent.mkdir(parents=True)
    script.write_bytes(b"# private published-looking worker\n")
    pool = generation / "src/prismabuild/pool.py"
    pool.parent.mkdir(parents=True)
    pool.write_bytes(POOL_BYTES)
    proc = root / "proc" / str(PID)
    proc.mkdir(parents=True)
    command = (b"python3\0" + str(script).encode("utf-8")
               + b"\0--label\0" + "\u00e9\u03bb".encode("utf-8") + b"\0\xff\0\0")
    (proc / "cmdline").write_bytes(command)
    (proc / "stat").write_text(
        f"{PID} (private worker (nested)) "
        + " ".join(["S"] + ["0"] * 18 + [str(TICKS)] + ["0"] * 4) + "\n",
        encoding="utf-8",
    )
    exe = root / "bin/private-python"
    exe.parent.mkdir()
    exe.write_bytes(b"private executable\x00\xff")
    (proc / "exe").symlink_to(exe)
    pyspy = root / "bin/py-spy"
    pyspy.write_bytes(PYSPY_BYTES)
    return SimpleNamespace(root=root, generation=generation, script=script,
                           pool=pool, proc=proc, exe=exe, command=command,
                           pyspy=pyspy, queue=root / "queue", cas=root / "cas")


def private_path(facts):
    def redirected(args, kwargs):
        # Redirect only the three literal external paths in this tool.
        path = Path(*args, **kwargs)
        redirects = {Path(f"/proc/{PID}"): facts.proc,
                     Path("/usr/local/bin/py-spy"): facts.pyspy,
                     Path("/mnt/shared/prismabuild-fleet/pb-queue"): facts.queue}
        return redirects.get(path, path)

    class PrivatePath(type(Path())):
        def __new__(cls, *args, **kwargs):
            return super().__new__(cls, redirected(args, kwargs))

        def __init__(self, *args, **kwargs):
            # pathlib moved path parsing from __new__ to __init__ in 3.12.
            if sys.version_info >= (3, 12):
                super().__init__(redirected(args, kwargs))
    return PrivatePath


def expected_identity(facts):
    return {"pid": PID, "start_ticks": TICKS,
            "command_sha256": digest(facts.command),
            "worker_script": str(facts.script), "generation_root": str(facts.generation),
            "pool_sha256": digest(POOL_BYTES), "exe": str(facts.exe)}


def test_identity_hashes_the_exact_binary_nul_command_and_source(variants, monkeypatch, tmp_path):
    facts = private_facts(tmp_path / "owned")
    results = []
    for module in variants:
        with monkeypatch.context() as patch:
            patch.setattr(module, "Path", private_path(facts))
            results.append(module.identity(PID))
    assert results == [expected_identity(facts)] * 2
    assert digest(facts.command) != digest(facts.command.replace(b"\0", b" "))
    assert digest(POOL_BYTES) != digest(POOL_BYTES.replace(b"\r\n", b"\n"))


@pytest.mark.parametrize("fault,exception,message", [
    ("no_script", ValueError, "observer target is not one published worker loop"),
    ("duplicate_script", ValueError, "observer target is not one published worker loop"),
    ("undecodable_script", UnicodeDecodeError, "utf-8"),
    ("missing_script", FileNotFoundError, "worker_loop.py"),
    ("missing_pool", FileNotFoundError, "pool.py"),
    ("bad_start_ticks", ValueError, "invalid literal"),
])
def test_identity_preserves_actual_read_resolve_and_decode_errors(
        variants, monkeypatch, tmp_path, fault, exception, message):
    facts = private_facts(tmp_path / "owned")
    if fault == "no_script":
        (facts.proc / "cmdline").write_bytes(b"python3\0not-a-worker\0")
    elif fault == "duplicate_script":
        (facts.proc / "cmdline").write_bytes(facts.command + str(facts.script).encode() + b"\0")
    elif fault == "undecodable_script":
        (facts.proc / "cmdline").write_bytes(b"/runtime-generations/\xff/tools/worker_loop.py\0")
    elif fault == "missing_script":
        facts.script.unlink()
    elif fault == "missing_pool":
        facts.pool.unlink()
    else:
        (facts.proc / "stat").write_text(
            f"{PID} (private worker) " + " ".join(["S"] + ["0"] * 18 + ["bad"]) + "\n")
    errors = []
    for module in variants:
        with monkeypatch.context() as patch:
            patch.setattr(module, "Path", private_path(facts))
            with pytest.raises(exception, match=message) as caught:
                module.identity(PID)
            errors.append(str(caught.value))
    assert errors[0] == errors[1]


def profile_bytes(facts):
    pool = str(facts.pool)
    profile = {
        "shared": {"frames": [
            {"name": "_claim_pass", "file": pool},
            {"name": "holder_bound", "file": pool},
            {"name": "_declared_run_bound", "file": pool},
            {"name": "holder_bound", "file": "/unrelated/pool.py"},
        ]},
        "profiles": [
            {"name": f'Process {PID} Thread {PID} "MainThread \u03bb"',
             "type": "sampled", "unit": "seconds",
             "samples": [[0], [0, 1, 2], [0, 1, 2, 1], [3]],
             "weights": [0.02, 0.02, 0.02, 0.02]},
            {"name": 'Process 456 Thread 456 "Child"', "type": "sampled",
             "unit": "seconds", "samples": [[0, 1, 2]], "weights": [0.02]},
        ],
    }
    return (json.dumps(profile, indent=1, ensure_ascii=False) + "\n").encode("utf-8")


def run_private_main(module, facts, monkeypatch, scenario, core):
    claimed_unix = {"nan": float("nan"), "positive-infinity": float("inf"),
                    "negative-infinity": -float("inf")}.get(scenario, 1.25)
    facts.queue.joinpath("claimed").mkdir(parents=True)
    claim = {"action_key": KEY, "claimed_host": HOST,
             "claimed_by": "private-holder-\u03bb", "claimed_unix": claimed_unix}
    facts.queue.joinpath("claimed", f"{KEY}.json").write_text(json.dumps(claim) + "\n")
    profile = profile_bytes(facts)
    netdata_raw = b'{ "labels": ["time", "' + "\u03bb".encode() + b'"], "data": [[1700000000, 1.25]] }\r\n'
    stdout_raw, stderr_raw = "profiled \u03bb\n", "private stderr \u00e9\n"
    commands, requests, cas_calls = [], [], []
    real_cas = core.PrismaBuildCAS
    real_gettarinfo = tarfile.TarFile.gettarinfo

    def version(argv, **kwargs):
        assert argv == [str(facts.pyspy), "--version"]
        assert kwargs == {"text": True}
        commands.append(tuple(argv))
        return "py-spy private \u00e9\n"

    def record(argv, *, stdout, stderr, check):
        assert argv == ["sudo", "-n", "--preserve-env=TMPDIR", str(facts.pyspy),
                        "record", "--pid", str(PID), "--idle", "--threads",
                        "--subprocesses", "--rate", "100", "--duration", "2",
                        "--format", "speedscope", "--output", "capture/worker.speedscope.json"]
        assert check is False
        assert Path(stdout.name) == Path("capture/py-spy.stdout")
        assert Path(stderr.name) == Path("capture/py-spy.stderr")
        commands.append(tuple(argv))
        Path(argv[-1]).write_bytes(profile)
        stdout.write(stdout_raw)
        stderr.write(stderr_raw)
        return SimpleNamespace(returncode=7 if scenario == "profiler-error" else 0)

    def urlopen(url, *, timeout):
        parsed = urllib.parse.urlsplit(url)
        assert (parsed.scheme, parsed.netloc, parsed.path) == ("http", "private.invalid", "/api/v1/data")
        query = urllib.parse.parse_qs(parsed.query)
        chart = query.pop("chart")[0]
        assert chart in CHARTS and timeout == 15
        assert query == {"after": [str(EPOCH)], "before": [str(EPOCH + 2)],
                         "points": ["2"], "format": ["json"], "group": ["average"]}
        requests.append(url)
        if scenario == "server-error" and chart == "nfsd.rpc":
            raise OSError("private server unavailable \u00e9")

        class Response(io.BytesIO):
            def read(self, size: int | None = -1):
                assert size == 4 * 1024 * 1024 + 1
                return super().read(size)
        return Response(netdata_raw)

    def private_cas(root):
        assert root == "/mnt/shared/prismabuild-fleet/cas"
        instance = real_cas(facts.cas)
        cas_calls.append((root, instance))
        return instance

    def stable_tarinfo(bundle, name=None, arcname=None, fileobj=None):
        assert name is not None
        assert Path(name).parent == Path("capture")
        info = real_gettarinfo(bundle, name=name, arcname=arcname, fileobj=fileobj)
        # Real file content/size/name/type, controlled private kernel metadata.
        info.uid = info.gid = 1000
        info.uname = info.gname = "private-owner"
        info.mode = 0o644
        info.mtime = EPOCH
        info.pax_headers = {}
        return info

    with monkeypatch.context() as patch:
        patch.chdir(facts.root)
        patch.setattr(module, "Path", private_path(facts))
        patch.setattr(module.socket, "gethostname", lambda: HOST)
        patch.setattr(module.os, "sched_getaffinity", lambda pid: {7, 3})
        times = iter((float(EPOCH), float(EPOCH + 2)))
        patch.setattr(module, "time", SimpleNamespace(time=lambda: next(times)))
        patch.setattr(time, "time", lambda: float(EPOCH))
        patch.setattr(tarfile.TarFile, "gettarinfo", stable_tarinfo)
        patch.setattr(module.subprocess, "check_output", version)
        patch.setattr(module.subprocess, "run", record)
        patch.setattr(module.urllib.request, "urlopen", urlopen)
        patch.setattr(core, "PrismaBuildCAS", private_cas)
        patch.setattr(sys, "path", sys.path.copy())
        patch.setattr(sys, "argv", [str(module.__file__), "--pid", str(PID),
                                   "--start-ticks", str(TICKS), "--generation-root", str(facts.generation),
                                   "--holder-key", KEY, "--duration", "2", "--netdata-url",
                                   "http://private.invalid/", "--out", "capture"])
        output = io.StringIO()
        with redirect_stdout(output):
            returncode = module.main()
        report_raw = output.getvalue().encode("utf-8")
        report = json.loads(report_raw)
        files = {p.name: p.read_bytes() for p in Path("capture").iterdir()}
        archive = Path("capture.tar.gz").read_bytes()
        assert len(cas_calls) == 1
        cas = cas_calls[0][1]
        artifact = report["artifact_blob"]
        assert artifact == {"id": "diag940.observer-artifacts", "sha256": digest(archive), "bytes": len(archive)}
        # The real CAS validates the input contract and rehashes the real blob.
        blob = cas.input_path(artifact)
        assert blob == cas.blob_path(digest(archive))
        assert blob.read_bytes() == archive
        assert blob.stat().st_mode & 0o222 == 0
        assert not list(facts.cas.joinpath(".staging").iterdir())
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
            assert bundle.getnames() == sorted(files)
            for member in bundle.getmembers():
                assert member.isfile()
                extracted = bundle.extractfile(member)
                assert extracted is not None
                with extracted:
                    assert extracted.read() == files[member.name]
        # No fake runpy or summary: assert meaningful results of the real analyzer.
        coverage = report["coverage"]
        assert coverage["sample_count"] == 4 and coverage["thread_count"] == 1
        assert coverage["functions"]["holder_bound"] == {"samples": 2, "inclusive_seconds": 0.04}
        assert coverage["functions"]["_declared_run_bound"] == {"samples": 2, "inclusive_seconds": 0.04}
        assert coverage["holder_read_path_sampled"] is True
        assert report["target"] == expected_identity(facts)
        assert report["pyspy_sha256"] == digest(PYSPY_BYTES)
        assert report["pyspy_version"] == "py-spy private \u00e9"
        assert report["observer_host"] == HOST
        assert report["observer_affinity"] == [3, 7]
        assert report["started_unix"] == float(EPOCH)
        assert report["ended_unix"] == float(EPOCH + 2)
        assert report["pyspy_returncode"] == (7 if scenario == "profiler-error" else 0)
        assert report["netdata_source"] == "http://private.invalid/"
        assert report["performance_delta"] is None
        for field in ("holder_before", "holder_after"):
            expected_holder = {"action_key": KEY, "present": True,
                               "claimed_host": HOST, "claimed_by": "private-holder-\u03bb",
                               "claimed_unix": claimed_unix}
            # Compare JSON spellings so NaN is not compared with NaN by ==.
            assert json.dumps(report[field], sort_keys=True) == json.dumps(expected_holder, sort_keys=True)
        assert files["worker.speedscope.json"] == profile
        assert files["py-spy.stdout"] == stdout_raw.encode("utf-8")
        assert files["py-spy.stderr"] == stderr_raw.encode("utf-8")
        for chart in CHARTS:
            if scenario == "server-error" and chart == "nfsd.rpc":
                assert f"{chart}.json" not in files
            else:
                assert files[f"{chart}.json"] == netdata_raw
        assert report["files"] == {name: {"bytes": len(raw), "sha256": digest(raw)}
                                   for name, raw in files.items()}
        assert report["netdata_errors"] == ({"nfsd.rpc": "private server unavailable \u00e9"}
                                            if scenario == "server-error" else {})
        assert returncode == (7 if scenario == "profiler-error" else int(scenario == "server-error"))
        assert len(commands) == 2 and len(requests) == 6
        # Stdout is the old default-ASCII/default-NaN sorted spelling plus LF,
        # NOT compact UTF-8 canonical JSON. This independently spells its bytes.
        assert report_raw == (json.dumps(report, sort_keys=True) + "\n").encode("utf-8")
        assert report_raw.endswith(b"\n") and not report_raw.endswith(b"\n\n")
        assert b"\\u00e9" in report_raw and b"\\u03bb" in report_raw
        compact = (json.dumps(report, sort_keys=True, separators=(",", ":"),
                              ensure_ascii=False) + "\n").encode("utf-8")
        assert report_raw != compact
        if scenario in ("nan", "positive-infinity", "negative-infinity"):
            with pytest.raises(ValueError, match="Out of range float"):
                json.dumps(report, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False, allow_nan=False)
        if scenario == "nan":
            assert math.isnan(report["holder_before"]["claimed_unix"])
            assert b'"claimed_unix": NaN' in report_raw
        elif "infinity" in scenario:
            assert math.isinf(report["holder_before"]["claimed_unix"])
            token = b"-Infinity" if scenario.startswith("negative") else b"Infinity"
            assert b'"claimed_unix": ' + token in report_raw
        # Capture precedes files/artifact and preserves insertion order, indent=2.
        capture_keys = ("schema", "target", "observer_host", "observer_affinity", "started_unix",
                        "ended_unix", "pyspy_returncode", "pyspy_version", "pyspy_sha256",
                        "holder_before", "holder_after", "netdata_source", "netdata_errors", "coverage",
                        "performance_delta", "limits")
        capture = json.loads(files["capture.json"])
        assert tuple(capture) == capture_keys
        assert files["capture.json"] == (json.dumps(capture, indent=2) + "\n").encode("utf-8")
        assert files["capture.json"] != (json.dumps(capture, sort_keys=True, indent=2) + "\n").encode("utf-8")
        assert "files" not in capture and "artifact_blob" not in capture
        assert json.dumps(capture, sort_keys=True) == json.dumps(
            {key: report[key] for key in capture_keys}, sort_keys=True)
        return {"returncode": returncode, "stdout": report_raw, "files": files,
                "archive": archive, "artifact": artifact, "commands": commands, "requests": requests}


@pytest.mark.parametrize("scenario", ["plain", "nan", "positive-infinity", "negative-infinity",
                                      "server-error", "profiler-error"])
def test_main_preserves_all_capture_report_archive_and_real_cas_bytes(
        variants, monkeypatch, tmp_path, scenario):
    core = variants[1].core
    owned = tmp_path / "owned"
    results = []
    for module in variants:
        # Identical absolute paths, filenames and content for both versions;
        # no distinct tmp roots hidden in identity, profile, tar or CAS records.
        facts = private_facts(owned)
        try:
            results.append(run_private_main(module, facts, monkeypatch, scenario, core))
        finally:
            shutil.rmtree(owned)
    assert results[0] == results[1]
    assert digest(results[0]["stdout"]) == digest(results[1]["stdout"])
    assert digest(results[0]["archive"]) == digest(results[1]["archive"])
