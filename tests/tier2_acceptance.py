"""Accept #372's Tier 2 modes on a real GPU box, and price them.

The fleet executes the *published* runtime, so a mode this branch adds cannot
be reached through ``pbrun --profile`` until the coordinator publishes it.  This
driver is the way to exercise it honestly in the meantime: it runs inside an
ordinary admitted GPU action and calls this branch's ``run_local_action``
against a scratch CAS, so every code path under test -- the launcher, the
relay, the ingest, the refusals -- is the one that will run on the fleet, on
the box the fleet would run it on, against the box's own nsys and GPU.

It prints one JSON document: the functional arms, then a paired overhead
measurement of fixed-iteration GPU work, unprofiled against each mode,
interleaved and repeated.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import socket
import statistics
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[0] / "src"))
import prismabuild.core as pb                                       # noqa: E402

PYTHON = sys.executable
ITERATIONS = int(os.environ.get("TIER2_ITERATIONS", "32000"))
REPEATS = int(os.environ.get("TIER2_REPEATS", "5"))


def _checkout(root: Path, nonce: str) -> Path:
    checkout = root / f"checkout-{nonce}"
    checkout.mkdir(parents=True, exist_ok=True)
    (checkout / "task_code.py").write_text(f"# {nonce}\n", encoding="utf-8")
    shutil.copytree(HERE, checkout / "tests", dirs_exist_ok=True)
    shutil.copytree(HERE.parents[0] / "tools", checkout / "tools",
                    dirs_exist_ok=True)
    return checkout


def _action(checkout: Path, *, profile: str | None, nonce: str,
            iterations: int, helper: bool, environment=None):
    argv = [PYTHON, "tests/gpu_profile_probe.py", "out.json", str(iterations)]
    if helper:
        argv.append("--helper")
    params: dict[str, object] = {"command": ["tier2", nonce]}
    if profile is not None:
        params["profile"] = profile
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tier2/acceptance",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": argv,
            "working_directory": ".",
            "result_path": "out.json",
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": params,
        "environment": {
            "variables": environment if environment is not None else {
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "TMPDIR": os.environ.get("TMPDIR", "/home/rob/tmp"),
                "HOME": os.environ.get("HOME", "/home/rob"),
                "CUDA_VISIBLE_DEVICES": os.environ.get(
                    "CUDA_VISIBLE_DEVICES", "0"),
            },
            "toolchain": {},
        },
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    }
    return pb.seal_action(body)


def _run(root: Path, *, profile: str | None, nonce: str,
         iterations: int = ITERATIONS, helper: bool = True,
         environment=None, timeout_seconds: float | None = None
         ) -> dict[str, object]:
    checkout = _checkout(root, nonce)
    action = _action(checkout, profile=profile, nonce=nonce,
                     iterations=iterations, helper=helper,
                     environment=environment)
    started = time.perf_counter()
    record: dict[str, object] = {"profile_requested": profile, "nonce": nonce}
    try:
        result = pb.run_local_action(
            action, cas_root=root / "cas", checkout_root=checkout,
            timeout_seconds=timeout_seconds,
        )
    except pb.LocalActionError as exc:
        record["status"] = "refused"
        record["error"] = str(exc)[:900]
        record["returncode"] = exc.returncode
        record["signal"] = exc.signal
        record["error_profile"] = exc.profile
    else:
        record["status"] = result["status"]
        record["profile"] = result.get("profile")
        payload = Path(str(result["payload_path"]))
        if payload.is_file():
            record["action_result"] = json.loads(payload.read_text())
    record["wall_s"] = round(time.perf_counter() - started, 4)
    return record


def _availability() -> dict[str, object]:
    table: dict[str, object] = {}
    for mode, backend in sorted(pb.PROFILE_BACKENDS.items()):
        entry: dict[str, object] = {"name": getattr(backend, "name", mode)}
        try:
            entry["path"] = backend.locate()
            entry["version"] = backend.version
            entry["backed"] = True
        except pb.ProfileBackendUnavailable as exc:
            entry["backed"] = False
            entry["reason"] = str(exc)[:400]
        table[mode] = entry
    return table


def _paired(deltas: list[float]) -> dict[str, object]:
    #: Student's t at 95 % for small paired samples; the table is short on
    #: purpose, and an n outside it reports no interval rather than a wrong one.
    critical = {2: 12.706, 3: 4.303, 4: 3.182, 5: 2.776, 6: 2.571, 7: 2.447,
                8: 2.365, 9: 2.306, 10: 2.262}
    n = len(deltas)
    mean = statistics.fmean(deltas)
    if n < 2 or n - 1 not in critical:
        return {"n": n, "mean": mean, "interval": None}
    stdev = statistics.stdev(deltas)
    half = critical[n - 1] * stdev / (n ** 0.5)
    return {
        "n": n, "mean": mean, "stdev": stdev,
        "low": mean - half, "high": mean + half,
    }


def main() -> int:
    root = Path(os.environ.get("TIER2_ROOT", Path.cwd() / "tier2-work"))
    root.mkdir(parents=True, exist_ok=True)
    report: dict[str, object] = {
        "host": socket.gethostname(),
        "python": PYTHON,
        "iterations": ITERATIONS,
        "repeats": REPEATS,
        "availability": _availability(),
        "functional": {},
        "overhead": {},
    }

    functional = [
        ("unprofiled", dict(profile=None, nonce="f-plain")),
        ("nsys", dict(profile="nsys", nonce="f-nsys")),
        ("nsys_window", dict(profile="nsys:3", nonce="f-nsysw",
                             iterations=ITERATIONS * 4)),
        ("torch", dict(profile="torch", nonce="f-torch")),
        ("torch_no_helper", dict(profile="torch", nonce="f-torch-bad",
                                 helper=False)),
        ("sample", dict(profile="sample", nonce="f-sample")),
        ("nsys_without_tmpdir", dict(
            profile="nsys", nonce="f-nsys-notmp",
            environment={"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                         "HOME": os.environ.get("HOME", "/home/rob")})),
        ("nsys_timeout", dict(profile="nsys", nonce="f-nsys-to",
                              iterations=ITERATIONS * 8, timeout_seconds=8.0)),
        ("torch_timeout", dict(profile="torch", nonce="f-torch-to",
                               iterations=ITERATIONS * 8, timeout_seconds=8.0)),
        ("sample_timeout", dict(profile="sample", nonce="f-sample-to",
                                iterations=ITERATIONS * 8, timeout_seconds=8.0)),
        ("torch_env_collision", dict(
            profile="torch", nonce="f-torch-clash",
            environment={"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                         "TMPDIR": os.environ.get("TMPDIR", "/home/rob/tmp"),
                         "HOME": os.environ.get("HOME", "/home/rob"),
                         pb.TorchProfileBackend.OUT_ENV: "/home/rob/tmp/x.gz"})),
    ]
    for label, kwargs in functional:
        report["functional"][label] = _run(root, **kwargs)
        print(f"# {label}: {report['functional'][label]['status']}",
              file=sys.stderr, flush=True)

    # Interleaved arms, one after another inside this one admitted action, so
    # the box's state is as close to shared as it can be.  A nonce per repeat
    # keeps every run a cache miss; without it the second repeat would be a
    # receipt lookup and time nothing at all.
    arms = ["unprofiled", "nsys", "torch"]
    timings: dict[str, list[float]] = {arm: [] for arm in arms}
    inner: dict[str, list[float]] = {arm: [] for arm in arms}
    for repeat in range(REPEATS):
        for arm in arms:
            mode = None if arm == "unprofiled" else arm
            outcome = _run(root, profile=mode, nonce=f"o-{arm}-{repeat}")
            if outcome["status"] not in {"published", "canonical_result_reused"}:
                report["overhead"][f"{arm}_failure"] = outcome
                continue
            timings[arm].append(float(outcome["wall_s"]))
            body = outcome.get("action_result") or {}
            inner[arm].append(float(body.get("wall_s", "nan")))
        print(f"# overhead repeat {repeat} done", file=sys.stderr, flush=True)

    report["overhead"]["wall_s"] = timings
    report["overhead"]["action_inner_s"] = inner
    base = timings["unprofiled"]
    for arm in arms[1:]:
        paired = [a - b for a, b in zip(timings[arm], base)]
        ratio = [100.0 * (a - b) / b for a, b in zip(timings[arm], base)]
        report["overhead"][arm] = {
            "paired_delta_s": _paired(paired),
            "paired_percent": _paired(ratio),
            "deltas_s": paired,
        }
    Path(sys.argv[1]).write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
