"""Measure replay stages inside a PB-admitted action, using private driver copies."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import platform
import shutil
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests/compat/support")]
import record_compat as replay


STAGE_FIELDS = {"compile_source", "profile_hook", "profile_calls", "setup",
                "adapter_with_attestation_hooks", "final_attestation"}


@contextmanager
def profiling_drivers(work, roots):
    original_here, original_driver_for = replay.HERE, replay.driver_for
    selected = {root: original_driver_for(root) for root in roots}
    sources = dict.fromkeys([original_here, *selected.values()])
    copies = {}
    for index, source in enumerate(sources):
        driver = work / f"driver-{index}"
        shutil.copytree(source, driver, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        isolate = driver / "compat_isolate.py"
        isolate.write_text(instrument_driver(isolate.read_text()))
        copies[source] = driver

    def driver_for(root):
        return copies[selected[root]]

    try:
        replay.HERE = copies[original_here]
        replay.driver_for = driver_for
        yield
    finally:
        replay.HERE, replay.driver_for = original_here, original_driver_for


def profile_row(case, label, observed, seconds):
    stages = observed.get("profile_stages")
    error = (observed.get("attestation_error") or observed.get("entry_error")
             or observed.get("error"))
    if observed["returncode"] != 0:
        error = error or f"profiling replay exited with status {observed['returncode']}"
    elif not isinstance(stages, dict) or not STAGE_FIELDS <= stages.keys():
        error = error or "profiling stages unavailable or incomplete"
    elif not isinstance(stages["profile_calls"], int) or stages["profile_calls"] <= 0:
        error = error or "profiling hook observed no calls"
    return {"case": case, "generation": label, "seconds": seconds,
            "returncode": observed["returncode"], "error": error, "stages": stages}


def instrument_driver(source):
    replacements = (
        ("import argparse", "import argparse\nimport time"),
        ("    parser = argparse.ArgumentParser",
         "    timings = {'compile_source': 0.0, 'profile_hook': 0.0, 'profile_calls': 0}\n"
         "    started = time.perf_counter()\n    parser = argparse.ArgumentParser"),
        ("                    compiled = compile(source_raw, filename,",
         "                    compile_started = time.perf_counter()\n"
         "                    compiled = compile(source_raw, filename,"),
        ("                    source_codes[filename] = candidates",
         "                    source_codes[filename] = candidates\n"
         "                    timings['compile_source'] += time.perf_counter() - compile_started"),
        ("    try:\n        adapter = runpy.run_path",
         "    def timed_entered(frame, event, arg):\n"
         "        hook_started = time.perf_counter()\n"
         "        try:\n            return entered(frame, event, arg)\n"
         "        finally:\n"
         "            timings['profile_hook'] += time.perf_counter() - hook_started\n"
         "            timings['profile_calls'] += 1\n\n"
         "    timings['setup'] = time.perf_counter() - started\n"
         "    started = time.perf_counter()\n    try:\n        adapter = runpy.run_path"),
        ("        sys.setprofile(entered)", "        sys.setprofile(timed_entered)"),
        ('        output["executed_lines"] =',
         "        timings['adapter_with_attestation_hooks'] = time.perf_counter() - started\n"
         "        started = time.perf_counter()\n"
         '        output["executed_lines"] ='),
        ("    print(json.dumps(output, sort_keys=True))",
         "    timings['final_attestation'] = time.perf_counter() - started\n"
         "    output['profile_stages'] = timings\n    print(json.dumps(output, sort_keys=True))"),
    )
    for before, after in replacements:
        if source.count(before) != 1:
            raise ValueError(f"profiling instrumentation requires one exact boundary: {before!r}")
        source = source.replace(before, after)
    return source


def profile_replays(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="compat-profile-") as temporary:
        work = Path(temporary)
        rows = []
        started = time.perf_counter()
        with replay.retained() as generations:
            rows.append({"stage": "materialise_generations", "seconds": time.perf_counter() - started})
            sources = [("current", replay.ROOT, None), *generations]
            with profiling_drivers(work, [root for _, root, _ in sources]):
                for case in ("shape-table/unhashed", "config-fleet/source"):
                    for label, root, manifest in sources:
                        started = time.perf_counter()
                        observed = replay.invoke(root, case, work / "stores" / label / case, manifest=manifest)
                        rows.append(profile_row(case, label, observed, time.perf_counter() - started))
        args.output.mkdir(parents=True, exist_ok=True)
        path = args.output / ("profile-stages-" + platform.machine() + ".json")
        path.write_text(json.dumps(rows, indent=2) + "\n")
        print(json.dumps(rows))


if __name__ == "__main__":
    profile_replays()
