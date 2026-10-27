"""Measure replay stages inside a PB-admitted action, using private driver copies."""
from __future__ import annotations

import argparse
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


def profile_replays():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="compat-profile-") as temporary:
        work = Path(temporary)
        driver = work / "driver"
        shutil.copytree(replay.HERE, driver, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        isolate = driver / "compat_isolate.py"
        isolate.write_text(instrument_driver(isolate.read_text()))
        replay.HERE = driver
        rows = []
        started = time.perf_counter()
        with replay.retained() as generations:
            rows.append({"stage": "materialise_generations", "seconds": time.perf_counter() - started})
            for case in ("shape-table/unhashed", "config-fleet/source"):
                for label, root, manifest in [("current", replay.ROOT, None), *generations]:
                    started = time.perf_counter()
                    observed = replay.invoke(root, case, work / "stores" / label / case, manifest=manifest)
                    rows.append({"case": case, "generation": label, "seconds": time.perf_counter() - started,
                                 "returncode": observed["returncode"], "error": observed.get("attestation_error"),
                                 "stages": observed.get("profile_stages")})
        args.output.mkdir(parents=True, exist_ok=True)
        path = args.output / ("profile-stages-" + platform.machine() + ".json")
        path.write_text(json.dumps(rows, indent=2) + "\n")
        print(json.dumps(rows))


if __name__ == "__main__":
    profile_replays()
