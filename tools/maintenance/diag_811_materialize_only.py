#!/usr/bin/env python3
"""Profile ordinary materialization of one retained request in an admitted action.

This reuses E1's ordinary arm only. It neither runs the retained payload nor
measures the live claim worker, launcher, or terminal publication.
"""
from __future__ import annotations

import argparse
import cProfile
import hashlib
import json
import os
from pathlib import Path
import pstats
import sys
import tempfile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--expect-runtime-manifest-sha256", required=True)
    parser.add_argument("--action-key", required=True)
    parser.add_argument("--expect-bundle-sha256", required=True)
    parser.add_argument("--profile-out", type=Path, required=True)
    parser.add_argument("--inspect-only", action="store_true",
                        help="verify the retained input without materializing or timing it")
    args = parser.parse_args()
    if not os.environ.get("PRISMABUILD_READER_HELPER_ROOT"):
        parser.error("run inside an admitted PB action")
    runtime = args.runtime_root.resolve(strict=True)
    if (runtime.parent != Path("/mnt/shared/prismabuild-fleet/runtime-generations")
            or not (runtime / "RUNTIME_VERSION.json").is_file()):
        parser.error("runtime must name an immutable published generation")
    manifest_path = runtime / "RUNTIME_VERSION.json"
    manifest_bytes = manifest_path.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != args.expect_runtime_manifest_sha256:
        raise RuntimeError("target runtime manifest changed")
    manifest = json.loads(manifest_bytes)
    sys.path.insert(0, str(runtime / "src"))
    from prismabuild import core, materialize
    import diag_811_e1_checkout_cache as diagnostic

    for module in (core, materialize):
        path = Path(module.__file__).resolve()
        if not path.is_relative_to(runtime):
            raise RuntimeError(f"unexpected source for {module.__name__}")
        if diagnostic.sha256_stream(path) != manifest["files"][path.relative_to(runtime).as_posix()]:
            raise RuntimeError(f"runtime member changed: {module.__name__}")
    cas = core.PrismaBuildCAS("/mnt/shared/prismabuild-fleet/cas")
    request = cas.read_action_request(args.action_key)
    if request is None:
        raise RuntimeError("retained request unavailable")
    request = core.validate_action(request)
    if request["action_key"] != args.action_key:
        raise RuntimeError("retained request key mismatch")
    snapshot = core.validate_pbrun_checkout_snapshot(request["params"]["checkout_snapshot"])
    if snapshot["input"]["sha256"] != args.expect_bundle_sha256:
        raise RuntimeError("retained bundle differs from approved input")
    bundle = cas.input_path(snapshot["input"])
    if diagnostic.sha256_stream(bundle) != args.expect_bundle_sha256:
        raise RuntimeError("retained bundle bytes differ from sealed digest")
    report = {
        "schema": "prismabuild.diag811.materialize_only.v1",
        "retained_action_key": args.action_key, "snapshot": snapshot,
        "target_runtime": str(runtime),
        "launching_runtime": os.environ["PRISMABUILD_READER_HELPER_ROOT"],
        "runtime_manifest_sha256": args.expect_runtime_manifest_sha256,
        "materializer_sha256": diagnostic.sha256_stream(Path(materialize.__file__)),
        "harness_sha256": diagnostic.sha256_stream(Path(__file__)),
        "ordinary_arm_sha256": diagnostic.sha256_stream(Path(diagnostic.__file__)),
    }
    if args.inspect_only:
        print(json.dumps({**report, "state": "inspected; no materialization or timing"}))
        return
    item = {"action_key": args.action_key, "cas_root": str(cas.root),
            "checkout_snapshot": snapshot, "request": request}
    parent = Path("/home/rob/tmp/prismabuild-checkouts")
    parent_stat = os.stat(parent)
    work = Path(tempfile.mkdtemp(prefix="pri198-811-profile-", dir=parent))
    checkout_root = work / "checkouts"
    checkout_root.mkdir()
    git = diagnostic.Git(materialize)
    profiler = cProfile.Profile()
    git.install()
    try:
        result = profiler.runcall(
            diagnostic.arm_a_rep, item=item, checkout_root=checkout_root,
            materialize=materialize, git=git, core=core, pair=0, order="A")
    finally:
        git.restore()
    if not result["preflight"]["ok"]:
        raise RuntimeError(f"materialized checkout preflight failed: {result['preflight']}")
    residue = sorted(path.name for path in checkout_root.iterdir())
    if residue:
        raise RuntimeError(f"private checkout cleanup incomplete: {residue}")
    checkout_root.rmdir()
    work.rmdir()
    args.profile_out.parent.mkdir(parents=True, exist_ok=True)
    profiler.dump_stats(str(args.profile_out))
    rows = [{"file": key[0], "line": key[1], "function": key[2],
             "primitive_calls": value[0], "calls": value[1],
             "self_s": value[2], "cumulative_s": value[3]}
            for key, value in pstats.Stats(profiler).stats.items()]
    print(json.dumps({**report,
        "parent_device": parent_stat.st_dev, "parent_inode": parent_stat.st_ino,
        "affinity": sorted(os.sched_getaffinity(0)), "result": result,
        "private_cleanup_complete": True,
        "profile_sha256": diagnostic.sha256_stream(args.profile_out),
        "profile_rows": rows,
        "boundary": "ordinary materialize + repository verification + private cleanup; cProfile enabled",
        "unmeasured": ["live claim worker", "resource launcher", "terminal publication"],
    }))


if __name__ == "__main__":
    main()
