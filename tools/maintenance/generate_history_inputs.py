"""Regenerate test history inputs from a full clone, or verify them with --check.

Run --capture-census through PrismaBuild to audit the census import/read closure.
It executes the test's unchanged observation program against both historical trees
and reports only source files opened, never the observed queue records.
"""
from __future__ import annotations

import argparse
import ast
import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from prismabuild.digest_primitives import raw_sha256

SOURCES = {
    "d2f92f5ff533489182dddaf286feb54afde6f34a": ["src/prismabuild/produced_output.py"],
    "161c9ae": ["tools/fleet/upgrade_client.py"],
    "fe416690b87aba99ad30c7df4f95ac8da47055d5": ["tools/fleet/pbcampaign.py"],
    "4815485c5060f3cecbf42655a37c7585e14c1791": [
        "src/prismabuild/core.py", "src/prismabuild/dependency_digest.py",
        "src/prismabuild/checkout_recovery.py", "tools/fleet/pbtest_pins.py",
        "tools/fleet/pbtest_outcomes.py", "tools/fleet/pbrecover_checkout.py",
        "tools/fleet/pbgang.py", "tools/fleet/qualify_rollout.py"],
    "3916c1f621": ["src/prismabuild/local_resident.py", "src/prismabuild/resident_sets.py"],
    "1c10a68": ["tools/fleet/resource_broker.py"],
    "a8ba9c0c7beeeb379908924a60c6933e997c8df6": ["src/prismabuild/submission.py"],
    "59719c7db39862490268caa055df9926a52a90c4": ["src/prismabuild/core.py"],
    "578e54d0788c0f3b138f5dcf217e980322bb7726": ["src/prismabuild/pool.py"],
}
SOURCE_TREES = {"43234d0fbd42d6ed76da38658cb9f447ed19a606": "src"}
CENSUS_REVISIONS = ("8e8572ee324b8508b9a7dd6a6dcb7d3548b663a7",
                    "082f75ea50b2601c8681fe070a5a342e1b042702")
CENSUS_PATHS = {
    "082f75ea50b2601c8681fe070a5a342e1b042702": [
        "src/prismabuild/__init__.py",
        "src/prismabuild/_bounded_reader.py",
        "src/prismabuild/_peer_placement.py",
        "src/prismabuild/action_client.py",
        "src/prismabuild/action_edges.py",
        "src/prismabuild/action_result.py",
        "src/prismabuild/action_wait.py",
        "src/prismabuild/adaptive_cpu.py",
        "src/prismabuild/adaptive_gpu.py",
        "src/prismabuild/adaptive_snapshot.py",
        "src/prismabuild/box_capacity.py",
        "src/prismabuild/box_window.py",
        "src/prismabuild/candidate_policy.py",
        "src/prismabuild/capability_eligibility.py",
        "src/prismabuild/census.py",
        "src/prismabuild/client_channel.py",
        "src/prismabuild/container_images.py",
        "src/prismabuild/contracts.py",
        "src/prismabuild/control_cpu.py",
        "src/prismabuild/core.py",
        "src/prismabuild/cpu_topology.py",
        "src/prismabuild/d38_gate.py",
        "src/prismabuild/data_manifest.py",
        "src/prismabuild/decomposition.py",
        "src/prismabuild/denial_records.py",
        "src/prismabuild/dependency_digest.py",
        "src/prismabuild/digest_primitives.py",
        "src/prismabuild/drain_policy.py",
        "src/prismabuild/endings.py",
        "src/prismabuild/filesystem_capacity.py",
        "src/prismabuild/filesystem_floor.py",
        "src/prismabuild/fleet_roster.py",
        "src/prismabuild/flock_owner.py",
        "src/prismabuild/gpu_capacity.py",
        "src/prismabuild/gpu_sample_facts.py",
        "src/prismabuild/holder_census.py",
        "src/prismabuild/immutable_files.py",
        "src/prismabuild/kept_reads.py",
        "src/prismabuild/local_dependencies.py",
        "src/prismabuild/local_scratch.py",
        "src/prismabuild/manifest_layout.py",
        "src/prismabuild/materialize.py",
        "src/prismabuild/movement_actions.py",
        "src/prismabuild/pool.py",
        "src/prismabuild/posix_lock.py",
        "src/prismabuild/produced_output.py",
        "src/prismabuild/progress.py",
        "src/prismabuild/publication_canary.py",
        "src/prismabuild/reader_lease.py",
        "src/prismabuild/record_kinds.py",
        "src/prismabuild/record_store.py",
        "src/prismabuild/residency_map.py",
        "src/prismabuild/residency_plan.py",
        "src/prismabuild/resource_scope.py",
        "src/prismabuild/scratch_lifetime.py",
        "src/prismabuild/slurm_lane.py",
        "src/prismabuild/storage_tiers.py",
        "src/prismabuild/submission.py",
        "src/prismabuild/submission_types.py",
        "src/prismabuild/target_evidence.py",
        "src/prismabuild/window_credit.py",
        "tools/fleet/fleet_submit.py",
        "tools/fleet/pbmcp.py",
        "tools/fleet/pbmetrics.py",
        "tools/fleet/pbstatus.py",
        "tools/fleet/runtime_paths.py",
        "tools/fleet/submission_frontend.py",
        "tools/fleet/upgrade_client.py"
    ],
    "8e8572ee324b8508b9a7dd6a6dcb7d3548b663a7": [
        "src/prismabuild/__init__.py",
        "src/prismabuild/_bounded_reader.py",
        "src/prismabuild/_peer_placement.py",
        "src/prismabuild/action_client.py",
        "src/prismabuild/action_edges.py",
        "src/prismabuild/action_result.py",
        "src/prismabuild/action_wait.py",
        "src/prismabuild/adaptive_cpu.py",
        "src/prismabuild/adaptive_gpu.py",
        "src/prismabuild/adaptive_snapshot.py",
        "src/prismabuild/box_capacity.py",
        "src/prismabuild/box_window.py",
        "src/prismabuild/candidate_policy.py",
        "src/prismabuild/capability_eligibility.py",
        "src/prismabuild/client_channel.py",
        "src/prismabuild/container_images.py",
        "src/prismabuild/contracts.py",
        "src/prismabuild/control_cpu.py",
        "src/prismabuild/core.py",
        "src/prismabuild/cpu_topology.py",
        "src/prismabuild/d38_gate.py",
        "src/prismabuild/data_manifest.py",
        "src/prismabuild/decomposition.py",
        "src/prismabuild/denial_records.py",
        "src/prismabuild/dependency_digest.py",
        "src/prismabuild/digest_primitives.py",
        "src/prismabuild/drain_policy.py",
        "src/prismabuild/endings.py",
        "src/prismabuild/filesystem_capacity.py",
        "src/prismabuild/filesystem_floor.py",
        "src/prismabuild/fleet_roster.py",
        "src/prismabuild/flock_owner.py",
        "src/prismabuild/gpu_capacity.py",
        "src/prismabuild/gpu_sample_facts.py",
        "src/prismabuild/holder_census.py",
        "src/prismabuild/immutable_files.py",
        "src/prismabuild/kept_reads.py",
        "src/prismabuild/local_dependencies.py",
        "src/prismabuild/local_scratch.py",
        "src/prismabuild/manifest_layout.py",
        "src/prismabuild/materialize.py",
        "src/prismabuild/movement_actions.py",
        "src/prismabuild/pool.py",
        "src/prismabuild/posix_lock.py",
        "src/prismabuild/produced_output.py",
        "src/prismabuild/progress.py",
        "src/prismabuild/publication_canary.py",
        "src/prismabuild/reader_lease.py",
        "src/prismabuild/record_kinds.py",
        "src/prismabuild/record_store.py",
        "src/prismabuild/residency_map.py",
        "src/prismabuild/residency_plan.py",
        "src/prismabuild/resource_scope.py",
        "src/prismabuild/scratch_lifetime.py",
        "src/prismabuild/slurm_lane.py",
        "src/prismabuild/storage_tiers.py",
        "src/prismabuild/submission.py",
        "src/prismabuild/submission_types.py",
        "src/prismabuild/target_evidence.py",
        "src/prismabuild/window_credit.py",
        "tools/fleet/fleet_submit.py",
        "tools/fleet/pbmcp.py",
        "tools/fleet/pbmetrics.py",
        "tools/fleet/pbstatus.py",
        "tools/fleet/runtime_paths.py",
        "tools/fleet/submission_frontend.py",
        "tools/fleet/upgrade_client.py"
    ]
}


def history_git(*arguments):
    return subprocess.check_output(["git", "-C", str(ROOT), *arguments])


def capture_census_inputs():
    sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "tools/fleet")]
    import pbmcp_fixture

    tree = ast.parse((ROOT / "tests/test_census_sections.py").read_bytes())
    observe = next(ast.literal_eval(node.value) for node in tree.body
                   if isinstance(node, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "_OBSERVE" for t in node.targets))
    prefix = r'''
import json, os, sys
from pathlib import Path
base = Path(sys.argv[1]).resolve()
log = sys.argv[4]
sys.dont_write_bytecode = True
def record_source(event, args):
    if event == "open" and isinstance(args[0], (str, bytes)):
        path = Path(os.fsdecode(args[0])).absolute()
        if path.is_relative_to(base) and path.is_file():
            fd = os.open(log, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            try:
                os.write(fd, (path.relative_to(base).as_posix() + "\n").encode())
            finally:
                os.close(fd)
sys.addaudithook(record_source)
'''
    with tempfile.TemporaryDirectory(prefix="history-census-") as temporary:
        scratch = Path(temporary)
        fleet = pbmcp_fixture.build_starved(scratch / "store")
        for revision in CENSUS_REVISIONS:
            source = scratch / revision
            source.mkdir()
            raw = history_git("archive", revision, "src", "tools/fleet")
            with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
                archive.extractall(source, filter="data")
            log = scratch / (revision + ".reads")
            run = subprocess.run([sys.executable, "-c", prefix + observe, str(source),
                                  str(fleet.queue_root), "2000000000", str(log)],
                                 env={**os.environ, "PYTHONPATH": ""},
                                 capture_output=True, text=True, timeout=120)
            if run.returncode:
                raise RuntimeError(run.stderr)
            files = sorted(set(log.read_text().splitlines()))
            assert files == CENSUS_PATHS[revision], f"census import/read closure changed: {revision}"
            print(json.dumps({"revision": revision, "files": files}), flush=True)


def generate_history_inputs(check):
    if history_git("rev-parse", "--is-shallow-repository").strip() != b"false":
        raise SystemExit("history inputs must be regenerated from a full clone")
    assert set(CENSUS_PATHS) == set(CENSUS_REVISIONS), "capture the census closure first"
    sources = SOURCES | CENSUS_PATHS
    for revision, directory in SOURCE_TREES.items():
        sources[revision] = history_git("ls-tree", "-r", "--name-only", revision,
                                       "--", directory).decode().splitlines()
    revisions, blobs = {}, {}
    for revision, paths in sorted(sources.items()):
        commit = history_git("rev-parse", "--verify", revision + "^{commit}").decode().strip()
        files = {}
        for path in sorted(paths):
            raw = history_git("show", commit + ":" + path)
            digest = raw_sha256(raw)
            assert digest not in blobs or blobs[digest] == raw
            blobs[digest] = raw
            files[path] = {"bytes": len(raw), "sha256": digest}
        revisions[revision] = {"commit": commit, "files": files}
        if revision in SOURCE_TREES:
            directory = SOURCE_TREES[revision]
            revisions[revision]["trees"] = {
                directory: history_git("rev-parse", commit + ":" + directory).decode().strip()}
    manifest = {"version": 1, "revisions": revisions}
    packed = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=packed, mtime=0, compresslevel=9) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
            for digest, raw in sorted(blobs.items()):
                member = tarfile.TarInfo(digest)
                member.size, member.mode, member.mtime = len(raw), 0o644, 0
                archive.addfile(member, io.BytesIO(raw))
    directory = ROOT / "tests/fixtures/history_inputs"
    outputs = {"manifest.json": (json.dumps(manifest, indent=2) + "\n").encode(),
               "blobs.tar.gz": packed.getvalue()}
    if not check:
        directory.mkdir(parents=True, exist_ok=True)
    for name, raw in outputs.items():
        if check:
            assert (directory / name).read_bytes() == raw, f"regenerate historical inputs: {name}"
        else:
            (directory / name).write_bytes(raw)
    print(json.dumps({"verified" if check else "generated": len(revisions),
                      "source_paths": sum(len(row["files"]) for row in revisions.values()),
                      "unique_blobs": len(blobs), "stored_bytes": sum(map(len, outputs.values()))}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--capture-census", action="store_true")
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.capture_census:
        capture_census_inputs()
    else:
        generate_history_inputs(args.check)
