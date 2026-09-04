"""Seal and enqueue the Tessera rate-band probe, one action per input shard.

What this buys, and why it is a separate stage from the export: the completion
axis is embedded, so ONE encode per Linear prices the whole band
``[rung/arity, cap/arity]`` bpp instead of one encode per rate point.  At rung 4
that is four rate points from one Viterbi -- the fan-out the DP needs to choose
a per-Linear allocation, at a quarter of the encodes.

It is a *generator*, not a shipping path.  Measured 2026-09-01 on a real GLM
expert: truncation costs 1.03x-1.28x, but choosing a low rung to widen the band
costs up to 2.15x at equal bpp.  So the band is priced by truncation and the
selected point is re-encoded natively -- surrogates generate, real measurement
selects, applied to the encoder itself.

``task_class`` is ``generation`` even though the receipt is a cost table: the
probe emits only deterministic encoder SSE, so its result is host-independent
and portable, and PrismaBuild reserves ``measurement`` for claims about a
machine (which it requires be keyed to one).
"""
import argparse
import shutil
import sys
from pathlib import Path

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, pool  # noqa: E402

CHECKOUT = SH / "checkout"
SOURCE = "/mnt/shared/models/GLM-5.3-Flash-BF16"
PYTHON = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python"
WRAPPER = "tessera_ladder_probe.py"
LOCAL_WRAPPER = Path("/home/rob/tessera/experiments/tessera_ladder_probe.py")


def closure_files():
    files = [WRAPPER]
    for path in sorted((CHECKOUT / "tessera").rglob("*.py")):
        files.append(str(path.relative_to(CHECKOUT)))
    return files


def build_action(shard, closure, rung, calibrate_every):
    result_path = f"results/glm53-tessera-ladder/rung{rung}/shard-{shard:05d}.json"
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tessera/glm53-ladder-probe",
            "definition_version": "v1",
            # NOT "measurement", though it is tempting.  PrismaBuild refuses
            # a portable measurement, and it is right to: a measurement is a
            # claim about a machine and must be keyed to one.  This probe emits
            # only deterministic encoder/decoder SSE -- no timing, no power, no
            # residency reaches the receipt -- so it is host-independent and
            # genuinely portable, which makes it a generation.  Keying it to a
            # platform to satisfy the enum would be a lie that also throws away
            # the free cross-box re-enqueue.
            "task_class": "generation",
            # The encoder is deterministic on these boxes (measured
            # byte-identical cross-box, 2026-09-01); a decode adds no entropy.
            "determinism": "deterministic",
            "artifact_family": "generic",
            "artifact_kind": "tessera-rate-band",
            "argv": [
                PYTHON, WRAPPER,
                "--shard", str(shard),
                "--source", SOURCE,
                "--rung", str(rung),
                "--calibrate-every", str(calibrate_every),
                "--result", result_path,
            ],
            "working_directory": ".",
            "result_path": result_path,
        },
        "inputs": [],
        "code_closure": closure,
        "params": {
            "source_model": SOURCE,
            "grid": "E2M1_K2",
            "rung": rung,
            "calibrate_every": calibrate_every,
            "shard": shard,
            "of_shards": 120,
        },
        "environment": {
            "variables": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "HOME": "/home/rob",
                "LANG": "C.UTF-8",
                "PYTHONPATH": str(CHECKOUT / "tessera" / "src"),
                "TMPDIR": "/home/rob/tmp",
                "TRITON_CACHE_DIR": "/home/rob/.triton-cache",
            },
            "toolchain": pb.executable_toolchain_contract(PYTHON),
        },
        "execution_scope": {
            "portability": "portable",
            "platform_key": None,
            "host_class": None,
        },
    }
    return pb.seal_action(body)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", required=True, help="e.g. 1 or 1-120")
    ap.add_argument("--rung", type=int, default=4)
    ap.add_argument("--calibrate-every", type=int, default=32)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    lo, _, hi = args.shards.partition("-")
    shards = range(int(lo), int(hi or lo) + 1)

    # Stage the wrapper INTO the shared checkout, so both boxes run one tree.
    # This is a NEW file, so it does not disturb any closure an in-flight
    # action already sealed -- unlike touching tessera/**/*.py, which every
    # running export shard has bound into its key and re-verifies at its CAS
    # commit point.  Staged even on a dry run, because the closure digest is
    # the thing being previewed and it cannot be computed without the file.
    shutil.copy2(LOCAL_WRAPPER, CHECKOUT / WRAPPER)

    closure = pb.build_code_closure(CHECKOUT, closure_files())
    cas = pb.PrismaBuildCAS(SH / "cas")
    queue = pool.PoolQueue(SH / "pb-queue")

    print(f"closure {closure['closure_sha256'][:16]} over "
          f"{len(closure['files'])} files   rung {args.rung}")
    published = 0
    for shard in shards:
        action = build_action(shard, closure, args.rung, args.calibrate_every)
        key = str(action["action_key"])
        if args.dry_run:
            print(f"  shard {shard:>3}  {key[:16]}  (dry run)")
            continue
        cas.publish_action_request(action)
        queue.publish(
            action_key=key,
            cas_root=str(SH / "cas"),
            checkout_root=str(CHECKOUT),
            worker_script=str(RUNTIME_ROOT / "tools" / "prismabuild_worker.py"),
            tags=["gb10"],
            needs_gpu=True,
            # A probe holds one weight plus its forests, and re-decodes in
            # place -- lighter than an export shard, which streams a whole
            # file.  Re-measure before trusting this if the rung changes: a
            # lower rung means a wider descendant table.
            resources={"gpu": 1, "mem_gb": 12},
        )
        published += 1
        print(f"  shard {shard:>3}  {key[:16]}  queued")
    print(f"published {published} action(s)")


if __name__ == "__main__":
    raise SystemExit(main())
