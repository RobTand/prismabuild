"""Seal and enqueue the GLM-5.3 Tessera export, one action per input shard.

This is PrismaBuild's first real quantization stage.  The properties that
matter, and where each comes from:

* **Shared source.**  Both boxes execute one tree -- the checkout on
  ``/mnt/shared`` -- not their own ``/home/rob/tessera``.  Those had silently
  diverged: sparklina's encoder was seven commits behind sparky's, and the two
  halves of the existing export were written by different code.  Only a plan
  that is uniformly q896 (the top rung, where the missing fix is provably a
  no-op, measured byte-identical on both boxes) kept that from producing two
  formats in one checkpoint.  The code closure now *binds* the encoder bytes
  into the action key, so the same drift would refuse instead of ship.

* **Re-enqueue is free.**  An action whose receipt is already in the CAS is a
  lookup, not a run.  That is what makes restart and resume trivial, and it is
  why every shard is enqueued rather than only the ones known to be missing.

* **One action per shard, each to its own directory.**  Shards share no state,
  so a per-shard action is the natural unit; a shared output directory would
  race on the exporter's report and aux copies.  ``merge_tessera_parts.py``
  takes ``nargs="+"``, so 120 self-consistent parts merge exactly as two would.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb  # noqa: E402
import fleet_submit  # noqa: E402

CHECKOUT = SH / "checkout"
SOURCE = "/mnt/shared/models/GLM-5.3-Flash-BF16"
PLAN = ("/mnt/shared/dq-runs/glm53-tessera-alloc-20260901/artifacts/"
        "glm53_tessera_plan.json")
PARTS = "/mnt/shared/models/GLM-5.3-Flash-Tessera-E2M1K2-20260901-parts"
PYTHON = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python"
WRAPPER = "tessera_export_shard.py"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 22), b""):
            h.update(block)
    return h.hexdigest()


#: How many shards the export is cut into. Bound into every action's
#: ``of_shards``, so a number outside 1..120 names no work at all.
#: Duplicated in the other Tessera dispatcher rather than shared, the way
#: ``closure_files`` already is: each dispatcher declares the shape of its own
#: run, and a ladder probe that is later cut differently must not silently
#: move the export's domain with it.
OF_SHARDS = 120


def shard_range(text):
    """``N`` or ``LO-HI``, inclusive, within ``1..OF_SHARDS``.

    A bare ``int()`` after ``parse_args`` raised a ``ValueError`` traceback at
    an operator who typed a range wrong, and accepted ``0``, ``500`` and
    ``9-4`` without complaint: the first two seal actions for shards that do
    not exist and the third seals nothing while reporting success. argparse
    prints an ``ArgumentTypeError`` as a usage error, so the domain is stated
    once and every wrong value gets it.
    """

    domain = f"a shard number or an inclusive LO-HI range within 1-{OF_SHARDS}"
    low, separator, high = text.partition("-")
    # ``1-`` is a half-typed range, not shard 1: taking the low end as the
    # high end would run one shard where the operator asked for many.
    if separator and not high:
        raise argparse.ArgumentTypeError(f"expected {domain}, got {text!r}")
    high = high or low
    if not (low.isdigit() and high.isdigit()):
        raise argparse.ArgumentTypeError(f"expected {domain}, got {text!r}")
    low, high = int(low), int(high)
    if not 1 <= low <= high <= OF_SHARDS:
        raise argparse.ArgumentTypeError(
            f"expected {domain}, got {text!r}: "
            f"{low}-{high} is empty or names a shard that does not exist")
    return range(low, high + 1)


def closure_files():
    """Every .py of the staged encoder, plus the wrapper that drives it."""
    files = [WRAPPER]
    for path in sorted((CHECKOUT / "tessera").rglob("*.py")):
        files.append(str(path.relative_to(CHECKOUT)))
    return files


def build_action(shard, closure, plan_sha):
    out_dir = f"{PARTS}/shard-{shard:05d}"
    result_path = f"results/glm53-tessera/shard-{shard:05d}.json"
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tessera/glm53-export-shard",
            "definition_version": "v1",
            "task_class": "generation",
            # Measured, not assumed: the same weight at q896 encodes to the
            # same bytes on both boxes (sha 6725e970..., 2026-09-01).
            "determinism": "deterministic",
            "artifact_family": "generic",
            "artifact_kind": "tessera-shard",
            "argv": [
                PYTHON, WRAPPER,
                "--shard", str(shard),
                "--source", SOURCE,
                "--plan", PLAN,
                "--out", out_dir,
                "--result", result_path,
            ],
            "working_directory": ".",
            "result_path": result_path,
        },
        "inputs": [],
        "code_closure": closure,
        # The plan is the allocation this artifact encodes.  Binding its digest
        # into the key means a re-allocated plan is a different action, not a
        # silent overwrite of one that looks the same.
        "params": {
            "source_model": SOURCE,
            "plan_sha256": plan_sha,
            "grid": "E2M1_K2",
            "rung_q256": 896,
            "shard": shard,
            "of_shards": OF_SHARDS,
        },
        "environment": {
            "variables": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "HOME": "/home/rob",
                "LANG": "C.UTF-8",
                # Relative to the tree this action runs in, not to the
                # submitter's copy of it.  An absolute path into the shared
                # checkout survives the SLURM lane's re-seal, so the worker
                # would verify the sealed encoder bytes in its private
                # checkout and then import whatever the shared tree held by
                # the time the job started -- and commit those bytes to the
                # CAS under the original sealed key.  A relative entry
                # resolves against the working directory, which is the
                # materialized snapshot under SLURM and the shared checkout
                # under the pull queue, so one tree still means one tree.
                "PYTHONPATH": "tessera/src",
                # Never /tmp -- an OOM cleared it once and took artifacts with
                # it.  Box-local, under $HOME.
                "TMPDIR": "/home/rob/tmp",
                # The default triton cache is root-owned on these boxes.
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
    ap.add_argument(
        "--shards", required=True, type=shard_range,
        help=f"which shards to seal: one number, or an inclusive LO-HI range, "
             f"within 1-{OF_SHARDS} (e.g. 61 or 1-{OF_SHARDS})")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the action key each shard would be sealed "
                         "under and enqueue nothing")
    fleet_submit.add_transport_argument(ap)
    args = ap.parse_args()

    shards = args.shards

    closure = pb.build_code_closure(CHECKOUT, closure_files())
    plan_sha = sha256_file(PLAN)
    cas = pb.PrismaBuildCAS(SH / "cas")

    print(f"closure {closure['closure_sha256'][:16]} over "
          f"{len(closure['files'])} files   plan {plan_sha[:16]}   "
          f"transport {args.transport}")
    published = 0
    for shard in shards:
        action = build_action(shard, closure, plan_sha)
        key = str(action["action_key"])
        if args.dry_run:
            print(f"  shard {shard:>3}  {key[:16]}  (dry run)")
            continue
        request = cas.publish_action_request(action)
        # 120 shards is the reason this one matters most: a direct publish
        # after the cutover queues the whole export where nothing drains it,
        # and each publish returns a path, so the run looks like it worked.
        submission = fleet_submit.submit(
            action,
            cas=cas,
            request_path=request,
            transport=args.transport,
            # Passed on both transports.  The pull queue carries it on the
            # queue item; the lane seals it as the snapshot the node
            # materializes, which is a different action key and the one
            # the submission reports.
            checkout_root=str(CHECKOUT),
            worker_script=str(RUNTIME_ROOT / "tools" / "prismabuild_worker.py"),
            tags=["gb10"],
            needs_gpu=True,
            # Measured: one exporter holds ~8 GB resident, and four concurrent
            # took a GB10 from 116 GB free to 55 GB.  16 GB is the honest cost
            # of one, so the ledger admits by what a shard actually takes.
            resources={"gpu": 1, "mem_gb": 16},
        )
        published += 1
        # The submitted key, not the sealed one: under SLURM the lane
        # seals the checkout into the action, and the key moves with it.
        print(f"  shard {shard:>3}  {submission.action_key[:16]}  "
              f"{submission.describe()}")
    print(f"published {published} action(s)")


if __name__ == "__main__":
    raise SystemExit(main())
