"""Seal one action on this box and enqueue it for whichever box takes it.

The smoke test for a dispatcher, so it goes out on the dispatcher under test:
``--transport`` picks the pull queue or the SLURM lane, and both report where
the action actually went -- a ``ready/`` item or a submission record beside the
job id.  Printing a ``ready/`` path for work the lane carried would be a smoke
test reporting a queue that had nothing to do with it.
"""
import argparse
import os
import subprocess
import sys
import socket
import json
from pathlib import Path
SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb
import fleet_submit

result_path = "fleet_result.txt"

#: The one file the smoke action's code closure covers.
closure_member = "task_code.py"

#: Author and dates for the smoke checkout's first commit, fixed so the two
#: boxes commit the same bytes and the smoke action has one key on both.  The
#: same identity and dates ``pbrun`` stamps on a snapshot commit, and for the
#: same reason: a commit that varies with who ran the submit makes the action
#: key vary with it.
COMMIT_ENVIRONMENT = {
    "GIT_AUTHOR_NAME": "PrismaBuild",
    "GIT_AUTHOR_EMAIL": "prismabuild@example.invalid",
    "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+00:00",
    "GIT_COMMITTER_NAME": "PrismaBuild",
    "GIT_COMMITTER_EMAIL": "prismabuild@example.invalid",
    "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+00:00",
}


def _git(root: Path, argv: list[str]) -> subprocess.CompletedProcess:
    environment = dict(os.environ)
    environment.update(COMMIT_ENVIRONMENT)
    return subprocess.run(
        ["git", "-C", str(root), *argv],
        capture_output=True, text=True, env=environment,
    )


def ensure_snapshottable_checkout(checkout: Path) -> None:
    """Give the smoke checkout the Git history the SLURM lane needs.

    The lane addresses a checkout only through a sealed snapshot, and the
    sealer needs a repository with a commit to parent that snapshot on.  This
    tool wrote a plain directory, so ``--transport slurm`` refused every run
    with ``a non-Git checkout cannot be materialized`` and no scheduler
    command was ever reached: the command that exists to prove the dispatcher
    works could not use it.

    Only the closure member is committed.  ``git add -A`` would commit
    whatever else the shared checkout holds, which is a producer's staged
    encoder and a campaign's results, and it would buy nothing: the sealer
    snapshots the dirty tree either way.  A checkout that already has a
    commit is left exactly as it is.
    """

    if _git(checkout, ["rev-parse", "--verify", "-q", "HEAD"]).returncode == 0:
        return
    inside = _git(checkout, ["rev-parse", "--is-inside-work-tree"])
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        initialized = _git(checkout, ["init", "-q"])
        if initialized.returncode != 0:
            raise SystemExit(
                f"seal_and_publish: cannot make {checkout} a Git checkout, "
                "which the SLURM lane needs to seal it: "
                f"{(initialized.stderr or initialized.stdout).strip()}"
            )
    for argv in (
        ["add", "--", closure_member],
        ["commit", "-q", "-m", "PrismaBuild fleet smoke closure member",
         "--", closure_member],
    ):
        completed = _git(checkout, argv)
        if completed.returncode != 0:
            raise SystemExit(
                f"seal_and_publish: cannot commit {closure_member} in "
                f"{checkout}: "
                f"{(completed.stderr or completed.stdout).strip()}"
            )


def build_action(checkout: Path) -> dict:
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "fleet/first-dispatch",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "deterministic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            # Deterministic output: the bytes must not depend on which box ran
            # it, or the CAS receipt would differ per host and the action key
            # would be a lie.  The executing hostname goes to stdout, never
            # into the result.
            "argv": [
                "/usr/bin/python3", "-c",
                "import pathlib,socket,sys;"
                "sys.stdout.write('executed on '+socket.gethostname()+'\\n');"
                f"pathlib.Path({result_path!r}).write_text('prismabuild-fleet-v1\\n')",
            ],
            "working_directory": ".",
            "result_path": result_path,
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, [closure_member]),
        "params": {"smoke": True},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    }
    return pb.seal_action(body)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    fleet_submit.add_transport_argument(ap)
    args = ap.parse_args(argv)

    checkout = SH / "checkout"
    checkout.mkdir(parents=True, exist_ok=True)
    # The code closure member must exist and be identical on both boxes.
    (checkout / closure_member).write_text("# closure member\n", encoding="utf-8")
    # And the checkout itself has to be sealable, or the lane refuses it.
    ensure_snapshottable_checkout(checkout)

    action = build_action(checkout)
    key = str(action["action_key"])

    cas = pb.PrismaBuildCAS(SH / "cas")
    request = cas.publish_action_request(action)

    try:
        submission = fleet_submit.submit(
            action,
            cas=cas,
            request_path=request,
            transport=args.transport,
            # Named on both transports: the pull queue carries it on the queue
            # item, the lane seals it as the snapshot the node materializes.
            checkout_root=str(checkout),
            worker_script=str(RUNTIME_ROOT / "tools" / "prismabuild_worker.py"),
            tags=["gb10"],
        )
    except fleet_submit.SubmitRefused as refusal:
        # A refusal is a verdict, so report it as one.  The advertised output
        # of this command is a submission record, and a traceback is neither
        # that record nor a reason an operator can act on.
        print(f"seal_and_publish: {args.transport} refused this action: "
              f"{refusal}", file=sys.stderr)
        return 2
    print(json.dumps({
        "sealed_on": socket.gethostname(),
        # The submitted key: under SLURM the lane seals the checkout into
        # the action, so the key it went under is not the one built here.
        "action_key": submission.action_key,
        "sealed_action_key": key,
        "request": str(request),
        "transport": submission.transport,
        "submitted": submission.describe(),
        # Where the submission actually is: the queue item under the pool, the
        # sealed submission record under the lane.
        "where": str(submission.where),
    }, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
