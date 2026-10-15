"""Pull one action off the shared queue and run it. Runs on any box.

Everything that reaches the queue is inside ``main``. Importing this file
must not serve the queue: an importer is a reader, and this module's one
statement used to claim a real action off the live pool and execute it on
whatever box did the importing. A constant sweep, a documentation build or
an editor's auto-import is enough to trigger that, and the action it takes
is somebody else's work, run under the wrong identity at the wrong time.

Admission is the same admission the loop performs, so it answers to the same
two stops.  A one-shot that ignored them was a hole in both: the host-local
maintenance gate is what ``maintenance_begin`` writes and what every loop
obeys, and ``containment`` is what puts a claim in front of the broker's
draining refusal.  Serving one action rather than many changes how long this
process lives, not whose permission it needs.  The stops are imported from
``prismabuild.role_session`` rather than restated, so a fleet has one definition of
"drained" and not two that can drift.
"""
import sys, socket, json
from contextlib import redirect_stdout
from pathlib import Path
SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
import worker_loop
from prismabuild import generation_receipt, role_session  # noqa: E402


def main() -> int:
    declined = ""
    code = 0
    if role_session.maintenance_requested():
        declined = "resource broker draining for maintenance; admission paused"
    else:
        decision = generation_receipt.compare(role_session.loaded_runtime(), role_session.published_runtime())
        if isinstance(decision, generation_receipt.Unknown):
            reasons = {
                name: value.reason if isinstance(value, generation_receipt.ReceiptUnavailable) else "known"
                for name, value in (("loaded", decision.loaded), ("published", decision.published))}
            declined = f"runtime receipt unknown; {json.dumps(reasons)}; admission paused"
            code = 75
        elif (isinstance(decision, generation_receipt.Drift)
              and decision.loaded.generation != decision.published.generation):
            declined = (f"runtime moved {decision.loaded.generation} -> {decision.published.generation}; "
                        "rerun from the live generation")
    if declined:
        print(json.dumps({
            "worker_host": socket.gethostname(),
            "declined": declined,
            "outcome": None,
        }, indent=1))
        return code
    outcome = None
    def observe_result(value):
        nonlocal outcome
        outcome = value
    # The same truthful topology, observed capacity, image/generation fences,
    # bounded discovery and admission as the loop. GPU capability and the
    # 120-second ceiling preserve this entrypoint's original declarations.
    # Diagnostics go to stderr; stdout remains one machine-readable object.
    # The result is delivered directly, never parsed from diagnostic text.
    with redirect_stdout(sys.stderr):
        code = worker_loop.main(["--once", "--gpu", "--timeout-s", "120"],
                                on_outcome=observe_result)
    print(json.dumps({
        "worker_host": socket.gethostname(),
        "outcome": None if outcome is None else {
            k: outcome[k] for k in ("status", "returncode", "stdout", "elapsed_s")
        },
    }, indent=1))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
