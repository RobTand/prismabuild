#!/usr/bin/env python3
"""Parse and display a typed PrismaBuild submission."""
from __future__ import annotations
import argparse
import contextlib
from dataclasses import fields
import math
import json
from collections.abc import Sequence
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
_PB_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_PB_ENTRYPOINT.parents[
    2 if _PB_ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
from prismabuild.runtime_paths import generation_root
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild.submission_frontend import render_submission, consumer_submission_display
from prismabuild import submission_timing
from prismabuild import action_edges, submission, sealed_submission, action_client, client_channel, core as pb, pool, fleet_roster
from prismabuild.submission_types import FleetStore, SubmissionRequest, SubmissionRefused, ArgumentRefused
from prismabuild.submission import TRANSPORTS, RESIDENCY_MODES, RESIDENCY_RAM_MODES, RESIDENCY_SHARE_MODES, DEFAULT_MAX_ATTEMPTS, CHECKOUT_SNAPSHOT_MAX_BYTES


def configured_store() -> FleetStore:
    return FleetStore(Path("/mnt/shared/prismabuild-fleet"), RUNTIME_ROOT, fleet_roster.Roster.beside(__file__))


def request_from_args(args: argparse.Namespace) -> SubmissionRequest:
    return SubmissionRequest(**{field.name: getattr(args, field.name) for field in fields(SubmissionRequest)})

def argument_parser() -> argparse.ArgumentParser:
    """The CLI owns usage and help; the package owns submission validation."""

    store = configured_store()
    ap = argparse.ArgumentParser(
        description="Submit one command to the PrismaBuild fleet and wait for "
                    "it. Either the pull queue or SLURM carries it, per "
                    "--transport or the published generation's default; the "
                    "result does not depend on which."
    )
    ap.add_argument("--client-events-fd", type=int, default=None, help=argparse.SUPPRESS)
    verbs = ap.add_mutually_exclusive_group()
    verbs.add_argument("--seal-only", action="store_true",
                       help="seal into the CAS, print prismabuild.sealed.v1, and publish no work")
    verbs.add_argument("--publish-sealed", metavar="KEY",
                       help="publish a verified CAS request without preparing the checkout again")
    ap.add_argument("--demand", default="",
                    help="resource demand, e.g. gpu=1,mem_gb=16")
    ap.add_argument("--gpu", action="store_true",
                    help="shorthand for gpu=1,mem_gb=16")
    ap.add_argument("--cpus", type=int, default=1,
                    help="cores this action will actually use; a parallel test "
                         "run wants its -n, not 1")
    ap.add_argument("--exclusive", action="store_true",
                    help="demand the whole GPU capacity of one box")
    ap.add_argument("--gpu-memory-gb", type=float, default=None,
                    help="GPU memory budget in GiB; separate VRAM on discrete GPUs, "
                         "a subset of --demand mem_gb on unified-memory GPUs")
    ap.add_argument("--gpu-capacity", type=int, default=0,
                    help="slots to demand for --exclusive; 0 reads the largest "
                         "a matching box actually offers")
    ap.add_argument("--tag", action="append", default=[],
                    help="require a box offering this tag (e.g. a hardware class)")
    ap.add_argument(
        "--requires-files", default=None, metavar="JSON",
        help="a JSON array of {path, sha256} entries this action's bytes "
             "depend on; the row requires the dependency-digest capability "
             "tag, the claim hashes the actual bytes before spending an "
             "attempt, and a missing or drifted digest is a named denial, "
             "never a run (#1495)")
    ap.add_argument(
        "--d38-receipt", default=None, metavar="KEY",
        help="the full 64-character action key of the CPU preflight that "
             "proves this GPU job (D38). Its CAS receipt must bind this job's "
             "identity, images and namespace")
    ap.add_argument(
        "--d38-exception", default=None, metavar="DECISION_ID",
        help="an explicit CEO decision that grants D38 for exactly this job; "
             "never together with --d38-receipt")
    ap.add_argument(
        "--d38-namespace", default=None, metavar="PATH",
        help="a JSON namespace descriptor (execution mode, cwd, interpreter, "
             "mounts, prerequisite identities). Its digest is sealed into the "
             "job, so a changed namespace is a changed job")
    ap.add_argument(
        "--container-image", action="append", default=[], metavar="REF",
        help="require the claiming box's local Docker to positively hold this "
             "image before the action is claimed (repeatable). Accepts "
             "sha256:<64 hex> for a local image ID, "
             "repository@sha256:<64 hex> for a manifest digest, or "
             "content:sha256:<64 hex> for a store-independent content "
             "reference; a mutable tag is refused because it cannot be part "
             "of the action's identity. An image ID is what the claiming "
             "box's own image store calls the image and the two Sparks run "
             "different stores, so prefer the content form for portable "
             "work and read one with "
             "'python3 -m prismabuild.container_images <repo:tag>' (#805). "
             "PrismaBuild does not pull, load or transfer images: the "
             "reference must already be local on the claiming box, a box that "
             "cannot show it leaves the item ready for a box that can, and "
             "dispatch refuses when no recorded eligible worker reports it. "
             "Sealed into the action key and requires the container-image-v1 "
             "worker capability")
    ap.add_argument("--measurement", action="store_true",
                    help="seal task_class=measurement (pool: verified platform, "
                         "local unless --host-class is explicit; "
                         "SLURM: --host-class required): the result is numerics "
                         "that do not transfer across architectures")
    ap.add_argument("--host-class", default=None, metavar="CLASS",
                    help="require CLASS placement (e.g. gb10). Pool measurements "
                         "opt into matching workers, with verified platform, ABI, "
                         "driver and device models; external dependencies must be "
                         "identical across the class. SLURM seals host_class_keyed "
                         "and attests its constraint through the controller")
    ap.add_argument("--target-evidence", default=None, metavar="PATH",
                    help="seal the class facts of a pool --measurement "
                         "--host-class submission from this packet instead of "
                         "probing this box (#1598). Run tools/fleet/pbevidence.py "
                         "as an action on a worker of the class to make one. "
                         "Each worker still checks the facts against its own "
                         "live facts before it runs")
    ap.add_argument("--anywhere", action="store_true",
                    help="assert that command/tool/data dependencies outside "
                         "the snapshot are identical on every eligible worker")
    ap.add_argument("--here", action="store_true",
                    help="pin the materialized checkout to this box")
    ap.add_argument("--resident-set", help="explicit resident set declaration for bounded lease renewal; Phase 1 serves canonical paths")
    ap.add_argument(
        "--data-manifest",
        help="path to a plain JSON or gzip data manifest (64 MiB stored; "
             "gzip expands to at most 512 MiB) naming the shared-mount bytes this "
             "action will read; attached as a second content-addressed input "
             "so a storage-role loop can make them resident before the action "
             "is claimed. It is part of the action key: the same command with "
             "a manifest is a different action from the same command without "
             "one, and from the same command with a different manifest",
    )
    ap.add_argument(
        "--after", action="append", default=[], metavar="PRODUCER:TEMPLATE_ID",
        help="defer this submission until PRODUCER succeeds (#913).  PRODUCER "
             "is an action key, or the pending id another --after submission "
             "printed; TEMPLATE_ID is the write-only produced-output template "
             "it declares.  PB then builds this action's data manifest from "
             "--data-manifest (the static part, optional) followed by every "
             "origin-only batch the producer's successful attempt committed "
             "under the template, seals the action and publishes it.  The "
             f"command may carry {action_edges.DATA_MANIFEST_PLACEHOLDER} and "
             f"{action_edges.DATA_MANIFEST_SHA256_PLACEHOLDER}, each once, as a "
             "whole argument; they become that manifest's path and the SHA-256 "
             "the sealed request binds for it.  "
             "Repeatable; pool transport only",
    )
    ap.add_argument(
        "--supersedes", default=None, metavar="KEY_OR_PENDING_ID",
        help="state that this submission replaces a failed or withdrawn key, "
             "or an unreleased deferred submission, so that --after edges "
             "naming the old one follow this one (#913).  Every publish moves "
             "every key; this is how a resubmission keeps its dependents",
    )
    ap.add_argument(
        "--produced-output-template", default=None, metavar="PATH",
        help="path to a tiny validated produced-output template JSON "
             "(at most 64 KiB) declaring the bounded working window this "
             "action will stage for bytes it produces itself. Captured as an "
             "ordinary CAS declared input plus action params, so changing the "
             "template changes the action key and editing the file after seal "
             "changes nothing the worker reads. The qualified tier demand is "
             "derived from the window (never the durable corpus) and added "
             "to the explicit CPU/memory/GPU reservation; the claim holds "
             "all of it before the producer starts. Pool transport only.",
    )
    ap.add_argument(
        "--residency", choices=RESIDENCY_MODES, default="none",
        help="stage this action's declared bytes onto a storage tier before it "
             "runs (#583).  'stage' seals one movement node per phase of the "
             "data manifest's read order and one egress node each, and admits "
             "the action only once its first phase's bytes are on the tier and "
             "still pinned there; the tiers loop publishes the window -- the "
             "first phase included -- taking over a range already resident "
             "rather than copying it, as accepted progress advances.  Needs --data-manifest, because a byte range "
             "is meaningless without the list it indexes.  'none', the "
             "default, publishes exactly what it published before",
    )
    ap.add_argument(
        "--residency-tier", default=None,
        help="which announced stage tier to stage onto; only needed when the "
             "fleet announces more than one",
    )
    ap.add_argument(
        "--residency-ram", choices=RESIDENCY_RAM_MODES, default="auto",
        help="seal a ram leg onto the residency plan: one promotion node and "
             "one egress node per phase, copying each landed stage range into "
             "the ram tier the storage box announces (#640).  'auto' seals the "
             "leg when a ram tier is live on the stage's own host, which is "
             "the tier turning on with the mount; 'off' is the A/B's other "
             "arm.  A plan already frozen keeps the leg it was frozen with",
    )
    ap.add_argument(
        "--residency-share", choices=RESIDENCY_SHARE_MODES, default="auto",
        help="stage each range once for every consumer that reads it (#1026): "
             "the first submission of a range seals and registers its mover "
             "under the range's identity, and every later submission of the "
             "same manifest range on the same tier names that mover, so the "
             "tier holds one copy charged once.  'off' seals a mover per "
             "consumer, as before, and is the A/B's other arm.  A plan "
             "already frozen keeps the movers it was frozen with",
    )
    ap.add_argument(
        "--residency-mover-mem-gb", type=int, default=1,
        help="memory one movement node reserves when no mover receipt has "
             "measured one yet.  The copy is a bounded read-ahead window, so "
             "this does not grow with the range; once receipts exist the row "
             "takes the maximum peak_rss_bytes they report instead",
    )
    ap.add_argument(
        "--residency-mover-readers", type=int, default=4,
        help="paced read buffers one movement node copies through.  It is "
             "passed to the mover and declared as its cpu demand until a "
             "receipt's cpu_seconds measures one, so the two cannot disagree",
    )
    ap.add_argument(
        "--residency-prefetch-depth-gib", type=int, default=None,
        help="how many GiB this action holds ahead of the phase it is reading "
             "(#909).  Carried on the residency plan, so the tier prices the "
             "window's read-ahead at it rather than at this action's memory "
             "reservation (mem_gb plus its GPU budget), which is the bound "
             "used when nothing is declared.  A fact about the reader, "
             "supplied by whoever knows its prefetch; 0 is a reader that "
             "holds nothing ahead",
    )
    ap.add_argument(
        "--residency-read-mb-s", type=int, default=None,
        help="how fast this action reads its staged bytes, in MB/s (#909).  "
             "Carried on the residency plan: the tier prices the window's "
             "consumption at it until the action reports progress, and its "
             "movers reserve it as their pool fill.  Undeclared, the window "
             "is priced at its run-ahead bound until it reports",
    )
    ap.add_argument(
        "--residency-mover-max-attempts", type=int, default=3,
        help="attempts one movement node gets, independent of --max-attempts. "
             "A mover copies to a temporary, verifies the digest and renames, "
             "so a retry is safe and a transient read error should not cost "
             "the whole staged range",
    )
    ap.add_argument(
        "--checkout-snapshot-max-bytes",
        type=int,
        default=CHECKOUT_SNAPSHOT_MAX_BYTES,
        help="lower the hard fleet ceiling for both the logical materialized "
             "Git tree and compressed bundle (cannot raise it)",
    )
    ap.add_argument(
        "--snapshot-ref",
        action="append",
        default=[],
        metavar="NAME",
        help="also advertise this source branch in the snapshot bundle, so "
             "the action can spell it (e.g. --snapshot-ref master for a "
             "master...HEAD gate); repeatable",
    )
    ap.add_argument("--cwd", default=os.getcwd(),
                    help="directory the command runs in, on this box and "
                         "inside the checkout being sealed; it is recorded "
                         "in the action as a path relative to the checkout "
                         "root, so the action stays portable")
    ap.add_argument(
        "--as-sealed-by", metavar="ACTION_KEY",
        help="reuse the retained runtime wrapper of this full action key and "
             "require the complete current seal to match before submission; "
             "collect pre-publication receipts without creating a new key",
    )
    ap.add_argument("--deterministic", action="store_true",
                    help="declare byte-identical output; enables CAS reuse")
    ap.add_argument("--gang-group", default=None,
                    help="32-hex gang group this action is a member of (#1517); use "
                         "pbgang.py, which seals every member and files the group")
    ap.add_argument("--gang-size", type=int, default=None,
                    help="members in the gang (with --gang-group)")
    ap.add_argument("--gang-index", type=int, default=None,
                    help="this member's index, 0..size-1 (with --gang-group)")
    ap.add_argument(
        "--retry-safe",
        action="store_true",
        help=("declare the whole command idempotent across failed attempts, "
              "including every external side effect"),
    )
    ap.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help=("bounded attempts for a --retry-safe action; arbitrary commands "
              "default to one"),
    )
    ap.add_argument("--timeout-s", type=float, default=None,
                    help="positive execution deadline in seconds, enforced by "
                         "both transports; the pool worker's safety ceiling "
                         "(7200 s by default, announced per box and reported "
                         "here when it would cut this request short) also "
                         "applies. Queue waiting is bounded by --wait-s")
    ap.add_argument("--progress-phase", "--progress", action="append", default=None,
                    metavar="NAME=SECONDS",
                    help="declare one phase of this action and the quiet it is "
                         "allowed in it, in order, once per phase. Declaring "
                         "any of them admits the action under the progress "
                         "contract: it is then bounded by how long it goes "
                         "without committing work rather than by how long it "
                         "runs, the worker's ceiling clamps each phase's "
                         "allowance instead of the whole run, and --timeout-s "
                         "if given still ends it whatever it is doing. The "
                         "action reports with prismabuild.progress.commit, or "
                         "by running $PRISMABUILD_ACTION_PROGRESS_HELPER when "
                         "it cannot import PrismaBuild")
    ap.add_argument("--progress-cycle", action="store_true",
                    help="allow progress phases to repeat; each phase gets one "
                         "allowance between increases in cumulative committed "
                         "units. Requires --progress-phase and cyclic-capable workers")
    ap.add_argument("--awaited-batch", default=None, metavar="PARENT:PLAN:STATE",
                    help="declare the decomposed batch this coordinator awaits, "
                         "as parent and plan keys and the controller-state "
                         "directory, separated by colons. Valid "
                         "only with --progress-phase on pool transport: the "
                         "worker's stall watch credits quiet while a verified "
                         "awaited child is ready or claimed (#1666)")
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="give up waiting for a worker to pick this up")
    ap.add_argument(
        "--filesystem-growth-gib", type=int, default=1, metavar="GIB",
        help="the GiB this submission may write to the shared store (snapshot, "
             "CAS objects, queue record), reserved from its filesystem_gib "
             "ledger while it is written when the used-filesystem floor is "
             "observed or enforced (#1483); ignored while the floor is off")
    ap.add_argument(
        "--detach", action="store_true",
        help="seal and submit exactly as usual, print one JSON line naming the "
             "action key, the transport, the job id or queue record and the "
             "terminal-record paths, and exit 0 without waiting; an action "
             "already in the CAS prints status=cache_hit and submits nothing, "
             "and one already running prints status=attached and joins that "
             "run rather than starting a second copy of it. "
             "Wait for it later with pbwait.py. Incompatible with "
             "--max-attempts greater than 1: a retry needs somebody alive to "
             "see the attempt fail",
    )
    ap.add_argument("--priority", type=int, default=0,
                    help="a queue hint, higher runs sooner; not part of the "
                         "action's identity, so two submissions that differ "
                         "only in priority are the same action. The pull "
                         "queue sorted its ready list on it, before age; the "
                         "SLURM lane spends it as a --nice "
                         "(slurm_lane.nice_for), scaled so one priority step "
                         "outranks submission order rather than one later "
                         "submission")
    ap.add_argument("--priority-reason", type=pool.normalize_priority_reason,
                    default=None, help="optional explanation of the priority, "
                    "stored with the submission and shown by pbstatus; not "
                    "part of the action identity or an admission requirement. "
                    "Use single-line printable text, at most 1024 characters")
    ap.add_argument("--profile", default=None,
                    metavar="{" + ",".join(pb.PROFILE_MODES) + "}",
                    help="run a profiler around this action's child and store "
                         "the profile as a CAS blob named on the ending. "
                         "Unlike a queue hint this IS part of the action's "
                         "identity: a profiled run has its own key, so it is "
                         "never answered from an unprofiled receipt and never "
                         "an A/B arm against one. 'sample' is py-spy at "
                         f"{pb.PROFILE_SAMPLE_RATE_HZ} Hz over the whole "
                         "process tree, or 'sample:HZ' seals a positive "
                         "whole rate in samples per second ('sample:10' "
                         "samples ten times a second); 'nsys' is Nsight "
                         "Systems over CUDA and NVTX, and takes a window in "
                         "seconds ('nsys:600' "
                         "traces the first ten minutes and lets the action run "
                         "on); 'torch' is a contract the action opts into, "
                         "exporting its own Chrome trace to the path in "
                         f"{pb.TorchProfileBackend.OUT_ENV}. Measured overhead "
                         "per mode is in docs/operating_prismabuild.md, with "
                         "the box load it was measured under")
    ap.add_argument("--env", action="append", default=[],
                    help="K=V added to the action's environment (repeatable)")
    ap.add_argument("--no-default-env", action="store_true",
                    help="declare only --env, without the fleet defaults")
    ap.add_argument("--json", action="store_true",
                    help="emit prismabuild.withdrawal.v1 for one --withdraw")
    ap.add_argument("--withdraw", action="append", default=[], metavar="KEY",
                    help="cancel this queued or running action (a key prefix is "
                         "enough) instead of submitting; repeatable")
    ap.add_argument("--reason", default="",
                    help="why, recorded on the withdrawal record, or on the "
                         "release --release-origin-consumer files")
    ap.add_argument(
        "--release-origin-consumer", nargs=2, default=None,
        metavar=("BATCH_REF", "CONSUMER_KEY"),
        help="release one consumer's declaration on one consumed origin batch "
             "instead of submitting (#926).  BATCH_REF is the batch's ref, as "
             "JSON or a path to a JSON file (output-origin-retirement-stalled "
             "prints it); CONSUMER_KEY is the declared consumer, which must "
             "have failed or been withdrawn, or be unpublished with nothing "
             "left that can publish it (#945).  The tier loop then retires the "
             "batch once every other declared consumer has succeeded")
    ap.add_argument(
        "--transport", choices=TRANSPORTS,
        default=submission.transport_default(store=store),
        help="which dispatcher carries this submission (env "
             "PRISMABUILD_TRANSPORT, else the published runtime generation's "
             "default_transport); the pull queue stays the default until the "
             "fleet has cut over to SLURM")
    ap.add_argument("command", nargs=argparse.REMAINDER,
                    help="the command to run, after a bare --; every word "
                         "past it belongs to the command and not to pbrun")
    return ap


def parse_args(argv: Sequence[str] | None = None, *, parser=None) -> argparse.Namespace:
    ap = parser if parser is not None else argument_parser()
    args = ap.parse_args(argv)
    if args.json and (len(args.withdraw) != 1 or args.release_origin_consumer is not None):
        ap.error("--json requires exactly one --withdraw KEY")

    if args.publish_sealed is not None:
        if (len(args.publish_sealed) != 64
                or any(c not in "0123456789abcdef" for c in args.publish_sealed)):
            ap.error("--publish-sealed requires a full lowercase 64-hex action key")
        supplied = list(sys.argv[1:] if argv is None else argv)
        allowed = {"--publish-sealed", "--detach", "--wait-s", "--client-events-fd"}
        if args.command or any(word.startswith("--") and word.split("=", 1)[0] not in allowed
                               for word in supplied):
            ap.error("--publish-sealed takes only KEY, --detach and --wait-s")
    if args.seal_only and (args.detach or args.withdraw or args.release_origin_consumer
                          or args.as_sealed_by):
        ap.error("--seal-only cannot combine with --detach, --withdraw, "
                 "--release-origin-consumer or --as-sealed-by")

    args.progress_policy = submission.parse_progress_phases(
        args.progress_phase, cycle=args.progress_cycle)
    args.awaited_batch = submission.parse_awaited_batch(args.awaited_batch)
    try:
        submission.validate_request(request_from_args(args))
    except SubmissionRefused as exc:
        if isinstance(exc, ArgumentRefused):
            ap.error(exc.reason)
        if args.seal_only or args.publish_sealed is not None or args.detach:
            print_consumer_refusal(exc, sealed=args.seal_only)
            raise SystemExit((exc.reason or 1) if plain_detach(args) else 1) from None
        raise SystemExit(exc.reason or exc.returncode) from None
    return args



def main(argv=None, *, publication_canary_intent=None, authorize_canary=None, report=None) -> int:
    report = report if report is not None else action_client.ClientReport()
    writer = None
    timing_scope = contextlib.ExitStack()
    previous_observer = report.on_event
    try:
        parser = argument_parser()
        args = parse_args(argv, parser=parser)
        display = (consumer_submission_display(configured_store(), render=render_submission)
                   if args.detach else render_submission)
        if args.client_events_fd is not None:
            try:
                writer = client_channel.ClientEventWriter(args.client_events_fd)
            except client_channel.ClientProtocolError as exc:
                raise SystemExit(f"pbrun: {exc}") from None
            def send(event):
                writer.send(event)
                if previous_observer is not None:
                    previous_observer(event)
            report.on_event = send
        timings = submission_timing.SubmissionTimings(
            on_phase=(writer.send if writer is not None else None),
            on_slow=lambda line: print(line, file=sys.stderr, flush=True))
        timing_scope.enter_context(timings.activate())
        json_options = {}
        withdrawal_rows = []
        if args.json:
            def withdrawal_result(key, state, withdrawn):
                withdrawal_rows.append({"schema": "prismabuild.withdrawal.v1",
                                        "action_key": key, "state_before": state,
                                        "withdrawn": withdrawn, "reason": args.reason})
            json_options["on_withdrawal"] = withdrawal_result

        if args.seal_only:
            record = sealed_submission.seal_consumer_request(request_from_args(args),
                store=configured_store(), display=display)
            print(json.dumps(record), flush=True)
            report.finish(action_client.ClientCommand(0))
            return 0
        if args.publish_sealed is not None:
            result = sealed_submission.publish_consumer_seal(args.publish_sealed,
                store=configured_store(), detach=args.detach, wait_s=args.wait_s,
                transport=args.transport, display=display, report=report)
        else:
            result = submission.submit_request(request_from_args(args), store=configured_store(), display=display,
                report=report, publication_canary_intent=publication_canary_intent,
                authorize_canary=authorize_canary, **json_options)
        if args.json:
            if not withdrawal_rows:
                withdrawal_result(args.withdraw[0], None, False)
            print(json.dumps(withdrawal_rows[0]))

        refusal = result if isinstance(result, SubmissionRefused) else None
        if refusal is not None:
            if isinstance(refusal, ArgumentRefused):
                parser.error(refusal.reason)
            if args.detach or args.publish_sealed is not None:
                print_consumer_refusal(refusal, sealed=False)
                if plain_detach(args):
                    raise SystemExit(refusal.reason or 1)
                return 1
            if refusal.returncode == 2:
                if refusal.reason:
                    print(refusal.reason, file=sys.stderr)
                raise SystemExit(2)
            if refusal.reason:
                raise SystemExit(refusal.reason)
            raise SystemExit(refusal.returncode)
        return report.ending.exit_code
    except SystemExit as exc:
        if report.ending is None:
            if isinstance(exc.code, int) and exc.code not in {0, 1, 2}:
                raise
            code = exc.code if isinstance(exc.code, int) else 0 if exc.code is None else 1
            report.finish(action_client.ClientCommand(code) if code == 0
                          else action_client.ClientRefused(code))
        raise
    except SubmissionRefused as exc:
        if report.ending is None:
            report.finish(action_client.ClientRefused(exc.returncode))
        if 'args' in locals() and (args.seal_only or args.publish_sealed is not None or args.detach):
            if isinstance(exc, ArgumentRefused):
                parser.error(exc.reason)
            print_consumer_refusal(exc, sealed=args.seal_only)
            if plain_detach(args):
                raise SystemExit(exc.reason or 1) from None
            return 1
        raise SystemExit(exc.reason or exc.returncode) from None
    except (OSError, pb.PrismaBuildError, ValueError) as exc:
        if 'args' not in locals() or not (args.seal_only or args.publish_sealed is not None):
            raise
        if report.ending is None:
            report.finish(action_client.ClientRefused(1))
        print_consumer_refusal(exc, sealed=args.seal_only)
        return 1
    finally:
        timing_scope.close()
        report.on_event = previous_observer
        if writer is not None:
            writer.close()


def plain_detach(args) -> bool:
    """Plain --detach exits 1 with the refusal's reason after its JSON line; the #1893 verbs exit 1 silently."""
    return args.detach and not args.seal_only and args.publish_sealed is None


def print_consumer_refusal(exc, *, sealed: bool) -> None:
    print(json.dumps(dict(schema=(sealed_submission.SEALED_SCHEMA if sealed
        else sealed_submission.SUBMISSION_SCHEMA), phases=submission_timing.submission_phase_snapshot(), refusal=dict(
        code=getattr(exc, "code", "submission_refused"),
        reason=getattr(exc, "reason", str(exc))))), flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
