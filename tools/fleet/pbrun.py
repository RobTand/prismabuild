#!/usr/bin/env python3
"""Parse and display a typed PrismaBuild submission."""
from __future__ import annotations
import argparse
from dataclasses import fields
import math
import json
from collections.abc import Sequence
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import action_edges, submission, action_client, client_channel, core as pb, pool, fleet_roster
from prismabuild.submission_types import FleetStore, SubmissionRequest, SubmissionRefused, Display
from prismabuild.submission import TRANSPORTS, RESIDENCY_MODES, RESIDENCY_RAM_MODES, DEFAULT_MAX_ATTEMPTS, CHECKOUT_SNAPSHOT_MAX_BYTES


def configured_store() -> FleetStore:
    return FleetStore(Path("/mnt/shared/prismabuild-fleet"), RUNTIME_ROOT, fleet_roster.Roster.beside(__file__))


def render(event: Display) -> None:
    print(event.text, file=sys.stderr if event.channel == "stderr" else sys.stdout,
          end=event.end, flush=event.flush)


def request_from_args(args: argparse.Namespace) -> SubmissionRequest:
    if args.request_json is not None:
        try:
            row = json.loads(args.request_json)
        except ValueError as exc:
            raise SubmissionRefused(f"invalid request JSON: {exc}", returncode=2) from None
        if not isinstance(row, dict):
            raise SubmissionRefused("request JSON must be an object", returncode=2)
        return submission.request_from_mapping(row)
    return SubmissionRequest(**{field.name: getattr(args, field.name) for field in fields(SubmissionRequest)})

def _profile_mode(text: str) -> str:
    """``--profile`` values, validated here so a bad one costs no submission.

    A mode may carry one option after a colon (``nsys:600`` traces the first
    ten minutes), and the whole string is sealed, so two windows are two
    actions.
    """

    try:
        return pb.parse_profile_mode(text)
    except pb.ProfileBackendUnavailable as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Read one submission's arguments, and nothing about the world.

    Split out so a caller that is not the command line -- ``pbcampaign``
    decomposing one logical request into many actions -- can build the same
    namespace the CLI builds and hand it to ``prepare_submission``.  Every
    refusal below is a statement about the arguments alone, so it holds
    wherever they came from.

    ``--progress-phase`` is parsed here rather than at its use: the policy is
    the normalized form of two flags, a typo in one of them is an argument
    error, and saying so before ``--withdraw`` is where ``main`` said it.
    """

    store = configured_store()
    ap = argparse.ArgumentParser(
        description="Submit one command to the PrismaBuild fleet and wait for "
                    "it. Either the pull queue or SLURM carries it, per "
                    "--transport or the published generation's default; the "
                    "result does not depend on which."
    )
    ap.add_argument("--request-json", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--client-events-fd", type=int, default=None, help=argparse.SUPPRESS)
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
        "--residency-share", choices=("auto", "off"), default="auto",
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
    ap.add_argument("--profile", type=_profile_mode, default=None,
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
        default=submission.default_transport(store=store),
        help="which dispatcher carries this submission (env "
             "PRISMABUILD_TRANSPORT, else the published runtime generation's "
             "default_transport); the pull queue stays the default until the "
             "fleet has cut over to SLURM")
    ap.add_argument("command", nargs=argparse.REMAINDER,
                    help="the command to run, after a bare --; every word "
                         "past it belongs to the command and not to pbrun")
    args = ap.parse_args(argv)
    args.progress_policy = submission.parse_progress_phases(
        args.progress_phase, cycle=args.progress_cycle)
    args.awaited_batch = submission.parse_awaited_batch(args.awaited_batch)
    try:
        submission.validate_request(request_from_args(args))
    except SubmissionRefused as exc:
        if exc.returncode == 2:
            ap.error(exc.reason)
        raise SystemExit(exc.reason or exc.returncode) from None
    return args



            if renewal["retired"]:
                print(
                    f"pbrun: renewing {key[:12]}: retired "
                    f"{len(renewal['retired'])} predecessor cancellation "
                    f"marker(s); their decisions stay under "
                    f"{q.superseded_dir()}", file=sys.stderr, flush=True)
            publication = publication_row(
                action, args=args, queue=q,
                tested_repository=template.get("tested_repository"))
            publication["residency"] = staged["residency"]
            if template.get("produced_output_template") is not None:
                publication["produced_output_template"] = template[
                    "produced_output_template"]
            if before_publish is not None:
                before_publish()
            if attach:
                # A release resuming after a crash (#913) finds its own row:
                # the plan above was first-writer and reused, and the row is
                # the one generation to wait on, not a second copy.
                queued_path, generation = publish_or_attach(
                    q, publication, key=key)
            else:
                # Not ``publish_or_attach``: a staged submission has already
                # frozen its window plan, which is first-writer and has its
                # own answer for a second seal of the same body, and the
                # duplicate #812 describes is a plain shard submission.
                queued_path = publish_or_refuse(q, publication)
                generation = published_generation(q, key, queued_path)
            # The consumer's row and nothing else.  Every phase of the
            # frozen plan is the tiers loop's to publish, the first included:
            # the loop adopts before it publishes, and its adoption pass skips
            # any leg whose row already exists, so a lead published here could
            # never be taken over from a range already on the tier.  One
            # publisher also means no interleaving to arbitrate.  A cold lead
            # therefore waits for the next cycle, which staged submissions
            # already depend on for phases 1..n.
            lead = staged["plan"]["phases"][0]
            print(f"pbrun: staging {len(staged['plan']['phases'])} phases onto "
                  f"{staged['plan']['tier_id']}; phase {lead['name']!r} "
                  f"({lead['stage_gib']} GiB) next, for the tiers loop to "
                  f"adopt or publish",
                  file=sys.stderr, flush=True)
    else:
        publication = publication_row(
            action, args=args, queue=q,
            tested_repository=template.get("tested_repository"))
        if template.get("produced_output_template") is not None:
            publication["produced_output_template"] = template[
                "produced_output_template"]
        if before_publish is not None:
            before_publish()
        queued_path, generation = publish_or_attach(q, publication, key=key)
    return queued_path, generation


def release_origin_consumer_cli(batch_ref: str, consumer_key: str, *,
                                reason: str, by: str) -> int:
    """``--release-origin-consumer``: release one declaration (#926, #945)."""

    from prismabuild import produced_output as produced_mod

    text = batch_ref
    if not batch_ref.lstrip().startswith("{"):
        try:
            text = Path(batch_ref).read_text()
        except OSError as exc:
            raise SystemExit(
                f"pbrun: --release-origin-consumer: BATCH_REF is neither JSON "
                f"nor a readable file: {exc}") from None
    try:
        ref = json.loads(text)
    except ValueError as exc:
        raise SystemExit(
            f"pbrun: --release-origin-consumer: BATCH_REF is not JSON: {exc}"
        ) from None
    q = pool.PoolQueue(SH / "pb-queue")
    # What an unpublished declaration's release compares against (#945): the
    # generation live now, and the CAS its sealed request sits in.  Unknown,
    # it refuses that case alone.
    try:
        live_runtime = (SH / "repo").resolve(strict=True)
    except OSError:
        live_runtime = None
    try:
        result = produced_mod.release_origin_consumer(
            q, ref, consumer_action_key=consumer_key, by=by, reason=reason,
            live_runtime=live_runtime, cas_root=SH / "cas")
    except (produced_mod.ProducedOutputError, OSError, ValueError) as exc:
        raise SystemExit(f"pbrun: --release-origin-consumer: {exc}") from None
    print(json.dumps({**result, "consumer_action_key": consumer_key,
                      "ref": ref}, sort_keys=True), flush=True)
    return 0


def main(*, publication_canary_intent=None, authorize_canary=None, report=None) -> int:
    report = report if report is not None else action_client.ClientReport()
    writer = None
    previous_observer = report.on_event
    with action_wait.client_readers():
        try:
            args = parse_args()
            fd = getattr(args, 'client_events_fd', None)
            if fd is not None:
                try:
                    writer = client_channel.ClientEventWriter(fd)
                except client_channel.ClientProtocolError as exc:
                    raise SystemExit(f'pbrun: {exc}') from None

                def send(event):
                    writer.send(event)
                    if previous_observer is not None:
                        previous_observer(event)

                report.on_event = send
            # Internal publisher handoff only; there is no public self-authorizing
            # switch. PoolQueue independently checks the publisher's exact-key grant.
            if publication_canary_intent is not None:
                if (not callable(authorize_canary) or args.transport != "pool"
                        or not args.detach or args.after or args.withdraw
                        or args.release_origin_consumer is not None):
                    raise SystemExit("pbrun: publication canary requires a bounded publisher handoff")
            if args.withdraw:
                # Withdrawing is not a submission and must not need one: the operator
                # cancelling four suites has no command to give and no checkout to
                # stamp, so this returns before any of the submit machinery runs.
                if [c for c in args.command if c != "--"]:
                    raise SystemExit("pbrun: --withdraw takes no command")
                try:
                    who = getpass.getuser()
                except Exception:                                        # noqa: BLE001
                    who = "unknown"      # no passwd entry is not a reason to refuse
                return report.finish(action_client.ClientCommand(withdraw_routed(
                    args.withdraw, transport=args.transport, reason=args.reason,
                    by=f"{who}@{socket.gethostname()}",
                )))
            if args.release_origin_consumer is not None:
                # Like a withdrawal, an operator's release is not a submission.
                if [c for c in args.command if c != "--"]:
                    raise SystemExit("pbrun: --release-origin-consumer takes no command")
                try:
                    who = getpass.getuser()
                except Exception:                                        # noqa: BLE001
                    who = "unknown"
                return report.finish(action_client.ClientCommand(release_origin_consumer_cli(
                    *args.release_origin_consumer, reason=args.reason,
                    by=f"{who}@{socket.gethostname()}")))
            # The used-filesystem floor (#1483): check what this submission writes,
            # and hold its growth allowance on the shared store while the snapshot,
            # CAS objects and queue record are written.  Released, whatever ends the
            # submission, before an attached pool or ``--after`` wait begins.  The
            # SLURM lane submits and waits in one call, so it holds no growth (its
            # used paths are still checked).  ``off`` (the default) reads only the
            # mode.
            try:
                with filesystem_floor.operation(
                        SH / "pb-queue", label="pbrun",
                        used_paths=[getattr(args, "cwd", None) or os.getcwd(), SH / "cas",
                                    SH / "pb-queue", tempfile.gettempdir()],
                        growth_gib=({SH / "cas": args.filesystem_growth_gib}
                                    if args.transport == "pool" else {})):
                    submitted = submit_and_publish(
                        args, publication_canary_intent=publication_canary_intent,
                        authorize_canary=authorize_canary, report=report)
            except filesystem_floor.FloorRefused as exc:
                raise SystemExit(f"pbrun: a used filesystem is below its floor; nothing "
                                 f"submitted: {exc}") from None
            code = submitted() if callable(submitted) else submitted
            if report.ending is None:
                raise RuntimeError('submission returned without a typed client ending')
            return code
        except OfferDiscoveryTimedOut:
            if report.ending is None:
                report.finish(action_client.ClientOfferTimeout())
            raise
        except SystemExit as exc:
            if report.ending is None:
                if isinstance(exc.code, int) and exc.code not in {0, 1, 2}:
                    raise
                code = (exc.code if isinstance(exc.code, int)
                        else 0 if exc.code is None else 1)
                if code == 0:
                    report.finish(action_client.ClientCommand(code))
                else:
                    identity = report.identity
                    report.finish(action_client.ClientRefused(code,
                        identity.action_key if identity is not None else None,
                        getattr(identity, 'published_unix', None)))
            raise
        finally:
            report.on_event = previous_observer
            if writer is not None:
                writer.close()


def submit_and_publish(args, *, publication_canary_intent=None,
                       authorize_canary=None, report=None):
    """Seal, publish and submit; an attached wait is returned, not run."""

    report = report if report is not None else action_client.ClientReport()
    prepared = prepare_submission(args)
    if args.after:
        return submit_deferred(prepared, args, report=report)
    template = prepared["template"]
    cwd = prepared["cwd"]
    portable_checkout = prepared["portable_checkout"]
    # The resolved half of the submission is the template's, not a second
    # copy kept alongside it: what was sealed is what the notices describe
    # and what the queue row is published with.
    tags = template["params"]["placement"]["required_tags"]
    demand = template["params"]["demand"]
    cas = template["cas"]
    action = (seal_action_from_template(
        template, extra_params={"publication_canary": publication_canary_intent})
        if publication_canary_intent is not None else seal_action_from_template(template))
    key = str(action["action_key"])

    if args.as_sealed_by is not None and key != args.as_sealed_by:
        raise SystemExit(
            f"pbrun: --as-sealed-by expected {args.as_sealed_by}, but current "
            f"work seals to {key}; nothing submitted. Restore the original "
            "checkout, command, inputs and options, or omit --as-sealed-by "
            "to request different work. A changed sealing contract can also "
            "prevent reproduction by this client."
        )

    placement_announced = False
    if publication_canary_intent is not None:
        # The placement verdict runs BEFORE the grant (#1278).  The grant is
        # one per runtime generation and host and is never replenished, so a
        # submission refused at placement after the mint would spend the
        # slot on work that never ran.
        announce_placement(
            prepared["offer_queue"](), action, args=args, cwd=cwd,
            portable_checkout=portable_checkout)
        placement_announced = True
        authorize_canary(action)
    request_path = cas.publish_action_request(action)

    cached = cas.lookup(action) if args.detach else None
    if cached is not None:
        identity = action_client.CachedClientIdentity(key, SH / 'pb-queue', Path(cas.root),
            action_client.ClientTransport(args.transport), cached['result']['sha256'])
        report.publish_identity(identity)
        # Nothing to submit and nothing to wait for.  The attached path lets
        # the worker discover this and file an ending, which is right when
        # somebody is holding the terminal open; detached, that ending would be
        # a job scheduled, a checkout materialized and a node occupied to learn
        # what this process already knows.  A campaign re-run is the case: every
        # row a hit, no new job ids.
        print(f"pbrun: {key[:12]} is already in the CAS; nothing submitted",
              file=sys.stderr, flush=True)
        print(sorted_json(action_client.detached_submission_record(
            key,
            transport=args.transport,
            status="cache_hit",
            queue_root=SH / "pb-queue",
        )), flush=True)
        return report.finish(action_client.ClientSubmitted(identity))

    if args.detach:
        # Not in the CAS, but perhaps already running: a campaign whose waiter
        # died is re-run to find out where it got to, and every row still on a
        # node must be attached to rather than submitted again.
        try:
            live = bounded_attachment(pool.PoolQueue(SH / "pb-queue"), key)
        except (action_wait.OutcomeReadUnavailable, OSError) as exc:
            print(f"pbrun: unavailable detached attachment for {key[:12]}: {exc}",
                  file=sys.stderr, flush=True)
            return report.finish(action_client.ClientUnavailable(
                key, None, action_wait.WaitEnd.UNRETRIED))
        if live is not None:
            transport, generation = live["transport"], live["generation"]
            record, job_id = live["submission"], live["job_id"]
            identity = (action_client.PoolClientIdentity(key, generation, SH / 'pb-queue',
                Path(cas.root), Path(record) if record is not None else None,
                action_client.ClientArrival.ATTACHED)
                if transport == 'pool' else action_client.SlurmClientIdentity(
                    key, generation, SH / 'pb-queue', Path(cas.root),
                    Path(record) if record is not None else None, job_id,
                    action_client.ClientArrival.ATTACHED))
            report.publish_identity(identity)
            print(f"pbrun: {key[:12]} is already running "
                  f"({transport}{' job ' + job_id if job_id else ''}); "
                  f"attaching to it rather than submitting a second copy",
                  file=sys.stderr, flush=True)
            print(sorted_json(action_client.detached_submission_record(
                key,
                transport=transport,
                status="attached",
                queue_root=SH / "pb-queue",
                published_unix=generation,
                job_id=job_id or None,
                submission=record,
            )), flush=True)
            return report.finish(action_client.ClientSubmitted(identity))

    # D38: new GPU work needs preflight evidence before anything runnable is
    # published, on every transport.  There is no liveness or cache exemption
    # here: a read taken now is stale by the time the queue decides whether the
    # key is new.  Without evidence this process publishes and submits nothing;
    # it may only wait on a pool run it can see, or refuse.  (The detached
    # branches above already answered a cache hit and a live attachment without
    # publishing.)
    verdict = d38_gate.judge_publication(
        args, action, cas=cas, queue_root=SH / "pb-queue",
        transport=args.transport)
    if verdict is not None:
        if not args.detach:
            try:
                live = bounded_attachment(pool.PoolQueue(SH / "pb-queue"), key)
            except (action_wait.OutcomeReadUnavailable, OSError):
                live = None
            if live is not None and live["transport"] == "pool":
                report.publish_identity(action_client.PoolClientIdentity(key, live['generation'],
                    SH / 'pb-queue', Path(cas.root),
                    Path(live['submission']) if live['submission'] is not None else None,
                    action_client.ClientArrival.ATTACHED))
                print(f"pbrun: {key[:12]} is already running on the pool; "
                      f"waiting on that run, and publishing nothing, because "
                      f"D38 evidence is missing", file=sys.stderr, flush=True)
                return functools.partial(
                    action_client.report_pool_outcome, pool.PoolQueue(SH / "pb-queue"), key,
                    wait_s=args.wait_s, generation=live["generation"], report=report)
        raise d38_gate.refusal_exit(verdict, action)

    if args.transport == "slurm":
        # Everything below this point reads the pull queue -- worker offers,
        # the placement census, the ready directory -- and none of it describes
        # a SLURM fleet.  Worse, it would answer *wrongly*: a retained offer
        # from a loop that has been stopped for the cutover would refuse a
        # submission the scheduler can place perfectly well.  The capability
        # check SLURM keeps is its own: an unknown Feature or an impossible
        # GRES makes ``sbatch`` refuse at submit time, which is the same moment
        # and the same intent as ``capability_verdict`` below.
        return slurm_outcome(
            action,
            cas=cas,
            request_path=request_path,
            # Built here because only ``main`` knows the checkout and the
            # flags it was asked with.  The census is unavailable rather than
            # empty: this branch builds no PoolQueue on purpose, and worker
            # offers do not describe a SLURM fleet.
            placement_notice=pin_notice(
                _NoCensus(),
                {"tags": tags, "needs_gpu": bool(demand.get("gpu")),
                 "resources": demand},
                cwd=cwd,
                hostname=socket.gethostname(),
                here=args.here,
                portable_checkout=portable_checkout,
                unknown_census=NO_CENSUS,
            ),
            tags=tags,
            demand=demand,
            exclusive=args.exclusive,
            timeout_s=args.timeout_s,
            wait_s=args.wait_s,
            retry_safe=args.retry_safe,
            max_attempts=args.max_attempts,
            priority=args.priority,
            priority_reason=args.priority_reason,
            anywhere=args.anywhere,
            detach=args.detach,
            report=report,
        )

    q = pool.PoolQueue(SH / "pb-queue")

    if not placement_announced:
        announce_placement(
            prepared["offer_queue"](), action, args=args, cwd=cwd,
            portable_checkout=portable_checkout)

    # Read the decision this submission is about to supersede, so the caller is
    # told rather than surprised.  ``publish`` retires the marker -- a key is a
    # content hash, so re-submitting one is how anybody asks for the same work
    # again -- and a submission that silently revived somebody's cancellation
    # would be as bad as the blacklist it replaced.
    superseding = None
    try:
        superseding = json.loads(
            q.item_path("withdrawn", key).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        superseding = None

    # A consumer of consumed produced-output batches is filed against them
    # before its row exists, so their retirement cannot run ahead of it.  A
    # crash after this and before the row leaves the batches held and
    # reported, never deleted (#914).  From the first declaration through
    # the row this process holds the key's transition lock, so an operator
    # cannot release the declaration while the row is still to come (#945).
    origin_refs = template.get(_ORIGIN_BATCHES_TEMPLATE_KEY) or []
    with submission_window(q, key, origin_refs):
        if origin_refs:
            declare_origin_consumers(q, origin_refs, consumer_action_key=key)
        # Filed before the row, so an edge that names the replaced key
        # follows this one from the moment it can run (#913), but only once
        # everything that can still refuse has had its say (#1585): the
        # record is immutable, and one filed for a replacement that then
        # refused named a key that never published, which the corrected
        # submission (another key) could not replace and a reader following
        # it could not find.  What can be refused now is refused now, with
        # nothing written; the record itself goes in right before the row.
        supersede = None
        if args.supersedes is not None:
            check_supersession_or_exit(q, args.supersedes, new=key,
                                       new_kind=action_edges.PRODUCER_KEY)

            def supersede() -> None:
                file_supersession_or_exit(q, args.supersedes, new=key,
                                          new_kind=action_edges.PRODUCER_KEY)

        queued_path, generation = publish_consumer_row(
            q, action, template, key=key, args=args, cas=cas,
            before_publish=supersede)
    identity = action_client.PoolClientIdentity(key, generation, q.root, Path(cas.root),
        queued_path, (action_client.ClientArrival.SUBMITTED if queued_path is not None
                      else action_client.ClientArrival.ATTACHED))
    report.publish_identity(identity)
    # Say that the slot has no device, every time.  The mask is correct and it
    # is also a silent narrowing: a suite that used to run its CUDA tests now
    # skips them, and a skip that nobody announced reads as the same green.
    if superseding is not None and queued_path is not None:
        # Only a publication retires a marker.  An attached duplicate
        # published nothing, so it superseded nothing.
        who = superseding.get("withdrawn_by") or "an operator"
        why = str(superseding.get("reason") or "").strip()
        print(f"pbrun: {key[:12]} had been withdrawn by {who}"
              f"{' -- ' + why if why else ''}; this submission supersedes that "
              f"decision", file=sys.stderr, flush=True)
    masked = "" if demand.get("gpu") else "  [no GPU: CUDA_VISIBLE_DEVICES='']"
    verb = "queued" if queued_path is not None else "attached to"
    # The one line that names the full key, so a caller can cite the action
    # rather than a prefix of it; ``pbtest`` records it per shard (#1012).
    # Every later line names it by prefix, which is what ``--withdraw`` takes.
    print(f"pbrun: {verb} {key} tags={tags} demand={demand}{masked} "
          f"published_unix={generation}",
          file=sys.stderr, flush=True)

    if args.detach:
        if queued_path is None:
            # The same display path ``_attachment_value`` names, read once:
            # this process started nothing, so it reports where the run it
            # joined lives rather than a submission of its own.
            ready = q.item_path(pool.READY, key)
            display_path = ready if ready.exists() else q.item_path(pool.CLAIMED, key)
            print(sorted_json(action_client.detached_submission_record(
                key,
                transport="pool",
                status="attached",
                queue_root=q.root,
                published_unix=generation,
                submission=display_path,
            )), flush=True)
            return report.finish(action_client.ClientSubmitted(identity, display_path))
        print(sorted_json(action_client.detached_submission_record(
            key,
            transport="pool",
            status="submitted",
            queue_root=q.root,
            published_unix=generation,
            submission=queued_path,
        )), flush=True)
        return report.finish(action_client.ClientSubmitted(identity))

    return functools.partial(action_client.report_pool_outcome, q, key, wait_s=args.wait_s,
                             generation=generation, report=report)


if __name__ == "__main__":
    raise SystemExit(main())
