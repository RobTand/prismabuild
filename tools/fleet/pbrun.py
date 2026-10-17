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



        # closed there rather than sealing drifted demand.
        try:
            with open(produced_template_opt, "rb") as handle:
                raw_pre = handle.read(pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES + 1)
        except OSError as exc:
            raise SystemExit(
                f"pbrun: cannot read --produced-output-template: {exc}") from None
        if len(raw_pre) > pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES:
            raise SystemExit(
                "pbrun: --produced-output-template exceeds "
                f"{pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES} bytes")
        try:
            pre_body = json.loads(raw_pre.decode())
        except (UnicodeDecodeError, ValueError) as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template is not JSON: {exc}") from None
        try:
            pre_validated = produced_mod.validate_template(pre_body)
            pre_terms = produced_mod.owner_demand_terms(pre_validated)
        except produced_mod.ProducedOutputError as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template invalid: {exc}") from None
        # The explicit user reservation (CPU/memory/GPU) is preserved; the
        # qualified tier demand is derived from the bounded working window,
        # never typed by hand and never the durable corpus.
        for qualified, need in pre_terms.items():
            if qualified in demand:
                raise SystemExit(
                    f"pbrun: --demand must not name tier demand {qualified!r}: "
                    "the produced-output template derives it")
            demand[qualified] = int(need)

    if portable_checkout:
        require_relocatable_checkout(
            command[:1], {}, cwd, repository_root=repository_root
        )

    # A CPU slot must not be able to run GPU work.  The pool's whole claim is
    # that the ledger knows what is on each accelerator, and that claim was
    # false in one direction: an action submitted WITHOUT ``--gpu`` inherited a
    # visible device and ran CUDA anyway.  A pytest suite queued as a 4 GB CPU
    # action executed its ``skipif(not torch.cuda.is_available())`` tests on a
    # box whose GPU slots were held by somebody else -- work the ledger could
    # not see, contending with work it had promised exclusivity to.
    #
    # The rule is enforced the way ``require_pool.py`` enforces its own escape
    # hatch, by the kernel rather than by belief: with no device visible the
    # child cannot do GPU work, so a mis-declared action fails instead of
    # stealing.  Declaring a device on a slot that did not reserve one is the
    # mis-declaration itself, so it is refused rather than honoured -- the fix
    # is ``--gpu``, and the message says so.  This applies under
    # ``--no-default-env`` too: an empty environment means every device is
    # visible, which is the case this exists for.
    declared = variables.get("CUDA_VISIBLE_DEVICES")
    try:
        require_gpu_memory_scope(
            gpu_memory_gb=args.gpu_memory_gb, gpu=bool(demand.get("gpu")),
            transport=args.transport,
        )
    except ValueError as exc:
        args.refuse_argument(str(exc))
    try:
        require_progress_scope(
            progress=progress_policy, transport=args.transport)
        require_awaited_batch_scope(
            awaited=getattr(args, "awaited_batch", None),
            progress=progress_policy, transport=args.transport)
    except ValueError as exc:
        args.refuse_argument(str(exc))
    if not demand.get("gpu"):
        if declared not in (None, ""):
            raise SystemExit(
                f"pbrun: this action reserves no GPU but sets "
                f"CUDA_VISIBLE_DEVICES={declared!r}.\n"
                "A CPU slot that touches the GPU is work the ledger cannot "
                "see, contending with work it promised exclusivity to.\n"
                "Add --gpu (and --gpu-capacity N if you need more than one "
                "slot), or drop the variable.")
        variables["CUDA_VISIBLE_DEVICES"] = ""

    template = freeze_action_template(
        command=command,
        cwd=cwd,
        logical_cwd=logical_cwd,
        demand=demand,
        placement=placement,
        variables=variables,
        determinism=determinism,
        retry_policy=retry_policy,
        host_class=args.host_class,
        measurement=args.measurement,
        transport=args.transport,
        pool_measurement_class=bool(pool_measurement_class),
        target_evidence=target_evidence,
        # A deferred submission's manifest is built at release (#913); its
        # static part is ingested beside the template, never sealed into it.
        data_manifest_path=(None if getattr(args, "after", None)
                            else args.data_manifest),
        produced_output_template_path=produced_template_opt,
        checkout_snapshot_max_bytes=args.checkout_snapshot_max_bytes,
        snapshot_refs=args.snapshot_ref,
        exclusive=args.exclusive,
        gpu_memory_gb=args.gpu_memory_gb,
        execution_timeout_s=args.timeout_s,
        progress=progress_policy,
        awaited_batch=getattr(args, "awaited_batch", None),
        profile=args.profile,
        container_image_refs=images,
        d38_namespace=(d38_gate.load_namespace(args.d38_namespace)[1]
                       if getattr(args, "d38_namespace", None) else None),
        wrapper_dir=wrapper_dir,
        dependency_queries=dependency_queries,
        gang=gang,
        resident_set=getattr(args, "resident_set", None),
        requires_files=requirements,
    )
    return {
        "args": args,
        "template": template,
        "cwd": cwd,
        "portable_checkout": portable_checkout,
        "offer_queue": offer_queue,
    }


def announce_placement(
    queue,
    action: Mapping[str, object],
    *,
    args: argparse.Namespace,
    cwd: Path,
    portable_checkout: bool,
) -> None:
    """Say how this work will be placed, and refuse it if it cannot be.

    Four notices and one refusal, all of them about one intent -- the tags,
    the GPU need and the demand -- which is why they are one function.  A
    decomposed campaign calls it once, before it publishes any child: the
    children of one plan share their whole placement, so asking the census N
    times would print the same paragraph N times and answer it N times.

    The refusal is the part that must not be skipped.  A required tag no box
    has offered is not a slow submission -- the item matches no worker's
    filter, so it sits in ``ready`` while every idle worker polls past it --
    and a campaign that published forty such children would have forty of
    them to withdraw.
    """

    params = action["params"]
    tags = params["placement"]["required_tags"]
    demand = params["demand"]
    progress_policy = args.progress_policy
    intent = {"tags": tags, "needs_gpu": bool(demand.get("gpu")), "resources": demand}
    if params.get("container_images"):
        # The sealed requirement, so every verdict below -- placeable,
        # placement_hosts, the refusal -- reads the same matcher a claim does.
        intent["container_images"] = list(params["container_images"])
    if params.get("interpreter"):
        # Same authority for the interpreter (#1263): the probe carries the
        # sealed path and the capability tag publish will add, so placeable
        # answers for the row it is about to write.
        intent["interpreter"] = str(params["interpreter"])
        intent["tags"] = [*tags, contracts.INTERPRETER_TAG]
    if params.get("local_dependencies"):
        intent["local_dependencies"] = dict(params["local_dependencies"])
    if params.get("requires_files"):
        # Same authority for the digest requirements (#1495): the probe
        # carries the sealed declarations and the capability tag publish adds, so
        # placeable answers for the row it is about to write.
        intent["requires_files"] = [dict(entry) for entry in params["requires_files"]]
        intent["tags"] = pool.normalize_placement_tags(
            [*tags, contracts.DEPENDENCY_DIGEST_TAG])
    # Say how wide this action is before saying it was queued.  A pin is a
    # consequence of the checkout path, and nothing used to report it, so a
    # submitter narrowed the fleet to one box without being told.
    notice = pin_notice(
        queue,
        intent,
        cwd=cwd,
        hostname=socket.gethostname(),
        here=args.here,
        portable_checkout=portable_checkout,
    )
    if notice:
        print(notice, file=sys.stderr, flush=True)

    # An offer can be fresh but stamped ahead of this submitter. Keep that
    # discrepancy visible, including an offer too far ahead to use at all.
    for host, skew in queue.offer_clock_skews().items():
        disposition = "tolerated" if skew <= pool.OFFER_FUTURE_TOLERANCE_S else "ignored"
        print(
            f"pbrun: {host} offer announced {skew:.3f}s in the future "
            f"(clock skew; {disposition}, limit {pool.OFFER_FUTURE_TOLERANCE_S:g}s)",
            file=sys.stderr, flush=True,
        )

    # Before the placement verdicts, not after.  ``intent`` now requires
    # ``PROGRESS_TAG``, so on a fleet that offers none the generic "no recorded
    # worker can run this action" would fire first and name a tag the operator
    # never typed.  Asked of the boxes eligible on every OTHER tag, which is
    # also the honest question: of the boxes that could run this work, which
    # can keep its stall policy?
    progress_tags = contracts.declared_contract_tags(contracts.Publication(
        progress=progress_policy,
        awaited_batch=progress_policy is not None
            and getattr(args, "awaited_batch", None) is not None))
    progress_notice = progress_contract_notice(
        queue,
        {**intent, "tags": [
            t for t in tags
            if t not in progress_tags
        ]},
        policy=progress_policy,
        requested_timeout_s=args.timeout_s,
        awaited_batch=getattr(args, "awaited_batch", None),
    )
    if progress_notice:
        print(progress_notice, file=sys.stderr, flush=True)

    image_notice = container_image_notice(queue, intent)
    if image_notice:
        print(image_notice, file=sys.stderr, flush=True)

    # Refuse work the RECORDED fleet cannot run, at the one moment the caller
    # is still watching.  A required tag no box has offered is not a slow
    # submission: the item matches no worker's placement filter, so it sits in
    # `ready` -- counted, reported as pending -- while every idle worker polls
    # past it until `--wait-s` expires a day later.
    #
    # Capability and liveness are deliberately different reads of the SAME
    # matcher.  Worker offers expire for claiming and fleet-width diagnostics,
    # but the latest record from each host remains evidence of what that box
    # can fit.  dl380g10 and gx10-6b77 have both spent longer than the 120 s TTL
    # inside work; while they were between announcements a fresh nonmatching
    # offer made ``placeable`` answer False and pbrun rejected a caller willing
    # to wait two hours.  An unbounded age reads retained capability and lets
    # ``--wait-s`` own an offline/busy box.  ``None`` still means no worker has
    # ever announced and stays a warning, so a fleet whose loops predate the
    # registry can submit unchecked.
    #
    # Images are one more term in the same matcher, and they make the refusal
    # sharper rather than a new kind of check: a declared reference no
    # recorded eligible offer positively reports is exactly as unplaceable as
    # a tag no box offers, and it fails here instead of in the container
    # (#714).
    # The interpreter's own verdict first (#1266 review): absent raises below
    # with the path named; unknown publishes with a notice and probes the
    # rest of the intent WITHOUT the interpreter requirement, because the
    # first submission of any path is unknown by construction and refusing it
    # would deadlock the tool on its own first use -- the claim-time stat is
    # the guard, and the fleet's next poll answers for the path.
    probe_intent = intent
    if intent.get("interpreter"):
        verdict, path = interpreter_submission_verdict(queue, intent)
        if verdict == "unknown":
            print(f"pbrun: no worker has answered for the interpreter "
                  f"{path} yet; the action publishes, the claim-time check "
                  "guards every box, and the fleet's next poll places it "
                  "where the path lives.", file=sys.stderr, flush=True)
            probe_intent = {
                name: value for name, value in intent.items()
                if name != "interpreter"}
            probe_intent["tags"] = [
                tag for tag in probe_intent.get("tags") or []
                if tag != contracts.INTERPRETER_TAG]
    # The digest requirements' verdict (#1495), asked where the caller is
    # still watching, in the interpreter's shape (#1266): a fleet that offers
    # no dependency-digest capability is refused -- nothing can ever claim
    # the row, and unlike a first path there is no first use that must
    # publish; a unanimous absent is refused naming the paths; a capable box
    # that has not answered for a path yet publishes with a notice, because
    # the claim gate hashes the bytes on every box and the fleet's next poll
    # answers for them.
    if intent.get("requires_files"):
        verdict = queue.dependency_placement_verdict(probe_intent)
        paths = ", ".join(sorted(
            str(entry["path"]) for entry in intent["requires_files"]))
        if verdict == "unknown_capability":
            raise SystemExit(
                "pbrun: no recorded worker offers the "
                f"{contracts.DEPENDENCY_DIGEST_TAG} capability, so "
                f"nothing can ever claim this action's pinned dependencies "
                f"({paths}); run a worker generation that carries the "
                "contract before submitting it (#1495)\n"
                + "\n".join(contracts.contract_remedies([contracts.DEPENDENCY_DIGEST_TAG])))
        if verdict == "absent":
            raise SystemExit(
                "pbrun: every recorded worker that could claim this action "
                f"names its required dependencies absent: {paths} (#1495)")
        if verdict == "unknown_paths":
            print(
                "pbrun: no worker has answered for the required "
                f"dependencies ({paths}) yet; the action publishes, the "
                "claim-time digest check guards every box, and the fleet's "
                "next poll places it where the bytes live.",
                file=sys.stderr, flush=True)
            probe_intent = {
                name: value for name, value in probe_intent.items()
                if name != "requires_files"}
            probe_intent["tags"] = [tag for tag in probe_intent.get("tags") or []
                                    if tag != contracts.DEPENDENCY_DIGEST_TAG]
    live_verdict = queue.placeable(probe_intent)
    capability_verdict = queue.placeable(
        probe_intent, max_age_s=RECORDED_OFFER_MAX_AGE_S)
    if capability_verdict is False:
        # The counterfactuals below start from ``probe_intent``, the intent the
        # verdict above was asked of (#1595).  An unknown interpreter is
        # already out of it.  Starting from ``intent`` kept the interpreter
        # requirement, so every question failed on the interpreter and the
        # missing image or capability was never named.  The interpreter keeps
        # its own verdict, asked of ``intent`` below.
        images = (queue.image_placement_census(
            probe_intent, max_age_s=RECORDED_OFFER_MAX_AGE_S)
            if intent.get("container_images") else None)
        image_blocked = bool(images and images["capable"])
        capability_blocked = bool(images and images["eligible"] and not image_blocked)
        image_line = ""
        if image_blocked:
            image_line = (
                "  container images: "
                + ", ".join(intent["container_images"])
                + " (no recorded eligible worker reports "
                + ("it" if len(intent["container_images"]) == 1 else "them")
                + ")\n")
        elif capability_blocked:
            image_line = (
                "  container images: " + ", ".join(intent["container_images"])
                + f" (no eligible worker offers {contracts.CONTAINER_IMAGE_TAG})\n")
        interpreter_line = ""
        if intent.get("interpreter"):
            # Absent is unanimous and final (#1266 review): every capable
            # eligible offer answered, and every answer named the path
            # missing.  Anything less is unknown, which publishes with a
            # notice instead of refusing -- a first submission is unknown by
            # construction, and the claim-time stat remains the guard.
            verdict, _path = interpreter_submission_verdict(queue, intent)
            if verdict == "absent":
                interpreter_line = (
                    "  interpreter:    " + str(intent["interpreter"])
                    + " (every recorded eligible worker names it absent)\n")
        capacity_line = ("" if image_blocked or capability_blocked else
                         placement_capacity_notice(queue, probe_intent))
        remedy = (
            "Load or pull the image on a box that offers these tags and the "
            f"{contracts.CONTAINER_IMAGE_TAG} capability, then wait for its worker's "
            "next inventory refresh. An image declaration is a claim-time "
            "requirement, so a box that cannot positively show it will not "
            "run the action."
            if image_line else
            "Fix the listed capacity demand or required tags, or wait for an "
            "offer that can fit them."
        )
        if capability_blocked:
            remedy = (f"Start a worker offering {contracts.CONTAINER_IMAGE_TAG} "
                      "on an eligible box; its image inventory must then "
                      "report the declared reference.")
        if interpreter_line:
            remedy = (
                "The interpreter path is part of the action's placement: a "
                "worker must report it in its offer before the action can "
                "run there. Install the interpreter on a box that offers "
                "these tags, let its worker's next poll answer for the path, "
                "and resubmit; submitting anyway would only queue a command "
                "that dies with 127 on the first box to claim it.")
        recorded_tags = queue.offered_tags(max_age_s=RECORDED_OFFER_MAX_AGE_S)
        missing_code_tags = set(tags) - set(recorded_tags)
        if capability_blocked:
            missing_code_tags.add(contracts.CONTAINER_IMAGE_TAG)
        guidance = contracts.contract_remedies(missing_code_tags)
        if guidance:
            remedy += "\n" + "\n".join(guidance)
        raise SystemExit(
            f"pbrun: no recorded worker can run this action.\n"
            f"  required tags: {tags or '(any box)'}\n"
            f"{image_line}"
            f"{interpreter_line}"
            f"  demand:        {demand}\n"
            f"{capacity_line}"
            f"  offered on record: "
            f"{recorded_tags}\n"
            f"{remedy}"
        )
    producer_room_refusal = producer_allowance_room_refusal(queue, action)
    if producer_room_refusal is not None:
        raise SystemExit(producer_room_refusal)
    if capability_verdict is None:
        if intent.get("container_images"):
            # No worker has announced at all, so no inventory exists to place
            # an image-pinned action against.  Submitting unchecked would put
            # the action back in the exact position #714 describes: any old
            # loop that matches the tags could claim it and fail inside the
            # container.  Refused.
            raise SystemExit(
                "pbrun: no worker offers on record, and this action declares "
                "container image "
                + ", ".join(intent["container_images"])
                + ".\n"
                "  An image requirement is checked against the claiming "
                "worker's local Docker inventory, and no worker has announced "
                "one. Start a worker whose loop offers "
                f"{contracts.CONTAINER_IMAGE_TAG}, load the image on a box that "
                "offers these tags, and resubmit."
            )
        print("pbrun: no worker offers on record; submitting unchecked",
              file=sys.stderr, flush=True)
    elif live_verdict is not True:
        print(
            "pbrun: no matching worker is live now; a recorded capable worker "
            "is between announcements or offline.  Submitting so --wait-s "
            f"{args.wait_s:g} owns how long to wait.",
            file=sys.stderr, flush=True,
        )

    ceiling_notice = timeout_ceiling_notice(
        queue, intent, requested=args.timeout_s if progress_policy is None else None)
    if ceiling_notice:
        print(ceiling_notice, file=sys.stderr, flush=True)


def interpreter_submission_verdict(queue, intent: Mapping[str, object]):
    """``("present"|"absent"|"unknown", detail)`` for the sealed path (#1266).

    One rule, asked where the caller is still watching: absent raises, naming
    the path; unknown publishes with a notice, because a first submission is
    unknown by construction and refusing it would deadlock the tool on its
    own first use -- the claim-time stat remains the guard on every box.
    """

    interpreter = str(intent.get("interpreter") or "")
    verdict = queue.interpreter_placement_verdict(
        intent, interpreter, max_age_s=RECORDED_OFFER_MAX_AGE_S)
    return verdict, interpreter


def interpreter_of(command: Sequence[str]) -> str | None:
    """The absolute interpreter a command names, when it names one (#1263).

    ``argv[0]`` that is an absolute path to a ``python*`` binary.  Nothing is
    guessed from a wrapper, a venv name or a tag: the item declares exactly
    the bytes it will exec, and the fleet answers for that path or not at
    all.  ``None`` keeps today's behaviour byte for byte.
    """

    if not command:
        return None
    first = str(command[0])
    if not first.startswith("/"):
        return None
    name = first.rsplit("/", 1)[-1]
    if not name.startswith("python"):
        return None
    return first


def publication_row(
    action: Mapping[str, object],
    *,
    args: argparse.Namespace,
    queue,
    max_attempts: int | None = None,
    retry_safe: bool | None = None,
    tested_repository: str | None = None,
) -> dict[str, object]:
    """The queue row that submits one sealed action.

    A queue row and the action it points at are two spellings of one
    submission, so every field that describes the work is read off the sealed
    body itself rather than off a template or a local the caller happens to
    still be holding.  That matters most for the owner: the lifecycle cleanup
    asks Docker for a label derived from it, so a row naming a different owner
    than the body would query a label nothing carries.  It matters for the
    rest because a decomposed child seals its own ``params``: reading the row
    off the body is what makes a child's row describe the child.

    Only the submitter's own handles -- priority, the attempt ceiling, retry
    safety -- come from ``args``, and they are exactly the fields no action
    body carries, because they say how hard to try rather than what to run.
    The tested-repository tag (#1565) is the one caller-supplied field that
    names the work: it arrives as an argument because it is frozen in the
    template, which the sealed body must not carry, and a caller that cannot
    name the tree it froze passes nothing.

    ``pbcampaign`` publishes decomposed children through this too.  The
    alternative is a second copy of the literal, which is how a row and a body
    come to disagree about one action.
    """

    params = action["params"]
    demand = params["demand"]
    variables = action["environment"]["variables"]
    row: dict[str, object] = {
        "action_key": str(action["action_key"]),
        "cas_root": str(SH / "cas"),
        "worker_script": str(RUNTIME_ROOT / "tools" / "prismabuild_worker.py"),
        "tags": params["placement"]["required_tags"],
        "needs_gpu": bool(demand.get("gpu")),
        "priority": args.priority,
        "resources": demand,
        # ``args`` for the submitter's own work; overridden only by a movement
        # node, whose idempotence is a property of the mover rather than of
        # what the submitter asked for (#603).
        "max_attempts": (args.max_attempts if max_attempts is None
                         else int(max_attempts)),
        "container_owner": str(variables[CONTAINER_OWNER_ENV]),
        "checkout_snapshot": params["checkout_snapshot"],
    }
    priority_reason = getattr(args, "priority_reason", None)
    if priority_reason is not None:
        parameters = inspect.signature(queue.publish).parameters
        if ("priority_reason" not in parameters
                and not any(p.kind is inspect.Parameter.VAR_KEYWORD
                            for p in parameters.values())):
            raise pool.PoolContractError(
                "loaded queue runtime does not support priority_reason; "
                "use a matching runtime or omit --priority-reason")
        row["priority_reason"] = pool.normalize_priority_reason(priority_reason)
    if tested_repository is not None:
        # The template's frozen answer for the tree this action seals (#1565).
        # Guarded like the annotation above: a loaded runtime that cannot
        # carry the tag refuses rather than silently untagging the submission.
        parameters = inspect.signature(queue.publish).parameters
        if ("tested_repository" not in parameters
                and not any(p.kind is inspect.Parameter.VAR_KEYWORD
                            for p in parameters.values())):
            raise pool.PoolContractError(
                "loaded queue runtime does not support tested_repository; "
                "use a matching runtime")
        row["tested_repository"] = pool.normalize_tested_repository(
            tested_repository)
    if params.get("container_images"):
        # Derived from the sealed body, never re-read from the caller: the row
        # describes the action, so the action's own params are the authority.
        row["container_images"] = list(params["container_images"])
    if params.get("interpreter"):
        row["interpreter"] = str(params["interpreter"])
    for field in ("local_dependencies", "dependency_queries"):
        if params.get(field):
            row[field] = dict(params[field])
    if params.get("gang"):
        row["gang"] = dict(params["gang"])
    if params.get("requires_files") and "requires_files" in (
            inspect.signature(queue.publish).parameters):
        # Derived from the sealed body like the interpreter (#1495), and
        # guarded like retry_safe: in a mixed runtime window the row simply
        # omits the field and the capability tag in the row's tags alone
        # still fences every worker that cannot read the requirement.
        row["requires_files"] = [
            {"path": str(entry["path"]), "sha256": str(entry["sha256"])}
            for entry in params["requires_files"]]
    # The repo checkout can advance just before the atomic runtime generation
    # rolls.  The previous PoolQueue already accepts the safety-critical bound,
    # so keep that mixed window usable; add the explanatory annotation once the
    # loaded runtime exposes it.  The sealed action params carry the full
    # contract in both cases.
    if "retry_safe" in inspect.signature(queue.publish).parameters:
        row["retry_safe"] = (args.retry_safe if retry_safe is None
                             else bool(retry_safe))
    if params.get("resident_set") is not None:
        row["resident_set"] = params["resident_set"]
    return row


#: The submitter's own handles a deferred submission freezes (#913): what
#: ``publication_row`` and ``residency_stage_rows`` read off ``args`` when the
#: release publishes the consumer.  Everything else about the action is in
#: its frozen template.
_DEFERRED_PUBLICATION_ARGS = (
    "priority", "priority_reason", "max_attempts", "retry_safe", "residency",
    "residency_tier",
    "residency_ram", "residency_share", "residency_mover_mem_gb",
    "residency_mover_readers",
    "residency_mover_max_attempts",
    # The reader's declaration (#909), which a deferred consumer's plan must
    # carry exactly as a direct submission's does.
    "residency_prefetch_depth_gib", "residency_read_mb_s",
    # D38 evidence the release checks again against the sealed key.
    "d38_receipt", "d38_exception",
)


def check_supersession_or_exit(q, old: str, *, new: str, new_kind: str) -> None:
    """Refuse now what :func:`file_supersession_or_exit` would refuse, writing nothing (#1585).

    For a submission that still has fallible work to do before its row can
    publish: the record is immutable, so one filed for a replacement that then
    refuses strands the old key at a successor that never existed.
    """

    try:
        action_edges.check_supersession(q, old, new=new, new_kind=new_kind)
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: --supersedes: {exc}") from None


def file_supersession_or_exit(q, old: str, *, new: str, new_kind: str) -> None:
    """File that ``new`` replaces ``old``, or refuse the submission (#913)."""

    try:
        action_edges.file_supersession(q, old, new=new, new_kind=new_kind)
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: --supersedes: {exc}") from None
    print(f"pbrun: {new[:12]} supersedes {old[:12]}; edges that name it "
          f"follow this submission", file=sys.stderr, flush=True)


def template_record(template: Mapping[str, object]) -> dict[str, object]:
    """A frozen template as JSON: everything but the CAS handle (#913).

    The CAS is the one live object a template carries; the release rebuilds
    it from the recorded root.  The marker root is a path, recorded as text.
    """

    record = {name: value for name, value in template.items() if name != "cas"}
    record["marker_root"] = str(template["marker_root"])
    try:
        return json.loads(json.dumps(record))
    except (TypeError, ValueError) as exc:
        raise SystemExit(
            f"pbrun: this submission cannot be deferred: its frozen template "
            f"is not plain data ({exc})") from None


def template_from_record(record: Mapping[str, object], *,
                         cas_root: str | Path) -> dict[str, object]:
    """The frozen template a deferred record carries, ready to seal."""

    template = dict(record)
    template["cas"] = pb.PrismaBuildCAS(Path(cas_root))
    template["marker_root"] = Path(str(record["marker_root"]))
    return template


def resolve_after_edges(q, cas_root: Path,
                        texts: Sequence[str]) -> list[dict[str, str]]:
    """Every ``--after`` edge, checked against what the queue has filed.

    An unknown producer refuses: a key needs a readable row or terminal
    record, a pending id a deferred record.  So does a producer that does
    not declare the template, or a template that is not write-only.
    """

    edges: list[dict[str, str]] = []
    for text in texts:
        try:
            edge = action_edges.parse_edge(text)
            kind = action_edges.producer_kind(q, edge["producer"])
            if kind == action_edges.PRODUCER_KEY:
                declared = action_edges.declared_template_id(
                    cas_root, edge["producer"])
                if declared != edge["template_id"]:
                    raise action_edges.ActionEdgeError(
                        f"the producer declares template {declared!r}, not "
                        f"{edge['template_id']!r}")
                action_edges.require_write_only_template(
                    q.root, edge["template_id"])
            else:
                deferred = action_edges.read_deferred(q.root, edge["producer"])
                assert deferred is not None
                produced = deferred["template"].get("produced_output_template")
                if (not isinstance(produced, Mapping)
                        or produced.get("template_id") != edge["template_id"]):
                    raise action_edges.ActionEdgeError(
                        "the deferred producer does not declare template "
                        f"{edge['template_id']!r}")
                if not produced.get("write_only"):
                    raise action_edges.ActionEdgeError(
                        f"template {edge['template_id']!r} is not write-only")
        except action_edges.ActionEdgeError as exc:
            raise SystemExit(f"pbrun: --after {text}: {exc}") from None
        edges.append({**edge, "kind": kind})
    return edges


def submit_deferred(prepared: Mapping[str, object],
                    args: argparse.Namespace, *, report=None):
    """File this submission for release once its producers succeed (#913).

    Everything ``prepare_submission`` checks has been checked: the checkout,
    the environment, the demand and the placement, which is announced and
    refused here exactly as for an ordinary submission.  What is left is
    what depends on the producer's bytes -- the data manifest, the key, the
    residency plan -- and the tiers loop's release does that.
    """

    report = report if report is not None else action_client.ClientReport()
    template = prepared["template"]
    d38_gate.refuse_deferred(args, template["params"])
    cas = template["cas"]
    q = pool.PoolQueue(SH / "pb-queue")
    edges = resolve_after_edges(q, Path(cas.root), args.after)
    try:
        action_edges.resolve_command(template["params"]["command"], "", "")
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: {exc}") from None
    require_releasable_template(template)
    static_input = None
    if args.data_manifest is not None:
        pb.load_data_manifest(args.data_manifest)
        static_input, _ = cas.ingest_input(
            args.data_manifest, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
        static, encoding = pb.read_data_manifest(cas.input_path(static_input))
        if encoding != "identity":
            raise SystemExit(
                "pbrun: a deferred submission's --data-manifest must be plain "
                "JSON: the release rewrites it with the committed batches")
        if static["schema"] == pb.DATA_MANIFEST_SCHEMA_V2:
            require_deferred_read_plan(static, edges, args=args)
        else:
            require_declared_origin_batches(
                static, transport=args.transport, queue_root=q.root)
            if (args.residency == "stage"
                    and not manifest_layout.manifest_phase_ranges(static)):
                raise SystemExit(
                    "pbrun: --residency stage needs the static --data-manifest "
                    "to declare its read order in phases; the release adds one "
                    "phase per committed batch after them")
    announce_placement(
        prepared["offer_queue"](), {"params": template["params"]}, args=args,
        cwd=prepared["cwd"], portable_checkout=prepared["portable_checkout"])
    body = action_edges.deferred_body(
        edges=edges, template=template_record(template), cas_root=cas.root,
        static_manifest=static_input,
        publication={name: getattr(args, name)
                     for name in _DEFERRED_PUBLICATION_ARGS})
    try:
        pending_id, path = action_edges.file_deferred(q.root, body)
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: cannot file the deferred submission: {exc}") from None
    if args.supersedes is not None:
        file_supersession_or_exit(q, args.supersedes, new=pending_id,
                                  new_kind=action_edges.PRODUCER_PENDING)
    producers = ", ".join(f"{edge['producer'][:12]}:{edge['template_id']}"
                          for edge in edges)
    print(f"pbrun: deferred {pending_id[:12]} until {producers} succeeds; the "
          f"tiers loop seals and publishes it then", file=sys.stderr, flush=True)
    if args.detach:
        code = report.finish(action_client.ClientDeferredSubmitted(pending_id, path,
            action_edges.published_path(q.root, pending_id)))
        print(sorted_json(action_client.reported_submission(report)), flush=True)
        return code
    # Returned, not run: ``main`` waits after the submission's used-
    # filesystem growth allowance is released (#1483).
    return functools.partial(await_release, q, pending_id, wait_s=args.wait_s,
                             report=report, cas_root=Path(cas.root))


def require_deferred_read_plan(static: Mapping[str, object],
                               edges: Sequence[Mapping[str, object]], *,
                               args: argparse.Namespace) -> None:
    """Refuse a deferred v2 read plan the release could not place (#946).

    The static plan names, under ``annotations.produced_output_slots``, the
    read phase each ``--after`` edge fills (`action_edges.after_slots`). The
    release puts the committed batches there, so what is checked here is
    everything a submission of the placed plan would be checked for that
    does not depend on those bytes: the published storage generation reads
    v2, every read phase is a progress phase in order, and with
    ``--residency stage`` the placed plan has a phase boundary to stage up
    to. That last holds unless the plan's first phase reads nothing and fills
    no slot, because a boundary at byte 0 ends no read.
    """

    if args.transport != "pool":
        raise SystemExit(
            f"pbrun: --after needs the pull queue; --transport {args.transport} "
            "has none")
    try:
        slots = action_edges.after_slots(static, edges)
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: --after: {exc}") from None
    require_deployed_read_plan_storage()
    require_linear_read_plan_progress(static, args.progress_policy)
    first = static["read_plan"]["phases"][0]
    if (args.residency == "stage" and not first["entry_indices"]
            and first["name"] not in {slot["phase"] for slot in slots}):
        raise SystemExit(
            f"pbrun: --residency stage: read phase {first['name']!r} opens the "
            "plan and reads nothing, so it ends no read to stage up to")


def require_releasable_template(template: Mapping[str, object]) -> None:
    """Refuse a deferred submission that no release could seal (#913).

    The tiers loop runs the published generation, and seals a deferred
    consumer into the generation its template was frozen under: that one,
    or a retained generation whose wrapper matches its receipt
    (``sealing_runtime``).  A template frozen by a ``pbrun`` outside the
    generation store, such as a development checkout's, is neither, and
    would be held until superseded.  Refuse it at submission instead.
    """

    prefix = action_edges.template_wrapper(template)
    try:
        published = (SH / "repo").resolve(strict=True)
    except OSError as exc:
        raise SystemExit(
            f"pbrun: --after: cannot read the published runtime: {exc}") from None
    if prefix == str(published / "tools"):
        return
    try:
        verify_retained_wrapper(prefix, where="--after")
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        raise SystemExit(
            f"pbrun: --after: no release could seal this submission ({exc}); "
            f"submit it with the published {SH / 'repo' / 'tools' / 'pbrun.py'}"
        ) from None


def await_release(q, pending_id: str, *, wait_s: float, report=None,
                  cas_root: Path | None = None) -> int:
    """Wait for a deferred submission's release, then for its action.

    The release record names the key and the generation the tiers loop
    published, which is what the ordinary wait is pinned to.  Waiting longer
    than ``wait_s`` in total exits ``GAVE_UP_EXIT``; the submission stays
    filed and is released whenever its producers succeed.
    """

    report = report if report is not None else action_client.ClientReport()
    deadline = time.monotonic() + max(0.0, float(wait_s))
    while True:
        try:
            published = action_edges.read_published(q.root, pending_id)
        except action_edges.ActionEdgeError as exc:
            print(f"pbrun: {exc}", file=sys.stderr, flush=True)
            return report.finish(action_client.ClientUnavailable(
                None, None, action_wait.WaitEnd.UNRETRIED))
        if published is not None:
            key = str(published["action_key"])
            generation = float(published['published_unix'])
            report.publish_identity(action_client.PoolClientIdentity(key, generation, q.root,
                cas_root if cas_root is not None else SH / 'cas', None,
                action_client.ClientArrival.SUBMITTED))
            print(f"pbrun: {pending_id[:12]} was released as {key[:12]}",
                  file=sys.stderr, flush=True)
            remaining = deadline - time.monotonic()
            # ``report_pool_outcome`` reads ``wait_s=0`` as "observe once", with the
            # full read budget.  A positive wait that ran out while the
            # release was being read is not that request: clamping it to 0
            # handed a caller past its deadline one more full read (#938).
            if wait_s > 0 and remaining <= 0:
                print(f"pbrun: the wait ended before {key[:12]}'s outcome was "
                      f"read; the action runs regardless. Run pbwait.py "
                      f"{key[:12]} to follow it", file=sys.stderr, flush=True)
                return report.finish(action_client.ClientPatienceExpired(key, generation))
            return action_client.report_pool_outcome(
                q, key, wait_s=max(0.0, remaining),
                generation=generation, report=report)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print(f"pbrun: {pending_id[:12]} is still deferred; it stays filed "
                  f"and is released when its producers succeed",
                  file=sys.stderr, flush=True)
            return report.finish(action_client.ClientDeferredTimeout(pending_id))
        time.sleep(min(action_wait.POLL_S, remaining))


def sealing_runtime(template: Mapping[str, object]) -> dict[str, str]:
    """The generation a deferred template is sealed into, proved present (#913).

    It is the template's own: a consumer frozen by ``pbrun --after`` runs
    under the generation that froze it, as an ordinary submission sealed
    just before a publish does.  When that is this process's own generation,
    the caller has already checked it is the published one.  Otherwise it
    must be a retained generation whose wrapper matches its receipt, the
    check ``--as-sealed-by`` makes.  Raises
    ``action_edges.RuntimeGenerationUnavailable`` when it is not, so that
    nothing is ever sealed into a different generation instead.
    """

    prefix = action_edges.template_wrapper(template)
    if prefix == str(CONTAINER_WRAPPER_DIR):
        root = RUNTIME_ROOT
    else:
        try:
            root = verify_retained_wrapper(
                prefix, where="deferred release").parent
        except (OSError, ValueError, pb.PrismaBuildError) as exc:
            raise action_edges.RuntimeGenerationUnavailable(
                f"runtime-generation-unavailable: {exc}") from None
    return {"root": str(root), "generation": action_edges.generation_name(root)}


def release_deferred(q, pending_id: str, record: Mapping[str, object], *,
                     producers: Sequence[Mapping[str, object]] = ()
                     ) -> dict[str, object]:
    """Seal and publish one deferred consumer whose producers succeeded (#913).

    ``producers`` holds each edge's resolved ``{key, nonce, template_id}``.
    The first release pins the batch refs, the manifest and the key in a
    first-writer record before anything is published.  A release that finds
    that record resumes from it: it never resolves the producers again, and
    ``producers`` is not read.  After that it is ``main``'s own path: the CAS
    request, the origin-consumer declarations (#914), the window and the
    row.  A key the queue already carries -- published before a crash kept
    the last step from being recorded -- is not published again; its
    generation is recorded instead.  Last, the release records that
    generation, which is what waiters and chains read.

    Returns the event to log.  Raises ``ActionEdgeError`` (or ``SystemExit``
    from pbrun's own refusals) when the consumer cannot be released yet.
    """

    from prismabuild import produced_output as produced_mod

    cas_root = Path(str(record["cas_root"]))
    template = template_from_record(record["template"], cas_root=cas_root)
    cas = template["cas"]
    pinned = action_edges.read_release(q.root, pending_id)
    if pinned is None:
        static = None
        if record["static_manifest"] is not None:
            static, _ = pb.read_data_manifest(
                cas.input_path(record["static_manifest"]))
        if static is not None and static["schema"] == pb.DATA_MANIFEST_SCHEMA_V2:
            # Each edge's batches go where the plan reads them (#946).
            manifest = action_edges.place_after_slots(q.root, static, producers)
        else:
            refs: list[dict[str, object]] = []
            for producer in producers:
                refs.extend(action_edges.committed_batch_refs(
                    q.root, producer_key=str(producer["key"]),
                    nonce=str(producer["nonce"]),
                    template_id=str(producer["template_id"])))
            try:
                batches = produced_mod.origin_batch_manifest(q.root, refs)
            except produced_mod.ProducedOutputError as exc:
                raise action_edges.ActionEdgeError(str(exc)) from None
            manifest = action_edges.merged_manifest(static, batches)
        runtime = sealing_runtime(template)
        manifest_input, _ = cas.ingest_bytes(
            pb._canonical_file_bytes(manifest),
            input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    else:
        manifest_input = dict(pinned["manifest_input"])
        manifest, _ = pb.read_data_manifest(cas.input_path(manifest_input))
    summary = {"input": manifest_input, "mount_prefix": manifest["mount_prefix"],
               "entry_count": manifest["entry_count"],
               "total_bytes": manifest["total_bytes"]}
    if manifest["schema"] == pb.DATA_MANIFEST_SCHEMA_V2:
        # As ``freeze_action_template`` seals a submitted v2 plan.
        summary["schema"] = pb.DATA_MANIFEST_SCHEMA_V2
        summary["read_bytes"] = manifest["read_plan"]["read_bytes"]
    command = action_edges.resolve_command(
        template["params"]["command"], cas.input_path(manifest_input),
        str(manifest_input["sha256"]))
    action = seal_action_from_template(
        template, command=command, extra_inputs=[manifest_input],
        extra_params={"data_manifest": summary})
    key = str(action["action_key"])
    all_refs = list((manifest["annotations"] or {}).get(
        _ORIGIN_BATCHES_ANNOTATION) or [])
    if pinned is None:
        pinned = action_edges.file_release(
            q.root, pending_id, action_key=key,
            producers=[dict(item) for item in producers], refs=all_refs,
            manifest_input=manifest_input, runtime=runtime)
        resumed = False
    else:
        resumed = True
    if str(pinned["action_key"]) != key:
        raise action_edges.ActionEdgeError(
            f"release-key-mismatch: the pinned release names "
            f"{str(pinned['action_key'])[:12]}, and this runtime seals "
            f"{key[:12]}")
    match RecordStore(q.root).standing(key):
        case Present(QueueRow(state, existing)):
            pass
        case Present(Moving()):
            state, existing = "moving", None
        case Absent():
            state, existing = "absent", None
        case Unknown():
            state, existing = "unknown", None
    if state == "absent":
        if resumed:
            # About to publish: the generation must still be there, and the
            # pinned batches must still be the ones the release pinned.
            # ``lstat`` only, as at submission, unless an origin's timestamps
            # alone moved and its content must be read (#1111).
            sealing_runtime(template)
            try:
                produced_mod.load_origin_batches(q.root, all_refs)
            except produced_mod.ProducedOutputError as exc:
                raise action_edges.ActionEdgeError(str(exc)) from None
        options = argparse.Namespace(**dict(record["publication"]))
        # D38 again, now that the key exists: a record an older client filed
        # carries no evidence for it, and nothing is published without it.
        d38_gate.require(options, action, cas=cas, queue_root=q.root,
                         transport="pool")
        cas.publish_action_request(action)
        sealed = {**template,
                  "params": {**template["params"], "data_manifest": summary},
                  "inputs": [*template["inputs"], manifest_input]}
        with submission_window(q, key, all_refs):
            declare_origin_consumers(q, all_refs, consumer_action_key=key)
            _queued, generation = publish_consumer_row(
                q, action, sealed, key=key, args=options, cas=cas, attach=True)
    elif state == "unknown" or existing is None or isinstance(
            existing.get("published_unix"), bool) or not isinstance(
            existing.get("published_unix"), (int, float)):
        raise action_edges.ActionEdgeError(
            f"release-row-unreadable: {key[:12]} reads {state}")
    else:
        # Published before a crash kept this from being recorded: that row
        # is the release's generation, whatever became of it since.
        generation = float(existing["published_unix"])
    if generation is None:
        raise action_edges.ActionEdgeError(
            f"release-generation-unknown: {key[:12]} was published with no "
            "generation stamp")
    action_edges.file_published(q.root, pending_id, action_key=key,
                                published_unix=float(generation))
    return {"event": action_edges.RELEASED_EVENT, "pending_id": pending_id,
            "action_key": key, "published_unix": float(generation),
            "resumed": resumed, "runtime": dict(pinned["runtime"]),
            "producers": [dict(item) for item in pinned["producers"]],
            "refs": all_refs, "manifest_sha256": str(manifest_input["sha256"])}


def publish_consumer_row(q, action: Mapping[str, object],
                         template: Mapping[str, object], *, key: str,
                         args: argparse.Namespace, cas,
                         attach: bool = False,
                         before_publish=None) -> tuple[object, float | None]:
    """Publish one sealed action's row, with its window when it stages.

    Returns ``(queued_path, generation)``; ``queued_path`` is ``None`` when
    the queue was already carrying the key and this call attached to it.
    ``main`` and the deferred release (#913) are its two callers, so a
    released consumer is published exactly as a submitted one is.
    ``attach`` is the release's: a staged row it finds already live is the
    one it published before a crash, and it waits on that row instead of
    refusing.

    Everything the window will ever publish is sealed and written down before
    the consumer's own row goes in, so a crash between the two leaves a
    frozen plan and no queue rows rather than a half-published window.

    ``before_publish`` is called, with nothing fallible left but the publish
    itself, immediately before the row goes in.  A submission that supersedes
    an ended key files its immutable record there (#1585): every refusal the
    window's preparation can raise -- the stage tier, the phase table, the
    seal, the row's own checks -- comes first, so none can strand a record at
    a replacement that never published.
    """

    staged = None
    if args.residency == "stage":
        # One ownership transaction, under the consumer's existing transition
        # lock: handoff, seal and the consumer's publication are indivisible.  A dead consumer's cleanup rereads an old failed or
        # withdrawn terminal every cycle, and between ``freeze`` and the
        # consumer's own row it would see a filed plan nobody owns and reap
        # it.  ``residency_stage_rows``, ``freeze``, ``reap`` and ``publish``
        # all take this same lock and nest inside it (#708 review).
        with q._transition_locked(key):
            staged = residency_stage_rows(
                template, consumer_action_key=key,
                tier=resolve_stage_tier(q, args.residency_tier),
                args=args, queue=q, cas=cas)
            # A seal is a new generation of this consumer's window, and so is
            # a resubmission that reuses its frozen plan.  The predecessor's
            # *visible* child cancellations -- an operator's withdrawal, or
            # the dead-consumer pass that stopped a dead consumer's movers --
            # do not cover it, but the window reads them as live and would
            # supersede it before its second phase ever published.  So the
            # plan is filed first, which is what makes this consumer's
            # interest visible to the dead-consumer pass, and then the
            # predecessor's markers are retired as evidence under this
            # consumer's lock and every mover's (#708 review, #1114).  A
            # cancellation filed after that is the new plan's own decision
            # and still supersedes it; automatic publication never retires
            # one.  A release re-attaching to its own live row (#913) renews
            # nothing: that window already owns its markers.
            renew = not (attach and staged.get("reused_frozen_plan"))
            try:
                renewal = residency_plan.seal_window(
                    q, staged["plan"], renew=renew)
            except residency_plan.ResidencyPlanError as exc:
                raise SystemExit(f"pbrun: {exc}") from None
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
