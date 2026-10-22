"""CAS-only recovery of a consumer's sealed submission (#1893)."""
from __future__ import annotations

from pathlib import Path

from . import core as pb, digest_primitives, immutable_files, submission
from .submission_types import FleetStore, SubmissionRequest, SubmissionRefused
from .submission_frontend import SUBMISSION_SCHEMA_V1
from .submission_timing import submission_phase, submission_phase_snapshot

SEALED_SCHEMA = "prismabuild.sealed.v1"
SUBMISSION_SCHEMA = SUBMISSION_SCHEMA_V1
CONTEXT_SCHEMA = "prismabuild.sealed_publication.v1"

# These publication choices are deliberately outside action identity. All
# execution policy is recovered from the verified action, never this context.
PUBLICATION_OPTIONS = (
    "transport", "priority", "priority_reason", "supersedes",
    "here", "anywhere",
    "d38_receipt", "d38_exception", "residency", "residency_tier",
    "residency_ram", "residency_share", "residency_mover_mem_gb",
    "residency_mover_readers", "residency_prefetch_depth_gib",
    "residency_read_mb_s", "residency_mover_max_attempts",
)


class SealedSubmissionRefused(SubmissionRefused):
    def __init__(self, code: str, reason: str):
        self.code = code
        super().__init__(reason)


def sealed_context_path(store: FleetStore, key: str) -> Path:
    return store.cas_root / "sealed-publications" / "v1" / key[:2] / f"{key}.json"


def seal_consumer_request(request: SubmissionRequest, *, store: FleetStore, display):
    """Prepare once, retain publication choices, and publish no runnable work."""
    if request.after:
        raise SealedSubmissionRefused("deferred_seal_unsupported",
            "--seal-only cannot seal --after work before its dependencies release")
    with submission.preparation(request, store=store, display=display) as prepared:
        template = prepared["template"]
        action = submission.seal(template)
        template["cas"].publish_action_request(action)
        request_sha = digest_primitives.raw_sha256(pb._canonical_file_bytes(action))
        context = dict(schema=CONTEXT_SCHEMA, action_key=action["action_key"],
            request_sha256=request_sha,
            options={name: getattr(request, name) for name in PUBLICATION_OPTIONS},
            tested_repository=template.get("tested_repository"),
            checkout_identity=template["checkout_identity"])
        context_path = sealed_context_path(store, action["action_key"])
        raw = pb._canonical_file_bytes(context)
        with submission_phase("cas_write"):
            immutable_files.publish_immutable(context_path, raw)
            if immutable_files.read_immutable_file(context_path, where="sealed publication") != raw:
                raise SealedSubmissionRefused("sealed_publication_conflict",
                    "this key already retains different publication options; "
                    "reuse the original options")
    return dict(schema=SEALED_SCHEMA, action_key=action["action_key"],
        request_sha256=request_sha, transport=request.transport, phases=submission_phase_snapshot())


@submission_phase("sealed_request_read")
def load_consumer_seal(key: str, *, store: FleetStore, detach: bool, wait_s: float,
                       transport: str):
    """Verify the request and every declared input without inspecting a checkout."""
    path = store.cas_root / "requests" / key[:2] / f"{key}.json"
    try:
        raw = immutable_files.read_immutable_file(path, where="sealed action request")
    except FileNotFoundError:
        raise SealedSubmissionRefused("missing_sealed_request",
            f"no sealed request for {key}") from None
    except (OSError, pb.PrismaBuildError) as exc:
        raise SealedSubmissionRefused("invalid_sealed_request", str(exc)) from None
    try:
        action = pb.validate_action(digest_primitives.decode_strict_json(
            raw, where="sealed action request"))
        if action["action_key"] != key or raw != pb._canonical_file_bytes(action):
            raise ValueError("sealed request key or canonical bytes differ")
        if action["task"]["definition_id"] != "fleet/pbrun":
            raise ValueError("sealed request is not a pbrun command")
        params = action["params"]
        snapshot = pb.validate_pbrun_checkout_snapshot(params.get("checkout_snapshot"))
        if snapshot["input"] not in action["inputs"]:
            raise ValueError("sealed snapshot is absent from the action inputs")
    except (KeyError, TypeError, ValueError, pb.PrismaBuildError) as exc:
        raise SealedSubmissionRefused("invalid_sealed_request", str(exc)) from None
    cas = pb.PrismaBuildCAS(store.cas_root)
    with submission_phase("cas_input_verify"):
        for entry in action["inputs"]:
            try:
                cas.input_path(entry)
            except (OSError, pb.PrismaBuildError) as exc:
                raise SealedSubmissionRefused("sealed_input_unavailable",
                    f"sealed input {entry['id']} ({entry['sha256']}) unavailable: {exc}") from None
    request_sha = digest_primitives.raw_sha256(raw)
    context_path = sealed_context_path(store, key)
    try:
        context_raw = immutable_files.read_immutable_file(context_path, where="sealed publication")
    except FileNotFoundError:
        # Older CAS requests have no publication context. Their sealed policy
        # remains authoritative; queue hints and routing use current defaults.
        options, repository, checkout_identity = {}, None, None
    else:
        try:
            context = digest_primitives.decode_strict_json(context_raw, where="sealed publication")
            if (context["schema"] != CONTEXT_SCHEMA or context["action_key"] != key
                    or context["request_sha256"] != request_sha
                    or set(context["options"]) != set(PUBLICATION_OPTIONS)):
                raise ValueError("publication context does not bind this sealed request")
            options, repository = context["options"], context["tested_repository"]
            checkout_identity = context["checkout_identity"]
        except (KeyError, TypeError, ValueError) as exc:
            raise SealedSubmissionRefused("invalid_sealed_publication", str(exc)) from None
    retry = params["retry_policy"]
    request = submission.request_from_mapping(options,
        cwd=str(store.root), detach=detach, wait_s=wait_s,
        transport=options.get("transport", transport),
        command=params["command"], max_attempts=retry["max_attempts"],
        retry_safe=retry["retry_safe"], retry_scope=retry.get("retry_scope"),
        timeout_s=params.get("execution_timeout_s"),
        progress_policy=params.get(pb.PROGRESS_PARAM),
        awaited_batch=params.get(pb.AWAITED_BATCH_PARAM),
        exclusive=params.get("gpu_exclusive", False),
        host_class=action["execution_scope"].get("host_class"))
    submission.validate_request(request)
    if detach and request.max_attempts > 1:
        raise SubmissionRefused(submission.detached_attempts_refusal(request.max_attempts))
    template = {**action, "cas": cas, "tested_repository": repository,
        "marker_root": Path(action["environment"]["variables"][submission.CONTAINER_MARKER_ENV]).parent,
        "checkout_identity": checkout_identity}
    manifest = params.get("data_manifest")
    if manifest:
        value, _ = submission.manifest_codec.read_data_manifest(cas.input_path(manifest["input"]))
        submission.require_declared_origin_batches(value, transport=request.transport,
            queue_root=store.queue_root)
        refs = (value.get("annotations") or {}).get(submission._ORIGIN_BATCHES_ANNOTATION)
        if refs:
            template[submission._ORIGIN_BATCHES_TEMPLATE_KEY] = refs
    declaration = params.get(pb.PRODUCED_OUTPUT_TEMPLATE_PARAM)
    if declaration:
        entry = next(item for item in action["inputs"]
            if item["id"] == pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
        template["produced_output_template"] = submission.oc.validate_template(
            digest_primitives.decode_strict_json(cas.input_path(entry).read_bytes(),
                where="sealed produced-output template"))
    return action, template, request, path


def publish_consumer_seal(key: str, *, store: FleetStore, detach: bool, wait_s: float,
                          transport: str, display, report):
    action, template, request, path = load_consumer_seal(key, store=store,
        detach=detach, wait_s=wait_s, transport=transport)
    with submission_phase("sealed_request_read"):
        submission.require_reachable_runtime(action["params"]["placement"]["required_tags"],
            hostname=submission.socket.gethostname(), runtime_root=store.runtime_root)
    return submission.submit_action(action, template=template, request=request,
        store=store, request_path=path, display=display, report=report)
