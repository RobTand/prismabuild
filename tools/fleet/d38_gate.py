    kind, interpreter, target = (entry.get("kind"), entry.get("interpreter"),
                                 entry.get("target"))
    if not isinstance(kind, str) or kind not in ("script", "module") or not all(
            isinstance(item, str) and item for item in (interpreter, target)):
        raise Refusal("the plan's entry point is not a script or module "
                      "invocation")
    if target.startswith("-"):  # type: ignore[union-attr]
        raise Refusal("the entry point is an option, not a program: an opaque "
                      "launcher or shell needs a reviewed invocation "
                      "descriptor, or a CEO exception")
    head = ([interpreter, target] if kind == "script"
            else [interpreter, "-m", target])
    if list(command[:len(head)]) != head:
        raise Refusal("a command does not run the entry point the plan names")
    return list(command[len(head):])


def check_plan_binds(plan: Mapping[str, object], preflight: Mapping[str, object], *,
                     job: str, images: Sequence[str], namespace: str,
                     target_command: Sequence[str]) -> None:
    """The plan names this target, and the CPU run is this target's run.

    The entry point is identified by the plan (a script or module) and must be
    the one both commands run.  A reviewed descriptor (:data:`INVOCATIONS`) lists
    the CPU changes that entry point allows, and the CPU arguments must equal the
    target's arguments with exactly the plan's listed changes applied: a changed
    program, module, input or check argument is a difference no descriptor lists.
    """

    target = plan.get("target")
    if not isinstance(target, Mapping):
        raise Refusal("the plan names no target")
    if not isinstance(target.get("job_identity_hash"), str):
        raise Refusal("the plan job_identity_hash is not a string")
    planned_images = target.get("image_digest")
    if not isinstance(planned_images, list) or any(
            not isinstance(item, str) for item in planned_images):
        raise Refusal("the plan image_digest is not a list of strings")
    if not isinstance(target.get("namespace"), str):
        raise Refusal("the plan namespace is not a string")
    if target.get("job_identity_hash") != job:
        raise Refusal("the plan binds a different job identity")
    if target.get("image_digest") != list(images):
        raise Refusal("the plan binds different container images")
    if target.get("namespace") != namespace:
        raise Refusal("the plan binds a different namespace")
    entry = plan.get("entry")
    if not isinstance(entry, Mapping):
        raise Refusal("the plan names no entry point")
    planned_target = _plan_token_list(plan.get("target_command"), "target_command")
    cpu = _plan_token_list(plan.get("cpu_command"), "cpu_command")
    if planned_target != list(target_command):
        raise Refusal("the plan's target command is not this job's command")
    if cpu != list(preflight["params"]["command"]):  # type: ignore[index]
        raise Refusal("the plan's CPU command is not the preflight action's command")
    target_args = _split_invocation(planned_target, entry)
    cpu_args = _split_invocation(cpu, entry)
    key = f"{entry['kind']}:{entry['interpreter']}:{entry['target']}"
    descriptor = INVOCATIONS.get(key)
    allowed = descriptor.get("cpu_changes") if isinstance(descriptor, Mapping) else None
    if not isinstance(allowed, list):
        raise Refusal(f"no reviewed invocation descriptor exists for the entry "
                      f"point {key}; a CEO exception is the only path until one "
                      "is added")
    changes = plan.get("changes")
    if not isinstance(changes, list):
        raise Refusal("the plan's changes are not a list")
    expected = list(target_args)
    for change in changes:
        if (not isinstance(change, Mapping)
                or set(change) != {"flag", "from", "to"}
                or any(not isinstance(change[name], str)
                       for name in ("flag", "from", "to"))
                or dict(change) not in allowed):
            raise Refusal("the plan lists a CPU change the reviewed descriptor "
                          "does not allow")
        at = [i for i in range(len(expected) - 1)
              if expected[i] == change["flag"] and expected[i + 1] == change["from"]]
        if len(at) != 1:
            raise Refusal("a listed CPU change does not occur exactly once in "
                          "the target's arguments")
        expected[at[0] + 1] = change["to"]
    if expected != cpu_args:
        raise Refusal("the CPU command differs from the target command by more "
                      "than the plan's reviewed CPU changes")


def check_cpu_visibility(preflight: Mapping[str, object], host_class: str) -> None:
    """The proved namespace hides every GPU, read from what the action sealed.

    The host's accelerator inventory is not task visibility: a CPU-only
    container on a Spark still has the Spark's GPU in its host evidence.  What
    proves the run was CPU-only is the environment the preflight action sealed.
    """

    variables = (preflight.get("environment") or {}).get("variables") or {}  # type: ignore[union-attr]
    if variables.get("CUDA_VISIBLE_DEVICES") != "":
        raise Refusal("the preflight action does not seal CUDA_VISIBLE_DEVICES "
                      "empty, so it did not hide the GPU")
    if host_class == "gb10" and variables.get("NVIDIA_VISIBLE_DEVICES") != "none":
        raise Refusal("a gb10 preflight must seal NVIDIA_VISIBLE_DEVICES=none")


def check_host_class(receipt: Mapping[str, object], host_class: str) -> None:
    """The producer ran on the host class the receipt says."""

    producer = receipt.get("producer")
    evidence = producer.get("evidence") if isinstance(producer, Mapping) else None
    if not isinstance(evidence, Mapping):
        raise Refusal("the receipt carries no producer host evidence")
    if str(evidence.get("machine")) not in _MACHINES[host_class]:
        raise Refusal(f"the producer ran on {evidence.get('machine')!r}, not on "
                      f"a {host_class} host")


def verify_receipt(cas, key: str, *, job: str, images: Sequence[str],
                   namespace: str, target_command: Sequence[str],
                   at: datetime.datetime) -> dict:
    """Prove that one preflight receipt binds this job, or raise Refusal."""

    preflight, receipt = _verified_receipt(cas, key)
    try:
        # What the worker executes is task.argv, not the descriptive
        # params.command: prove the first is the recipe of the second.
        action_result.bind_standard_capture_command(preflight)
    except action_result.ActionResultError as exc:
        raise Refusal(f"the preflight task does not execute its declared "
                      f"command: {exc}") from None
    plan = load_plan(cas, preflight)
    check_plan_binds(plan, preflight, job=job, images=images,
                     namespace=namespace, target_command=target_command)
    result = receipt.get("result")
    if (not isinstance(result, Mapping)
            or type(result.get("bytes")) is not int
            or not 0 <= result["bytes"] <= MAX_RECEIPT_BYTES):
        raise Refusal("the receipt result is missing or larger than 64 KiB")
    if not isinstance(result.get("sha256"), str) or not _HASH.match(result["sha256"]):
        raise Refusal("the receipt result sha256 is not a 64-character hash")
    try:
        raw = Path(cas.blob_path(result["sha256"])).read_bytes()
    except Exception as exc:                                    # noqa: BLE001
        raise Refusal(f"the receipt result cannot be read: {exc}") from None
    body = _parse_receipt(raw)
    if body["preflight_action_key"] != key:
        raise Refusal("the receipt names a different preflight action")
    if body["job_identity_hash"] != job:
        raise Refusal("the receipt binds a different job identity")
    if body["image_digest"] != list(images):
        raise Refusal("the receipt binds different container images")
    if body["namespace"] != namespace:
        raise Refusal("the receipt binds a different namespace")
    if body["result"] != "pass":
        raise Refusal("the preflight did not pass")
    created = _aware(body["created_at"], "created_at")
    if created > at:
        raise Refusal("the receipt is dated in the future")
    if body["expires"] is not None and not at < _aware(body["expires"], "expires"):
        raise Refusal("the receipt has expired")
    check_cpu_visibility(preflight, body["host_class"])
    check_host_class(receipt, body["host_class"])
    return {"authorization": "receipt", "preflight_action_key": key,
            "receipt_sha256": receipt["receipt_sha256"],
            "host_class": body["host_class"]}


# --------------------------------------------------------- exception grants

def verify_exception(decision_id: str, *, job: str, images: Sequence[str],
                     namespace: str, at: datetime.datetime) -> dict:
    """Prove that a CEO decision grants D38 for exactly this job."""

    if not isinstance(decision_id, str) or not _DECISION_ID.match(decision_id):
        raise Refusal("--d38-exception is not a decision id")
    path = Path(DECISION_DIR) / f"{decision_id}.json"
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise Refusal(f"the decision cannot be read: {exc}") from None
    try:
        decision = _json_without_duplicates(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise Refusal(f"the decision is not valid JSON: {exc}") from None
    if not isinstance(decision, dict):
        raise Refusal("the decision is not a JSON object")
    if any(not isinstance(decision.get(name), str)
           for name in ("kind", "state", "verdict")):
        raise Refusal("the decision kind, state and verdict must be strings")
    if (decision.get("kind") != "decision" or decision.get("state") != "decided"
            or decision.get("verdict") != "approve"):
        raise Refusal("the decision is not a decided approval")
    grant = decision.get("grant")
    scoped = grant.get("d38_exception") if isinstance(grant, dict) else None
    if not isinstance(scoped, dict):
        raise Refusal("the decision carries no structured d38_exception grant")
    if not isinstance(scoped.get("job_identity_hash"), str):
        raise Refusal("the grant job_identity_hash is not a string")
    if scoped.get("job_identity_hash") != job:
        raise Refusal("the grant binds a different job identity")
    granted = scoped.get("image_digest")
    if not isinstance(granted, list) or any(not isinstance(item, str) for item in granted):
        raise Refusal("the grant image_digest is not a list of strings")
    if sorted(granted) != list(images):
        raise Refusal("the grant binds different container images")
    if not isinstance(scoped.get("namespace"), str):
        raise Refusal("the grant namespace is not a string")
    if scoped.get("namespace") != namespace:
        raise Refusal("the grant binds a different namespace")
    if not at < _aware(scoped.get("expires"), "grant expires"):
        raise Refusal("the grant has expired")
    return {"authorization": "exception", "decision_id": decision_id,
            "decision_sha256": digest_primitives.raw_sha256(raw)}


# -------------------------------------------------------------------- audit

def write_audit(queue_root: str | Path, event: Mapping[str, object]) -> Path:
    """File one immutable audit event; raise OSError when it cannot be written."""

    directory = Path(queue_root) / AUDIT_DIR_NAME / str(event["job_identity_hash"])
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{int(utc_now().timestamp() * 1e6):016d}-{uuid.uuid4().hex}.json"
    payload = (digest_primitives.sorted_json(dict(event)) + "\n").encode()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


# --------------------------------------------------------------------- gate

def _exit_refused(reason: str, *, job: str, images: Sequence[str],
            namespace: str | None) -> "SystemExit":
    print(f"pbrun: D38 refuses GPU publication: {reason}.\n"
          f"job={job} images={json.dumps(list(images))} "
          f"namespace={namespace or 'none'}\n"
          "Run the same entry point on CPU through PrismaBuild. "
          "Supply --d38-receipt <action key>.\n"
          "Use --d38-exception <decision id> only for an explicit CEO "
          "exception. No runnable submission was published.",
          file=sys.stderr, flush=True)
    return SystemExit(2)


def authorize(args, action: Mapping[str, object], *, cas,
              at: datetime.datetime) -> dict:
    """The evidence that authorizes this job, or raise Refusal."""

    params = action["params"]
    namespace = params.get(NAMESPACE_PARAM)  # type: ignore[union-attr]
    job = str(action["action_key"])
    images = target_images(action)
    receipt = getattr(args, "d38_receipt", None)
    exception = getattr(args, "d38_exception", None)
    if receipt and exception:
        raise Refusal("supply --d38-receipt or --d38-exception, not both")
    if not isinstance(namespace, str) or not _NAMESPACE.match(namespace):
        raise Refusal("the job declares no --d38-namespace descriptor, so no "
                      "receipt can bind its namespace")
    if receipt:
        return verify_receipt(
            cas, receipt, job=job, images=images, namespace=namespace,
            target_command=params["command"], at=at)  # type: ignore[index]
    if exception:
        return verify_exception(exception, job=job, images=images,
                                namespace=namespace, at=at)
    raise Refusal("no preflight receipt was supplied")


def evaluate(args, action: Mapping[str, object], *, cas,
             at: datetime.datetime) -> dict:
    """Return authorized evidence or a failed check, independently of policy."""

    try:
        evidence = authorize(args, action, cas=cas, at=at)
    except Refusal as exc:
        return {"result": "failed", "reason": str(exc)}
    return {"result": "authorized", **evidence}


def judge_publication(args, action: Mapping[str, object], *, cas, queue_root: str | Path,
           transport: str) -> tuple[str, str | None] | None:
    """Evaluate GPU evidence and audit it under the reviewed enforcement policy.

    Return ``(reason, namespace)`` only when enforcement refuses publication.
    The caller owns cache and attachment handling and must never publish on
    a refusal.
    """

    params = action["params"]
    demand = params.get("demand") or {}  # type: ignore[union-attr]
    tags = (params.get("placement") or {}).get("required_tags") or []  # type: ignore[union-attr]
    if not requires_receipt(demand, tags,
                            host_class=getattr(args, "host_class", None)):
        return None
    enforce = enforcement_enabled()
    job = str(action["action_key"])
    images = target_images(action)
    namespace = params.get(NAMESPACE_PARAM)  # type: ignore[union-attr]
    at = utc_now()
    check = evaluate(args, action, cas=cas, at=at)
    checked_namespace = namespace if isinstance(namespace, str) else None
    if enforce and check["result"] == "failed":
        return (check["reason"], checked_namespace)
    event = {
        "schema": AUDIT_SCHEMA, "job_identity_hash": job,
        "image_digest": images, "namespace": namespace,
        "submitter": f"{_user()}@{socket.gethostname()}",
        "time": at.isoformat(), "transport": transport,
        "mode": "enforce" if enforce else "record_only", **check}
    try:
        write_audit(queue_root, event)
    except OSError as exc:
        reason = f"the audit event cannot be written: {exc}"
        if enforce:
            return (reason, checked_namespace)
        print(f"pbrun: D38 record_only: {reason}; publication permitted",
              file=sys.stderr, flush=True)
    if check.get("authorization") == "exception":
        print(f"pbrun: D38 exception {check['decision_id']} authorizes job "
              f"{job}", file=sys.stderr, flush=True)
    return None


def refusal_exit(verdict: tuple[str, str | None], action: Mapping[str, object]
            ) -> SystemExit:
    """The exit-2 refusal for a publication verdict."""

    reason, namespace = verdict
    return _exit_refused(reason, job=str(action["action_key"]),
                   images=target_images(action), namespace=namespace)


def require(args, action: Mapping[str, object], *, cas, queue_root: str | Path,
            transport: str) -> None:
    """Apply the reviewed D38 policy to a new publication."""

    verdict = judge_publication(args, action, cas=cas, queue_root=queue_root,
                     transport=transport)
    if verdict is not None:
        raise refusal_exit(verdict, action)


def refuse_deferred(args, params: Mapping[str, object]) -> None:
    """Enforced GPU intent cannot register before a sealed identity exists.

    Record-only registration defers evaluation and auditing to release.
    """
    demand = params.get("demand") or {}
    tags = (params.get("placement") or {}).get("required_tags") or []  # type: ignore[union-attr]
    if not requires_receipt(demand, tags,  # type: ignore[arg-type]
                            host_class=getattr(args, "host_class", None)):
        return
    if not enforcement_enabled():
        return
    images = list(container_images.normalize_refs(
        params.get("container_images") or []))
    namespace = params.get(NAMESPACE_PARAM)
    raise _exit_refused(
        "a deferred --after submission has no job identity until its producer "
        "ends, so no receipt or grant can bind it; submit it after the "
        "producer succeeds", job="none (deferred)", images=images,
        namespace=namespace if isinstance(namespace, str) else None)


def _user() -> str:
    try:
        return getpass.getuser()
    except Exception:                                           # noqa: BLE001
        return "unknown"
