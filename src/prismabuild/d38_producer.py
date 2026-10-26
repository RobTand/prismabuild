"""Source-owned D38 plans and CPU receipt-producing actions."""
from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path, PurePosixPath
from collections.abc import Mapping, Sequence

from . import action_result, core as pb, d38_gate, digest_primitives, pool, submission
from .submission_types import DisplaySink, FleetStore, SubmissionRefused, SubmissionRequest, discard_display

SOURCE = "tools/fleet/pbcanary_legs/leg2.py"
ENTRY = {"kind": "script", "interpreter": "/usr/bin/python3", "interpreter_args": ["-I"], "target": SOURCE}
ENTRY_KEY = "script:/usr/bin/python3:" + SOURCE
CHECKS_PATH = ".d38/leg2-checks.json"
RESULT_PATH = ".d38/receipt.json"
LOG_PATH = ".d38-preflight.log"
DRIVER_INPUT = "d38-driver"
DIGEST_INPUT = "d38-digests"
DIGEST_SOURCE = "src/prismabuild/digest_primitives.py"
DRIVER_PYTHON = "/usr/bin/python3"
CHECKS = ("imports", "arguments", "payload_digest", "image_digest", "shape", "kernel_value", "input_reads", "device_visibility")
CPU_CHANGE = {"flag": "--device", "from": "cuda", "to": "cpu"}
CPU_REASON = "The production canary image is ARM64; its CPU invocation requires a GB10 host."


def _source_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _owned_tool(relative: str) -> Path:
    root = _source_root()
    for path in (root / relative, root / relative.replace("tools/fleet/", "tools/", 1)):
        if path.is_file():
            return path
    raise FileNotFoundError(f"reviewed runtime tool is unavailable: {relative}")


def driver_path() -> Path:
    return Path(__file__).with_name("d38_preflight_driver.py")


def invocation(command: Sequence[str]) -> dict:
    """Parse the single reviewed canary argument form without shell inference."""
    words = list(command)
    if len(words) < 9 or words[:3] != ["/usr/bin/python3", "-I", SOURCE]:
        raise d38_gate.Refusal("the D38 producer supports the reviewed canary leg-2 script invocation only")
    if words[3] != "--image" or words[5:7] != ["--device", "cuda"] or words[7:9] != ["--checks-path", CHECKS_PATH]:
        raise d38_gate.Refusal("the canary invocation does not match its reviewed argument form")
    image = words[4]
    import re
    if re.fullmatch(r".+@sha256:[0-9a-f]{64}", image) is None:
        raise d38_gate.Refusal("the canary image is not digest-pinned")
    inputs = []
    for at in range(9, len(words), 2):
        if at + 1 >= len(words) or words[at] != "--input":
            raise d38_gate.Refusal("the canary invocation has an unknown argument")
        parts = words[at + 1].split("=")
        if len(parts) != 3:
            raise d38_gate.Refusal("a canary input must be ID=PATH=SHA256")
        identity, path, sha = parts
        logical = PurePosixPath(path)
        if (re.fullmatch(r"[A-Za-z0-9_.-]+", identity) is None or any(item["id"] == identity for item in inputs)
                or logical.is_absolute() or ".." in logical.parts or str(logical) != path
                or path in ("", ".") or "," in path or re.fullmatch(r"[0-9a-f]{64}", sha) is None):
            raise d38_gate.Refusal("a canary input identity, relative path or digest is invalid")
        inputs.append({"id": identity, "path": path, "sha256": sha})
    return {"image": image, "inputs": inputs}


def namespace_for(command: Sequence[str]) -> dict:
    contract = invocation(command)
    mounts = [
        {"source": SOURCE, "target": "/pb-canary/leg2.py", "options": ["ro"]},
        {"source": DIGEST_SOURCE, "target": "/pb-canary/digest_primitives.py", "options": ["ro"]},
        {"source": ".d38", "target": "/pb-checks", "options": ["rw"]},
    ]
    mounts.extend({"source": item["path"], "target": f"/pb-inputs/{index}", "options": ["ro"]}
                  for index, item in enumerate(contract["inputs"]))
    return {"mode": "container", "cwd": "/pb-canary", "interpreter": "python3", "interpreter_args": ["-I"],
            "entrypoint": "", "images": [contract["image"]], "mounts": mounts,
            "prerequisites": contract["inputs"]}


def descriptor() -> dict:
    return {"source": SOURCE,
            "source_sha256": digest_primitives.raw_sha256(_owned_tool(SOURCE).read_bytes()),
            "docker_sha256": digest_primitives.raw_sha256(_owned_tool("tools/fleet/docker").read_bytes()),
            "digest_source": DIGEST_SOURCE,
            "digests_sha256": digest_primitives.raw_sha256(Path(digest_primitives.__file__).read_bytes()),
            "cpu_changes": [dict(CPU_CHANGE)], "host_class": "gb10", "d29_reason": CPU_REASON,
            "checks_path": CHECKS_PATH,
            "completion": {"schema": "fleet.d38.leg2_checks.v1",
                "checks": dict.fromkeys(CHECKS, "passed"), "device": "cpu",
                "payload_sha256": digest_primitives.raw_sha256(b"pbcanary-leg2:v1"),
                "shape": [16], "result_shape": [], "kernel_value": 1240}}


def validate_d38_plan(plan: Mapping) -> dict:
    """Validate a complete target and its one reviewed CPU transformation."""
    try:
        if plan.get("schema") != d38_gate.PLAN_SCHEMA:
            raise d38_gate.Refusal("the plan schema is not fleet.d38.plan.v1")
        target = pb.validate_action(plan["target_action"])
        action_result.bind_standard_capture_command(target)
        variables = target["environment"]["variables"]
        overrides = sorted(name for name in variables
            if name in {"PRISMABUILD_DOCKER_TESTING", "BASH_ENV", "ENV"}
            or name.startswith("BASH_FUNC_") or name.startswith("LD_"))
        if overrides:
            raise d38_gate.Refusal("the reviewed canary forbids execution overrides: " + ", ".join(overrides))
        command = target["params"]["command"]
        contract = invocation(command)
        namespace = namespace_for(command)
        expected_target = {"job_identity_hash": target["action_key"],
                           "image_digest": d38_gate.target_images(target),
                           "namespace": "sha256:" + pb.canonical_sha256(namespace)}
        cpu_command = list(command)
        cpu_command[6] = "cpu"
        if (plan.get("target") != expected_target or plan.get("namespace_descriptor") != namespace
                or plan.get("entry") != ENTRY or plan.get("target_command") != command
                or plan.get("cpu_command") != cpu_command or plan.get("changes") != [CPU_CHANGE]
                or plan.get("descriptor") != descriptor()):
            raise d38_gate.Refusal("the plan does not bind the reviewed target, namespace, source and CPU invocation")
        if target["params"].get(d38_gate.NAMESPACE_PARAM) != expected_target["namespace"]:
            raise d38_gate.Refusal("the target does not seal the plan's namespace")
        if expected_target["image_digest"] != [contract["image"]]:
            raise d38_gate.Refusal("the target does not declare exactly the canary image")
        if target["params"]["demand"].get("gpu") != 1:
            raise d38_gate.Refusal("the reviewed canary target must demand exactly one GPU")
        if target["task"]["task_class"] != "generation" or target["params"].get("cwd") != ".":
            raise d38_gate.Refusal("the canary producer needs a generation target at its checkout root")
        snapshot = pb.validate_pbrun_checkout_snapshot(target["params"]["checkout_snapshot"])
        if target["inputs"] != [snapshot["input"]]:
            raise d38_gate.Refusal("the first canary invocation supports checkout-declared inputs only")
        if target["params"].get("data_manifest") or target["params"].get("gang") or target["params"].get("progress"):
            raise d38_gate.Refusal("the first canary invocation does not support manifests, gangs or progress policies")
        return target
    except (KeyError, TypeError, ValueError, pb.PrismaBuildError, action_result.ActionResultError) as exc:
        raise d38_gate.Refusal(f"the D38 plan is invalid: {exc}") from None


def read_plan(path: str | Path) -> tuple[dict, bytes]:
    with Path(path).open("rb") as handle:
        raw = handle.read(d38_gate.MAX_RECEIPT_BYTES + 1)
    if len(raw) > d38_gate.MAX_RECEIPT_BYTES:
        raise d38_gate.Refusal("the D38 plan is larger than 64 KiB")
    try:
        plan = d38_gate._json_without_duplicates(raw)
    except (ValueError, UnicodeError) as exc:
        raise d38_gate.Refusal(f"the D38 plan is not strict JSON: {exc}") from None
    if not isinstance(plan, dict):
        raise d38_gate.Refusal("the D38 plan is not an object")
    validate_d38_plan(plan)
    return plan, raw


def write_d38_plan(request: SubmissionRequest, path: str | Path, *, store: FleetStore,
               display: DisplaySink = discard_display) -> dict:
    """Seal a target plan without publishing an action request or runnable work."""
    if request.after or request.as_sealed_by or request.withdraw or request.release_origin_consumer:
        raise SubmissionRefused("pbrun: --d38-plan cannot combine with deferred or lifecycle operations")
    try:
        if not request.d38_namespace:
            raise d38_gate.Refusal("--d38-plan requires --d38-namespace")
        namespace, _ = d38_gate.load_namespace(request.d38_namespace)
        with submission.preparation(request, store=store, display=display) as prepared:
            target = submission.seal(prepared["template"])
            command = list(target["params"]["command"])
            cpu_command = list(command)
            if len(cpu_command) > 6:
                cpu_command[6] = "cpu"
            plan = {"schema": d38_gate.PLAN_SCHEMA,
                    "target_action": target,
                    "target": {"job_identity_hash": target["action_key"],
                               "image_digest": d38_gate.target_images(target),
                               "namespace": target["params"].get(d38_gate.NAMESPACE_PARAM)},
                    "namespace_descriptor": namespace, "entry": dict(ENTRY),
                    "target_command": command, "cpu_command": cpu_command,
                    "changes": [dict(CPU_CHANGE)], "descriptor": descriptor()}
            validate_d38_plan(plan)
            raw = pb._canonical_file_bytes(plan)
            if len(raw) > d38_gate.MAX_RECEIPT_BYTES:
                raise d38_gate.Refusal("the D38 plan is larger than 64 KiB")
            destination = Path(path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            import tempfile
            with tempfile.NamedTemporaryFile(dir=destination.parent,
                    prefix="." + destination.name + ".", delete=False) as handle:
                temporary = Path(handle.name)
                handle.write(raw)
            try:
                temporary.replace(destination)
            finally:
                temporary.unlink(missing_ok=True)
            return {"schema": d38_gate.PLAN_SCHEMA, "action_key": target["action_key"],
                    "plan_sha256": digest_primitives.raw_sha256(raw), "plan_path": str(destination)}
    except d38_gate.Refusal as exc:
        raise SubmissionRefused(f"pbrun: D38 plan refused: {exc}") from None


def _driver_input(cas) -> dict:
    entry, _ = cas.ingest_bytes(driver_path().read_bytes(), input_id=DRIVER_INPUT)
    return entry


def _cpu_template(plan, cas) -> dict:
    target = validate_d38_plan(plan)
    template = copy.deepcopy({key: value for key, value in target.items() if key != "action_key"})
    params = template["params"]
    params["demand"]["gpu"] = 0
    params["interpreter"] = DRIVER_PYTHON
    params.pop("gpu_exclusive", None)
    params.pop("gpu_memory_gb", None)
    params["placement"]["required_tags"] = pool.normalize_placement_tags([
        *params["placement"]["required_tags"], "gb10"])
    variables = template["environment"]["variables"]
    variables["CUDA_VISIBLE_DEVICES"] = ""
    variables["NVIDIA_VISIBLE_DEVICES"] = "none"
    template.update(cas=cas, log_name=LOG_PATH,
                    marker_root=Path(variables[submission.CONTAINER_MARKER_ENV]).parent,
                    checkout_identity={"d38_target": target["action_key"]}, tested_repository=None)
    return template


def driver_command(cas, plan_input: Mapping, driver_input: Mapping, digest_input: Mapping) -> list[str]:
    return [DRIVER_PYTHON, "-I", str(cas.input_path(driver_input)),
            "--plan", str(cas.input_path(plan_input)), "--plan-sha256", str(plan_input["sha256"]),
            "--result", RESULT_PATH, "--digests", str(cas.input_path(digest_input))]


def seal_preflight(plan: Mapping, plan_input: Mapping, driver_input: Mapping, digest_input: Mapping, *, cas) -> tuple[dict, dict]:
    template = _cpu_template(plan, cas)
    command = driver_command(cas, plan_input, driver_input, digest_input)
    action = submission.seal(template, command=command, result_path=RESULT_PATH,
        extra_inputs=[plan_input, driver_input, digest_input], extra_params={d38_gate.PRODUCER_PARAM: {
            "producer": d38_gate.PRODUCER_ID, "plan_sha256": plan_input["sha256"],
            "driver_sha256": driver_input["sha256"], "digests_sha256": digest_input["sha256"]}})
    return action, template


def prepare_preflight(path: str | Path, request: SubmissionRequest, *, store: FleetStore,
                      display: DisplaySink = discard_display) -> tuple[dict, dict, SubmissionRequest]:
    """Recover CPU work solely from the plan and immutable source-driver bytes."""
    try:
        plan, raw = read_plan(path)
        cas = pb.PrismaBuildCAS(store.cas_root)
        plan_input, _ = cas.ingest_bytes(raw, input_id=d38_gate.PLAN_INPUT_ID)
        digest_input, _ = cas.ingest_bytes(Path(digest_primitives.__file__).read_bytes(), input_id=DIGEST_INPUT)
        action, template = seal_preflight(plan, plan_input, _driver_input(cas), digest_input, cas=cas)
        params = action["params"]
        effective = replace(request, cwd=str(store.root), command=params["command"],
                            demand=",".join(f"{key}={value}" for key, value in params["demand"].items()),
                            cpus=params["demand"]["cpu"], tag=params["placement"]["required_tags"],
                            container_image=params.get("container_images", ()),
                            timeout_s=params.get("execution_timeout_s"),
                            retry_safe=params["retry_policy"]["retry_safe"],
                            max_attempts=params["retry_policy"]["max_attempts"],
                            retry_scope=params["retry_policy"].get("retry_scope"),
                            host_class=action["execution_scope"].get("host_class"))
        submission.validate_request(effective)
        return action, template, effective
    except d38_gate.Refusal as exc:
        raise SubmissionRefused(f"pbrun: D38 preflight refused: {exc}") from None


def bind_preflight(cas, action: Mapping, plan: Mapping) -> None:
    """Prove executed driver bytes and the complete CPU/target relationship."""
    validate_d38_plan(plan)
    plans = [entry for entry in action.get("inputs", ()) if entry.get("id") == d38_gate.PLAN_INPUT_ID]
    drivers = [entry for entry in action.get("inputs", ()) if entry.get("id") == DRIVER_INPUT]
    if len(plans) != 1 or len(drivers) != 1:
        raise d38_gate.Refusal("the preflight needs exactly one source driver and one plan input")
    driver = drivers[0]
    expected_driver = driver_path().read_bytes()
    if (driver.get("sha256") != digest_primitives.raw_sha256(expected_driver)
            or driver.get("bytes") != len(expected_driver)
            or cas.input_path(driver).read_bytes() != expected_driver):
        raise d38_gate.Refusal("the preflight driver is not the reviewed source driver")
    owners = [entry for entry in action.get("inputs", ()) if entry.get("id") == DIGEST_INPUT]
    expected_owner = Path(digest_primitives.__file__).read_bytes()
    if (len(owners) != 1 or owners[0].get("sha256") != digest_primitives.raw_sha256(expected_owner)
            or owners[0].get("bytes") != len(expected_owner)
            or cas.input_path(owners[0]).read_bytes() != expected_owner):
        raise d38_gate.Refusal("the preflight digest owner is not the reviewed source owner")
    expected, _ = seal_preflight(plan, plans[0], driver, owners[0], cas=cas)
    if dict(action) != expected:
        raise d38_gate.Refusal("the preflight task, inputs or execution context differ from the reviewed CPU recipe")


def validated_cpu_preflight(cas, action: Mapping) -> bool:
    declared = action.get("params", {}).get(d38_gate.PRODUCER_PARAM)
    if not isinstance(declared, Mapping) or "driver_sha256" not in declared:
        return False
    try:
        plan = d38_gate.load_plan(cas, action)
        bind_preflight(cas, action, plan)
    except (d38_gate.Refusal, OSError, ValueError, pb.PrismaBuildError):
        return False
    return True
