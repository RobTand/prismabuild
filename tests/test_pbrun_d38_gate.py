"""D38: pbrun refuses a GPU publication without a matching preflight receipt (#1639).

A new or changed GPU job needs a CPU dry run first.  The gate sits in the shared
submission path, reads evidence only (a CAS-verified receipt or a scoped CEO
grant) and fails closed.  Everything here is a private fixture: a real sealed
target, a real PrismaBuildCAS receipt published through ``publish_result``, a
real PoolQueue, and a tmp decision store.  No GPU and no live queue.

The producer that makes receipts (``--d38-plan``, ``--d38-preflight-for``) is a
later change; these tests fabricate the preflight action it will declare.
"""
from __future__ import annotations

import datetime
import hashlib
import json
from pathlib import Path
import socket
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import movement_actions, pool  # noqa: E402
import d38_gate  # noqa: E402
import pbrun  # noqa: E402

from test_pbrun_detach import (  # noqa: E402,F401
    _checkout, _one_json_line, fleet)
from test_slurm_lane import _submissions  # noqa: E402

NAMESPACE = {"mode": "host", "cwd": "/work", "interpreter": "/usr/bin/python3",
             "mounts": [], "prerequisites": []}
COMMAND = ("/bin/bash", "-lc", "printf ok")
NOW = datetime.datetime(2026, 10, 8, 16, 30, tzinfo=datetime.timezone.utc)
ZERO_KEY = "0" * 64


def _iso(delta_s: float = 0.0) -> str:
    return (NOW + datetime.timedelta(seconds=delta_s)).isoformat()


def _digest(descriptor: dict) -> str:
    return "sha256:" + pb.canonical_sha256(descriptor)


def _namespace_file(tmp_path: Path, descriptor: dict = NAMESPACE,
                    name: str = "namespace.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(descriptor), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    """A controlled clock, a private decision store, no outer launch identity."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(d38_gate, "now", lambda: NOW)
    monkeypatch.setattr(d38_gate, "ENFORCE", True)
    # The published runtime is not box-local; a test checkout is.  Without this
    # every placement would be pinned to the Spark it was submitted from.
    monkeypatch.setattr(pool, "is_box_local_path", lambda _path: False)
    decisions = tmp_path / "ceo-decisions"
    decisions.mkdir()
    monkeypatch.setattr(d38_gate, "DECISION_DIR", decisions)
    return decisions


def _queue(tmp_path: Path) -> pool.PoolQueue:
    """Two Sparks with a GPU and one x86 box, so each placement has an offer."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    for host in ("sparky", "sparklina"):
        queue.announce(host=host, tags=[host, "gb10"], has_gpu=True,
                       capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    queue.announce(host="dl380g10", tags=["dl380g10", "x86"], has_gpu=False,
                   capacity={"cpu": 8, "mem_gb": 32})
    return queue


def _argv(work: Path, *options: str, command=COMMAND) -> list[str]:
    return ["pbrun.py", "--cwd", str(work), "--wait-s", "0.01", *options,
            "--", *command]


def _prepare(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")


def _run(tmp_path, monkeypatch, work, *options, command=COMMAND) -> int:
    """pbrun.main(), returning its exit code whether it returned or exited."""

    _prepare(tmp_path, monkeypatch)
    monkeypatch.setattr(sys, "argv", _argv(work, *options, command=command))
    try:
        return int(pbrun.main() or 0)
    except SystemExit as exc:
        if isinstance(exc.code, int):
            return exc.code
        print(exc.code, file=sys.stderr)      # a refusal's message, not lost
        return 1


def _seal(tmp_path, monkeypatch, work, *options, command=COMMAND):
    """The action pbrun would seal for these options, published nowhere."""

    _prepare(tmp_path, monkeypatch)
    args = pbrun.parse_args(_argv(work, *options, command=command)[1:])
    prepared = pbrun.prepare_submission(args)
    return (pbrun.seal_action_from_template(prepared["template"]),
            prepared["template"]["cas"])


def _plan(target: dict, *, namespace: dict = NAMESPACE, images: tuple = (),
          job: str | None = None, target_command: list | None = None,
          cpu_command: list | None = None, differences: list | None = None) -> dict:
    """The plan a producer files: the target it proves and how the CPU run differs."""

    tc = list(target["params"]["command"] if target_command is None
              else target_command)
    cc = list(cpu_command) if cpu_command is not None else [
        *tc[:-1], tc[-1] + " # cpu"]
    return {"schema": d38_gate.PLAN_SCHEMA,
            "target": {"job_identity_hash": job or target["action_key"],
                       "image_digest": list(images),
                       "namespace": _digest(namespace)},
            "target_command": tc, "cpu_command": cc,
            "differences": [len(tc) - 1] if differences is None
            else list(differences)}


def _preflight_action(cas, checkout: Path, target: dict, *, tag: str = "a",
                      declared: bool = True, gpu: int = 0, plan_input: bool = True,
                      plan: dict | None = None, digest: str | None = None,
                      cuda_mask: bool = True, nvidia_none: bool = False) -> dict:
    """A sealed CPU action that declares itself a D38 producer run."""

    checkout.mkdir(parents=True, exist_ok=True)
    code = checkout / "task_code.py"
    if not code.exists():
        code.write_text("# closure member\n", encoding="utf-8")
    plan = plan if plan is not None else _plan(target)
    inputs, plan_digest = [], "ab" * 32
    if plan_input:
        entry, _ = cas.ingest_bytes(pb._canonical_file_bytes(plan),
                                    input_id=d38_gate.PLAN_INPUT_ID)
        inputs, plan_digest = [entry], str(entry["sha256"])
    command = list(plan["cpu_command"])
    argv = movement_actions.standard_capture_argv(
        command, "pbrun_result.txt", path_prefix="/opt/pb-tools")
    params: dict[str, object] = {
        "command": command, "demand": {"cpu": 1, "mem_gb": 1, "gpu": gpu}}
    if declared:
        params[d38_gate.PRODUCER_PARAM] = {
            "producer": d38_gate.PRODUCER_ID,
            "plan_sha256": digest or plan_digest}
    variables = {"PATH": "/opt/pb-tools:/usr/bin:/bin"}
    if cuda_mask:
        variables["CUDA_VISIBLE_DEVICES"] = ""
    if nvidia_none:
        variables["NVIDIA_VISIBLE_DEVICES"] = "none"
    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/d38-preflight",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic", "artifact_family": "generic",
                 "artifact_kind": "generic", "argv": argv,
                 "working_directory": ".", "result_path": "pbrun_result.txt"},
        "inputs": inputs,
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": params,
        "environment": {"variables": variables, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })


def _body(target: dict, *, key: str, namespace: dict = NAMESPACE,
          images: tuple = (), host_class: str = "x86", result: str = "pass",
          created: float = -60.0, expires: float | None = None) -> dict:
    return {
        "schema": d38_gate.RECEIPT_SCHEMA,
        "job_identity_hash": target["action_key"],
        "image_digest": list(images),
        "namespace": _digest(namespace),
        "preflight_action_key": key,
        "host_class": host_class,
        "result": result,
        "created_at": _iso(created),
        "expires": None if expires is None else _iso(expires),
    }


def _publish_receipt(tmp_path: Path, cas, target: dict, *, tag: str = "a",
                     raw: str | None = None, declared: bool = True,
                     gpu: int = 0, plan_input: bool = True,
                     plan: dict | None = None, digest: str | None = None,
                     cuda_mask: bool = True, nvidia_none: bool = False,
                     **body_options) -> str:
    """Publish a preflight action and its result through the real CAS."""

    checkout = tmp_path / f"pf-checkout-{tag}"
    action = _preflight_action(
        cas, checkout, target, tag=tag, declared=declared, gpu=gpu,
        plan_input=plan_input, plan=plan, digest=digest, cuda_mask=cuda_mask,
        nvidia_none=nvidia_none)
    key = str(action["action_key"])
    body = _body(target, key=body_options.pop("key", key), **body_options)
    out = tmp_path / f"receipt-{tag}.json"
    out.write_text(json.dumps(body) if raw is None else raw, encoding="utf-8")
    cas.publish_action_request(action)
    attestation = pb.preflight_action(
        action, cas_root=tmp_path / "cas", checkout_root=checkout,
        worker_launcher_identity=None)
    cas.publish_result(action, out, attestation=attestation,
                       return_execution_receipt=True)
    return key


def _grant(decisions: Path, decision_id: str, target: dict, *,
           namespace: dict = NAMESPACE, images: tuple = (),
           expires: float = 3600.0, verdict: str = "approve",
           state: str = "decided", kind: str = "decision",
           job: str | None = None) -> Path:
    path = decisions / f"{decision_id}.json"
    path.write_text(json.dumps({
        "kind": kind, "state": state, "verdict": verdict,
        "grant": {"d38_exception": {
            "job_identity_hash": job or target["action_key"],
            "image_digest": list(images),
            "namespace": _digest(namespace),
            "expires": _iso(expires)}}}), encoding="utf-8")
    return path


def _ready(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "pb-queue" / "ready").glob("*.json")) \
        if (tmp_path / "pb-queue" / "ready").exists() else []


def _audit(tmp_path: Path, key: str) -> list[dict]:
    root = tmp_path / "pb-queue" / d38_gate.AUDIT_DIR_NAME / key
    return [json.loads(p.read_text(encoding="utf-8"))
            for p in sorted(root.glob("*.json"))] if root.exists() else []


# --------------------------------------------------------------------------
# Applicability
# --------------------------------------------------------------------------

@pytest.mark.parametrize("demand,tags,host_class,expected", [
    ({"gpu": 1}, [], None, True),
    ({"gpu": 1}, ["x86"], None, True),
    ({"gpu": 0}, ["gb10"], None, True),
    ({"gpu": 0}, ["sparky"], None, True),
    ({"gpu": 0}, ["sparklina"], None, True),
    ({"gpu": 0}, [], "gb10", True),
    ({"gpu": 0}, ["x86"], None, False),
    ({}, [], None, False),
    ({"gpu": 0}, ["x86"], "x86", False),
])
def test_applicability_follows_demand_tags_and_host_class(
        demand, tags, host_class, expected) -> None:
    assert d38_gate.requires_receipt(
        demand, tags, host_class=host_class) is expected


# --------------------------------------------------------------------------
# The refusal
# --------------------------------------------------------------------------

def test_a_gpu_publication_without_a_receipt_is_refused(
        tmp_path, monkeypatch, capsys) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    target, _cas = _seal(tmp_path, monkeypatch, work, "--gpu",
                         "--d38-namespace", str(ns))
    code = _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns))
    err = capsys.readouterr().err
    assert code == 2, err
    assert "pbrun: D38 refuses GPU publication:" in err
    assert f"job={target['action_key']}" in err
    assert f"namespace={_digest(NAMESPACE)}" in err
    assert "No runnable submission was published." in err
    assert _ready(tmp_path) == []
    assert _audit(tmp_path, target["action_key"]) == []


def test_a_cpu_x86_publication_needs_no_evidence(
        tmp_path, monkeypatch, capsys) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    code = _run(tmp_path, monkeypatch, work, "--detach", "--tag", "x86")
    assert code == 0, capsys.readouterr().err
    assert len(_ready(tmp_path)) == 1


@pytest.mark.parametrize("options", [
    ("--tag", "gb10"),
    ("--tag", "sparklina"),
    ("--host-class", "gb10", "--measurement"),
    ("--here",),
    ("--gpu", "--exclusive"),
], ids=["gb10-tag", "spark-tag", "host-class", "derived-pin", "exclusive"])
def test_every_gpu_intent_needs_evidence(
        tmp_path, monkeypatch, capsys, options) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    code = _run(tmp_path, monkeypatch, work, "--detach", *options)
    err = capsys.readouterr().err
    assert code == 2, err
    assert "D38 refuses GPU publication" in err
    assert _ready(tmp_path) == []


def test_a_gpu_job_with_no_namespace_descriptor_is_refused_even_with_a_receipt(
        tmp_path, monkeypatch, capsys) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    target, cas = _seal(tmp_path, monkeypatch, work, "--gpu")
    key = _publish_receipt(tmp_path, cas, target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-receipt", key) == 2
    assert "namespace" in capsys.readouterr().err
    assert _ready(tmp_path) == []


# --------------------------------------------------------------------------
# A matching receipt
# --------------------------------------------------------------------------

def test_a_matching_receipt_permits_publication_and_is_audited(
        tmp_path, monkeypatch, capsys) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    target, cas = _seal(tmp_path, monkeypatch, work, "--gpu",
                        "--d38-namespace", str(ns))
    key = _publish_receipt(tmp_path, cas, target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns), "--d38-receipt", key) == 0
    assert len(_ready(tmp_path)) == 1
    events = _audit(tmp_path, target["action_key"])
    assert len(events) == 1
    event = events[0]
    receipt = cas.lookup(cas.read_action_request(key))
    assert event["authorization"] == "receipt"
    assert event["preflight_action_key"] == key
    assert event["receipt_sha256"] == receipt["receipt_sha256"]
    assert event["job_identity_hash"] == target["action_key"]
    assert event["namespace"] == _digest(NAMESPACE)


def test_a_valid_unchanged_receipt_permits_another_publication(
        tmp_path, monkeypatch, capsys) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    target, cas = _seal(tmp_path, monkeypatch, work, "--gpu",
                        "--d38-namespace", str(ns))
    key = _publish_receipt(tmp_path, cas, target)
    for _ in range(2):
        assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                    "--d38-namespace", str(ns), "--d38-receipt", key) == 0
        capsys.readouterr()
    assert len(_audit(tmp_path, target["action_key"])) >= 1


def test_the_namespace_is_part_of_the_job_identity(
        tmp_path, monkeypatch) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    first = _namespace_file(tmp_path)
    other = _namespace_file(tmp_path, {**NAMESPACE, "mounts": ["/data:ro"]},
                            name="other.json")
    plain, _ = _seal(tmp_path, monkeypatch, work, "--gpu")
    a, _ = _seal(tmp_path, monkeypatch, work, "--gpu", "--d38-namespace",
                 str(first))
    b, _ = _seal(tmp_path, monkeypatch, work, "--gpu", "--d38-namespace",
                 str(other))
    assert len({plain["action_key"], a["action_key"], b["action_key"]}) == 3
    assert d38_gate.NAMESPACE_PARAM not in plain["params"]
    assert a["params"][d38_gate.NAMESPACE_PARAM] == _digest(NAMESPACE)


# --------------------------------------------------------------------------
# A receipt that does not bind this job
# --------------------------------------------------------------------------

def _case_missing(tmp_path, cas, target):
    return ZERO_KEY


def _case_other_job(tmp_path, cas, target):
    other = dict(target, action_key="1" * 64)
    return _publish_receipt(tmp_path, cas, other, tag="other-job")


def _case_other_namespace(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="other-ns",
                            namespace={**NAMESPACE, "mounts": ["/x:ro"]})


def _case_other_images(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="other-img",
                            images=("content:sha256:" + "c" * 64,))


def _case_failed(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="failed", result="fail")


def _case_future(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="future", created=300.0)


def _case_expired(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="expired", expires=-1.0)


def _case_expires_now(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="boundary", expires=0.0)


def _case_wrong_action_key(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="wrongkey", key="2" * 64)


def _case_bad_host_class(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="badhost",
                            host_class="arm")


def _case_gb10_from_an_x86_producer(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="gb10x86",
                            host_class="gb10")


def _case_malformed_json(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="malformed",
                            raw="{not json")


def _case_unknown_field(tmp_path, cas, target):
    body = _body(target, key="3" * 64)
    body["extra"] = 1
    return _publish_receipt(tmp_path, cas, target, tag="unknown",
                            raw=json.dumps(body))


def _case_duplicate_key(tmp_path, cas, target):
    return _publish_receipt(
        tmp_path, cas, target, tag="duplicate",
        raw='{"schema": "fleet.d38.preflight.v1", "result": "fail", "result": "pass"}')


def _case_nan(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="nan",
                            raw='{"expires": NaN}')


def _case_oversized(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="big",
                            raw=json.dumps({"pad": "x" * 70000}))


def _case_gpu_preflight(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="gpu", gpu=1)


def _case_undeclared_preflight(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="undeclared",
                            declared=False)


def _case_tampered(tmp_path, cas, target):
    key = _publish_receipt(tmp_path, cas, target, tag="tamper")
    receipt = cas.lookup(cas.read_action_request(key))
    blob = cas.blob_path(receipt["result"]["sha256"])
    blob.chmod(0o644)
    blob.write_bytes(blob.read_bytes() + b" ")
    return key


def _case_no_plan_input(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="noplan", plan_input=False)


def _case_unrelated_entry_point(tmp_path, cas, target):
    unrelated = _plan(target, cpu_command=["/bin/echo", "unrelated"],
                      differences=[0, 1])
    return _publish_receipt(tmp_path, cas, target, tag="unrelated", plan=unrelated)


def _case_plan_for_another_job(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="otherplan",
                            plan=_plan(target, job="5" * 64))


def _case_plan_for_another_command(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="othercmd",
                            plan=_plan(target, target_command=["/bin/true", "x"],
                                       cpu_command=["/bin/true", "y"],
                                       differences=[1]))


def _case_plan_digest_mismatch(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="baddigest", digest="9" * 64)


def _case_plan_moves_the_entry_point(tmp_path, cas, target):
    tc = list(target["params"]["command"])
    return _publish_receipt(
        tmp_path, cas, target, tag="moved",
        plan=_plan(target, cpu_command=[tc[0], tc[1] + "-cpu", *tc[2:]],
                   differences=[1]))


def _case_undeclared_difference(tmp_path, cas, target):
    tc = list(target["params"]["command"])
    return _publish_receipt(
        tmp_path, cas, target, tag="undeclared-diff",
        plan=_plan(target, cpu_command=[*tc[:-1], "something else"],
                   differences=[]))


def _case_no_cuda_mask(tmp_path, cas, target):
    return _publish_receipt(tmp_path, cas, target, tag="nomask", cuda_mask=False)


CASES = [_case_missing, _case_other_job, _case_other_namespace,
         _case_other_images, _case_failed, _case_future, _case_expired,
         _case_expires_now, _case_wrong_action_key, _case_bad_host_class,
         _case_gb10_from_an_x86_producer, _case_malformed_json,
         _case_unknown_field, _case_duplicate_key, _case_nan,
         _case_oversized, _case_gpu_preflight, _case_undeclared_preflight,
         _case_tampered, _case_no_plan_input, _case_unrelated_entry_point,
         _case_plan_for_another_job, _case_plan_for_another_command,
         _case_plan_digest_mismatch, _case_plan_moves_the_entry_point,
         _case_undeclared_difference, _case_no_cuda_mask]


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.__name__[6:])
def test_a_receipt_that_does_not_bind_this_job_publishes_nothing(
        tmp_path, monkeypatch, capsys, case) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    target, cas = _seal(tmp_path, monkeypatch, work, "--gpu",
                        "--d38-namespace", str(ns))
    key = case(tmp_path, cas, target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns), "--d38-receipt", key) == 2
    assert "pbrun: D38 refuses GPU publication:" in capsys.readouterr().err
    assert _ready(tmp_path) == []
    assert _audit(tmp_path, target["action_key"]) == []


def test_a_changed_job_invalidates_the_old_receipt(
        tmp_path, monkeypatch, capsys) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    target, cas = _seal(tmp_path, monkeypatch, work, "--gpu",
                        "--d38-namespace", str(ns))
    key = _publish_receipt(tmp_path, cas, target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns), "--d38-receipt", key,
                command=("/bin/bash", "-lc", "printf changed")) == 2
    assert _ready(tmp_path) == []


def test_a_receipt_filed_after_a_refusal_does_not_authorize_the_refused_one(
        tmp_path, monkeypatch, capsys) -> None:
    """The receipt must exist before the publication, not after it."""

    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    target, cas = _seal(tmp_path, monkeypatch, work, "--gpu",
                        "--d38-namespace", str(ns))
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns)) == 2
    assert _ready(tmp_path) == []
    key = _publish_receipt(tmp_path, cas, target)
    capsys.readouterr()
    assert _ready(tmp_path) == []
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns), "--d38-receipt", key) == 0
    assert len(_ready(tmp_path)) == 1


# --------------------------------------------------------------------------
# The explicit CEO exception
# --------------------------------------------------------------------------

def _gpu_target(tmp_path, monkeypatch):
    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    target, cas = _seal(tmp_path, monkeypatch, work, "--gpu",
                        "--d38-namespace", str(ns))
    return work, ns, target, cas


def test_a_scoped_grant_permits_publication_and_leaves_an_audit_event(
        tmp_path, monkeypatch, capsys, _isolated) -> None:
    work, ns, target, _cas = _gpu_target(tmp_path, monkeypatch)
    grant = _grant(_isolated, "dec-1008-000000-aaaa", target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns),
                "--d38-exception", "dec-1008-000000-aaaa") == 0
    err = capsys.readouterr().err
    assert ("pbrun: D38 exception dec-1008-000000-aaaa authorizes job "
            f"{target['action_key']}") in err
    events = _audit(tmp_path, target["action_key"])
    assert len(events) == 1
    assert events[0]["authorization"] == "exception"
    assert events[0]["decision_id"] == "dec-1008-000000-aaaa"
    assert events[0]["decision_sha256"] == hashlib.sha256(
        grant.read_bytes()).hexdigest()
    assert len(_ready(tmp_path)) == 1


def test_a_receipt_and_an_exception_together_are_refused(
        tmp_path, monkeypatch, capsys, _isolated) -> None:
    work, ns, target, cas = _gpu_target(tmp_path, monkeypatch)
    _grant(_isolated, "dec-1008-000000-bbbb", target)
    key = _publish_receipt(tmp_path, cas, target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns), "--d38-receipt", key,
                "--d38-exception", "dec-1008-000000-bbbb") == 2
    assert _ready(tmp_path) == []


@pytest.mark.parametrize("make", [
    lambda d, t: None,
    lambda d, t: _grant(d, "dec-x", t, expires=-1.0),
    lambda d, t: _grant(d, "dec-x", t, verdict="refuse"),
    lambda d, t: _grant(d, "dec-x", t, state="pending"),
    lambda d, t: _grant(d, "dec-x", t, kind="report"),
    lambda d, t: _grant(d, "dec-x", t, job="4" * 64),
    lambda d, t: _grant(d, "dec-x", t, namespace={**NAMESPACE, "mounts": ["/y"]}),
    lambda d, t: _grant(d, "dec-x", t, images=("content:sha256:" + "d" * 64,)),
    lambda d, t: (d / "dec-x.json").write_text("{broken", encoding="utf-8"),
], ids=["missing", "expired", "refused", "undecided", "wrong-kind",
        "other-job", "other-namespace", "other-images", "unreadable"])
def test_an_exception_that_does_not_bind_this_job_publishes_nothing(
        tmp_path, monkeypatch, capsys, _isolated, make) -> None:
    work, ns, target, _cas = _gpu_target(tmp_path, monkeypatch)
    make(_isolated, target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns), "--d38-exception", "dec-x") == 2
    assert "D38 refuses GPU publication" in capsys.readouterr().err
    assert _ready(tmp_path) == []
    assert _audit(tmp_path, target["action_key"]) == []


def test_an_audit_that_cannot_be_written_publishes_nothing(
        tmp_path, monkeypatch, capsys, _isolated) -> None:
    work, ns, target, _cas = _gpu_target(tmp_path, monkeypatch)
    _grant(_isolated, "dec-1008-000000-cccc", target)

    def broken(*_args, **_kwargs):
        raise OSError("the audit store is read-only")

    monkeypatch.setattr(d38_gate, "write_audit", broken)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns),
                "--d38-exception", "dec-1008-000000-cccc") == 2
    err = capsys.readouterr().err
    assert "audit" in err
    assert "authorizes job" not in err
    assert _ready(tmp_path) == []


# --------------------------------------------------------------------------
# Work that publishes nothing new needs no receipt
# --------------------------------------------------------------------------

def test_a_live_attachment_needs_no_new_receipt(
        tmp_path, monkeypatch, capsys, _isolated) -> None:
    work, ns, target, _cas = _gpu_target(tmp_path, monkeypatch)
    _grant(_isolated, "dec-1008-000000-dddd", target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns),
                "--d38-exception", "dec-1008-000000-dddd") == 0
    capsys.readouterr()
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns)) == 0
    line = _one_json_line(capsys.readouterr())
    assert line["status"] == "attached"
    assert len(_ready(tmp_path)) == 1


def test_a_cache_hit_needs_no_new_receipt(
        tmp_path, monkeypatch, capsys) -> None:
    """A CAS hit publishes nothing, so it needs no evidence.

    A private queue cannot run a GPU item (a claim needs a trusted broker
    snapshot), so the hit is the CAS answering for exactly this job's key, at
    the ``lookup`` the gate and pbrun both read.  Without the hit the same
    submission is refused.
    """

    work, ns, target, cas = _gpu_target(tmp_path, monkeypatch)
    options = ("--gpu", "--detach", "--d38-namespace", str(ns))
    assert _run(tmp_path, monkeypatch, work, *options) == 2
    capsys.readouterr()

    real = type(cas).lookup

    def lookup(self, action):
        if str(action["action_key"]) == target["action_key"]:
            return {"receipt_sha256": "e" * 64,
                    "result": {"sha256": "f" * 64, "bytes": 1}}
        return real(self, action)

    monkeypatch.setattr(type(cas), "lookup", lookup)
    code = _run(tmp_path, monkeypatch, work, *options)
    out = capsys.readouterr()
    assert code == 0, out.err
    assert _one_json_line(out)["status"] == "cache_hit", out.err
    assert _ready(tmp_path) == []
    assert _audit(tmp_path, target["action_key"]) == []


# --------------------------------------------------------------------------
# Every transport and mode keeps the gate
# --------------------------------------------------------------------------

def test_the_slurm_lane_keeps_the_gate(
        tmp_path, monkeypatch, capsys, fleet) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--transport", "slurm", "--d38-namespace", str(ns)) == 2
    assert "D38 refuses GPU publication" in capsys.readouterr().err
    assert _submissions(fleet) == []


def test_an_attached_submission_keeps_the_gate(
        tmp_path, monkeypatch, capsys) -> None:
    work = _checkout(tmp_path)
    _queue(tmp_path)
    ns = _namespace_file(tmp_path)
    assert _run(tmp_path, monkeypatch, work, "--gpu",
                "--d38-namespace", str(ns)) == 2
    assert "D38 refuses GPU publication" in capsys.readouterr().err
    assert _ready(tmp_path) == []


def test_the_shipped_gate_is_on_and_no_caller_input_can_turn_it_off() -> None:
    """Read the value a fresh interpreter ships with, not the fixture's."""

    import subprocess

    shipped = subprocess.run(
        [sys.executable, "-c",
         "import sys; sys.path[:0] = ['src', 'tools/fleet'];"
         "import d38_gate; print(d38_gate.ENFORCE)"],
        cwd=REPOSITORY, capture_output=True, text=True)
    assert shipped.stdout.strip() == "True", shipped.stderr
    source = (REPOSITORY / "tools" / "fleet" / "d38_gate.py").read_text(
        encoding="utf-8")
    assert "os.environ" not in source and "getenv" not in source
    parsed = pbrun.parse_args(["--cwd", ".", "--", "/bin/true"])
    assert not any("enforce" in name.lower() for name in vars(parsed))


# --------------------------------------------------------------------------
# Deferred (--after) submissions
# --------------------------------------------------------------------------

def _deferred_setup(tmp_path, monkeypatch):
    import test_deferred_action_edges as de

    queue, work = de._env(tmp_path, monkeypatch)
    template = de._template(tmp_path / "canonical")
    producer = de._producer_key(tmp_path, template, "d38-band")
    de._publish_producer(queue, template, producer)
    de._announce_tier(queue, mountpoint=tmp_path / "stage")
    return de, queue, work, template, producer


def _submit_deferred(work, monkeypatch, de, producer, template, *options):
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        "--after", f"{producer}:{template['template_id']}",
        "--residency", "stage", *options, "--", "/bin/cat", de.PLACEHOLDER])
    try:
        return int(pbrun.main() or 0)
    except SystemExit as exc:
        if not isinstance(exc.code, int):
            print(exc.code, file=sys.stderr)
            return 1
        return exc.code


def test_a_deferred_gpu_submission_is_refused_at_registration(
        tmp_path, monkeypatch, capsys) -> None:
    """A deferred job has no identity until its producer ends, so no receipt or
    grant can bind it beforehand: it files nothing rather than file work D38
    could never authorize."""

    de, queue, work, template, producer = _deferred_setup(tmp_path, monkeypatch)
    ns = _namespace_file(tmp_path)
    code = _submit_deferred(work, monkeypatch, de, producer, template,
                            "--d38-namespace", str(ns))
    err = capsys.readouterr().err
    assert code == 2, err
    assert "D38 refuses GPU publication" in err
    assert "deferred" in err
    deferred = tmp_path / "pb-queue" / "deferred"
    assert not deferred.exists() or list(deferred.glob("*.json")) == []


def test_a_deferred_cpu_submission_still_files(
        tmp_path, monkeypatch, capsys) -> None:
    de, queue, work, template, producer = _deferred_setup(tmp_path, monkeypatch)
    # An x86 placement on a non-pinned runtime is not GPU intent.
    monkeypatch.setattr(d38_gate, "requires_receipt", lambda *a, **k: False)
    assert _submit_deferred(work, monkeypatch, de, producer, template) == 0, \
        capsys.readouterr().err


def test_a_release_re_checks_the_gate_for_a_record_an_old_client_filed(
        tmp_path, monkeypatch, capsys) -> None:
    """Defence in depth: a record filed with the gate off publishes nothing at
    release once the gate is on, because the sealed key has no evidence."""

    de, queue, work, template, producer = _deferred_setup(tmp_path, monkeypatch)
    monkeypatch.setattr(d38_gate, "ENFORCE", False)
    assert _submit_deferred(work, monkeypatch, de, producer, template) == 0, \
        capsys.readouterr().err
    instance = de._start(queue, template, producer)
    de._commit(queue, template, instance, "b1", b"d38 handoff bytes")
    queue.finish(producer, status="executed")
    monkeypatch.setattr(d38_gate, "ENFORCE", True)
    events = de.dr.release_tick(queue)
    assert de._released(events) == [], events
    assert list(queue.dir(pool.READY).glob("*.json")) == [], (
        "the consumer was published without D38 evidence")
    # The control: the same pending record releases once the gate is off, so the
    # gate, and nothing else, held it back.
    monkeypatch.setattr(d38_gate, "ENFORCE", False)
    assert len(de._released(de.dr.release_tick(queue))) == 1


# --------------------------------------------------------------------------
# A CPU-only gb10 proof is not rejected for the host's physical GPU
# --------------------------------------------------------------------------

def _spark_receipt() -> dict:
    """What a gb10 worker attests: an ARM64 host that lists its GPU."""

    return {"producer": {"evidence": {
        "machine": "aarch64",
        "accelerators": [{"kind": "nvidia", "compute_capability": "12.1"}]}}}


def test_a_valid_cpu_only_gb10_proof_passes_though_its_host_lists_a_gpu(
        tmp_path, monkeypatch) -> None:
    work, ns, target, cas = _gpu_target(tmp_path, monkeypatch)
    action = _preflight_action(cas, tmp_path / "gb10-pf", target,
                               nvidia_none=True)
    d38_gate.check_cpu_visibility(action, "gb10")
    d38_gate.check_host_class(_spark_receipt(), "gb10")


@pytest.mark.parametrize("options,host_class", [
    ({"nvidia_none": False}, "gb10"),
    ({"nvidia_none": True, "cuda_mask": False}, "gb10"),
    ({"cuda_mask": False}, "x86"),
], ids=["gb10-without-nvidia-none", "gb10-without-cuda-mask", "x86-without-cuda-mask"])
def test_a_preflight_that_does_not_hide_the_devices_is_refused(
        tmp_path, monkeypatch, options, host_class) -> None:
    work, ns, target, cas = _gpu_target(tmp_path, monkeypatch)
    action = _preflight_action(cas, tmp_path / "pf-vis", target, **options)
    with pytest.raises(d38_gate.Refusal):
        d38_gate.check_cpu_visibility(action, host_class)


def test_a_host_class_lie_is_still_refused() -> None:
    with pytest.raises(d38_gate.Refusal):
        d38_gate.check_host_class(_spark_receipt(), "x86")


# --------------------------------------------------------------------------
# An exempt attachment can never become a new publication
# --------------------------------------------------------------------------

def test_an_attached_slurm_call_beside_a_live_pool_run_submits_nothing(
        tmp_path, monkeypatch, capsys, _isolated, fleet) -> None:
    """The reviewer's cross-transport hole: ``slurm_outcome`` excludes a pool
    attachment and submits a new SLURM job."""

    work, ns, target, _cas = _gpu_target(tmp_path, monkeypatch)
    _grant(_isolated, "dec-1008-000000-xxxx", target)
    assert _run(tmp_path, monkeypatch, work, "--gpu", "--detach",
                "--d38-namespace", str(ns),
                "--d38-exception", "dec-1008-000000-xxxx") == 0
    capsys.readouterr()
    code = _run(tmp_path, monkeypatch, work, "--gpu", "--transport", "slurm",
                "--d38-namespace", str(ns))
    assert _submissions(fleet) == [], capsys.readouterr().err
    assert len(_ready(tmp_path)) == 1
    assert len(_audit(tmp_path, target["action_key"])) == 1
    assert code != 2 or "D38" in capsys.readouterr().err


def test_a_generation_that_ends_before_publication_cannot_publish_without_evidence(
        tmp_path, monkeypatch, capsys) -> None:
    """The liveness read is stale by the time of publication: nothing is live,
    and the earlier read must not stand in for authorization."""

    work, ns, target, _cas = _gpu_target(tmp_path, monkeypatch)
    live = {"transport": "pool", "generation": 1.0, "submission": None,
            "job_id": None}
    reads = iter([None])

    def liveness(_queue, _key, **_kw):
        return next(reads, live)

    monkeypatch.setattr(pbrun, "bounded_attachment", liveness)
    _run(tmp_path, monkeypatch, work, "--gpu", "--d38-namespace", str(ns))
    assert _ready(tmp_path) == []
    assert _audit(tmp_path, target["action_key"]) == []


def test_an_attached_cache_hit_without_evidence_publishes_nothing(
        tmp_path, monkeypatch, capsys) -> None:
    work, ns, target, cas = _gpu_target(tmp_path, monkeypatch)
    real = type(cas).lookup

    def lookup(self, action):
        if str(action["action_key"]) == target["action_key"]:
            return {"receipt_sha256": "e" * 64,
                    "result": {"sha256": "f" * 64, "bytes": 1}}
        return real(self, action)

    monkeypatch.setattr(type(cas), "lookup", lookup)
    code = _run(tmp_path, monkeypatch, work, "--gpu", "--d38-namespace", str(ns))
    assert code == 2, capsys.readouterr().err
    assert _ready(tmp_path) == []
