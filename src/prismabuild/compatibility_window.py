"""Read-only support-window identity and rollback reachability checks."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .digest_primitives import canonical_sha256, raw_sha256
from .generation_receipt import GenerationReceipt, SourceDeclarations


class WindowError(ValueError):
    pass


RECEIPT_MEMBERS = frozenset({"generation_receipt.py", "digest_primitives.py"})


def client_protocol(root, *, receipt=None):
    root = Path(root)
    candidates = (root / "tools/fleet/upgrade_client.py", root / "tools/upgrade_client.py")
    path = next((path for path in candidates if path.is_file()), None)
    if path is None:
        raise WindowError("generation has no updater source")
    raw = path.read_bytes()
    relative = str(path.relative_to(root))
    try:
        if receipt is None and (root / "RUNTIME_VERSION.json").is_file():
            receipt = GenerationReceipt.capture((root / "RUNTIME_VERSION.json").read_bytes())
            receipt.require_generation(root.name)
        if receipt is None:
            source = SourceDeclarations.capture(raw)
            digest = raw_sha256(raw)
        else:
            source = receipt.source_declarations(relative, raw)
            digest = receipt.member_digest(relative)
        optional = source.members("OPTIONAL_MEMBERS")
        if optional is None:
            raise WindowError("updater optional-member contract is unknown")
        present = sorted(name for name, member in optional.items() if (root / member).is_file())
        return {"optional": sorted(optional), "present": present,
                "optional_protocol": source.protocol("CLIENT_UPGRADE_PROTOCOL"),
                "proof_protocol": source.protocol("CLIENT_UPGRADE_PROOF_MEMBER_PROTOCOL"),
                "receipt_member_protocol": source.protocol("CLIENT_UPGRADE_RECEIPT_MEMBER_PROTOCOL"),
                "receipt_reader_protocol": source.protocol("CLIENT_UPGRADE_RECEIPT_READER_PROTOCOL"),
                "updater_member": relative, "updater_sha256": digest}
    except ValueError as exc:
        raise WindowError(str(exc)) from exc


def require_reachable(candidate, rollback):
    forward, target = client_protocol(candidate), client_protocol(rollback)
    return require_client_transition(forward, target)


def require_client_transition(forward, target):
    for row in (forward, target):
        present = set(row["present"]) & RECEIPT_MEMBERS
        if present and present != RECEIPT_MEMBERS:
            raise WindowError("incomplete receipt reader dependency closure")
        if row["receipt_reader_protocol"] == 1 and present != RECEIPT_MEMBERS:
            raise WindowError("receipt reader requires both installed dependencies")
    union = set(forward["present"]) | set(target["present"])
    if union & RECEIPT_MEMBERS and any(row["receipt_member_protocol"] != 1 for row in (forward, target)):
        raise WindowError("receipt client transition requires receipt-member recovery protocol 1 in both updaters")
    if union and any(row["optional_protocol"] != 2 for row in (forward, target)):
        raise WindowError("optional member transition requires dependency-aware protocol 2")
    if "park_proof.py" in union and any(row["proof_protocol"] != 2 for row in (forward, target)):
        raise WindowError("B09 proof member requires a protocol-2 bridge rollback target; a17477 is unreachable")
    return {"candidate": forward, "rollback": target,
            "physical_rollback": "separate qualification required"}


def require_receipt_participants(direction, installed, roster):
    sources = (direction["candidate"], direction["rollback"])
    if not any(set(row["present"]) & RECEIPT_MEMBERS for row in sources):
        return
    if not installed or set(installed) != set(roster):
        raise WindowError("receipt activation requires current installed updater proof for every enrolled participant; "
                          "a separately authorized installed-reader bootstrap is required")
    capable = {row["updater_sha256"] for row in sources if row["receipt_member_protocol"] == 1}
    if any(digest not in capable for digest in installed.values()):
        raise WindowError("current installed updater lacks receipt-member recovery protocol 1")


def require_predeploy(window, status, *, captured_unix=None, max_age_s=300, now=None):
    """Deployment must rerun this with a fresh census, never reuse capture freshness."""
    if (status.get("complete") is not True or status.get("timed_out_sections")
            or status.get("unavailable_sections")):
        raise WindowError("predeploy census is incomplete")
    now = datetime.now(timezone.utc).timestamp() if now is None else now
    captured = status.get("captured_unix") if captured_unix is None else captured_unix
    if type(captured) not in (int, float) or not 0 <= now - captured <= max_age_s:
        raise WindowError("predeploy census is stale or in the future")
    live = [row for row in status["nodes"] if row.get("state") == "live"]
    sources = window["generations"]
    rollback = window["rollback"]
    supported = {row["commit"] for row in sources}
    observed = {row.get("runtime_commit") for row in live}
    identities = {(row["generation"], row["commit"]) for row in sources}
    if (window.get("complete") is not True or len(identities) != len(sources)
            or (rollback["generation"], rollback["commit"]) not in identities):
        raise WindowError("window does not contain its exact designated rollback identity")
    if not live or not observed <= supported:
        raise WindowError("live window changed; recapture and rerun the corpus without relabeling rollback")
    if supported != observed | {rollback["commit"]}:
        raise WindowError("retained directories outside live-plus-rollback are not supported window members")
    return tuple(sorted(row["node"] for row in live))


def content_digest(files):
    """Identity of the complete receipt file map, after checking each actual byte."""
    return canonical_sha256(files)


def require_generation(directory, designation):
    directory = Path(directory)
    if directory.is_symlink():
        raise WindowError("rollback generation must be an immutable directory, not a symlink")
    if str(directory.resolve()) != designation.get("directory"):
        raise WindowError("rollback directory differs from the persistent operator designation")
    raw = (directory / "RUNTIME_VERSION.json").read_bytes()
    captured = GenerationReceipt.capture(raw)
    try:
        value = captured.require_generation(designation["generation"])
        captured.require_commit()
    except ValueError as exc:
        raise WindowError(str(exc)) from exc
    if (directory.name != designation["generation"] or value["generation"] != designation["generation"]
            or value["commit"] != designation["commit"]
            or raw_sha256(raw) != designation["receipt_sha256"]
            or content_digest(value["files"]) != designation["content_sha256"]):
        raise WindowError("rollback directory, commit or receipt digest changed")
    for relative, expected in value["files"].items():
        try:
            path = captured.member_path(directory, relative)
        except ValueError as exc:
            raise WindowError("unsafe rollback source path") from exc
        if any(parent.is_symlink() for parent in (path, *path.parents)
               if parent.is_relative_to(directory)):
            raise WindowError("unsafe rollback source path")
        if not captured.matches_bytes(relative, path.read_bytes()):
            raise WindowError(f"rollback source content changed: {relative}")
    return value


# This designation is deliberately narrower than the automatic transition API.
BOOTSTRAP_SOURCE = {
    "generation": "a17477b2b206-1791745175-65dffa700dfb",
    "commit": "a17477b2b206adf79d89e65c7b9d09584f0bfa34",
    "receipt_sha256": "5cef73692997fbd9f3d8d187c9f7aad15ee6b699912ceb1cf88fe9444082c9dd",
    "content_sha256": "dd694fdb23b193f46ab82349c81067b6a9fd68eeaaba3ec24e24bf4176767ff3",
}
BOOTSTRAP_SERVICE_MEMBERS = ("resource_broker.py", "resource_payload.py", "gpu_memory.py", "gpu_capacity.py")
BOOTSTRAP_LEAVES = ("digest_primitives.py", "generation_receipt.py", "park_proof.py")
BOOTSTRAP_MEMBERS = (*BOOTSTRAP_SERVICE_MEMBERS, *BOOTSTRAP_LEAVES, "upgrade_client.py")


class RootBootstrapResponses:
    """In-memory collector result, never deserialized from a shared JSON claim.

    The trusted coordinator constructs this only from completed authenticated
    root subprocess responses to its own fresh challenge.
    """
    def __init__(self, rows, *, nonce, started_unix):
        self.rows = rows
        self.nonce = nonce
        self.started_unix = started_unix


class BootstrapQualification:
    """Receipt verifier result, distinct from the operator's unverified index."""
    def __init__(self, plan, *, action_keys, runtimes=()):
        self.plan = plan
        self.action_keys = tuple(action_keys)
        self.runtimes = tuple(runtimes)


def require_bootstrap_host_identity(plan, host, row):
    """A changed boot needs the helper's explicit root-local reconciliation."""
    process = row.get('broker_process')
    if (not isinstance(process, dict) or set(process) != {'pid', 'starttime'}
            or type(process['pid']) is not int or process['pid'] <= 0
            or not isinstance(process['starttime'], str) or not process['starttime'].isdigit()):
        raise WindowError('bootstrap broker process identity is malformed for ' + host)
    if row.get('boot_id') == plan['boots'][host] and row.get('broker_process') == plan['brokers'][host]:
        return
    record = row.get('boot_reconciliation')
    if (not isinstance(record, dict) or not isinstance(row.get('boot_id'), str)
            or not row['boot_id'] or row['boot_id'] == plan['boots'][host]
            or record.get('original_boot_id') != plan['boots'][host]
            or record.get('boot_id') != row['boot_id']
            or record.get('broker_process') != row.get('broker_process')
            or record.get('plan_sha256') != canonical_sha256({**plan, 'direction': 'forward'})
            or type(record.get('approved_unix')) not in (int, float)
            or not 0 < record['approved_unix'] <= row.get('observed_unix', 0)):
        raise WindowError('bootstrap boot/broker changed without root reconciliation for ' + host)


def require_installed_bootstrap(source, target, observations, *, roster,
                                qualification, rollback_designation):
    """Assess an already installed closure; never authorize automatic transport."""
    import math
    import time

    if not isinstance(qualification, BootstrapQualification) or not qualification.action_keys:
        raise WindowError("bootstrap requires verified PB corpus and crash/reverse qualification receipts")
    plan = qualification.plan
    if plan.get("qualified_runtimes") != list(qualification.runtimes):
        raise WindowError("bootstrap plan does not bind its verified native runtimes")
    if any(plan["source"].get(key) != value for key, value in BOOTSTRAP_SOURCE.items()):
        raise WindowError("bootstrap source must be the exact designated a174 generation")
    if (rollback_designation.get("mode") != "operator-restoration"
            or rollback_designation.get("automatic_reachability") is not False
            or rollback_designation.get("retained_leaves") != list(BOOTSTRAP_LEAVES)
            or any(rollback_designation["rollback"].get(key) != value
                   for key, value in BOOTSTRAP_SOURCE.items())):
        raise WindowError("bootstrap requires operator restoration with retained leaves, never automatic a174 reachability")
    for root, identity in ((source, plan["source"]), (target, plan["target"])):
        receipt = require_generation(root, identity)
        if root.stat().st_mode & 0o222:
            raise WindowError("bootstrap generation is not sealed")
        for member in receipt["files"]:
            if (Path(root) / member).stat().st_mode & 0o222:
                raise WindowError("bootstrap generation member is not sealed")
    if plan["source"] == plan["target"]:
        raise WindowError("bootstrap target must be a distinct qualified generation")
    target_receipt = GenerationReceipt.capture((Path(target) / "RUNTIME_VERSION.json").read_bytes())
    if target_receipt.files.get("tools/bootstrap_client.py") != plan["tool_sha256"]:
        raise WindowError("bootstrap tool identity differs from the sealed target")
    if len(set(plan["boots"].values())) != len(roster):
        raise WindowError("bootstrap boot identities are ambiguous")
    members = plan["members"]
    final_protocol = client_protocol(target)
    updater_member = final_protocol["updater_member"]
    declarations = target_receipt.source_declarations(updater_member, (Path(target) / updater_member).read_bytes())
    member_paths = {**declarations.members("MEMBERS"), **declarations.members("OPTIONAL_MEMBERS")}
    if members != member_paths:
        raise WindowError("bootstrap member paths differ from the final updater's source contract")
    if set(members) != set(BOOTSTRAP_MEMBERS):
        raise WindowError("bootstrap requires exactly eight target members")
    for root, field in ((source, "source_files"), (target, "target_files")):
        captured = GenerationReceipt.capture((Path(root) / "RUNTIME_VERSION.json").read_bytes())
        for name, expected in plan[field].items():
            actual = captured.files.get(members[name])
            if expected != actual:
                raise WindowError("bootstrap member map differs from sealed receipt: " + name)
    if (set(plan["source_files"]) != set((*BOOTSTRAP_SERVICE_MEMBERS, "upgrade_client.py"))
            or set(plan["target_files"]) != set(BOOTSTRAP_MEMBERS)):
        raise WindowError("bootstrap source/target closure differs")
    service = {name: plan["source_files"][name] for name in BOOTSTRAP_SERVICE_MEMBERS}
    if any(plan["target_files"][name] != digest for name, digest in service.items()):
        raise WindowError("bootstrap changes a broker service member")
    if roster != plan["roster"] or not roster:
        raise WindowError("bootstrap enrollment roster changed")
    aliases = [alias for names in roster.values() for alias in names]
    if len(set(aliases)) != len(aliases) or any(host not in names for host, names in roster.items()):
        raise WindowError("bootstrap aliases are ambiguous")
    if not isinstance(observations, RootBootstrapResponses):
        raise WindowError("shared evidence is not a fresh authenticated root response")
    rows = observations.rows
    if set(rows) != set(roster):
        raise WindowError("bootstrap requires every enrolled host")
    now = time.time()
    if not 0 <= now - observations.started_unix <= 60:
        raise WindowError("bootstrap collector challenge is stale")
    direction = plan.get("direction", "forward")
    if direction not in ("forward", "restore"):
        raise WindowError("unknown bootstrap direction")
    expected = dict(plan["target_files"])
    if direction == "restore":
        expected["upgrade_client.py"] = plan["source_files"]["upgrade_client.py"]
    agent = expected["upgrade_client.py"]
    for host, row in rows.items():
        require_bootstrap_host_identity(plan, host, row)
        if row.get("runtime") not in qualification.runtimes:
            raise WindowError("root bootstrap interpreter has no matching qualified runtime receipt for " + host)
        stamp = row.get("observed_unix")
        if (row.get("schema") != "prismabuild.client_bootstrap.observation.v1"
                or row.get("nonce") != observations.nonce
                or not isinstance(stamp, (int, float)) or not math.isfinite(stamp)
                or not observations.started_unix <= stamp <= now
                or row.get("host") not in roster[host]
                or row.get("plan_sha256") != canonical_sha256(plan)
                or row.get("source") != plan["source"] or row.get("target") != plan["target"]):
            raise WindowError("stale, replayed or substituted bootstrap response for " + host)
        if (row.get("files") != expected or row.get("executing_sha256") != agent
                or row.get("captured_sha256") != agent
                or row.get("reader_loaded") is not True or row.get("proof_loaded") is not True):
            raise WindowError("installed bootstrap reader/leaf closure differs for " + host)
        metadata = row.get("metadata", {})
        if set(metadata) != set(expected) or any(
                type(item.get("uid")) is not int or item["uid"] != 0 or type(item.get("gid")) is not int or not 0 <= item["gid"] < 2**32 - 1
                or type(item.get("mode")) is not int or not 0 <= item["mode"] <= 0o7777 or item["mode"] & 0o022
                for item in metadata.values()):
            raise WindowError("unsafe root custody for " + host)
        if (row.get("fenced") is not True or row.get("oneshot_exited") is not True
                or row.get("automatic_journal") is not False or row.get("epoch") is not None
                or row.get("enrollment_sha256") != plan["enrollments"].get(host)
                or not isinstance(row.get("journal_sha256"), str)
                or len(row["journal_sha256"]) != 64
                or any(character not in "0123456789abcdef" for character in row["journal_sha256"])
                or row.get("upgrade_lock", {}).get("held") is not True
                or row["upgrade_lock"].get("inode") != plan["locks"].get(host)):
            raise WindowError("bootstrap fence, journal or enrollment differs for " + host)
        broker = row.get("broker", {})
        if (broker.get("health") is not True or broker.get("installed_sha256") != service
                or broker.get("maintenance_durable_protocol") != 1):
            raise WindowError("bootstrap broker health/loaded identity differs for " + host)
        gate = row.get("gate")
        if not isinstance(gate, dict) or type(gate.get("draining")) is not bool:
            raise WindowError("bootstrap gate is unknown for " + host)
        if (broker.get("draining") is not gate["draining"]
                or broker.get("maintenance_owner") != gate.get("owner")):
            raise WindowError("bootstrap gate/broker owner differs for " + host)
    protocol = client_protocol(target)
    final_direction = require_client_transition(protocol, protocol)
    if direction == "forward":
        require_receipt_participants(final_direction, {host: agent for host in rows}, roster)
    return {"mode": "operator-restoration", "automatic_reachability": False,
            "direction": direction, "hosts": sorted(rows), "actions": list(qualification.action_keys),
            "qualified_runtimes": list(qualification.runtimes),
            "source": plan["source"], "target": plan["target"]}
