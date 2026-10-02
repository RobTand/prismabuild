"""Bounded local scratch is a host reservation, charged at claim (#911).

An action that writes scratch to a box's local disk -- a replay spill, a
cotangent sink, a cache root -- already bounds it with a pair of sealed
environment variables: a root and a byte ceiling.  PrismaBuild could not see
those pairs, so it could place two large scratch holders on one box, or one
on a box whose disk cannot hold it, and the action failed closed only after
it had taken its claim.

An action now names its pairs in one more sealed variable,
:data:`PAIRS_ENV`, as ``ROOT_ENV:MAX_ENV`` items separated by commas.  pbrun
derives the reservation from the named ceilings, ``ceil(MAX / 2**30)`` per
pair, and charges it to :data:`KIND` -- the one local-disk host kind a box
declares with ``worker_loop.py --spool-gb`` in the roster (#910).  The
produced-output spool window (#747) draws from the same kind, because both
use the same disk.  No new ledger and no new dispatcher: the kind passes
through the ordinary host ledger like ``mem_gb``.

Off -- :data:`PAIRS_ENV` absent or empty -- nothing is derived and nothing
is read, whatever other variables the environment carries.  A pair is never
inferred from a variable's name. ROOT/MAX declares a reservation, not a quota.

The public SDK also exposes nondestructive ephemeral namespace declarations
(Refs #1360). They match sealed/claim/launch metadata and derive isolated child
names only; they do not register directories or implement scratch cleanup.
"""
from __future__ import annotations

import math
import os
import re
import runpy
import socket
import time
import uuid
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any, cast

# The isolated recorder loads only the sibling owner from its sealed snapshot.
# Core's self-source capture is startup work, outside the I/O measurement.
if __package__:
    from .core import _canonical_file_bytes, _local_scratch_profile_block
    from .core import _positive_finite as _core_positive_finite
else:
    _recipe_core = runpy.run_path(str(Path(__file__).resolve().with_name("core.py")))
    _canonical_file_bytes = _recipe_core["_canonical_file_bytes"]
    _local_scratch_profile_block = _recipe_core["_local_scratch_profile_block"]
    _core_positive_finite = _recipe_core["_positive_finite"]
    del _recipe_core

#: The sealed variable that names an action's bounded-local pairs.
PAIRS_ENV = "PRISMABUILD_LOCAL_SCRATCH_PAIRS"
#: The one local-disk host kind, in GiB like ``mem_gb``.  It keeps the name
#: #747 gave it for the spool window; scratch and spool share it.
KIND = "spool_gb"
GIB = 1 << 30
#: The spool window's own pair.  It is charged through
#: ``PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW`` (#747), and listing it here too
#: would charge the same bytes twice.
SPOOL_PAIR = ("PRISMABUILD_PRODUCED_SPOOL_ROOT",
              "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class LocalScratchError(ValueError):
    """A scratch reservation or ephemeral namespace declaration is invalid."""


def _canonical_root(value: object) -> str | None:
    """``value`` when it is a canonical absolute POSIX path other than ``/``."""

    if (not isinstance(value, str) or not value.startswith("/")
            or "\x00" in value or ".." in PurePosixPath(value).parts
            or str(PurePosixPath(value)) != value or value == "/"):
        return None
    return value


def scratch_pairs(variables: Mapping[str, str]) -> list[dict[str, object]]:
    """The pairs ``variables`` declares, validated, in declaration order.

    Each entry is ``{"root_env", "max_env", "root", "max_bytes"}``.  Raises
    :class:`LocalScratchError` for a malformed list, a name that is not a
    variable name, a name used twice, the spool window's pair, a named
    variable the environment does not carry, a root that is not a canonical
    absolute path, a root declared twice, or a ceiling that is not a
    positive decimal integer.
    """

    raw = variables.get(PAIRS_ENV)
    if raw is None or raw == "":
        return []
    if not isinstance(raw, str):
        raise LocalScratchError(f"{PAIRS_ENV} must be a string")
    pairs: list[dict[str, object]] = []
    names: set[str] = set()
    roots: set[str] = set()
    for item in raw.split(","):
        root_env, sep, max_env = item.partition(":")
        if not sep or ":" in max_env:
            raise LocalScratchError(
                f"{PAIRS_ENV} item {item!r} is not ROOT_ENV:MAX_ENV")
        for name in (root_env, max_env):
            if not _NAME.match(name):
                raise LocalScratchError(
                    f"{PAIRS_ENV} names {name!r}, which is not a variable name")
            if name == PAIRS_ENV:
                raise LocalScratchError(f"{PAIRS_ENV} cannot name itself")
            if name in names:
                raise LocalScratchError(f"{PAIRS_ENV} names {name} twice")
            names.add(name)
        if {root_env, max_env} & set(SPOOL_PAIR):
            raise LocalScratchError(
                f"{PAIRS_ENV} must not list the produced spool's pair: its window "
                "is charged by the declared PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES "
                "(PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW, 1 by default)")
        if root_env not in variables or max_env not in variables:
            missing = [n for n in (root_env, max_env) if n not in variables]
            raise LocalScratchError(
                f"{PAIRS_ENV} declares {root_env}:{max_env}, but the sealed "
                f"environment does not set {', '.join(missing)}")
        root = _canonical_root(variables[root_env])
        if root is None:
            raise LocalScratchError(
                f"{root_env} must be a canonical absolute path other than /, "
                f"not {variables[root_env]!r}")
        if root in roots:
            raise LocalScratchError(f"{PAIRS_ENV} declares the root {root} twice")
        roots.add(root)
        maximum = variables[max_env]
        if (not isinstance(maximum, str) or not maximum.isascii()
                or not maximum.isdigit() or int(maximum) <= 0):
            raise LocalScratchError(
                f"{max_env} must be a positive integer byte ceiling, not {maximum!r}")
        pairs.append({"root_env": root_env, "max_env": max_env,
                      "root": root, "max_bytes": int(maximum)})
    return pairs


def scratch_terms(variables: Mapping[str, str]) -> dict[str, int]:
    """The host demand an environment's declared pairs derive, or ``{}``.

    Each pair's ceiling is rounded up to whole GiB on its own, so each
    reservation covers its own bound; the terms are their sum.
    """

    total = sum(-(-cast(int, pair["max_bytes"]) // GIB) for pair in scratch_pairs(variables))
    return {KIND: total} if total else {}


# -- nondestructive ephemeral namespace declarations (Refs #1360) -----------

#: Naming schema only: this is not a cleanup/lifetime capability.
EPHEMERAL_SCRATCH_SCHEMA_V1 = "prismabuild.ephemeral_scratch.v1"
_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_HOST_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}\Z")
_EPHEMERAL_FIELDS = frozenset({
    "schema", "lifetime", "root_env", "max_env", "root", "max_bytes", "name",
    "owner_action_key", "owner_published_unix", "owner_host", "owner_attempt"})


def _scratch_hex(value: object, length: int, field: str) -> str:
    if (not isinstance(value, str) or len(value) != length
            or any(c not in "0123456789abcdef" for c in value)):
        raise LocalScratchError(f"{field} must be {length} lowercase hex characters")
    return value


def _scratch_time(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LocalScratchError(f"{field} must be a finite number")
    try:
        result = float(value)
    except OverflowError:
        raise LocalScratchError(f"{field} must be a finite number") from None
    if not math.isfinite(result):
        raise LocalScratchError(f"{field} must be a finite number")
    return result


def _scratch_attempt(value: object, key: str) -> dict[str, str]:
    from .produced_output import _broker_scope_id

    if not isinstance(value, Mapping) or set(value) != {"nonce", "scope_id"}:
        raise LocalScratchError("owner_attempt must contain nonce and scope_id")
    nonce = _scratch_hex(value["nonce"], 32, "nonce")
    scope_id = value["scope_id"]
    if scope_id != _broker_scope_id(key, nonce):
        raise LocalScratchError("scope_id does not match the action/nonce broker formula")
    return {"nonce": nonce, "scope_id": scope_id}


def _scratch_host(value: object) -> str:
    if not isinstance(value, str) or not _HOST_COMPONENT.fullmatch(value):
        raise LocalScratchError("owner_host must be one ASCII host-name component")
    return value


def _scratch_declaration(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _EPHEMERAL_FIELDS:
        raise LocalScratchError("ephemeral scratch declaration fields do not match the schema")
    if (value["schema"] != EPHEMERAL_SCRATCH_SCHEMA_V1
            or value["lifetime"] != "ephemeral"):
        raise LocalScratchError("scratch declaration must use the ephemeral naming schema")
    root_env, max_env = value["root_env"], value["max_env"]
    for variable in (root_env, max_env):
        if (not isinstance(variable, str) or not _NAME.fullmatch(variable)
                or variable in (PAIRS_ENV, *SPOOL_PAIR)):
            raise LocalScratchError("scratch pair must name nonreserved environment variables")
    if root_env == max_env:
        raise LocalScratchError("scratch root and ceiling must use distinct variables")
    root = _canonical_root(value["root"])
    if root is None or root.startswith("//"):
        raise LocalScratchError("scratch root must be a canonical absolute path other than /")
    maximum = value["max_bytes"]
    if type(maximum) is not int or maximum <= 0:
        raise LocalScratchError("max_bytes must be a positive integer")
    name = value["name"]
    if not isinstance(name, str) or not _COMPONENT.fullmatch(name):
        raise LocalScratchError("scratch name must be one ASCII path component")
    key = _scratch_hex(value["owner_action_key"], 64, "owner_action_key")
    return {**value, "root": root,
            "owner_published_unix": _scratch_time(value["owner_published_unix"],
                                                 "owner_published_unix"),
            "owner_host": _scratch_host(value["owner_host"]),
            "owner_attempt": _scratch_attempt(value["owner_attempt"], key)}


def ephemeral_scratch_path(declaration: Mapping[str, object]) -> Path:
    """Derive an ephemeral child namespace, with no filesystem operations.

    Checks the exact naming schema, not its authenticity or current ownership.
    No stat/open, mkdir, allocation, quota, symlink/inode check, registration or
    deletion authority is provided. A returned path is NOT cleanup permission.
    """

    from .core import canonical_sha256

    checked = _scratch_declaration(declaration)
    # Same submission-generation recipe as PoolQueue.attempt_generation;
    # tests pin this agreement. This is not the runtime generation's identity.
    generation = canonical_sha256({
        "action_key": checked["owner_action_key"],
        "published_unix": checked["owner_published_unix"]})
    attempt = checked["owner_attempt"]
    assert isinstance(attempt, Mapping)
    return (Path(str(checked["root"])) / "prismabuild-ephemeral"
            / canonical_sha256({"host": checked["owner_host"]})
            / str(checked["owner_action_key"]) / generation
            / str(attempt["nonce"]) / str(checked["name"]))


def _scratch_claim_envelope(claim: object) -> dict[str, object]:
    """Complete typed metadata used for matching, not containment proof."""

    if not isinstance(claim, Mapping):
        raise LocalScratchError("no complete claimed record")
    key = _scratch_hex(claim.get("action_key"), 64, "action_key")
    _scratch_time(claim.get("published_unix"), "published_unix")
    _scratch_time(claim.get("claimed_unix"), "claimed_unix")
    attempts = claim.get("attempts")
    if type(attempts) is not int or attempts < 0:
        raise LocalScratchError("claim attempts must be a nonnegative integer")
    _scratch_host(claim.get("claimed_host"))
    worker = claim.get("claimed_by")
    if not isinstance(worker, str) or not worker.strip() or not worker.isprintable():
        raise LocalScratchError("claim must name its worker")
    cas_root = _canonical_root(claim.get("cas_root"))
    if cas_root is None or cas_root.startswith("//"):
        raise LocalScratchError("claim must name a canonical absolute CAS root")
    control = claim.get("resource_scope")
    if not isinstance(control, Mapping) or control.get("action_key") != key:
        raise LocalScratchError("no complete matching control context")
    attempt = _scratch_attempt({field: control.get(field)
                                for field in ("nonce", "scope_id")}, key)
    resources = claim.get("resources")
    if (not isinstance(resources, Mapping) or type(resources.get(KIND)) is not int
            or resources[KIND] <= 0):
        raise LocalScratchError("claim must carry a positive integer scratch reservation")
    fields = ("action_key", "cas_root", "claimed_by", "claimed_host",
              "claimed_unix", "published_unix", "attempts", "resources")
    return {**{field: claim[field] for field in fields}, "owner_attempt": attempt}


def bind_ephemeral_scratch(queue, *, root_env: str, name: str,
                           claim_snapshot: Mapping[str, object],
                           env: Mapping[str, str] | None = None) -> dict[str, object]:
    """Bind a nondestructive naming declaration to sealed and claim metadata.

    ROOT/MAX comes only from the validated sealed request. The complete claim
    snapshot, current claimed row, launch nonce/scope and broker-formula control
    must agree; the host uses the queue's existing holder resolution. Reads are
    not an atomic snapshot and do not prove live broker/cgroup membership, real
    funding, filesystem locality or stopped descendants. No directory or queue
    record is written, no resource is allocated/released, and nothing is deleted.
    """

    from . import core, pool

    try:
        expected = _scratch_claim_envelope(claim_snapshot)
        key = str(expected["action_key"])
        live = pool._read_json(queue.item_path(pool.CLAIMED, key))
        observed = _scratch_claim_envelope(live)
        if core.canonical_sha256(observed) != core.canonical_sha256(expected):
            raise LocalScratchError("claim snapshot no longer matches the live envelope")
        source = os.environ if env is None else env
        if not isinstance(source, Mapping):
            raise LocalScratchError("launch environment must be a mapping")
        attempt = observed["owner_attempt"]
        assert isinstance(attempt, Mapping)
        if (source.get(core.ACTION_KEY_ENV) != key
                or source.get(core.ACTION_NONCE_ENV) != attempt["nonce"]
                or source.get(core.ACTION_SCOPE_ENV) != attempt["scope_id"]):
            raise LocalScratchError("launch and control context are incomplete or disagree")
        host = _scratch_host(queue.resolve_claim_holder(key, live))
        action = pool._sealed_action_request(str(observed["cas_root"]), key)
        if action is None:
            raise LocalScratchError("no sealed action request for scratch binding")
        environment = action["environment"]
        assert isinstance(environment, Mapping)
        raw_variables = environment["variables"]
        assert isinstance(raw_variables, Mapping)
        variables = cast(Mapping[str, str], raw_variables)
        pairs = scratch_pairs(variables)
        pair = next((pair for pair in pairs if pair["root_env"] == root_env), None)
        if pair is None:
            raise LocalScratchError("root_env does not name a sealed scratch pair")
        params = action["params"]
        assert isinstance(params, Mapping)
        demand = params.get("demand")
        charged = demand.get(KIND) if isinstance(demand, Mapping) else None
        resources = observed["resources"]
        assert isinstance(resources, Mapping)
        if (type(charged) is not int or cast(int, charged) < scratch_terms(variables)[KIND]
                or resources[KIND] != charged):
            raise LocalScratchError("sealed scratch demand and claim reservation disagree")
        declaration = _scratch_declaration({
            "schema": EPHEMERAL_SCRATCH_SCHEMA_V1, "lifetime": "ephemeral",
            **pair, "name": name, "owner_action_key": key,
            "owner_published_unix": observed["published_unix"],
            "owner_host": host, "owner_attempt": attempt})
        # Refuse a successor observed during the sealed-request/holder reads.
        # This recheck cannot make shared reads atomic or authorize a mutation.
        final = _scratch_claim_envelope(pool._read_json(queue.item_path(pool.CLAIMED, key)))
        if core.canonical_sha256(final) != core.canonical_sha256(expected):
            raise LocalScratchError("claim changed during scratch binding")
        return declaration
    except (OSError, ValueError, TypeError,
            core.CASTamperError, core.CASUnavailableError) as exc:
        if isinstance(exc, LocalScratchError):
            raise
        raise LocalScratchError(f"scratch binding refused: {exc}") from exc


# -- durable declaration evidence, not required cleanup (Refs #1360) --------

DECLARATIONS_ENV = "PRISMABUILD_EPHEMERAL_SCRATCH_DECLARATIONS"
SCRATCH_DECLARATION_RECORD_SCHEMA_V1 = "prismabuild.scratch_declaration_record.v1"
_MAX_DECLARATION_INPUT_BYTES = 16 * 1024
_MAX_DECLARATIONS = 64
_MAX_DECLARATION_RECORD_BYTES = 256 * 1024
_DECLARATION_RECORD_FIELDS = frozenset({
    "schema", "purpose", "cleanup_required", "claim_envelope", "declarations"})


def _sealed_scratch_variables(claim: Mapping[str, object], *,
                              allow_missing: bool = False) -> Mapping[str, str]:
    from . import pool

    key = _scratch_hex(claim.get("action_key"), 64, "action_key")
    root = _canonical_root(claim.get("cas_root"))
    if root is None:
        raise LocalScratchError("scratch declarations need a canonical CAS root")
    action = pool._sealed_action_request(root, key)
    if action is None:
        if allow_missing:
            # Only the existing executor's proven-absent-request legacy path.
            # Unreadable/malformed/mismatched requests still propagate refusal.
            return {}
        raise LocalScratchError("no sealed action request for scratch declarations")
    environment = action["environment"]
    assert isinstance(environment, Mapping)
    variables = environment["variables"]
    assert isinstance(variables, Mapping)
    return cast(Mapping[str, str], variables)


def _scratch_selection_input(variables: Mapping[str, str]) -> object:
    """Decode the one bounded, explicit sealed selection input."""
    from . import core

    raw = variables.get(DECLARATIONS_ENV)
    if raw is None or raw == "":
        return None
    if not isinstance(raw, str):
        raise LocalScratchError("scratch declaration selection must be JSON text")
    try:
        data = raw.encode("utf-8")
        if len(data) > _MAX_DECLARATION_INPUT_BYTES:
            raise LocalScratchError("scratch declaration selection exceeds 16 KiB")
        selected = core._decode_strict_json(data, where="sealed scratch declaration selection")
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise LocalScratchError(f"invalid scratch declaration selection: {exc}") from exc
    if selected is None:
        raise LocalScratchError("scratch declaration selection must not be JSON null")
    return selected


def _scratch_selections(variables: Mapping[str, str]) -> list[dict[str, str]]:
    """Read only legacy naming selections; lifetime requests need their owner."""
    selected = _scratch_selection_input(variables)
    if selected is None:
        return []
    if not isinstance(selected, list) or len(selected) > _MAX_DECLARATIONS:
        raise LocalScratchError("scratch declaration selection must be a list of at most 64 entries")
    if not selected:
        return []
    roots = {pair["root_env"] for pair in scratch_pairs(variables)}
    result: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for entry in selected:
        if not isinstance(entry, dict) or set(entry) != {"root_env", "name"}:
            raise LocalScratchError("scratch selection entries must contain only root_env and name")
        root, name = entry["root_env"], entry["name"]
        if not isinstance(root, str) or root not in roots:
            raise LocalScratchError("scratch selection root_env is not a sealed scratch pair")
        if not isinstance(name, str) or not _COMPONENT.fullmatch(name):
            raise LocalScratchError("scratch selection name must be one ASCII path component")
        if (root, name) in seen:
            raise LocalScratchError("duplicate scratch declaration selection")
        seen.add((root, name))
        result.append({"root_env": root, "name": name})
    return result


SCRATCH_LIFETIME_SELECTION_SCHEMA_V1 = "prismabuild.scratch_lifetime_selection.v1"
SCRATCH_LIFETIME_TAG = "scratch-lifetime-v1"


def _scratch_lifetime_selections(variables: Mapping[str, str]) -> list[dict[str, object]]:
    """Validate versioned lifetime intent without granting filesystem authority.

    Legacy arrays retain naming-only semantics. Ephemeral and persistent root
    pairs cannot overlap; intent never creates, traverses, or removes a path.
    """
    selected = _scratch_selection_input(variables)
    if selected is None:
        return []
    if isinstance(selected, list):
        _scratch_selections(variables)
        return []
    if (not isinstance(selected, dict) or set(selected) != {"schema", "entries"}
            or selected["schema"] != SCRATCH_LIFETIME_SELECTION_SCHEMA_V1):
        raise LocalScratchError("scratch lifetime selection has an unknown schema or fields")
    entries = selected["entries"]
    if not isinstance(entries, list) or len(entries) > _MAX_DECLARATIONS:
        raise LocalScratchError("scratch lifetime selection needs at most 64 entries")
    if not entries:
        return []
    for entry in entries:
        if (not isinstance(entry, dict) or set(entry) != {"root_env", "name", "lifetime"}
                or entry["lifetime"] not in ("ephemeral", "persistent")):
            raise LocalScratchError("scratch lifetime entries need root_env, name and a known lifetime")
    names = [{"root_env": entry["root_env"], "name": entry["name"]} for entry in entries]
    # Naming validation and pair accounting keep their existing authoritative home.
    import json
    _scratch_selections({**variables, DECLARATIONS_ENV: json.dumps(names)})
    pairs = {pair["root_env"]: pair for pair in scratch_pairs(variables)}
    result = [{**entry, **pairs[entry["root_env"]]} for entry in entries]
    temporary = [PurePosixPath(str(entry["root"])) for entry in result
                 if entry["lifetime"] == "ephemeral"]
    persistent = [PurePosixPath(str(entry["root"])) for entry in result
                  if entry["lifetime"] == "persistent"]
    if any(first == second or first in second.parents or second in first.parents
           for first in temporary for second in persistent):
        raise LocalScratchError("ephemeral and persistent scratch root pairs overlap")
    return result


def _scratch_declaration_record(value: object) -> dict[str, object]:
    """Validate audit evidence only; never a cleanup completion or authority."""
    from . import core

    if (not isinstance(value, Mapping) or set(value) != _DECLARATION_RECORD_FIELDS
            or value["schema"] != SCRATCH_DECLARATION_RECORD_SCHEMA_V1
            or value["purpose"] != "declaration-only"
            or type(value["cleanup_required"]) is not bool or value["cleanup_required"]):
        raise LocalScratchError("scratch declaration record is not declaration-only evidence")
    envelope = value["claim_envelope"]
    if not isinstance(envelope, Mapping):
        raise LocalScratchError("scratch declaration record lacks its claim envelope")
    attempt = envelope.get("owner_attempt")
    if not isinstance(attempt, Mapping):
        raise LocalScratchError("scratch declaration record lacks its attempt identity")
    checked = _scratch_claim_envelope({
        **envelope, "resource_scope": {"action_key": envelope.get("action_key"), **attempt}})
    if core.canonical_sha256(checked) != core.canonical_sha256(envelope):
        raise LocalScratchError("scratch declaration envelope fields disagree")
    declarations = value["declarations"]
    if not isinstance(declarations, list) or not 0 < len(declarations) <= _MAX_DECLARATIONS:
        raise LocalScratchError("scratch declaration record needs 1 to 64 declarations")
    seen: set[tuple[object, object]] = set()
    normalized = []
    for raw in declarations:
        declaration = _scratch_declaration(raw)
        if (declaration["owner_action_key"] != checked["action_key"]
                or declaration["owner_published_unix"] != checked["published_unix"]
                or declaration["owner_host"] != checked["claimed_host"]
                or declaration["owner_attempt"] != checked["owner_attempt"]):
            raise LocalScratchError("scratch declaration record has a foreign owner")
        identity = (declaration["root_env"], declaration["name"])
        if identity in seen:
            raise LocalScratchError("scratch declaration record contains duplicates")
        seen.add(identity)
        normalized.append(declaration)
    record = {**value, "claim_envelope": checked, "declarations": normalized}
    if len(core._canonical_bytes(record)) > _MAX_DECLARATION_RECORD_BYTES:
        raise LocalScratchError("scratch declaration record exceeds 256 KiB")
    return record


def record_ephemeral_scratch_declarations(
    queue, *, claim_snapshot: Mapping[str, object],
    env: Mapping[str, str] | None = None,
) -> dict[str, object] | None:
    """Commit sealed naming evidence through the queue's existing key lock.

    Returns None when off. Persistence failures propagate. This is not required
    cleanup registration, filesystem/quota or broker authenticity proof; no
    directory is created, traversed or removed and no capacity is changed.
    """
    if not isinstance(claim_snapshot, Mapping):
        raise LocalScratchError("scratch recording needs a claim snapshot mapping")
    key = _scratch_hex(claim_snapshot.get("action_key"), 64, "action_key")
    return queue._record_ephemeral_scratch_declarations(
        key, claim_snapshot=claim_snapshot,
        env=os.environ if env is None else env)


# -- the live offer: measured by the supervisor, read by the loops (#1190) ---

#: Where each box keeps its live ``--spool-gb`` measurement, carried from the
#: supervisor to its loops (#1190): host-local and persistent, beside the
#: supervisor's claim file and the local checkouts.  Not
#: ``adaptive_cpu.box_state``'s directory, which defaults under ``/tmp``: the
#: fleet writes nothing new there, because an OOM once cleared it.
OFFER_ROOT = Path(os.environ.get("PRISMABUILD_SPOOL_OFFER_ROOT")
                  or "/home/rob/tmp/prismabuild-spool-offer")
OFFER_SUFFIX = ".spool-offer.json"


def spool_offer_path(ledger_base) -> Path:
    """Where the box whose host ledger is ``ledger_base`` keeps its live offer.

    The supervisor and its loops name the same ledger, so they meet here
    without a round trip through the shared queue.  The name is the digest
    ``adaptive_cpu.box_identity`` gives the box's other host-local state.
    """

    from . import adaptive_cpu

    return Path(OFFER_ROOT) / (adaptive_cpu.box_identity(ledger_base) + OFFER_SUFFIX)


def _private_offer_root() -> Path:
    """``OFFER_ROOT``, created private to this uid, or ``RuntimeError``."""

    import stat

    directory = Path(OFFER_ROOT)
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError(f"unsafe spool offer directory {directory}")
    if info.st_mode & 0o077:
        directory.chmod(0o700)
    return directory


def write_spool_offer(ledger_base, gib: int, detail: str) -> None:
    """Record ``gib`` as the box's measured ``spool_gb`` offer, atomically.

    Only the supervisor writes it, and only from a measurement taken while
    the host ledger held no ``spool_gb`` (see ``supervise._spool_budget``).
    """

    from . import adaptive_cpu

    if type(gib) is not int or gib < 0:
        raise ValueError(f"a spool offer is a whole number of GiB, not {gib!r}")
    _private_offer_root()
    adaptive_cpu.write_json(spool_offer_path(ledger_base), {
        KIND: gib, "detail": str(detail), "measured_unix": time.time(),
        "pid": os.getpid()})


def clear_spool_offer(ledger_base) -> None:
    """Remove the recorded offer: this process has no measurement to give."""

    spool_offer_path(ledger_base).unlink(missing_ok=True)


def read_spool_offer(ledger_base) -> int | None:
    """The recorded offer in GiB, or ``None`` when there is none to trust.

    A missing or unreadable file -- a first start, or a file removed because
    this supervisor has not measured -- is "no information", never a zero.
    """

    from . import adaptive_cpu

    try:
        path = spool_offer_path(ledger_base)
    except OSError:
        return None
    value = adaptive_cpu.read_json(path).get(KIND)
    return value if type(value) is int and value >= 0 else None


# -- opt-in measured I/O service placement (Refs #1182) ----------------------
IO_ENV = "PRISMABUILD_LOCAL_SCRATCH_IO"
IO_SCHEMA = "prismabuild.local_scratch_io.v1"
IO_CAPABILITY = "local-scratch-io-v1"
PROFILE_SCHEMA = "prismabuild.local_scratch_io_profile.v1"
CONFIG_SCHEMA = "prismabuild.local_scratch_io_profiles.v1"
PLACEMENT_SCHEMA = "prismabuild.local_scratch_io_placement.v1"
PROFILES = "local_scratch_io_profiles"
DEVICES = "local_scratch_devices"
PLACEMENT = "local_scratch_io_placement"
PROFILE_CONTRACT = "prismabuild.local_scratch_io.buffered_seq_sync.v1"
PROFILE_METHOD = "buffered-sequential-write-fdatasync-read.v1"
PROFILE_PATTERN = "repeated-shake256-block.v1"
RECORDER = "tools/fleet/local_scratch_profile.py"
PROFILE_RESULT = "prismabuild-local-scratch-profile.json"
PROFILE_ROOT_ENV = "PQ_PROFILE_ROOT"
PROFILE_MAX_ENV = "PQ_PROFILE_MAX_BYTES"
PROFILE_OBSERVATION_ENV = "PRISMABUILD_PROFILE_OBSERVATION_ID"
PROFILE_PATH = "/usr/bin:/bin"
PROFILE_OWNER_ENV = "PRISMABUILD_CONTAINER_OWNER"
PROFILE_MARKER_ENV = "PRISMABUILD_CONTAINER_MARKER"
PRODUCER_FILES = (RECORDER, "src/prismabuild/local_scratch.py", "src/prismabuild/core.py")
LOCAL_FILESYSTEM_TYPES = frozenset({"ext4", "xfs", "btrfs", "zfs"})
#: Host-local *state* -- admission fences and the measurement census -- is a
#: small-file rendezvous, not an I/O-qualified scratch profile, so its closed
#: policy adds tmpfs and nothing else (#1451).  Shared storage and unknown
#: mounts still refuse; the disk-scratch policy above is unchanged.
LOCAL_STATE_FILESYSTEM_TYPES = frozenset({"ext4", "xfs", "btrfs", "zfs", "tmpfs"})


def _is_positive_finite(value: object) -> bool:
    """Predicate adapter; retain the caller's original int/float value."""
    try:
        _core_positive_finite(value, where="scratch positive finite observation")
    except (ValueError, OverflowError):
        return False
    return True


def io_intent(variables, *, transport="pool"):
    """Optional sealed traffic, never inferred from occupancy or host rates."""
    if IO_ENV not in variables:
        return None
    from . import core
    try:
        raw = variables[IO_ENV]
        if not isinstance(raw, str):
            raise TypeError("traffic declaration is not text")
        value = core._decode_strict_json(raw.encode(), where=IO_ENV)
    except (TypeError, ValueError) as exc:
        raise LocalScratchError(f"{IO_ENV} must be versioned JSON") from exc
    keys = {"schema", "write_bytes", "read_bytes", "profile_contract", "max_profile_age_s"}
    if not isinstance(value, dict) or set(value) != keys or value["schema"] != IO_SCHEMA:
        raise LocalScratchError(f"{IO_ENV} requires exactly the {IO_SCHEMA} fields")
    if any(type(value[d]) is not int or value[d] < 0 for d in ("write_bytes", "read_bytes")):
        raise LocalScratchError(f"{IO_ENV} traffic must be nonnegative integers")
    if not value["write_bytes"] and not value["read_bytes"]:
        raise LocalScratchError(f"{IO_ENV} requires positive traffic")
    contract = value["profile_contract"]
    if not isinstance(contract, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", contract):
        raise LocalScratchError(f"{IO_ENV} requires an explicit profile_contract")
    if not _is_positive_finite(value["max_profile_age_s"]):
        raise LocalScratchError(f"{IO_ENV} max_profile_age_s must be positive finite")
    pairs = scratch_pairs(variables)
    if transport != "pool" or len(pairs) != 1:
        raise LocalScratchError(f"{IO_ENV} requires pool and exactly one scratch pair")
    return {"declaration": value, "root": pairs[0]["root"]}


def _descriptor_mount(fd):
    """Exact open-object device/FSID, with mount type from its Linux mount ID.

    FSID is an identity, not the filesystem type. No pathname-prefix mount
    inference, ancestor fallback or operator type override is accepted.  This
    observer applies no purpose policy: each caller compares
    ``filesystem_type`` against its own closed allowlist, so the disk-scratch
    profile qualification and the host-local state check cannot drift (#1451).
    """
    info = os.fstat(fd)
    import stat
    if not stat.S_ISDIR(info.st_mode):
        raise LocalScratchError("scratch root is not a directory")
    fsid = getattr(os.fstatvfs(fd), "f_fsid", None)
    if type(fsid) is not int:
        raise LocalScratchError("scratch descriptor filesystem ID unknown")
    ids = [line.split(":", 1)[1].strip() for line in
           Path(f"/proc/self/fdinfo/{fd}").read_text().splitlines()
           if line.startswith("mnt_id:")]
    if len(ids) != 1 or not ids[0].isascii() or not ids[0].isdigit():
        raise LocalScratchError("scratch descriptor mount ID unknown")
    matches = [line.split(" - ", 1) for line in
               Path("/proc/self/mountinfo").read_text().splitlines()
               if line.split(" ", 1)[0] == ids[0]]
    if len(matches) != 1 or len(matches[0]) != 2:
        raise LocalScratchError("scratch descriptor mount identity unknown")
    fields = matches[0][1].split()
    if len(fields) < 3:
        raise LocalScratchError("scratch descriptor mount type malformed")
    return {"device": str(info.st_dev), "filesystem": str(fsid),
            "filesystem_type": fields[0], "root_inode": str(info.st_ino)}


def _descriptor_identity(fd):
    """One disk-scratch I/O profile root, under its closed filesystem policy.

    ``LOCAL_FILESYSTEM_TYPES`` is the qualified local disk set.  tmpfs is an
    I/O profile refusal here and stays one: the profile measurement does not
    transfer across filesystems, and this predicate is unchanged (#1451).
    """
    identity = _descriptor_mount(fd)
    filesystem_type = identity["filesystem_type"]
    if filesystem_type not in LOCAL_FILESYSTEM_TYPES:
        raise LocalScratchError(f"scratch filesystem type not supported: {filesystem_type}")
    return identity


def _descriptor_state_identity(fd):
    """One host-local state directory, under its closed filesystem policy.

    Admission fences and the measurement census write small regular files into
    a private rendezvous; that is not an I/O-qualified scratch profile, so this
    purpose policy accepts the local disk set and tmpfs -- the ``/tmp`` mount
    the affected worker's ``BOX_STATE_ROOT`` lives on -- and nothing else.
    Shared storage (NFS and friends) and unknown types still refuse (#1451).
    """
    identity = _descriptor_mount(fd)
    filesystem_type = identity["filesystem_type"]
    if filesystem_type not in LOCAL_STATE_FILESYSTEM_TYPES:
        raise LocalScratchError(
            f"state filesystem type not supported: {filesystem_type}")
    return identity


def root_identity(root):
    if _canonical_root(root) is None:
        raise LocalScratchError("scratch root must be canonical absolute")
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        return _descriptor_identity(fd)
    finally:
        os.close(fd)


def profile_command(command):
    """Recognize only the isolated, checkout-root recorder entrypoint.

    Returns None for all ordinary commands. Named recorder commands with an
    ambiguous envelope refuse rather than being silently reclassified.
    """
    if not isinstance(command, (list, tuple)) or any(not isinstance(token, str) for token in command):
        raise LocalScratchError("scratch recorder command must be an argv sequence of strings")
    if RECORDER not in command:
        return None
    if (len(command) != 14 or command[1:4] != ["-I", "-S", RECORDER]
            or not str(command[0]).startswith("/")
            or not Path(command[0]).name.startswith("python")
            or command[4::2] != ["--root", "--bytes", "--block-bytes", "--repetitions", "--result"]
            or command[13] != PROFILE_RESULT):
        raise LocalScratchError("scratch recorder requires direct PYTHON -I -S and exact file-result envelope")
    root = command[5]
    counts = command[7], command[9], command[11]
    if (_canonical_root(root) is None or any(not isinstance(c, str) or not c.isascii()
            or not c.isdigit() or int(c) <= 0 for c in counts)):
        raise LocalScratchError("scratch recorder requires canonical root and positive integer sizes")
    size, block, repetitions = map(int, counts)
    if block > size:
        raise LocalScratchError("scratch recorder block exceeds bytes")
    return {"root": root, "bytes": size, "block_bytes": block, "repetitions": repetitions}


def profile_environment(variables, *, derived=False):
    """Closed recorder environment, never startup/loader or custom PATH hooks."""
    defaults = {"HOME": "/home/rob", "TMPDIR": "/home/rob/tmp",
                "TRITON_CACHE_DIR": "/home/rob/.triton-cache", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    threads = {"OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"}
    allowed = set(defaults) | threads | {"PATH", "CUDA_VISIBLE_DEVICES", PAIRS_ENV,
               PROFILE_ROOT_ENV, PROFILE_MAX_ENV, PROFILE_OBSERVATION_ENV}
    if derived:
        allowed |= {PROFILE_OWNER_ENV, PROFILE_MARKER_ENV}
    if set(variables) - allowed:
        raise LocalScratchError("scratch recorder environment contains unsupported startup/loader/custom fields")
    if any(k in variables and variables[k] != v for k, v in defaults.items()):
        raise LocalScratchError("scratch recorder requires exact default environment values")
    paths = {PROFILE_PATH} if derived else {PROFILE_PATH, "/usr/local/bin:/usr/bin:/bin"}
    if (("PATH" in variables and variables["PATH"] not in paths)
            or any(k in variables and variables[k] != "1" for k in threads)
            or ("CUDA_VISIBLE_DEVICES" in variables and variables["CUDA_VISIBLE_DEVICES"] != "")):
        raise LocalScratchError("scratch recorder PATH/thread/CUDA environment differs")
    observation = variables.get(PROFILE_OBSERVATION_ENV)
    if observation is not None and (not isinstance(observation, str)
                                   or not re.fullmatch(r"[A-Za-z0-9_.-]+", observation)):
        raise LocalScratchError("scratch observation identity must be an explicit token")
    if derived:
        owner, marker = variables.get(PROFILE_OWNER_ENV), variables.get(PROFILE_MARKER_ENV)
        if (variables.get("PATH") != PROFILE_PATH or not isinstance(owner, str)
                or not re.fullmatch(r"[a-f0-9]{64}", owner) or _canonical_root(marker) is None
                or Path(marker).name != f"{owner}.used" or variables.get("CUDA_VISIBLE_DEVICES") != ""):
            raise LocalScratchError("scratch recorder derived owner/marker/CUDA environment invalid")


def check_profile_request(command, variables, demand, *, cwd, determinism, derived=False):
    envelope = profile_command(command)
    if envelope is None:
        return None
    if (not isinstance(variables, Mapping) or not isinstance(demand, Mapping)
            or any(not isinstance(k, str) or not isinstance(v, str) for k, v in variables.items())
            or any(not isinstance(k, str) or type(v) is not int or v < 0 for k, v in demand.items())):
        raise LocalScratchError("scratch recorder environment/demand containers invalid")
    pairs = scratch_pairs(variables)
    profile_environment(variables, derived=derived)
    if (cwd != "." or io_intent(variables) is not None or demand.get("gpu")
            or determinism != "stochastic" or len(pairs) != 1
            or type(demand.get("cpu")) is not int or demand["cpu"] <= 0
            or type(demand.get("mem_gb")) is not int or demand["mem_gb"] <= 0
            or (pairs[0]["root_env"], pairs[0]["max_env"]) != (PROFILE_ROOT_ENV, PROFILE_MAX_ENV)
            or pairs[0]["root"] != envelope["root"]
            or cast(int, pairs[0]["max_bytes"]) < envelope["bytes"]):
        raise LocalScratchError("scratch recorder requires CPU-only stochastic root-checkout, "
                                "one sufficient pair and no I/O-placement declaration")
    if demand.get(KIND, 0) < scratch_terms(variables)[KIND]:
        raise LocalScratchError("scratch recorder demand does not hold its declared occupancy")
    return envelope


def record_profile(envelope):
    """Bounded buffered sequential calls + write fdatasync + warm reads.

    This scope is NOT physical-disk-read bandwidth, cache eviction, pressure
    qualification or a representative spill benchmark. Only admitted PB
    children call this function; the CLI enforces the action environment.
    """
    root = envelope["root"]
    size, block, repetitions = (envelope[k] for k in ("bytes", "block_bytes", "repetitions"))
    # Allocate the deterministic incompressible block before timing I/O calls.
    payload = _local_scratch_profile_block(block)
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    name = f".prismabuild-scratch-profile-{uuid.uuid4().hex}"
    fd = None
    start = time.time()
    body = {"schema": PROFILE_SCHEMA, "host": socket.gethostname(), "root": root,
            "profile_contract": PROFILE_CONTRACT, "method": PROFILE_METHOD,
            "envelope": dict(envelope), "pattern": PROFILE_PATTERN,
            "producer_action_key": os.environ.get("PRISMABUILD_ACTION_KEY", ""),
            "measured_unix": start, "started_unix": start,
            "write_bytes": 0, "read_bytes": 0, "write_elapsed_s": 0., "read_elapsed_s": 0.,
            "completed": False, "errors": []}
    try:
        identity = _descriptor_identity(directory)
        body.update(identity, start_identity=identity)
        fd = os.open(name, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600, dir_fd=directory)
        if (str(os.fstat(fd).st_dev) != identity["device"]
                or str(os.fstatvfs(fd).f_fsid) != identity["filesystem"]):
            raise LocalScratchError("scratch profile file not on root filesystem")
        for _ in range(repetitions):
            os.lseek(fd, 0, os.SEEK_SET)
            begin = time.perf_counter()
            remaining = size
            while remaining:
                count = os.write(fd, memoryview(payload)[:min(block, remaining)])
                if count <= 0:
                    raise OSError("scratch write made no progress")
                remaining -= count
                body["write_bytes"] += count
            os.fdatasync(fd)
            body["write_elapsed_s"] += time.perf_counter() - begin
            os.lseek(fd, 0, os.SEEK_SET)
            begin = time.perf_counter()
            remaining = size
            while remaining:
                data = os.read(fd, min(block, remaining))
                if not data:
                    raise OSError("scratch read ended early")
                remaining -= len(data)
                body["read_bytes"] += len(data)
            body["read_elapsed_s"] += time.perf_counter() - begin
        end_identity = _descriptor_identity(directory)
        body["end_identity"] = end_identity
        if end_identity != identity or root_identity(root) != identity:
            raise LocalScratchError("scratch root identity changed during measurement")
        if os.fstat(fd).st_size != size or any(not _is_positive_finite(body[f"{d}_elapsed_s"])
                                              for d in ("write", "read")):
            raise LocalScratchError("scratch measurement size/elapsed incomplete")
        body["completed"] = True
    except (OSError, ValueError) as exc:
        body["errors"].append(f"{type(exc).__name__}: {exc}")
    finally:
        body["ended_unix"] = time.time()
        if fd is not None:
            os.close(fd)
            os.unlink(name, dir_fd=directory)
        os.close(directory)
    return body


def profile_cost(offer, intent, *, now):
    """Validate the newest observed profile, derive only required rates.

    Offers are the qualified worker trust boundary, like CPU/GPU samples.
    Worker qualification verifies actual executed recorder receipts; generic
    library callers may supply labelled synthetic observations for tests.
    """
    from . import core
    if not isinstance(offer, Mapping):
        raise LocalScratchError("scratch observation malformed")
    detail = offer.get("observed_detail")
    if not isinstance(detail, Mapping):
        raise LocalScratchError("scratch observed detail malformed")
    profiles = detail.get(PROFILES)
    root, declaration = intent["root"], intent["declaration"]
    if not isinstance(profiles, list) or not profiles:
        raise LocalScratchError("scratch profile missing")
    relevant = []
    for profile in profiles:
        if not isinstance(profile, dict):
            raise LocalScratchError("scratch profile malformed")
        if profile.get("root") == root:
            stamp = profile.get("measured_unix")
            if not _is_positive_finite(stamp):
                raise LocalScratchError("scratch profile original time invalid")
            relevant.append(profile)
    if not relevant:
        raise LocalScratchError("scratch profile root missing")
    latest = max(p["measured_unix"] for p in relevant)
    newest = [p for p in relevant if p["measured_unix"] == latest]
    if len(newest) != 1:
        raise LocalScratchError("scratch newest profile ambiguous")
    profile = newest[0]
    envelope = profile.get("envelope")
    if (not isinstance(envelope, dict)
            or set(envelope) != {"root", "bytes", "block_bytes", "repetitions"}
            or envelope["root"] != root
            or any(type(envelope[k]) is not int or envelope[k] <= 0
                   for k in ("bytes", "block_bytes", "repetitions"))
            or envelope["block_bytes"] > envelope["bytes"]
            or not isinstance(profile.get("pattern"), str) or not profile["pattern"]):
        raise LocalScratchError("scratch profile workload envelope/pattern missing or malformed")
    if profile["pattern"] != PROFILE_PATTERN:
        raise LocalScratchError("incomparable_workload")
    body = {k: v for k, v in profile.items() if k != "artifact_sha256"}
    if (profile.get("schema") != PROFILE_SCHEMA
            or profile.get("artifact_sha256") != core.canonical_sha256(body)
            or profile.get("host") != offer.get("host")
            or profile.get("completed") is not True or profile.get("errors") != []
            or declaration["profile_contract"] != PROFILE_CONTRACT
            or profile.get("profile_contract") != PROFILE_CONTRACT
            or profile.get("method") != PROFILE_METHOD
            or not 0 <= now - latest <= declaration["max_profile_age_s"]):
        raise LocalScratchError("scratch profile invalid, incompatible or stale")
    inode = profile.get("root_inode")
    if not isinstance(inode, str) or not inode.isascii() or not inode.isdigit() or int(inode) <= 0:
        raise LocalScratchError("scratch root inode missing or malformed")
    devices = detail.get(DEVICES)
    if not isinstance(devices, Mapping):
        raise LocalScratchError("scratch devices observation malformed")
    current = devices.get(root)
    if not isinstance(current, dict) or any(current.get(k) != profile.get(k)
                                           or current.get(k) is None
                                           for k in ("device", "filesystem", "root_inode")):
        raise LocalScratchError("scratch current device/filesystem differs from profile")
    result = {"host": offer["host"], "artifact_sha256": profile["artifact_sha256"],
              "measured_unix": latest, "method": profile["method"],
              "device": profile["device"], "filesystem": profile["filesystem"], "root_inode": inode,
              "envelope": dict(envelope), "pattern": profile["pattern"],
              "io_seconds": 0., "write_bytes_per_s": None, "read_bytes_per_s": None}
    for direction in ("write", "read"):
        traffic = declaration[f"{direction}_bytes"]
        if traffic:
            count, elapsed = profile.get(f"{direction}_bytes"), profile.get(f"{direction}_elapsed_s")
            if type(count) is not int or count <= 0 or not _is_positive_finite(elapsed):
                raise LocalScratchError(f"scratch required {direction} observation invalid")
            try:
                rate = count / elapsed
                seconds = traffic / rate
            except ArithmeticError as exc:
                raise LocalScratchError("scratch service cost cannot be represented") from exc
            if not _is_positive_finite(rate) or not _is_positive_finite(seconds):
                raise LocalScratchError("scratch measured service cost not finite positive")
            result[f"{direction}_bytes_per_s"] = rate
            result["io_seconds"] += seconds
    if not _is_positive_finite(result["io_seconds"]):
        raise LocalScratchError("scratch combined service cost invalid")
    return result


class ProfileInputs:
    """Configured executed CAS profiles, verified outside host admission.

    Only immutable source verification is cached. Requests/receipts/results
    are revalidated and root identity observed on refresh; a broken newer ref
    invalidates its root instead of hiding behind an older successful input.
    """
    def __init__(self, config_path, *, source_root, checkout_root, producer_python):
        from . import core
        self.config_path = Path(config_path)
        self.source_root = Path(source_root)
        self.checkout_root = Path(checkout_root)
        # Independent operator-configured PB runtime, never artifact-supplied
        # approval or comparison with this consumer's executable bytes.
        self.producer_python = producer_python
        self.verified = set()
        try:
            self.expected_sources = self._source_digests()
        except (OSError, ValueError, core.PrismaBuildError):
            self.expected_sources = None

    def _source_digests(self):
        from . import core
        return tuple(core.raw_sha256(core._read_regular_file_nofollow(
            self.source_root / name, where="installed scratch producer source"))
                     for name in PRODUCER_FILES)

    def _load(self, ref):
        from . import core, materialize, movement_actions
        keys = {"cas_root", "action_key", "artifact_sha256", "root", "profile_contract"}
        if (not isinstance(ref, dict) or set(ref) != keys
                or _canonical_root(ref["root"]) is None or _canonical_root(ref["cas_root"]) is None):
            raise LocalScratchError("scratch profile reference malformed")
        core._sha256(ref["action_key"], where="scratch profile producer key")
        core._sha256(ref["artifact_sha256"], where="scratch profile body digest")
        cas = core.PrismaBuildCAS(ref["cas_root"])
        requested = cas.read_action_request(ref["action_key"])
        if requested is None:
            raise LocalScratchError("scratch producer request absent")
        action = cast(dict[str, Any], requested)
        bounded = cas._lookup_receipt_only(action)
        if bounded is not None and cast(dict[str, Any], bounded)["result"]["bytes"] > 65536:
            raise LocalScratchError("scratch profile result exceeds bounded metadata size")
        found = cas.lookup(action)
        # Core publishes a canonical result only after successful execution;
        # there is no invented status field in the v3 CAS receipt.
        if found is None:
            raise LocalScratchError("scratch producer successful CAS receipt absent")
        receipt = cast(dict[str, Any], found)
        raw = core._read_regular_file_nofollow(cas.result_path(receipt, action),
                                              where="scratch profile result", max_bytes=65536,
                                              require_readonly=True)
        decoded = core._decode_strict_json(raw, where="scratch profile")
        if not isinstance(decoded, dict):
            raise LocalScratchError("scratch profile body must be an object")
        body = cast(dict[str, Any], decoded)
        if raw != core._canonical_file_bytes(body) or core.canonical_sha256(body) != ref["artifact_sha256"]:
            raise LocalScratchError("scratch result/body digest or canonical bytes differ")
        params, task = action["params"], action["task"]
        required = {"command", "cwd", "demand", "placement", "checkout_snapshot",
                    "retry_policy", "local_scratch_profile"}
        if (not isinstance(params, Mapping) or not required.issubset(params)
                or not isinstance(params["command"], list)
                or any(not isinstance(token, str) for token in params["command"])
                or not isinstance(params["cwd"], str)
                or any(not isinstance(params[k], Mapping) for k in (
                    "demand", "placement", "checkout_snapshot", "retry_policy", "local_scratch_profile"))):
            raise LocalScratchError("scratch producer params containers invalid")
        variables = action["environment"]["variables"]
        envelope = check_profile_request(params["command"], variables, params["demand"],
                                         cwd=params["cwd"], determinism=task["determinism"], derived=True)
        # Recognition precedes all command[0]/envelope indexing. Core params
        # are arbitrary JSON independent of the argv that actually executed.
        if envelope is None:
            raise LocalScratchError("scratch producer command is not a recorder envelope")
        for recorded in (params["local_scratch_profile"], body.get("envelope")):
            if (not isinstance(recorded, Mapping) or set(recorded) != set(envelope)
                    or not isinstance(recorded["root"], str)
                    or any(type(recorded[k]) is not int or recorded[k] <= 0
                           for k in ("bytes", "block_bytes", "repetitions"))
                    or recorded != envelope):
                raise LocalScratchError("scratch producer envelope invalid or contradictory")
        declared = action["environment"]["toolchain"]
        verified = receipt["producer"]["toolchain"]["verified"]
        executable = receipt["producer"]["executable"]
        version = verified.get("python")
        if (_canonical_root(self.producer_python) is None
                or params["command"][0] != self.producer_python
                or executable["path"] != self.producer_python
                or any(k not in verified or declared.get(k) != verified[k]
                       for k in ("python", "argv0.sha256", "argv0.bytes"))
                or verified["argv0.sha256"] != executable["sha256"]
                or verified["argv0.bytes"] != str(executable["bytes"])
                or not isinstance(version, str) or not re.fullmatch(r"3\.[0-9]+\.[0-9]+", version)
                or int(version.split(".")[1]) < 10
                or receipt["producer"]["evidence"]["system"] != "linux"):
            raise LocalScratchError("scratch producer interpreter is not the configured verified PB runtime")
        if (envelope is None or task["definition_id"] != "fleet/pbrun"
                or task["working_directory"] != "." or task["argv"] != params["command"]
                or task["result_path"] != PROFILE_RESULT or params.get(core.PROFILE_PARAM) is not None
                or params.get("local_scratch_profile") != envelope
                or body.get("envelope") != envelope or body.get("root") != ref["root"]
                or envelope["root"] != ref["root"]
                or body.get("profile_contract") != ref["profile_contract"]
                or ref["profile_contract"] != PROFILE_CONTRACT or body.get("method") != PROFILE_METHOD
                or body.get("pattern") != PROFILE_PATTERN
                or body.get("producer_action_key") != action["action_key"]
                or body.get("host") != receipt["producer"]["evidence"]["hostname"]
                or body.get("completed") is not True or body.get("errors") != []):
            raise LocalScratchError("scratch producer command/result contract differs")
        identity = {k: body.get(k) for k in ("device", "filesystem", "filesystem_type", "root_inode")}
        inode = identity["root_inode"]
        if (not isinstance(inode, str) or not inode.isascii() or not inode.isdigit() or int(inode) <= 0
                or identity["filesystem_type"] not in LOCAL_FILESYSTEM_TYPES
                or body.get("start_identity") != identity or body.get("end_identity") != identity
                or body.get("measured_unix") != body.get("started_unix")
                or not _is_positive_finite(body.get("started_unix"))
                or not _is_positive_finite(body.get("ended_unix"))
                or body["ended_unix"] < body["started_unix"]
                or any(body.get(f"{d}_bytes") != envelope["bytes"] * envelope["repetitions"]
                       or not _is_positive_finite(body.get(f"{d}_elapsed_s")) for d in ("write", "read"))):
            raise LocalScratchError("scratch producer measurement incomplete")
        expected = self.expected_sources
        if expected is None or self._source_digests() != expected:
            raise LocalScratchError("installed scratch producer source changed or missing")
        key = (str(cas.root), action["action_key"], receipt["receipt_sha256"],
               receipt["result"]["sha256"], expected, self.producer_python)
        if key not in self.verified:
            with materialize._execution_checkout(
                    {"action_key": action["action_key"], "cas_root": str(cas.root),
                     "checkout_snapshot": params["checkout_snapshot"]},
                    local_checkout_root=self.checkout_root) as checkout:
                core._verify_pbrun_checkout_identity(action, checkout)
                core.verify_code_closure(action["code_closure"], checkout)
                stamps = [f["path"] for f in action["code_closure"]["files"]
                          if Path(f["path"]).name.startswith(core.PBRUN_STAMP_PREFIX)]
                stamp = core._decode_strict_json(core._read_regular_file_nofollow(
                    checkout / stamps[0], where="scratch producer original identity", max_bytes=4096),
                    where="scratch producer original identity")
                if not isinstance(stamp, dict):
                    raise LocalScratchError("scratch producer identity stamp malformed")
                stamp = cast(dict[str, Any], stamp)
                marker_root = Path(variables[PROFILE_MARKER_ENV]).parent
                plain = {k: v for k, v in variables.items()
                         if k not in {PROFILE_OWNER_ENV, PROFILE_MARKER_ENV}}
                expected_owner = movement_actions.container_owner(
                    params["command"], params["cwd"], params["demand"], plain,
                    determinism=task["determinism"], retry_policy=params["retry_policy"],
                    marker_root=marker_root, identity={"head": stamp["head"],
                    "dirty_sha256": stamp["dirty_sha256"]}, logical_cwd=params["cwd"],
                    placement=params["placement"], container_images=())
                if variables[PROFILE_OWNER_ENV] != expected_owner:
                    raise LocalScratchError("scratch producer owner/marker not derived from sealed intent")
                actual = tuple(core.raw_sha256(core._read_regular_file_nofollow(
                    checkout / name, where="scratch producer source"))
                               for name in PRODUCER_FILES)
                if actual != expected:
                    raise LocalScratchError("executed scratch producer source differs from installed producer")
            self.verified.add(key)
        return {**body, "artifact_sha256": ref["artifact_sha256"]}

    def observe(self):
        # Bounded configuration capture; no profile work runs in a worker poll.
        from . import core
        try:
            with self.config_path.open("rb") as stream:
                raw = stream.read(65537)
            if len(raw) > 65536:
                raise LocalScratchError("scratch profile config exceeds bounded metadata size")
            config = core._decode_strict_json(raw, where="scratch profile config")
            if (not isinstance(config, dict) or set(config) != {"schema", "profiles"}
                    or config["schema"] != CONFIG_SCHEMA or not isinstance(config["profiles"], list)):
                raise LocalScratchError("scratch profile config malformed")
        except (OSError, ValueError, RecursionError):
            return {PROFILES: [], DEVICES: {}}
        profiles, devices, failed = [], {}, set()
        for ref in config["profiles"]:
            root = ref.get("root") if isinstance(ref, dict) else None
            if _canonical_root(root) is None:
                return {PROFILES: [], DEVICES: {}}
            try:
                profile = self._load(ref)
                current = root_identity(root)
                if profile["host"] != socket.gethostname() or any(current[k] != profile[k]
                        for k in ("device", "filesystem", "filesystem_type", "root_inode")):
                    raise LocalScratchError("scratch profile not this current host/filesystem")
                profiles.append(profile)
                devices[root] = current
            except (OSError, ValueError, RecursionError, KeyError, TypeError,
                    core.PrismaBuildError):
                failed.add(root)
        return {PROFILES: [p for p in profiles if p["root"] not in failed],
                DEVICES: {r: v for r, v in devices.items() if r not in failed}}
