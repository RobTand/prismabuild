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
import time
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import cast

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
