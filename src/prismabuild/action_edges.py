"""Action edges: a consumer submitted before the producer it reads (#913).

A consumer's key covers its data manifest (``core.validate_data_manifest``),
and a manifest entry names a path and a positive byte count.  A handoff's
paths and sizes exist only once its producer has committed them (#912), so a
consumer that reads a handoff cannot be keyed before its producer runs.
Instead ``pbrun --after PRODUCER:TEMPLATE_ID`` freezes everything else about
the submission and files it here as a *deferred record*.  The tier loop's
release tick (``tools/fleet/deferred_release.py``) seals and publishes the
consumer once the producer's attempt has succeeded, from the origin-only
batches that attempt committed.  The consumer is then an ordinary action whose
key covers the bytes it reads.

This module holds the records and the rules; it seals nothing.  Three
namespaces live under the queue root, each written first-writer-wins through
``pool._publish_immutable``:

- ``deferred/<pending_id>.json``: the frozen submission.  The pending id is
  the SHA-256 of the record's canonical JSON, so an identical submission
  finds its own record.
- ``deferred-releases/<pending_id>.json``: the resolution the release pinned
  (the producers' attempts, the batch refs, the manifest input and the
  consumer's key), filed before anything is published.  A crash resumes from
  it and never resolves again.  ``<pending_id>.published.json`` beside it
  records the generation the consumer's row was published as.
- ``supersessions/<old>.json``: a resubmission's statement that it replaces
  ``old``, which edges naming ``old`` then follow.

Nothing here runs unless a submission names ``--after`` or ``--supersedes``:
the directories are created on first use, and no other path reads them.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import json
import os
from pathlib import Path

from . import core as pb
from . import produced_output as po

DEFERRED_SUBDIR = "deferred"
RELEASES_SUBDIR = "deferred-releases"
SUPERSESSIONS_SUBDIR = "supersessions"

DEFERRED_SCHEMA_V1 = "prismabuild.deferred_submission.v1"
RELEASE_SCHEMA_V1 = "prismabuild.deferred_release.v1"
PUBLISHED_SCHEMA_V1 = "prismabuild.deferred_release_published.v1"
SUPERSESSION_SCHEMA_V1 = "prismabuild.supersession.v1"

#: The whole argument a deferred consumer's command may carry once; the
#: release replaces it with the CAS path of the resolved data manifest.
#: The tier log's line for one released consumer.
RELEASED_EVENT = "deferred-released"

DATA_MANIFEST_PLACEHOLDER = "{pb.data_manifest}"
#: The digest the sealed request binds for that manifest (#933): the
#: SHA-256 of its bytes, so a consumer can verify the file it reads.
DATA_MANIFEST_SHA256_PLACEHOLDER = "{pb.data_manifest_sha256}"

PRODUCER_KEY = "key"
PRODUCER_PENDING = "pending"

#: How many supersession and release links one resolution follows before it
#: calls the chain unknown.  A link is filed by a submission, one per
#: resubmission of one piece of work, so a chain this long is a loop.
MAX_LINKS = 64

_PUBLICATION_KEYS = frozenset({
    "priority", "max_attempts", "retry_safe", "residency", "residency_tier",
    "residency_ram", "residency_mover_mem_gb", "residency_mover_readers",
    "residency_mover_max_attempts",
})


class ActionEdgeError(ValueError):
    """A deferred record, edge or supersession that PB refuses."""


class ActionEdgeUnreadable(ActionEdgeError):
    """A record that exists and could not be read: an I/O failure, not a verdict."""


class RuntimeGenerationUnavailable(ActionEdgeError):
    """The generation a deferred template froze is gone or fails its receipt."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _hex64(value: object, *, where: str) -> str:
    try:
        return po._hex64(value, where=where)
    except po.ProducedOutputError as exc:
        raise ActionEdgeError(str(exc)) from None


def _name(value: object, *, where: str) -> str:
    try:
        return po._name(value, where=where)
    except po.ProducedOutputError as exc:
        raise ActionEdgeError(str(exc)) from None


def parse_edge(text: str) -> dict[str, str]:
    """``PRODUCER:TEMPLATE_ID`` as ``{"producer", "template_id"}``."""

    producer, sep, template_id = str(text).partition(":")
    if not sep:
        raise ActionEdgeError(
            f"--after {text!r}: expected PRODUCER:TEMPLATE_ID, the producer's "
            "action key or pending id and the write-only template it declares")
    return {"producer": _hex64(producer, where="--after producer"),
            "template_id": _name(template_id, where="--after template id")}


def _path(queue_root: str | Path, subdir: str, name: str) -> Path:
    return Path(queue_root) / subdir / f"{name}.json"


def deferred_path(queue_root: str | Path, pending_id: str) -> Path:
    return _path(queue_root, DEFERRED_SUBDIR, pending_id)


def release_path(queue_root: str | Path, pending_id: str) -> Path:
    return _path(queue_root, RELEASES_SUBDIR, pending_id)


def published_path(queue_root: str | Path, pending_id: str) -> Path:
    return _path(queue_root, RELEASES_SUBDIR, f"{pending_id}.published")


def supersession_path(queue_root: str | Path, old: str) -> Path:
    return _path(queue_root, SUPERSESSIONS_SUBDIR, old)


def _read(path: Path, *, where: str) -> dict[str, object] | None:
    """The JSON object at ``path``; ``None`` when absent; raise if unreadable."""

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ActionEdgeUnreadable(f"{where} unreadable: {exc}") from None
    try:
        value = json.loads(raw)
    except ValueError as exc:
        raise ActionEdgeError(f"{where} is not JSON: {exc}") from None
    if not isinstance(value, dict):
        raise ActionEdgeError(f"{where} is not an object")
    return value


def _file(path: Path, value: Mapping[str, object], *, where: str) -> None:
    from . import pool as pool_mod

    raw = _canonical(value) + b"\n"
    try:
        pool_mod._publish_immutable(path, raw, where=where)
    except pool_mod.PoolContractError as exc:
        raise ActionEdgeError(str(exc)) from None


# --------------------------------------------------------------------------
# Deferred records
# --------------------------------------------------------------------------

def _checked_edges(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise ActionEdgeError("a deferred record needs at least one edge")
    edges: list[dict[str, str]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping) or set(raw) != {
                "producer", "kind", "template_id"}:
            raise ActionEdgeError(
                f"edge {index} must carry exactly producer, kind and template_id")
        kind = raw.get("kind")
        if kind not in (PRODUCER_KEY, PRODUCER_PENDING):
            raise ActionEdgeError(f"edge {index} kind must be key or pending")
        edges.append({
            "producer": _hex64(raw.get("producer"), where=f"edge {index} producer"),
            "kind": str(kind),
            "template_id": _name(raw.get("template_id"),
                                 where=f"edge {index} template_id")})
    if len({(e["producer"], e["template_id"]) for e in edges}) != len(edges):
        raise ActionEdgeError("a deferred record names one edge twice")
    return edges


def deferred_body(*, edges: Sequence[Mapping[str, object]],
                  template: Mapping[str, object], cas_root: str | Path,
                  static_manifest: Mapping[str, object] | None,
                  publication: Mapping[str, object]) -> dict[str, object]:
    """The canonical body of one deferred submission, without its id."""

    if set(publication) != _PUBLICATION_KEYS:
        raise ActionEdgeError(
            "deferred publication options must be exactly "
            f"{sorted(_PUBLICATION_KEYS)}")
    body: dict[str, object] = {
        "schema": DEFERRED_SCHEMA_V1,
        "edges": _checked_edges([dict(edge) for edge in edges]),
        "template": json.loads(_canonical(template)),
        "cas_root": str(cas_root),
        "static_manifest": (None if static_manifest is None
                            else json.loads(_canonical(static_manifest))),
        "publication": json.loads(_canonical(publication)),
    }
    return body


def pending_id_of(body: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical(
        {k: v for k, v in body.items() if k != "pending_id"})).hexdigest()


def validate_deferred(value: object, *, pending_id: str | None = None
                      ) -> dict[str, object]:
    """Check one filed deferred record, and that its id is its digest."""

    if not isinstance(value, Mapping):
        raise ActionEdgeError("a deferred record must be an object")
    keys = {"schema", "pending_id", "edges", "template", "cas_root",
            "static_manifest", "publication"}
    if set(value) != keys:
        raise ActionEdgeError(
            f"a deferred record must carry exactly {sorted(keys)}")
    if value.get("schema") != DEFERRED_SCHEMA_V1:
        raise ActionEdgeError(f"deferred record schema must be {DEFERRED_SCHEMA_V1!r}")
    _checked_edges(value.get("edges"))
    template = value.get("template")
    if not isinstance(template, Mapping) or not isinstance(
            template.get("params"), Mapping):
        raise ActionEdgeError("a deferred record's template must carry params")
    if "data_manifest" in template["params"]:
        raise ActionEdgeError(
            "a deferred template carries no data manifest; the release builds it")
    publication = value.get("publication")
    if not isinstance(publication, Mapping) or set(publication) != _PUBLICATION_KEYS:
        raise ActionEdgeError("a deferred record's publication options are malformed")
    static = value.get("static_manifest")
    if static is not None and not isinstance(static, Mapping):
        raise ActionEdgeError("a deferred record's static manifest must be an input")
    recorded = _hex64(value.get("pending_id"), where="pending_id")
    derived = pending_id_of(value)
    if recorded != derived:
        raise ActionEdgeError("a deferred record's pending id is not its digest")
    if pending_id is not None and recorded != pending_id:
        raise ActionEdgeError("a deferred record is filed under another id")
    return dict(value)


def file_deferred(queue_root: str | Path, body: Mapping[str, object]
                  ) -> tuple[str, Path]:
    """File one deferred submission; an identical one finds its own record."""

    pending_id = pending_id_of(body)
    record = {**body, "pending_id": pending_id}
    validate_deferred(record)
    path = deferred_path(queue_root, pending_id)
    _file(path, record, where="deferred submission")
    return pending_id, path


def read_deferred(queue_root: str | Path, pending_id: str
                  ) -> dict[str, object] | None:
    value = _read(deferred_path(queue_root, pending_id),
                  where=f"deferred record {pending_id[:12]}")
    return None if value is None else validate_deferred(value, pending_id=pending_id)


def unreleased_ids(queue_root: str | Path) -> list[str]:
    """Every filed pending id with no publication record, in id order.

    Two directory listings and a set difference: released submissions stay
    filed, and a per-cycle reader must not stat each of them.  A listing
    that fails raises ``OSError``: the caller cannot tell what is pending.
    """

    root = Path(queue_root)

    def names(subdir: str, suffix: str) -> set[str]:
        try:
            listed = os.listdir(root / subdir)
        except FileNotFoundError:
            return set()
        return {name[:-len(suffix)] for name in listed
                if name.endswith(suffix) and not name.startswith(".")
                and len(name) == 64 + len(suffix)}

    filed = names(DEFERRED_SUBDIR, ".json")
    if not filed:
        return []
    return sorted(filed - names(RELEASES_SUBDIR, ".published.json"))


def held_producer_batches(queue) -> set[tuple[str, str]]:
    """The producers whose batches unreleased consumers will read (#913, #914).

    A consumed batch has no declared consumer until its deferred consumer is
    released, and #914 would retire it as an orphan, or after the consumers
    already declared, before that.  This names what must stay, as ``(producer
    key, template id)``: every attempt's batches under that template, because
    which attempt a release reads is only settled when it is pinned.

    * A pinned release that is not yet published holds the producers its
      pinned refs name: the resume reads exactly those.
    * Otherwise an edge holds the key its chain reaches, unless that key has
      failed or been withdrawn, or the chain ends at a producer that is itself
      still deferred and has committed nothing.
    * A superseded submission is never released and holds nothing, and
      neither does a record that fails validation: it can never be released.

    Raises ``OSError`` when a record exists and cannot be read, so that the
    caller keeps every batch rather than guessing.
    """

    holds: set[tuple[str, str]] = set()
    try:
        for pending_id in unreleased_ids(queue.root):
            try:
                record = read_deferred(queue.root, pending_id)
                if record is None:
                    continue
                pinned = read_release(queue.root, pending_id)
                if pinned is not None:
                    for ref in pinned["refs"]:
                        holds.add((str(ref["owner_action_key"]),
                                   str(ref["template_id"])))
                    continue
                if read_supersession(queue.root, pending_id) is not None:
                    continue
                for edge in record["edges"]:
                    answer = resolve_producer(
                        queue, str(edge["producer"]), str(edge["kind"]),
                        str(edge["template_id"]))
                    if answer["key"] is not None and answer["state"] not in (
                            "failed", "withdrawn", "absent"):
                        holds.add((str(answer["key"]), str(edge["template_id"])))
            except ActionEdgeUnreadable:
                raise
            except ActionEdgeError:
                continue
    except ActionEdgeUnreadable as exc:
        raise OSError(str(exc)) from None
    return holds


DEFERRED_LISTING_SCHEMA_V1 = "prismabuild.deferred_listing.v1"


def deferred_listing(queue_root: str | Path, *,
                     published_root: str | Path | None) -> dict[str, object]:
    """Every unreleased deferred consumer and the generation it is pinned to.

    A consumer is released into the generation its template froze, so one
    submitted before a publish runs the older generation.  This lists each
    one -- ``unreleased``, or ``pinned`` when its release record is filed
    and its row is not yet recorded -- with that generation, and marks the
    ones that differ from ``published_root`` (``off_published``): an
    operator whose publish fixed something they need supersedes those with
    ``pbrun --after ... --supersedes <pending_id>``.  Records that cannot be
    read are listed under ``unreadable`` and make the listing incomplete.
    """

    published = None if published_root is None else generation_name(published_root)
    consumers: list[dict[str, object]] = []
    unreadable: list[dict[str, str]] = []
    for pending_id in unreleased_ids(queue_root):
        try:
            record = read_deferred(queue_root, pending_id)
            if record is None:
                continue
            pinned = read_release(queue_root, pending_id)
            successor = read_supersession(queue_root, pending_id)
            if pinned is not None:
                runtime = dict(pinned["runtime"])
                state = "pinned"
            else:
                root = Path(template_wrapper(record["template"])).parent
                runtime = {"root": str(root), "generation": generation_name(root)}
                state = "superseded" if successor is not None else "unreleased"
        except ActionEdgeError as exc:
            unreadable.append({"pending_id": pending_id, "error": str(exc)})
            continue
        consumers.append({
            "pending_id": pending_id, "state": state,
            "edges": [dict(edge) for edge in record["edges"]],
            "runtime": runtime,
            "off_published": (published is not None
                              and runtime["generation"] != published),
            "superseded_by": None if successor is None else successor["new"],
        })
    return {"schema": DEFERRED_LISTING_SCHEMA_V1,
            "published_generation": published, "consumers": consumers,
            "unreadable": unreadable, "complete": not unreadable}


def deferred_ids(queue_root: str | Path) -> list[str]:
    """Every filed pending id, sorted; ``[]`` before the first deferral."""

    try:
        names = os.listdir(Path(queue_root) / DEFERRED_SUBDIR)
    except FileNotFoundError:
        return []
    return sorted(name[:-len(".json")] for name in names
                  if name.endswith(".json") and not name.startswith(".")
                  and len(name) == 64 + len(".json"))


def template_wrapper(template: Mapping[str, object]) -> str:
    """The Docker-wrapper directory a frozen template's ``PATH`` leads with.

    ``freeze_action_template`` puts the submitter generation's wrapper
    first, and everything fingerprinted at freeze -- the container owner, the
    stamp name, the checkout snapshot -- is computed over it.  So it names
    the generation the template belongs to, and the only one it can be
    sealed into.
    """

    try:
        path = str(template["environment"]["variables"]["PATH"])  # type: ignore[index]
    except (KeyError, TypeError):
        raise ActionEdgeError("a deferred template carries no PATH") from None
    prefix = path.split(":", 1)[0]
    if Path(prefix).name != "tools":
        raise ActionEdgeError(
            f"a deferred template's PATH does not lead with a fleet wrapper: {prefix}")
    return prefix


def generation_name(root: str | Path) -> str:
    """How a runtime root is named in records: its store name, else its path."""

    root = Path(root)
    return root.name if root.parent.name == "runtime-generations" else str(root)


# --------------------------------------------------------------------------
# Releases
# --------------------------------------------------------------------------

def file_release(queue_root: str | Path, pending_id: str, *,
                 action_key: str, producers: Sequence[Mapping[str, object]],
                 refs: Sequence[Mapping[str, object]],
                 manifest_input: Mapping[str, object],
                 runtime: Mapping[str, str]) -> dict[str, object]:
    """Pin one release before anything is published; first writer wins.

    ``runtime`` names the generation the consumer was sealed into -- its
    template's, which may be an older retained generation than the one
    published when it is released -- as ``{"root", "generation"}``.
    """

    if set(runtime) != {"root", "generation"}:
        raise ActionEdgeError("a release names its runtime by root and generation")
    record = {
        "schema": RELEASE_SCHEMA_V1,
        "pending_id": _hex64(pending_id, where="pending_id"),
        "action_key": _hex64(action_key, where="released action key"),
        "producers": [dict(item) for item in producers],
        "refs": [dict(ref) for ref in refs],
        "manifest_input": dict(manifest_input),
        "runtime": {"root": str(runtime["root"]),
                    "generation": str(runtime["generation"])},
    }
    _file(release_path(queue_root, pending_id), record, where="deferred release")
    return record


def read_release(queue_root: str | Path, pending_id: str
                 ) -> dict[str, object] | None:
    value = _read(release_path(queue_root, pending_id),
                  where=f"release record {pending_id[:12]}")
    if value is None:
        return None
    runtime = value.get("runtime")
    if (value.get("schema") != RELEASE_SCHEMA_V1
            or value.get("pending_id") != pending_id
            or not isinstance(runtime, Mapping)
            or set(runtime) != {"root", "generation"}):
        raise ActionEdgeError(f"release record {pending_id[:12]} is malformed")
    _hex64(value.get("action_key"), where="released action key")
    return value


def file_published(queue_root: str | Path, pending_id: str, *,
                   action_key: str, published_unix: float) -> dict[str, object]:
    record = {"schema": PUBLISHED_SCHEMA_V1,
              "pending_id": _hex64(pending_id, where="pending_id"),
              "action_key": _hex64(action_key, where="released action key"),
              "published_unix": float(published_unix)}
    _file(published_path(queue_root, pending_id), record,
          where="deferred release publication")
    return record


def read_published(queue_root: str | Path, pending_id: str
                   ) -> dict[str, object] | None:
    value = _read(published_path(queue_root, pending_id),
                  where=f"release publication {pending_id[:12]}")
    if value is None:
        return None
    stamp = value.get("published_unix")
    if (value.get("schema") != PUBLISHED_SCHEMA_V1
            or value.get("pending_id") != pending_id
            or isinstance(stamp, bool) or not isinstance(stamp, (int, float))):
        raise ActionEdgeError(f"release publication {pending_id[:12]} is malformed")
    _hex64(value.get("action_key"), where="released action key")
    return value


# --------------------------------------------------------------------------
# Supersession
# --------------------------------------------------------------------------

def read_supersession(queue_root: str | Path, old: str
                      ) -> dict[str, object] | None:
    value = _read(supersession_path(queue_root, old),
                  where=f"supersession of {old[:12]}")
    if value is None:
        return None
    if (value.get("schema") != SUPERSESSION_SCHEMA_V1 or value.get("old") != old
            or value.get("new_kind") not in (PRODUCER_KEY, PRODUCER_PENDING)):
        raise ActionEdgeError(f"supersession of {old[:12]} is malformed")
    _hex64(value.get("new"), where="superseding submission")
    return value


def file_supersession(queue, old: str, *, new: str, new_kind: str
                      ) -> dict[str, object]:
    """File that ``new`` replaces ``old``; refuse unless ``old`` has ended.

    A key may be superseded once its latest generation is ``failed`` or
    ``withdrawn``: a key that succeeded, or still runs, has nothing to be
    replaced.  A pending id may be superseded while it is unreleased.  The
    record is first-writer: a second, different successor refuses, and the
    same successor finds its own record.

    Once filed, a supersession is followed unconditionally.  If ``old`` is
    later run again and succeeds, edges still read ``new``: every consumer of
    "the producer" reads the same bytes, whenever it is released.  A link
    that would close a loop of supersessions refuses.
    """

    old = _hex64(old, where="--supersedes")
    new = _hex64(new, where="superseding submission")
    if new_kind not in (PRODUCER_KEY, PRODUCER_PENDING):
        raise ActionEdgeError("the superseding submission's kind is unknown")
    if old == new:
        raise ActionEdgeError("a submission cannot supersede itself")
    if read_deferred(queue.root, old) is not None:
        if read_release(queue.root, old) is not None:
            raise ActionEdgeError(
                f"{old[:12]} is already released; supersede the key it was "
                "released as once that key has failed")
    else:
        state, _record = po._key_generation(queue, old)
        if state not in ("failed", "withdrawn"):
            raise ActionEdgeError(
                f"{old[:12]} cannot be superseded: its latest generation is "
                f"{state}, and only a failed or withdrawn key has ended")
    # A filed supersession is followed whatever becomes of ``old`` later, so
    # a chain of them must end.  Refuse the link that would close a loop.
    seen, current = {new}, new
    for _ in range(MAX_LINKS):
        successor = read_supersession(queue.root, current)
        if successor is None:
            break
        current = str(successor["new"])
        if current == old or current in seen:
            raise ActionEdgeError(
                f"{new[:12]} already leads back to {old[:12]} through "
                "supersessions; this link would close a loop")
        seen.add(current)
    record = {"schema": SUPERSESSION_SCHEMA_V1, "old": old, "new": new,
              "new_kind": new_kind}
    _file(supersession_path(queue.root, old), record, where="supersession")
    return record


def successor_of(queue_root: str | Path, old: str) -> dict[str, object] | None:
    """Where a chain of supersessions starting at ``old`` ends, or ``None``.

    ``None`` when nothing supersedes ``old``.  Otherwise ``{"id", "kind",
    "path"}``: the last submission the chain reaches, following each
    supersession and each released pending id to the key it was released
    as.  ``kind`` is ``key``, or ``pending`` for a submission not released
    yet.  ``path`` lists every link after ``old``.  Raises
    ``ActionEdgeError`` on an unreadable or malformed record, or a chain that
    does not end.
    """

    path: list[str] = []
    seen = {old}
    current, kind = old, PRODUCER_KEY
    for _ in range(MAX_LINKS):
        successor = read_supersession(queue_root, current)
        if successor is not None:
            current, kind = str(successor["new"]), str(successor["new_kind"])
        elif kind == PRODUCER_PENDING and (
                published := read_published(queue_root, current)) is not None:
            current, kind = str(published["action_key"]), PRODUCER_KEY
        else:
            return {"id": current, "kind": kind, "path": path} if path else None
        if current in seen:
            raise ActionEdgeError(
                f"the supersessions of {old[:12]} lead back to {current[:12]}")
        seen.add(current)
        path.append(current)
    raise ActionEdgeError(f"the supersessions of {old[:12]} do not end")


# --------------------------------------------------------------------------
# Producers
# --------------------------------------------------------------------------

def producer_kind(queue, producer: str) -> str:
    """Whether ``producer`` is a pending id or an action key; refuse unknown.

    A pending id is one with a filed deferred record.  A key is one the queue
    carries: a readable row or terminal record.  ``absent`` and ``unknown``
    both refuse, because an edge to a key nobody filed never releases.
    """

    producer = _hex64(producer, where="--after producer")
    if read_deferred(queue.root, producer) is not None:
        return PRODUCER_PENDING
    state, _record = po._key_generation(queue, producer)
    if state == "absent":
        raise ActionEdgeError(
            f"--after {producer[:12]}: no action or deferred submission with "
            "this key is filed in the queue")
    if state == "unknown":
        raise ActionEdgeError(
            f"--after {producer[:12]}: the queue cannot say what this key is")
    return PRODUCER_KEY


def declared_template_id(cas_root: str | Path, action_key: str) -> str | None:
    """The produced-output template id a sealed action declares, or ``None``."""

    key = _hex64(action_key, where="producer key")
    request = Path(str(cas_root)) / "requests" / key[:2] / f"{key}.json"
    try:
        raw = pb._read_regular_file_nofollow(request, where="producer request")
        action = pb.validate_action(
            pb._decode_strict_json(raw, where="producer request"))
    except FileNotFoundError:
        return None
    except (pb.PrismaBuildError, ValueError, OSError) as exc:
        raise ActionEdgeError(f"producer request unreadable: {exc}") from None
    if action.get("action_key") != key:
        raise ActionEdgeError("producer request does not match its key")
    declaration = action["params"].get(pb.PRODUCED_OUTPUT_TEMPLATE_PARAM)
    if not isinstance(declaration, Mapping):
        return None
    return str(declaration.get("template_id"))


def require_write_only_template(queue_root: str | Path, template_id: str) -> None:
    """Refuse an edge whose template is not filed as write-only."""

    path = (Path(queue_root) / "residency" / po.OUTPUT_TEMPLATES_SUBDIR
            / f"{_name(template_id, where='template id')}.json")
    try:
        template = po.validate_template(json.loads(path.read_text()))
    except FileNotFoundError:
        raise ActionEdgeError(
            f"template {template_id!r} is not filed; its producer declares it "
            "when it is published") from None
    except (OSError, ValueError) as exc:
        raise ActionEdgeError(f"template {template_id!r} unreadable: {exc}") from None
    if not template.get("write_only"):
        raise ActionEdgeError(
            f"template {template_id!r} is not write-only: its batches are "
            "staged by their own producer, never committed at origin")


def committed_attempt(queue_root: str | Path, producer_key: str,
                      template_id: str) -> str | None:
    """The one attempt of a producer that holds committed origin batches.

    For a producer whose ``done`` record does not name an attempt that
    executed.  A ``cache_hit`` generation ran nothing: its attempt found the
    receipt an earlier attempt published, and that earlier attempt may have
    died before it could file ``done`` itself.  Its committed batches are the
    producer's output, and they are found where PB filed them: the unique
    instance of this template under this key with an origin-only batch that
    is not reclaimed.  ``None`` when there is none or more than one.
    """

    scopes = (Path(queue_root) / "residency" / po.OUTPUT_SCOPES_SUBDIR
              / _hex64(producer_key, where="producer key"))
    try:
        names = sorted(os.listdir(scopes))
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ActionEdgeUnreadable(f"producer scopes unreadable: {exc}") from None
    found: list[str] = []
    prefix = f"{_name(template_id, where='template id')}."
    for name in names:
        if not name.startswith(prefix):
            continue
        scope = scopes / name
        try:
            instance = po.validate_instance(json.loads(
                (scope / "instance.json").read_text()))
            if (instance["template_id"] != template_id
                    or po.instance_dir(queue_root, instance) != scope):
                continue
            batches = po._read_commitments(scope / "commitments.json")["batches"]
        except FileNotFoundError:
            continue
        except (OSError, ValueError) as exc:
            raise ActionEdgeUnreadable(
                f"producer instance {name} unreadable: {exc}") from None
        if any(isinstance(entry, Mapping) and entry.get("origin_only") is True
               and not entry.get("origin_reclaimed")
               for entry in batches.values()):
            attempt = instance["owner_attempt"]
            assert isinstance(attempt, dict)
            found.append(str(attempt["nonce"]))
    return found[0] if len(found) == 1 else None


def resolve_producer(queue, producer: str, kind: str,
                     template_id: str) -> dict[str, object]:
    """Where one edge's producer stands, following releases and supersessions.

    Returns ``{"state", "key", "nonce", "path"}``.  ``state`` is ``done``
    (with the key and the nonce of the attempt whose batches a release
    reads), ``live`` (queued, claimed or being moved), ``pending`` (a
    deferred producer not yet released), or a hold to report: ``failed``,
    ``withdrawn``, ``no-committed-attempt``, ``absent``, ``unknown`` or
    ``superseded-loop``.  ``path`` lists every link followed, so a report
    names the chain it read.

    A filed supersession is followed first and unconditionally.  A ``done``
    producer is read by the attempt its record names when that attempt
    executed; any other ``done`` -- a cache hit, or a record with no attempt
    -- by the one attempt that committed batches (`committed_attempt`).
    """

    path: list[str] = []
    seen: set[str] = set()
    current, current_kind = producer, kind
    for _ in range(MAX_LINKS):
        if current in seen:
            break
        seen.add(current)
        path.append(current)
        successor = read_supersession(queue.root, current)
        if successor is not None:
            current = str(successor["new"])
            current_kind = str(successor["new_kind"])
            continue
        if current_kind == PRODUCER_PENDING:
            published = read_published(queue.root, current)
            if published is not None:
                current, current_kind = str(published["action_key"]), PRODUCER_KEY
                continue
            return {"state": "pending", "key": None, "nonce": None, "path": path}
        state, record = po._key_generation(queue, current)
        if state in ("claimed", "ready", "moving"):
            return {"state": "live", "key": current, "nonce": None, "path": path}
        if state == "done":
            assert record is not None
            nonce = po._record_nonce(record)
            if record.get("status") == "executed" and nonce:
                return {"state": "done", "key": current, "nonce": nonce,
                        "path": path}
            nonce = committed_attempt(queue.root, current, template_id)
            if nonce is not None:
                return {"state": "done", "key": current, "nonce": nonce,
                        "path": path}
            return {"state": "no-committed-attempt", "key": current,
                    "nonce": None, "path": path}
        return {"state": state, "key": current, "nonce": None, "path": path}
    return {"state": "superseded-loop", "key": None, "nonce": None, "path": path}


def committed_batch_refs(queue_root: str | Path, *, producer_key: str,
                         nonce: str, template_id: str
                         ) -> list[dict[str, object]]:
    """The origin-only batches one attempt committed under one template.

    Read from what PB filed: the attempt's instance, its commitments and each
    batch through ``load_origin_batch``, which rechecks every origin by the
    identity its commit recorded (``lstat``; no file is read or hashed).
    Batch-id order.  Raises ``ActionEdgeError`` naming why the edge cannot
    resolve: no instance, no batch, or a batch that is retiring, reclaimed or
    changed.
    """

    scope = (Path(queue_root) / "residency" / po.OUTPUT_SCOPES_SUBDIR
             / producer_key / f"{template_id}.{nonce}")
    try:
        instance = po.validate_instance(json.loads(
            (scope / "instance.json").read_text()))
    except FileNotFoundError:
        raise ActionEdgeError(
            f"producer-committed-nothing: attempt {nonce[:8]} of "
            f"{producer_key[:12]} bound no {template_id} instance") from None
    except (OSError, ValueError) as exc:
        raise ActionEdgeError(f"producer instance unreadable: {exc}") from None
    try:
        commitments = po._read_commitments(
            po._commitments_path(queue_root, instance))
    except po.ProducedOutputError as exc:
        raise ActionEdgeError(f"producer commitments unreadable: {exc}") from None
    refs: list[dict[str, object]] = []
    for batch_id, entry in sorted(commitments["batches"].items()):
        if not isinstance(entry, Mapping) or entry.get("origin_only") is not True:
            continue
        ref = po.origin_batch_ref(instance, batch_id=batch_id,
                                  manifest_digest=str(entry.get("manifest_digest")))
        try:
            po.load_origin_batch(queue_root, ref)
        except po.ProducedOutputError as exc:
            raise ActionEdgeError(str(exc)) from None
        refs.append(ref)
    if not refs:
        raise ActionEdgeError(
            f"producer-committed-nothing: attempt {nonce[:8]} of "
            f"{producer_key[:12]} committed no origin-only batch under "
            f"{template_id}")
    return refs


# --------------------------------------------------------------------------
# Manifests
# --------------------------------------------------------------------------

def merged_manifest(static: Mapping[str, object] | None,
                    batches: Mapping[str, object]) -> dict[str, object]:
    """The consumer's manifest: its static part, then the committed batches.

    ``batches`` is ``produced_output.origin_batch_manifest`` over the
    resolved refs.  The static entries keep their order and their phases;
    each batch follows as its own phase, as that function lays it out.  A
    static manifest without phases yields a manifest without phases, because
    a boundary nobody declared is not one the release may invent.  The
    ``produced_output_batches`` annotation lists the static manifest's refs,
    if any, then the resolved ones.
    """

    checked_batches = pb.validate_data_manifest(batches)
    if static is None:
        return checked_batches
    checked_static = pb.validate_data_manifest(static)
    if checked_static["schema"] != pb.DATA_MANIFEST_SCHEMA_V1:
        raise ActionEdgeError(
            "a deferred consumer's static manifest must be data_manifest.v1")
    static_notes = dict(checked_static["annotations"])
    batch_notes = checked_batches["annotations"]
    static_total = int(checked_static["total_bytes"])
    annotations = {key: value for key, value in static_notes.items()
                   if key not in ("phases", po.ORIGIN_BATCHES_ANNOTATION)}
    static_phases = static_notes.get("phases")
    if static_phases is not None:
        shifted = [{**phase,
                    "cumulative_bytes": int(phase["cumulative_bytes"]) + static_total}
                   for phase in batch_notes["phases"]]
        annotations["phases"] = [*static_phases, *shifted]
    annotations[po.ORIGIN_BATCHES_ANNOTATION] = [
        *(static_notes.get(po.ORIGIN_BATCHES_ANNOTATION) or []),
        *batch_notes[po.ORIGIN_BATCHES_ANNOTATION]]
    prefix = os.path.commonpath([str(checked_static["mount_prefix"]),
                                 str(checked_batches["mount_prefix"])])
    if prefix == "/":
        raise ActionEdgeError(
            "the static manifest and the committed batches share no directory "
            "below / to mount")
    entries = [*checked_static["entries"], *checked_batches["entries"]]
    return pb.validate_data_manifest({
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "prismabuild.action_edges.merged_manifest"},
        "mount_prefix": prefix,
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": static_total + int(checked_batches["total_bytes"]),
        "annotations": annotations,
    })


def resolve_command(command: Sequence[str], manifest_path: str | Path,
                    manifest_sha256: str) -> list[str]:
    """Put the resolved manifest's path and digest where the command asks.

    ``{pb.data_manifest}`` becomes the manifest's path and
    ``{pb.data_manifest_sha256}`` the digest the sealed request binds for
    it, which is the SHA-256 of the file at that path.  Substitution of a
    whole argument, as ``decomposition.resolve_task_batch`` does; a command
    may carry each placeholder at most once, or not at all.
    """

    values = {DATA_MANIFEST_PLACEHOLDER: str(manifest_path),
              DATA_MANIFEST_SHA256_PLACEHOLDER: str(manifest_sha256)}
    for placeholder in values:
        count = sum(part == placeholder for part in command)
        if count > 1:
            raise ActionEdgeError(
                f"the command carries {placeholder} {count} times; "
                "at most once, as a whole argument")
    return [values.get(part, part) for part in command]


__all__ = [
    "ActionEdgeError", "DATA_MANIFEST_PLACEHOLDER",
    "DATA_MANIFEST_SHA256_PLACEHOLDER", "DEFERRED_SCHEMA_V1",
    "DEFERRED_SUBDIR", "MAX_LINKS", "PRODUCER_KEY", "PRODUCER_PENDING",
    "PUBLISHED_SCHEMA_V1", "RELEASES_SUBDIR", "RELEASE_SCHEMA_V1",
    "SUPERSESSIONS_SUBDIR", "SUPERSESSION_SCHEMA_V1", "committed_batch_refs",
    "ActionEdgeUnreadable", "DEFERRED_LISTING_SCHEMA_V1", "RELEASED_EVENT",
    "RuntimeGenerationUnavailable", "committed_attempt",
    "declared_template_id", "deferred_body", "deferred_ids",
    "deferred_listing", "generation_name", "template_wrapper",
    "deferred_path", "held_producer_batches",
    "file_deferred", "file_published", "file_release", "file_supersession",
    "merged_manifest", "parse_edge", "pending_id_of", "producer_kind",
    "published_path", "read_deferred", "read_published", "read_release",
    "read_supersession", "release_path", "require_write_only_template",
    "resolve_command", "resolve_producer", "successor_of", "supersession_path",
    "unreleased_ids", "validate_deferred",
]
