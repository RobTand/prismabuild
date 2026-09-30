"""Exact container-image requirements, and the bounded inventory behind them.

A sealed action may declare the container images it needs *present on its
claiming box* before it runs (``pbrun --container-image``).  PrismaBuild does
not pull, load, copy or otherwise transfer an image: the declaration is a
placement requirement over the box's own Docker, and the inventory here is
what lets a claim refuse a box that cannot positively show the reference.
#714 is the failure this exists to end -- an action pinned to an image that
existed on one Spark was claimed by the other, and died inside its wrapper
after the attempt was already spent.

Three reference forms are accepted, and they are deliberately not aliases:

* ``sha256:<64 hex>`` names the local **image ID**: whatever the box's own
  image store calls the image.  It is satisfied only by an image the inventory
  reports by that ID, and it **is not portable between stores**.  Docker's
  classic store reports the config digest; Docker's containerd store reports
  the digest of the image's top-level descriptor -- an OCI index for 16 of
  sparky's 27 images and a manifest for the rest, so it varies with how the
  image arrived.  One image therefore has two IDs on two boxes running the
  same engine (#805, measured 2026-09-21 on sparky and sparklina, both
  Engine 29.6.2).
* ``repository@sha256:<64 hex>`` names a repository **manifest digest**.  It is
  satisfied only by that exact ``repository@sha256:...`` string among the
  box's RepoDigests.  A bare digest from a RepoDigest is never presented, so a
  manifest digest cannot accidentally satisfy an ID requirement (or the
  reverse) merely because the hex matches.  It exists only for an image that
  was pulled: 18 of 32 images on sparklina's classic store carry an empty
  RepoDigests list, the campaign image among them, so this form does not
  rescue a locally built or ``docker load``-ed image.
* ``content:sha256:<64 hex>`` names the image's **store-independent content**
  (#805): :func:`content_ref` over the ordered ``RootFS.Layers`` diff ids and
  the execution-bearing fields of the OCI image config.  Two daemons holding
  the same image publish the same string whatever their store calls its ID,
  and an image with any different layer or config field publishes a different
  one.  This is the form to seal into an action that must be claimable by
  every box that holds the image.

A mutable tag (``repo:tag``) is refused at declaration time: it is not an
identity, so it cannot be sealed into an action key, and it would let the
requirement move under the receipt it was admitted against.

The inventory is one ``docker image ls`` read followed by one
``docker image inspect`` of exactly the IDs that listing reported, both pinned
to the local Unix daemon socket and both spent from a single
:data:`INVENTORY_TIMEOUT_S` budget, each bounded by
:data:`MAX_INVENTORY_BYTES` -- the output is read through a capped,
deadline-bound stream, never accumulated and then measured.  A failure, a
timeout, an unexpected listing shape or an oversized answer is **unknown**,
never empty.  An unknown inventory satisfies no requirement.  In particular a
failed or unparseable inspect makes the whole inventory unknown rather than
publishing the IDs alone: an inventory that silently dropped its content
references would deny content-form work with ``container_image_absent``, which
is the misleading refusal #805 exists to end.

``docker image inspect`` exits nonzero when any named ID is gone, so an image
removed between the two reads makes one refresh unknown.  That is bounded by
the refresh interval and heals on the next one; no claim is admitted from it.

Two staleness bounds, deliberately different:

* The **offer** reads the shared record with :data:`INVENTORY_TTL_S`, so the
  placement surface is one bounded Docker listing per box per TTL rather than
  one per worker loop per poll.
* A **claim** reads it with :data:`CLAIM_FRESHNESS_S`: while image-pinned
  work is waiting, the record is re-probed (once for the box, under the same
  local lock) at least that often before a claim uses it.  A loop that finds
  a sibling mid-probe waits for that sibling's record, within the same
  :data:`INVENTORY_TIMEOUT_S` budget, rather than reading the stale one as
  unknown (#1143).  The probe and the wait run in the worker's poll, outside
  every pool lock.  A claim can still race an
  image removal that happens after the observation and before the container
  starts; that interval is small and documented rather than claimed closed.

The record lives in this uid's private directory (``0700``, checked on open,
no symlinks followed) and is read as a stable, size-capped regular file whose
schema, timestamp, reference syntax and entry count are all validated.  A
malformed or replaced record is unknown, so it can never authorize a claim.

Archive-backed containers stay the consumer's own behavior.  A spec that loads
its image from an archive at run time (a client's ``container.archive``)
establishes presence *inside* the action, so it must not declare
``container_images``: PB would refuse the claim before the loader ever ran.
Declare only images that are already local when the action is claimed.
"""
from __future__ import annotations

from dataclasses import dataclass
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import select
import shutil
import signal
import stat
import subprocess
import time

#: How long one observed inventory answers the *offer*.  It bounds the
#: placement surface's staleness and how often one box pays for a Docker read.
INVENTORY_TTL_S = 30.0

#: How fresh the inventory a *claim* reads must be while image-pinned work is
#: waiting.  The shared record is re-probed under its local lock, so the whole
#: box pays one listing per interval, not one per loop.
CLAIM_FRESHNESS_S = 5.0

#: One Docker metadata read's ceiling.  A probe that overruns is unknown.  A
#: loop waiting on a sibling's refresh spends the same budget (#1143).
INVENTORY_TIMEOUT_S = 5.0

#: How often a loop waiting on a sibling's refresh retries the local lock.
#: A nonblocking ``flock`` of a host-local file, so the poll costs nothing
#: next to the listing it waits for (0.23 s on sparky, 2026-09-25).
REFRESH_LOCK_POLL_S = 0.02

#: The local system daemon, the same endpoint the action Docker shim pins.
#: ``DOCKER_HOST`` and ``DOCKER_CONTEXT`` are scrubbed from the probe's
#: environment so a remote context cannot answer for this box's frames.
LOCAL_ENDPOINT = "unix:///var/run/docker.sock"

#: Refuse an answer larger than this, and a record holding more entries.
MAX_INVENTORY_BYTES = 8 * 1024 * 1024
MAX_INVENTORY_ENTRIES = 4096

#: One read's ceiling.  The probe reads through ``os.read`` in chunks of this
#: size -- never ``BufferedReader.read()`` with no size, which a nonblocking
#: stream may satisfy by draining a continuously-fed pipe into one arbitrarily
#: large object -- so this is the most a single read can allocate.
PROBE_CHUNK_BYTES = 64 * 1024

INVENTORY_SCHEMA = "prismabuild.container_image_inventory.v2"
CLASS_REQUIREMENTS_SCHEMA = "prismabuild.container_class_requirements.v1"
CLASS_VERDICT_SCHEMA = "prismabuild.container_class_verdict.v1"
MAX_REQUIREMENT_CLASSES = 64
MAX_CLASS_IMAGES = 256
_CLASS_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_IMAGE_DOMAIN = r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?(?::[0-9]+)?"
_IMAGE_PATH = r"[a-z0-9]+(?:(?:[._]|__|[-]+)[a-z0-9]+)*"
_NAMED_IMAGE = re.compile(
    rf"(?:(?:{_IMAGE_DOMAIN})/)?{_IMAGE_PATH}(?:/{_IMAGE_PATH})*"
    r":[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}\Z")

#: Bound into the hashed payload, so the covered set is part of the identity.
#: Widening or narrowing that set changes every digest, which is the intended
#: failure: an already-sealed ``content:`` reference stops matching and the
#: item stays ready rather than being satisfied under a different rule.
CONTENT_SCHEMA = "prismabuild.container_image_content.v1"

#: The whole covered set beside ``Config``: the platform an image declares.
#: Docker refuses or warns on a mismatch, so both are execution-bearing, and
#: both stores project them straight from the image's own config blob.
#:
#: Excluded, deliberately.  *Store bookkeeping*, which differs by
#: construction: ``Id``, ``RepoTags``, ``RepoDigests``, ``Metadata``,
#: ``Parent``, ``Size`` (measured to disagree on every shared image --
#: 9736792922 on the containerd store against 20742216431 on the classic
#: one), and the store-exclusive ``GraphDriver``, ``Descriptor``,
#: ``Identity`` and ``DockerVersion``.  *Metadata that does not reach the
#: container*: ``Created``, ``Author``, ``Comment``, ``Variant``.  Those four
#: agreed on all 17 shared images, but they are rendered strings that a
#: client formats (``Created`` appears in more than one format across rows),
#: and a rendering difference between two daemons would refuse a box that
#: holds the image -- the exact defect this form exists to end.  They cannot
#: buy safety in exchange, because the requirement asks whether the box can
#: run what the action runs, and none of the four changes that.
CONTENT_PLATFORM_FIELDS = ("Architecture", "Os")

#: The image-config keys the digest covers, by the type each must hold.
#: This is the OCI image-spec config plus Docker's own extensions, not
#: "whatever the daemon printed": a daemon that pads the object with
#: container-config keys of its own would otherwise make two boxes disagree
#: about one image.  A key outside this set is refused rather than ignored
#: when it carries a value -- see :func:`_canonical_config`.
_CONFIG_STRINGS = ("StopSignal", "User", "WorkingDir")
_CONFIG_STRING_LISTS = ("Cmd", "Entrypoint", "Env", "OnBuild", "Shell")
_CONFIG_KEY_SETS = ("ExposedPorts", "Volumes")
_CONFIG_BOOLS = ("ArgsEscaped",)
_CONFIG_INTS = ("StopTimeout",)
_HEALTHCHECK_LISTS = ("Test",)
_HEALTHCHECK_INTS = ("Interval", "Retries", "StartInterval", "StartPeriod",
                     "Timeout")

_DIGEST = r"sha256:[0-9a-f]{64}"
_IMAGE_ID = re.compile(rf"{_DIGEST}\Z")
_REPO_DIGEST = re.compile(rf"[a-zA-Z0-9][a-zA-Z0-9._:/-]*@{_DIGEST}\Z")
_CONTENT_REF = re.compile(rf"content:{_DIGEST}\Z")

#: ID, repository and digest per image, tab-separated: one listing covers both
#: name-bearing reference forms, and ``--all`` keeps dangling images visible.
_LIST_FORMAT = "{{.ID}}\t{{.Repository}}\t{{.Digest}}"

#: One JSON object per line, so the inspect answer is read and parsed as a
#: bounded stream of rows rather than one array that must be whole first.
_INSPECT_FORMAT = "{{json .}}"


def validate_ref(value: object) -> str:
    """Return ``value`` if it is an immutable image reference, else raise.

    The refusal names the accepted forms and says why a tag cannot be one:
    an action's image requirement is sealed into its key, so it has to be an
    identity the receipt can be held against.
    """

    if not isinstance(value, str) or not value:
        raise ValueError(
            "container image must be a nonempty string: sha256:<64 hex> for a "
            "local image ID, repository@sha256:<64 hex> for a manifest "
            "digest, or content:sha256:<64 hex> for a store-independent "
            "content reference")
    if (_IMAGE_ID.fullmatch(value) or _REPO_DIGEST.fullmatch(value)
            or _CONTENT_REF.fullmatch(value)):
        return value
    raise ValueError(
        f"{value!r} is not an immutable container image reference; use "
        "sha256:<64 hex>, repository@sha256:<64 hex> or "
        "content:sha256:<64 hex>. A mutable tag cannot be part of an "
        "action's identity")


def _is_zero(value) -> bool:
    """Whether Go's ``omitempty`` would have dropped this value.

    Docker serializes the image config with ``omitempty``, so a field at its
    zero value and a field that is absent are the same statement about the
    image: ``"Cmd": null``, ``"Cmd": []`` and no ``Cmd`` key all say "this
    image sets no command".  Treating them as one makes two daemons agree by
    construction rather than by luck, and it cannot admit a different image,
    because an omission and a zero value are identical to a container
    runtime.  The test is by type, never ``==``: ``0 == False`` in Python and
    these are not the same value.
    """

    if value is None or value is False:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value == 0
    if isinstance(value, (str, list, dict)):
        return len(value) == 0
    return False


def _drop_zero(mapping: dict) -> dict:
    return {key: value for key, value in mapping.items()
            if not _is_zero(value)}


def _string_list(value, *, where: str) -> list:
    if not isinstance(value, list) or not all(
            isinstance(entry, str) for entry in value):
        raise ValueError(f"container image {where} is not a list of strings")
    return list(value)                  # order is content: later Env wins


def _canonical_config(config: dict) -> dict:
    """The covered image config, typed and canonicalized.

    Keys outside the covered set are refused when they carry a value and
    ignored when they are at their zero: a daemon that pads the object with
    zero-valued container-config keys is saying nothing, but one that puts a
    *value* somewhere this does not model is saying something this cannot
    price, and pricing it wrong is the one failure that must not happen.
    """

    canonical: dict = {}
    for key, value in config.items():
        if not isinstance(key, str):
            raise ValueError("container image config has a foreign key")
        if _is_zero(value):
            continue
        if key in _CONFIG_STRINGS:
            if not isinstance(value, str):
                raise ValueError(f"container image config {key} is not a string")
            canonical[key] = value
        elif key in _CONFIG_STRING_LISTS:
            canonical[key] = _string_list(value, where=f"config {key}")
        elif key in _CONFIG_KEY_SETS:
            # The set of keys is the content; the values are ``{}`` on one
            # store and ``null`` on another and mean nothing either way.
            if not isinstance(value, dict) or not all(
                    isinstance(entry, str) for entry in value):
                raise ValueError(f"container image config {key} is not a set")
            canonical[key] = sorted(value)
        elif key == "Labels":
            # Kept whole, empty values included: 10 of 27 images on sparky
            # and 12 of 32 on sparklina carry a label with an empty value,
            # and an empty label is still a label the image declares.
            if not isinstance(value, dict) or not all(
                    isinstance(name, str) and isinstance(entry, str)
                    for name, entry in value.items()):
                raise ValueError("container image config Labels is not a map")
            canonical[key] = dict(sorted(value.items()))
        elif key in _CONFIG_BOOLS:
            if not isinstance(value, bool):
                raise ValueError(f"container image config {key} is not a bool")
            canonical[key] = value
        elif key in _CONFIG_INTS:
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(f"container image config {key} is not an int")
            canonical[key] = value
        elif key == "Healthcheck":
            if not isinstance(value, dict):
                raise ValueError("container image Healthcheck is not an object")
            check: dict = {}
            for name, entry in _drop_zero(value).items():
                if name in _HEALTHCHECK_LISTS:
                    check[name] = _string_list(entry, where=f"Healthcheck {name}")
                elif name in _HEALTHCHECK_INTS:
                    if not isinstance(entry, int) or isinstance(entry, bool):
                        raise ValueError(
                            f"container image Healthcheck {name} is not an int")
                    check[name] = entry
                else:
                    raise ValueError(
                        f"container image Healthcheck carries {name!r}, which "
                        "this identity does not cover")
            if check:
                canonical[key] = check
        else:
            raise ValueError(
                f"container image config carries {key!r} with a value, which "
                "this identity does not cover")
    return canonical


def content_ref(payload) -> str:
    """The store-independent ``content:sha256:...`` reference for one image.

    ``payload`` is one ``docker image inspect`` row.  The digest covers the
    ordered ``RootFS.Layers`` diff ids, :data:`CONTENT_PLATFORM_FIELDS` and
    the covered image config, under :data:`CONTENT_SCHEMA`.

    Why it cannot admit a different image.  What a container does is its root
    filesystem plus its process spec.  Each diff id is the sha256 of one
    layer's uncompressed changeset tar -- bytes, modes, owners, xattrs and
    whiteouts -- and the filesystem is those tars applied in order, so an
    equal ordered list is an equal filesystem; the list is hashed as a JSON
    array, so a permutation, a prefix or an extension all differ, which is
    right because the order whiteouts apply in matters.  Every build step
    that adds no layer (``ENV``, ``CMD``, ``USER``, ``EXPOSE``) lands
    entirely in the config, which is covered.  What it deliberately does not
    distinguish is an image from a rebuild of itself that changed only
    ``Created``, ``Author`` or ``Comment``: those images run identically.

    The residuals it does not close, named rather than claimed away: a future
    config key that changes execution (refused, not ignored -- see
    :func:`_canonical_config`); the action's own ``docker run`` flags, which
    are sealed elsewhere in the action; the box's runtime, driver and kernel,
    which are not properties of the image; and the fact that this proves the
    *content* is present, not that the name the action runs resolves to it.

    Anything this cannot read raises, and :func:`observe` turns that into an
    unknown inventory.
    """

    if not isinstance(payload, dict):
        raise ValueError("container image inspect row is not an object")
    rootfs = payload.get("RootFS")
    if not isinstance(rootfs, dict) or rootfs.get("Type") != "layers":
        # Asserted, not hashed: one accepted value carries no information.
        raise ValueError("container image inspect row has no layer rootfs")
    layers = rootfs.get("Layers")
    if not isinstance(layers, list) or not layers:
        raise ValueError("container image inspect row lists no layers")
    for layer in layers:
        if not isinstance(layer, str) or not _IMAGE_ID.fullmatch(layer):
            raise ValueError("container image layer is not a sha256 diff id")
    config = payload.get("Config")
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise ValueError("container image inspect row has no config")
    covered = {"schema": CONTENT_SCHEMA, "layers": list(layers),
               "config": _canonical_config(config)}
    for field in CONTENT_PLATFORM_FIELDS:
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"container image inspect row has no {field}")
        covered[field.lower()] = value
    # ``sort_keys`` is code-point order, not RFC 8785's UTF-16 order, and the
    # parsed object is hashed rather than the daemon's bytes, so one store's
    # ``<`` escaping cannot change the digest.
    blob = json.dumps(covered, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")
    return "content:sha256:" + hashlib.sha256(blob).hexdigest()


def normalize_refs(values) -> tuple[str, ...]:
    """Validate, deduplicate and sort declared references."""

    return tuple(sorted({validate_ref(value) for value in values}))


def required_from_items(items) -> tuple[str, ...]:
    """Every declared reference among a snapshot of queue items.

    Only used to decide whether a fresh presence read is worth taking; the
    claim itself reads each item's field directly.  A malformed field is not
    repaired here -- the claim denies it.
    """

    refs: set[str] = set()
    for item in items:
        declared = item.get("container_images")
        if isinstance(declared, list):
            refs.update(entry for entry in declared if isinstance(entry, str))
    return tuple(sorted(refs))


def parse_inventory(text: str) -> frozenset[str]:
    """Every reference one ``docker image ls`` listing positively shows.

    The result holds bare image IDs and exact ``repository@sha256:...``
    RepoDigests.  Any line this does not understand raises: a listing shape
    that changed under us is unknown evidence, and an unknown inventory must
    not be usable as a confident partial answer.
    """

    entries: set[str] = set()
    for line in text.splitlines():
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 3:
            raise ValueError("unexpected docker image listing row shape")
        image_id, repository, digest = fields
        if not _IMAGE_ID.fullmatch(image_id):
            raise ValueError("docker image listing row does not start with an ID")
        entries.add(image_id)
        if repository != "<none>" and digest != "<none>":
            if not _IMAGE_ID.fullmatch(digest):
                raise ValueError("docker image listing row carries a foreign digest")
            entries.add(f"{repository}@{digest}")
    return frozenset(entries)


def parse_inspect(text: str) -> frozenset[str]:
    """Every ``content:sha256:...`` reference one inspect answer shows.

    One JSON object per line.  A line this cannot read raises, for the same
    reason :func:`parse_inventory` does: an answer whose shape changed under
    us is unknown evidence, and unknown must never become a confident partial
    inventory.  A box that published its IDs but silently dropped its content
    references would deny content-form work as *absent*, which is the
    misleading refusal this form exists to end.
    """

    entries: set[str] = set()
    for line in text.splitlines():
        if not line:
            continue
        try:
            row = json.loads(line)
        except ValueError as error:
            raise ValueError("docker image inspect row is not JSON") from error
        entries.add(content_ref(row))
    return frozenset(entries)


@dataclass(frozen=True)
class ClassImageRequirement:
    """One class's named content requirements and lexical DockerRootDir."""

    store_root: str
    images: tuple[tuple[str, str], ...]


def _canonical_store_root(value: object) -> str:
    if (not isinstance(value, str) or len(value) > 1024
            or not value.isprintable() or not value.startswith("/")
            or value.startswith("//")):
        raise ValueError("image store root must be a canonical absolute path")
    path = PurePosixPath(value)
    if str(path) != value or ".." in path.parts:
        raise ValueError("image store root must be a canonical absolute path")
    return value


def _named_image(value: object) -> str:
    if (not isinstance(value, str) or len(value) > 512
            or not _NAMED_IMAGE.fullmatch(value)
            or value.startswith(("sha256:", "content:"))):
        raise ValueError("class image name must be an explicit repository:tag")
    return value


def _content_requirement(value: object) -> str:
    if not isinstance(value, str) or not _CONTENT_REF.fullmatch(value):
        raise ValueError("class image content must be a content:sha256 reference")
    return value


def normalize_class_image_requirements(value: object) -> dict[str, ClassImageRequirement]:
    """Validate a versioned declaration without borrowing another class's set.

    Names are resolution requirements, not action identities. Their expected
    content is always the existing store-independent reference. An explicit
    empty class is valid, but still needs a known inventory and matching store.
    """

    try:
        if (not isinstance(value, dict) or set(value) != {"schema", "classes"}
                or value.get("schema") != CLASS_REQUIREMENTS_SCHEMA):
            raise ValueError("invalid schema or fields")
        classes = value["classes"]
        if not isinstance(classes, dict) or len(classes) > MAX_REQUIREMENT_CLASSES:
            raise ValueError("classes must be a bounded object")
        result: dict[str, ClassImageRequirement] = {}
        for name, row in classes.items():
            if not isinstance(name, str) or not _CLASS_NAME.fullmatch(name):
                raise ValueError("invalid class name")
            if not isinstance(row, dict) or set(row) != {"store_root", "images"}:
                raise ValueError("class must declare only store_root and images")
            store = _canonical_store_root(row["store_root"])
            images = row["images"]
            if not isinstance(images, dict) or len(images) > MAX_CLASS_IMAGES:
                raise ValueError("images must be a bounded name-to-content object")
            pairs = tuple(sorted((_named_image(alias), _content_requirement(ref))
                                 for alias, ref in images.items()))
            result[name] = ClassImageRequirement(store, pairs)
        return {name: result[name] for name in sorted(result)}
    except ValueError as error:
        raise ValueError(f"class image requirements: {error}") from error


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate JSON key in image evidence")
        result[name] = value
    return result


def parse_named_inspect(text: str) -> dict[str, str]:
    """Extract a complete alias projection from supplied inspect rows only.

    No Docker read occurs here. Missing tag projections or conflicting aliases
    refuse the whole answer; last-write-wins must not hide image drift. Null
    RepoTags is Docker's explicit untagged form. The legacy parse_inspect API
    and its content identity remain unchanged.
    """

    if not isinstance(text, str) or len(text.encode("utf-8")) > MAX_INVENTORY_BYTES:
        raise ValueError("named image inspect must be bounded text")
    result: dict[str, str] = {}
    rows = 0
    for line in text.splitlines():
        if not line:
            continue
        rows += 1
        if rows > MAX_INVENTORY_ENTRIES:
            raise ValueError("too many named image inspect rows")
        try:
            row = json.loads(line, object_pairs_hook=_unique_json_object)
        except (ValueError, RecursionError) as error:
            raise ValueError("named image inspect row is not unambiguous JSON") from error
        ref = content_ref(row)
        if not isinstance(row, dict) or "RepoTags" not in row:
            raise ValueError("named image inspect row has no tag projection")
        tags = [] if row["RepoTags"] is None else row["RepoTags"]
        if not isinstance(tags, list) or len(tags) > MAX_INVENTORY_ENTRIES:
            raise ValueError("named image inspect tags must be a bounded list")
        for tag in tags:
            if tag == "<none>:<none>":
                continue
            alias = _named_image(tag)
            previous = result.get(alias)
            if previous is not None and previous != ref:
                raise ValueError("conflicting named image inspect alias")
            result[alias] = ref
            if len(result) > MAX_INVENTORY_ENTRIES:
                raise ValueError("too many named image inspect aliases")
    return {name: result[name] for name in sorted(result)}


def _inventory_record_fields(record: object, *, now: float,
                             ttl_s: float) -> tuple[float, frozenset[str] | None] | None:
    """The shared cache's schema, freshness and reference validation."""

    if not isinstance(record, dict) or record.get("schema") != INVENTORY_SCHEMA:
        return None
    observed = record.get("observed_unix")
    if isinstance(observed, bool) or not isinstance(observed, (int, float)):
        return None
    try:
        observed = float(observed)
    except OverflowError:
        return None
    if not math.isfinite(observed) or observed <= 0:
        return None
    age = now - observed
    if age < 0 or age > ttl_s:
        return None
    entries = record.get("entries")
    if entries is None:
        return observed, None
    if not isinstance(entries, list) or len(entries) > MAX_INVENTORY_ENTRIES:
        return None
    normalized: set[str] = set()
    for entry in entries:
        if not isinstance(entry, str):
            return None
        try:
            normalized.add(validate_ref(entry))
        except ValueError:
            return None
    return observed, frozenset(normalized)


def _snapshot_verdict(klass: str, now: float) -> dict[str, object]:
    return {"schema": CLASS_VERDICT_SCHEMA, "class": klass,
            "scope": "container_work_only", "authority": "supplied_snapshot_only",
            "evaluated_unix": now, "status": "unknown",
            "container_work_eligible": False, "reason": "inventory_unknown",
            "missing_images": [], "mismatched_images": []}


def class_image_verdict(requirements: object, klass: str, inventory: object, *,
                        now: float, max_age_s: float = INVENTORY_TTL_S) -> dict[str, object]:
    """Evaluate supplied evidence; this is not live permission to claim.

    A caller must supply a complete named projection and DockerRootDir beside
    the existing v2 inventory fields. Current cache producers do not supply
    those fields and therefore remain unknown to this offline contract. This
    function neither probes a daemon nor changes native-work admission.
    """

    parsed = normalize_class_image_requirements(requirements)
    if not isinstance(klass, str) or not _CLASS_NAME.fullmatch(klass):
        raise ValueError("class image requirements: invalid class name")
    bounds: list[float] = []
    for label, number in (("clock", now), ("freshness", max_age_s)):
        if isinstance(number, bool) or not isinstance(number, (int, float)):
            raise ValueError(f"class image evaluation {label} must be finite and positive")
        try:
            bound = float(number)
        except OverflowError as error:
            raise ValueError(f"class image evaluation {label} is not representable") from error
        if not math.isfinite(bound) or bound <= 0:
            raise ValueError(f"class image evaluation {label} must be finite and positive")
        bounds.append(bound)
    now, max_age_s = bounds
    if max_age_s > INVENTORY_TTL_S:
        raise ValueError("class image freshness cannot exceed the offer TTL")
    result = _snapshot_verdict(klass, float(now))
    requirement = parsed.get(klass)
    if requirement is None:
        result["reason"] = "class_requirements_missing"
        return result
    fields = _inventory_record_fields(inventory, now=now, ttl_s=max_age_s)
    if fields is None or fields[1] is None or not isinstance(inventory, dict):
        return result
    try:
        store = _canonical_store_root(inventory.get("store_root"))
        aliases = inventory.get("image_contents")
        if not isinstance(aliases, dict) or len(aliases) > MAX_INVENTORY_ENTRIES:
            return result
        contents = {_named_image(name): _content_requirement(ref)
                    for name, ref in aliases.items()}
        if any(ref not in fields[1] for ref in contents.values()):
            return result
    except ValueError:
        return result
    result.update(observed_unix=fields[0], expected_store_root=requirement.store_root,
                  observed_store_root=store)
    if store != requirement.store_root:
        result.update(status="refused", reason="image_store_mismatch")
        return result
    missing = [name for name, _ in requirement.images if name not in contents]
    mismatched = [{"name": name, "expected": ref, "observed": contents[name]}
                  for name, ref in requirement.images
                  if name in contents and contents[name] != ref]
    result.update(missing_images=missing, mismatched_images=mismatched)
    if missing:
        result.update(status="refused", reason="required_image_missing")
    elif mismatched:
        result.update(status="refused", reason="image_content_mismatch")
    else:
        result.update(status="satisfied", reason="class_images_present",
                      container_work_eligible=True)
    return result


def image_ids(text: str) -> tuple[str, ...]:
    """The distinct image IDs one ``docker image ls`` listing reports.

    In listing order, deduplicated: a listing names one image once per tag,
    and the inspect that follows must ask for each image once.
    """

    seen: list[str] = []
    known: set[str] = set()
    for line in text.splitlines():
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 3:
            raise ValueError("unexpected docker image listing row shape")
        image_id = fields[0]
        if not _IMAGE_ID.fullmatch(image_id):
            raise ValueError("docker image listing row does not start with an ID")
        if image_id not in known:
            known.add(image_id)
            seen.append(image_id)
    return tuple(seen)


def missing(required, present) -> tuple[str, ...]:
    """Which requirements ``present`` does not positively satisfy.

    Exact string membership is the whole test, and that is the point: the
    inventory presents IDs bare and manifest digests repository-qualified, so
    a hex collision between the two kinds cannot satisfy either.
    """

    known = {str(entry) for entry in present}
    return tuple(str(ref) for ref in required if str(ref) not in known)


def _read_capped(stream, *, limit: int, deadline: float) -> bytes | None:
    """Read a nonblocking stream up to ``limit`` bytes before ``deadline``.

    The cap is enforced *while reading*: one ``os.read`` of at most
    :data:`PROBE_CHUNK_BYTES` (or the remaining budget plus one byte, so an
    overrun is detectable) at a time, with the deadline re-checked between
    reads.  ``stream.read()`` is deliberately never called: with no size, a
    nonblocking ``BufferedReader`` may drain a continuously-fed pipe into a
    single arbitrarily large object, which is a bound checked after the
    allocation rather than a bound on it.  A read that returns nothing yet is
    a spurious wakeup and the loop continues.
    """

    descriptor = stream.fileno()
    os.set_blocking(descriptor, False)
    chunks: list[bytes] = []
    total = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            ready, _, _ = select.select([descriptor], [], [], remaining)
        except (OSError, ValueError):
            return None
        if not ready:
            return None
        request = min(PROBE_CHUNK_BYTES, limit - total + 1)
        try:
            chunk = os.read(descriptor, request)
        except (BlockingIOError, InterruptedError):
            continue                           # spurious wakeup, no data yet
        except OSError:
            return None
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def _signal_probe_group(pid: int) -> None:
    """SIGKILL the probe's process group while its pid is still ours.

    ``start_new_session`` made the spawned process a session and group
    leader, so its pid is the group id.  This is called only while that
    process is an unreaped child of this process: an unreaped child keeps its
    pid allocated, so the group id cannot have been recycled and the signal
    reaches the probe's own descendants and nothing else.  Failure is silent;
    a group that no longer exists is already gone.
    """

    try:
        os.killpg(pid, signal.SIGKILL)
    except OSError:
        pass


def _probe_exit_status(process, deadline: float):
    """``("exited", code)`` without reaping, ``("reaped", code)``, or ``None``.

    Linux reports through ``waitid(... | WNOWAIT)``, which leaves the child
    unreaped: the caller can still signal its group under the ownership the
    pid had when spawned.  A platform without ``waitid`` has no such report;
    its only path reaps the leader and gives that ownership up, so the caller
    is told which happened and keeps the weaker cleanup.
    """

    if not hasattr(os, "waitid"):
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except (subprocess.TimeoutExpired, OSError, subprocess.SubprocessError):
            return None
        return ("reaped", process.returncode)
    while True:
        try:
            info = os.waitid(os.P_PID, process.pid,
                             os.WEXITED | os.WNOHANG | os.WNOWAIT)
        except (ChildProcessError, OSError):
            return None
        if info is not None:
            code = info.si_status if info.si_code == os.CLD_EXITED else -1
            return ("exited", code)
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.01)


def _run_bounded(argv, *, env, timeout_s: float, limit: int) -> bytes | None:
    """One subprocess read, bounded in time and bytes; ``None`` on any failure.

    Cleanup is ownership-safe.  On every path that did not observe a clean
    exit, the whole process group the probe was spawned in is signalled while
    the leader is still unreaped: a leader that exited leaving a descendant
    holding the stdout pipe cannot leak that descendant, and a pid that has
    been reaped is never signalled after it could have been recycled.
    """

    try:
        process = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env,
            # No shell: argv is a list.  Its own process group, so a probe
            # that spawned children is cleaned up rather than leaked.
            start_new_session=True,
        )
    except (OSError, ValueError):
        return None
    deadline = time.monotonic() + timeout_s
    clean = False
    try:
        stream = process.stdout
        if stream is None:
            return None
        data = _read_capped(stream, limit=limit, deadline=deadline)
        if data is None:
            return None
        report = _probe_exit_status(process, deadline)
        if report is None:
            return None
        if report[1] != 0:
            return None
        clean = True
        return data
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    finally:
        if not clean and process.returncode is None:
            _signal_probe_group(process.pid)
        if process.returncode is None:
            try:
                process.wait(timeout=1.0)
            except (OSError, subprocess.SubprocessError):
                # A leader wedged past SIGKILL must not hold the worker's
                # poll open; the group signal has already gone out.
                pass
        if process.stdout is not None:
            try:
                process.stdout.close()
            except OSError:
                pass


def _observe(
    *,
    probe=None,
    timeout_s: float = INVENTORY_TIMEOUT_S,
    docker: str | None = None,
    retain_metadata: bool = False,
) -> tuple[frozenset[str] | None, dict[str, object]]:
    """Read legacy references and optional class inputs under one budget.

    Listing/inspect failures make the reference inventory unknown. Failed
    supplemental name/store evidence does not discard valid references, but
    cannot satisfy a class. The store is observed from the same local daemon,
    never supplied by a requirement. It is lexical DockerRootDir evidence,
    not daemon, driver or filesystem attestation.
    """

    binary = docker or shutil.which("docker") or "/usr/bin/docker"
    environment = dict(os.environ)
    environment.pop("DOCKER_HOST", None)
    environment.pop("DOCKER_CONTEXT", None)
    deadline = time.monotonic() + timeout_s
    read = _run_bounded if probe is None else probe

    def _take(argv):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            data = read(argv, env=environment, timeout_s=remaining,
                        limit=MAX_INVENTORY_BYTES)
        except (OSError, subprocess.SubprocessError, ValueError):
            return None
        if not isinstance(data, bytes) or len(data) > MAX_INVENTORY_BYTES:
            return None
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return None

    listing = _take([
        binary, "--host", LOCAL_ENDPOINT,
        "image", "ls", "--all", "--no-trunc", "--digests",
        "--format", _LIST_FORMAT,
    ])
    if listing is None:
        return None, {}
    try:
        entries = set(parse_inventory(listing))
        identifiers = image_ids(listing)
    except ValueError:
        return None, {}
    metadata: dict[str, object] = {}
    if len(identifiers) > MAX_INVENTORY_ENTRIES:
        return None, {}
    if identifiers:
        inspected = _take([
            binary, "--host", LOCAL_ENDPOINT,
            "image", "inspect", "--format", _INSPECT_FORMAT, *identifiers,
        ])
        if inspected is None:
            # Removal between listing and inspect remains unknown, not a
            # partial reference inventory that could deny content as absent.
            return None, {}
        try:
            entries |= parse_inspect(inspected)
        except ValueError:
            return None, {}
        if retain_metadata:
            try:
                metadata["image_contents"] = parse_named_inspect(inspected)
            except ValueError:
                metadata["image_contents"] = None
    elif retain_metadata:
        metadata["image_contents"] = {}
    if retain_metadata:
        metadata["store_root"] = None
        info = _take([binary, "--host", LOCAL_ENDPOINT,
                      "info", "--format", "{{json .DockerRootDir}}"])
        if info is not None:
            try:
                metadata["store_root"] = _canonical_store_root(json.loads(info))
            except (ValueError, RecursionError):
                pass
    return frozenset(entries), metadata


def observe(
    *,
    probe=None,
    timeout_s: float = INVENTORY_TIMEOUT_S,
    docker: str | None = None,
) -> frozenset[str] | None:
    """One bounded local reference reading; ``None`` is unknown, not empty.

    The public legacy API remains listing plus one inspect, sharing one
    timeout. Cache refresh optionally retains names from those same inspect
    bytes and reads store metadata with only the remaining budget.
    """

    return _observe(probe=probe, timeout_s=timeout_s, docker=docker)[0]


def _open_private_directory(path: Path) -> int | None:
    """Open this uid's private cache directory, creating it ``0700``.

    The same discipline the worker's offer-publication lock applies: a
    writable or symlinked parent would let anyone replace the record the
    checks below validated, so the directory is opened ``O_NOFOLLOW``, must
    be a real directory owned by this uid with no group/world bits, and the
    record and lock are opened through that descriptor with ``dir_fd``.
    """

    try:
        descriptor = os.open(
            path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        try:
            os.mkdir(path, 0o700)
        except FileExistsError:
            pass
        except OSError:
            return None
        try:
            descriptor = os.open(
                path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        except OSError:
            return None
    except OSError:
        return None
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077):
            os.close(descriptor)
            return None
    except OSError:
        os.close(descriptor)
        return None
    return descriptor


def _read_regular(directory: int, name: str, *, limit: int) -> bytes | None:
    """Read a stable, this-uid, size-capped regular file; ``None`` otherwise."""

    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOFOLLOW
    try:
        descriptor = os.open(name, flags, dir_fd=directory)
    except OSError:
        return None
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or before.st_nlink != 1 or before.st_size > limit):
            return None
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                return None
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mode,
                before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mode,
                after.st_mtime_ns, after.st_ctime_ns):
            return None
        return b"".join(chunks)
    except OSError:
        return None
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass


def _write_record(directory: int, name: str, payload: bytes) -> bool:
    """Atomically replace one record inside the private directory."""

    temporary = f".{name}.{os.getpid()}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        descriptor = os.open(temporary, flags, 0o600, dir_fd=directory)
    except OSError:
        return False
    try:
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    except OSError:
        try:
            os.unlink(temporary, dir_fd=directory)
        except OSError:
            pass
        return False
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass
    try:
        os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
    except OSError:
        try:
            os.unlink(temporary, dir_fd=directory)
        except OSError:
            pass
        return False
    return True


class InventoryCache:
    """One box's bounded image inventory, shared by every worker loop on it.

    The record lives on host-local disk because the loops are separate
    processes and the daemon is box-wide: one loop refreshes after the
    requested freshness under a local lock and the rest read the same small
    record, so a box with sixteen loops pays one Docker listing per interval
    rather than sixteen per poll.  A loop that finds the lock held waits for
    the sibling's record, bounded by :attr:`timeout_s`, instead of reading
    the stale one (#1143).  Every failure -- an unsafe directory, a malformed
    or stale record, a sibling refresh that overruns the timeout, a failed
    probe -- leaves the caller with ``None``: unknown, which refuses every
    requirement and lets ordinary work through untouched.
    """

    def __init__(
        self,
        *,
        ttl_s: float = INVENTORY_TTL_S,
        timeout_s: float = INVENTORY_TIMEOUT_S,
        root: str | Path | None = None,
        probe=None,
        docker: str | None = None,
        clock=time.time,
    ) -> None:
        self.ttl_s = float(ttl_s)
        self.timeout_s = float(timeout_s)
        self.root = Path(root) if root is not None else Path(
            f"/tmp/prismabuild-container-images-{os.getuid()}")
        self.probe = probe
        self.docker = docker
        self.clock = clock

    @property
    def record_name(self) -> str:
        return "inventory.json"

    def _record(self, directory: int, *, now: float):
        """The parsed record, or ``None`` when it is not usable evidence."""

        payload = _read_regular(directory, self.record_name,
                                limit=MAX_INVENTORY_BYTES)
        if payload is None:
            return None
        try:
            record = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None
        return _inventory_record_fields(record, now=now, ttl_s=self.ttl_s)

    def _refresh(self, directory: int, *, limit: float) -> None:
        """Re-probe the shared record, or wait for the sibling that is.

        One loop probes under ``refresh.lock``; a loop that finds it held
        waits for that probe, polling the lock until :attr:`timeout_s` runs
        out, and then reads the sibling's record rather than probing again
        (#1143).  Returning at once made every sibling's claim read a stale
        record as unknown after each refresh boundary.  The wait and any
        probe spend one :attr:`timeout_s` budget, so a claim pass is never
        held past the inventory timeout; a sibling that overruns it leaves
        this caller unknown, exactly as before.
        """

        deadline = time.monotonic() + self.timeout_s
        try:
            lock = os.open(
                "refresh.lock",
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600,
                dir_fd=directory)
        except OSError:
            return
        try:
            info = os.fstat(lock)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_nlink != 1):
                return
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    pass                    # a sibling loop is refreshing
                except OSError:
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return                  # the sibling overran our budget
                time.sleep(min(REFRESH_LOCK_POLL_S, remaining))
            # Under the lock.  A sibling that held it has just written its
            # record, and one may also have finished between the caller's read
            # and this lock: either way, that record answers, and the box
            # pays one listing, not one per loop.
            cached = self._record(directory, now=self.clock())
            if cached is not None and self.clock() - cached[0] <= limit:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            entries, metadata = _observe(probe=self.probe,
                                         timeout_s=min(self.timeout_s, remaining),
                                         docker=self.docker, retain_metadata=True)
            record = {
                "schema": INVENTORY_SCHEMA,
                "observed_unix": self.clock(),
                "entries": None if entries is None else sorted(entries),
                **metadata,
            }
            payload = (json.dumps(record, sort_keys=True) + "\n").encode()
            if len(payload) > MAX_INVENTORY_BYTES and metadata:
                # Supplemental evidence cannot turn a readable legacy
                # inventory into an oversized, hence unknown cache record.
                for field in metadata:
                    record.pop(field, None)
                payload = (json.dumps(record, sort_keys=True) + "\n").encode()
            _write_record(directory, self.record_name, payload)
        finally:
            try:
                os.close(lock)
            except OSError:
                pass

    def get(self, *, max_age_s: float | None = None) -> frozenset[str] | None:
        """The inventory for this poll, or ``None`` when it is unknown.

        ``max_age_s`` is how stale the evidence a caller will act on may be;
        it defaults to the offer TTL.  A claim passes
        :data:`CLAIM_FRESHNESS_S`, which re-probes while image-pinned work is
        waiting -- once for the box, under the local lock, outside every pool
        lock.
        """

        limit = self.ttl_s if max_age_s is None else min(self.ttl_s, max_age_s)
        directory = _open_private_directory(self.root)
        if directory is None:
            return None
        try:
            cached = self._record(directory, now=self.clock())
            if cached is not None and self.clock() - cached[0] <= limit:
                return cached[1]
            self._refresh(directory, limit=limit)
            cached = self._record(directory, now=self.clock())
            if cached is not None and self.clock() - cached[0] <= limit:
                return cached[1]
            return None
        finally:
            try:
                os.close(directory)
            except OSError:
                pass


def local_content_ref(
    name: str,
    *,
    probe=None,
    timeout_s: float = INVENTORY_TIMEOUT_S,
    docker: str | None = None,
) -> str | None:
    """The ``content:sha256:...`` reference for one locally named image.

    This is how a submitter gets a reference to seal: name the image the way
    a human has it (``repo:tag``, an ID, anything the local daemon resolves)
    and read back the portable form.  The same bounded, endpoint-pinned,
    environment-scrubbed read the inventory uses; ``None`` on any failure.
    """

    binary = docker or shutil.which("docker") or "/usr/bin/docker"
    environment = dict(os.environ)
    environment.pop("DOCKER_HOST", None)
    environment.pop("DOCKER_CONTEXT", None)
    read = _run_bounded if probe is None else probe
    try:
        data = read([binary, "--host", LOCAL_ENDPOINT, "image", "inspect",
                     "--format", _INSPECT_FORMAT, name],
                    env=environment, timeout_s=timeout_s,
                    limit=MAX_INVENTORY_BYTES)
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    if not isinstance(data, bytes):
        return None
    try:
        refs = parse_inspect(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return next(iter(refs)) if len(refs) == 1 else None


def _read_offline_json(path: Path) -> object:
    """Size-capped input or None, never a cache read or Docker fallback."""

    try:
        with path.open("rb") as source:
            payload = source.read(MAX_INVENTORY_BYTES + 1)
        if len(payload) > MAX_INVENTORY_BYTES:
            return None
        return json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except (OSError, UnicodeError, ValueError, RecursionError):
        return None


def _class_verdict_main(names: list[str]) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python3 -m prismabuild.container_images --class-verdict",
        description="Evaluate explicit snapshots only; never probe or authorize a claim.")
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--class", dest="klass", required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--max-age-s", type=float, default=INVENTORY_TTL_S)
    args = parser.parse_args(names)
    try:
        requirements = _read_offline_json(args.requirements)
        normalize_class_image_requirements(requirements)
    except (OSError, UnicodeError, ValueError, RecursionError):
        result = _snapshot_verdict(args.klass, time.time())
        result["reason"] = "requirements_invalid"
        print(json.dumps(result, sort_keys=True))
        return 2
    try:
        inventory = _read_offline_json(args.inventory)
    except (OSError, UnicodeError, ValueError, RecursionError):
        inventory = None
    try:
        result = class_image_verdict(requirements, args.klass, inventory,
                                     now=time.time(), max_age_s=args.max_age_s)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "satisfied" else 1


def main(argv=None) -> int:
    """``python3 -m prismabuild.container_images REF [REF ...]``.

    Prints ``<name>\\t<content reference>`` for each image, so a submitter can
    read a portable reference off the box that holds the image and hand it to
    ``pbrun --container-image``.
    """

    import sys

    names = list(sys.argv[1:] if argv is None else argv)
    if names and names[0] == "--class-verdict":
        return _class_verdict_main(names[1:])
    if not names:
        print("usage: python3 -m prismabuild.container_images REF [REF ...]",
              file=sys.stderr)
        return 2
    status = 0
    for name in names:
        reference = local_content_ref(name)
        if reference is None:
            print(f"{name}\tunreadable", file=sys.stderr)
            status = 1
        else:
            print(f"{name}\t{reference}")
    return status


if __name__ == "__main__":       # pragma: no cover - a submitter's helper
    raise SystemExit(main())
