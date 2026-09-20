"""Exact container-image requirements, and the bounded inventory behind them.

A sealed action may declare the container images it needs *present on its
claiming box* before it runs (``pbrun --container-image``).  PrismaBuild does
not pull, load, copy or otherwise transfer an image: the declaration is a
placement requirement over the box's own Docker, and the inventory here is
what lets a claim refuse a box that cannot positively show the reference.
#714 is the failure this exists to end -- an action pinned to an image that
existed on one Spark was claimed by the other, and died inside its wrapper
after the attempt was already spent.

Two reference forms are accepted, and they are deliberately not aliases:

* ``sha256:<64 hex>`` names the local **image ID** (Docker's config digest).
  It is satisfied only by an image the inventory reports by that ID.
* ``repository@sha256:<64 hex>`` names a repository **manifest digest**.  It is
  satisfied only by that exact ``repository@sha256:...`` string among the
  box's RepoDigests.  A bare digest from a RepoDigest is never presented, so a
  manifest digest cannot accidentally satisfy an ID requirement (or the
  reverse) merely because the hex matches.

A mutable tag (``repo:tag``) is refused at declaration time: it is not an
identity, so it cannot be sealed into an action key, and it would let the
requirement move under the receipt it was admitted against.

The inventory is one ``docker image ls`` read, pinned to the local Unix daemon
socket, bounded by :data:`INVENTORY_TIMEOUT_S` and by
:data:`MAX_INVENTORY_BYTES` -- the output is read through a capped,
deadline-bound stream, never accumulated and then measured.  A failure, a
timeout, an unexpected listing shape or an oversized answer is **unknown**,
never empty.  An unknown inventory satisfies no requirement.

Two staleness bounds, deliberately different:

* The **offer** reads the shared record with :data:`INVENTORY_TTL_S`, so the
  placement surface is one bounded Docker listing per box per TTL rather than
  one per worker loop per poll.
* A **claim** reads it with :data:`CLAIM_FRESHNESS_S`: while image-pinned
  work is waiting, the record is re-probed (once for the box, under the same
  local lock) at least that often before a claim uses it.  The probe runs in
  the worker's poll, outside every pool lock.  A claim can still race an
  image removal that happens after the observation and before the container
  starts; that interval is small and documented rather than claimed closed.

The record lives in this uid's private directory (``0700``, checked on open,
no symlinks followed) and is read as a stable, size-capped regular file whose
schema, timestamp, reference syntax and entry count are all validated.  A
malformed or replaced record is unknown, so it can never authorize a claim.

Archive-backed containers stay the consumer's own behavior.  A spec that loads
its image from an archive at run time (PrismaQuant's ``container.archive``)
establishes presence *inside* the action, so it must not declare
``container_images``: PB would refuse the claim before the loader ever ran.
Declare only images that are already local when the action is claimed.
"""
from __future__ import annotations

import fcntl
import json
import math
import os
from pathlib import Path
import re
import select
import shutil
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

#: One Docker metadata read's ceiling.  A probe that overruns is unknown.
INVENTORY_TIMEOUT_S = 5.0

#: The local system daemon, the same endpoint the action Docker shim pins.
#: ``DOCKER_HOST`` and ``DOCKER_CONTEXT`` are scrubbed from the probe's
#: environment so a remote context cannot answer for this box's frames.
LOCAL_ENDPOINT = "unix:///var/run/docker.sock"

#: Refuse an answer larger than this, and a record holding more entries.
MAX_INVENTORY_BYTES = 8 * 1024 * 1024
MAX_INVENTORY_ENTRIES = 4096

INVENTORY_SCHEMA = "prismabuild.container_image_inventory.v1"

_DIGEST = r"sha256:[0-9a-f]{64}"
_IMAGE_ID = re.compile(rf"{_DIGEST}\Z")
_REPO_DIGEST = re.compile(rf"[a-zA-Z0-9][a-zA-Z0-9._:/-]*@{_DIGEST}\Z")

#: ID, repository and digest per image, tab-separated: one listing covers both
#: reference forms, and ``--all`` keeps dangling images visible.
_LIST_FORMAT = "{{.ID}}\t{{.Repository}}\t{{.Digest}}"


def validate_ref(value: object) -> str:
    """Return ``value`` if it is an immutable image reference, else raise.

    The refusal names both accepted forms and says why a tag cannot be one:
    an action's image requirement is sealed into its key, so it has to be an
    identity the receipt can be held against.
    """

    if not isinstance(value, str) or not value:
        raise ValueError(
            "container image must be a nonempty string: sha256:<64 hex> for a "
            "local image ID, or repository@sha256:<64 hex> for a manifest "
            "digest")
    if _IMAGE_ID.fullmatch(value) or _REPO_DIGEST.fullmatch(value):
        return value
    raise ValueError(
        f"{value!r} is not an immutable container image reference; use "
        "sha256:<64 hex> or repository@sha256:<64 hex>. A mutable tag cannot "
        "be part of an action's identity")


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

    The cap is enforced while reading, not after: a Docker answer that grows
    past the bound is refused without first being held in memory.
    """

    os.set_blocking(stream.fileno(), False)
    chunks: list[bytes] = []
    total = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            ready, _, _ = select.select([stream], [], [], remaining)
        except (OSError, ValueError):
            return None
        if not ready:
            return None
        try:
            chunk = stream.read()
        except (OSError, ValueError):
            return None
        if chunk is None:                      # spurious wakeup, no data yet
            continue
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def _run_bounded(argv, *, env, timeout_s: float, limit: int) -> bytes | None:
    """One subprocess read, bounded in time and bytes; ``None`` on any failure."""

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
    try:
        stream = process.stdout
        if stream is None:
            return None
        data = _read_capped(stream, limit=limit, deadline=deadline)
        if data is None:
            return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        process.wait(timeout=remaining)
        if process.returncode != 0:
            return None
        return data
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    finally:
        if process.poll() is None:
            try:
                os.killpg(process.pid, 9)
            except OSError:
                try:
                    process.kill()
                except OSError:
                    pass
            try:
                process.wait(timeout=1.0)
            except (OSError, subprocess.SubprocessError):
                pass
        if process.stdout is not None:
            try:
                process.stdout.close()
            except OSError:
                pass


def observe(
    *,
    probe=None,
    timeout_s: float = INVENTORY_TIMEOUT_S,
    docker: str | None = None,
) -> frozenset[str] | None:
    """One bounded local reading; ``None`` means unknown, never empty.

    Every failure mode collapses here deliberately -- no binary, a refused
    daemon, a timeout, a foreign endpoint, a malformed or oversized answer --
    because they all mean the same thing to a claim: this box cannot show the
    image, so it must not take the work.
    """

    binary = docker or shutil.which("docker") or "/usr/bin/docker"
    environment = dict(os.environ)
    environment.pop("DOCKER_HOST", None)
    environment.pop("DOCKER_CONTEXT", None)
    argv = [
        binary, "--host", LOCAL_ENDPOINT,
        "image", "ls", "--all", "--no-trunc", "--digests",
        "--format", _LIST_FORMAT,
    ]
    read = _run_bounded if probe is None else probe
    try:
        data = read(argv, env=environment, timeout_s=timeout_s,
                    limit=MAX_INVENTORY_BYTES)
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    if not isinstance(data, bytes):
        return None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    try:
        return parse_inventory(text)
    except ValueError:
        return None


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
    requested freshness under a nonblocking local lock and the rest read the
    same small record, so a box with sixteen loops pays one Docker listing
    per interval rather than sixteen per poll.  Every failure -- an unsafe
    directory, a malformed or stale record, a held lock, a failed probe --
    leaves the caller with ``None``: unknown, which refuses every requirement
    and lets ordinary work through untouched.
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
        if not isinstance(record, dict) or record.get("schema") != INVENTORY_SCHEMA:
            return None
        observed = record.get("observed_unix")
        if isinstance(observed, bool) or not isinstance(observed, (int, float)):
            return None
        observed = float(observed)
        if not math.isfinite(observed) or observed <= 0:
            return None
        age = now - observed
        if age < 0 or age > self.ttl_s:
            # A timestamp in the future or older than the TTL is not evidence;
            # accepting the future form made a corrupt record fresh forever.
            return None
        entries = record.get("entries")
        if entries is None:
            return observed, None
        if (not isinstance(entries, list)
                or len(entries) > MAX_INVENTORY_ENTRIES):
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

    def _refresh(self, directory: int) -> None:
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
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return                      # a sibling loop is refreshing
            entries = observe(probe=self.probe, timeout_s=self.timeout_s,
                              docker=self.docker)
            record = {
                "schema": INVENTORY_SCHEMA,
                "observed_unix": self.clock(),
                "entries": None if entries is None else sorted(entries),
            }
            _write_record(directory, self.record_name,
                          (json.dumps(record, sort_keys=True) + "\n").encode())
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
            self._refresh(directory)
            cached = self._record(directory, now=self.clock())
            if cached is not None and self.clock() - cached[0] <= limit:
                return cached[1]
            return None
        finally:
            try:
                os.close(directory)
            except OSError:
                pass
