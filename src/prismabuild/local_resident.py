"""Verified whole-tree copies and their ordinary host-pinned movement actions."""
from contextlib import contextmanager
import os
from pathlib import Path
import stat
import time
import json
import shutil
import socket
import uuid
from . import posix_lock

from . import core, local_tier, movement_actions, pool, reader_lease, resident_sets, residency_map

BLOCK_BYTES = 8 * 1024 * 1024


def hash_file(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError(f"not a regular file: {path}")
        digest = core.new_sha256()
        size = 0
        while block := stream.read(BLOCK_BYTES):
            digest.update(block)
            size += len(block)
        return size, digest.hexdigest()


def copy_file(source, destination, entry):
    """Hash the bytes written, then fsync before any file is considered landed."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    dest_fd = None
    try:
        if not stat.S_ISREG(os.fstat(source_fd).st_mode):
            raise ValueError(f"not a regular copy source: {source}")
        dest_fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(source_fd, "rb", closefd=False) as src, os.fdopen(dest_fd, "wb", closefd=False) as dst:
            digest = core.new_sha256()
            size = 0
            while block := src.read(BLOCK_BYTES):
                dst.write(block)
                digest.update(block)
                size += len(block)
            dst.flush()
            os.fsync(dst.fileno())
        if size != entry["bytes"] or digest.hexdigest() != entry["sha256"]:
            raise ValueError(f"sha256 byte verification failed: {entry['path']}")
        resident_sets.fsync_directory(destination.parent)
        return digest.hexdigest()
    finally:
        os.close(source_fd)
        if dest_fd is not None:
            os.close(dest_fd)


@contextmanager
def sources(store, record, reader_context):
    """Prefer published stage bytes only while their ordinary reader pin is held."""
    canonical = {entry["path"]: entry["path"] for entry in record["manifest"]["entries"]}
    queue = pool.PoolQueue(store.queue_root)
    base = queue.root / pool.RESIDENCY
    expected = {residency_map.residency_map_key(entry["path"], 0):
                {"bytes": entry["bytes"], "sha256": entry["sha256"]}
                for entry in record["manifest"]["entries"]}
    material_root = base / "material"
    pinned = None
    if reader_context and material_root.exists():
        seen = set()
        for namespace in sorted(material_root.iterdir()):
            if not namespace.is_dir():
                continue
            for path in sorted(namespace.glob("*.json")):
                material = reader_lease.read_material(base, namespace.name, path.stem)
                if not isinstance(material, dict):
                    continue
                tier = material["tier_id"]
                if tier.startswith(("ram:", "local:")):
                    continue
                signature = (namespace.name, tier, material["manifest_sha256"])
                if signature in seen:
                    continue
                seen.add(signature)
                cover = reader_lease.covers_for_keys(base, namespace.name, list(expected),
                    tier_id=tier, manifest_sha256=material["manifest_sha256"], epoch="")
                if not cover.get("ok"):
                    continue
                answer = reader_lease.acquire_for(reader_context, tier_id=tier, epoch="",
                    covers=cover["covers"], expected=expected,
                    span={"start_bytes": 0, "end_bytes": record["manifest"]["total_bytes"]},
                    acquire_token="resident-copy:" + record["set_id"], material_namespace=namespace.name)
                if answer.get("ok"):
                    pinned = answer
                    break
            if pinned:
                break
    try:
        if pinned:
            paths = {entry["key"]: entry["stage_path"] for entry in pinned["pin"]["entries"]}
            yield {path: paths[residency_map.residency_map_key(path, 0)] for path in canonical}
        else:
            yield canonical
    finally:
        if pinned:
            released = reader_lease.release(queue, pinned["pin_id"], pinned["ref_id"],
                consumer_action_key=pinned["pin"]["owner_action_key"], stage_root=pinned["pin"]["stage_root"])
            if released is not True:
                raise ValueError(f"stage reader pin release failed: {released}")


def _paths(set_id, spec):
    resident_sets._set_id(set_id)
    root = Path(spec["root"])
    if root.is_symlink():
        raise ValueError("unsafe local tier root")
    root.mkdir(parents=True, exist_ok=True)
    return root, root / set_id, root / (set_id + ".partial"), root / (set_id + ".evicting")


def _verify_tree(root, record):
    expected = {str(Path(entry["path"]).relative_to(record["canonical_root"])): entry["bytes"]
                for entry in record["manifest"]["entries"]}
    if resident_sets.directory_files(root) != expected:
        raise ValueError("local tree does not cover the whole manifest")
    receipt = []
    for entry in record["manifest"]["entries"]:
        relative = str(Path(entry["path"]).relative_to(record["canonical_root"]))
        size, digest = hash_file(root / relative)
        if size != entry["bytes"] or digest != entry["sha256"]:
            raise ValueError(f"sha256 byte verification failed: {entry['path']}")
        receipt.append({"path": entry["path"], "bytes": size, "sha256": digest})
    return receipt


def copy(store, set_id, host, spec, *, reader_context=None, now=None):
    record = store.read(set_id)
    root, final, partial, evicting = _paths(set_id, spec)
    queue = pool.PoolQueue(store.queue_root)
    # A mover lock serializes retries without holding the host transition lock
    # across source reads. Eviction checks this lock nonblocking. The lease
    # check and the token reservation both happen INSIDE it, after the
    # evicting check: an eviction that releases the ledger while this copy
    # waited on the lock must not leave a resident tree holding no tokens,
    # and a copy whose lease expired while queued must copy nothing.
    with posix_lock.held(store.copy_path(set_id, host).with_suffix(".move.lock")):
        current = store.read_copy(set_id, host)
        if evicting.exists() or current["state"] == "evicting":
            raise ValueError("resident copy is being evicted")
        if not lease_active(store, set_id, now=now):
            raise ValueError("resident lease expired")
        local_tier.reserve(queue, set_id, [host], record["manifest"]["total_bytes"])
        if final.exists():
            verification = _verify_tree(final, record)
        else:
            partial.mkdir(exist_ok=True)
            if partial.is_symlink():
                raise ValueError("unsafe partial tree")
            # A resumed tree must not smuggle extra files into the final directory.
            expected_names = {str(Path(e["path"]).relative_to(record["canonical_root"])) for e in record["manifest"]["entries"]}
            if set(resident_sets.directory_files(partial)) - expected_names:
                raise ValueError("partial tree contains unlisted files")
            with local_tier.host_lock(root):
                store.write_copy(set_id, host, {**current, "state": "copying", "local_root": str(final)})
            remaining = sum(entry["bytes"] for entry in record["manifest"]["entries"]
                            if not (partial / Path(entry["path"]).relative_to(record["canonical_root"])).exists())
            local_tier.require_write_space(spec, os.statvfs(root), remaining)
            stamp = {}
            verification = []
            with sources(store, record, reader_context) as paths:
                for entry in record["manifest"]["entries"]:
                    relative = Path(entry["path"]).relative_to(record["canonical_root"])
                    destination = partial / relative
                    if destination.exists():
                        size, digest = hash_file(destination)
                        reusable = size == entry["bytes"] and digest == entry["sha256"]
                    else:
                        reusable = False
                    if not reusable:
                        local_tier.require_write_space(spec, os.statvfs(root), entry["bytes"])
                        digest = copy_file(paths[entry["path"]], destination, entry)
                    else:
                        # An interrupted run may have died before its fsync.
                        with destination.open("rb") as stream:
                            os.fsync(stream.fileno())
                    info = os.stat(paths[entry["path"]])
                    stamp[entry["path"]] = {"size": info.st_size, "mtime_ns": info.st_mtime_ns}
                    verification.append({"path": entry["path"], "bytes": entry["bytes"], "sha256": digest})
                # All files are fsynced. Persist nested directory entries too.
                for directory, _, _ in os.walk(partial, topdown=False):
                    resident_sets.fsync_directory(directory)
                with local_tier.host_lock(root):
                    os.rename(partial, final)
                    resident_sets.fsync_directory(root)
            current["source_stamp"] = stamp
        with local_tier.host_lock(root):
            result = {**current, "state": "resident", "local_root": str(final),
                      "verification": verification, "bytes": record["manifest"]["total_bytes"], "completed_unix": time.time()}
            store.write_copy(set_id, host, result)
        return store.read_copy(set_id, host)


def movement_action(template, store, set_id, tier, *, policy_path, operation, source=None):
    host = tier["host"]
    python, tool, _ = movement_actions.movement_tools(tier, mover="local_resident.py")
    command = [python, tool, "--pool-root", str(store.queue_root), "--set-id", set_id,
               "--host", host, "--policy", str(policy_path), "--operation", operation]
    if operation == "adopt":
        if source is None:
            raise ValueError("adoption movement requires source")
        command.extend(["--source", str(source)])
    return movement_actions.seal_movement_action(template, command=command,
        demand={"cpu": 1, "mem_gb": 1}, tags=[host], log_name=f"resident-{operation}-{set_id}-{host}.log")


def publish_actions(template, store, set_id, tiers, *, policy_path):
    """Publish copies now and retain host-pinned egress rows for lease expiry."""
    record = store.read(set_id)
    if not lease_active(store, set_id):
        raise ValueError("resident lease expired")
    queue = pool.PoolQueue(store.queue_root)
    cas = template["cas"]
    by_host = {tier["host"]: tier for tier in tiers if tier.get("tier_id", "").startswith("local:")}
    rows = {}
    for host in record["hosts"]:
        tier = by_host[host]
        pair = {}
        for operation in ("copy", "evict"):
            action = movement_action(template, store, set_id, tier, policy_path=policy_path, operation=operation)
            cas.publish_action_request(action)
            pair[operation] = movement_row(action, cas, tier)
        store.update_movements(set_id, host, pair)
        if store.read_copy(set_id, host)["state"] != "resident":
            try:
                queue.publish(**pair["copy"], recompute=True, refuse_if_live=True)
            except pool.ActionAlreadyLiveError:
                pass  # Attach to the existing generation; never replace a live copy.
        rows[host] = pair
    return rows


def _resident_live_rows(store, set_id):
    for state in (pool.READY, pool.CLAIMED):
        directory = store.queue_root / state
        if not directory.exists():
            continue
        for path in directory.glob("*.json"):
            try:
                row = json.loads(path.read_text())
            except (ValueError, OSError):
                # Unknown rows can extend only up to the explicit hard maximum.
                return True
            if row.get("resident_set") == set_id:
                return True
    return False


def lease_active(store, set_id, *, now=None):
    now = time.time() if now is None else now
    store.read(set_id)
    lease = None
    released = False
    for event in store.read_lease_log(set_id):
        if event.get("manifest_sha256") != set_id:
            raise ValueError("lease journal manifest disagrees")
        if event["event"] in ("published", "renewed"):
            lease = event["lease"]
            released = False
        elif event["event"] == "released":
            released = True
    if lease is None:
        raise ValueError("resident set has no published lease journal")
    if released or now >= lease["hard_max"]:
        return False
    return "campaign" in lease or now < lease["until"] or _resident_live_rows(store, set_id)


def _pins_root(store, set_id, host):
    return store.queue_root / "resident-pins" / resident_sets._resident_name(host, "host") / resident_sets._set_id(set_id)


def pin(store, set_id, host, spec, context, *, now=None):
    """Explicit readers hold a pin until their last open/read is finished.

    Container injection and containment cleanup integration are Phase 2. A
    crashed reader's pin stays until the existing scope attestation proves stop.
    """
    for field in ("action_key", "nonce", "scope_id"):
        if not isinstance(context.get(field), str) or not context[field]:
            raise ValueError(f"resident reader context requires {field}")
    root, final, _, _ = _paths(set_id, spec)
    with local_tier.host_lock(root):
        if not lease_active(store, set_id, now=now):
            raise ValueError("resident lease expired")
        copy_record = store.read_copy(set_id, host)
        if copy_record["state"] != "resident" or not final.is_dir() or final.is_symlink():
            raise ValueError("resident copy unavailable")
        token = uuid.uuid4().hex
        resident_sets.write_record(_pins_root(store, set_id, host) / (token + ".json"), {
            "schema": "prismabuild.resident_pin.v1", "set_id": set_id, "host": host,
            "local_root": str(final), "token": token,
            "action_key": context["action_key"], "nonce": context["nonce"], "scope_id": context["scope_id"]})
    return token


def release_pin(store, set_id, host, spec, token):
    if not isinstance(token, str) or len(token) != 32 or any(c not in "0123456789abcdef" for c in token):
        raise ValueError("invalid resident pin token")
    with local_tier.host_lock(spec["root"]):
        path = _pins_root(store, set_id, host) / (token + ".json")
        path.unlink(missing_ok=True)
        if path.parent.exists():
            resident_sets.fsync_directory(path.parent)


def _pinned(store, set_id, host, final):
    directory = _pins_root(store, set_id, host)
    if directory.exists() and any(directory.iterdir()):
        return True
    # Also respect existing reader-lease pins naming local files. A tainted
    # census cannot grant deletion, even if the named paths are unknowable.
    wanted = {str(final / Path(entry["path"]).relative_to(store.read(set_id)["canonical_root"]))
              for entry in store.read(set_id)["manifest"]["entries"]}
    pins, tainted = reader_lease.live_for(pool.PoolQueue(store.queue_root), wanted)
    return bool(pins or tainted)


def reclaim_pins(store, set_id, host, spec):
    directory = _pins_root(store, set_id, host)
    if not directory.exists():
        return
    queue = pool.PoolQueue(store.queue_root)
    with local_tier.host_lock(spec["root"]):
        for path in directory.iterdir():
            try:
                row = json.loads(path.read_text())
                if (row.get("schema") != "prismabuild.resident_pin.v1" or row.get("set_id") != set_id
                        or row.get("host") != host):
                    continue
                stopped, _ = reader_lease.attestation_proves_empty(queue, row["action_key"], row["nonce"], row["scope_id"])
                if stopped:
                    path.unlink()
                    resident_sets.fsync_directory(directory)
            except (ValueError, KeyError, OSError):
                continue  # Unknown pins protect; clocks and absent PIDs do not release.


def evict_resident_copy(store, set_id, host, spec, *, now=None):
    root, final, partial, evicting = _paths(set_id, spec)
    with posix_lock.held(store.copy_path(set_id, host).with_suffix(".move.lock"), blocking=False) as acquired:
        if not acquired:
            return {"state": store.read_copy(set_id, host)["state"], "reason": "copy_in_progress"}
        with local_tier.host_lock(root):
            current = store.read_copy(set_id, host)
            if current["state"] != "evicting" and lease_active(store, set_id, now=now):
                return {"state": current["state"], "reason": "leased"}
            if _pinned(store, set_id, host, final):
                return {"state": current["state"], "reason": "pinned"}
            # This write is durable BEFORE the rename, under the same flock.
            store.write_copy(set_id, host, {**current, "state": "evicting"})
            if not evicting.exists():
                source = final if final.exists() else partial if partial.exists() else None
                if source is not None:
                    if source.is_symlink():
                        raise ValueError("unsafe eviction source")
                    os.rename(source, evicting)
                    resident_sets.fsync_directory(root)
        # No host lock during the slow delete. The record already refuses use.
        for tree in (evicting, partial):
            if tree.exists():
                if tree.is_symlink():
                    raise ValueError("unsafe evicting tree")
                shutil.rmtree(tree)
                resident_sets.fsync_directory(root)
        # Capacity is released only once ALL local bytes have been deleted.
        pool.PoolQueue(store.queue_root).tier_ledger(local_tier.local_resident_tier_id(host)).release(set_id)
        with local_tier.host_lock(root):
            store.write_copy(set_id, host, {**current, "state": "absent", "bytes": 0,
                "verification": [], "completed_unix": None, "local_root": None})
        return store.read_copy(set_id, host)


def request_eviction(store, set_id, host, spec, *, caller_host=None, now=None):
    """The #801 shape: only the owner deletes; other hosts queue its egress."""
    if host == (socket.gethostname() if caller_host is None else caller_host):
        return evict_resident_copy(store, set_id, host, spec, now=now)
    row = store.read_movements(set_id, host).get("evict")
    if row is None:
        raise ValueError("copy has no retained host-pinned egress action")
    queue = pool.PoolQueue(store.queue_root)
    try:
        queue.publish(**row, recompute=True, refuse_if_live=True)
    except pool.ActionAlreadyLiveError:
        pass
    return {"state": "queued", "action_key": row["action_key"], "host": host}


def _lease_error_path(store, set_id, host):
    return store.set_path(set_id).parent / "lease-errors" / (resident_sets._resident_name(host, "host") + ".json")


def _lease_failure(store, set_id, host, error, now):
    result = {"schema": "prismabuild.resident_lease_error.v1", "set_id": set_id,
              "host": host, "error": f"{type(error).__name__}: {error}",
              "unix": time.time() if now is None else now}
    try:
        resident_sets.write_record(_lease_error_path(store, set_id, host), result)
    except (ValueError, OSError) as record_error:
        result["record_error"] = f"{type(record_error).__name__}: {record_error}"
    return result


def _clear_lease_failure(store, set_id, host):
    path = _lease_error_path(store, set_id, host)
    try:
        path.unlink()
    except FileNotFoundError:
        return
    resident_sets.fsync_directory(path.parent)


def lease_pass(store, host, spec, *, now=None):
    """Attempt interrupted evictions first; isolate failures without freeing bytes."""
    results = []
    try:
        directories = sorted(store.root.iterdir())
    except FileNotFoundError:
        return results
    statuses = []
    for directory in directories:
        if not directory.is_dir():
            continue
        set_id = directory.name
        try:
            try:
                (directory / "body.json").stat()
            except FileNotFoundError:
                continue  # A refused publication can leave only its record lock.
            row = store.read(set_id)
            if host in row["hosts"]:
                row["copies"] = {host: store.read_copy(set_id, host)}
                statuses.append(row)
        except Exception as error:  # One corrupt set must not abort the host pass.
            results.append(_lease_failure(store, set_id, host, error, now))
    statuses.sort(key=lambda row: row["copies"][host]["state"] != "evicting")
    queue = pool.PoolQueue(store.queue_root)
    for row in statuses:
        set_id = row["set_id"]
        try:
            state = row["copies"][host]["state"]
            _, final, partial, evicting = _paths(set_id, spec)
            held = pool.held_names_visible(queue.tier_ledger(local_tier.local_resident_tier_id(host)), set_id)
            if (state == "absent" and not final.exists() and not partial.exists()
                    and not evicting.exists() and not held):
                _clear_lease_failure(store, set_id, host)
                continue
            reclaim_pins(store, set_id, host, spec)
            result = None
            if state == "evicting" or not lease_active(store, set_id, now=now):
                result = evict_resident_copy(store, set_id, host, spec, now=now)
            _clear_lease_failure(store, set_id, host)
            if result is not None:
                results.append(result)
        except Exception as error:
            results.append(_lease_failure(store, set_id, host, error, now))
    return results



def same_filesystem(source, destination_root):
    return os.stat(source).st_dev == os.stat(destination_root).st_dev


def require_adoption_unmounted(record, source):
    """Do not move manual bytes still owned by a global mount/old container."""
    def decode(value):
        for escaped, character in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
            value = value.replace(escaped, character)
        return value
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        if len(fields) >= 5 and decode(fields[4]) in {str(source), record["canonical_root"]}:
            raise ValueError("adoption requires removing global bind mount units before moving bytes")
    if Path(record["canonical_root"]).is_relative_to("/mnt/shared"):
        import subprocess
        def docker(*arguments):
            try:
                return subprocess.run(["docker", *arguments], capture_output=True, text=True)
            except FileNotFoundError as error:
                raise ValueError("adoption refused: docker was not found; cannot inspect running shared-mount containers") from error

        census = docker("ps", "--quiet", "--filter", "status=running")
        if census.returncode:
            raise ValueError("adoption cannot establish whether old shared-mount containers are running")
        ids = census.stdout.split()
        if ids:
            inspected = docker("inspect", *ids)
            if inspected.returncode:
                raise ValueError("adoption cannot inspect running containers")
            for container in json.loads(inspected.stdout):
                for mount in container.get("Mounts", []):
                    if mount.get("Source") == "/mnt/shared" and mount.get("Destination") == "/mnt/shared":
                        raise ValueError("adoption requires stopping containers that captured the shared recursive bind")


def adopt_resident_copy(store, set_id, host, spec, source, *, now=None):
    record = store.read(set_id)
    root, final, partial, evicting = _paths(set_id, spec)
    source = Path(source).absolute()
    if source == Path(record["canonical_root"]) or Path(record["canonical_root"]).is_relative_to(source):
        raise ValueError("adoption must not move the authoritative canonical directory")
    if root.is_relative_to(source) or source.is_relative_to(root):
        raise ValueError("adoption source must be outside the local tier root")
    with posix_lock.held(store.copy_path(set_id, host).with_suffix(".move.lock")):
        current = store.read_copy(set_id, host)
        if partial.exists() or evicting.exists() or current["state"] == "evicting":
            raise ValueError("adoption conflicts with an unfinished copy or eviction")
        if not lease_active(store, set_id, now=now):
            raise ValueError("resident lease expired")
        local_tier.reserve(pool.PoolQueue(store.queue_root), set_id, [host], record["manifest"]["total_bytes"])
        if final.exists():
            if source.exists():
                raise ValueError("adoption destination already exists")
            verification = _verify_tree(final, record)
        else:
            if not same_filesystem(source, root):
                raise ValueError("adoption requires the same filesystem")
            require_adoption_unmounted(record, source)
            verification = _verify_tree(source, record)
            for entry in record["manifest"]["entries"]:
                file = source / Path(entry["path"]).relative_to(record["canonical_root"])
                with file.open("rb") as stream:
                    os.fsync(stream.fileno())
            for directory, _, _ in os.walk(source, topdown=False):
                resident_sets.fsync_directory(directory)
            with local_tier.host_lock(root):
                store.write_copy(set_id, host, {**current, "state": "copying", "adoption_source": str(source), "local_root": str(final)})
                os.rename(source, final)
                resident_sets.fsync_directory(source.parent)
                resident_sets.fsync_directory(root)
        with local_tier.host_lock(root):
            store.write_copy(set_id, host, {**current, "state": "resident", "adoption_source": str(source),
                "local_root": str(final), "verification": verification,
                "bytes": record["manifest"]["total_bytes"], "completed_unix": time.time()})
        return store.read_copy(set_id, host)


def movement_row(action, cas, tier):
    return {"action_key": action["action_key"], "cas_root": str(cas.root),
        "worker_script": str(Path(tier["mover_tools_root"]) / "prismabuild_worker.py"),
        "checkout_snapshot": action["params"].get("checkout_snapshot"), "tags": [tier["host"]],
        "resources": action["params"]["demand"], "max_attempts": 3, "retry_safe": True,
        "container_owner": action["environment"]["variables"][pool.CONTAINER_OWNER_ENV],
        "interpreter": tier["mover_python"]}

