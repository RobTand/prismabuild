"""Verified whole-tree copies and their ordinary host-pinned movement actions."""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import stat
import time

from . import local_tier, movement_actions, pool, reader_lease, resident_sets, residency_map

BLOCK_BYTES = 8 * 1024 * 1024


def hash_file(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError(f"not a regular file: {path}")
        digest = hashlib.sha256()
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
            digest = hashlib.sha256()
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


def copy(store, set_id, host, spec, *, reader_context=None):
    record = store.read(set_id)
    root, final, partial, evicting = _paths(set_id, spec)
    queue = pool.PoolQueue(store.queue_root)
    local_tier.reserve(queue, set_id, [host], record["manifest"]["total_bytes"])
    # A mover lock serializes retries without holding the host transition lock
    # across source reads. Eviction checks this lock nonblocking (slice 4).
    from . import posix_lock
    with posix_lock.held(store.copy_path(set_id, host).with_suffix(".move.lock")):
        current = store.read_copy(set_id, host)
        if evicting.exists() or current["state"] == "evicting":
            raise ValueError("resident copy is being evicted")
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


def movement_action(template, store, set_id, tier, *, policy_path, operation):
    host = tier["host"]
    python, tool, _ = movement_actions.movement_tools(tier, mover="local_resident.py")
    command = [python, tool, "--pool-root", str(store.queue_root), "--set-id", set_id,
               "--host", host, "--policy", str(policy_path), "--operation", operation]
    return movement_actions.seal_movement_action(template, command=command,
        demand={"cpu": 1, "mem_gb": 1}, tags=[host], log_name=f"resident-{operation}-{set_id}-{host}.log")


def publish_actions(template, store, set_id, tiers, *, policy_path):
    """Publish copies now and retain host-pinned egress rows for lease expiry."""
    record = store.read(set_id)
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
            pair[operation] = {"action_key": action["action_key"], "cas_root": str(cas.root),
                "worker_script": str(Path(tier["mover_tools_root"]) / "prismabuild_worker.py"),
                "checkout_snapshot": action["params"].get("checkout_snapshot"), "tags": [host],
                "resources": {"cpu": 1, "mem_gb": 1}, "max_attempts": 3, "retry_safe": True,
                "container_owner": action["environment"]["variables"][pool.CONTAINER_OWNER_ENV],
                "interpreter": tier["mover_python"]}
        with store.lock(set_id):
            current = store.read_copy(set_id, host)
            store.write_copy(set_id, host, {**current, "movement_rows": pair})
        queue.publish(**pair["copy"], recompute=True, refuse_if_live=True)
        rows[host] = pair
    return rows
