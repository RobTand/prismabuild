"""The dead R13 Stage A instance, installed under a ``tmp_path`` queue (#1053).

`tests/fixtures/r13_1053_dead_instance.json.gz` holds read-only copies of the
records R13 (``03f50d8e390b``) left when it failed at chain-042 on
2026-09-23: its template, instance, 436 committed batches, 28 outstanding
prewrites, the ten unretired batches' immutable records and retired funding,
and the terminal records of the producer and those ten movers
(`tests/fixtures/build_r13_1053_fixture.py` says how it was built).

:func:`install` files them into a queue under test, with the output prefix
and the stage root moved under ``tmp_path``.  Every digest that covers a path
is recomputed: the template's, the instance's binding to it, each unretired
batch record's manifest digest, and every batch namespace derived from them.
Nothing here reads or writes the live queue, stage or origin.

The batch classes the replay asserts on:

* ``unretired``: the ten batches whose stage retirement never filed;
* ``colliding``: the eight of them the relaunch regenerates (``b43p*-g0`` and
  ``b44p*-g7``, chain-042's work in flight);
* ``read``: the 392 batches the relaunch reads from origin -- every batch
  whose origin was never reclaimed, except the colliding eight.  Two of them
  (``b1-g6``, ``b43-g7``) are unretired;
* ``reclaimed``: the 36 whose origin was already reclaimed, with no files.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import gzip
import hashlib
import json
import os
from pathlib import Path

from prismabuild import pool
import prismabuild.produced_output as po

FIXTURE = (Path(__file__).resolve().parent / "fixtures"
           / "r13_1053_dead_instance.json.gz")
#: Where the live records put the stage copies.
LIVE_STAGE = "/stage/prewarm"


@dataclass
class Replay:
    template: dict
    instance: dict
    scope: Path
    unretired: list[str]
    colliding: list[str]
    read: list[str]
    reclaimed: list[str]
    prewrites: list[str]
    #: batch id -> its origin paths.
    paths: dict[str, list[str]] = field(default_factory=dict)
    #: prewrite batch id -> its planned paths.
    prewrite_paths: dict[str, list[str]] = field(default_factory=dict)
    movers: dict[str, str] = field(default_factory=dict)

    @property
    def owner(self) -> str:
        return str(self.instance["owner_action_key"])

    def coordinates(self, batch_id: str) -> str:
        return (f"{self.owner}/{self.template['template_id']}."
                f"{self.instance['owner_attempt']['nonce']}/{batch_id}")

    def entry(self, queue_root: Path, batch_id: str) -> dict:
        return po._read_commitments(
            po._commitments_path(queue_root, self.instance))["batches"][batch_id]


def load_bundle() -> dict:
    return json.loads(gzip.decompress(FIXTURE.read_bytes()))


def _variant_of(raw: str, variant: int) -> str:
    """The bundle as another dead attempt of its own template (#1072).

    The live tier loop met fourteen dead Stage A instances at once, each with
    its own owner, attempt, template and movers.  The owner key, the attempt
    nonce, the template id and the unretired batches' mover keys are replaced
    by names derived from ``variant``; every digest that covers them is
    recomputed by :func:`install` as it already is for the prefix.
    """

    bundle = json.loads(raw)
    instance = bundle["instance"]
    template_id = str(bundle["template"]["template_id"])

    def derived(value: str, width: int) -> str:
        return hashlib.sha256(
            f"r13-1072-variant-{variant}:{value}".encode()).hexdigest()[:width]

    for key in [str(instance["owner_action_key"]), *sorted(bundle["queue"]["done"])]:
        raw = raw.replace(key, derived(key, 64))
    nonce = str(instance["owner_attempt"]["nonce"])
    raw = raw.replace(nonce, derived(nonce, 32))
    return raw.replace(f'"{template_id}', f'"{template_id}-v{variant}')


def install(queue: pool.PoolQueue, *, prefix: Path, stage: Path,
            origin_files: bool = True, variant: int = 0) -> Replay:
    """File the dead instance into ``queue``; create its origin files.

    ``origin_files`` creates one small file for every path of a batch whose
    origin was never reclaimed, and for every ``.pt`` an outstanding prewrite
    plans (none of the ``.tmp`` names), as the live prefix held them.

    ``variant`` above zero files the same records as another dead attempt,
    with its own owner, nonce, template and movers (:func:`_variant_of`), so
    one queue can hold several; give each its own ``prefix``.
    """

    raw = gzip.decompress(FIXTURE.read_bytes()).decode()
    if variant:
        raw = _variant_of(raw, variant)
    live_prefix = json.loads(raw)["template"]["output_prefix"]
    raw = raw.replace(live_prefix, str(prefix))
    raw = raw.replace(f'"{LIVE_STAGE}/', f'"{stage}/')
    bundle = json.loads(raw)

    template = po.validate_template(bundle["template"])
    po.declare_template(queue.root, template)
    tsha = po.template_sha256(template)
    instance = dict(bundle["instance"])
    instance["template_sha256"] = tsha
    instance = po.validate_instance(instance)
    scope = po.instance_dir(queue.root, instance)
    assert scope.name == bundle["scope_name"], scope
    (scope / "prewrites").mkdir(parents=True, exist_ok=True)
    # Filed as the live one was: by a producer that predates any index.
    (scope / "instance.json").write_text(json.dumps(
        instance, sort_keys=True, separators=(",", ":")) + "\n")
    os.chmod(scope / "instance.json", 0o444)

    commitments = bundle["commitments"]
    batches: dict[str, dict] = commitments["batches"]
    records: dict[str, dict] = bundle["batch_records"]
    batch_root = (Path(queue.root) / "residency" / po.OUTPUT_BATCHES_SUBDIR
                  / po.instance_namespace(instance))
    batch_root.mkdir(parents=True, exist_ok=True)
    for batch_id in sorted(batches):
        entry = batches[batch_id]
        manifest = str(entry["manifest_digest"])
        record = records.get(batch_id)
        if record is not None:
            record["template_sha256"] = tsha
            sealed = [po.validate_descriptor(item, template, instance)
                      for item in record["entries"]]
            manifest = po.output_manifest_sha256(sealed)
            record["manifest_digest"] = manifest
        old = str(entry["batch_namespace"])
        new = po.batch_namespace(instance, batch_id, manifest)
        entry = json.loads(json.dumps(entry).replace(old, new))
        entry["manifest_digest"] = manifest
        batches[batch_id] = entry
        if record is not None:
            record = json.loads(json.dumps(record).replace(old, new))
            record["batch_namespace"] = new
            (batch_root / f"{batch_id}.json").write_text(json.dumps(
                record, sort_keys=True, separators=(",", ":")) + "\n")
    po._write_commitments(scope / "commitments.json", commitments)

    for name, record in bundle["prewrites"].items():
        (scope / "prewrites" / name).write_text(json.dumps(
            record, sort_keys=True, separators=(",", ":")) + "\n")

    for mover, record in bundle["funding"].items():
        record["template_sha256"] = tsha
        checked = queue.validate_output_funding(record)
        path = queue.funding_output_retired_path(mover, str(checked["tier_id"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        pool._write_json_atomic(path, checked)
    for state, rows in bundle["queue"].items():
        for key, record in rows.items():
            pool._write_json_atomic(queue.item_path(state, key), record)

    unretired = sorted(batch_id for batch_id, entry in batches.items()
                       if not po._batch_stage_retired(entry))
    colliding = sorted(batch_id for batch_id in unretired
                       if "-cotangent-" in batch_id)
    reclaimed = sorted(batch_id for batch_id, entry in batches.items()
                       if entry.get("origin_reclaimed"))
    read = sorted(batch_id for batch_id in batches
                  if batch_id not in reclaimed and batch_id not in colliding)
    replay = Replay(
        template=template, instance=instance, scope=scope,
        unretired=unretired, colliding=colliding, read=read,
        reclaimed=reclaimed,
        prewrites=sorted(name[:-len(".prewrite.json")]
                         for name in bundle["prewrites"]),
        paths={batch_id: list(entry["paths"])
               for batch_id, entry in batches.items()},
        prewrite_paths={name[:-len(".prewrite.json")]: list(record["paths"])
                        for name, record in bundle["prewrites"].items()},
        movers={batch_id: str(po._active_materialization(batches[batch_id])
                              ["mover_key"]) for batch_id in unretired})
    if origin_files:
        wanted = {path for batch_id, paths in replay.paths.items()
                  if batch_id not in reclaimed for path in paths}
        wanted |= {path for paths in replay.prewrite_paths.values()
                   for path in paths if not path.endswith(".tmp")}
        for path in sorted(wanted):
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(os.path.basename(path).encode())
    return replay


def origin_identities(replay: Replay, batch_ids) -> dict[str, tuple[int, int]]:
    """``path -> (inode, size)`` for every origin path of these batches."""

    found: dict[str, tuple[int, int]] = {}
    for batch_id in batch_ids:
        for path in replay.paths[batch_id]:
            info = os.lstat(path)
            found[path] = (info.st_ino, info.st_size)
    return found
