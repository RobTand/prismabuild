#!/usr/bin/env python3
"""Build ``r13_1053_dead_instance.json.gz`` from a read-only copy (#1053).

The input is a directory holding copies of the dead R13 Stage A instance's
records, taken 2026-09-23 by named path only (never a listing of the live
queue or the shared mount):

* ``<template_id>.<nonce>/``: the scope's ``instance.json``,
  ``commitments.json`` and ``prewrites/*.prewrite.json``;
* ``extra/template.json``: the template;
* ``extra/batches/<batch_id>.json``: the immutable batch records of the ten
  batches that were never retired;
* ``extra/funding/<mover>.json``: those ten movers' retired funding records;
* ``extra/queue/{failed,done}-<key>.json``: the producer's and the movers'
  terminal records.

The bundle keeps every record's bytes except the queue records, which are cut
down to the fields a terminal state is read from (``action_key``, ``status``,
``published_unix`` and the attempt nonce).  Paths keep the live prefix; the
replay (`tests/r13_1053_replay.py`) moves them under ``tmp_path`` and
recomputes every digest that covers them.

Usage::

    python3 tests/fixtures/build_r13_1053_fixture.py <snapshot dir>
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path
import sys

OUT = Path(__file__).resolve().parent / "r13_1053_dead_instance.json.gz"


def _load(path: Path) -> object:
    return json.loads(path.read_text())


def _terminal(record: dict) -> dict:
    control = record.get("resource_scope") or {}
    return {"schema": record.get("schema"), "action_key": record["action_key"],
            "status": record["status"],
            "published_unix": record["published_unix"],
            "resource_scope": {"nonce": control.get("nonce")}}


def main(argv: list[str]) -> int:
    snapshot = Path(argv[1])
    scopes = [child for child in snapshot.iterdir()
              if child.is_dir() and child.name != "extra"]
    assert len(scopes) == 1, scopes
    scope = scopes[0]
    extra = snapshot / "extra"
    bundle = {
        "scope_name": scope.name,
        "template": _load(extra / "template.json"),
        "instance": _load(scope / "instance.json"),
        "commitments": _load(scope / "commitments.json"),
        "prewrites": {path.name: _load(path) for path in
                      sorted((scope / "prewrites").glob("*.prewrite.json"))},
        "batch_records": {path.stem: _load(path) for path in
                          sorted((extra / "batches").glob("*.json"))},
        "funding": {path.stem: _load(path) for path in
                    sorted((extra / "funding").glob("*.json"))},
        "queue": {},
    }
    for path in sorted((extra / "queue").glob("*.json")):
        state, _, key = path.stem.partition("-")
        bundle["queue"].setdefault(state, {})[key] = _terminal(_load(path))
    raw = json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode()
    OUT.write_bytes(gzip.compress(raw, compresslevel=9, mtime=0))
    print(f"{OUT}: {OUT.stat().st_size} bytes from {len(raw)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
