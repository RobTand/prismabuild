#!/usr/bin/env python3
"""Read-only: does a row that requires a bare image digest now place on each GPU box? Author pb-integrator, 2026-10-05.

usage: pb-confirm-image-placement.py [ACTION_KEY_PREFIX ...]   (default: fd9ca6b5, plus any ready/claimed row declaring sha256:5be13705...)

Uses the PUBLISHED runtime's own code (/mnt/shared/prismabuild-fleet/repo/src): the same placeable_hosts() that pbrun and the gang
election use, and container_images.missing() per offer. Writes nothing. Run it BEFORE publication to see the old answer (sparky only) and
AFTER, once the sparklina loop has restarted onto the new generation, to see the new one.
"""
import json
import sys

sys.path.insert(0, "/mnt/shared/prismabuild-fleet/repo/src")
from prismabuild import container_images as ci, pool  # noqa: E402

ROOT = "/mnt/shared/prismabuild-fleet/pb-queue"
BARE = "sha256:5be13705acaecc7b4aaf342a84f80d67844c9970ff8375bf9fbeecc9c98ce84a"
queue = pool.PoolQueue(ROOT)
version = json.load(open("/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json"))
print("published generation:", version.get("generation"), "commit", str(version.get("commit"))[:12])

prefixes = sys.argv[1:] or ["fd9ca6b5"]
rows = {}
for state in ("ready", "claimed"):
    for path in sorted((queue.root / state).glob("*.json")):
        try:
            item = json.load(open(path))
        except (OSError, ValueError):
            continue
        key = str(item.get("action_key") or path.stem)
        declared = item.get("container_images") or []
        if any(key.startswith(p) for p in prefixes) or BARE in declared:
            rows[key] = (state, item)
if not rows:
    print("no matching ready/claimed row")
# A row that already ended cannot be re-placed, so also evaluate the real record of the row that motivated this as a read-only template
# (placeable_hosts reads the item and the live offers; it publishes nothing).
for state in ("failed", "done"):
    for prefix in prefixes:
        for path in sorted((queue.root / state).glob(prefix + "*.json")):
            try:
                template = json.load(open(path))
                # Offers answer for an interpreter path only while a live item asks about it; this ended row's
                # interpreter is no longer asked about, so keeping it would empty every answer for a reason that has nothing
                # to do with the image. Drop it, so tags, GPU and the image requirement alone decide.
                template.pop("interpreter", None)
                rows[str(template.get("action_key"))] = (state + " (template, ended; interpreter requirement dropped)", template)
            except (OSError, ValueError):
                pass
offers = queue.offers()
print("offers:", ", ".join(str(o.get("host")) for o in offers))
for key, (state, item) in rows.items():
    declared = item.get("container_images") or []
    print(f"\nrow {key[:12]} state={state} claimed_host={item.get('claimed_host')} declares {[d[:19] + '...' for d in declared]}")
    try:
        hosts = queue.placeable_hosts(item)
    except Exception as exc:  # report, never guess
        hosts = f"placeable_hosts raised {exc!r}"
    print("  placeable_hosts (published code):", hosts)
    for offer in offers:
        if not offer.get("has_gpu"):
            continue
        absent = ci.missing(declared, [str(e) for e in offer.get("container_images") or []])
        print(f"  {str(offer.get('host')).ljust(10)} image requirement: {'SATISFIED' if not absent else 'ABSENT ' + str([a[:19] for a in absent])}")
