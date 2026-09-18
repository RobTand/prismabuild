#!/usr/bin/env python3
"""Take a staged range off the tier and give its tokens back (#583).

The egress node.  It is the only thing that may return a mover's tier tokens,
because it is the only thing that removes the bytes they stand for, and those
two have to be one operation: **held tier tokens equal bytes on the stage, at
every instant**.  Releasing at ``finish`` instead would bound concurrent copies
rather than resident bytes --- twenty-one movers of 34.4 GB, run one after
another, leave 722 GB on a 721 GB stage while the ledger reads its full supply
free at every step --- so a mover keeps its tokens from ``finish`` until an
egress deletes its files.

There is no retained-but-unpinned state.  Bytes the ledger cannot see are the
overfill the reservation exists to prevent, arriving by another road, so
"retain the read-order prefix for a later artifact" is deferred (#598) rather
than approximated.  Evicting is deleting.

**Delete, then release, then drop the fragment.**  Each order is wrong in one
direction and this one is wrong in none that matters: a crash after the deletes
and before the release leaves tokens held for bytes that are gone, which costs
capacity until the orphan sweep or a rerun returns them, and a crash after the
release and before the deletes would leave bytes on a stage the ledger thinks
is empty --- which is the failure this whole node exists to prevent.  Releasing
last is the direction that fails safe.

It runs two ways.  As an **action row** it is published by the tier loop once a
consumer's accepted phase has passed the range, and its receipt is what makes
the eviction visible.  As a **sweep** the tier loop calls :func:`evict` directly
for a mover that no ready or claimed item still names --- a consumer that was
withdrawn leaves its movers holding the stage forever otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402


def _prune_empty(directory: Path, stop: Path) -> None:
    """Remove the directories a deleted range leaves behind, never past the stage."""

    stop = stop.resolve()
    while directory != stop:
        try:
            if directory.resolve() == stop or stop not in directory.resolve().parents:
                return
            directory.rmdir()
        except OSError:
            return
        directory = directory.parent


def evict(queue: pool.PoolQueue, mover_action_key: str, *,
          consumer_action_key: str, stage_root: str,
          residency_root: str | Path | None = None,
          reason: str = "egress") -> dict[str, object]:
    """Delete one mover's staged files and return its tier tokens.

    Idempotent in both halves: a file already gone is counted as gone rather
    than raised on, and ``ResourceLedger.release`` is documented safe to call
    twice.  A second egress of the same range is therefore a no-op receipt, not
    a failure --- which matters, because the tier loop may publish one while a
    sweep is doing the same work.
    """

    root = Path(residency_root if residency_root is not None
                else queue.root / pool.RESIDENCY)
    fragment_path = residency_map.fragment_path(
        root, consumer_action_key, mover_action_key)
    entries: dict[str, object] = {}
    errors: list[str] = []
    try:
        with open(fragment_path) as stream:
            entries = dict(residency_map.validate_fragment(json.load(stream))["entries"])
    except FileNotFoundError:
        # No fragment at all.  Either the mover never published one -- in which
        # case it staged nothing -- or an earlier egress already removed it.
        # Both mean nothing of this mover's is on the stage, so the tokens come
        # back; holding them would cost the tier its capacity for good.
        entries = {}
    except (OSError, ValueError) as exc:
        # A fragment that exists and cannot be read is the opposite case: its
        # bytes may well still be there and this egress cannot name them.
        # Releasing on that would let the ledger admit a mover onto capacity
        # that is occupied, so the tokens stay and the next sweep retries.
        errors.append(f"{fragment_path.name}: {exc}")
    stage = Path(stage_root)
    deleted = missing = 0
    bytes_deleted = 0
    for key, entry in entries.items():
        path = Path(str(entry["stage_path"]))
        try:
            if stage.resolve() not in path.resolve().parents:
                # A fragment naming a path outside the stage is not a thing to
                # act on: the writer validated it, so this is corruption or
                # someone else's file.
                errors.append(f"{key}: outside {stage}")
                continue
        except OSError as exc:
            errors.append(f"{key}: {exc}")
            continue
        try:
            os.unlink(path)
        except FileNotFoundError:
            missing += 1
            continue
        except OSError as exc:
            errors.append(f"{key}: {exc}")
            continue
        deleted += 1
        bytes_deleted += int(entry["bytes"])
        _prune_empty(path.parent, stage)

    released = 0 if errors else queue.release_tier_reservations(mover_action_key)
    if not errors:
        fragment_path.unlink(missing_ok=True)
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "action_key": mover_action_key,
        "consumer_action_key": consumer_action_key,
        "stage_root": str(stage),
        "reason": reason,
        "entries_deleted": deleted,
        "entries_already_gone": missing,
        "bytes_deleted": bytes_deleted,
        "tokens_released": released,
        # Errors mean the stage still holds bytes, so the tokens stay held:
        # releasing them would let the ledger admit a mover onto capacity that
        # is not there.  The receipt says so and the next sweep retries.
        "complete": not errors,
        "errors": errors,
        "host": socket.gethostname(),
        "unix": time.time(),
    }


def sweep(queue: pool.PoolQueue, *, stage_roots: dict[str, str],
          residency_root: str | Path | None = None) -> list[dict[str, object]]:
    """Evict every pinned mover no live item still names as a lead.

    A consumer withdrawn between its movers finishing and its own claim would
    otherwise hold the stage for the life of the fleet: nothing publishes its
    egress, because nothing is waiting for its bytes.  The test is deliberately
    the queue's own live state --- ready or claimed --- rather than a policy: a
    lead some item may still be admitted on is not an orphan, however old.

    **A live consumer protects its whole plan, not just its leads.**  The
    consumer depends on its first phase only, so a pinned mover three phases
    ahead of it is named by nothing in the queue: testing ``leads`` alone would
    make this sweep delete the window it exists to protect, on the cycle after
    it was staged.  The frozen plan is what says a mover is still wanted.
    """

    wanted: set[str] = set()
    owners: dict[str, str] = {}
    for state in (pool.READY, pool.CLAIMED):
        for path in pool._scan(queue.dir(state)):
            item = pool._read_json(path)
            residency = item.get("residency") if isinstance(item, dict) else None
            if not isinstance(residency, dict):
                continue
            for lead in residency.get("leads") or []:
                wanted.add(str(lead))
            key = path.name[:-len(".json")] if path.name.endswith(".json") else path.name
            owners[key] = key
            plan = residency_plan.read(queue, key)
            if plan is not None:
                wanted.update(residency_plan.mover_keys(plan))
    swept: list[dict[str, object]] = []
    for tier_id, stage_root in stage_roots.items():
        try:
            held = queue.tier_ledger(tier_id).held_keys()
        except (OSError, pool.PoolContractError):
            continue
        for key in held:
            if key in wanted or key in owners:
                continue
            receipt = queue.move_record(key)
            consumer = (str(receipt.get("consumer_action_key")) if isinstance(receipt, dict)
                        else "")
            if not consumer:
                continue
            swept.append(evict(queue, key, consumer_action_key=consumer,
                               stage_root=stage_root,
                               residency_root=residency_root, reason="orphan-sweep"))
    return swept


def own_action_key(declared: str | None) -> str:
    """This node's own action key, from the flag or from the launcher.

    A movement node files its receipt and holds its tier tokens under its own
    key, and that key is ``canonical_sha256`` of the action body --- so a
    ``--action-key`` sealed into the argv would be hashed into the very value
    it states, and no fixed point exists.  The launcher sets
    :data:`prismabuild.core.ACTION_KEY_ENV` for every action it starts, from
    the action in hand, which is the one place the answer is already known.
    The flag stays, because a direct run and every test needs to say which key
    it is acting as.
    """

    key = declared or os.environ.get(pb.ACTION_KEY_ENV) or ""
    if len(key) != 64 or any(character not in "0123456789abcdef" for character in key):
        raise SystemExit(
            f"pass --action-key, or run under a launcher that sets "
            f"{pb.ACTION_KEY_ENV}; got {key!r}")
    return key


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="delete one mover's staged files and return its tier tokens")
    parser.add_argument("--pool-root", required=True,
                        help="the pull queue root this egress files its receipt under")
    parser.add_argument("--action-key", default=None,
                        help="this egress node's own action key; defaults to "
                             f"{pb.ACTION_KEY_ENV}, which the launcher sets")
    parser.add_argument("--mover-action-key", required=True,
                        help="the movement node whose staged bytes are being taken back")
    parser.add_argument("--consumer-action-key", required=True,
                        help="the action those bytes were staged for")
    parser.add_argument("--stage-root", required=True,
                        help="the staging dataset's mountpoint; nothing outside it "
                             "is ever deleted")
    parser.add_argument("--residency-root", default=None,
                        help="where residency-map fragments are filed "
                             "(default <pool-root>/residency)")
    parser.add_argument("--receipt", default=None,
                        help="also write the receipt here (it is always filed in "
                             "the queue's movers directory)")
    args = parser.parse_args(argv)
    args.action_key = own_action_key(args.action_key)

    queue = pool.PoolQueue(Path(args.pool_root))
    receipt = evict(queue, args.mover_action_key,
                    consumer_action_key=args.consumer_action_key,
                    stage_root=args.stage_root,
                    residency_root=args.residency_root)
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        with open(args.receipt, "w") as stream:
            json.dump(receipt, stream, indent=1, sort_keys=True)
            stream.write("\n")
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 0 if receipt["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
