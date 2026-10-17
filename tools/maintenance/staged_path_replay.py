"""Replay the real old and current staged-copy writers inside one PB action."""
from __future__ import annotations

import argparse
import importlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

STAGED_REPLAY_BASE = "a8ba9c0c7beeeb379908924a60c6933e997c8df6"


def staged_replay_capture(checkout: Path, data: Path):
    sys.path[:0] = [str(checkout / "src"), str(checkout / "tools/fleet"), str(checkout / "tests")]
    from prismabuild import core, pool, reader_lease, residency_map, storage_tiers
    from prismabuild import stage_move, ram_promote
    import tier_loop
    import test_a_kill_between_a_movers_two_documents_blocks_nothing as fixture

    owner = importlib.import_module("prismabuild." + (
        "staged_path" if (checkout / "src/prismabuild/staged_path.py").exists() else "stage_release"))
    adopt = getattr(owner, "adopt", None) or getattr(tier_loop, "adopt")
    data.mkdir()
    queue = pool.PoolQueue(data / "pb-queue")
    queue.records.ensure_layout()
    root = queue.records.residency_fragment_root()
    manifest, digest, body = fixture._manifest(data)
    stage_move.FRAGMENT_PUBLISH_S = 1e12
    queue.tier(fixture.STAGE_TIER).mint({"stage_gib": 4})
    assert queue.tier(fixture.STAGE_TIER).take(fixture.STAGE_MOVER, {"stage_gib": 1})
    events, wires, generations, epochs = [], [], {}, {}
    phase = ["stage-copy"]

    def normalized(value):
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if key == "file_id":
                    result[key] = {field: "$live-" + field for field in item}
                elif key == "generation":
                    result[key] = generations.setdefault(item, "$generation-" + str(len(generations)))
                elif key == "epoch":
                    result[key] = epochs.setdefault(item, "$epoch-" + str(len(epochs)))
                else:
                    result[key] = normalized(item)
            return result
        if isinstance(value, list):
            return [normalized(item) for item in value]
        return value.replace(str(data), "$data") if isinstance(value, str) else value

    def wire(path, kind):
        raw = path.read_bytes()
        document = json.loads(raw)
        assert raw == (core.digest_primitives.sorted_json(document) + "\n").encode()
        if kind == "material":
            for entry in document["entries"].values():
                assert entry["file_id"] == reader_lease.stat_identity(entry["stage_path"])
                assert entry["sha256"] == reader_lease.content_identity(entry["stage_path"])["sha256"]
        row = {"phase": phase[0], "op": kind, "path": str(path).replace(str(data), "$data"),
               "wire": core.digest_primitives.sorted_json(normalized(document)) + "\n"}
        events.append(row)
        wires.append({**row, "raw": raw.decode()})

    actual_fragment, actual_material = residency_map.write_fragment, reader_lease.write_material
    actual_unlink = Path.unlink

    def fragment(where, document):
        path = actual_fragment(where, document)
        wire(path, "fragment")
        return path

    def material(where, **kwargs):
        path = actual_material(where, **kwargs)
        wire(path, "material")
        return path

    def unlink(path, *args, **kwargs):
        kind = None
        if root in path.parents and path.exists():
            if path.name.endswith(".retiring.json"):
                kind = "unlink_retiring"
            elif path.suffix == ".json" and len(path.name) == 69:
                kind = "unlink_material" if root / "material" in path.parents else "unlink_fragment"
        result = actual_unlink(path, *args, **kwargs)
        if kind:
            events.append({"phase": phase[0], "op": kind,
                           "path": str(path).replace(str(data), "$data")})
        return result

    residency_map.write_fragment, reader_lease.write_material, Path.unlink = fragment, material, unlink
    try:
        stage_receipt = stage_move.move(fixture._stage_args(
            data, queue, manifest, digest, fixture.CONSUMER, fixture.STAGE_MOVER))
        assert stage_receipt["complete"] and stage_receipt["bytes_copied"] == fixture.TOTAL
        queue.record_move(fixture.STAGE_MOVER, stage_receipt)
        owner.register_stage_root(queue, tier_id=fixture.STAGE_TIER, stage_root=data / "stage")

        phase[0] = "ram-copy"
        ram = data / "ram"
        ram.mkdir()
        assert storage_tiers.ensure_ram_epoch(ram, host="testbox") is not None
        queue.ledger("testbox").ensure_capacity({"mem_gb": 4})
        queue.tier(fixture.RAM_TIER).mint({"ram_gib": 4})
        assert queue.tier(fixture.RAM_TIER).take(fixture.RAM_MOVER, {"ram_gib": 1})
        ram_receipt = ram_promote.promote(fixture._promote_args(
            data, queue, manifest, digest, fixture.RAM_MOVER))
        assert ram_receipt["complete"] and ram_receipt["bytes_copied"] == fixture.TOTAL
        queue.record_move(fixture.RAM_MOVER, ram_receipt)
        owner.register_stage_root(queue, tier_id=fixture.RAM_TIER, stage_root=ram)

        phase[0] = "adoption"
        before = {str(p): reader_lease.stat_identity(str(p)) for p in fixture._named(data / "stage", body)}
        adopted = adopt(queue, old_key=fixture.STAGE_MOVER, new_key=fixture.OTHER_STAGE_MOVER,
            consumer_action_key=fixture.OTHER_CONSUMER, tier_id=fixture.STAGE_TIER,
            phase="read", range_start_bytes=0, range_end_bytes=fixture.TOTAL, residency_root=root)
        assert adopted["adopted"] and adopted["tokens_moved"] == 1
        assert before == {str(p): reader_lease.stat_identity(str(p)) for p in fixture._named(data / "stage", body)}

        phase[0] = "shrink"
        path = residency_map.fragment_path(root, fixture.OTHER_CONSUMER, fixture.OTHER_STAGE_MOVER)
        observed = residency_map.validate_fragment(json.loads(path.read_bytes()))
        first = Path(next(iter(observed["entries"].values()))["stage_path"])
        replacement = first.with_name("replacement")
        replacement.write_bytes(first.read_bytes())
        os.replace(replacement, first)
        with queue.mover_transition_lock(fixture.OTHER_STAGE_MOVER):
            pruned = owner.prune_stale_mentions(
                queue, fixture.OTHER_STAGE_MOVER, consumer_action_key=fixture.OTHER_CONSUMER,
                stage=data / "stage", tier_id=fixture.STAGE_TIER, root=root, observed=observed)
        assert pruned["complete"] and pruned["entries_pruned"] == 1 and pruned["document_pair_complete"]
        assert queue.tier(fixture.STAGE_TIER).holdings(fixture.OTHER_STAGE_MOVER) == {"stage_gib": 1}

        phase[0] = "egress-stage"
        dated = reader_lease.read_material(root, fixture.OTHER_CONSUMER, fixture.OTHER_STAGE_MOVER)
        reader_lease.write_retiring(reader_lease.leases_root(queue, root),
            consumer_action_key=fixture.OTHER_CONSUMER, mover_action_key=fixture.OTHER_STAGE_MOVER,
            generation=dated["generation"])
        egress = owner.evict(queue, fixture.OTHER_STAGE_MOVER,
            consumer_action_key=fixture.OTHER_CONSUMER, stage_root=str(data / "stage"), residency_root=root)
        assert egress["complete"] and egress["entries_deleted"] == fixture.N - 1
        assert queue.tier(fixture.STAGE_TIER).holdings(fixture.OTHER_STAGE_MOVER) == {}
        again = owner.evict(queue, fixture.OTHER_STAGE_MOVER,
            consumer_action_key=fixture.OTHER_CONSUMER, stage_root=str(data / "stage"), residency_root=root)
        assert again["complete"] and again["entries_deleted"] == again["tokens_released"] == 0

        phase[0] = "egress-ram"
        egress_ram = owner.evict(queue, fixture.RAM_MOVER,
            consumer_action_key=fixture.CONSUMER, stage_root=str(ram), residency_root=root)
        assert egress_ram["complete"] and egress_ram["entries_deleted"] == fixture.N
        assert queue.tier(fixture.RAM_TIER).holdings(fixture.RAM_MOVER) == {}
        assert all(not p.exists() for p in fixture._named(data / "stage", body) + fixture._named(ram, body))
    finally:
        residency_map.write_fragment, reader_lease.write_material, Path.unlink = actual_fragment, actual_material, actual_unlink
    return {"events": events, "wire_documents": wires,
            "facts": {"stage_bytes": stage_receipt["bytes_copied"], "ram_bytes": ram_receipt["bytes_copied"],
                      "adopted_without_replacement": True, "shrunk": pruned["entries_pruned"],
                      "stage_deleted": egress["entries_deleted"], "ram_deleted": egress_ram["entries_deleted"],
                      "second_egress_released": again["tokens_released"]}}


def staged_replay_cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", nargs=2, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not os.environ.get("PRISMABUILD_ACTION_SCOPE"):
        raise SystemExit("Execute this runtime replay through published PrismaBuild pbrun.py")
    if args.capture:
        print(json.dumps(staged_replay_capture(*args.capture)))
        return
    checkout = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(checkout / "src"))
    from prismabuild import digest_primitives
    executable = Path(sys.executable).resolve()
    executable_bytes = executable.read_bytes()
    with tempfile.TemporaryDirectory(prefix="c09-replay-") as temporary:
        scratch = Path(temporary)
        original = scratch / "original"
        original.mkdir()
        raw = subprocess.check_output(["git", "archive", STAGED_REPLAY_BASE, "src", "tools/fleet",
            "tests/test_a_kill_between_a_movers_two_documents_blocks_nothing.py"], cwd=checkout)
        with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
            archive.extractall(original, filter="data")
        captures = []
        for source in (original, checkout):
            data = scratch / "data"
            if data.exists():
                shutil.rmtree(data)
            run = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--capture", str(source), str(data)],
                                 text=True, capture_output=True)
            if run.returncode:
                raise RuntimeError(run.stdout + run.stderr)
            captures.append(json.loads(run.stdout.splitlines()[-1]))
        old, current = captures
        old_order = [e["op"] for e in old["events"] if e["phase"] == "egress-stage"]
        new_order = [e["op"] for e in current["events"] if e["phase"] == "egress-stage"]
        assert old_order == ["unlink_fragment", "unlink_retiring", "unlink_material"]
        assert new_order == ["unlink_fragment", "unlink_material", "unlink_retiring"]
        def comparable(capture):
            events = capture["events"]
            return [e for e in events if e["phase"] != "egress-stage"] + sorted(
                (e for e in events if e["phase"] == "egress-stage"), key=lambda e: e["op"])
        assert comparable(old) == comparable(current), "wire or ordered effects changed outside the withdrawal repair"
        assert old["facts"] == current["facts"]
        result = {"base": STAGED_REPLAY_BASE, "wire_and_effects_equal": True,
                  "explicit_withdrawal_change": {"old": old_order, "current": new_order},
                  "old": old, "current": current}
        assert executable.read_bytes() == executable_bytes
        result["runtime"] = {
            "version": sys.version, "executable": sys.executable,
            "resolved_executable": str(executable), "bytes": len(executable_bytes),
            "sha256": digest_primitives.raw_sha256(executable_bytes),
            "affinity": sorted(os.sched_getaffinity(0)),
            "native_threads": {key: os.environ.get(key) for key in
                ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS")}}
        if args.output:
            args.output.write_text(json.dumps(result, indent=2) + "\n")
            args.output.with_suffix(".python-executable").write_bytes(executable_bytes)
        print(json.dumps({"wire_and_effects_equal": True, "wire_documents_per_arm": len(old["wire_documents"]),
                          "facts": old["facts"], "runtime": result["runtime"],
                          "report": ({"path": str(args.output), "bytes": args.output.stat().st_size,
                                      "sha256": digest_primitives.raw_sha256(args.output.read_bytes())}
                                     if args.output else None)}))


if __name__ == "__main__":
    staged_replay_cli()
