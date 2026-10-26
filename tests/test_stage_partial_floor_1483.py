"""Retained owner temporaries receive a floor verdict before truncation."""
import builtins
import json
from pathlib import Path

import pytest

from prismabuild import filesystem_floor as floor, pool, ram_promote, residency_map
from prismabuild import stage_move, storage_tiers
from test_a_mover_stages_the_range_it_declared_and_no_more import _args, _manifest, _write
from test_used_filesystem_callers_2049 import guard


@pytest.mark.parametrize("route", ["stage", "ram"])
@pytest.mark.parametrize("retry", [False, True], ids=["initial", "retry"])
@pytest.mark.parametrize("denied", [False, True], ids=["allowed", "denied"])
def test_retained_partial_is_checked_before_each_write(tmp_path, monkeypatch, route, retry, denied):
    monkeypatch.setenv(floor.MODE_ENV, "off")
    if route == "stage":
        source = tmp_path / "origin" / "payload"
        entry = _write(source, b"payload")
        args = _args(tmp_path, _manifest(source.parent, [entry]), start=0, end=7,
                     max_readers=1, readers=1)
        queue = pool.PoolQueue(args.pool_root)
        source_args = ["--stage-root", args.stage_root, "--unpaced"]
        run = stage_move.main
        payload = source.read_bytes()
    else:
        import test_a_promotion_handoff_defers_the_source_retirement as fixture

        queue, stage, source, manifest_sha, manifest = fixture._world(tmp_path)
        ram = tmp_path / "ram"
        ram.mkdir()
        storage_tiers.ensure_ram_epoch(ram, host="testbox")
        args = ram_promote.build_parser().parse_args([
            "--pool-root", str(queue.root), "--cas-root", str(tmp_path / "cas"),
            "--action-key", "3" * 64, "--consumer-action-key", fixture.CONSUMER_A,
            "--tier-id", fixture.RAM_TIER, "--ram-root", str(ram),
            "--source-stage-root", str(stage), "--manifest-sha256", manifest_sha,
            "--range-start-bytes", "0", "--range-end-bytes", str(fixture.SIZE),
            "--manifest", str(manifest), "--residency-root", str(queue.root / "residency"),
            "--readers", "1", "--max-readers", "1"])
        source_args = ["--ram-root", args.ram_root,
                       "--source-stage-root", args.source_stage_root]
        run = ram_promote.main
        payload = source.read_bytes()
    receipt_path = tmp_path / "movement-receipt.json"
    argv = [str(value) for value in [
        "--pool-root", args.pool_root, "--cas-root", args.cas_root,
        "--action-key", args.action_key, "--consumer-action-key", args.consumer_action_key,
        "--tier-id", args.tier_id, "--manifest-sha256", args.manifest_sha256,
        "--range-start-bytes", args.range_start_bytes, "--range-end-bytes", args.range_end_bytes,
        "--manifest", args.manifest, "--residency-root", args.residency_root,
        "--readers", "1", "--max-readers", "1", "--receipt", receipt_path, *source_args]]
    prior_fragments = residency_map.read_fragments(args.residency_root, args.consumer_action_key)
    guard(monkeypatch)
    original_temporary = stage_move._Copier._temporary
    observed = {"opens": 0}

    def retained_temporary(copier, destination):
        temporary = original_temporary(copier, destination)
        temporary.write_bytes(b"retained-before-retry")
        foreign = destination.with_name(f".{destination.name}.foreign-owner.partial")
        foreign.write_bytes(b"another owner's bytes")
        observed.update(temporary=temporary, destination=destination, foreign=foreign)
        guard(monkeypatch, temporary if denied and not retry else None)
        return temporary

    def open_temporary(path, mode="r", *positional, **keywords):
        if Path(path) == observed.get("temporary") and mode == "wb":
            observed["opens"] += 1
            if retry and observed["opens"] == 1:
                guard(monkeypatch, path if denied else None)
                raise FileNotFoundError("deterministic parent-prune retry")
        return builtins.open(path, mode, *positional, **keywords)

    monkeypatch.setattr(stage_move._Copier, "_temporary", retained_temporary)
    monkeypatch.setattr(stage_move, "open", open_temporary, raising=False)

    result = run(argv)

    assert observed["foreign"].read_bytes() == b"another owner's bytes"
    if denied:
        assert result == 75
        assert observed["opens"] == int(retry)
        assert observed["temporary"].read_bytes() == b"retained-before-retry"
        assert not observed["destination"].exists()
        assert queue.move_record(args.action_key) is None
        assert not receipt_path.exists()
        assert residency_map.read_fragments(args.residency_root, args.consumer_action_key) == prior_fragments
    else:
        assert result == 0
        assert observed["opens"] == 1 + int(retry)
        assert observed["destination"].read_bytes() == payload
        assert not observed["temporary"].exists()
        receipt = json.loads(receipt_path.read_text())
        assert receipt["complete"]
        assert queue.move_record(args.action_key)["complete"]
        fragments = residency_map.read_fragments(args.residency_root, args.consumer_action_key)
        assert any(fragment["tier_id"] == args.tier_id for fragment in fragments)
