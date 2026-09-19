"""A delivery may raise ``best`` only if its reader reserved fill.

A record with no sealed fill demand -- a prewarm cycle's incidental pool
reads, or a mover priced before fill tokens existed -- measured the pool's
idle moments, not its capability.  On 2026-09-19 a prewarm-shaped record
delivering 1.4 MB/s buried a fast mover's 415 MB/s delivery, the stage tier
minted 1 MB/s of fill, no mover could reserve, and the campaign starved its
GPU between bursts (#654 act three).
"""

from prismabuild.storage_tiers import fill_supply_from_records


def _mover(unix: float, delivered: float, sealed: float,
           achieved: float, schema: str | None = "prismabuild.mover_receipt.v1") -> dict:
    return {
        "unix": unix,
        "schema": schema,
        "action_key": f"mover-{unix}",
        "fill_demand_mb_s_pool_side": sealed,
        "bytes_staged": int(achieved * 40 * 2**20),
        "seconds": 40.0,
        "disk_pacing": {
            "mean_pool_read_mb_s": delivered,
            "pool_read_bytes": int(achieved * 40 * 2**20),
        },
    }


def _prewarm(unix: float, delivered: float) -> dict:
    return {
        "unix": unix,
        "schema": "prismabuild.prewarm_record.v1",
        "disk_pacing": {
            "mean_pool_read_mb_s": delivered,
            "pool_read_bytes": int(80 * 2**30),
        },
    }


def test_a_delivery_without_reserved_fill_raises_no_best():
    history = [_mover(1000.0, 415.0, 300.0, 305.0), _prewarm(2000.0, 1.4)]
    supply = fill_supply_from_records(history)
    assert supply["best_mb_s"] == 415.0
    assert supply["ceiling_mb_s"] is None
    assert supply["may_grow"] is True


def test_the_live_collapse_shape_a_sealless_delivery_rebuilds_best_after_a_shortfall():
    # The 2026-09-19 live history's shape: an honest shortfall sets a
    # ceiling; a fast mover refutes it (best reset, then rebuilt at 415);
    # a LATER honest shortfall re-sets a ceiling and resets best; the only
    # non-shortfall delivery after that is a seal-less prewarm record at
    # 1.4 MB/s.  Without the gate, best rebuilds at 1.4 and the tier
    # starves; with it, the seal-less record cannot speak and the supply
    # falls to the last honest ceiling instead.
    history = [
        _mover(1000.0, 311.0, 400.0, 300.0),   # fell short -> ceiling 311
        _mover(1500.0, 415.0, 300.0, 305.0),   # refutes; best rebuilt at 415
        _mover(2000.0, 200.0, 300.0, 250.0),   # fell short -> ceiling 200, best None
        _prewarm(2500.0, 1.4),                 # not a reader; must not rebuild best
    ]
    supply = fill_supply_from_records(history)
    assert supply["ceiling_mb_s"] == 200.0
    assert supply["best_mb_s"] is None
    assert supply["may_grow"] is False


def test_mover_only_history_is_unchanged():
    history = [_mover(1000.0, 300.0, 280.0, 285.0), _mover(1500.0, 320.0, 300.0, 310.0)]
    supply = fill_supply_from_records(history)
    assert supply["best_mb_s"] == 320.0
    assert supply["ceiling_mb_s"] is None
