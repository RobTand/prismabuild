"""#1248: an admitter-readable pool-read pressure signal on the tier records.

The signal is additive and observable only: no admission term reads it yet.
No fleet queue, live mount, container, or other process is touched.
"""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import storage_tiers as st  # noqa: E402

from test_storage_tiers import _runner, _sysfs, STATUS_WITH_AUX, STAGE_STATUS  # noqa: E402


PSI = "some avg10=2.50 avg60=74.12 avg300=1.02\nfull avg10=1.00 avg60=41.30 avg300=0.40\n"


def _discover(tmp_path: Path, *, proc_pressure: str | None, now: float = 1.0):
    listing = "prismabuild-stage\t800166076416\t1048576\t800165027840\tONLINE\n"
    run = _runner(
        pools=listing,
        statuses={"storage_pool": STATUS_WITH_AUX, "prismabuild-stage": STAGE_STATUS},
        mountpoints={"prismabuild-stage": "/storage_pool/prismabuild-stage"},
    )
    sysfs = _sysfs(tmp_path, {"sdb": ["sdb1"], "sdc": ["sdc1"]})
    arcstats = tmp_path / "arcstats"
    arcstats.write_text(
        "c_max 4 257698037760\narc_meta_used 4 5423149896\nsize 4 1\nc 4 2\n")
    kwargs = {} if proc_pressure is None else {"proc_pressure": proc_pressure}
    return st.discover_tiers(
        host="dl380g10", runner=run, arcstats_path=str(arcstats), sysfs=sysfs,
        by_id=str(tmp_path), source_pool="storage_pool", now=now, **kwargs)


def test_tier_records_carry_the_pool_read_pressure_signal(tmp_path: Path) -> None:
    psi = tmp_path / "pressure.io"
    psi.write_text(PSI)
    tiers = _discover(tmp_path, proc_pressure=str(psi))
    expected = {
        "io_psi_some": {"avg10": 2.5, "avg60": 74.12, "avg300": 1.02},
        "window_s": st.PRESSURE_WINDOW_S,
        "source": "proc_pressure_io",
    }
    assert tiers["prismabuild-stage:dl380g10"]["pool_read_pressure"] == expected
    assert tiers["arc:dl380g10"]["pool_read_pressure"] == expected


def test_an_unreadable_signal_is_never_a_quiet_pool(tmp_path: Path) -> None:
    tiers = _discover(tmp_path, proc_pressure=str(tmp_path / "absent"))
    block = tiers["prismabuild-stage:dl380g10"]["pool_read_pressure"]
    assert block["io_psi_some"] is None
    assert block["window_s"] == st.PRESSURE_WINDOW_S
    assert block["unavailable_reason"]


def test_pressure_fold_takes_the_newest_record_not_the_worst() -> None:
    old = {"sampled_unix": 1.0, "pool_read_pressure": {
        "io_psi_some": {"avg60": 95.0}, "window_s": 60}}
    new = {"sampled_unix": 2.0, "pool_read_pressure": {
        "io_psi_some": {"avg60": 3.0}, "window_s": 60}}
    absent = {"sampled_unix": 3.0}
    unreadable = {"sampled_unix": 4.0, "pool_read_pressure": {
        "io_psi_some": None, "unavailable_reason": "gone"}}
    assert st.pool_read_pressure_from_records([old, new]) == 3.0
    assert st.pool_read_pressure_from_records([new, absent, unreadable]) is None
    assert st.pool_read_pressure_from_records([]) is None
