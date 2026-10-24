"""Publication refuses ambiguous JSON before a surviving old supervisor sees it."""
from __future__ import annotations

import pytest

from test_publication_refuses_a_dropped_roster_option_1664 import (
    _checkout, _publish, publish_runtime,
)
from test_roster_presence_status import _generation_with_roster, _offer, fleet


@pytest.mark.parametrize("raw", [
    b'{"boxes":{"one":{}},"boxes":{"two":{}}}',
    b'{"boxes":{"one":{"loops":1,"loops":2}}}',
    b'{"boxes":{"one":{}},"unknown":NaN}',
    b'{"boxes":{"one":{}},"unknown":Infinity}',
    b'{"boxes":{"one":{}},"unknown":1e999}',
    b'{"boxes":{"one":{}},"d38_enforce":1}',
    b'{"boxes":{"one":{}},"gpu_interpreters":"/bad/python"}',
    b'{"boxes":{"one":{}},"pb_cpu_live_roots":"/live"}',
    b'{"boxes":{"one":{}},"pb_cpu_live_roots":["relative/live"]}',
    b'{"boxes":{"one":{}},"pb_cpu_live_roots":null}',
    b'{"boxes":{"one":{}},"local_disk_free_floor_percent":true}',
    b'{"boxes":{"one":{"local_disk_free_floor_percent":100}}}',
    b'{"boxes":{"one":{}},"local_disk_free_floor_percent":null}',
    b'{"boxes":{"one":{"local_disk_free_floor_percent":null}}}',
    b'{"boxes":{"one":{"args":null}}}',
    b'{"boxes":{"one":{"roles":null}}}',
])
def test_publication_refuses_invalid_roster_before_staging(tmp_path, monkeypatch, raw):
    checkout = _checkout(tmp_path / "checkout", {})
    (checkout / "tools/fleet/fleet_boxes.json").write_bytes(raw)
    mirror = _publish(tmp_path, monkeypatch, checkout, [])
    with pytest.raises(SystemExit, match="roster"):
        publish_runtime.main()
    assert not mirror.exists()
    assert not (mirror.parent / "runtime-generations").exists()


def test_barrier_membership_is_bound_to_qualified_exact_bytes(fleet):
    source = _generation_with_roster(fleet, "source", {"one": {}})
    target = _generation_with_roster(fleet, "target", {"one": {}})
    _offer(fleet, "one")
    path = source[0] / "tools/fleet/fleet_boxes.json"
    path.write_bytes(b'{"boxes":{"one":{"_alias":"unattested"}}}')
    with pytest.raises(SystemExit, match="manifest|digest|bytes"):
        publish_runtime._barrier_roster([source, target])
