"""Large valid chart responses must not erase a long action's CPU window."""
import io
import json
import math
from urllib.parse import parse_qs, urlsplit

import pytest

from prismabuild import box_window


@pytest.mark.parametrize("seconds", [30, 86400])
def test_netdata_window_survives_response_size(seconds, monkeypatch, tmp_path):
    requests = []

    def respond(url, timeout):
        query = parse_qs(urlsplit(url).query)
        requests.append(query)
        if query["chart"] != ["system.cpu"]:
            payload = {"labels": ["time"], "data": []}
            interval = 1
        else:
            # One-second, multi-dimensional samples exceed the reader's 4 MiB
            # cap for a day. Netdata groups rows only when points is supplied.
            requested = int(query.get("points", [seconds])[0])
            interval = max(1, seconds // requested)
            points = math.ceil(seconds / interval)
            payload = {"labels": ["time", "user", "system", "nice", "irq",
                                  "softirq", "steal", "iowait", "guest"],
                       "data": [[1700000000 + i, 10.125, 5.25, 0.0, 0.0,
                                 0.0, 0.0, 0.0, 0.0] for i in range(points)]}
        if query.get("options") == ["jsonwrap"]:
            payload = {"result": payload, "update_every": 1,
                       "view_update_every": interval}
        response = io.BytesIO(json.dumps(payload).encode())
        response.status = 200
        return response

    monkeypatch.setattr(box_window.urllib.request, "urlopen", respond)
    window = box_window.read_window(
        1700000000, 1700000000 + seconds, host="test",
        csv_dir=tmp_path, deadline_s=10)
    assert window["source"] == "netdata", window
    assert window["cpu"]["busy_percent_mean"] == pytest.approx(15.375)
    assert requests[0]["points"] == ["4096"]
    assert window["cpu"]["samples"] == (30 if seconds == 30 else 4115)
    assert window["cpu"]["update_every_s"] == (1 if seconds == 30 else 21)


def test_netdata_window_labels_aggregation_resolution(monkeypatch, tmp_path):
    def respond(url, timeout):
        query = parse_qs(urlsplit(url).query)
        chart = query["chart"][0]
        label = {"system.cpu": "user", "system.cpu_some_pressure": "some 10",
                 "system.cpu_full_pressure": "full 10"}[chart]
        assert query.get("group") == ["average"]
        payload = {"labels": ["time", label], "data": [[1, 2.0], [2, 4.0]]}
        if query.get("options") == ["jsonwrap"]:
            payload = {"result": payload, "update_every": 1,
                       "view_update_every": 60 if chart == "system.cpu" else 120}
        response = io.BytesIO(json.dumps(payload).encode())
        response.status = 200
        return response

    monkeypatch.setattr(box_window.urllib.request, "urlopen", respond)
    window = box_window.read_window(1, 86401, host="test", csv_dir=tmp_path)
    assert window["source"] == "netdata", window
    cpu = window["cpu"]
    assert cpu["time_group"] == "average"
    assert cpu["update_every_s"] == 60
    assert cpu["psi_some_avg10_update_every_s"] == 120
    assert cpu["psi_full_avg10_update_every_s"] == 120
    assert cpu["busy_percent_peak"] == 4.0
