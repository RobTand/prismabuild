"""#2085: portable, independently inspectable fleet qualification disposition.

This deliberately accepts explicit UNQUALIFIED populations with exact missing
prerequisites, as the issue requires. It does not manufacture idle-host evidence
from the action running pytest. A report pass is not fleet qualification.
"""
from __future__ import annotations

import json
from pathlib import Path
import statistics

from prismabuild.digest_primitives import raw_sha256
from prismabuild.fleet_roster import ACTIVE, Roster, box_status

ROOT = Path(__file__).resolve().parents[1]
REPORT = Path("docs/measurements/2085-cpu-ambient/acceptance.json")


def retained_input(record):
    relative = Path(record["path"])
    assert not relative.is_absolute() and ".." not in relative.parts, "portable retained input required"
    path = ROOT / relative
    assert path.is_file(), f"missing retained source: {relative}"
    raw = path.read_bytes()
    assert raw_sha256(raw) == record["sha256"], f"source digest mismatch: {relative}"
    return json.loads(raw)


def test_every_active_cpu_host_has_an_honest_acceptance_disposition():
    assert (ROOT / REPORT).is_file(), (
        "#2085 needs a retained per-host acceptance report; source tests and "
        "historical sparklina constants are not measured fleet qualification")
    report = json.loads((ROOT / REPORT).read_text())
    roster = Roster.at(ROOT)
    expected = {name for name, entry in roster.boxes.items()
                if box_status(name, entry) == ACTIVE and entry.get("loops", 0) > 0}
    assert report["schema"] == "prismabuild.cpu-ambient-acceptance.v1"
    assert report["roster_sha256"] == roster.sha256
    assert set(report["hosts"]) == expected, "derive population from configured active CPU workers"
    assert {"1317", "1322"} <= {str(i) for i in report["related_issues"]}
    assert report["deployed"] is False, "this work does not authorize deployment"
    assert report["interpretation"] == "empirical-envelope-not-gaussian-confidence-bound"
    for host, record in report["hosts"].items():
        assert record["status"] in {"qualified", "unqualified"}
        assert record["observed_sources"], f"{host}: inventory inspected sources and their limitations"
        if record["status"] == "unqualified":
            assert record["unresolved_prerequisites"], f"{host}: name exact missing observations/access"
            assert all(isinstance(p, str) and len(p.strip()) >= 30
                       for p in record["unresolved_prerequisites"])
            assert record.get("proposed_configuration") is None
            continue
        reference = retained_input(record["reference"])
        assert reference["qualification"]["approved"] is True
        assert record["cpu_ids"] == sorted(reference["topology"]["preferred"]
                                             + reference["topology"]["fallback"])
        assert reference["attribution_version"]
        assert record["host_identity_evidence"]
        assert record["holder_free_provenance"]
        assert record["cadence_analysis"]
        assert record["dependence_analysis"]
        assert record["k_justification"]
        assert record["minimum_requirements_justification"]
        rows = reference["samples"]
        assert record["count"] == len(rows)
        assert record["span_s"] == rows[-1]["sampled_unix"] - rows[0]["sampled_unix"]
        assert len({row["sampled_unix"] for row in rows}) == len(rows)
        assert all(row["holder_free"] is True and row["holder_tail_free"] is True for row in rows)
        for cpu in record["cpu_ids"]:
            values = [row["foreign_per_cpu_busy"][str(cpu)] for row in rows]
            line = record["per_cpu"][str(cpu)]
            assert line["max"] == max(values)
            assert line["stdev"] == statistics.pstdev(values)
            assert line["threshold"] == max(values) + record["k"] * statistics.pstdev(values)
        evaluation = retained_input(record["evaluation"])
        assert evaluation["reference_sha256"] == record["reference"]["sha256"]
        assert set(evaluation["source_timestamps"]).isdisjoint(row["sampled_unix"] for row in rows)
        assert evaluation["ambient_false_refusals"] >= 0
        assert evaluation["ambient_observations"] > 0
        assert evaluation["foreign_refused"] is True
        assert evaluation["sustained_foreign_refused"] is True
        assert evaluation["clear_recovered"] is True
        assert evaluation["replay_command"], "provide a reproducible offline evaluation command"
        assert record["proposed_configuration"], "proposed host inputs, not deployed authority"
