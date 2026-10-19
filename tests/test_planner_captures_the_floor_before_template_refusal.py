"""The planner keeps the original filesystem-floor capture boundary."""
from pathlib import Path

import test_manifest_rows_gain_residency_plans as rows


def test_the_planner_captures_floor_cwd_before_refusing_a_template(tmp_path, monkeypatch):
    fleet = rows.Fleet(tmp_path)
    key = rows._manifest_row(fleet, "floor-capture-order",
        annotations={"phases": rows.phase_table(rows.NAMED_FILES)})
    ready = [rows._ready_item(fleet, key)]
    events = []
    original = Path.cwd

    def capture_cwd(cls):
        events.append("cwd")
        return original()

    def refuse_template(*args, **kwargs):
        events.append("template")
        return None

    monkeypatch.setattr(Path, "cwd", classmethod(capture_cwd))
    monkeypatch.setattr(rows.manifest_promotion, "movement_template_of", refuse_template)
    outcomes = rows.manifest_promotion.promote_ready_manifest_rows(
        fleet.queue, fleet.cas_root, rows._stage_tier(fleet), ready=ready)
    assert len(outcomes) == 1 and outcomes[0]["outcome"] == "refused", outcomes
    assert events == ["cwd", "template"]
