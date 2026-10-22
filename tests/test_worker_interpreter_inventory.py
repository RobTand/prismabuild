"""Offer publications remember bounded path names, never executable answers."""
from prismabuild import capability_eligibility as eligibility
import test_worker_loop_offers_what_is_free as worker_offers


def test_sibling_and_empty_polls_recheck_the_same_inventory(tmp_path):
    inventory = tmp_path / "inventory.json"
    python = tmp_path / "python"
    python.write_text("#!/bin/sh\n")
    python.chmod(0o700)
    path = str(python)
    observe = lambda rows: eligibility.observe_interpreters(rows, inventory_path=inventory)
    assert observe([{"interpreter": path}]).answer(path) is eligibility.Presence.PRESENT
    # A sibling uses only persisted names, including on an empty READY poll.
    assert observe([]).answer(path) is eligibility.Presence.PRESENT
    python.unlink()
    assert observe([{"interpreter": str(tmp_path / "other-python")}]).answer(path) is (
        eligibility.Presence.ABSENT)
    python.write_text("#!/bin/sh\n")
    python.chmod(0o700)
    assert observe([]).answer(path) is eligibility.Presence.PRESENT
    python.chmod(0o600)
    assert observe([]).answer(path) is eligibility.Presence.ABSENT


def test_inventory_keeps_only_recently_requested_paths(tmp_path):
    inventory = tmp_path / "inventory.json"
    paths = [str(tmp_path / f"python-{index}")
             for index in range(eligibility.INTERPRETER_INVENTORY_LIMIT + 1)]
    for path in paths[:-1]:
        eligibility.observe_interpreters([{"interpreter": path}], inventory_path=inventory)
    # Refresh the oldest before crossing the bound: the second path is evicted.
    eligibility.observe_interpreters([{"interpreter": paths[0]}], inventory_path=inventory)
    answers = eligibility.observe_interpreters([{"interpreter": paths[-1]}], inventory_path=inventory)
    assert answers.answer(paths[0]) is eligibility.Presence.ABSENT
    assert answers.answer(paths[1]) is eligibility.Presence.UNKNOWN
    assert len(answers.absent) == eligibility.INTERPRETER_INVENTORY_LIMIT
    assert eligibility.observe_interpreters([], inventory_path=inventory) == answers


def test_actual_worker_publications_keep_sibling_names_and_restat(tmp_path, monkeypatch):
    worker = worker_offers._module()
    monkeypatch.setattr(worker_offers, "_module", lambda: worker)
    python = tmp_path / "python"
    python.write_text("#!/bin/sh\n")
    python.chmod(0o700)
    path = str(python)
    rows = [{"interpreter": path}]
    monkeypatch.setattr(worker, "discover_ready_snapshot", lambda *a, **k:
        worker.DiscoveryResult("ready", list(rows), None, ""))
    monkeypatch.setattr(worker.pool.PoolQueue, "serve_once", lambda *a, **k: None)
    for expected, requested in ((True, rows[:]), (True, []), (False, [])):
        rows[:] = requested
        if not expected:
            python.unlink()
        queue = worker_offers._run(tmp_path, [*worker_offers.BASE, "--once"],
                                   gpu_sample=worker_offers.sample())
        offer = queue._offer_records()[0]
        answer = eligibility.InterpreterAnswers.from_offer(offer).answer(path)
        assert answer is (eligibility.Presence.PRESENT if expected else eligibility.Presence.ABSENT)
