"""A producer's export rates are learned per declared family (#1126).

PQ #1225 Phase 1, row 029 on sparky, 2026-09-25: a GLM-5.3 Stage B row
handed off 32 groups through its spool and waited 204 s for them.  #999 sizes
a producer's export allowance from this host's completed exports of *the same
produced-output template*, and every Stage B row is its own template (layer,
chain and inputs differ), so no row was ever measured: each one ran with the
unmeasured single slot.  The exports never overlapped, and the ones past that
slot fell through to ordinary admission, where eight were
``deferred_behind_withholding`` while another item held the box.

A template may now declare ``export_rate_family``.  The family is part of the
template's canonical bytes, so ``template_sha256`` and the sealed request's
declaration cover it; ``learn_export`` and ``export_slots`` key on it, and a
producer that declares none keys on its template, as before.  The first row of
a family still starts ``unmeasured``; every later row inherits the
measurement.  The Little's-law rule is unchanged.

Fixture concessions: the producers are sealed by the produced-output fixtures
and claimed through the real adaptive path on sparklina's CPU map.  An
export's completion is fed through ``PoolQueue._learn_export``, the hook its
resource-scope cleanup calls, with its landing time stated.
"""
from __future__ import annotations

import math
from pathlib import Path

import pytest

import test_a_producers_exports_run_on_its_allowance as al
import test_prepaid_writer_integration as fx
from prismabuild import adaptive_cpu, core, pool, produced_output as po
from prismabuild import produced_spool as ps

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

#: The family PQ's Stage B handoff templates declare (PQ #1230's stub).
FAMILY = "pq-stageb-handoff"
#: Small enough that three producers and their allowances share the box.
SMALL = {"cpu": 4, "mem_gb": 8}


def _template(tmp_path: Path, template_id: str, family: str | None) -> dict:
    """One Stage B row's template: its own id and prefix, as every row's is."""

    body = dict(fx._template(str(tmp_path / template_id)), template_id=template_id)
    if family is not None:
        body["export_rate_family"] = family
    return po.validate_template(body)


def _publish(tmp_path: Path, queue: pool.PoolQueue, template: dict,
             demand: dict[str, int], **variables: str) -> str:
    """Seal a spool producer of ``template`` and publish it; return its key."""

    cas_root = tmp_path / "cas"
    initial = fx._producer_request(tmp_path, cas_root, template)
    cas, request = po._read_producer_request(cas_root, initial)
    request.pop("action_key")
    request["environment"]["variables"].update({
        ps.ROOT_ENV: str(tmp_path / "local"), ps.MAX_ENV: "256", **variables})
    action = core.seal_action(request)
    cas.publish_action_request(action)
    owner = str(action["action_key"])
    queue.publish(action_key=owner, cas_root=str(cas_root),
                  worker_script=str(fx.REPO / "tools" / "prismabuild_worker.py"),
                  checkout_root=str(tmp_path / "mover-checkout"),
                  resources={**demand, **po.owner_demand_terms(template)},
                  produced_output_template=template)
    return owner


def _claim_producer(queue: pool.PoolQueue, monkeypatch, owner: str) -> dict:
    al._sample(monkeypatch, psi=0.)
    claimed = queue.claim(owner="spool-producer", capacity=al.CAPACITY,
                          cpu_tiers=al.CPU_TIERS, adaptive_cpu=True)
    assert claimed is not None and claimed["action_key"] == owner
    return claimed


def _allowance(queue: pool.PoolQueue, owner: str) -> dict:
    allowance = al._meta(queue, owner).get("dependent_allowance")
    assert isinstance(allowance, dict), "the producer was claimed without an allowance"
    return allowance


def _land(queue: pool.PoolQueue, owner: str, export: str, *,
          published_unix: float, landing_s: float) -> bool:
    """One of ``owner``'s exports finished, as its scope cleanup reports it."""

    return queue._learn_export(
        {"action_key": export, "dependent_of": owner, "published_unix": published_unix},
        {"complete": True, "action_key": export, "wall_seconds": landing_s})


def _rates(queue: pool.PoolQueue) -> dict:
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    return adaptive_cpu.read_json(base / adaptive_cpu.EXPORT_RATES)


def test_a_second_producer_of_a_family_inherits_its_measured_slots(
        tmp_path, monkeypatch) -> None:
    """Row 029 starts the family unmeasured and lands two exports, 10 s
    apart, the slower in 21 s.  Row 030 -- another template of the family --
    is claimed with ``ceil(21 / 10) = 3`` measured slots, not the one slot
    every Stage B row ran with."""

    queue = fx._queue(tmp_path)
    first_template = _template(tmp_path, "stageb-row-029", FAMILY)
    second_template = _template(tmp_path, "stageb-row-030", FAMILY)
    assert po.template_sha256(first_template) != po.template_sha256(second_template)

    first = _publish(tmp_path, queue, first_template, SMALL)
    _claim_producer(queue, monkeypatch, first)
    allowance = _allowance(queue, first)
    assert allowance["slots"] == 1
    assert allowance["basis"] == {
        "basis": "unmeasured", "template_sha256": po.template_sha256(first_template),
        "export_rate_family": FAMILY, "landings": 0, "spacings": 0}

    assert _land(queue, first, "1" * 64, published_unix=1000., landing_s=12.)
    assert _land(queue, first, "2" * 64, published_unix=1010., landing_s=21.)
    rates = _rates(queue)
    # Learned under the family, never under the template it came from.
    assert po.template_sha256(first_template) not in rates
    assert list(rates) == [adaptive_cpu.export_rate_key(None, FAMILY)]

    second = _publish(tmp_path, queue, second_template, SMALL)
    _claim_producer(queue, monkeypatch, second)
    allowance = _allowance(queue, second)
    if allowance["basis"]["basis"] != "measured":
        raise AssertionError(
            "a second producer of the family must inherit the first one's "
            f"measured export rate; it was claimed {allowance['basis']}")
    assert allowance["slots"] == math.ceil(21. / 10.) == 3
    assert allowance["basis"] == {
        "basis": "measured", "rule": "ceil(max landing_s / min spacing_s)",
        "template_sha256": po.template_sha256(second_template),
        "export_rate_family": FAMILY, "landing_s": 21., "spacing_s": 10.,
        "landings": 2, "spacings": 1}
    assert al._tokens(queue, second, "cpu") == SMALL["cpu"] + 3
    assert al._tokens(queue, second, "mem_gb") == SMALL["mem_gb"] + 3


def test_an_undeclared_producer_still_keys_on_its_template(
        tmp_path, monkeypatch) -> None:
    """A family's measured rates are never applied to a producer outside it:
    an undeclared producer is claimed on its template's own rates, which a
    second producer of the same template inherits, as #999 left it."""

    queue = fx._queue(tmp_path, gib=8)
    family_template = _template(tmp_path, "stageb-row-029", FAMILY)
    plain_template = _template(tmp_path, "plain-writer", None)
    assert "export_rate_family" not in plain_template

    member = _publish(tmp_path, queue, family_template, SMALL)
    _claim_producer(queue, monkeypatch, member)
    assert _land(queue, member, "1" * 64, published_unix=1000., landing_s=12.)
    assert _land(queue, member, "2" * 64, published_unix=1010., landing_s=21.)

    plain = _publish(tmp_path, queue, plain_template, SMALL)
    _claim_producer(queue, monkeypatch, plain)
    allowance = _allowance(queue, plain)
    assert allowance["slots"] == 1
    assert allowance["basis"] == {
        "basis": "unmeasured", "template_sha256": po.template_sha256(plain_template),
        "landings": 0, "spacings": 0}, allowance

    assert _land(queue, plain, "3" * 64, published_unix=2000., landing_s=30.)
    assert _land(queue, plain, "4" * 64, published_unix=2020., landing_s=8.)
    rates = _rates(queue)
    assert rates[po.template_sha256(plain_template)]["landing_s"] == [30., 8.]
    assert rates[adaptive_cpu.export_rate_key(None, FAMILY)]["landing_s"] == [12., 21.]

    # Same template, a different producer: it inherits the template's rates,
    # ceil(30 / 20) = 2, and not the family's 3.
    again = _publish(tmp_path, queue, plain_template, SMALL, PRISMABUILD_TEST_ROW="2")
    assert again != plain
    _claim_producer(queue, monkeypatch, again)
    allowance = _allowance(queue, again)
    assert allowance["slots"] == 2
    assert allowance["basis"]["basis"] == "measured"
    assert allowance["basis"]["template_sha256"] == po.template_sha256(plain_template)
    assert "export_rate_family" not in allowance["basis"]


def test_a_funded_export_runs_under_a_withhold_and_an_unfunded_one_waits(
        tmp_path, monkeypatch) -> None:
    """Row 029's window: a foreign 2-CPU/3 GiB action withholds the box for
    the producer to drain.  The producer is a later row of a family measured
    at two slots, so both of its first two exports are funded and run past
    the withhold -- under the template key the second was past the one
    unmeasured slot, and deferred behind it.  A third export, past the
    allowance, would take free tokens: it is still deferred, with no pass."""

    template = _template(tmp_path, "stageb-row-030", FAMILY)
    cas_root = tmp_path / "cas"
    queue = fx._queue(tmp_path)
    ledger = queue.ledger()
    # An earlier row of the family landed its exports on this host: the
    # slower in 20 s, 10 s apart.
    adaptive_cpu.write_json(ledger.base / "cpu-map.json", al.CPU_TIERS)
    earlier, earlier_template = "b" * 64, "c" * 64
    assert adaptive_cpu.learn_export(ledger, earlier_template, earlier, 1000., 12.,
                                     family=FAMILY)
    assert adaptive_cpu.learn_export(ledger, earlier_template, earlier, 1010., 20.,
                                     family=FAMILY)

    owner = _publish(tmp_path, queue, template, al.PRODUCER_DEMAND)
    claimed = _claim_producer(queue, monkeypatch, owner)
    allowance = _allowance(queue, owner)
    assert allowance["slots"] == 2 and allowance["basis"]["basis"] == "measured", allowance
    control = fx._broker_control(queue, owner)
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
        claim_snapshot=claimed, env={"PRISMABUILD_ACTION_KEY": owner,
            "PRISMABUILD_ACTION_NONCE": control["nonce"],
            "PRISMABUILD_ACTION_SCOPE": control["scope_id"]})
    po.declare_instance(queue.root, instance)
    assert po.admit_instance(queue, instance, template)["ok"]
    fx._announce_tier(queue, tmp_path / "stage")
    spool = ps.ProducedSpool(queue, instance, template, cas_root=cas_root,
                             root=tmp_path / "local", max_bytes=256)
    cas = core.PrismaBuildCAS(cas_root)

    foreign = al._foreign(spool, cas, "three-gib", {"cpu": 2, "mem_gb": 3})
    al._running(spool, monkeypatch, psi=0.)
    for _ in range(pool.STARVATION_FLOOR + 1):
        assert al._claim(queue, spool.host) is None, "the foreign action took the export's room"
        if al._denial(queue, foreign)["reason"].endswith("_withholding"):
            break
    assert al._denial(queue, foreign)["reason"] == "reservation_unavailable_withholding"

    funded = []
    for batch in ("g0", "g1"):
        export_key = al._export(spool, batch)
        al._running(spool, monkeypatch, psi=0.)
        claim = al._claim(queue, spool.host)
        if claim is None:
            denial = al._denial(queue, export_key)
            raise AssertionError(
                f"the producer's funded export {batch} was held out behind the "
                f"withhold: {denial['reason']} "
                f"({denial['evidence'].get('decision', {}).get('allowance')})")
        assert claim["action_key"] == export_key
        export = al._meta(queue, export_key)
        assert export["funded_by"] == owner
        assert al._tokens(queue, export_key, "cpu") == al._tokens(queue, export_key, "mem_gb") == 0
        funded.append(export["funded"]["cpus"])
    assert sorted(funded) == [[cpu] for cpu in sorted(_allowance(queue, owner)["cpus"])]
    assert queue.item_path(pool.READY, foreign).exists()

    unfunded = al._export(spool, "g2")
    bystander = al._foreign(spool, cas, "bystander", {"cpu": 1, "mem_gb": 1})
    assert al._claim(queue, spool.host) is None
    assert al._denial(queue, foreign)["reason"] == "reservation_unavailable_withholding"
    denial = al._denial(queue, unfunded)
    assert denial["reason"] == "deferred_behind_withholding", denial
    assert denial["evidence"]["withheld_for"] == foreign
    assert denial["evidence"]["dependent_of"] == owner
    assert queue.passes(unfunded) == 0
    assert queue.item_path(pool.READY, bystander).exists()
    assert queue.passes(bystander) == 0


def test_the_family_is_sealed_and_a_malformed_one_is_refused(tmp_path) -> None:
    """The family is in the template's canonical bytes, so its digest -- and
    through the sealed declaration, the producer's action key -- covers it.
    A template without it keeps its bytes.  A malformed family is refused by
    the template, by the allowance reader and by the learner, and no template
    digest can name a family's entry."""

    plain = _template(tmp_path, "stageb-row-029", None)
    member = _template(tmp_path, "stageb-row-029", FAMILY)
    assert "export_rate_family" not in plain
    assert member["export_rate_family"] == FAMILY
    assert po.template_sha256(member) != po.template_sha256(plain)
    assert po.template_sha256(member) != po.template_sha256(
        _template(tmp_path, "stageb-row-029", "another-family"))
    for malformed in ("", "Upper", "-leading", "has space", "x" * 257, None, 3, ["a"]):
        with pytest.raises(po.ProducedOutputError, match="export_rate_family"):
            po.validate_template(dict(plain, export_rate_family=malformed))

    # The reader refuses a row whose reference names a malformed family,
    # rather than falling back to the template.
    cas = core.PrismaBuildCAS(tmp_path / "cas")
    key = al._plain_request(cas, "tests/family", {adaptive_cpu.SPOOL_ROOT_ENV: "/spool"})
    template = po.template_sha256(plain)
    rates = {template: {"landing_s": [30.], "spacing_s": [10.]},
             adaptive_cpu.export_rate_key(None, FAMILY): {"landing_s": [21.], "spacing_s": [10.]}}

    def allowance(ref):
        return adaptive_cpu.producer_allowance(
            {"action_key": key, "cas_root": str(cas.root), "produced_output": ref}, rates)

    assert allowance({"template_sha256": template})["slots"] == 3
    assert allowance({"template_sha256": template, "export_rate_family": FAMILY})["slots"] == 3
    assert allowance({"template_sha256": template,
                      "export_rate_family": FAMILY})["basis"]["export_rate_family"] == FAMILY
    for malformed in ("Upper", "", None, 7):
        assert allowance({"template_sha256": template, "export_rate_family": malformed}) is None

    # No template digest reads or writes a family's entry.
    family_key = adaptive_cpu.export_rate_key(None, FAMILY)
    assert family_key not in (template, FAMILY)
    assert adaptive_cpu.export_rate_key(family_key, None) is None
    assert adaptive_cpu.export_slots(rates, family_key)[1]["basis"] == "unmeasured"
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    ledger = queue.ledger()
    adaptive_cpu.write_json(ledger.base / "cpu-map.json", al.CPU_TIERS)
    owner = "b" * 64
    assert not adaptive_cpu.learn_export(ledger, family_key, owner, 1000., 12.)
    assert not adaptive_cpu.learn_export(ledger, template, owner, 1000., 12., family="Upper")
    assert adaptive_cpu.read_json(
        adaptive_cpu.local_state_base(ledger.base) / adaptive_cpu.EXPORT_RATES) == {}
