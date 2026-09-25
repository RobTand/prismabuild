"""A measurement is judged idle against its host's own idle history (#997).

Measurement isolation refused a measurement when the host's busy CPUs passed
a fixed 5% of its CPUs (1.0 CPU on a 20-CPU GB10), and when system CPU PSI
"some" reached 0.10.  Measured on sparky at 12:16Z on 2026-09-23 with no pool
work running: 0.373 busy CPUs from idle Claude Code sessions, containerd,
dockerd and netdata.  One more session, or one oversubscribed daemon holding
PSI up, and no measurement can run on the box, with nothing in the refusal
saying what "idle" was taken to mean.

The host now keeps its own idle history: the fresh samples admission takes
while none of the pool's work runs on it.  A measurement is refused only when
the current sample is above the largest idle sample the host has shown, and
every refusal carries that baseline (count, span, mean, standard deviation,
maximum and ``margin = max - mean``).  The claim record stamps the baseline an
admitted measurement ran against.

Fixture concessions: the measurement is sealed by the real ``pbrun`` sealer
into a real CAS and claimed through the real adaptive path on a Spark-sized
host.  Only the CPU sample is stated.
"""
from __future__ import annotations

from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import adaptive_cpu, core as pb, pool  # noqa: E402

import test_a_measurement_admits_its_own_spool_exports as fx  # noqa: E402

#: A 0.9-CPU housekeeping load on a 20-CPU host, as its per-pass samples see
#: it: busy CPUs scatter around 0.9 across the old 1.0 line, one oversubscribed
#: daemon holds system PSI "some" around 0.11 most passes, and one pass
#: catches it quiet. A sample the old fixed line itself would refuse no
#: longer seeds the window (#1014), so the host's idle state can only be
#: learned starting from a reading that line would also have let through.
HOUSEKEEPING = [(1.10, .11), (0.70, .11), (1.05, .12), (0.75, .03), (0.90, .11)]


def _sample(monkeypatch, busy: float, psi: float) -> None:
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": time.time(), "cpu_count": 20, "interval_s": 1e-3,
        "busy_cpus": busy, "psi_some": psi})


def _measurement(tmp_path: Path):
    import pbrun

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = pbrun.seal_action_from_template(
        fx._template(measurement=True, command=["python", "stage_a.py"]))
    key = fx._publish(queue, cas, action, fx.MEASUREMENT_DEMAND)
    return queue, key


def test_a_measurement_is_admitted_beside_housekeeping(tmp_path, monkeypatch) -> None:
    """The host's 0.9-CPU housekeeping is its idle state.  The measurement is
    admitted within the passes that observe it, and its claim record names
    the baseline it was judged against.

    On a cold host every reading the old fixed line refuses stays refused,
    however many passes that takes (#1014): none of them may seed the window,
    so each denial's baseline stays ``basis: unmeasured`` with zero samples
    -- unlike before the fix, where the first refused reading became the
    window's only (and so its own) baseline and let the same load through on
    the very next pass.  The measurement is admitted on the first pass the
    line itself would not have refused, judged the same way the line always
    judged it."""

    queue, key = _measurement(tmp_path)
    claim = None
    for busy, psi in HOUSEKEEPING:
        _sample(monkeypatch, busy, psi)
        claim = fx._claim(queue)
        if claim is not None:
            break
        decision = fx._denial(queue, key)["evidence"]["decision"]
        assert "baseline" in decision, (
            "a measurement refused as not idle must say what idle was judged "
            f"against; it was refused {decision.get('reason')} with no baseline")
        assert decision["baseline"]["basis"] == "unmeasured", decision["baseline"]
        assert decision["baseline"]["samples"] == 0, (
            "a sample the old fixed line refuses must not seed the window "
            f"(#1014); the window already holds {decision['baseline']['samples']}")
    assert claim is not None and claim["action_key"] == key, (
        "a measurement must run beside the host's own housekeeping")
    baseline = claim["idle_baseline"]
    assert baseline["state"] == "idle" and baseline["exceeds"] is False
    assert baseline["basis"] == "unmeasured" and baseline["samples"] == 0
    assert baseline["current"] == {"busy_cpus": 0.75, "psi_some": .03}
    meta = adaptive_cpu.read_json(queue.ledger().held_dir / key / adaptive_cpu.METADATA)
    assert meta["idle_baseline"] == baseline


def test_foreign_load_above_the_baseline_is_refused_with_the_baseline(
        tmp_path, monkeypatch) -> None:
    """Housekeeping teaches the host its idle state; a 3-CPU foreign load
    arrives with no pool holder in the way.  The measurement is refused for
    as long as the load runs, and every refusal carries the baseline."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 20})
    controller = adaptive_cpu.Controller(ledger, fx.CPU_TIERS)

    # Samples half a second apart, all inside the freshness bound.
    base = time.time()
    clock = iter(base - 4.5 + .5 * i for i in range(10))

    def decide(busy: float, psi: float):
        controller._host_sample = {"sampled_unix": next(clock), "cpu_count": 20,
                                   "interval_s": 1e-3, "busy_cpus": busy, "psi_some": psi}
        return controller.decision({"action_key": "a" * 64}, {"cpu": 10},
                                   identity=("shape", True))

    for busy, psi in HOUSEKEEPING:
        decide(busy, psi)
    # Only the one housekeeping pass the old fixed line would not have
    # refused seeded the window (#1014); the two after it join too, so three
    # samples -- not all five -- are the baseline a foreign load is judged
    # against below.
    assert decide(0.8, .10) is not None, controller.last_decision

    for _ in range(3):
        assert decide(3.9, .30) is None
        decision = controller.last_decision
        assert decision["reason"] == "measurement_host_not_idle", decision
        baseline = decision["baseline"]
        assert baseline["exceeds"] is True and baseline["state"] == "idle"
        assert baseline["samples"] == 3, baseline
        assert baseline["window_bound"] == adaptive_cpu.IDLE_WINDOW
        assert baseline["current"] == {"busy_cpus": 3.9, "psi_some": .30}
        busy = baseline["busy_cpus"]
        assert busy["max"] == 0.90 and abs(busy["margin"] - (busy["max"] - busy["mean"])) < 1e-6
        assert busy["stdev"] > 0 and baseline["span_s"] >= 0
    # The load ends; the host is idle again.
    assert decide(0.9, .11) is not None, controller.last_decision


def test_a_sample_across_a_holders_tail_is_not_idle(tmp_path, monkeypatch) -> None:
    """A sample taken beside a holder, or whose interval began while one ran,
    measures the holder, so it never joins the idle history.  Beside a holder
    a load above the history refuses as not idle; a quiet one leaves the
    holder to refuse the measurement (#982)."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 20})
    controller = adaptive_cpu.Controller(ledger, fx.CPU_TIERS)

    def decide(busy, interval):
        controller._host_sample = {"sampled_unix": time.time(), "cpu_count": 20,
                                   "interval_s": interval, "busy_cpus": busy, "psi_some": 0.}
        return controller.decision({"action_key": "a" * 64}, {"cpu": 10},
                                   identity=("shape", True))

    assert decide(0.5, 1e-3) is not None      # unmeasured: the pre-#997 line admits
    assert decide(0.5, 1e-3) is not None
    assert ledger.acquire("0" * 64, {"cpu": 1})
    assert decide(9.0, 1e-3) is None
    decision = controller.last_decision
    assert decision["reason"] == "measurement_host_not_idle", decision
    assert decision["baseline"]["state"] == "holders_present"
    assert decision["baseline"]["exceeds"] is True
    assert decide(0.4, 1e-3) is None
    assert controller.last_decision["reason"] == "measurement_holder"
    ledger.release("0" * 64)
    assert decide(0.4, 60.) is None
    decision = controller.last_decision
    assert decision["reason"] == "measurement_host_not_idle"
    assert decision["baseline"]["state"] == "holder_tail", decision
    state = adaptive_cpu.read_json(controller.base / adaptive_cpu.IDLE_BASELINE)
    assert len(state["samples"]) == 2


def test_a_sustained_change_becomes_the_hosts_idle_state() -> None:
    """A run above the baseline that outlasts the span the window remembers
    is the host's new idle state, not a foreign load to wait out forever."""

    state: dict = {}
    t = 1_000.

    def judge(busy):
        nonlocal state
        verdict, state = adaptive_cpu.idle_judgement(
            state, {"sampled_unix": t, "interval_s": 1e-3, "busy_cpus": busy, "psi_some": 0.},
            holders=False, identity=20)
        return verdict

    first = judge(0.4)                         # no history and no prior rule
    assert first["samples"] == 0 and first["basis"] == "unmeasured" and first["exceeds"]
    for _ in range(4):
        t += 10.
        assert judge(0.4)["exceeds"] is False
    t += 10.
    start = t
    # The span the run must outlive is measured from the oldest remembered
    # sample (t=1000) to the run's own start (t=1050), not between the five
    # remembered samples themselves (which would give 40 s and let the run
    # outlive them one pass early, #1014): 50 s.
    assert judge(2.0)["exceeds"] is True
    while t - start < 50.:
        t += 10.
        verdict = judge(2.0)
        if t - start < 50.:
            assert verdict["exceeds"] is True and verdict["samples"] == 5, verdict
    assert verdict["exceeds"] is False, verdict
    assert "excursion_unix" not in state


def _fixed_line(current):
    # The pre-#997 line as ``Controller.idle`` builds it for a 20-CPU host:
    # ``busy_cpus > .05 x CPUs or psi_some >= .10``.
    return current["busy_cpus"] > 1.0 or current["psi_some"] >= .10


_fixed_line.__doc__ = "busy_cpus > 1.0 (.05 x 20 CPUs) or psi_some >= .10"


def test_a_cold_host_refuses_a_constant_foreign_load_on_every_pass() -> None:
    """A cold host absorbed a sustained foreign load on its second pass
    (#1014): a sample the old fixed line refused still joined the window, so
    on the next pass that refused sample was the window's own maximum and the
    same load no longer exceeded it.  A refused sample must not seed the
    window, so an unchanging load above the line is refused on every pass it
    runs -- not only the first."""

    state: dict = {}
    t = 1_000.

    def judge():
        nonlocal state, t
        t += 10.
        verdict, state = adaptive_cpu.idle_judgement(
            state, {"sampled_unix": t, "interval_s": 1e-3, "busy_cpus": 3.9, "psi_some": .30},
            holders=False, identity=20, prior_rule=_fixed_line)
        return verdict

    for pass_number in range(1, 4):
        verdict = judge()
        assert verdict["exceeds"] is True, (pass_number, verdict)
        assert verdict["basis"] == "unmeasured", (pass_number, verdict)
        assert verdict["samples"] == 0, (pass_number, verdict)
    assert state["samples"] == [], "a refused sample must not seed the window"


def test_a_foreign_load_is_refused_until_it_outlives_the_remembered_idle_span() -> None:
    """After a clean idle sample seeds the window, the same load as above is
    refused until it has lasted as long as the host's remembered idle span
    (#1014) -- measured from that oldest remembered sample to the run's own
    start, not between the remembered samples themselves (there is only one
    of them here, which would make that span zero and admit the load at
    once)."""

    state: dict = {}
    t = 1_000.

    def judge(busy, psi):
        nonlocal state
        verdict, state = adaptive_cpu.idle_judgement(
            state, {"sampled_unix": t, "interval_s": 1e-3, "busy_cpus": busy, "psi_some": psi},
            holders=False, identity=20, prior_rule=_fixed_line)
        return verdict

    seed = judge(0.4, 0.)                      # the one clean sample; seeds
    assert seed["exceeds"] is False and seed["basis"] == "unmeasured"
    t += 30.
    start = t
    # The remembered idle span is measured to here, the run's start: 30 s.
    assert judge(3.9, .30)["exceeds"] is True
    while t - start < 30.:
        t += 10.
        verdict = judge(3.9, .30)
        if t - start < 30.:
            assert verdict["exceeds"] is True and verdict["samples"] == 1, verdict
    assert verdict["exceeds"] is False, verdict


def test_the_window_is_bounded() -> None:
    state: dict = {}
    for i in range(adaptive_cpu.IDLE_WINDOW + 10):
        _, state = adaptive_cpu.idle_judgement(
            state, {"sampled_unix": float(i + 1), "interval_s": 1e-3,
                    "busy_cpus": 0.5, "psi_some": 0.}, holders=False, identity=20)
    assert len(state["samples"]) == adaptive_cpu.IDLE_WINDOW
