"""#1399 RED: real proc attribution, CPU sampler, sealed queue and drain rules.

The retained PID3--6 census names suspects, NOT the interval's consumers.
Constructed PF_KTHREAD and IRQ/softirq intervals separately supply that proof.
Only hardware observations and clocks are fake; no admission rule is replaced.
"""
from __future__ import annotations

import builtins
import json
import os
from functools import partial
from pathlib import Path

import pytest
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

from prismabuild import adaptive_cpu, control_cpu

# Expose the existing fixture without copying its sealed-action/GPU setup.
fleet = fleet_fixture
REAL_SAMPLE = adaptive_cpu.Controller.sample
REAL_CENSUS = adaptive_cpu.Controller._foreign_pids
PF_KTHREAD = 0x00200000  # Linux include/linux/sched.h, not a PID/name heuristic.
TOTAL = 203
BUSY = 21
FOREIGN = BUSY / TOTAL  # exact retained CPU0 ratio, ~.1034 > .1
PREDICTED = [18, 19, 0, 1, 2, 3, 4, 10]
HELD = [5, 6, 7, 8, 9, 15, 16, 17]
TIERS = {"preferred": [*HELD, *PREDICTED, 11, 12, 13, 14], "fallback": []}
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
RECORDED = json.loads((Path(__file__).parent / "fixtures" /
                       "measurement_kernel_denial_1399.json").read_text())["decision"]
INTERVAL = RECORDED["sample"]["interval_s"]
# Per-CPU tick denominators reconstruct the retained sample, not an invented
# holder pressure. CPU0's reduced 3/29 is the observed 21/203 interval.
RECORDED_TOTALS = [203, 205, 205, 209, 207, 207, 208, 208, 208, 208,
                   205, 206, 207, 206, 206, 207, 208, 208, 207, 210]


class ProcInterval:
    """Deterministic Linux proc files read by the actual existing readers."""

    def __init__(self, root, runtime):
        self.root, self.runtime = root, runtime
        root.mkdir()
        self.host = {cpu: [0] * 8 for cpu in range(20)}
        self.psi_total = 0
        self.tasks = {}
        for pid in (3, 4, 5, 6):
            self.add(pid, flags=0)
        self.write()

    def add(self, pid, *, flags, control=False):
        process = self.root / str(pid)
        (process / "task" / str(pid)).mkdir(parents=True)
        self.tasks[pid] = {"flags": flags, "cpu": 0, "ticks": 0,
                           "start": 100 + pid, "migrations": 0}
        if control:
            script = self.runtime / "tools" / "fleet" / "worker_loop.py"
            script.parent.mkdir(parents=True, exist_ok=True)
            script.write_text("# fixture\n")
            (process / "cmdline").write_bytes(f"/usr/bin/python3\0{script}\0".encode())
            (process / "environ").write_bytes(b"PRISMABUILD_SUPERVISED_WORKER=sparklina\0")
        else:
            (process / "cmdline").write_bytes(b"")
            (process / "environ").write_bytes(b"")

    def write(self):
        for pid, task in self.tasks.items():
            fields = ["0"] * 40
            fields[0], fields[6] = "S", str(task["flags"])
            fields[11], fields[19], fields[36] = (
                str(task["ticks"]), str(task["start"]), str(task["cpu"]))
            raw = f"{pid} (fixture with ) spaces) " + " ".join(fields)
            process = self.root / str(pid)
            (process / "stat").write_text(raw)
            (process / "task" / str(pid) / "stat").write_text(raw)
            (process / "task" / str(pid) / "sched").write_text(
                f"se.nr_migrations : {task['migrations']}\n")
        (self.root / "stat").write_text("".join(
            f"cpu{cpu} " + " ".join(map(str, ticks)) + "\n"
            for cpu, ticks in self.host.items()))
        (self.root / "pressure").mkdir(exist_ok=True)
        (self.root / "pressure" / "cpu").write_text(
            f"some avg10=0.00 avg60=0.00 avg300=0.00 total={self.psi_total}\n")

    def advance(self, *, user=0, kernel=0, irq=0, softirq=0,
                control=0, holders=False, saturated=False, recorded=False):
        for cpu, ticks in self.host.items():
            delta = [0] * 8
            total = TOTAL
            if recorded:
                total = RECORDED_TOTALS[cpu]
                delta[0] = round(RECORDED["sample"]["per_cpu_busy"][str(cpu)] * total)
            elif saturated:
                delta[5] = 193
            elif cpu == 0:
                delta[0], delta[2], delta[5], delta[6] = user, kernel + control, irq, softirq
            elif holders and cpu == 9:
                delta[0] = TOTAL
            delta[3] = total - sum(delta)
            self.host[cpu] = [old + new for old, new in zip(ticks, delta, strict=True)]
        if recorded:
            self.psi_total += 11689  # retained PSI delta / retained interval
        self.tasks[3]["ticks"] += kernel
        if 7 in self.tasks:
            self.tasks[7]["ticks"] += control
        self.write()


@pytest.fixture
def kernel_rig(fleet, tmp_path, monkeypatch):
    queue, clock, readings, gpu_sample, publish, tick, _claim, denial = fleet
    proc = ProcInterval(tmp_path / "proc", tmp_path / "runtime")
    # Restore the CPU implementations overridden by the reusable fleet
    # fixture; bind the actual collector's existing public input parameters.
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", REAL_SAMPLE)
    monkeypatch.setattr(adaptive_cpu.Controller, "_foreign_pids", REAL_CENSUS)
    monkeypatch.setattr(adaptive_cpu, "control_plane_counters", partial(
        control_cpu.control_plane_counters, proc_root=proc.root,
        runtime_root=proc.runtime, hostname="sparklina"))
    read_text, open_file, listdir = Path.read_text, builtins.open, os.listdir

    def redirect(path):
        if isinstance(path, (str, os.PathLike)):
            candidate = Path(path)
            if candidate.is_relative_to("/proc"):
                return proc.root / candidate.relative_to("/proc")
        return path

    monkeypatch.setattr(Path, "read_text", lambda path, *a, **kw:
                        read_text(Path(redirect(path)), *a, **kw))
    monkeypatch.setattr(builtins, "open", lambda path, *a, **kw:
                        open_file(redirect(path), *a, **kw))
    monkeypatch.setattr(os, "listdir", lambda path=".": listdir(redirect(path)))
    controller = adaptive_cpu.Controller(queue.ledger(), TIERS)
    assert controller.sample() == {}

    def step(**load):
        # Fixture publication advances its clock to distinguish generations.
        # Reproduce the recorded host-sampling interval, not that extra gap.
        duration = INTERVAL
        if load.get("recorded"):
            previous = adaptive_cpu.read_json(controller.base / "cpu-sample.json")
            duration -= clock[0] - previous["sampled_unix"]
            assert duration > 0
        tick(duration)
        proc.advance(**load)
        return controller.sample()

    def claim():
        result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                             has_gpu=True, tags=["gb10", "sparklina"])
        return None if result is None else result["action_key"]

    return queue, proc, controller, publish, step, claim, denial


def incident(rig):
    queue, proc, cpu, publish, step, claim, denial = rig
    step()
    holder = publish("incumbent", cpu=8)
    assert claim() == holder
    assert queue.ledger().cpu_allocation(holder, TIERS)["preferred"] == HELD
    # Reconstruct the retained unmeasured state of an already running holder,
    # not the measured quiet baseline introduced by fixture startup.
    (cpu.base / adaptive_cpu.IDLE_BASELINE).unlink(missing_ok=True)
    measurement = publish("measurement", measurement=True, pinned=True, cpu=8)
    observed = step(recorded=True)
    assert observed["busy_cpus"] == pytest.approx(RECORDED["sample"]["busy_cpus"])
    assert observed["psi_some"] == pytest.approx(RECORDED["sample"]["psi_some"])
    assert observed["foreign_per_cpu_busy"] == pytest.approx(RECORDED["sample"]["foreign_per_cpu_busy"])
    assert claim() is None
    old = denial(measurement)["evidence"]["decision"]
    assert old["reason"] == "measurement_foreign_ambient"
    assert old["predicted_cpus"] == PREDICTED
    assert old["held_cpus"] == HELD
    assert old["per_cpu_foreign_busy"]["0"] == RECORDED["per_cpu_foreign_busy"]["0"]
    assert old["per_cpu_foreign_max"] == .1
    assert sorted(old["foreign_pids"]["0"]) == [3, 4, 5, 6]
    assert {key: old["baseline"][key] for key in ("basis", "samples", "state")} == {
        "basis": "unmeasured", "samples": 0, "state": "holders_present"}
    return holder, measurement


def test_recorded_foreign_denial_is_typed_with_real_sampler_and_census(kernel_rig):
    incident(kernel_rig)


@pytest.mark.parametrize("kind", ["kthread", "irq_softirq"])
def test_proven_kernel_baseline_is_not_foreign_but_raw_busy_stays_raw(kernel_rig, kind):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    if kind == "kthread":
        for task in proc.tasks.values():
            task["flags"] = PF_KTHREAD
        load = {"kernel": BUSY}
    else:
        # Same census and zero suspect ticks: observed stat deltas, not PIDs,
        # prove this baseline. No user or kernel ticks are invented.
        load = {"irq": 10, "softirq": 11}
    step(**load)
    observed = step(**load)
    assert observed["per_cpu_busy"]["0"] == pytest.approx(FOREIGN)
    assert observed["busy_cpus"] == pytest.approx(FOREIGN)
    assert observed["foreign_per_cpu_busy"]["0"] == pytest.approx(0), observed


def test_proven_kernel_load_retires_foreign_veto_and_withholds_backfill(kernel_rig):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    holder, measurement = incident(kernel_rig)
    for task in proc.tasks.values():
        task["flags"] = PF_KTHREAD
    step(kernel=BUSY, holders=True)
    step(kernel=BUSY, holders=True)
    backfill = publish("backfill")
    assert claim() is None, "kernel-only CPU0 kept the foreign veto and refilled the host"
    waiting = denial(measurement)["evidence"]
    assert waiting["decision"]["measurement_pool_drain"]["foreign_clear"] is True, waiting
    assert waiting["withhold"]["why"] == "draining_for_measurement", waiting
    assert denial(backfill)["evidence"]["withheld_for"] == measurement
    assert queue.ledger().held_keys() == [holder]


def test_same_kernel_irq_measurement_admits_after_one_holder_drain(kernel_rig):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    holder, measurement = incident(kernel_rig)
    for task in proc.tasks.values():
        task["flags"] = PF_KTHREAD
    load = {"irq": 10, "softirq": 11}
    step(**load, holders=True)
    step(**load, holders=True)
    assert claim() is None  # never admit beside the real GPU/CPU holder
    queue.finish(holder, status="executed")
    # Sample six real counter intervals (~12.6 s), not a clock-only jump;
    # the last interval and the GPU avg10 horizon no longer reach the holder.
    for _ in range(5):
        step(**load)
    observed = step(**load)
    predicted = queue.ledger().free_cpu_allocation(8, TIERS)
    assert claim() == measurement, denial(measurement)
    assert queue.ledger().cpu_allocation(measurement, TIERS)["preferred"] == predicted
    meta = adaptive_cpu.read_json(queue.ledger().held_dir / measurement / adaptive_cpu.METADATA)
    assert meta["declared_cpu"] == 8 and meta["cost"] == 8 and meta["borrowing"] is False
    assert observed["per_cpu_busy"]["0"] == pytest.approx(FOREIGN)
    assert observed["foreign_per_cpu_busy"]["0"] == pytest.approx(0), observed


@pytest.mark.parametrize("fault", ["ordinary_low_pid", "unreadable_flags", "missing_sched",
                                   "vanished", "reused", "migrating", "cpu_changed", "reset",
                                   "overcount"])
def test_unproven_or_user_cpu0_ticks_cannot_be_subtracted(kernel_rig, fleet, fault):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    for task in proc.tasks.values():
        task["flags"] = PF_KTHREAD
    step(kernel=BUSY)
    task = proc.tasks[3]
    if fault == "ordinary_low_pid":
        task["flags"] = 0
    elif fault == "reused":
        task["start"] += 1
    elif fault == "migrating":
        task["migrations"] += 1
    elif fault == "cpu_changed":
        task["cpu"] = 1
    elif fault == "reset":
        task["ticks"] = -30
    elif fault == "overcount":
        task["ticks"] += TOTAL
    proc.advance(kernel=BUSY)
    process = proc.root / "3"
    thread = process / "task" / "3"
    if fault == "missing_sched":
        (thread / "sched").unlink()
    elif fault == "vanished":
        (thread / "stat").unlink()
    elif fault == "unreadable_flags":
        for path in (process / "stat", thread / "stat"):
            raw = path.read_text()
            close = raw.rfind(")")
            fields = raw[close + 1:].split()
            fields[6] = "unknown"
            path.write_text(raw[:close + 1] + " " + " ".join(fields))
    # Advance the existing fixture clock without repairing the damaged proc
    # endpoint. Raw host counters remain complete, coherent and unchanged.
    fleet[5](INTERVAL)
    observed = cpu.sample()
    assert observed["per_cpu_busy"]["0"] == pytest.approx(FOREIGN)
    assert observed["foreign_per_cpu_busy"]["0"] == pytest.approx(FOREIGN), observed


@pytest.mark.parametrize("pid", [3, 12345])
def test_stable_real_user_pid_at_recorded_cpu0_busy_stays_foreign(kernel_rig, fleet, pid):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    if pid not in proc.tasks:
        proc.add(pid, flags=0)
    # Stable identity, scheduler evidence and real ticks are insufficient
    # without PF_KTHREAD, whether the PID is low or ordinary user-space sized.
    for _ in range(2):
        proc.advance(user=BUSY)
        proc.tasks[pid]["ticks"] += BUSY
        proc.write()
        fleet[5](INTERVAL)
        cpu.sample()
    observed = cpu.sample()
    assert observed["per_cpu_busy"]["0"] == pytest.approx(FOREIGN)
    assert observed["foreign_per_cpu_busy"]["0"] == pytest.approx(FOREIGN), observed


def test_mixed_user_kernel_irq_and_supervised_control_keep_user_ticks(kernel_rig):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    for task in proc.tasks.values():
        task["flags"] = PF_KTHREAD
    proc.add(7, flags=0, control=True)
    load = {"user": BUSY, "kernel": 2, "irq": 1, "softirq": 1, "control": 4}
    step(**load)
    observed = step(**load)
    assert observed["per_cpu_busy"]["0"] == pytest.approx(29 / TOTAL)
    assert observed["control_plane_busy"]["0"] == pytest.approx(4 / TOTAL)
    # Proc evidence does not prove thread CPU time and IRQ time disjoint.
    # The safe union lower bound is max(control+kernel, IRQ+softirq), not
    # their sum. It must retain all 21 user ticks, plus possible overlap.
    assert observed["foreign_per_cpu_busy"]["0"] == pytest.approx((29 - max(4 + 2, 1 + 1)) / TOTAL), observed


def test_overlapping_thread_irq_proof_is_credited_once(kernel_rig):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    for task in proc.tasks.values():
        task["flags"] = PF_KTHREAD
    step(irq=BUSY)
    # The same raw IRQ interval can overlap task time. Do not assume disjoint
    # accounting merely because each separate counter fits inside raw busy.
    proc.tasks[3]["ticks"] += BUSY
    proc.write()
    observed = step(irq=BUSY)
    assert observed["per_cpu_busy"]["0"] == pytest.approx(FOREIGN)
    assert observed["foreign_per_cpu_busy"]["0"] == pytest.approx(0)
    assert observed["system_baseline_busy"]["0"] == pytest.approx(FOREIGN)


@pytest.mark.parametrize("fault", ["missing_history", "incomplete_history", "reset"])
def test_unproven_irq_interval_leaves_user_busy_foreign(kernel_rig, fault):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    step(irq=BUSY)
    if fault != "reset":
        previous = adaptive_cpu.read_json(cpu.base / "cpu-sample.json")
        if fault == "missing_history":
            previous.pop("irq_cpus")
        else:
            previous["irq_cpus"].pop("19")
        cpu.write_state("cpu-sample.json", previous)
        observed = step(user=BUSY)
    else:
        observed = step(user=BUSY + 1, irq=-1)
    assert observed["per_cpu_busy"]["0"] == pytest.approx(FOREIGN)
    assert observed["foreign_per_cpu_busy"]["0"] == pytest.approx(FOREIGN)


def test_raw_irq_saturation_still_refuses_measurement(kernel_rig):
    queue, proc, cpu, publish, step, claim, denial = kernel_rig
    measurement = publish("saturated", measurement=True, pinned=True, cpu=8)
    step(saturated=True)
    observed = step(saturated=True)
    assert observed["busy_cpus"] == pytest.approx(20 * 193 / TOTAL)
    assert claim() is None
    assert denial(measurement)["evidence"]["decision"]["reason"] == "host_pressure"
    assert not queue.ledger().held_keys()
