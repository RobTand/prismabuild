from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbmetrics  # noqa: E402

NOW = 10_000.0
CPU_KEY = "a1" * 32
GPU_KEY = "b2" * 32
READY_KEY = "c3" * 32


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _offer(host: str, announced: float, *, gpu: bool) -> dict:
    return {
        "schema": pool.POOL_OFFER_SCHEMA_V1,
        "host": host,
        "announced_unix": announced,
        "tags": [host, "gb10" if gpu else "x86"],
        "has_gpu": gpu,
        "capacity": {"cpu": 8, "gpu": int(gpu), "mem_gb": 32},
        "observed_capacity": {"cpu": 7, "gpu": int(gpu), "mem_gb": 30},
        "foreign": {},
        "observed_detail": {
            "mem_available_gb": 29,
            "gpu_memory_domains": ["shared_system"] if gpu else [],
        },
    }


def _item(key: str, *, host: str | None, gpu: bool, published: float,
          claimed: float | None = None, nonce: str | None = None) -> dict:
    record = {
        "schema": pool.POOL_ITEM_SCHEMA_V1,
        "action_key": key,
        "published_unix": published,
        "published_by": "submitter",
        "tags": [],
        "needs_gpu": gpu,
        "resources": {"cpu": 2, "mem_gb": 4, **({"gpu": 1} if gpu else {})},
    }
    if claimed is not None:
        record.update({
            "claimed_unix": claimed,
            "claimed_host": host,
            "claimed_by": f"worker-{host}",
            "resource_scope": {"nonce": nonce},
        })
    return record


def _ending(queue: Path, key: str, status: str, host: str,
            published: float, claimed: float, finished: float) -> None:
    state = "done" if status in {"executed", "cache_hit"} else (
        "withdrawn" if status == "withdrawn" else "failed")
    _write(queue / state / f"{key}.json", {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": key,
        "status": status,
        "claimed_host": host,
        "finished_host": host,
        "published_unix": published,
        "claimed_unix": claimed,
        "finished_unix": finished,
        "detail": {"elapsed_s": finished - claimed, "returncode": 0},
    })


def _samples(text: str, name: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith(name + "{")
            or line.startswith(name + " ")]


class MetricsFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.queue = Path(self.temporary.name) / "pb-queue"
        for name in ("ready", "claimed", "done", "failed", "withdrawn", "workers"):
            (self.queue / name).mkdir(parents=True)
        for patcher in (
            mock.patch.object(pbmetrics.time, "time", return_value=NOW),
            mock.patch.object(pbmetrics.pbstatus.time, "time", return_value=NOW),
            mock.patch.object(pool, "_now", return_value=NOW),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

        _write(self.queue / "workers" / "sparky.json",
               _offer("sparky", NOW - 2, gpu=True))
        _write(self.queue / "workers" / "dl380.json",
               _offer("dl380", NOW - 3, gpu=False))
        _write(
            self.queue / "workers" / "gx10-old.json",
            _offer("gx10-old", NOW - pool.OFFER_TIMEOUT_S - 5, gpu=True),
        )
        _write(self.queue / "ready" / f"{READY_KEY}.json",
               _item(READY_KEY, host=None, gpu=False, published=NOW - 40))
        for key, host, gpu, nonce in (
            (CPU_KEY, "dl380", False, "1" * 32),
            (GPU_KEY, "sparky", True, "2" * 32),
        ):
            _write(self.queue / "claimed" / f"{key}.json",
                   _item(key, host=host, gpu=gpu, published=NOW - 30,
                         claimed=NOW - 20, nonce=nonce))
            _write(self.queue / "claimed" / f"{key}.lease", {
                "schema": pool.POOL_LEASE_SCHEMA_V1,
                "action_key": key,
                "heartbeat_unix": NOW - 1,
            })
            _write(self.queue / "reservations" / host / "telemetry" / f"{key}.json", {
                "action_key": key,
                "nonce": nonce,
                "sampled_unix": NOW - 1,
                "complete": True,
                "cpu_seconds": 12.0 if gpu else 20.0,
                "wall_seconds": 10.0,
                "memory_current_bytes": 3 * 1024 ** 3 if gpu else 1024 ** 3,
            })
        for host in ("sparky", "dl380"):
            _write(self.queue / "reservations" / host / "adaptive" / "cpu-sample.json", {
                "sampled_unix": NOW - 1,
                "observation": {"busy_cpus": 1.0},
            })
        _write(self.queue / "reservations" / "sparky" / "adaptive" / "gpu-state.json", {
            "sampled_unix": NOW - 2,
            "power_feedback": {"status": "plateau"},
        })

        _ending(self.queue, "d4" * 32, "executed", "sparky",
                NOW - 50, NOW - 40, NOW - 10)
        _ending(self.queue, "e5" * 32, "timeout", "dl380",
                NOW - 45, NOW - 35, NOW - 5)

    def collect(self, **kwargs: object) -> str:
        return pbmetrics.collect_metrics(self.queue, now=NOW, **kwargs)

    def test_fresh_state_resources_gpu_kind_and_trusted_telemetry(self) -> None:
        text = self.collect(terminal_limit=20)

        self.assertIn('prismabuild_queue_items{state="ready"} 1', text)
        self.assertIn('prismabuild_queue_items{state="claimed"} 2', text)
        self.assertIn('prismabuild_active_jobs{host="dl380",kind="cpu"} 1', text)
        self.assertIn('prismabuild_active_jobs{host="dl380",kind="gpu"} 0', text)
        self.assertIn('prismabuild_active_jobs{host="sparky",kind="cpu"} 0', text)
        self.assertIn('prismabuild_active_jobs{host="sparky",kind="gpu"} 1', text)
        self.assertIn(
            'prismabuild_reserved_resources{host="sparky",resource="memory_bytes"} '
            '4294967296', text)
        self.assertIn(
            'prismabuild_attempt_observed_resources{host="sparky",resource="cpu"} '
            '1.2', text)
        self.assertIn(
            'prismabuild_attempt_observed_resources{host="sparky",resource="memory_bytes"} '
            '3221225472', text)
        self.assertIn('prismabuild_attempt_telemetry_jobs{host="sparky"} 1', text)

    def test_idle_fresh_worker_has_known_zero_reservations(self) -> None:
        (self.queue / "claimed" / f"{CPU_KEY}.json").unlink()
        (self.queue / "claimed" / f"{CPU_KEY}.lease").unlink()
        text = self.collect()

        self.assertIn(
            'prismabuild_reserved_resources{host="dl380",resource="cpu"} 0', text)
        self.assertIn(
            'prismabuild_reserved_resources{host="dl380",resource="memory_bytes"} 0', text)

    def test_stale_offers_have_health_but_no_capacity_or_memory_facts(self) -> None:
        text = self.collect()

        self.assertIn('prismabuild_collection_success 1', text)
        self.assertIn('prismabuild_worker_up{host="gx10-old"} 0', text)
        self.assertIn('prismabuild_worker_offer_age_seconds{host="gx10-old"}', text)
        self.assertFalse(any('host="gx10-old"' in line for line in _samples(
            text, "prismabuild_worker_capacity")))
        self.assertFalse(any('host="gx10-old"' in line for line in _samples(
            text, "prismabuild_worker_observed_capacity")))
        self.assertFalse(any('host="gx10-old"' in line for line in _samples(
            text, "prismabuild_worker_memory_available_bytes")))
        self.assertIn(
            'prismabuild_worker_memory_domain_info{domain="shared_system",host="sparky"} 1',
            text)
        self.assertIn(
            'prismabuild_worker_memory_available_bytes{host="sparky"} 31138512896', text)

    def test_admission_and_terminal_metrics_are_window_gauges(self) -> None:
        text = self.collect(terminal_window_seconds=60, terminal_limit=20)

        self.assertIn(
            'prismabuild_admission_evidence_age_seconds{host="sparky",resource="gpu"} 2',
            text)
        self.assertIn(
            'prismabuild_admission_plateau{host="sparky",resource="gpu"} 1', text)
        self.assertIn(
            'prismabuild_terminal_outcomes{host="sparky",outcome="executed"} 1', text)
        self.assertIn(
            'prismabuild_terminal_outcomes{host="dl380",outcome="timeout"} 1', text)
        self.assertIn('prismabuild_terminal_outcomes_window_seconds 60', text)
        self.assertIn('prismabuild_terminal_outcomes_window_jobs 2', text)
        self.assertIn('prismabuild_terminal_outcomes_window_complete 1', text)
        self.assertIn('prismabuild_terminal_collection_success 1', text)
        self.assertIn(
            'prismabuild_queue_wait_seconds{host="sparky",stat="mean"} 10', text)
        self.assertIn(
            'prismabuild_execution_seconds{host="sparky",stat="max"} 30', text)
        self.assertNotIn("_total", text)
        self.assertTrue(all(line.startswith("# TYPE ") and line.endswith(" gauge")
                            for line in text.splitlines() if line.startswith("# TYPE ")))

    def test_bounded_terminal_scan_marks_a_truncated_window(self) -> None:
        text = self.collect(terminal_window_seconds=60, terminal_limit=1)
        self.assertIn('prismabuild_terminal_outcomes_window_jobs 1', text)
        self.assertIn('prismabuild_terminal_outcomes_window_complete 0', text)
        self.assertIn('prismabuild_terminal_collection_success 1', text)

    def test_corrupt_active_record_is_unknown_and_collection_fails(self) -> None:
        (self.queue / "ready" / f"{READY_KEY}.json").write_text("{", encoding="utf-8")
        text = self.collect()

        self.assertNotIn('prismabuild_queue_items{state="ready"}', text)
        self.assertIn('prismabuild_queue_items{state="claimed"} 2', text)
        self.assertIn('prismabuild_collection_success 0', text)
        self.assertNotIn(CPU_KEY, text)
        self.assertNotIn(GPU_KEY, text)

    def test_missing_one_live_scope_omits_the_host_aggregate(self) -> None:
        (self.queue / "reservations" / "sparky" / "telemetry" / f"{GPU_KEY}.json").unlink()
        text = self.collect()

        self.assertFalse(any('host="sparky"' in line for line in _samples(
            text, "prismabuild_attempt_observed_resources")))
        self.assertTrue(any('host="dl380"' in line for line in _samples(
            text, "prismabuild_attempt_observed_resources")))

    def test_a_claim_that_stopped_reporting_does_not_erase_its_host(self) -> None:
        """The host in trouble must not be the host that reports nothing.

        A claim blocked on the shared mount keeps its lease -- the worker loop
        is alive and heartbeating -- while the sampler that writes its
        telemetry does not run, so its record goes stale. Withholding the
        aggregate is right; withholding it silently is what made this morning's
        diagnosis start from scratch, because a box whose claims had all
        stopped reporting looked exactly like a box with nothing running.
        """

        stale = self.queue / "reservations" / "sparky" / "telemetry" / f"{GPU_KEY}.json"
        record = json.loads(stale.read_text())
        record["sampled_unix"] = NOW - pool.cpu_admission.MAX_SAMPLE_AGE_S - 300
        _write(stale, record)

        text = self.collect()

        self.assertIn('prismabuild_attempt_telemetry_unavailable_jobs{host="sparky"} 1',
                      text)
        self.assertIn('prismabuild_attempt_telemetry_unavailable_jobs{host="dl380"} 0',
                      text)
        age = _samples(text, "prismabuild_attempt_telemetry_age_seconds")
        self.assertTrue(any('host="sparky"' in line for line in age), age)
        self.assertFalse(any('host="sparky"' in line for line in _samples(
            text, "prismabuild_attempt_observed_resources")))

    def test_a_sample_from_a_clock_slightly_ahead_is_still_fresh(self) -> None:
        """The freshest record must not be the one that gets thrown away.

        A telemetry record is stamped by the box executing the action and read
        by whichever box runs the exporter. Those are different clocks --
        dl380g10 runs milliseconds ahead of sparky -- so a record written a
        moment "in the future" is ordinary, and rejecting it inverted the test
        the check exists to make. It fell hardest on the busiest box, whose
        records are the ones most likely to be a fraction of a second old, and
        under the all-or-nothing aggregate a single such record erased its whole
        host: fourteen live claims on dl380g10 reported as unusable while every
        one of them was fresh, matched and complete on disk.
        """

        path = self.queue / "reservations" / "sparky" / "telemetry" / f"{GPU_KEY}.json"
        record = json.loads(path.read_text())
        record["sampled_unix"] = NOW + 0.02
        _write(path, record)

        text = self.collect()

        self.assertIn('prismabuild_attempt_telemetry_unavailable_jobs{host="sparky"} 0',
                      text)
        self.assertTrue(any('host="sparky"' in line for line in _samples(
            text, "prismabuild_attempt_observed_resources")))
        self.assertTrue(any('host="sparky"' in line for line in _samples(
            text, "prismabuild_attempt_telemetry_age_seconds")))

    def test_a_stamp_further_ahead_than_one_sampling_period_is_not_skew(self) -> None:
        """Tolerating skew is not tolerating a wrong clock."""

        path = self.queue / "reservations" / "sparky" / "telemetry" / f"{GPU_KEY}.json"
        record = json.loads(path.read_text())
        record["sampled_unix"] = NOW + pool.cpu_admission.MAX_SAMPLE_AGE_S + 60
        _write(path, record)

        text = self.collect()

        self.assertIn('prismabuild_attempt_telemetry_unavailable_jobs{host="sparky"} 1',
                      text)
        self.assertFalse(any('host="sparky"' in line for line in _samples(
            text, "prismabuild_attempt_observed_resources")))

    def test_recent_cores_tells_a_blocked_claim_from_a_working_one(self) -> None:
        """Lifetime average cannot see a job that has just stopped moving.

        The blocked claim here has burned 20 CPU-seconds over 10 wall-seconds
        and then advances only its wall clock, which is the shape of a worker
        waiting on a lock. Its lifetime average stays high and says nothing;
        the difference between two readings says it is doing nothing now, with
        no deadline and no threshold consulted.
        """

        store: dict = {}
        pbmetrics.collect_metrics(self.queue, now=NOW, previous=store)

        later = NOW + 4.0
        for key, host, cpu in ((CPU_KEY, "dl380", 20.0), (GPU_KEY, "sparky", 16.0)):
            path = self.queue / "reservations" / host / "telemetry" / f"{key}.json"
            record = json.loads(path.read_text())
            record.update(sampled_unix=later - 1, cpu_seconds=cpu,
                          wall_seconds=record["wall_seconds"] + 4.0)
            _write(path, record)
        text = pbmetrics.collect_metrics(self.queue, now=later, previous=store)

        self.assertIn('prismabuild_attempt_recent_cores{host="dl380"} 0', text)
        self.assertIn('prismabuild_attempt_recent_cores{host="sparky"} 1', text)
        self.assertIn('prismabuild_attempt_recent_cores_jobs{host="dl380"} 1', text)

    def test_unusable_sample_breaks_recent_cpu_coverage(self) -> None:
        path = self.queue / "reservations" / "dl380" / "telemetry" / f"{CPU_KEY}.json"
        original = json.loads(path.read_text())
        for invalid in (
            {"complete": False},
            {"sampled_unix": NOW - 60},
            {"sampled_unix": NOW + 60},
            {"nonce": "wrong-scope"},
        ):
            with self.subTest(invalid=invalid):
                _write(path, original)
                store: dict = {}
                pbmetrics.collect_metrics(self.queue, now=NOW, previous=store)
                # A failed cgroup read retains CPU but advances wall and stamp.
                fallback = dict(original, sampled_unix=NOW + 3, wall_seconds=14.0)
                fallback.update(invalid)
                _write(path, fallback)
                text = pbmetrics.collect_metrics(self.queue, now=NOW + 4, previous=store)
                for metric in ("prismabuild_attempt_recent_cores",
                               "prismabuild_attempt_recent_cores_jobs",
                               "prismabuild_attempt_observed_resources"):
                    self.assertFalse(any('host="dl380"' in line
                                         for line in _samples(text, metric)), text)
                self.assertNotIn((CPU_KEY, "1" * 32), store)
                self.assertIn('prismabuild_attempt_telemetry_unavailable_jobs{host="dl380"} 1',
                              text)
                # Recovery needs two usable samples; never bridge the gap.
                recovered = dict(original, sampled_unix=NOW + 7,
                                 wall_seconds=18.0, cpu_seconds=28.0)
                _write(path, recovered)
                text = pbmetrics.collect_metrics(self.queue, now=NOW + 8, previous=store)
                self.assertFalse(any('host="dl380"' in line for line in _samples(
                    text, "prismabuild_attempt_recent_cores")))
                recovered.update(sampled_unix=NOW + 11, wall_seconds=22.0,
                                 cpu_seconds=32.0)
                _write(path, recovered)
                text = pbmetrics.collect_metrics(self.queue, now=NOW + 12, previous=store)
                self.assertIn('prismabuild_attempt_recent_cores{host="dl380"} 1', text)
                self.assertIn('prismabuild_attempt_recent_cores_jobs{host="dl380"} 1', text)

    def test_one_moment_reports_no_rate(self) -> None:
        """A first reading has nothing to difference, and says so by absence."""

        self.assertEqual([], _samples(self.collect(), "prismabuild_attempt_recent_cores"))
        self.assertEqual(
            [], _samples(pbmetrics.collect_metrics(self.queue, now=NOW, previous={}),
                         "prismabuild_attempt_recent_cores"))

    def test_a_later_attempt_is_not_differenced_against_an_earlier_one(self) -> None:
        """An action key outlives one attempt; a cgroup counter does not.

        Reusing a key after a retry would otherwise difference the new
        attempt's counters against the old attempt's, which is a subtraction
        between two unrelated cgroups and can produce any number at all. The
        scope nonce is what makes the identity an attempt rather than an action.
        """

        store: dict = {}
        pbmetrics.collect_metrics(self.queue, now=NOW, previous=store)

        later = NOW + 4.0
        claim = self.queue / "claimed" / f"{CPU_KEY}.json"
        record = json.loads(claim.read_text())
        record["resource_scope"] = {"nonce": "9" * 32}
        _write(claim, record)
        path = self.queue / "reservations" / "dl380" / "telemetry" / f"{CPU_KEY}.json"
        telemetry = json.loads(path.read_text())
        telemetry.update(nonce="9" * 32, sampled_unix=later - 1,
                         cpu_seconds=1.0, wall_seconds=2.0)
        _write(path, telemetry)
        text = pbmetrics.collect_metrics(self.queue, now=later, previous=store)

        self.assertFalse(any('host="dl380"' in line for line in _samples(
            text, "prismabuild_attempt_recent_cores")))

    def test_prometheus_format_and_no_sensitive_labels(self) -> None:
        text = self.collect()
        metric_names = [line.split()[2] for line in text.splitlines()
                        if line.startswith("# HELP ")]

        self.assertTrue(text.endswith("\n"))
        self.assertEqual(len(metric_names), len(set(metric_names)))
        self.assertEqual(
            len(metric_names),
            sum(line.startswith("# TYPE ") for line in text.splitlines()),
        )
        self.assertNotIn("action_key=", text)
        self.assertNotIn("nonce=", text)
        self.assertNotIn("argv=", text)
        self.assertNotIn(" NaN", text)
        self.assertNotIn(" Inf", text)

    def test_terminal_failure_has_a_separate_failure_gauge(self) -> None:
        with mock.patch.object(pbmetrics.pbstatus, "read_endings", side_effect=OSError("offline")):
            text = self.collect()
        self.assertIn('prismabuild_terminal_collection_success 0', text)
        self.assertIn('prismabuild_collection_success 0', text)

    def test_missing_workers_directory_is_incomplete(self) -> None:
        for path in (self.queue / "workers").iterdir():
            path.unlink()
        (self.queue / "workers").rmdir()
        text = self.collect()
        self.assertIn('prismabuild_collection_success 0', text)

    def test_huge_malformed_number_is_omitted_without_crashing(self) -> None:
        self.assertIsNone(pbmetrics._number(10 ** 10_000))

    def test_unhashable_memory_domain_maps_to_unknown(self) -> None:
        record = json.loads((self.queue / "workers" / "sparky.json").read_text())
        record["observed_detail"]["gpu_memory_domains"] = [{"bad": "shape"}]
        _write(self.queue / "workers" / "sparky.json", record)
        text = self.collect()
        self.assertIn(
            'prismabuild_worker_memory_domain_info{domain="unknown",host="sparky"} 1',
            text)

    def test_once_prints_one_snapshot(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            status = pbmetrics.main(["--queue-root", str(self.queue), "--once"])
        self.assertEqual(status, 0)
        self.assertTrue(output.getvalue().startswith("# HELP "))
        self.assertIn('prismabuild_collection_success 1', output.getvalue())


class CacheTest(unittest.TestCase):
    def test_cache_reuses_a_snapshot_until_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            calls: list[int] = []
            with (mock.patch.object(pbmetrics.time, "monotonic",
                                    side_effect=(100.0, 105.0, 111.0)),
                  mock.patch.object(pbmetrics, "collect_metrics",
                                    side_effect=lambda *args, **kwargs:
                                    calls.append(1) or f"snapshot {len(calls)}\n")):
                cache = pbmetrics.MetricsCache(Path(directory), 10.0, 60.0, 20)
                self.assertEqual(cache.get(), "snapshot 1\n")
                self.assertEqual(cache.get(), "snapshot 1\n")
                self.assertEqual(cache.get(), "snapshot 2\n")
            self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
