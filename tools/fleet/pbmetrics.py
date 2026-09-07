#!/usr/bin/env python3
"""Export the pull queue's read-only operational state as Prometheus text."""
from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import math
import os
from pathlib import Path
import re
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
import pbstatus  # noqa: E402

pool = pbstatus.pool

DEFAULT_QUEUE_ROOT = pbstatus.DEFAULT_QUEUE_ROOT
DEFAULT_LISTEN = "127.0.0.1"
DEFAULT_PORT = 9469
DEFAULT_CACHE_SECONDS = 10.0
DEFAULT_TERMINAL_WINDOW_SECONDS = 3600.0
DEFAULT_TERMINAL_LIMIT = 500
GIB = 1024 ** 3
#: How far ahead of this reader another box's clock may be before its sample
#: stops being credible. A telemetry record is stamped by the box executing the
#: action and read by whichever box runs the exporter, so their clocks are not
#: the same clock: dl380g10's runs milliseconds ahead of sparky's, and a record
#: written "in the future" was being rejected as unusable. Rejecting the
#: freshest record is the wrong way round -- and it fell hardest on the busiest
#: box, whose records are the ones most likely to be seconds old rather than
#: minutes. The bound is the sampler's own period rather than a new constant:
#: a stamp further ahead than one sampling interval is not skew, it is wrong.
_SKEW_S = pool.cpu_admission.MAX_SAMPLE_AGE_S
HOST = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
OUTCOMES = frozenset({
    "executed", "cache_hit", "failed", "timeout", "withdrawn", "reset",
    "finish_lost_race", "unreadable", "unknown",
})
#: What ``_file_superseded`` names an unstarted-claim release: the action key,
#: the epoch second it was filed at, and the kind.  The timestamp is IN the
#: name, so the window can be applied without stat'ing a record the scan is
#: about to discard -- the same reason ``_ending_paths`` stats entries rather
#: than reading them.
RELEASE_FILING = re.compile(
    r"\A(?P<key>[0-9a-f]{64})\.(?P<when>[0-9]+\.[0-9]{6})\.unstarted-claim\.json\Z")

RESOURCE_NAMES = {
    "cpu": ("cpu", 1),
    "gpu": ("gpu", 1),
    "mem_gb": ("memory_bytes", GIB),
    "gpu_mem_gb": ("gpu_memory_bytes", GIB),
    "gpu_memory_gb": ("gpu_memory_bytes", GIB),
}


def _number(value: object) -> float | None:
    if type(value) not in (int, float):
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) and result >= 0 else None


def _host(value: object) -> str | None:
    text = str(value or "")
    return text if HOST.fullmatch(text) else None


def _labels(values: Mapping[str, str]) -> str:
    if not values:
        return ""
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
    return "{" + ",".join(
        f'{key}="{escape(str(value))}"' for key, value in sorted(values.items())
    ) + "}"


def _value(value: float) -> str:
    return str(int(value)) if value.is_integer() else format(value, ".15g")


@dataclass
class Family:
    name: str
    help: str
    samples: list[tuple[dict[str, str], float]] = field(default_factory=list)

    def add(self, value: object, **labels: str) -> None:
        number = _number(value)
        if number is not None:
            self.samples.append((dict(labels), number))

    def render(self) -> list[str]:
        help_text = self.help.replace("\\", "\\\\").replace("\n", "\\n")
        lines = [f"# HELP {self.name} {help_text}", f"# TYPE {self.name} gauge"]
        lines.extend(
            f"{self.name}{_labels(labels)} {_value(value)}"
            for labels, value in sorted(
                self.samples, key=lambda sample: tuple(sorted(sample[0].items()))
            )
        )
        return lines


class Metrics:
    def __init__(self) -> None:
        self.families: dict[str, Family] = {}

    def family(self, name: str, help_text: str) -> Family:
        family = self.families.get(name)
        if family is None:
            family = self.families[name] = Family(name, help_text)
        return family

    def render(self) -> str:
        lines: list[str] = []
        for name in sorted(self.families):
            lines.extend(self.families[name].render())
        return "\n".join(lines) + "\n"


def _resource_samples(values: object) -> list[tuple[str, float]]:
    if not isinstance(values, Mapping):
        return []
    result: list[tuple[str, float]] = []
    for source, value in values.items():
        mapping = RESOURCE_NAMES.get(str(source))
        number = _number(value)
        if mapping is not None and number is not None:
            result.append((mapping[0], number * mapping[1]))
    return result


def _read_claim(queue: pool.PoolQueue, key: str) -> dict | None:
    try:
        return pool._read_json(queue.item_path(pool.CLAIMED, key))
    except (OSError, ValueError):
        return None


@dataclass
class _HostAttempts:
    """What one host's live claims reported, including what they did not.

    The aggregate is deliberately all-or-nothing, but the counts beside it are
    not: a host whose aggregate is withheld still says how many live claims it
    has and how stale the oldest of them is, so "nothing running here" and
    "the claim that would have told you is the one that stopped reporting" stop
    rendering identically.
    """

    cpu: float = 0.0
    memory_bytes: float = 0.0
    jobs: int = 0
    unavailable: int = 0
    oldest_age_s: float | None = None
    #: ``(action_key, nonce) -> (cpu_seconds, wall_seconds)`` for the claims
    #: whose counters were readable, whether or not the aggregate qualified.
    counters: dict[tuple[str, str], tuple[float, float]] = field(default_factory=dict)

    @property
    def complete(self) -> bool:
        return self.jobs > 0 and self.unavailable == 0


def _attempt_telemetry(
    queue: pool.PoolQueue,
    live_jobs: Mapping[str, list[Mapping[str, object]]],
    now: float,
) -> dict[str, _HostAttempts]:
    """Read each live claim's resource-scope telemetry, by claiming host.

    The aggregate a host publishes stays all-or-nothing: a partial sum reads as
    a low whole-host total, which is worse than no number. What changed is that
    withholding it is no longer silent. Every live claim is examined rather than
    abandoning the host at the first unusable record, and the count that could
    not be used is reported beside the count that could.

    That distinction is the whole point. A claim blocked on the shared mount
    keeps its lease -- the loop is alive and heartbeating -- while the sampler
    that writes its telemetry does not run, so its record goes stale. Under the
    old early exit that claim erased its host from the observation entirely, and
    the box in the most trouble was the box that reported nothing. The signal
    was strongest exactly where it was being deleted.

    Raw counters come back too, per claim, because the ratio the aggregate
    carries is a lifetime average: a job that ran hot for fifty minutes and has
    been blocked for ten still averages high. Differencing consecutive readings
    is what tells those apart, and that is done by the caller, which is the only
    party that has a previous reading.
    """
    answer: dict[str, _HostAttempts] = {}
    for host, jobs in live_jobs.items():
        host_attempts = _HostAttempts(jobs=len(jobs))
        for job in jobs:
            key = str(job.get("action_key") or "")
            claim = _read_claim(queue, key)
            scope = claim.get("resource_scope") if isinstance(claim, dict) else None
            nonce = scope.get("nonce") if isinstance(scope, dict) else None
            try:
                record = pool._read_json(
                    queue.ledger(host).base / "telemetry" / f"{key}.json"
                )
            except (OSError, ValueError):
                record = None
            sampled = _number(record.get("sampled_unix")) if isinstance(record, dict) else None
            cpu_seconds = _number(record.get("cpu_seconds")) if isinstance(record, dict) else None
            wall_seconds = _number(record.get("wall_seconds")) if isinstance(record, dict) else None
            current = _number(record.get("memory_current_bytes")) if isinstance(record, dict) else None
            # Age is reported for any record whose timestamp is credible, even
            # one too old for the aggregate -- that is precisely the reading a
            # reader wants when the aggregate is missing.
            if sampled is not None and now - sampled >= -_SKEW_S:
                # Floored at zero: a stamp from a clock a few milliseconds ahead
                # is as new as this reader can tell, and a negative age is not a
                # reading anyone can act on. The tolerance above is what decides
                # whether the sample counts; this only decides how it reads.
                age = max(0.0, now - sampled)
                if (host_attempts.oldest_age_s is None
                        or age > host_attempts.oldest_age_s):
                    host_attempts.oldest_age_s = age
            matched = (isinstance(record, dict) and record.get("action_key") == key
                       and bool(nonce) and record.get("nonce") == nonce)
            if (matched and cpu_seconds is not None and wall_seconds is not None
                    and wall_seconds > 0):
                host_attempts.counters[(key, str(nonce))] = (cpu_seconds, wall_seconds)
            if (not isinstance(record, dict) or record.get("complete") is not True
                    or not matched or sampled is None
                    or not -_SKEW_S <= now - sampled <= pool.cpu_admission.MAX_SAMPLE_AGE_S
                    or cpu_seconds is None or wall_seconds is None or wall_seconds <= 0
                    or current is None):
                host_attempts.unavailable += 1
                continue
            host_attempts.cpu += cpu_seconds / wall_seconds
            host_attempts.memory_bytes += current
        answer[host] = host_attempts
    return answer


def _recent_cores(
    observations: Mapping[str, _HostAttempts],
    previous: dict[tuple[str, str], tuple[float, float]] | None,
) -> dict[str, tuple[float, int]]:
    """Cores used since the previous reading of the same claim, by host.

    The exporter carries no counter of its own, by design -- one would reset on
    every restart and lie about a rate. This is not that: the counters belong to
    the attempt's cgroup and are differenced only against a previous reading of
    *that same attempt*, identified by action key and scope nonce together, so a
    key reused by a later attempt cannot be differenced against an earlier one.

    The first refresh after a start has nothing to difference and reports
    nothing, which is the honest answer to "what has been happening" when the
    answer is "I have seen one moment". ``previous`` is replaced in place with
    the current reading, and only for claims that are live now, so it cannot
    grow with the queue's history.
    """
    answer: dict[str, tuple[float, int]] = {}
    if previous is None:
        return answer
    current: dict[tuple[str, str], tuple[float, float]] = {}
    for host, host_attempts in observations.items():
        cores = 0.0
        covered = 0
        for identity, (cpu_seconds, wall_seconds) in host_attempts.counters.items():
            current[identity] = (cpu_seconds, wall_seconds)
            before = previous.get(identity)
            if before is None:
                continue
            cpu_delta = cpu_seconds - before[0]
            wall_delta = wall_seconds - before[1]
            # A counter that went backwards is not this attempt's; a wall that
            # did not advance gives no rate. Neither is an error worth a false
            # zero, so the claim simply is not covered this refresh.
            if wall_delta <= 0 or cpu_delta < 0:
                continue
            cores += cpu_delta / wall_delta
            covered += 1
        if covered:
            answer[host] = (cores, covered)
    previous.clear()
    previous.update(current)
    return answer


def _terminal_metrics(
    metrics: Metrics,
    queue_root: Path,
    *,
    now: float,
    window_seconds: float,
    limit: int,
) -> bool:
    accessible = True
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        try:
            with os.scandir(queue_root / state):
                pass
        except OSError:
            accessible = False
    endings = pbstatus.read_endings(queue_root, limit=limit)
    selected = [
        row for row in endings
        if (age := _number(now - float(row.get("finished_unix", -math.inf)))) is not None
        and age <= window_seconds
    ]
    complete = accessible and len(endings) < limit
    metrics.family(
        "prismabuild_terminal_outcomes_window_seconds",
        "Configured lookback window for recent terminal outcome gauges.",
    ).add(window_seconds)
    metrics.family(
        "prismabuild_terminal_outcomes_window_jobs",
        "Terminal records included in the current bounded lookback window.",
    ).add(len(selected))
    metrics.family(
        "prismabuild_terminal_outcomes_window_complete",
        "Whether the terminal record scan proved the configured window was not truncated by its record limit.",
    ).add(1 if complete else 0)
    terminal_success = metrics.family(
        "prismabuild_terminal_collection_success",
        "Whether terminal directories and every selected terminal record were readable for this snapshot.",
    )

    outcomes: dict[tuple[str, str], int] = defaultdict(int)
    timings: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in selected:
        host = _host(row.get("host")) or "unknown"
        outcome = str(row.get("status") or "unknown")
        outcome = outcome if outcome in OUTCOMES else "unknown"
        outcomes[(host, outcome)] += 1
        if row.get("unreadable"):
            continue
        try:
            record = pool._read_json(Path(str(row["path"])))
        except (OSError, ValueError):
            record = None
        if not isinstance(record, dict):
            continue
        published = _number(record.get("published_unix"))
        claimed = _number(record.get("claimed_unix"))
        finished = _number(record.get("finished_unix"))
        if published is not None and claimed is not None and claimed >= published:
            timings[(host, "queue_wait")].append(claimed - published)
        if claimed is not None and finished is not None and finished >= claimed:
            timings[(host, "execution")].append(finished - claimed)

    outcomes_family = metrics.family(
        "prismabuild_terminal_outcomes",
        "Terminal outcomes observed in the bounded recent window; this is a restart-safe gauge, not a counter.",
    )
    for (host, outcome), count in outcomes.items():
        outcomes_family.add(count, host=host, outcome=outcome)

    queue_timing = metrics.family(
        "prismabuild_queue_wait_seconds",
        "Mean or maximum publish-to-claim time among terminal records with usable timestamps in the bounded window.",
    )
    execution_timing = metrics.family(
        "prismabuild_execution_seconds",
        "Mean or maximum claim-to-finish time among terminal records with usable timestamps in the bounded window.",
    )
    timing_jobs = metrics.family(
        "prismabuild_terminal_timing_window_jobs",
        "Terminal records contributing to each timing gauge in the bounded window.",
    )
    for (host, phase), values in timings.items():
        target = queue_timing if phase == "queue_wait" else execution_timing
        target.add(sum(values) / len(values), host=host, stat="mean")
        target.add(max(values), host=host, stat="max")
        timing_jobs.add(len(values), host=host, phase=phase)
    readable = accessible and all(not row.get("unreadable") for row in selected)
    terminal_success.add(1 if readable else 0)
    return readable


def _release_metrics(
    metrics: Metrics,
    queue_root: Path,
    *,
    now: float,
    window_seconds: float,
    limit: int,
) -> bool:
    """Count the unstarted-claim releases each box produced in the window.

    Issue #263.  ``reap_stale`` releases a claim whose lease never appeared
    and whose attempt was never published: the action returns to ``ready``
    unrunnable-through-no-fault-of-its-own, uncharged, and counted.  A rising
    release rate on ONE box is the signal, because the window it comes out of
    -- between the claim rename and the first lease write -- is filesystem
    latency on that box, measured at 45 s against a 30 s grace (issue #222).

    The box is not on the requeued item.  ``_shape_as_ready_item`` pops every
    claim-scoped field, ``claimed_host`` included, so by the time the counter
    is readable in ``ready`` the record no longer says who was holding it.
    The filing under ``withdrawn/superseded/`` does: ``_file_superseded`` is
    called with the claimed record, before the pops.  So the per-box series is
    read there and the queue-wide one from the census.

    Bounded exactly as ``_terminal_metrics`` is, and for the same reason: this
    directory keeps every release this fleet has ever filed, and a scrape that
    reads all of them to report a handful is the slow path.  ``complete`` says
    whether the window was proved rather than truncated.
    """

    directory = Path(queue_root) / pool.WITHDRAWN / "superseded"
    selected: list[tuple[float, str]] = []
    readable = True
    try:
        with os.scandir(directory) as scan:
            for entry in scan:
                match = RELEASE_FILING.match(entry.name)
                if match is None:
                    continue
                age = now - float(match.group("when"))
                if -_SKEW_S <= age <= window_seconds:
                    selected.append((float(match.group("when")), entry.path))
    except FileNotFoundError:
        pass                     # a fleet that has never released a claim
    except OSError:
        readable = False
    selected.sort(reverse=True)
    complete = readable and len(selected) <= limit

    counts: dict[str, int] = defaultdict(int)
    for _, path in selected[:limit]:
        try:
            record = pool._read_json(Path(path))
        except (OSError, ValueError):
            record = None
        if not isinstance(record, dict):
            readable = False
            continue
        owner = str(record.get("claimed_by") or "").split(":", 1)[0]
        counts[_host(record.get("claimed_host")) or _host(owner) or "unknown"] += 1

    events = metrics.family(
        "prismabuild_unstarted_release_events",
        "Claims released without an attempt in the bounded recent window, by the box that held the claim; this is a restart-safe gauge, not a counter.",
    )
    for host, count in counts.items():
        events.add(count, host=host)
    metrics.family(
        "prismabuild_unstarted_release_events_complete",
        "Whether the unstarted-release scan proved the configured window was not truncated by its record limit and every selected filing was readable.",
    ).add(1 if complete and readable else 0)
    return readable


def collect_metrics(
    queue_root: str | Path = DEFAULT_QUEUE_ROOT,
    *,
    now: float | None = None,
    terminal_window_seconds: float = DEFAULT_TERMINAL_WINDOW_SECONDS,
    terminal_limit: int = DEFAULT_TERMINAL_LIMIT,
    previous: dict[tuple[str, str], tuple[float, float]] | None = None,
) -> str:
    """Collect one non-atomic, read-only snapshot in Prometheus text format.

    ``previous`` is the caller's store of the last refresh's per-attempt
    counters, replaced in place. Passing it opts into the recent-cores rate;
    omitting it -- which ``--once`` does, having only one moment to report --
    simply leaves that family out. The store belongs to the caller rather than
    to this module so that two collectors cannot silently difference against
    each other's readings.
    """
    sampled = time.time() if now is None else float(now)
    root = Path(queue_root).absolute()
    metrics = Metrics()
    success = True
    try:
        census = pbstatus.read_pool(root)
    except Exception:  # A scrape reports the failure rather than dropping HTTP.
        census = {"nodes": [], "jobs": [], "queue": {"ready": None, "claimed": None}}
        success = False
    try:
        with os.scandir(root / pool.WORKERS):
            pass
    except OSError:
        success = False

    queue_summary = census.get("queue", {})
    queue_items = metrics.family(
        "prismabuild_queue_items", "Readable pull-queue records by active state.")
    oldest = metrics.family(
        "prismabuild_queue_oldest_age_seconds",
        "Age of the oldest readable record in each active queue state; zero means that readable state is empty.",
    )
    jobs = census.get("jobs", []) if isinstance(census.get("jobs"), list) else []
    releases = metrics.family(
        "prismabuild_queue_unstarted_releases",
        "Releases without an attempt carried by readable records now in each active queue state; this counts what the queue is still holding, not what happened in a window.",
    )
    for state in ("ready", "claimed"):
        count = queue_summary.get(state) if isinstance(queue_summary, Mapping) else None
        if _number(count) is None:
            success = False
            continue
        queue_items.add(count, state=state)
        state_jobs = [row for row in jobs if row.get("state") == state.upper()]
        releases.add(sum(int(row.get("unstarted_releases") or 0) for row in state_jobs),
                     state=state)
        if not state_jobs:
            oldest.add(0, state=state)
        else:
            ages = [_number(row.get("age_s")) for row in state_jobs]
            if all(age is not None for age in ages):
                oldest.add(max(ages), state=state)
            else:
                success = False

    worker_up = metrics.family(
        "prismabuild_worker_up",
        "Whether a syntactically valid worker offer is fresh enough for pool placement.",
    )
    offer_age = metrics.family(
        "prismabuild_worker_offer_age_seconds", "Age of a valid worker's last offer.")
    capacity = metrics.family(
        "prismabuild_worker_capacity",
        "Configured fresh-worker capacity; cpu is cores, gpu is devices, and memory resources are bytes.",
    )
    observed_capacity = metrics.family(
        "prismabuild_worker_observed_capacity",
        "Windowed fresh-worker capacity after foreign work; units follow worker capacity.",
    )
    memory_domain = metrics.family(
        "prismabuild_worker_memory_domain_info",
        "Fresh worker GPU memory domains; value is always one.",
    )
    memory_available = metrics.family(
        "prismabuild_worker_memory_available_bytes",
        "Coarse available host memory from the fresh worker offer, converted from integer GiB.",
    )
    loops = metrics.family(
        "prismabuild_worker_loops",
        "PrismaBuild worker loops running on the box that wrote this fresh offer; "
        "absent, not zero, when the offer did not measure it.",
    )
    evidence_age = metrics.family(
        "prismabuild_admission_evidence_age_seconds",
        "Age of the last persisted admission evidence; this is not continuous hardware monitoring.",
    )
    plateau = metrics.family(
        "prismabuild_admission_plateau",
        "Whether the last persisted GPU admission decision recorded a power-response plateau; this is not live hardware state.",
    )

    valid_hosts: set[str] = set()
    fresh_hosts: set[str] = set()
    resource_dimensions: dict[str, set[str]] = defaultdict(set)
    for node in census.get("nodes", []):
        host = _host(node.get("node"))
        state = node.get("state")
        if host is None or state not in {"live", "stale"}:
            success = False
            continue
        valid_hosts.add(host)
        fresh = state == "live"
        worker_up.add(1 if fresh else 0, host=host)
        age = _number(node.get("age_s"))
        if age is not None:
            offer_age.add(age, host=host)
        else:
            success = False
        if fresh:
            fresh_hosts.add(host)
            for resource, value in _resource_samples(node.get("capacity")):
                capacity.add(value, host=host, resource=resource)
                resource_dimensions[host].add(resource)
            for resource, value in _resource_samples(node.get("observed_capacity")):
                observed_capacity.add(value, host=host, resource=resource)
            detail = node.get("observed_detail")
            if isinstance(detail, Mapping):
                domains = detail.get("gpu_memory_domains")
                if isinstance(domains, list):
                    normalized = {
                        domain if isinstance(domain, str) and domain in {
                            "shared_system", "discrete", "unknown"
                        } else "unknown"
                        for domain in domains
                    }
                    for domain in sorted(normalized):
                        memory_domain.add(1, host=host, domain=domain)
                available_gib = _number(detail.get("mem_available_gb"))
                if available_gib is not None:
                    memory_available.add(available_gib * GIB, host=host)
            # A pre-#254 offer carries no count.  That is a missing series and
            # not a zero one, and it is emphatically not a scrape failure:
            # every loop published before the field existed announces without
            # it, so setting ``success = False`` here would mark the whole
            # scrape bad for the duration of a rolling generation change.
            count = node.get("loops")
            if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
                loops.add(count, host=host)
        admission = node.get("admission")
        if isinstance(admission, Mapping):
            for resource in ("cpu", "gpu"):
                sample = admission.get(resource)
                if not isinstance(sample, Mapping) or sample.get("state") == "not applicable":
                    continue
                age = _number(sample.get("age_s"))
                if age is not None:
                    evidence_age.add(age, host=host, resource=resource)
                record = sample.get("record")
                if resource == "gpu" and isinstance(record, Mapping):
                    feedback = record.get("power_feedback")
                    is_plateau = isinstance(feedback, Mapping) and feedback.get("status") == "plateau"
                    plateau.add(1 if is_plateau else 0, host=host, resource="gpu")

    claimed_known = _number(queue_summary.get("claimed")) is not None
    active = metrics.family(
        "prismabuild_active_jobs",
        "Claims with fresh leases by host and mutually exclusive cpu or gpu job kind.",
    )
    reserved = metrics.family(
        "prismabuild_reserved_resources",
        "Declared resources in valid claimed queue records; units follow worker capacity.",
    )
    active_by_host: dict[str, dict[str, int]] = defaultdict(lambda: {"cpu": 0, "gpu": 0})
    reserved_by_host: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    live_jobs: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    unknown_active_hosts: set[str] = set()
    if claimed_known:
        for job in jobs:
            if job.get("state") != "CLAIMED":
                continue
            host = _host(job.get("node"))
            if host is None:
                success = False
                continue
            valid_hosts.add(host)
            for resource, value in _resource_samples(job.get("resources")):
                reserved_by_host[host][resource] += value
            if job.get("stale") is False:
                kind = "gpu" if dict(job.get("resources") or {}).get("gpu", 0) else "cpu"
                active_by_host[host][kind] += 1
                live_jobs[host].append(job)
            else:
                unknown_active_hosts.add(host)
        reported_hosts = fresh_hosts | set(active_by_host) | set(reserved_by_host)
        for host in sorted(reported_hosts):
            dimensions = resource_dimensions[host] | set(reserved_by_host[host])
            for resource in sorted(dimensions):
                reserved.add(reserved_by_host[host].get(resource, 0),
                             host=host, resource=resource)
            if host not in unknown_active_hosts:
                for kind in ("cpu", "gpu"):
                    active.add(active_by_host[host][kind], host=host, kind=kind)

    # A claim retained because its cleanup could not be proved is retried by a
    # local reaper forever, deliberately: concluding it would release tokens
    # for a payload nobody showed had stopped.  What that costs is a claim
    # nobody is looking at, so the retry loop has to be able to raise its hand
    # (#288).  Two aggregate series, not one per action: an alert asks "is
    # anything stuck and for how long", and an action key is unbounded
    # cardinality.
    pinned = metrics.family(
        "prismabuild_cleanup_pending_claims",
        "Claims on the host retained because their payload could not be proved stopped; the reservation is still held.",
    )
    pinned_age = metrics.family(
        "prismabuild_cleanup_pending_oldest_seconds",
        "Age of the oldest unproven cleanup on the host, from its first failure; absent when nothing is pending.",
    )
    if claimed_known:
        pinned_by_host: dict[str, int] = defaultdict(int)
        oldest_by_host: dict[str, float] = {}
        for job in jobs:
            if job.get("state") != "CLAIMED" or not job.get("cleanup_pending"):
                continue
            host = _host(job.get("node"))
            if host is None:
                continue
            pinned_by_host[host] += 1
            age = _number(job.get("cleanup_pending_s"))
            # A record written before #288 carries no first-failure stamp.
            # That is a missing series, never a zero one: reporting 0 would
            # say "just started" about a cleanup that may have been pending
            # for hours.
            if age is not None:
                oldest_by_host[host] = max(oldest_by_host.get(host, age), age)
        for host in sorted(fresh_hosts | set(pinned_by_host)):
            pinned.add(pinned_by_host.get(host, 0), host=host)
            if host in oldest_by_host:
                pinned_age.add(oldest_by_host[host], host=host)

    observations = _attempt_telemetry(pool.PoolQueue(root), live_jobs, sampled)
    observed = metrics.family(
        "prismabuild_attempt_observed_resources",
        "Aggregate complete fresh exact-scope observations for every live claim on a host; cpu is lifetime-average cores and memory is current bytes.",
    )
    observed_jobs = metrics.family(
        "prismabuild_attempt_telemetry_jobs",
        "Live claims represented in the host's complete aggregate attempt telemetry.",
    )
    unavailable_jobs = metrics.family(
        "prismabuild_attempt_telemetry_unavailable_jobs",
        "Live claims on the host whose resource-scope telemetry could not be used; the aggregate is withheld whenever this is above zero.",
    )
    telemetry_age = metrics.family(
        "prismabuild_attempt_telemetry_age_seconds",
        "Age of the oldest credible resource-scope sample among the host's live claims; reported even when the aggregate is withheld.",
    )
    recent = metrics.family(
        "prismabuild_attempt_recent_cores",
        "Cores used by the host's live claims since the previous refresh, differenced per attempt; absent until a second refresh has something to difference.",
    )
    recent_jobs = metrics.family(
        "prismabuild_attempt_recent_cores_jobs",
        "Live claims the host's recent-cores figure was differenced over; it is the denominator, not a total.",
    )
    for host, values in sorted(observations.items()):
        # The count of live claims and how stale the oldest is are reported for
        # every host that has any, so a withheld aggregate is legible as a
        # withheld aggregate rather than as an absent host.
        unavailable_jobs.add(values.unavailable, host=host)
        if values.oldest_age_s is not None:
            telemetry_age.add(values.oldest_age_s, host=host)
        if values.complete:
            observed.add(values.cpu, host=host, resource="cpu")
            observed.add(values.memory_bytes, host=host, resource="memory_bytes")
            observed_jobs.add(float(values.jobs), host=host)
    for host, (cores, covered) in sorted(_recent_cores(observations, previous).items()):
        recent.add(cores, host=host)
        recent_jobs.add(float(covered), host=host)

    try:
        success = _terminal_metrics(
            metrics, root, now=sampled,
            window_seconds=terminal_window_seconds, limit=terminal_limit,
        ) and success
    except Exception:
        success = False
        metrics.family(
            "prismabuild_terminal_collection_success",
            "Whether terminal directories and every selected terminal record were readable for this snapshot.",
        ).add(0)

    try:
        success = _release_metrics(
            metrics, root, now=sampled,
            window_seconds=terminal_window_seconds, limit=terminal_limit,
        ) and success
    except Exception:  # A scrape reports the failure rather than dropping HTTP.
        success = False
        metrics.family(
            "prismabuild_unstarted_release_events_complete",
            "Whether the unstarted-release scan proved the configured window was not truncated by its record limit and every selected filing was readable.",
        ).add(0)

    metrics.family(
        "prismabuild_collection_success",
        "Whether critical queue inputs and selected terminal records were read and validated for this snapshot.",
    ).add(1 if success else 0)
    return metrics.render()


class MetricsCache:
    def __init__(self, queue_root: Path, cache_seconds: float,
                 terminal_window_seconds: float, terminal_limit: int) -> None:
        self.queue_root = queue_root
        self.cache_seconds = cache_seconds
        self.terminal_window_seconds = terminal_window_seconds
        self.terminal_limit = terminal_limit
        # One store per collector, so the rate is differenced only against
        # this collector's own previous reading.
        self.previous: dict[tuple[str, str], tuple[float, float]] = {}
        self._lock = threading.Lock()
        self._expires = 0.0
        self._text = ""

    def get(self) -> str:
        with self._lock:
            now = time.monotonic()
            if self._text and now < self._expires:
                return self._text
            self._text = collect_metrics(
                self.queue_root,
                terminal_window_seconds=self.terminal_window_seconds,
                terminal_limit=self.terminal_limit,
                previous=self.previous,
            )
            self._expires = now + self.cache_seconds
            return self._text


def _handler(cache: MetricsCache) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path.split("?", 1)[0] != "/metrics":
                self.send_error(404)
                return
            body = cache.get().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--queue-root", type=Path, default=DEFAULT_QUEUE_ROOT,
                        help="shared pull-queue directory to observe without writing")
    result.add_argument("--listen", default=DEFAULT_LISTEN,
                        help="HTTP bind address (default: loopback only)")
    result.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="HTTP port serving /metrics")
    result.add_argument("--cache-seconds", type=float, default=DEFAULT_CACHE_SECONDS,
                        help="reuse a snapshot for this many seconds between scrapes")
    result.add_argument("--terminal-window-seconds", type=float,
                        default=DEFAULT_TERMINAL_WINDOW_SECONDS,
                        help="lookback interval for retained terminal outcome gauges")
    result.add_argument("--terminal-limit", type=int, default=DEFAULT_TERMINAL_LIMIT,
                        help="maximum newest terminal records read in one snapshot")
    result.add_argument("--once", action="store_true",
                        help="print one Prometheus snapshot and exit")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser().error("--port must be between 1 and 65535")
    if not math.isfinite(args.cache_seconds) or args.cache_seconds < 0:
        parser().error("--cache-seconds must be finite and nonnegative")
    if (not math.isfinite(args.terminal_window_seconds)
            or args.terminal_window_seconds <= 0):
        parser().error("--terminal-window-seconds must be positive and finite")
    if args.terminal_limit <= 0:
        parser().error("--terminal-limit must be positive")
    if args.once:
        sys.stdout.write(collect_metrics(
            args.queue_root,
            terminal_window_seconds=args.terminal_window_seconds,
            terminal_limit=args.terminal_limit,
        ))
        return 0
    cache = MetricsCache(
        args.queue_root.absolute(), args.cache_seconds,
        args.terminal_window_seconds, args.terminal_limit,
    )
    server = ThreadingHTTPServer((args.listen, args.port), _handler(cache))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
