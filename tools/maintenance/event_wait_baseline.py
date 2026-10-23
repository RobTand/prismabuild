#!/usr/bin/env python3
"""Read-only, PB-admitted event-wait baseline capture and deterministic analysis."""
from __future__ import annotations

import argparse
import ctypes
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import stat
import struct
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.digest_primitives import canonical_sha256, raw_sha256

EWB_SCHEMA = "prismabuild.event_wait_baseline.v1"
EWB_NAMESPACES = ("ready", "claimed", "done", "failed", "withdrawn")
EWB_KEY = re.compile(r"^[0-9a-f]{64}$")


def ewb_percentiles(values):
    """Nearest rank, including p95 for the TL acceptance floor."""
    ordered = sorted(values)
    return {f"p{p}_s": ordered[math.ceil(len(ordered) * p / 100) - 1]
            if ordered else None for p in (50, 95, 99)}


def ewb_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def ewb_analyze(raw):
    start, end = raw["started_unix"], raw["ended_unix"]
    publications, claims, drains = {}, {}, {}
    invalid = 0
    for observation in raw["records"]:
        row, namespace = observation["body"], observation["namespace"]
        key, published = row.get("action_key"), row.get("published_unix")
        if not EWB_KEY.fullmatch(str(key)) or not ewb_number(published):
            continue
        publication = (key, published)
        if namespace in ("ready", "claimed", "done", "failed", "withdrawn", "attempts"):
            entry = publications.setdefault(publication, {"tags": [], "host": None})
            if row.get("tags"):
                entry["tags"] = row["tags"]
            entry["host"] = row.get("claimed_host") or entry["host"]
            claimed = row.get("claimed_unix")
            if ewb_number(claimed):
                if claimed < published or claimed > end:
                    invalid += 1
                    continue
                # Mutable claimed rows carry the number of COMPLETED attempts.
                attempt = (row.get("attempt") if namespace == "attempts" else
                           row.get("attempts", 0) + (namespace == "claimed"))
                if type(attempt) is not int or attempt < 1:
                    continue
                identity = (*publication, attempt)
                claims.setdefault(identity, set()).add(claimed)
        if namespace == "denials":
            evidence = row.get("evidence") or {}
            verdict = evidence.get("withhold") or {}
            stamp = row.get("denied_unix")
            if not ewb_number(stamp) or not start <= stamp <= end:
                continue
            # Only explicit drain verdicts establish a relevant refusal. Early
            # adaptive diagnostics omit ineligible verdicts; do not guess them.
            if type(verdict.get("eligible")) is not bool:
                continue
            episode = (*publication, row.get("attempts", 0), row.get("host"))
            drains.setdefault(episode, {})[stamp] = {**verdict, "_observed_denials": evidence.get("denials")}
    cohort, backlog, excluded, latencies, retry_latencies, censored = 0, 0, 0, [], [], []
    conflicts = []
    for publication, metadata in sorted(publications.items()):
        key, published = publication
        if not (set(metadata["tags"]) & set(raw["x86_tags"])
                or metadata["host"] in raw["x86_hosts"]):
            excluded += 1
            continue
        if published < start:
            backlog += 1
            continue
        if published > end:
            continue
        cohort += 1
        first = claims.get((*publication, 1), set())
        if len(first) == 1:
            latencies.append(next(iter(first)) - published)
        else:
            censored.append({"action_key": key, "published_unix": published,
                             "observed_for_s": end - published,
                             "reason": "conflicting_claims" if first else "first_attempt_not_observed"})
        for identity, stamps in claims.items():
            if identity[:2] != publication:
                continue
            if len(stamps) != 1:
                conflicts.append({"identity": list(identity), "claimed_unix": sorted(stamps)})
            elif identity[2] > 1:
                # These are publication-to-RETRY claim, not fresh publication latency.
                retry_latencies.append(next(iter(stamps)) - published)
    windows, incomplete, immediate, left_censored = [], 0, 0, 0
    for episode, versions in sorted(drains.items(), key=lambda entry: repr(entry[0])):
        ordered = sorted(versions.items())
        first, verdict = ordered[0]
        if verdict["eligible"]:
            immediate += 1
            left_censored += 1
            continue
        if verdict.get("_observed_denials") != 1:
            left_censored += 1
            continue
        mature = next((stamp for stamp, value in ordered if value["eligible"]), None)
        if mature is None:
            incomplete += 1
        else:
            windows.append({"identity": list(episode), "first_refusal_unix": first,
                            "mature_unix": mature, "elapsed_s": mature - first})
    roles = {}
    for name in raw["roles"]:
        samples = [(s["monotonic"], s["roles"][name]) for s in raw["samples"]
                   if name in s["roles"] and "error" not in s["roles"][name]]
        role = raw["roles"][name]
        initial = role.get("initial")
        # Legacy captures without an initial snapshot use the first verified
        # sample. An explicitly unavailable initial identity stays unavailable.
        identity = (initial.get("start_ticks") if initial is not None else
                    samples[0][1]["start_ticks"] if samples else None)
        samples = [(t, s) for t, s in samples if identity is not None and s["start_ticks"] == identity]
        segments = []
        for (t0, a), (t1, b) in zip(samples, samples[1:]):
            if a["start_ticks"] != b["start_ticks"] or t1 <= t0:
                continue
            delta = {field: b[field] - a[field]
                     for field in ("read_bytes", "rchar", "syscr", "syscw", "cpu_ticks")}
            if min(delta.values()) < 0:
                continue
            segments.append((t1 - t0, delta))
        seconds = sum(t for t, _ in segments)
        totals = {field: sum(d[field] for _, d in segments)
                  for field in ("read_bytes", "rchar", "syscr", "syscw", "cpu_ticks")}
        roles[name] = {"covered_s": seconds, "samples": len(samples), "deltas": totals,
                       "physical_read_MB_s": totals["read_bytes"] / seconds / 1e6 if seconds else None,
                       "logical_read_MB_s": totals["rchar"] / seconds / 1e6 if seconds else None,
                       "io_syscalls_per_min": (totals["syscr"] + totals["syscw"]) * 60 / seconds if seconds else None,
                       "cpu_s_per_hour": totals["cpu_ticks"] / raw["clock_ticks"] * 3600 / seconds if seconds else None,
                       "total_syscalls_per_min": None}
    return {"schema": EWB_SCHEMA, "phase": raw["phase"], "duration_s": raw["duration_s"],
            "roles": roles, "claim_latency": {**ewb_percentiles(latencies), "samples": len(latencies),
            "window_publications": cohort, "censored": censored, "backlog_publications": backlog,
            "non_x86_or_unknown": excluded, "invalid_clock_records": invalid,
            "conflicting_attempt_claims": conflicts, "retry_publication_to_claim": {
                **ewb_percentiles(retry_latencies), "samples": len(retry_latencies)},
            "treatment": "First attempt per (action_key,published_unix); retries separate; no zero imputation."},
            "drain_maturity": {**ewb_percentiles([w["elapsed_s"] for w in windows]),
            "windows": windows, "right_censored": incomplete, "left_censored_or_immediate": left_censored,
            "eligible_at_first_observation": immediate,
            "coverage": "Explicit withhold verdicts only; latest-only diagnostics may miss refusals. No inferred window."},
            "limits_hit": raw["limits_hit"], "tracing": "Separate optional trace; proc reports read/write calls only."}


class EventWaitCapture:
    """Fixed-depth local watches; no terminal directory enumeration."""

    def __init__(self, args):
        self.args = args
        self.raw = {"schema": EWB_SCHEMA, "phase": args.phase, "roles": {}, "samples": [],
                    "records": [], "tier_cycles": [], "tier_events": [], "limits_hit": [], "errors": [],
                    "x86_tags": args.x86_tag, "x86_hosts": args.x86_host,
                    "source_revision": args.source_revision,
                    "collector_sha256": raw_sha256(Path(__file__).read_bytes()),
                    "clock_ticks": os.sysconf("SC_CLK_TCK"), "host": socket.gethostname(),
                    "bounds": {k: getattr(args, k) for k in ("max_entries", "max_keys", "max_records", "max_bytes")},
                    "load_note": args.load_note, "tree_sizes": args.tree_size,
                    "clock_assumptions": "Same publication/attempt epoch. Writer wall clocks must be synchronized; not verified. Rates use local monotonic time.",
                    "cache_warmth": "Operator phase label; no cache flush or role restart. Process uptime is retained."}
        self.bytes_read, self.keys, self.latest = 0, set(), {}
        self.selector = selectors.DefaultSelector()
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.inotify_init1.argtypes = [ctypes.c_int]
        self.libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), "inotify_init1")
        self.selector.register(self.fd, selectors.EVENT_READ)
        self.watches = {}
        for namespace in EWB_NAMESPACES:
            self.watch(args.queue / namespace, namespace)
        log_info = args.tier_log.stat() if args.tier_log else None
        self.log_offset = log_info.st_size if log_info else 0
        self.log_identity = (log_info.st_dev, log_info.st_ino) if log_info else None
        self.log_pending = b""
        self.log_unread = False

    def limit(self, text):
        if text not in self.raw["limits_hit"]:
            self.raw["limits_hit"].append(text)

    def read(self, path, cap=262144):
        if self.bytes_read + cap > self.args.max_bytes:
            self.limit("max_bytes")
            return None
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise ValueError("not a regular file")
                data = os.read(fd, cap + 1)
                self.bytes_read += len(data)
                if len(data) > cap:
                    self.limit("oversized_file:" + str(path))
                    return None
                return data
            finally:
                os.close(fd)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            message = f"{path}: {exc}"
            if message not in self.raw["errors"]:
                self.raw["errors"].append(message)
            return None

    def document(self, path):
        data = self.read(path)
        if data is None:
            return None
        try:
            body = json.loads(data)
            return body if isinstance(body, dict) else None
        except (ValueError, UnicodeError):
            self.limit("invalid_json:" + str(path))
            return None

    def watch(self, path, namespace):
        # Atomic replacement, completed in-place write, and watch loss. Watches
        # precede the bounded bootstrap; this must run on the queue's LOCAL host.
        wd = self.libc.inotify_add_watch(self.fd, os.fsencode(path), 0x8 | 0x80 | 0x400 | 0x800)
        if wd < 0:
            self.limit("watch_unavailable:" + str(path))
        else:
            self.watches[wd] = namespace

    def record_available(self):
        if len(self.raw["records"]) + len(self.raw["tier_events"]) >= self.args.max_records:
            self.limit("max_records")
            return False
        return True

    def retain(self, namespace, path, body):
        identity = (namespace, str(path))
        if self.latest.get(identity) == body:
            return
        if not self.record_available():
            return
        self.latest[identity] = body
        self.raw["records"].append({"namespace": namespace, "path": str(path), "body": body,
                                    "observed_unix": time.time()})

    def observe_key(self, key):
        if key not in self.keys and len(self.keys) >= self.args.max_keys:
            self.limit("max_keys")
            return
        self.keys.add(key)
        for namespace in EWB_NAMESPACES:
            path = self.args.queue / namespace / f"{key}.json"
            body = self.document(path)
            if body is None:
                continue
            self.retain(namespace, path, body)
            # Follow only immutable links carried by this selected publication.
            # Never list attempts/, done/, or failed/. Limit per-record links.
            links = body.get("attempt_history") or []
            if len(links) > 16:
                self.limit("attempt_history_links")
            generation = body.get("published_unix")
            if not ewb_number(generation):
                continue
            digest = canonical_sha256({"action_key": key, "published_unix": float(generation)})
            for link in links[:16]:
                attempt = link.get("attempt")
                if type(attempt) is not int or attempt < 1:
                    continue
                target = self.args.queue / "attempts" / key / digest / f"{attempt:08d}.json"
                outcome = self.document(target)
                if (outcome and outcome.get("action_key") == key
                        and outcome.get("published_unix") == generation and outcome.get("attempt") == attempt):
                    self.retain("attempts", target, outcome)

    def diagnostics(self):
        for path in self.args.denials:
            body = self.document(path) or {}
            for identity, row in (body.get("records") or {}).items():
                self.retain("denials", str(path) + ":" + identity, row)

    def census(self):
        counts = {}
        for namespace in ("ready", "claimed"):
            count, complete = 0, True
            with os.scandir(self.args.queue / namespace) as entries:
                for entry in entries:
                    count += 1
                    if count > self.args.max_entries:
                        complete = False
                        break
                    if EWB_KEY.fullmatch(entry.name.removesuffix(".json")):
                        self.observe_key(entry.name.removesuffix(".json"))
            counts[namespace] = {"entries": min(count, self.args.max_entries), "complete": complete}
            if not complete:
                self.limit("startup_entries:" + namespace)
        return counts

    def role_sample(self, pid):
        base = Path("/proc") / str(pid)
        proc_stat = self.read(base / "stat", 16384)
        io = self.read(base / "io", 16384)
        if proc_stat is None or io is None:
            return {"error": "process vanished or proc counters unavailable"}
        proc_stat = proc_stat.decode().rsplit(")", 1)[1].split()
        counters = {k: int(v) for k, v in (line.split(":") for line in io.decode().splitlines())}
        counters.update(start_ticks=int(proc_stat[19]), cpu_ticks=int(proc_stat[11]) + int(proc_stat[12]))
        group = self.read(base / "cgroup", 16384)
        counters["cgroup_membership"] = group.decode() if group else None
        counters["cgroup"] = {}
        if group:
            for line in group.decode().splitlines():
                if line.startswith("0::/"):
                    relative = Path(line[4:])
                    if ".." not in relative.parts:
                        root = Path("/sys/fs/cgroup") / relative
                        for name in ("cpu.stat", "io.stat", "memory.current", "cpu.pressure", "memory.pressure"):
                            value = self.read(root / name, 16384)
                            counters["cgroup"][name] = value.decode() if value is not None else None
        final_stat = self.read(base / "stat", 16384)
        if final_stat is None:
            return {"error": "process identity unavailable after counter read"}
        if int(final_stat.decode().rsplit(")", 1)[1].split()[19]) != counters["start_ticks"]:
            return {"error": "process identity changed during counter read"}
        return counters

    def sample(self):
        sample = {"unix": time.time(), "monotonic": time.monotonic(), "loadavg": os.getloadavg(), "roles": {}}
        for name, role in self.raw["roles"].items():
            sample["roles"][name] = self.role_sample(role["pid"])
        self.raw["samples"].append(sample)
        self.diagnostics()
        self.tier_sample()

    def tier_sample(self):
        """Tail the producer's ordered JSONL stream, not just cycle summaries."""
        if not self.args.tier_log:
            return
        info = self.args.tier_log.stat()
        identity = (info.st_dev, info.st_ino)
        if self.log_identity is not None and (identity != self.log_identity or info.st_size < self.log_offset):
            self.limit("tier_log_rotated")
            self.log_offset, self.log_pending = 0, b""
        self.log_identity = identity
        remaining = min(262144, self.args.max_bytes - self.bytes_read)
        if remaining <= 0:
            self.limit("max_bytes")
            return
        with self.args.tier_log.open("rb") as handle:
            handle.seek(self.log_offset)
            data = handle.read(remaining)
            self.log_offset = handle.tell()
            self.log_unread = os.fstat(handle.fileno()).st_size > self.log_offset
        self.bytes_read += len(data)
        lines = (self.log_pending + data).split(b"\n")
        self.log_pending = lines.pop()
        if len(self.log_pending) > 262144:
            self.limit("tier_log_line")
            self.log_pending = b""
        for line in lines:
            if len(line) > 262144:
                self.limit("tier_log_line")
                continue
            try:
                row = json.loads(line)
            except (ValueError, UnicodeError):
                self.limit("invalid_tier_log_json")
                continue
            if not isinstance(row, dict) or not isinstance(row.get("event"), str) or not row["event"]:
                self.limit("invalid_tier_log_event")
                continue
            if not self.record_available():
                continue
            # _emit writes flat policy events beside tier_cycle_line's summary.
            # Retain every event, including future policy names, in wire order.
            self.raw["tier_events"].append(row)
            if row["event"] == "tier-cycle":
                self.raw["tier_cycles"].append(row)

    def run(self):
        self.raw.update(started_unix=time.time(), started_monotonic=time.monotonic())
        self.raw["collector_cpu_start_s"] = time.process_time()
        for role in self.args.role:
            name, value = role.split("=", 1)
            pid = int(value)
            command = self.read(Path("/proc") / str(pid) / "cmdline")
            argv = command.decode().rstrip("\0").split("\0") if command is not None else []
            snapshot = self.role_sample(pid)
            self.raw["roles"][name] = {"pid": pid, "argv": argv, "initial": snapshot,
                                      "uptime_s": (time.clock_gettime(time.CLOCK_BOOTTIME) - snapshot["start_ticks"] / self.raw["clock_ticks"]
                                                   if "start_ticks" in snapshot else None),
                                      "runtime_paths": [arg for arg in argv if "/runtime-generations/" in arg]}
            if command is None:
                self.raw["roles"][name]["error"] = "initial cmdline unavailable"
        self.raw["startup_queue"] = self.census()
        self.sample()
        deadline = self.raw["started_monotonic"] + self.args.duration_s
        next_sample = time.monotonic() + self.args.sample_s
        while time.monotonic() < deadline:
            # /proc has no counter-change callback. This is its named sampling
            # deadline, not a recurring queue discovery or service poll.
            ready = self.selector.select(max(0, min(next_sample, deadline) - time.monotonic()))
            if ready:
                data = os.read(self.fd, 65536)
                offset, changed = 0, set()
                while offset < len(data):
                    wd, mask, _, length = struct.unpack_from("iIII", data, offset)
                    name = data[offset + 16:offset + 16 + length].split(b"\0", 1)[0].decode()
                    offset += 16 + length
                    if mask & (0x4000 | 0x8000 | 0x400 | 0x800):
                        self.limit("inotify_coverage_lost")
                    key = name.removesuffix(".json")
                    if EWB_KEY.fullmatch(key):
                        changed.add(key)
                for key in sorted(changed):
                    self.observe_key(key)
            if time.monotonic() >= next_sample:
                self.sample()
                next_sample = time.monotonic() + self.args.sample_s
        for key in sorted(self.keys):
            self.observe_key(key)
        self.sample()
        if self.log_pending or self.log_unread:
            self.limit("tier_log_incomplete")
        self.raw.update(ended_unix=time.time(), duration_s=time.monotonic() - self.raw["started_monotonic"],
                        bytes_read=self.bytes_read, selected_keys=len(self.keys),
                        collector_cpu_s=time.process_time() - self.raw["collector_cpu_start_s"])
        self.selector.close()
        os.close(self.fd)
        return self.raw


def ewb_trace(pid, seconds):
    """Optional privileged attachment, always separate from latency capture."""
    started = time.monotonic()
    limits, output = [], {"stdout": bytearray(), "stderr": bytearray()}
    child = None
    selector = selectors.DefaultSelector()
    try:
        child = subprocess.Popen(["strace", "-c", "-p", str(pid)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for name, pipe in (("stdout", child.stdout), ("stderr", child.stderr)):
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, name)
        deadline, stage = started + seconds, 0
        while selector.get_map() or child.poll() is None:
            now = time.monotonic()
            if now >= deadline:
                if stage == 0:
                    if child.poll() is None:
                        child.send_signal(signal.SIGINT)
                    deadline, stage = now + 5, 1
                elif stage == 1:
                    if child.poll() is None:
                        child.kill()
                    deadline, stage = now + 1, 2
                else:
                    limits.append("trace_teardown")
                    break
            for key, _ in selector.select(max(0, min(.1, deadline - time.monotonic()))):
                data = os.read(key.fileobj.fileno(), 8192)
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                remaining = 65536 - sum(len(value) for value in output.values())
                output[key.data].extend(data[:remaining])
                if len(data) > remaining and "trace_output_bytes" not in limits:
                    limits.append("trace_output_bytes")
        # Never wait on a pipe held by a descendant, or signal the observed PID.
        try:
            child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            if "trace_teardown" not in limits:
                limits.append("trace_teardown")
        duration = time.monotonic() - started
        # Dropping incomplete UTF-8 avoids replacement characters expanding the
        # retained byte budget. Truncation is explicitly not a syscall sample.
        stdout, stderr = (bytes(output[name]).decode(errors="ignore") for name in ("stdout", "stderr"))
        total = next((line.split() for line in stderr.splitlines() if line.endswith(" total")), None)
        return {"pid": pid, "duration_s": duration, "returncode": child.returncode,
                "stdout": stdout, "stderr": stderr, "limits_hit": limits,
                "syscalls_per_min": int(total[3]) * 60 / duration if total and not limits else None}
    except OSError as exc:
        return {"pid": pid, "unavailable": str(exc), "syscalls_per_min": None}
    finally:
        selector.close()
        if child is not None:
            if child.poll() is None:
                child.kill()
                try:
                    child.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
            child.stdout.close()
            child.stderr.close()


def ewb_compare(left, right):
    """Exact ordered event/accounting comparison; only log emission time is volatile."""
    unavailable = []
    for name, raw in (("left", left), ("right", right)):
        events = raw.get("tier_events", [])
        if not any(row.get("event") != "tier-cycle" for row in events):
            unavailable.append(f"{name}: no non-summary tier events (legacy or summary-only capture)")
        if not raw.get("tier_cycles"):
            unavailable.append(f"{name}: no tier-cycle accounting")
        if raw.get("limits_hit") or raw.get("errors"):
            unavailable.append(f"{name}: incomplete capture (limits or read/watch errors)")
    normalized = lambda raw: [{k: v for k, v in row.items() if k != "unix"}
                              for row in raw.get("tier_events", [])]
    a, b = normalized(left), normalized(right)
    return {"equal": not unavailable and a == b,
            "left_cycles": len(left.get("tier_cycles", [])), "right_cycles": len(right.get("tier_cycles", [])),
            "left_events": len(a), "right_events": len(b), "unavailable": unavailable,
            "normalization": "Only top-level unix; inputs must be an identical recorded/replayed experiment."}


def ewb_cli():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("collect")
    capture.add_argument("--queue", type=Path, required=True, help='Local queue root; run on its filesystem server.')
    capture.add_argument("--role", action="append", required=True, metavar="NAME=PID", help='Explicit role name and current PID; repeat for each process.')
    capture.add_argument("--denials", type=Path, action="append", default=[], help='Latest-only claim-denials JSON path; repeat for each host.')
    capture.add_argument("--tier-log", type=Path, help='Existing tier-role JSONL log; capture new lines only.')
    capture.add_argument("--phase", choices=("cold-start", "steady"), required=True, help='Operator label; never restarts roles or flushes caches.')
    capture.add_argument("--duration-s", type=float, default=300, help='Collection deadline in seconds, at most 3600.')
    capture.add_argument("--sample-s", type=float, default=1, help='Proc and diagnostic sample interval in seconds.')
    capture.add_argument("--load-note", required=True, help='Matched workload and cache-state description.')
    capture.add_argument("--source-revision", required=True, help='Checkout revision supplying this collector.')
    capture.add_argument("--tree-size", action="append", default=[], help="LABEL=COUNT:SOURCE, externally known sizes; never scanned")
    capture.add_argument("--x86-tag", action="append", default=["x86"], help='Additional tag identifying the measured x86 cohort.')
    capture.add_argument("--x86-host", action="append", default=[], help='Claimed hostname identifying otherwise untagged x86 rows.')
    capture.add_argument("--max-entries", type=int, default=512, help='Startup entry limit per ready/claimed directory.')
    capture.add_argument("--max-keys", type=int, default=2048, help='Maximum distinct selected action keys.')
    capture.add_argument("--max-records", type=int, default=20000, help='Maximum retained queue record versions plus tier-log events.')
    capture.add_argument("--max-bytes", type=int, default=64 * 1024 * 1024, help='Total source-read byte budget.')
    capture.add_argument("--output", type=Path, required=True, help='New evidence JSON destination outside the queue.')
    analyze = commands.add_parser("analyze")
    analyze.add_argument("raw", type=Path, help='Retained raw baseline JSON.')
    compare = commands.add_parser("compare")
    compare.add_argument("left", type=Path, help='First identical-input tier-event capture (not summary-only).')
    compare.add_argument("right", type=Path, help='Second identical-input tier-event capture (not summary-only).')
    trace = commands.add_parser("trace")
    trace.add_argument("--pid", type=int, required=True, help='Existing role PID to attach the optional tracer to.')
    trace.add_argument("--seconds", type=float, default=5, help='Separate trace duration in seconds, at most 10.')
    trace.add_argument("--output", type=Path, required=True, help='New evidence JSON destination outside the queue.')
    args = parser.parse_args()
    if args.command == "collect":
        if not (0 < args.duration_s <= 3600 and 0.1 <= args.sample_s <= args.duration_s):
            parser.error("duration must be in (0,3600], sample in [0.1,duration]")
        if min(args.max_entries, args.max_keys, args.max_records, args.max_bytes) < 1:
            parser.error("read bounds must be positive")
        raw = EventWaitCapture(args).run()
        args.output.write_text(json.dumps(raw, indent=2) + "\n")
        result = ewb_analyze(raw)
    elif args.command == "trace":
        if not 0 < args.seconds <= 10:
            parser.error("trace must be in (0,10] seconds")
        result = ewb_trace(args.pid, args.seconds)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        def load_evidence(path):
            with path.open("rb") as handle:
                content = handle.read(128 * 1024 * 1024 + 1)
            if len(content) > 128 * 1024 * 1024:
                parser.error("evidence exceeds 128 MiB")
            raw = json.loads(content)
            if raw.get("schema") != EWB_SCHEMA:
                parser.error("unsupported baseline schema")
            return raw
        result = (ewb_analyze(load_evidence(args.raw)) if args.command == "analyze" else
                  ewb_compare(load_evidence(args.left), load_evidence(args.right)))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    ewb_cli()
