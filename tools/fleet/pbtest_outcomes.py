"""Every counted test outcome of a pbtest shard, by node ID, sealed into its argv.

A shard's terminal summary says ``231 passed, 6 skipped`` and nothing else:
not which six, not why.  A skip certifies nothing unless its reason is on
record, and pytest's ``-rs`` folds skips by location and drops their node IDs
(#942).  So the shard runs pytest through this module, which records each
report pytest's own terminal counts, and prints the record as one line,
``pbtest-outcomes: {json}``, where the pool stores the shard's stdout.
``pbtest.py`` reads it back into the receipt, and reconciles each shard's
outcomes with its collection by node ID (:func:`reconcile`, #941).

What is recorded is what the summary line counts, and it is classified the
way the terminal classifies it: a test report takes the category
``pytest_report_teststatus`` gives it, and a collection report that failed or
skipped counts as an ``error`` or a ``skipped``, exactly as
``TerminalReporter.pytest_collectreport`` files it.  That last rule is why a
summary can count more outcomes than a ``--collect-only`` pass counts items:
a module that skips at import (``pytest.importorskip``) is one ``skipped`` in
the summary and no item in the collection.

The module is loaded in the *target* interpreter, which is not this
repository's: it uses the standard library and pytest only, and pytest only
inside :func:`main`, so ``pbtest.py`` can import :func:`parse` under an
interpreter that has no pytest at all.  The recorder is handed to
``pytest.main`` as an object, not named with ``-p``: under pytest-xdist every
``-p`` plugin is imported again in each worker, where this module does not
exist, while an object plugin stays in the controller, which is where xdist
delivers every worker's reports.
"""
from __future__ import annotations

import json
import math
from prismabuild.digest_primitives import raw_sha256
import os
import posixpath
import re
import secrets
from pathlib import Path
import sys
import tempfile
import threading
import time

#: The version of the record's shape.  A reader refuses any other.
SCHEMA = "prismabuild.pbtest_outcomes.v1"
#: The one line a shard prints its record on.
PREFIX = "pbtest-outcomes: "
#: Diagnostic events survive an interrupted session; they are never outcomes.
TRACE_OPTION = "--pbtest-trace"
TRACE_PREFIX = "pbtest-trace: "
TRACE_SCHEMA = "prismabuild.pbtest_trace.v1"
TRACE_NODEID_MAX_BYTES = 4096
COMPLETION_PREFIX = "pbtest-completion: "
COMPLETION_SCHEMA = "prismabuild.pbtest_completion.v1"
RECORD_BINDING_VERSION = 1
RECORD_BINDING_ENV = "PRISMABUILD_PBTEST_RECORD_BINDING"


def record_binding_for_action(action):
    """Producer declaration from the validated action, never its stdout."""
    value = action["environment"]["variables"].get(RECORD_BINDING_ENV)
    return RECORD_BINDING_VERSION if value == str(RECORD_BINDING_VERSION) else value

# xdist forwards selected IDs but neither successful collection reports nor
# pytest_deselected to its controller. This tiny worker plugin returns only
# actual deselected node IDs through xdist's own workeroutput channel.
XDIST_ROSTER_PLUGIN = '''\
_deselected = []
_collect_seen = []

def pytest_deselected(items):
    _deselected.extend(item.nodeid for item in items)

def pytest_collectreport(report):
    # xdist's controller keeps one collection report per distinct longrepr, so
    # two modules that skip through one shared helper look identical and the
    # second is dropped (#1220).  The worker sees every report.
    if report.failed or report.skipped:
        _collect_seen.append(report.nodeid)

def pytest_sessionfinish(session, exitstatus):
    output = getattr(session.config, "workeroutput", None)
    if output is not None:
        output["pbtest_deselected"] = _deselected
        output["pbtest_collect_seen"] = _collect_seen
        selection = session.config.pluginmanager.getplugin("pbtest-file-selection")
        if selection is not None:
            output["pbtest_file_selection"] = selection.report()
'''

# This plugin runs in the test process, including each xdist worker. Reports
# travel over pytest/xdist's existing channel to the controller's trace writer.
RESOURCE_TRACE_PLUGIN = '''\
import os
import resource
import time
import pytest
from pbtest_resource_scope import read_process_io

# A test may legitimately patch Path.read_text or a stat reader and assert it
# only ever sees its own fake pid. This plugin samples the real worker inside
# pytest's report hooks while such a patch is live, so its procfs reads must
# not dispatch through any global a test can patch (#1550): bind the os calls
# here, at plugin load, before any test code runs.
_OPEN, _READ, _CLOSE = os.open, os.read, os.close
_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)

def pinned_read_text(path):
    fd = _OPEN(path, _FLAGS)
    try:
        chunks = []
        while True:
            chunk = _READ(fd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        _CLOSE(fd)
    return b"".join(chunks).decode("utf-8", "replace")

def pytest_addoption(parser):
    parser.addoption("--pbtest-trace", action="store_true", default=False,
                     help="flush diagnostic test/process resource samples")

def sample():
    result = {"pid": os.getpid(), "sampled_unix": time.time(),
              "scope": "test-process-and-reaped-children",
              "process_io": None, "rss_bytes": None,
              "max_rss_watermark_bytes": None, "errors": []}
    try:
        row = read_process_io(os.getpid(), read_text=pinned_read_text)
        if row is None or row[2] is None:
            raise ValueError("process I/O unavailable")
        result["identity"], _, result["process_io"] = row
    except (OSError, ValueError) as exc:
        result["errors"].append(str(exc))
    try:
        status = pinned_read_text("/proc/self/status")
        rss = next(line.split()[1:] for line in status.splitlines()
                   if line.startswith("VmRSS:"))
        if len(rss) != 2 or rss[1] != "kB":
            raise ValueError("unknown VmRSS units")
        result["rss_bytes"] = int(rss[0]) * 1024
    except (OSError, ValueError, StopIteration) as exc:
        result["errors"].append("RSS unavailable: " + str(exc))
    result["max_rss_watermark_bytes"] = (
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)
    return result

@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_setup(item):
    item._pbtest_resource_before = sample()
    yield

@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_call(item):
    item._pbtest_resource_before = sample()
    yield

@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_teardown(item):
    item._pbtest_resource_before = sample()
    yield

@pytest.hookimpl(hookwrapper=True, trylast=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    after = sample()
    before = getattr(item, "_pbtest_resource_before", None)
    delta = None
    if (before is not None and before.get("identity") == after.get("identity")
            and before["process_io"] is not None and after["process_io"] is not None):
        candidate = {k: v - before["process_io"][k]
                     for k, v in after["process_io"].items()}
        if all(v >= 0 for v in candidate.values()):
            delta = candidate
        else:
            after["errors"].append("process I/O counter regressed")
    outcome.get_result().pbtest_resources = {
        "before": before, "after": after, "process_io_delta": delta}
'''


def source_file(nodeid: str, rootdir: str) -> str:
    """The repository-relative file a node ID belongs to.

    The same normalization ``pbtest.py`` reconciles with: the node ID's file
    part joined onto the record's ``rootdir_relative``, so a duration sample
    from one run keys the same file in the next run's packing (#1246).
    """

    return posixpath.normpath(posixpath.join(
        rootdir or ".", nodeid.split("::", 1)[0]))


def skip_reason(report) -> str:
    """The reason a skipped report gives, as pytest's ``-rs`` would print it."""

    longrepr = report.longrepr
    reason = (str(longrepr[2]) if isinstance(longrepr, tuple) and len(longrepr) == 3
              else str(longrepr or ""))
    if reason.startswith("Skipped: "):
        return reason[len("Skipped: "):]
    return "" if reason == "Skipped" else reason


def skip_location(report, start: Path | None = None) -> str | None:
    """``path:line`` of the skip as ``-rs`` prints it, or ``None``.

    pytest stores the line already 1-based in a skip's ``longrepr``: from
    the raising frame for an imperative or collection skip, and from the
    test's own definition for a marker.  The path is absolute there; like
    ``-rs``, this prints it relative to ``start`` when it lies under it, so
    a shard's location does not name the worker's private checkout.
    """

    longrepr = report.longrepr
    if not (isinstance(longrepr, tuple) and len(longrepr) == 3):
        return None
    path, lineno, _ = longrepr
    shown = Path(str(path))
    if start is not None:
        try:
            shown = shown.relative_to(start)
        except ValueError:
            pass
    return f"{shown}:{lineno}" if isinstance(lineno, int) else str(shown)


#: The fields of one ``reports`` row, in order.  ``reason`` is the skip reason
#: for a ``skipped`` row and the xfail reason for an ``xfailed`` or ``xpassed``
#: one; ``location`` is where a skip was raised.  Both are ``None`` otherwise.
REPORT_FIELDS = ("nodeid", "when", "category", "reason", "location")


def parse(output: str, *, record_binding: int | None = None) -> dict | None:
    """The last outcome record in a shard's output, or ``None``.

    ``None`` means the shard printed no final outcomes. Exit hooks can block
    this record after the selected tests finish. Completion evidence uses a
    separate record and cannot replace final outcomes.
    """

    return _parse_record(output, PREFIX, SCHEMA, record_binding=record_binding)


def _parse_record(output: str, prefix: str, schema: str, *,
                  record_binding: int | None = None) -> dict | None:
    record = next(_record_candidates(output, prefix, record_binding=record_binding), None)
    return record if isinstance(record, dict) and record.get("schema") == schema else None


def _record_candidates(output: str, prefix: str, *, record_binding: int | None = None):
    # Only invocation-owned metadata can select bound mode. In legacy mode,
    # retain the original last-marker behavior even with nested preflights.
    run_nonce = None
    if record_binding is not None:
        if type(record_binding) is not int or record_binding != RECORD_BINDING_VERSION:
            return
        decision, _ = _preflight(output)
        run_nonce = decision.get("run_nonce") if decision is not None else None
        if not isinstance(run_nonce, str) or re.fullmatch(r"[0-9a-f]{64}", run_nonce) is None:
            return
    for line in reversed((output or "").splitlines()):
        if not line.startswith(prefix):
            continue
        try:
            record = json.loads(line[len(prefix):])
        except ValueError:
            record = None
        if record_binding is not None and (
                not isinstance(record, dict) or record.get("run_nonce") != run_nonce):
            continue
        yield record


def completion(output: str, *, record_binding: int | None = None) -> dict:
    """Verify selected-test completion, not final outcomes or process exit."""
    return _completion_evidence(_parse_record(
        output, COMPLETION_PREFIX, COMPLETION_SCHEMA, record_binding=record_binding))


def _completion_evidence(record: dict | None) -> dict:
    if record is None:
        return {"status": "unknown", "problems": ["no valid test completion record"]}
    lists = ("collected", "teardown_finished", "outcome_nodeids")
    flags = ("collection_complete", "collect_only", "collection_errors",
             "duplicate_teardown")
    if (any(not isinstance(record.get(key), list)
            or any(not isinstance(nodeid, str) or not nodeid
                   for nodeid in record[key]) for key in lists)
            or any(type(record.get(key)) is not bool for key in flags)):
        return {"status": "unknown", "problems": ["invalid test completion record"]}
    collected = record["collected"]
    selected = set(collected)
    finished = set(record["teardown_finished"])
    outcomes = set(record["outcome_nodeids"])
    missing = sorted(selected - finished)
    missing_outcomes = sorted(selected - outcomes)
    unexpected = sorted((finished | outcomes) - selected)
    problems = []
    if not record["collection_complete"]:
        problems.append("collection is incomplete or inconsistent")
    if record["collect_only"] or not collected:
        problems.append("no selected test execution")
    if record["collection_errors"]:
        problems.append("collection errors leave the population incomplete")
    if len(selected) != len(collected):
        problems.append("duplicate collected node IDs")
    if record["duplicate_teardown"]:
        problems.append("duplicate teardown reports")
    if missing:
        problems.append("selected tests have no completed teardown")
    if missing_outcomes:
        problems.append("selected tests have no observed outcome")
    if unexpected:
        problems.append("reports name tests outside the selected population")
    return {**record, "status": "incomplete" if problems else "complete",
            "missing_teardown": missing, "missing_outcomes": missing_outcomes,
            "unexpected_nodeids": unexpected, "problems": problems}


def reconcile(record: dict, counts: dict[str, int] | None,
              collected_count: int | None = None) -> dict:
    """Match one shard's outcomes against its own collection, by node ID (#941).

    ``counts`` is the shard's summary line by category (``error`` singular,
    warnings and deselections left out), and ``collected_count`` is the
    count a ``--collect-only`` summary states instead.  The answer names:

    * ``never_ran``: collected, and no outcome.  A missing test.
    * ``not_collected``: an outcome for a node the collection did not hold.
    * ``collected_twice``: one node ID collected twice in this session.
    * ``at_collection``: outcomes of collectors, not tests -- a module that
      skipped at import or failed to import.  The summary counts each; a
      ``--collect-only`` pass counts none, which is how the two differ
      without any test running twice.
    * ``extra_phases``: tests the summary counts more than once, such as a
      pass whose teardown then errors or skips.

    ``problems`` holds every finding that makes the shard unreconciled; the
    last two lists are how a summary exceeds the tests, and are not problems.
    """

    rows = [dict(zip(REPORT_FIELDS, row)) for row in record.get("reports") or ()]
    collect_only = bool(record.get("collect_only"))
    collected = list(record.get("collected") or ())
    at_collection = [{"nodeid": row["nodeid"], "category": row["category"],
                      "reason": row["reason"]}
                     for row in rows if row["when"] == "collect"]
    phases: dict[str, list[str]] = {}
    for row in rows:
        if row["when"] != "collect":
            phases.setdefault(row["nodeid"], []).append(
                f"{row['when']}:{row['category']}")
    ran = set(phases) | {row[0] for row in record.get("uncounted") or ()}
    seen: dict[str, int] = {}
    for nodeid in collected:
        seen[nodeid] = seen.get(nodeid, 0) + 1
    never_ran = [] if collect_only else [
        nodeid for nodeid in seen if nodeid not in ran]
    not_collected = sorted(ran - set(seen))
    collected_twice = sorted(nodeid for nodeid, count in seen.items() if count > 1)
    record_counts: dict[str, int] = {}
    for row in rows:
        record_counts[row["category"]] = record_counts.get(row["category"], 0) + 1

    problems = []
    if never_ran:
        problems.append(f"{len(never_ran)} collected test(s) never ran")
    if not_collected:
        problems.append(f"{len(not_collected)} outcome(s) for tests the "
                        "collection did not hold")
    if collected_twice:
        problems.append(f"{len(collected_twice)} node ID(s) collected twice")
    if collect_only:
        if collected_count is not None and collected_count != len(collected):
            problems.append(f"the summary states {collected_count} collected "
                            f"and the record holds {len(collected)}")
    elif counts is not None and counts != record_counts:
        problems.append(f"the summary counts {counts} and the record "
                        f"holds {record_counts}")
    return {
        "collect_only": collect_only,
        "collected": len(collected),
        "ran": len(set(seen) & ran),
        "outcomes": len(rows),
        "at_collection": at_collection,
        "extra_phases": {nodeid: kinds for nodeid, kinds in phases.items()
                         if len(kinds) > 1},
        "never_ran": never_ran,
        "not_collected": not_collected,
        "collected_twice": collected_twice,
        "summary_counts": counts,
        "record_counts": record_counts,
        "problems": problems,
    }


from prismabuild.pbtest_collection import (
    checkout_nodeid, ignored_named_paths, node_group_spec, partition_nodes,
)


_COUNTED = r"\d+ (?:subtests? )?[A-Za-z][\w-]*"
_COLLECT_ONLY = (r"no tests collected(?: \(\d+ deselected\))?"
                 r"|\d+/\d+ tests collected \(\d+ deselected\)"
                 r"|\d+ tests? collected")
_PARTS = rf"(?:no tests ran|{_COLLECT_ONLY}|{_COUNTED})(?:, {_COUNTED})*"
_DURATION = r"\d+\.\d+s(?: \([^)]*\))?"
PYTEST_SUMMARY = re.compile(rf"^(?:=+ )?{_PARTS} in {_DURATION}(?: =+)?$")
#: Colour is off down a pipe, but ``FORCE_COLOR`` in a shard's environment
#: would wrap the line in escapes and make it unmatchable.
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def pytest_summary(lines: list[str]) -> str:
    """The last line that is pytest's terminal summary, or ``""``."""

    for line in reversed(lines):
        if PYTEST_SUMMARY.match(ANSI.sub("", line).strip()):
            return line.strip()
    return ""



NOT_OUTCOMES = {"warning", "warnings", "deselected"}
_COLLECTED_COUNT = re.compile(r"^(?:no tests|(\d+)(?:/\d+)? tests?) collected\b")


def summary_outcomes(summary: str) -> tuple[dict[str, int] | None, int | None]:
    """A summary line's outcome counts by category, and its collected count.

    Outcomes are keyed the way the outcome record keys them: pytest prints
    ``N error``/``N errors`` for the ``error`` category, and every other
    counted word is the category itself.  A ``--collect-only`` summary
    counts items, not outcomes, and answers ``(None, N)``.
    """

    line = ANSI.sub("", summary).strip()
    line = re.sub(r"^=+ | =+$", "", line)
    body = line.rsplit(" in ", 1)[0]
    collected = _COLLECTED_COUNT.match(body)
    if collected:
        return None, int(collected.group(1) or 0)
    counts: dict[str, int] = {}
    for part in body.split(", "):
        match = re.fullmatch(r"(\d+) (.+)", part)
        if match is None or match.group(2) in NOT_OUTCOMES:
            continue
        word = "error" if match.group(2) == "errors" else match.group(2)
        counts[word] = counts.get(word, 0) + int(match.group(1))
    return counts, None



def recorded_skips(record: dict | None) -> list[dict] | None:
    """Every skip a shard's record holds, with its reason; ``None`` without one."""

    if record is None:
        return None
    skips = []
    for row in record.get("reports") or ():
        entry = dict(zip(REPORT_FIELDS, row))
        if entry.get("category") == "skipped":
            skips.append({"nodeid": entry["nodeid"], "when": entry["when"],
                          "reason": entry.get("reason") or "",
                          "location": entry.get("location")})
    return skips



VERDICT_SCHEMA = "prismabuild.pbtest_shard_verdict.v1"
PREFLIGHT_SCHEMA = "prismabuild.pbtest_preflight.v1"
PREFLIGHT_PREFIX = "pbtest-preflight: "
PROGRESS_SCHEMA = "prismabuild.pbtest_progress.v1"
CONTROL_ENV = ("PRISMABUILD_PBTEST_RUN_ID", "PRISMABUILD_PBTEST_ENTRYPOINT")


def _valid_runtime_refusal(value):
    return (isinstance(value, dict) and set(value) == {"code", "message"}
            and value["code"] in ("dependency_pin", "interpreter_absent")
            and isinstance(value["message"], str) and bool(value["message"]))


def preflight_record(exit_code: int, refusal: dict | None, *,
                     run_nonce: str | None = None) -> str:
    record = {"schema": PREFLIGHT_SCHEMA,
        "status": "refused" if exit_code else "accepted", "exit_code": exit_code,
        "runtime_refusal": refusal}
    if run_nonce is not None:
        record["run_nonce"] = run_nonce
    return PREFLIGHT_PREFIX + json.dumps(record, separators=(",", ":"))


def _preflight(output):
    for line in output.splitlines():
        if not line.startswith(PREFLIGHT_PREFIX):
            continue
        try:
            value = json.loads(line[len(PREFLIGHT_PREFIX):])
            code = value["exit_code"]
            refusal = value["runtime_refusal"]
            if (value.get("schema") == PREFLIGHT_SCHEMA and type(code) is int
                    and code >= 0 and value.get("status") == ("refused" if code else "accepted")
                    and (refusal is None or code > 0 and _valid_runtime_refusal(refusal))):
                return value, None
        except (ValueError, KeyError, TypeError):
            pass
        return None, "invalid first preflight decision"
    return None, None


def progress_path(report: Path) -> Path:
    return Path(str(report) + ".progress.json")


def read_progress(path: Path, *, run_id: str, launch_entrypoint: str,
                  generation: str) -> dict | None:
    try:
        value = json.loads(Path(path).read_text())
        if (not isinstance(value, dict) or value.get("schema") != PROGRESS_SCHEMA
                or value.get("run_id") != run_id or value.get("launch_entrypoint") != launch_entrypoint
                or value.get("producer_generation") != generation
                or value.get("state") not in ("running", "finished", "refused")
                or any(type(value.get(k)) is not int or value[k] < 0
                       for k in ("sequence", "total_shards", "completed_shards", "producer_pid"))
                or value["completed_shards"] > value["total_shards"]
                or not isinstance(value.get("shards"), list)
                or len(value["shards"]) != value["total_shards"]
                or value.get("runtime_refusal") is not None and not _valid_runtime_refusal(value["runtime_refusal"])):
            return None
        shards = value["shards"]
        if any(not isinstance(s, dict) or s.get("shard") != i
               or s.get("status") not in ("pending", "submitted", "completed")
               or type(s.get("attempts")) is not int or s["attempts"] < 0
               or s.get("returncode") is not None and type(s["returncode"]) is not int
               for i, s in enumerate(shards)):
            return None
        if sum(s["status"] == "completed" for s in shards) != value["completed_shards"]:
            return None
        if value["state"] == "finished" and (value["completed_shards"] != value["total_shards"]
                or not isinstance(value.get("report_sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", value["report_sha256"])):
            return None
        if value["state"] == "refused" and (value["completed_shards"]
                or any(s["status"] != "pending" for s in shards)):
            return None
        return value
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _valid_record(record):
    if not isinstance(record, dict):
        return False
    if any(not isinstance(record.get(key, []), list)
           or any(not isinstance(n, str) or not n for n in record.get(key, []))
           for key in ("collected", "deselected", "collect_seen")):
        return False
    for key, size in (("reports", len(REPORT_FIELDS)), ("uncounted", 3)):
        rows = record.get(key, [])
        if not isinstance(rows, list) or any(not isinstance(row, list) or len(row) != size
                or any(not isinstance(v, str) for v in row[:3]) for row in rows):
            return False
    if "bound_failures" in record:
        failures = record["bound_failures"]
        failed_phases = {(r[0], r[1]) for r in record.get("reports", [])
                         if r[2] in ("failed", "error")}
        if not isinstance(failures, list):
            return False
        seen = set()
        for failure in failures:
            if (not isinstance(failure, dict)
                    or not isinstance(failure.get("nodeid"), str)
                    or failure.get("when") not in ("setup", "call", "teardown")
                    or failure.get("kind") not in ("per_test_bound", "shard_budget_exhausted")):
                return False
            phase = (failure["nodeid"], failure["when"])
            if phase not in failed_phases or phase in seen:
                return False
            seen.add(phase)
    if "first_tightened_test_budget_remaining_s" in record:
        remaining = record["first_tightened_test_budget_remaining_s"]
        if type(remaining) not in (int, float) or not math.isfinite(remaining):
            return False
    return True


def _judge_row(row, index):
    files = row.get("files", []) if isinstance(row, dict) else []
    if not isinstance(files, list) or any(not isinstance(f, str) or not f for f in files):
        files = []
    verdict = {"schema": VERDICT_SCHEMA, "shard": index, "green": False,
               "failing_nodes": [], "bound_failures": None,
               "unobserved_files": [], "problems": [],
               "runtime_refusal": None, "reconciliation": None, "collected": [],
               "coverage": "unknown", "summary": "", "ran": False, "skipped": None,
               "completion": {"status": "unknown", "problems": ["no valid test completion record"]}}
    def problem(code, message, *, unknown=True, nodeids=()):
        verdict["problems"].append({"code": code, "message": message,
                                    "files": list(files), "nodeids": list(nodeids)})
        if unknown:
            verdict["unobserved_files"].extend(files)
    if (not isinstance(row, dict) or files != row.get("files") or not files
            or not isinstance(row.get("output"), str) or type(row.get("returncode")) is not int
            or type(row.get("timed_out", False)) is not bool):
        problem("report_invalid", "invalid shard observation")
        return verdict
    groups = {}
    if "node_groups" in row:
        try:
            groups = node_group_spec(row["node_groups"], files)
        except ValueError as exc:
            problem("node_groups_invalid", str(exc))
    verdict["shard"] = row.get("shard", index)
    record_binding = row.get("record_binding")
    if record_binding is not None and (
            type(record_binding) is not int or record_binding != RECORD_BINDING_VERSION):
        problem("record_binding_invalid", "unsupported shard record binding")
        return verdict
    output = row["output"]
    summary = pytest_summary(output.splitlines())
    verdict.update(summary=summary, ran=bool(summary),
                   completion=completion(output, record_binding=record_binding))
    decision, preflight_problem = _preflight(output)
    if preflight_problem:
        problem("preflight_invalid", preflight_problem)
    record = parse(output, record_binding=record_binding)
    if record is None:
        problem("outcomes_missing", "no outcome record: the shard's tests cannot be matched against its collection")
        if summary:
            verdict["reconciliation"] = {"problems": [verdict["problems"][-1]["message"]]}
        if decision is not None:
            verdict["runtime_refusal"] = decision["runtime_refusal"]
    elif not _valid_record(record):
        problem("outcomes_invalid", "invalid test outcome record")
        record = None
    if not summary:
        problem("summary_missing", "no pytest terminal summary")
    if record is not None:
        verdict["bound_failures"] = record.get("bound_failures")
        if "first_tightened_test_budget_remaining_s" in record:
            verdict["first_tightened_test_budget_remaining_s"] = record[
                "first_tightened_test_budget_remaining_s"]
        verdict["collected"] = record.get("collected", [])
        verdict["skipped"] = recorded_skips(record)
        reports = [dict(zip(REPORT_FIELDS, r)) for r in record.get("reports", [])]
        failed = [r["nodeid"] for r in reports if r["category"] in ("failed", "error")]
        verdict["failing_nodes"].extend(failed)
        counts, count = summary_outcomes(summary) if summary else (None, None)
        rec = reconcile(record, counts, count)
        rootdir = record.get("rootdir_relative", ".")
        rootdir = rootdir if isinstance(rootdir, str) else ""
        observed = {source_file(n, rootdir) for n in (
            *record.get("collected", []), *record.get("deselected", []),
            *record.get("collect_seen", []), *(r["nodeid"] for r in reports if r["when"] == "collect"))}
        if "file_selection" in record:
            selection = record["file_selection"]
            fields = {"files", "ignored"} | ({"node_groups", "node_rosters"} if groups else set())
            if (not isinstance(selection, dict) or set(selection) != fields
                    or selection["files"] != files or not isinstance(selection["ignored"], list)
                    or any(not isinstance(f, str) or f not in files for f in selection["ignored"])
                    or selection["ignored"] != sorted(set(selection["ignored"]))
                    or set(selection["ignored"]) & observed or record.get("file_selection_error")):
                rec["problems"].append("invalid or inconsistent file selection evidence")
            else:
                observed.update(selection["ignored"])
                if groups and selection["node_groups"] != groups:
                    rec["problems"].append("node group selection differs from its declaration")
        elif groups:
            rec["problems"].append("node group selection evidence is missing")
        rec["missing_files"] = sorted(set(files) - observed)
        if rec["missing_files"]:
            rec["problems"].append("assigned file(s) had no collection or outcome: " + ", ".join(rec["missing_files"]))
        verdict["reconciliation"] = rec
        verdict["failing_nodes"].extend(n + " (never ran)" for n in rec["never_ran"])
        for message in rec["problems"]:
            problem("reconciliation", message)
        verdict["coverage"] = ("collection_only" if record.get("collect_only") else
                               "executed" if verdict["collected"] else "excluded")
    complete = verdict["completion"]
    if any(True for _ in _record_candidates(output, COMPLETION_PREFIX,
                                           record_binding=record_binding)):
        for message in complete["problems"]:
            if message == "no selected test execution" and ((record or {}).get("collect_only") or not verdict["collected"]):
                continue
            if ((record or {}).get("collect_only") and complete.get("collect_only")
                    and message in ("selected tests have no completed teardown", "selected tests have no observed outcome")
                    and not complete["teardown_finished"] and not complete["outcome_nodeids"]):
                continue
            if verdict["failing_nodes"] and complete.get("collection_errors") and message in (
                    "collection errors leave the population incomplete", "collection is incomplete or inconsistent"):
                continue
            if (message == "selected tests have no completed teardown" and record is not None
                    and set(complete["missing_teardown"]).issubset(
                        r[0] for r in record.get("reports", [])
                        if r[1:3] == ["???", "failed"])):
                # xdist's synthetic crash report is a real terminal failure,
                # even though the dead worker cannot finish its teardown.
                continue
            problem("completion", message)
        if record is not None and "collected" in complete:
            if sorted(complete["collected"]) != sorted(verdict["collected"]):
                problem("completion_population", "completion and final selected populations differ")
            final_nodes = {r[0] for r in (*record.get("reports", []), *record.get("uncounted", [])) if r[1] != "collect"}
            if final_nodes != set(complete["outcome_nodeids"]):
                problem("completion_outcomes", "completion and final outcome populations differ")
    if row.get("timed_out"):
        problem("client_timeout", "shard client timed out")
    if row["returncode"]:
        ordinary_failure = row["returncode"] in (1, 2) and bool(verdict["failing_nodes"])
        problem("client_exit", f"shard client exited {row['returncode']}", unknown=not ordinary_failure)
    if verdict["failing_nodes"]:
        problem("test_failure", "test failures or unexecuted collected tests", unknown=False,
                nodeids=verdict["failing_nodes"])
    verdict["unobserved_files"] = sorted(set(verdict["unobserved_files"]))
    if verdict["unobserved_files"]:
        verdict["coverage"] = "unknown"
    verdict["green"] = not verdict["problems"] and not verdict["runtime_refusal"]
    return verdict


def _node_group_assignment(rows, shards, filename, indices):
    """Require every sealed group once and one agreed current population."""
    declarations = []
    reference = None
    for index in indices:
        row = rows[index]
        try:
            groups = node_group_spec(row.get("node_groups"), row["files"])
        except ValueError:
            return "grouped file has an invalid or missing group declaration"
        if filename not in groups:
            return "grouped file also has an ungrouped owner"
        group = groups[filename]
        if group["count"] != len(indices):
            return "node group indices are incomplete or repeated"
        if not isinstance(row.get("output"), str):
            return "node group outcome evidence is missing"
        record = parse(row["output"], record_binding=row.get("record_binding"))
        if record is None or not _valid_record(record):
            return "node group outcome evidence is invalid"
        rootdir = record.get("rootdir_relative", ".")
        if not isinstance(rootdir, str):
            return "node group rootdir evidence is invalid"
        view = record.get("file_selection")
        if not isinstance(view, dict) or view.get("node_groups") != groups:
            return "node group evidence differs from its sealed declaration"
        rosters = view.get("node_rosters")
        if not isinstance(rosters, dict) or set(rosters) != set(groups):
            return "node group population evidence is missing"
        roster = rosters[filename]
        if (not isinstance(roster, list) or any(not isinstance(node, str) or "\0" in node
                or node.split("::", 1)[0] != filename for node in roster)
                or roster != sorted(set(roster))):
            return "node group population evidence is invalid"
        signature = (group["count"], group["durations"], roster)
        if reference is None:
            reference = signature
        elif signature != reference:
            return "node groups disagree on the current population or duration model"
        expected = partition_nodes(roster, group["count"], group["durations"])[group["index"]]
        actual = [checkout_nodeid(node, rootdir)
                  for node in shards[index]["collected"]
                  if source_file(node, rootdir) == filename]
        if sorted(actual) != sorted(expected):
            return "node group's selected population differs from its assigned partition"
        declarations.append(group["index"])
    if reference is None or sorted(declarations) != list(range(reference[0])):
        return "node group indices are incomplete or repeated"
    return None


def judge_shards(rows, *, requested_files, client_returncode=0, required_nodes=None):
    """Judge raw shard evidence against the caller's independent population."""
    requested = set(requested_files)
    shards = []
    problems = []
    unknown = set()
    assignments = {}
    owners = {}
    def problem(code, message, files):
        problems.append({"code": code, "message": message, "files": sorted(files), "nodeids": []})
        unknown.update(requested & set(files))
    if not isinstance(rows, list):
        problem("report_invalid", "report is not a list of shard observations", requested)
        rows = []
    for index, row in enumerate(rows):
        shard = _judge_row(row, index)
        shards.append(shard)
        for name in row.get("files", []) if isinstance(row, dict) and isinstance(row.get("files"), list) else []:
            if isinstance(name, str):
                assignments.setdefault(name, []).append(index)
        for node in set(shard["collected"]):
            owners.setdefault(node, []).append(index)
    missing = requested - assignments.keys()
    foreign = assignments.keys() - requested
    duplicated = {name for name, ids in assignments.items() if len(ids) > 1}
    for name, indices in assignments.items():
        if not any(isinstance(rows[index].get("node_groups"), dict)
                   and name in rows[index]["node_groups"] for index in indices):
            continue
        message = _node_group_assignment(rows, shards, name, indices)
        if message is not None:
            problem("node_group_assignment", message, [name])
        duplicated.discard(name)
    if missing:
        problem("assigned_missing", "requested files have no shard assignment", missing)
    if foreign:
        problem("assigned_foreign", "shards assign files outside the request", requested | foreign)
    if duplicated:
        problem("assigned_duplicate", "files assigned to multiple shards", duplicated)
    for node, ids in owners.items():
        if len(ids) > 1:
            for index in ids:
                shard = shards[index]
                rec = shard["reconciliation"]
                rec.setdefault("in_other_shards", []).append(node)
                message = "node ID(s) also collected by another shard"
                if message not in rec["problems"]:
                    rec["problems"].append(message)
                    shard["problems"].append({"code": "cross_shard_collection", "message": message,
                        "files": list(rows[index]["files"]), "nodeids": [node]})
                shard["unobserved_files"] = sorted(set(shard["unobserved_files"]) | set(rows[index]["files"]))
                shard["green"] = False
                shard["coverage"] = "unknown"
    if shards and not any(s["collected"] or s["failing_nodes"] for s in shards):
        problem("population_empty", "no tests collected in the requested population; no passing test coverage", requested)
        for shard in shards:
            shard["green"] = False
    if required_nodes is not None:
        expected = sorted(required_nodes)
        selected = sorted(n for s in shards for n in s["collected"])
        records = [parse(row["output"], record_binding=row.get("record_binding")) for row in rows
                   if isinstance(row, dict) and isinstance(row.get("output"), str)]
        records = [r for r in records if _valid_record(r)]
        reports = [r for record in records if record for r in record.get("reports", [])]
        if (selected != expected or any(record.get("collect_only") or record.get("uncounted") for record in records if record)
                or sorted((r[0], r[1], r[2]) for r in reports) != sorted((n, "call", "passed") for n in expected)):
            problem("required_population", "not every reference table passed exactly once in the required population", requested)
    if client_returncode and shards and (client_returncode != 1 or all(s["green"] for s in shards)):
        problem("unexplained_client_exit", f"pbtest client exited {client_returncode} without an ordinary failed-run ending", requested)
    if not shards and requested:
        unknown.update(requested)
    all_problems = [*problems, *(p for s in shards for p in s["problems"])]
    return {"shards": shards, "green": bool(shards) and not all_problems,
            "failing_nodes": sorted({n for s in shards for n in s["failing_nodes"]}),
            "unobserved_files": sorted((unknown | {f for s in shards for f in s["unobserved_files"]}) & requested),
            "problems": all_problems,
            "runtime_refusal": [s["runtime_refusal"] for s in shards if s["runtime_refusal"]]}


def main(argv: list[str] | None = None, *, preflight=None,
         resource_source: str | None = None, collection_spec=None,
         collection_source: str | None = None,
         roster_parent: str | None = None) -> int:
    """Run pytest on ``argv`` with the recorder, then return its exit code.

    ``preflight`` runs first and ends the shard, before pytest, on a
    non-zero answer: it is the reviewed-dependency guard when the checkout
    pins one.  This mirrors ``pytest.console_main``, which is what
    ``python -m pytest`` runs.  ``roster_parent`` is where the plugin
    roster's temporary directory goes: the attempt's own directory, so a
    killed shard's roster expires with its base temp instead of staying in
    ``TMPDIR``.
    """

    # Execution-local, never sealed into argv or placed in os.environ. Nested
    # pytest invocations get their own nonce, even with identical source/env.
    run_nonce = secrets.token_hex(32)
    getpid = os.getpid
    recorder_pid = getpid()
    refused, refusal = (0, None) if preflight is None else preflight()
    print(preflight_record(refused, refusal, run_nonce=run_nonce), flush=True)
    if refused:
        return int(refused)
    import pytest

    if argv is None:
        # ``python -m pytest`` leaves pytest's own ``__main__`` in argv[0]; a
        # test that re-reads it sees the same thing under this program.
        sys.argv[0] = str(Path(pytest.__file__).with_name("__main__.py"))

    # Preserve the harness stream before pytest installs capture. Neither a
    # replaced terminal.write nor capture's test-visible buffers see bound bytes.
    output_fd = os.dup(sys.stdout.fileno())
    write_output = os.write
    encode_record = json.JSONEncoder(separators=(",", ":")).encode

    def emit_line(line):
        data = ("\n" + line + "\n").encode("utf-8", errors="replace")
        while data:
            data = data[write_output(output_fd, data):]

    def emit_bound(prefix, record):
        # A fork without exec inherits the closure, but never its authority.
        if getpid() != recorder_pid:
            return
        emit_line(prefix + encode_record({**record, "run_nonce": run_nonce}))

    class RecorderState:
        def __init__(self) -> None:
            self.config = None
            self.session = None
            self.sessionstart_finished = False
            self.sessionfinish_started = False
            self.collect_only = False
            self.collection_started = False
            self.collection_finished = False
            self.runtestloop_started = False
            self.runtestloop_finished = False
            self.collected: list[str] | None = None
            self.deselected: list[str] = []
            # Modules whose collection failed or skipped, as every xdist
            # worker saw them.  Not counted: the summary counts the
            # controller's deduplicated reports (#1220).
            self.collect_seen: list[str] = []
            self.reports: list[list] = []
            self.bound_failures: list[dict] = []
            self.first_tightened_test: dict | None = None
            self.uncounted: list[list] = []
            self.file_durations: dict[str, float] = {}
            self.node_durations: dict[str, float] = {}
            self.written = False
            self.selection_views = []
            self.last_record = None
            self.teardown_finished: set[str] = set()
            self.duplicate_teardown = False
            self.worker_collections: dict[str, list[str]] = {}
            self.expected_workers = 0
            self.completion_written = False

    state = RecorderState()

    def trace(event: str, nodeid: str, **fields) -> None:
        if not trace_enabled:
            return
        encoded = nodeid.encode("utf-8", errors="replace")
        if len(encoded) > TRACE_NODEID_MAX_BYTES:
            fields.update(nodeid_truncated=True,
                          nodeid_sha256=raw_sha256(encoded))
            nodeid = encoded[:TRACE_NODEID_MAX_BYTES].decode("utf-8", errors="ignore")
        line = TRACE_PREFIX + json.dumps({
            "schema": TRACE_SCHEMA, "event": event, "nodeid": nodeid,
            "reported_unix": time.time(),
            **fields,
        }, separators=(",", ":"))
        emit_line(line)

    def emit_completion(*, session_finished: bool = False) -> None:
        if state.completion_written:
            return
        collected = state.collected
        if not session_finished and (
                collected is None or not collected
                or len(state.teardown_finished) < len(collected)):
            return
        collection_complete = collected is not None
        if state.expected_workers:
            collection_complete = (
                len(state.worker_collections) >= state.expected_workers
                and all(ids == collected
                        for ids in state.worker_collections.values()))
        record = {
            "schema": COMPLETION_SCHEMA,
            "collection_complete": collection_complete,
            "collect_only": state.collect_only,
            "collection_errors": any(row[1:3] == ["collect", "error"]
                                     for row in state.reports),
            "collected": collected or [],
            "teardown_finished": sorted(state.teardown_finished),
            "duplicate_teardown": state.duplicate_teardown,
            "outcome_nodeids": sorted(
                {row[0] for row in state.reports if row[1] != "collect"}
                | {row[0] for row in state.uncounted}),
        }
        if session_finished or _completion_evidence(record)["status"] == "complete":
            emit_bound(COMPLETION_PREFIX, record)
            state.completion_written = True

    def outcome_record() -> dict:
        config = state.config
        record = {
            "schema": SCHEMA,
            "rootdir_relative": (os.path.relpath(config.rootpath,
                config.invocation_params.dir) if config is not None else "."),
            "collect_only": state.collect_only,
            "collected": state.collected,
            "deselected": state.deselected,
            "collect_seen": sorted(set(state.collect_seen)),
            "reports": state.reports,
            "bound_failures": state.bound_failures,
            "uncounted": state.uncounted,
            # Each file's summed phase seconds, to the millisecond:
            # the packing model a later run reads (#1246).  Additive,
            # so a reader that predates it sees the same record.
            "file_durations": {
                name: round(seconds, 3)
                for name, seconds in state.file_durations.items()},
            "node_durations": {
                node: round(seconds, 3)
                for node, seconds in state.node_durations.items()},
        }
        if state.first_tightened_test is not None:
            record["first_tightened_test_budget_remaining_s"] = state.first_tightened_test[
                "budget_remaining_s"]
        if collection_spec is not None:
            view = None
            error = None
            if xdist:
                if (state.selection_views and state.selection_views[0] is not None
                        and all(v == state.selection_views[0] for v in state.selection_views)):
                    view = state.selection_views[0]
                else:
                    error = "missing or inconsistent worker file selection evidence"
            else:
                plugin = config.pluginmanager.getplugin("pbtest-file-selection")
                if plugin is not None:
                    view = plugin.report()
                else:
                    error = "file selection owner is unavailable"
            record["file_selection"] = view
            record["file_selection_error"] = error
        return record

    # The public dispatch monitor, installed before conftests load, owns phase
    # authority. Advancing a registered hook's generator is not evidence that
    # pytest dispatched (let alone completed) collection or the test loop.
    # xdist's receiver dispatches hooks concurrently with the controller.
    hook_stacks = threading.local()

    def thread_calls():
        return hook_stacks.__dict__.setdefault("calls", [])

    monitored_manager = None
    command_call = None

    def before_hook(name, implementations, kwargs):
        nonlocal command_call
        hook_calls = thread_calls()
        parent = hook_calls[-1] if hook_calls else None
        call = {"name": name, "kwargs": dict(kwargs), "phase": False, "consumed": False}
        hook_calls.append(call)
        if (name == "pytest_cmdline_main" and parent is None and command_call is None
                and kwargs["config"].pluginmanager is monitored_manager):
            command_call = call
        # Lifecycle events belong directly to this invocation's command, not
        # to an intervening hook such as sessionstart or collection_finish.
        if parent is not command_call or command_call is None:
            return
        if name == "pytest_configure" and state.config is None:
            state.config = kwargs["config"]
            state.collect_only = bool(state.config.getoption("collectonly", False))
        session = kwargs.get("session")
        if (name == "pytest_sessionstart" and state.session is None
                and session.config is state.config):
            state.session = session
            call["phase"] = True
        elif session is not None and session is state.session:
            if (name == "pytest_collection" and state.sessionstart_finished
                    and not state.collection_started):
                state.collection_started = True
                call["phase"] = True
            elif (name == "pytest_runtestloop" and state.collection_finished
                    and not state.runtestloop_started):
                state.runtestloop_started = True
                call["phase"] = True
            elif (name == "pytest_sessionfinish" and state.runtestloop_finished
                    and not state.sessionfinish_started):
                state.sessionfinish_started = True
                call["phase"] = True
                emit_completion(session_finished=True)

    def after_hook(result, name, implementations, kwargs):
        hook_calls = thread_calls()
        call = None
        for index in range(len(hook_calls) - 1, -1, -1):
            if hook_calls[index]["name"] == name:
                call = hook_calls[index]
                del hook_calls[index:]
                break
        if name == "pytest_runtest_logreport":
            record_test_report(kwargs["report"])
        if call is None or not call["phase"]:
            return
        if name == "pytest_sessionstart":
            state.sessionstart_finished = result.excinfo is None
        elif name == "pytest_collection":
            # The controller's population arrives from xdist workers. Snapshot
            # local items only after the whole real collection dispatch returns.
            if not state.expected_workers:
                state.collected = [item.nodeid for item in state.session.items]
            state.collection_finished = result.excinfo is None
        elif name == "pytest_runtestloop":
            # KeyboardInterrupt must still allow outcomes before unconfigure.
            state.runtestloop_finished = True
        elif name == "pytest_sessionfinish":
            state.last_record = outcome_record()
            emit_bound(PREFIX, state.last_record)
            state.written = True

    def accept_hook(name, **arguments):
        hook_calls = thread_calls()
        if not hook_calls:
            return False
        call = hook_calls[-1]
        if (call["name"] != name or call["consumed"]
                or any(call["kwargs"].get(key) is not value
                       for key, value in arguments.items())):
            return False
        # These synchronous adapters consume each dispatched argument once;
        # fabricated arguments and direct calls from other hooks have no claim.
        call["consumed"] = True
        return True

    class OutcomeRecorder:
        # Only pytest event adapters are registered. State and emission helpers
        # are reachable only by inspecting their closures, never via attributes.
        __slots__ = ()

        def pytest_plugin_registered(self, plugin, plugin_name, manager) -> None:
            nonlocal monitored_manager
            if plugin is self and monitored_manager is None:
                monitored_manager = manager
                manager.add_hookcall_monitoring(before_hook, after_hook)

        # Keep the lifecycle adapter surface, but it has no authority. Only
        # the private monitor can observe real dispatch entry and return.
        def pytest_configure(self, config) -> None:
            pass

        def pytest_sessionstart(self, session) -> None:
            pass

        @pytest.hookimpl(hookwrapper=True, tryfirst=True)
        def pytest_sessionfinish(self, session, exitstatus):
            yield

        def pytest_runtest_logstart(self, nodeid, location) -> None:
            trace("start", nodeid)

        @pytest.hookimpl(hookwrapper=True, tryfirst=True)
        def pytest_collection(self, session):
            yield

        @pytest.hookimpl(hookwrapper=True, tryfirst=True)
        def pytest_runtestloop(self, session):
            yield

        @pytest.hookimpl(specname="pytest_collection", tryfirst=True)
        def pytest_collection_ignore_named(self, session):
            hook_calls = thread_calls()
            if (not hook_calls or not hook_calls[-1]["phase"]
                    or not accept_hook("pytest_collection", session=session)
                    or collection_spec is not None):
                return None
            # pytest never asks its ignore rules about a path named on the
            # command line, and pbtest names every file (#1304).  Drop the
            # named files a loaded conftest's ``collect_ignore`` /
            # ``collect_ignore_glob`` excludes, which is what a directory
            # run of the same suite would have done.
            ignored = ignored_named_paths(session.config)
            if not ignored:
                return None
            session.config.args[:] = [
                arg for arg in session.config.args
                if str(arg) not in ignored]
            if not session.config.args:
                # Every named file is ignored: collect nothing rather than
                # widen an empty argument list to the whole suite.
                session.testscollected = 0
                return True
            return None

        def pytest_collection_finish(self, session) -> None:
            # Only the dispatch monitor snapshots after collection returns.
            # Direct registry calls, including during collection, are ignored.
            pass

        @pytest.hookimpl(optionalhook=True)
        def pytest_xdist_setupnodes(self, config, specs) -> None:
            if accept_hook("pytest_xdist_setupnodes", config=config, specs=specs):
                state.expected_workers = len(specs)

        def pytest_deselected(self, items) -> None:
            if accept_hook("pytest_deselected", items=items):
                state.deselected.extend(item.nodeid for item in items)

        @pytest.hookimpl(optionalhook=True)
        def pytest_xdist_node_collection_finished(self, node, ids) -> None:
            if not accept_hook("pytest_xdist_node_collection_finished", node=node, ids=ids):
                return
            # The xdist controller collects nothing itself; every worker
            # collects the whole population and reports its IDs here.
            if state.collected is None:
                state.collected = list(ids)
            state.worker_collections[node.gateway.id] = list(ids)

        @pytest.hookimpl(optionalhook=True)
        def pytest_testnodedown(self, node, error) -> None:
            if not accept_hook("pytest_testnodedown", node=node, error=error):
                return
            output = getattr(node, "workeroutput", None)
            if isinstance(output, dict):
                state.deselected.extend(output.get("pbtest_deselected") or ())
                state.collect_seen.extend(output.get("pbtest_collect_seen") or ())
                if collection_spec is not None:
                    state.selection_views.append(output.get("pbtest_file_selection"))

        def pytest_collectreport(self, report) -> None:
            if not accept_hook("pytest_collectreport", report=report):
                return
            if report.failed:
                state.reports.append([report.nodeid, "collect", "error", None, None])
            elif report.skipped:
                state.reports.append([report.nodeid, "collect", "skipped",
                                     skip_reason(report),
                                     skip_location(report, state.config.invocation_params.dir)])

        @pytest.hookimpl(hookwrapper=True, tryfirst=True)
        def pytest_runtest_logreport(self, report) -> None:
            # The monitor records the real dispatch even if a direct caller
            # advances and then abandons this generator.
            yield

    def record_test_report(report):
        first = getattr(report, "pbtest_first_tightened_test", None)
        if first is not None and (state.first_tightened_test is None
                or first["started_monotonic"] < state.first_tightened_test["started_monotonic"]):
            # xdist can deliver reports out of start order. All workers
            # share this host's monotonic clock; compare starts, not arrivals.
            state.first_tightened_test = first
        trace("phase", report.nodeid, when=report.when,
              outcome=report.outcome,
              worker=getattr(report, "worker_id", None),
              resources=getattr(report, "pbtest_resources", None))
        # Every phase consumes wall time, counted or not: a passing
        # setup or teardown never reaches the summary line, but the
        # file still paid its seconds, and the packing model (#1246)
        # must charge them.  Summed by repository-relative file, so a
        # later run can key the same file out of its own history.
        try:
            duration = float(getattr(report, "duration", 0.0) or 0.0)
        except (TypeError, ValueError):
            duration = 0.0
        if duration > 0:
            config = state.config
            rootdir = ""
            if config is not None:
                try:
                    rootdir = os.path.relpath(
                        config.rootpath, config.invocation_params.dir)
                except Exception:
                    rootdir = ""
            name = source_file(report.nodeid, rootdir)
            state.file_durations[name] = (
                state.file_durations.get(name, 0.0) + duration)
            nodeid = checkout_nodeid(report.nodeid, rootdir)
            state.node_durations[nodeid] = state.node_durations.get(nodeid, 0.0) + duration
        status = state.config.hook.pytest_report_teststatus(
            report=report, config=state.config)
        category = status[0] if status else ""
        if category:
            if not getattr(report, "count_towards_summary", True):
                # Uncounted reports still establish that the test ran.
                state.uncounted.append([report.nodeid, report.when, category])
            else:
                reason = location = None
                if category == "skipped":
                    reason = skip_reason(report)
                    location = skip_location(report, state.config.invocation_params.dir)
                elif hasattr(report, "wasxfail"):
                    reason = str(report.wasxfail or "") or None
                state.reports.append([report.nodeid, report.when, category,
                                     reason, location])
                kind = getattr(report, "pbtest_bound_kind", None)
                if kind is not None and category in ("failed", "error"):
                    state.bound_failures.append({
                        "nodeid": report.nodeid, "when": report.when, "kind": kind})
        if report.when == "teardown":
            if report.nodeid in state.teardown_finished:
                state.duplicate_teardown = True
            state.teardown_finished.add(report.nodeid)
            # xdist can wait for a worker's exit before sessionfinish.
            emit_completion()

    arguments = list(sys.argv[1:] if argv is None else argv)
    trace_enabled = TRACE_OPTION in arguments
    xdist = "-n" in arguments or any(arg.startswith("-n") and arg != "-n"
                                     for arg in arguments)
    if collection_spec is not None:
        if collection_source is None:
            raise ValueError("pbtest file selection needs its sealed collection owner")
        owner = {"__name__": "pbtest_selection_owner"}
        exec(compile(collection_source, "<pbtest collection owner>", "exec"), owner)
        selection = owner["selection_plugin"](collection_spec)
        files = collection_spec["files"]
        if arguments[-len(files):] != files:
            raise ValueError("pbtest assigned files do not match the sealed pytest argv")
        arguments = arguments[:-len(files)] + selection.targets
    recorder = OutcomeRecorder()
    if trace_enabled and resource_source is None:
        raise ValueError("--pbtest-trace needs the sealed resource accounting source")
    try:
        if xdist or trace_enabled or collection_spec is not None:
            # The shard is already inside one admitted PB action. The worker
            # plugin lives only for this pytest invocation and changes no checkout
            # or sealed input. xdist's workeroutput is the transport it owns.
            with tempfile.TemporaryDirectory(prefix="pbtest-xdist-roster-",
                                             dir=roster_parent) as folder:
                Path(folder, "pbtest_xdist_roster.py").write_text(XDIST_ROSTER_PLUGIN)
                if collection_spec is not None:
                    Path(folder, "pbtest_file_selection.py").write_text(
                        collection_source + "\n\ndef pytest_configure(config):\n"
                        "    config.pluginmanager.register(selection_plugin("
                        + repr(collection_spec) + "), 'pbtest-file-selection')\n")
                if trace_enabled:
                    Path(folder, "pbtest_resource_scope.py").write_text(resource_source)
                    Path(folder, "pbtest_resource_trace.py").write_text(RESOURCE_TRACE_PLUGIN)
                sys.path.insert(0, folder)
                old_path = os.environ.get("PYTHONPATH")
                os.environ["PYTHONPATH"] = folder + os.pathsep + (old_path or "")
                try:
                    plugins = ["-p", "pbtest_xdist_roster"]
                    if collection_spec is not None:
                        plugins += ["-p", "pbtest_file_selection"]
                    if trace_enabled:
                        plugins += ["-p", "pbtest_resource_trace"]
                    code = pytest.main([*plugins, *arguments],
                                       plugins=[recorder])
                finally:
                    sys.path.remove(folder)
                    if old_path is None:
                        os.environ.pop("PYTHONPATH", None)
                    else:
                        os.environ["PYTHONPATH"] = old_path
        else:
            code = pytest.main(arguments, plugins=[recorder])
        if not state.written and state.config is not None:
            state.last_record = outcome_record()
            emit_bound(PREFIX, state.last_record)
    finally:
        os.close(output_fd)
    sys.stdout.flush()
    # Excluded/collection-skipped file quanta are resolved work, not test
    # passes. The parent still refuses a globally empty population.
    record = state.last_record
    if (int(code) == 5 and collection_spec is not None and record is not None
            and not record.get("collected") and not record.get("file_selection_error")
            and isinstance(record.get("file_selection"), dict)):
        covered = set(record["file_selection"]["ignored"])
        rootdir = record["rootdir_relative"]
        covered.update(source_file(row[0], rootdir) for row in record["reports"]
                       if row[1:3] == ["collect", "skipped"])
        covered.update(source_file(nodeid, rootdir) for nodeid in record["deselected"])
        if set(collection_spec["files"]) <= covered and all(
                row[2] == "skipped" for row in record["reports"]):
            code = 0
    return int(code)
