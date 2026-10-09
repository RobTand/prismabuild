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
from prismabuild.digest_primitives import raw_sha256
import os
import posixpath
from contextlib import nullcontext
from pathlib import Path
import sys
import tempfile
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


def parse(output: str) -> dict | None:
    """The last outcome record in a shard's output, or ``None``.

    ``None`` means the shard printed no final outcomes. Exit hooks can block
    this record after the selected tests finish. Completion evidence uses a
    separate record and cannot replace final outcomes.
    """

    return _parse_record(output, PREFIX, SCHEMA)


def _parse_record(output: str, prefix: str, schema: str) -> dict | None:
    for line in reversed((output or "").splitlines()):
        if not line.startswith(prefix):
            continue
        try:
            record = json.loads(line[len(prefix):])
        except ValueError:
            return None
        if isinstance(record, dict) and record.get("schema") == schema:
            return record
        return None
    return None


def completion(output: str) -> dict:
    """Verify selected-test completion, not final outcomes or process exit."""
    record = _parse_record(output, COMPLETION_PREFIX, COMPLETION_SCHEMA)
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


from pbtest_collection import ignored_named_paths


def main(argv: list[str] | None = None, *, preflight=None,
         resource_source: str | None = None, collection_spec=None,
         collection_source: str | None = None) -> int:
    """Run pytest on ``argv`` with the recorder, then return its exit code.

    ``preflight`` runs first and ends the shard, before pytest, on a
    non-zero answer: it is the reviewed-dependency guard when the checkout
    pins one.  This mirrors ``pytest.console_main``, which is what
    ``python -m pytest`` runs.
    """

    if preflight is not None:
        refused = preflight()
        if refused:
            return int(refused)
    import pytest

    if argv is None:
        # ``python -m pytest`` leaves pytest's own ``__main__`` in argv[0]; a
        # test that re-reads it sees the same thing under this program.
        sys.argv[0] = str(Path(pytest.__file__).with_name("__main__.py"))

    class OutcomeRecorder:
        def __init__(self) -> None:
            self.config = None
            self.collected: list[str] | None = None
            self.deselected: list[str] = []
            # Modules whose collection failed or skipped, as every xdist
            # worker saw them.  Not counted: the summary counts the
            # controller's deduplicated reports (#1220).
            self.collect_seen: list[str] = []
            self.reports: list[list] = []
            self.uncounted: list[list] = []
            self.file_durations: dict[str, float] = {}
            self.written = False
            self.selection_views = []
            self.last_record = None
            self.teardown_finished: set[str] = set()
            self.duplicate_teardown = False
            self.worker_collections: dict[str, list[str]] = {}
            self.expected_workers = 0
            self.completion_written = False

        def pytest_configure(self, config) -> None:
            self.config = config

        def trace(self, event: str, nodeid: str, **fields) -> None:
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
            self.emit(line)

        def emit(self, line: str) -> None:
            # The terminal writer owns the stream outside pytest's capture.
            # Direct stdout writes can be captured with the dying test and
            # never reach the pool log. Flush each record explicitly.
            terminal = self.config.pluginmanager.getplugin("terminalreporter")
            if terminal is not None:
                # Another hook may just have printed a progress dot. Start
                # our own line rather than depending on its cursor state.
                terminal.write("\n" + line + "\n", flush=True)
            else:
                capture = self.config.pluginmanager.getplugin("capturemanager")
                with (capture.global_and_fixture_disabled()
                      if capture is not None else nullcontext()):
                    sys.stdout.write(line + "\n")
                    sys.stdout.flush()

        def emit_completion(self, *, session_finished: bool = False) -> None:
            if self.completion_written:
                return
            collected = self.collected
            if not session_finished and (
                    collected is None or not collected
                    or len(self.teardown_finished) < len(collected)):
                return
            collection_complete = collected is not None
            if self.expected_workers:
                collection_complete = (
                    len(self.worker_collections) == self.expected_workers
                    and all(ids == collected
                            for ids in self.worker_collections.values()))
            record = {
                "schema": COMPLETION_SCHEMA,
                "collection_complete": collection_complete,
                "collect_only": bool(self.config.getoption("collectonly", False)),
                "collection_errors": any(row[1:3] == ["collect", "error"]
                                         for row in self.reports),
                "collected": collected or [],
                "teardown_finished": sorted(self.teardown_finished),
                "duplicate_teardown": self.duplicate_teardown,
                "outcome_nodeids": sorted(
                    {row[0] for row in self.reports if row[1] != "collect"}
                    | {row[0] for row in self.uncounted}),
            }
            line = COMPLETION_PREFIX + json.dumps(record, separators=(",", ":"))
            if session_finished or completion(line)["status"] == "complete":
                self.emit(line)
                self.completion_written = True

        @pytest.hookimpl(hookwrapper=True, tryfirst=True)
        def pytest_sessionfinish(self, session, exitstatus):
            self.emit_completion(session_finished=True)
            yield

        def pytest_runtest_logstart(self, nodeid, location) -> None:
            self.trace("start", nodeid)

        @pytest.hookimpl(tryfirst=True)
        def pytest_collection(self, session):
            if collection_spec is not None:
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
                self.collected = []
                session.testscollected = 0
                return True
            return None

        def location(self, report) -> str | None:
            return skip_location(report, self.config.invocation_params.dir)

        def pytest_collection_finish(self, session) -> None:
            self.collected = [item.nodeid for item in session.items]

        @pytest.hookimpl(optionalhook=True)
        def pytest_xdist_setupnodes(self, config, specs) -> None:
            self.expected_workers = len(specs)

        def pytest_deselected(self, items) -> None:
            self.deselected.extend(item.nodeid for item in items)

        @pytest.hookimpl(optionalhook=True)
        def pytest_xdist_node_collection_finished(self, node, ids) -> None:
            # The xdist controller collects nothing itself; every worker
            # collects the whole population and reports its IDs here.
            if self.collected is None:
                self.collected = list(ids)
            self.worker_collections[node.gateway.id] = list(ids)

        @pytest.hookimpl(optionalhook=True)
        def pytest_testnodedown(self, node, error) -> None:
            output = getattr(node, "workeroutput", None)
            if isinstance(output, dict):
                self.deselected.extend(output.get("pbtest_deselected") or ())
                self.collect_seen.extend(output.get("pbtest_collect_seen") or ())
                if collection_spec is not None:
                    self.selection_views.append(output.get("pbtest_file_selection"))

        def pytest_collectreport(self, report) -> None:
            if report.failed:
                self.reports.append([report.nodeid, "collect", "error", None, None])
            elif report.skipped:
                self.reports.append([report.nodeid, "collect", "skipped",
                                     skip_reason(report), self.location(report)])

        @pytest.hookimpl(hookwrapper=True, tryfirst=True)
        def pytest_runtest_logreport(self, report) -> None:
            yield
            self.trace("phase", report.nodeid, when=report.when,
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
                config = self.config
                rootdir = ""
                if config is not None:
                    try:
                        rootdir = os.path.relpath(
                            config.rootpath, config.invocation_params.dir)
                    except Exception:
                        rootdir = ""
                name = source_file(report.nodeid, rootdir)
                self.file_durations[name] = (
                    self.file_durations.get(name, 0.0) + duration)
            status = self.config.hook.pytest_report_teststatus(
                report=report, config=self.config)
            category = status[0] if status else ""
            if category:
                if not getattr(report, "count_towards_summary", True):
                    # Uncounted reports still establish that the test ran.
                    self.uncounted.append([report.nodeid, report.when, category])
                else:
                    reason = location = None
                    if category == "skipped":
                        reason, location = skip_reason(report), self.location(report)
                    elif hasattr(report, "wasxfail"):
                        reason = str(report.wasxfail or "") or None
                    self.reports.append([report.nodeid, report.when, category,
                                         reason, location])
            if report.when == "teardown":
                if report.nodeid in self.teardown_finished:
                    self.duplicate_teardown = True
                self.teardown_finished.add(report.nodeid)
                # xdist can wait for a worker's exit before sessionfinish.
                self.emit_completion()

        def record(self) -> str:
            config = self.config
            record = {
                "schema": SCHEMA,
                "rootdir_relative": (os.path.relpath(config.rootpath,
                    config.invocation_params.dir) if config is not None else "."),
                "collect_only": bool(config is not None
                                     and config.getoption("collectonly", False)),
                "collected": self.collected,
                "deselected": self.deselected,
                "collect_seen": sorted(set(self.collect_seen)),
                "reports": self.reports,
                "uncounted": self.uncounted,
                # Each file's summed phase seconds, to the millisecond:
                # the packing model a later run reads (#1246).  Additive,
                # so a reader that predates it sees the same record.
                "file_durations": {
                    name: round(seconds, 3)
                    for name, seconds in self.file_durations.items()},
            }
            if collection_spec is not None:
                view = None
                error = None
                if xdist:
                    if (self.selection_views and self.selection_views[0] is not None
                            and all(v == self.selection_views[0] for v in self.selection_views)):
                        view = self.selection_views[0]
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
            self.last_record = record
            return PREFIX + json.dumps(record, separators=(",", ":"))

        def pytest_terminal_summary(self, terminalreporter) -> None:
            terminalreporter.write_line(self.record())
            self.written = True

        def pytest_unconfigure(self, config) -> None:
            # Without the terminal plugin there is no summary hook; the
            # record still has to reach the shard's stdout.
            if not self.written:
                sys.stdout.write(self.record() + "\n")
                sys.stdout.flush()
                self.written = True

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
    if xdist or trace_enabled or collection_spec is not None:
        # The shard is already inside one admitted PB action. The worker
        # plugin lives only for this pytest invocation and changes no checkout
        # or sealed input. xdist's workeroutput is the transport it owns.
        with tempfile.TemporaryDirectory(prefix="pbtest-xdist-roster-") as folder:
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
    sys.stdout.flush()
    # Excluded/collection-skipped file quanta are resolved work, not test
    # passes. The parent still refuses a globally empty population.
    record = recorder.last_record
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
