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
from pathlib import Path
import sys

#: The version of the record's shape.  A reader refuses any other.
SCHEMA = "prismabuild.pbtest_outcomes.v1"
#: The one line a shard prints its record on.
PREFIX = "pbtest-outcomes: "


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

    ``None`` means the shard printed none: pytest never reached the end of
    its session, or the shard did not run through this module.  A record
    with another schema is not this reader's to interpret, and is ``None``
    too, so a reader never mistakes a foreign shape for an empty one.
    """

    for line in reversed((output or "").splitlines()):
        if not line.startswith(PREFIX):
            continue
        try:
            record = json.loads(line[len(PREFIX):])
        except ValueError:
            return None
        if isinstance(record, dict) and record.get("schema") == SCHEMA:
            return record
        return None
    return None


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


def main(argv: list[str] | None = None, *, preflight=None) -> int:
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
            self.reports: list[list] = []
            self.uncounted: list[list] = []
            self.written = False

        def pytest_configure(self, config) -> None:
            self.config = config

        def location(self, report) -> str | None:
            return skip_location(report, self.config.invocation_params.dir)

        def pytest_collection_finish(self, session) -> None:
            self.collected = [item.nodeid for item in session.items]

        @pytest.hookimpl(optionalhook=True)
        def pytest_xdist_node_collection_finished(self, node, ids) -> None:
            # The xdist controller collects nothing itself; every worker
            # collects the whole population and reports its IDs here.
            if self.collected is None:
                self.collected = list(ids)

        def pytest_collectreport(self, report) -> None:
            if report.failed:
                self.reports.append([report.nodeid, "collect", "error", None, None])
            elif report.skipped:
                self.reports.append([report.nodeid, "collect", "skipped",
                                     skip_reason(report), self.location(report)])

        def pytest_runtest_logreport(self, report) -> None:
            status = self.config.hook.pytest_report_teststatus(
                report=report, config=self.config)
            category = status[0] if status else ""
            if not category:
                return  # a passing setup or teardown: the summary counts nothing
            if not getattr(report, "count_towards_summary", True):
                # The terminal leaves it out of the summary line, so the
                # record's counts do too; it still shows the test ran.
                self.uncounted.append([report.nodeid, report.when, category])
                return
            reason = location = None
            if category == "skipped":
                reason, location = skip_reason(report), self.location(report)
            elif hasattr(report, "wasxfail"):
                reason = str(report.wasxfail or "") or None
            self.reports.append([report.nodeid, report.when, category,
                                 reason, location])

        def record(self) -> str:
            config = self.config
            return PREFIX + json.dumps({
                "schema": SCHEMA,
                "collect_only": bool(config is not None
                                     and config.getoption("collectonly", False)),
                "collected": self.collected,
                "reports": self.reports,
                "uncounted": self.uncounted,
            }, separators=(",", ":"))

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

    code = pytest.main(sys.argv[1:] if argv is None else argv,
                       plugins=[OutcomeRecorder()])
    sys.stdout.flush()
    return int(code)
