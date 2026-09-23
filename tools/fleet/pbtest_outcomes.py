"""Every counted test outcome of a pbtest shard, by node ID, sealed into its argv.

A shard's terminal summary says ``231 passed, 6 skipped`` and nothing else:
not which six, not why.  A skip certifies nothing unless its reason is on
record, and pytest's ``-rs`` folds skips by location and drops their node IDs
(#942).  So the shard runs pytest through this module, which records each
report pytest's own terminal counts, and prints the record as one line,
``pbtest-outcomes: {json}``, where the pool stores the shard's stdout.
``pbtest.py`` reads it back into the receipt.

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


def skip_location(report) -> str | None:
    """``path:line`` of the skip as ``-rs`` prints it, or ``None``.

    pytest stores the line already 1-based in a skip's ``longrepr``: from
    the raising frame for an imperative or collection skip, and from the
    test's own definition for a marker.
    """

    longrepr = report.longrepr
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        path, lineno, _ = longrepr
        return f"{path}:{lineno}" if isinstance(lineno, int) else str(path)
    return None


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
            self.written = False

        def pytest_configure(self, config) -> None:
            self.config = config

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
                                     skip_reason(report), skip_location(report)])

        def pytest_runtest_logreport(self, report) -> None:
            status = self.config.hook.pytest_report_teststatus(
                report=report, config=self.config)
            category = status[0] if status else ""
            if not category:
                return  # a passing setup or teardown: the summary counts nothing
            reason = location = None
            if category == "skipped":
                reason, location = skip_reason(report), skip_location(report)
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
