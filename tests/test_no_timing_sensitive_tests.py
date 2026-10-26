"""D-19 guard mechanics, amended by the PB lead on 2026-10-24 (#1771).

Sleep sites are purely syntactic. Every `from time import sleep`, under any
alias, and `from time import *` is a finding, even when unused or followed
only by sleep(0). Every <m>.sleep attribute whose name <m> occurs in
`import time [as <m>]` in the
file is a finding, except as the func of a Call with one literal numeric zero
argument and no keywords. Use time.sleep(0) or t.sleep(0) for cooperative
yields. Module names are collected across the file, conservatively including
shadowed names. There is no alias-use, binding-order or deferred-call resolver.
Imports and attribute sites guard lambdas, closures, generators, walrus
bindings, functools.partial and passing the sleep object as a value.

Constant timeouts below 60 seconds are findings on .wait, .join, Queue-shaped
.get/.put, subprocess.run and imported run aliases, .communicate and
.settimeout. Numeric constants include signed literals and arithmetic using
+, -, *, /, // and %. Names, calls and other expressions are non-constant
for this guard. Bounds of 60 seconds or more and None are allowed. Positional
slots are wait/join/settimeout's first, communicate's second, get's second,
and put's third. Positional get uses imported Queue constructors, annotations
or a statically boolean block expression to distinguish mapping defaults.
Queue receiver names and boolean bindings are collected across each file.
The broad method matching also conservatively catches unrelated objects
with those same timeout shapes. Ordinary str.join and dict.get are unrelated.
Opaque get receivers with opaque block expressions remain ambiguous; the
scanner cannot distinguish their Queue timeout from a mapping default.

Out of scope: getattr strings (including computed strings), exec, eval,
importlib tricks, non-constant timeouts, asyncio sleeps/waits, select/poll,
monotonic deadline loops, elapsed assertions, and sleeps in src/ or tools/.
Fixtures below assert those limits. The backstop is TrustLadder's test-quality
checker on every PB pull request, the PB reviewer rules and the five-run flake
check. See docs/contributing.md's Test quality section for authoring policy.

Only controlled timing may use '# pb-timing-ok: <reason>' on the import,
attribute or timed call's starting line. Reasons are mandatory; stale pragmas
fail. Counts use path::qualname kind, never lines or source hashes. Existing
debt matches exactly both ways. Regeneration may only shrink counts, preserves
issue links and reads no Git history. Reviewers block allowlist growth.
The scan reports its runtime through pytest durations; it never asserts it.
"""

import ast
from collections import Counter
from io import StringIO
import operator
from pathlib import Path, PurePosixPath
import re
import tokenize
from typing import NamedTuple
import unicodedata


TIMING_KINDS = frozenset({"sleep", "wait", "join", "get", "put", "communicate", "run", "settimeout"})


class TimingIdentity(NamedTuple):
    path: str
    qualname: str
    kind: str


class TimingScan(NamedTuple):
    findings: Counter
    files: int
    skipped: int


# The prefilter only skips files lacking all subject spellings after Python's
# identifier normalization. It does not decide whether a finding exists.
def timing_source_needs_ast(source):
    if "pb-timing-ok" in source:
        return True
    source = unicodedata.normalize("NFKC", source).replace("\\\n", "").replace("\\\r\n", "")
    return any(word in source for word in (*TIMING_KINDS, "time", "timeout", "pb-timing-ok"))


def _constant_timeout(node):
    """Numeric literals and arithmetic only; names and calls are dynamic."""
    if isinstance(node, ast.Constant) and type(node.value) in {int, float, bool}:
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        value = _constant_timeout(node.operand)
        if value is not None:
            return -value if isinstance(node.op, ast.USub) else value
    if isinstance(node, ast.BinOp) and type(node.op) in _TIMEOUT_ARITHMETIC:
        left, right = _constant_timeout(node.left), _constant_timeout(node.right)
        if left is not None and right is not None:
            try:
                return _TIMEOUT_ARITHMETIC[type(node.op)](left, right)
            except ArithmeticError:
                pass
    return None


_TIMEOUT_ARITHMETIC = {
    ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
    ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv, ast.Mod: operator.mod,
}


def _queue_annotation_name(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            node = ast.parse(node.value, mode="eval").body
        except SyntaxError:
            return None
    while isinstance(node, ast.Subscript):
        node = node.value
    return ast.unparse(node) if node is not None else None


def _queue_boolean_expression(node, known_boolean):
    return (node is not None and known_boolean(node)
            or isinstance(node, ast.Constant) and type(node.value) is bool
            or isinstance(node, ast.Compare)
            or isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not))


def _queue_get_calls(tree):
    nodes = list(ast.walk(tree))
    queue_types, receivers, booleans = set(), set(), set()
    type_names = {"Queue", "SimpleQueue", "LifoQueue", "PriorityQueue"}
    for node in nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "queue":
                    queue_types.update(f"{alias.asname or alias.name}.{name}" for name in type_names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == "queue":
            queue_types.update(alias.asname or alias.name for alias in node.names
                               if alias.name in type_names)

    def is_boolean(node):
        return _queue_boolean_expression(node, lambda value: ast.unparse(value) in booleans)

    for node in nodes:
        if isinstance(node, (ast.arg, ast.AnnAssign)):
            name = node.arg if isinstance(node, ast.arg) else ast.unparse(node.target)
            annotation = _queue_annotation_name(node.annotation)
            if annotation in queue_types:
                receivers.add(name)
            if annotation == "bool":
                booleans.add(name)
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            value = node.value
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names = {ast.unparse(target) for target in targets}
            if isinstance(value, ast.Call) and _queue_annotation_name(value.func) in queue_types:
                receivers.update(names)
            if is_boolean(value):
                booleans.update(names)
    return {node for node in nodes if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute) and node.func.attr == "get"
            and (ast.unparse(node.func.value) in receivers
                 or isinstance(node.func.value, ast.Call)
                 and _queue_annotation_name(node.func.value.func) in queue_types
                 or node.args and is_boolean(node.args[0]))}


def _timing_call_kind(call, subprocess_modules, subprocess_runs, queue_get_calls):
    callee = call.func
    if isinstance(callee, ast.Name) and callee.id in subprocess_runs:
        method = "run"
    elif isinstance(callee, ast.Attribute):
        method = callee.attr
        if method == "run" and not (
            isinstance(callee.value, ast.Name) and callee.value.id in subprocess_modules
        ):
            return None
    else:
        return None
    if method not in TIMING_KINDS - {"sleep"}:
        return None
    bounds = [kw.value for kw in call.keywords if kw.arg == "timeout"]
    # The positional slots are timeout for wait/join/settimeout, after input
    # for communicate, and after the blocking flag for Queue.get/put.
    position = {"wait": 0, "join": 0, "settimeout": 0, "communicate": 1,
                "get": 1, "put": 2}.get(method)
    if method == "get" and call not in queue_get_calls:
        position = None
    if position is not None and len(call.args) > position:
        bounds.append(call.args[position])
    if method == "settimeout":
        bounds.extend(kw.value for kw in call.keywords if kw.arg == "value")
    return method if any((value := _constant_timeout(bound)) is not None and value < 60
                         for bound in bounds) else None


class _TimingReferences(ast.NodeVisitor):
    """Identify syntax sites, without evaluating bindings or deferred code."""

    def __init__(self, tree):
        self.qualname = "<module>"
        self.references = []
        self.time_modules = set()
        self.subprocess_modules = set()
        self.subprocess_runs = set()
        self.zero_callees = set()
        self.queue_get_calls = _queue_get_calls(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                self.time_modules.update(alias.asname or alias.name for alias in node.names
                                         if alias.name == "time")
                self.subprocess_modules.update(alias.asname or alias.name for alias in node.names
                                               if alias.name == "subprocess")
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == "subprocess":
                self.subprocess_runs.update(alias.asname or alias.name for alias in node.names
                                            if alias.name == "run")
            elif (isinstance(node, ast.Call) and len(node.args) == 1 and not node.keywords
                  and isinstance(node.args[0], ast.Constant)
                  and type(node.args[0].value) in {int, float} and node.args[0].value == 0):
                self.zero_callees.add(node.func)

    def visit_ImportFrom(self, node):
        if node.level == 0 and node.module == "time":
            for alias in node.names:
                if alias.name in {"sleep", "*"}:
                    self.references.append((self.qualname, node.lineno, "sleep"))

    def visit_Attribute(self, node):
        if (node.attr == "sleep" and isinstance(node.value, ast.Name)
                and node.value.id in self.time_modules and node not in self.zero_callees):
            self.references.append((self.qualname, node.lineno, "sleep"))
        self.generic_visit(node)

    def visit_Call(self, node):
        kind = _timing_call_kind(node, self.subprocess_modules, self.subprocess_runs,
                                 self.queue_get_calls)
        if kind:
            self.references.append((self.qualname, node.lineno, kind))
        self.generic_visit(node)

    def _definition(self, node):
        for field, value in ast.iter_fields(node):
            if field != "body":
                for child in value if isinstance(value, list) else [value]:
                    if isinstance(child, ast.AST):
                        self.visit(child)
        outer = self.qualname
        self.qualname = node.name if outer == "<module>" else f"{outer}.{node.name}"
        for child in node.body:
            self.visit(child)
        self.qualname = outer

    visit_FunctionDef = _definition
    visit_AsyncFunctionDef = _definition
    visit_ClassDef = _definition


def scan_timing_source(source, path="tests/snippet.py"):
    """Count syntax sites by identity; only actual comment tokens are pragmas."""
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        raise ValueError(f"{path}:{exc.lineno}: syntax error: {exc.msg}") from exc
    visitor = _TimingReferences(tree)
    visitor.visit(tree)
    references = visitor.references
    pragmas, errors = {}, []
    if "pb-timing-ok" in source:
        for token in tokenize.generate_tokens(StringIO(source).readline):
            if token.type != tokenize.COMMENT or not re.match(r"#\s*pb-timing-ok\b", token.string):
                continue
            line = token.start[0]
            match = re.fullmatch(r"#\s*pb-timing-ok:\s*(\S.*)", token.string)
            if not match:
                errors.append(f"{path}:{line}: pb-timing-ok requires a reason")
            pragmas[line] = match is not None
        for line in pragmas.keys() - {line for _, line, _ in references}:
            errors.append(f"{path}:{line}: stale pb-timing-ok; no in-scope reference on this line")
    if errors:
        raise ValueError("\n".join(errors))
    return Counter(TimingIdentity(path, qualname, kind) for qualname, line, kind in references
                   if not pragmas.get(line))


def scan_timing_tests(repository):
    counts, files, skipped = Counter(), 0, 0
    for path in sorted((repository / "tests").rglob("*.py")):
        files += 1
        source = path.read_text(encoding="utf-8")
        if timing_source_needs_ast(source):
            counts.update(scan_timing_source(source, path.relative_to(repository).as_posix()))
        else:
            skipped += 1
    return TimingScan(counts, files, skipped)


def parse_timing_allowlist(contents):
    """Read path::qualname kind count #issue, rejecting ambiguous debt entries."""
    entries = {}
    for number, row in enumerate(contents.splitlines(), 1):
        if not row.strip() or row.lstrip().startswith("#"):
            continue
        label = f"timing_allowlist.txt:{number}"
        fields = row.split()
        if len(fields) != 4 or fields[0].count("::") != 1:
            raise ValueError(f"{label}: expected path::qualname kind count #N")
        identity, kind, count, issue = fields
        path, qualname = identity.split("::")
        if (not path.startswith("tests/") or not path.endswith(".py")
                or PurePosixPath(path).as_posix() != path
                or any(part in {".", ".."} for part in path.split("/"))
                or any(char in path for char in "*?[]\\:")
                or any(ord(char) < 32 for char in path)):
            raise ValueError(f"{label}: malformed repository-relative Python path")
        if qualname != "<module>" and not all(part.isidentifier() for part in qualname.split(".")):
            raise ValueError(f"{label}: malformed qualname")
        if kind not in TIMING_KINDS:
            raise ValueError(f"{label}: unknown timing kind {kind!r}")
        if not re.fullmatch(r"[1-9][0-9]*", count):
            raise ValueError(f"{label}: count must be a positive integer")
        if not re.fullmatch(r"#[1-9][0-9]*", issue):
            raise ValueError(f"{label}: missing or malformed issue reference #N")
        key = TimingIdentity(path, qualname, kind)
        if key in entries:
            raise ValueError(f"{label}: duplicate identity {identity} {kind}")
        entries[key] = (int(count), issue)
    return entries


def validate_timing_allowlist(counts, contents):
    entries = parse_timing_allowlist(contents)
    errors = []
    for key in sorted(counts.keys() | entries.keys()):
        found = counts.get(key, 0)
        allowed = entries.get(key, (0, None))[0]
        label = f"{key.path}::{key.qualname} {key.kind}"
        if found > allowed:
            errors.append(f"{label}: new offender/count mismatch, found {found}, listed {allowed}; "
                          "remove the timing dependency (allowlist growth requires PB lead approval)")
        elif found < allowed:
            errors.append(f"{label}: stale entry/count mismatch, found {found}, listed {allowed}; "
                          "lower or remove its allowlist entry")
    if errors:
        raise ValueError("\n".join(errors))


def test_repository_has_no_unallowlisted_timing_calls(capsys):
    repository = Path(__file__).resolve().parents[1]
    scan = scan_timing_tests(repository)
    validate_timing_allowlist(scan.findings,
                              (repository / "tests/timing_allowlist.txt").read_text(encoding="utf-8"))
    with capsys.disabled():
        print(f"timing-prefilter: files={scan.files} parsed={scan.files - scan.skipped} skipped={scan.skipped}")


POSITIVE_TIMING_CASES = [
    ("import time", "time.sleep(1)", "sleep"),
    ("import time as clock", "clock.sleep(delay)", "sleep"),
    ("from time import sleep", "sleep(1)", "sleep"),
    ("from time import sleep as nap", "nap(secs=delay)", "sleep"),
    ("from time import sleep as \uff4e\uff41\uff50", "\uff4e\uff41\uff50(1)", "sleep"),
    ("from time import sleep as nap\u0301", "nap\u0301(1)", "sleep"),
    ("from time import \uff53\uff4c\uff45\uff45\uff50 as nap", "nap(1)", "sleep"),
    *[("import time", f"time.sleep({value})", "sleep") for value in (
        "-1", "-0.1", "+0", "-0.0", "False", "None", "1 - 1", "settings.delay", "duration()")],
    *[("", expression, "wait") for expression in (
        "event.wait(0)",
        "event.wait(timeout=0)", "event.wait(timeout=False)")],
    *[("", expression, "join") for expression in (
        "thread.join(5)", "thread.join(0.0)", "thread.join(-1)", "thread.join(+1)",
        "thread.join(timeout=0)",
        "thread.join((+.5))", "thread.join(0x10)",
        "thread.join(\n# argument\n(-1))")],
    *[("", expression, "get") for expression in (
        "q.get(timeout=0)", "q.get(timeout=False)",
        "q.get(True, 5)", "q.get((True), 5)",
        "q.get(\n# blocking\nFalse, 5)")],
    ("", "(event. # attribute\nwait # call\n(1))", "wait"),
    ("", "event.\uff57\uff41\uff49\uff54(1)", "wait"),
    ("", "event.\\\nwait(1)", "wait"),
    ("", "q.get(\uff54\uff49\uff4d\uff45\uff4f\uff55\uff54=1)", "get"),
]


def test_every_in_scope_timing_form():
    for imports, expression, kind in POSITIVE_TIMING_CASES:
        assert scan_timing_source(f"{imports}\n{expression}\n") == {
            TimingIdentity("tests/snippet.py", "<module>", kind): 1}, expression


def test_prefilter_keeps_every_in_scope_positive_snippet():
    for imports, expression, _ in POSITIVE_TIMING_CASES:
        assert timing_source_needs_ast(f"{imports}\n{expression}\n"), expression


def test_full_and_prefiltered_scans_agree_on_fixed_repository_sample():
    repository = Path(__file__).resolve().parents[1]
    sample = (
        "tests/conftest.py", "tests/test_no_timing_sensitive_tests.py",
        "tests/test_profile_window_deadline.py", "tests/test_c26_progress_characterization.py",
        "tests/test_design_doc_line_references.py",
    )
    full, filtered = Counter(), Counter()
    for relative in sample:
        source = (repository / relative).read_text(encoding="utf-8")
        full.update(scan_timing_source(source, relative))
        if timing_source_needs_ast(source):
            filtered.update(scan_timing_source(source, relative))
    assert full == filtered


def test_untimed_unrelated_and_policy_out_of_scope_forms():
    cases = [
        "time.sleep(0)", "time.sleep(0.0)",
        "clock.sleep(5)", "event.wait()", "event.wait(timeout=None)",
        "thread.join()", "thread.join(None)", "thread.join(timeout=None)",
        "thread.join(delay)", "', '.join(items)", "sep.join(parts)",
        "d.get('k', 5)", "q.get(True)", "q.get(timeout=None)",
        "asyncio.sleep(1)", "asyncio.wait_for(coro(), timeout=1)",
        "subprocess.Popen(command, timeout=1)",
        "select.select(read, write, error, 1)",
        "poller.poll(1000)",
        "getattr(time, 'sleep')(1)", "'time.sleep(5)'", "# time.sleep(5)",
        "while time.monotonic() < deadline:\n    check()",
        "assert time.monotonic() - start < 1",
    ]
    for expression in cases:
        assert not scan_timing_source("import time, asyncio, subprocess, select, functools\n" + expression), expression


def test_import_scopes_and_qualnames_are_distinct_and_line_independent():
    source = """import time as clock
clock.sleep(1)
class Example:
    clock.sleep(1)
    async def test_one(self):
        clock.sleep(1)
        clock.sleep(2)
        def inner():
            from time import sleep as nap
            nap(1)
    def test_two(self):
        clock.sleep(1)
def shadowed(clock):
    clock.sleep(1)
def other_import():
    from fake_clock import sleep as nap
    nap(1)
def unrelated():
    nap(1)
(lambda clock: clock.sleep(5))(fake_clock)
"""
    expected = {TimingIdentity("tests/snippet.py", scope, "sleep"): count for scope, count in (
        ("<module>", 2), ("shadowed", 1), ("Example", 1), ("Example.test_one", 2),
        ("Example.test_one.inner", 1), ("Example.test_two", 1))}
    assert scan_timing_source(source) == expected
    assert scan_timing_source("\n\n# unrelated edit\n" + source) == expected


def test_scan_includes_conftest_and_support_but_excludes_src_and_tools(tmp_path, monkeypatch):
    import pytest

    source = "raise RuntimeError('must not execute')\nimport time\ntime.sleep(1)\n"
    paths = ("tests/conftest.py", "tests/support/nested/helper.py", "src/example.py", "tools/example.py")
    for relative in paths:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")
    (tmp_path / "tests/test_example.py").write_text(
        "from src.example import helper\nfrom tools.example import other\nhelper()\nother()\n",
        encoding="utf-8")
    parsed = []
    real_parse = ast.parse

    def record_parse(source, filename):
        parsed.append(filename)
        return real_parse(source, filename=filename)

    monkeypatch.setattr(ast, "parse", record_parse)
    scan = scan_timing_tests(tmp_path)
    assert scan.findings == {
        TimingIdentity(path, "<module>", "sleep"): 1 for path in paths[:2]}
    assert scan.files == 3 and scan.skipped == 1
    assert sorted(parsed) == sorted(paths[:2])
    (tmp_path / "tests/broken.py").write_text("def broken(: # sleep\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"tests/broken.py:1: syntax error"):
        scan_timing_tests(tmp_path)


def test_skipped_file_still_rejects_stale_allowlist_debt(tmp_path):
    import pytest

    path = tmp_path / "tests/test_clean.py"
    path.parent.mkdir()
    path.write_text("value = 2\n", encoding="utf-8")
    scan = scan_timing_tests(tmp_path)
    assert scan.skipped == 1
    with pytest.raises(ValueError, match="stale entry/count mismatch, found 0, listed 1"):
        validate_timing_allowlist(scan.findings, "tests/test_clean.py::<module> sleep 1 #2035")


def test_controlled_timing_pragma_exempts_only_the_call_line():
    source = """import time
time.sleep(5)  # pb-timing-ok: monkeypatch replaced sleep with the fake clock
time.sleep(5)
"""
    assert scan_timing_source(source) == {TimingIdentity("tests/snippet.py", "<module>", "sleep"): 1}


def test_scanner_keeps_decorators_defaults_and_multiline_or_unicode_calls():
    source = """@event.wait(timeout=1)
def example(value=queue.get(True, 5)):
    thread.join(
        1
    )
"""
    assert timing_source_needs_ast(source)
    assert scan_timing_source(source) == {
        TimingIdentity("tests/snippet.py", "<module>", "wait"): 1,
        TimingIdentity("tests/snippet.py", "<module>", "get"): 1,
        TimingIdentity("tests/snippet.py", "example", "join"): 1,
    }
    for expression in ("event.\uff57\uff41\uff49\uff54(1)", "event.\\\nwait(1)", "event.wait(1) # \u2603"):
        assert timing_source_needs_ast(expression)
        assert scan_timing_source(expression) == {
            TimingIdentity("tests/snippet.py", "<module>", "wait"): 1}
    for expression, kind in (("(event.wait)(5)", "wait"), ("(thread.join)(5)", "join"),
                             ("((q.get) )(True, 5)", "get")):
        assert timing_source_needs_ast(expression), expression
        assert scan_timing_source(expression) == {
            TimingIdentity("tests/snippet.py", "<module>", kind): 1}


def test_empty_or_stale_pragmas_fail():
    import pytest

    cases = [
        ("import time\ntime.sleep(1)  # pb-timing-ok:", "requires a reason"),
        ("import time\ntime.sleep(1)  # pb-timing-ok:   ", "requires a reason"),
        ("import time\ntime.sleep(1)  # pb-timing-ok", "requires a reason"),
        ("# pb-timing-ok\u0301: fake clock", "requires a reason"),
        ("# pb-timing-ok: fake clock", "stale pb-timing-ok"),
        ("clock.sleep(1)  # pb-timing-ok: fake clock", "stale pb-timing-ok"),
        ("import time\ntime.sleep(0)  # pb-timing-ok: fake clock", "stale pb-timing-ok"),
        ("import time\n# pb-timing-ok: fake clock\ntime.sleep(1)", "stale pb-timing-ok"),
    ]
    for source, error in cases:
        assert timing_source_needs_ast(source)
        with pytest.raises(ValueError, match=error):
            scan_timing_source(source)


def test_pragma_text_in_strings_cannot_exempt_calls_or_become_stale():
    assert not scan_timing_source("text = '# pb-timing-ok: fake clock'")
    assert scan_timing_source("import time\ntime.sleep('# pb-timing-ok: fake clock')") == {
        TimingIdentity("tests/snippet.py", "<module>", "sleep"): 1}


def test_allowlist_matches_both_ways_and_tracks_counts_by_identity():
    import pytest

    counts = scan_timing_source("import time\ntime.sleep(1)\ntime.sleep(2)\n")
    row = "tests/snippet.py::<module> sleep 2 #2035\n"
    validate_timing_allowlist(counts, row)
    with pytest.raises(ValueError, match="new offender"):
        validate_timing_allowlist(counts, "")
    with pytest.raises(ValueError, match="stale entry"):
        validate_timing_allowlist({}, row)
    for count, error in ((1, "new offender/count mismatch"), (3, "stale entry/count mismatch")):
        with pytest.raises(ValueError, match=error):
            validate_timing_allowlist(counts, row.replace("sleep 2", f"sleep {count}"))
    with pytest.raises(ValueError, match="new offender"):
        validate_timing_allowlist(counts, row.replace("<module>", "another_test"))
    with pytest.raises(ValueError, match="duplicate identity"):
        parse_timing_allowlist(row + row)
    assert parse_timing_allowlist("# debt\n\n" + row) == {
        TimingIdentity("tests/snippet.py", "<module>", "sleep"): (2, "#2035")}


def test_allowlist_rejects_missing_issues_and_malformed_lines():
    import pytest

    cases = [
        "bad line", "tests/x.py::<module> sleep 1", "tests/x.py::<module> sleep 1 2035",
        "tests/x.py::<module> sleep 1 #0", "tests/x.py::<module> sleep 0 #2035",
        "tests/x.py::<module> sleep -1 #2035", "tests/x.py::<module> sleep 1.5 #2035",
        "tests/x.py::<module> other 1 #2035", "tests/x.py:: sleep 1 #2035",
        "tests/x.py::A..test_x sleep 1 #2035", "tests/x.py::test_x::other sleep 1 #2035",
        *[f"{path}::<module> sleep 1 #2035" for path in (
            "/tests/x.py", "tests/../x.py", "tests//x.py", "tests/./x.py",
            "tests/*.py", "tests/x?.py", "tests/[x].py", "tests/x.txt", "src/x.py", "tests\\x.py")],
    ]
    for row in cases:
        with pytest.raises(ValueError, match="expected|malformed|positive integer|unknown"):
            parse_timing_allowlist(row)


# Amended #1771 criteria. Snippets are parsed, never executed.
def test_amended_generator_lambda_walrus_reproducer():
    source = """from time import sleep as nap
callbacks = ((nap := print, lambda: nap(1))[1] for _ in (0,))
callback = next(callbacks)
from time import sleep as nap
callback()
"""
    assert scan_timing_source(source) == {TimingIdentity("tests/snippet.py", "<module>", "sleep"): 2}


def test_amended_sleep_routes_are_positive():
    for source in (
        "from time import sleep as nap",
        "import time\ncallback = time.sleep",
        "import time as clock\nregister(clock.sleep)",
        "import time\nfunctools.partial(time.sleep, 1)",
        "from time import sleep as nap\ncallback = lambda: nap",
    ):
        assert scan_timing_source(source), source


def test_amended_generous_and_dynamic_timeout_bounds():
    for method in ("wait", "join", "get", "put", "communicate", "run"):
        for bound in ("60", "60.0", "120", "30 * 2", "None", "bound"):
            source = f"subject.{method}(timeout={bound})"
            assert not scan_timing_source(source), source
    for bound in ("60", "120", "None", "bound"):
        source = f"sock.settimeout({bound})"
        assert not scan_timing_source(source), source


def test_amended_short_timeout_subjects():
    for expression in (
        "event.wait(59)", "condition.wait(timeout=0.5)",
        "thread.join(timeout=30)", "queue.get(True, 5)",
        "queue.put(item, True, 5)", "queue.put(item, timeout=5)",
        "process.wait(timeout=5)", "process.communicate(timeout=5)",
        "subprocess.run(command, timeout=5)", "sock.settimeout(5)",
        "sock.settimeout(value=5)", "event.wait(timeout=30 + 29)",
    ):
        assert scan_timing_source("import subprocess\n" + expression), expression


def test_amended_dynamic_forms_are_out_of_scope():
    for source in (
        "import time\ngetattr(time, 'sl' + 'eep')(1)",
        "exec('import time; time.sleep(1)')",
        "eval('time.sleep(1)')",
        "import importlib\nimportlib.import_module('time').sleep(1)",
        "event.wait(timeout=settings.bound)",
        "sock.settimeout(compute_bound())",
    ):
        assert not scan_timing_source(source), source


def render_shrunk_timing_allowlist(counts, contents):
    """Render debt reductions atomically; never admit a new identity or count."""
    entries = parse_timing_allowlist(contents)
    for key, count in counts.items():
        if count > entries.get(key, (0, None))[0]:
            raise ValueError(f"allowlist growth refused: {key.path}::{key.qualname} {key.kind}")
    rows = ["# path::qualname kind count #issue; shrink only (PB #1771)"]
    for key, count in sorted(counts.items()):
        rows.append(f"{key.path}::{key.qualname} {key.kind} {count} {entries[key][1]}")
    return "\n".join(rows) + "\n"


def test_shrink_regeneration_preserves_links_and_refuses_growth():
    import pytest

    contents = ("tests/a.py::test_one sleep 2 #2246\n"
                "tests/a.py::test_gone wait 1 #2247\n")
    expected = ("# path::qualname kind count #issue; shrink only (PB #1771)\n"
                "tests/a.py::test_one sleep 1 #2246\n")
    counts = {TimingIdentity("tests/a.py", "test_one", "sleep"): 1}
    assert render_shrunk_timing_allowlist(counts, contents) == expected
    assert render_shrunk_timing_allowlist(counts, expected) == expected
    for growth in (
        {TimingIdentity("tests/a.py", "test_one", "sleep"): 3},
        {TimingIdentity("tests/a.py", "test_new", "sleep"): 1},
    ):
        with pytest.raises(ValueError, match="growth refused"):
            render_shrunk_timing_allowlist(growth, contents)


def test_syntactic_sleep_subject_and_zero_yield_exception():
    for source, count in (
        ("from time import sleep as nap; nap(0)", 1),
        ("from time import sleep as nap; nap = print; nap(0)", 1),
        ("import time; time.sleep(0)", 0),
        ("import time as t; t.sleep(0.0)", 0),
        ("import time; time.sleep(secs=0)", 1),
        ("import time; time.sleep(0, unexpected=True)", 1),
        ("import time; time.sleep(False)", 1),
        ("import time; time.sleep(-0)", 1),
        ("import time as t; x = (lambda: t.sleep for _ in (0,))", 1),
        ("import time; callback = (nap := time.sleep)", 1),
        ("import time; time = fake_clock; time.sleep(1)", 1),
    ):
        expected = {TimingIdentity("tests/snippet.py", "<module>", "sleep"): count} if count else {}
        assert scan_timing_source(source) == expected, source


def test_all_timeout_slots_and_subprocess_aliases():
    for expression, kind in (
        ("condition.wait(30 * 1)", "wait"),
        ("thread.join(-1)", "join"),
        ("queue.get(False, 59.9)", "get"),
        ("queue.put(item, True, timeout=0)", "put"),
        ("process.communicate(data, 5)", "communicate"),
        ("sp.run(command, timeout=1)", "run"),
        ("launch(command, timeout=1)", "run"),
        ("socket.settimeout(60 / 2)", "settimeout"),
    ):
        source = "import subprocess as sp\nfrom subprocess import run as launch\n" + expression
        assert scan_timing_source(source) == {TimingIdentity("tests/snippet.py", "<module>", kind): 1}
    for source in (
        "queue.get('key', 1)", "queue.put(item, True, 60)",
        "process.communicate(data, 60)", "subject.run(timeout=1)",
        "thread.join(60)", "event.wait(30 * 2)", "event.wait(timeout=None)",
        "event.wait(timeout=1 / 0)", "clock.sleep(1)",
    ):
        assert not scan_timing_source(source), source


def test_queue_get_timeout_does_not_depend_on_block_expression():
    for block in ("blocking", "settings.blocking", "should_block()", "True", "False"):
        source = f"from queue import Queue\nqueue = Queue()\nqueue.get({block}, 5)"
        assert scan_timing_source(source) == {
            TimingIdentity("tests/snippet.py", "<module>", "get"): 1}, source
        assert not scan_timing_source(f"queue.get({block}, 60)")
    for source in ("mapping.get('key', 5)", "mapping.get(key, default)",
                   "mapping.get(key, 5)", "mapping = {}\nmapping.get(key, 0)"):
        assert not scan_timing_source(source), source


def test_queue_get_boolean_bindings_and_imported_types():
    for source in (
        "blocking = True\nq.get(blocking, 5)",
        "blocking: bool\nq.get(blocking, 5)",
        "from queue import Queue as Q\nq = Q()\nq.get(should_block(), 5)",
        "import queue as queues\nq: queues.Queue[int]\nq.get(settings.blocking, 5)",
        "from queue import SimpleQueue\ndef check(q: 'SimpleQueue'):\n q.get(blocking, 5)",
        "from queue import PriorityQueue\nPriorityQueue().get(blocking, 5)",
    ):
        assert sum(scan_timing_source(source).values()) == 1, source


def test_queue_put_timeout_does_not_depend_on_block_expression():
    for block in ("blocking", "settings.blocking", "should_block()", "True", "False"):
        source = f"blocking = True\nqueue.put(item, {block}, 5)"
        assert scan_timing_source(source) == {
            TimingIdentity("tests/snippet.py", "<module>", "put"): 1}, source
        assert not scan_timing_source(f"queue.put(item, {block}, 60)")


def test_wildcard_time_import_is_a_sleep_route():
    for source in ("from time import *\nsleep(1)", "from time import *"):
        assert timing_source_needs_ast(source), source
        assert scan_timing_source(source) == {
            TimingIdentity("tests/snippet.py", "<module>", "sleep"): 1}, source


def test_queue_receiver_evidence_does_not_leak_between_functions():
    source = """from queue import Queue
def test_queue():
    q = Queue()
def test_mapping(key):
    q = {}
    return q.get(key, 5)
"""
    assert not scan_timing_source(source)


def test_boolean_evidence_does_not_leak_between_functions():
    source = """def test_queue(q):
    blocking = True
    q.get(blocking, 5)
def test_mapping(mapping, blocking):
    return mapping.get(blocking, 5)
"""
    assert scan_timing_source(source) == {
        TimingIdentity("tests/snippet.py", "test_queue", "get"): 1}


def test_queue_evidence_respects_local_imports_and_shadowed_bindings():
    for source in (
        "def queue_scope():\n from queue import Queue\ndef mapping_scope(Queue, key):\n q = Queue()\n q.get(key, 5)",
        "from queue import Queue\ndef mapping_scope(Queue, key):\n q = Queue()\n q.get(key, 5)",
        "from queue import Queue\nq = Queue()\ndef mapping_scope(q, key):\n q.get(key, 5)",
        "blocking = True\ndef mapping_scope(mapping, blocking):\n mapping.get(blocking, 5)",
        "blocking = True\ndef outer():\n def inner(mapping, blocking):\n  mapping.get(blocking, 5)",
    ):
        assert not scan_timing_source(source), source


def test_queue_evidence_remains_visible_to_closures():
    source = """from queue import Queue as Q
def outer():
    q = Q()
    blocking = True
    def inner(mapping):
        q.get(should_block(), 5)
        mapping.get(blocking, 5)
"""
    assert scan_timing_source(source) == {
        TimingIdentity("tests/snippet.py", "outer.inner", "get"): 2}
