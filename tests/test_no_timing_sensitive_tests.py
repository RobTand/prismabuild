"""D-19: tests validate correctness, without depending on the host's clock.

Scan every Python file in tests/, including conftest and support modules.
The subject is imported time.sleep except literal 0 or 0.0, positional or
timed .wait, timed or numeric-literal .join, and timed .get or .get(bool, x).
The allowlist counts each kind per path::qualname, never per line number.
It must match exactly in both directions. Growing it is reviewer-blocking
unless the PB lead approves, as with test_duplication_baseline.py. This test
reads no Git history; allowlist growth is a visible diff for review.

Only controlled timing that does not wait on the host's real clock may use
an inline '# pb-timing-ok: <reason>' on the call's starting line. Reviewers
must check the reason. Empty reasons and pragmas without a subject fail.
Hang backstops such as join(timeout=60) belong in the allowlist, not pragmas;
the harness --test-timeout-s already bounds hangs.

Out of scope and NON-BLOCKING for review: asyncio.sleep and asyncio.wait_for;
subprocess timeout= and communicate(timeout=); select and poll timeouts;
time.monotonic deadline loops; elapsed-time assertions; indirect calls such
as functools.partial(time.sleep, 1) and getattr(time, 'sleep'); and sleeps in
src/ or tools/ code called by tests. These are explicit policy limits, not
reasons to reject this guard when a reviewer finds an exotic uncovered form.
"""

import ast
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from io import StringIO
import multiprocessing
import os
from pathlib import Path, PurePosixPath
import re
import tokenize
from typing import NamedTuple
import unicodedata


TIMING_KINDS = frozenset({"sleep", "wait", "join", "get"})
TIMING_METHODS = TIMING_KINDS - {"sleep"}
TIMING_WORDS = re.compile(r"\b(?:" + "|".join(sorted(TIMING_KINDS)) + r")\b")


class TimingIdentity(NamedTuple):
    path: str
    qualname: str
    kind: str


class TimingScan(NamedTuple):
    findings: Counter
    files: int
    skipped: int


class _TimingScope:
    def __init__(self, qualname, parent=None, is_class=False):
        self.qualname = qualname
        self.parent = parent
        self.is_class = is_class
        self.imports = {}
        self.bound = set()
        self.calls = []

    def resolve(self, name):
        if name in self.bound:
            return None
        if name in self.imports:
            return self.imports[name]
        return self.parent.resolve(name) if self.parent else None


class _TimingScopes:
    """Collect imports and candidate calls without importing test modules."""

    def __init__(self):
        self.scopes = [_TimingScope("<module>")]

    def _nested(self, node, outer):
        name = getattr(node, "name", None)
        if name:
            outer.bound.add(name)
        qualname = outer.qualname
        if name:
            qualname = name if outer.qualname == "<module>" else f"{outer.qualname}.{name}"
        scope = _TimingScope(qualname, outer.parent if outer.is_class else outer,
                             isinstance(node, ast.ClassDef))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            args = node.args
            scope.bound.update(arg.arg for arg in (
                args.posonlyargs + args.args + args.kwonlyargs
                + ([args.vararg] if args.vararg else [])
                + ([args.kwarg] if args.kwarg else [])))
        self.scopes.append(scope)
        return scope

    def visit(self, tree, relevant_lines=None):
        # Avoid NodeVisitor's per-node dispatch and visiting operator/context
        # leaves. On the full tests tree those costs dwarf classification.
        work = [(self.scopes[0], [tree])]
        scope_types = {ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda}
        value_types = {ast.Attribute, ast.Expr, ast.keyword}
        sequence_types = {ast.List, ast.Tuple, ast.Set}
        while work:
            scope, pending = work.pop()
            while pending:
                node = pending.pop()
                if node is None or isinstance(node, str):
                    continue
                kind = type(node)
                if kind is ast.Name:
                    if isinstance(node.ctx, (ast.Store, ast.Del)):
                        scope.bound.add(node.id)
                    continue
                if kind is ast.Constant:
                    continue
                if relevant_lines is not None and kind not in scope_types:
                    start = getattr(node, "lineno", None)
                    if start and relevant_lines[node.end_lineno] == relevant_lines[start - 1]:
                        continue
                if kind in scope_types:
                    child = self._nested(node, scope)
                    work.append((child, list(node.body) if isinstance(node.body, list) else [node.body]))
                    # Defaults, annotations, decorators and bases use the outer scope.
                    fields = (field for field in node._fields if field != "body")
                elif kind is ast.Import:
                    for alias in node.names:
                        scope.imports[alias.asname or alias.name.split(".")[0]] = (
                            "time" if alias.name == "time" else None)
                    continue
                elif kind is ast.ImportFrom:
                    for alias in node.names:
                        scope.imports[alias.asname or alias.name] = (
                            "time.sleep" if node.level == 0 and node.module == "time"
                            and alias.name == "sleep" else None)
                    continue
                elif kind is ast.Call:
                    if isinstance(node.func, ast.Name) or (
                            isinstance(node.func, ast.Attribute)
                            and node.func.attr in TIMING_KINDS):
                        scope.calls.append(node)
                    pending.append(node.func)
                    pending.extend(node.args)
                    pending.extend(node.keywords)
                    continue
                elif kind in value_types:
                    pending.append(node.value)
                    continue
                elif kind in sequence_types:
                    pending.extend(node.elts)
                    continue
                else:
                    fields = node._fields
                for field in fields:
                    value = getattr(node, field)
                    if isinstance(value, list):
                        pending.extend(reversed(value))
                    elif isinstance(value, ast.AST):
                        pending.append(value)


# These lexical cues overapproximate the subject immediately below. Imports of
# time.sleep necessarily contain "sleep", including every imported alias; all
# timed methods contain "timeout". Positional subjects need their method prefix
# and, for join/get, the start of a literal. Comments, parentheses and signs may
# precede that literal. False positives are harmless: only the AST makes findings.
_TIMING_MARKERS = ("sleep", "timeout", "pb-timing-ok")
_TRIVIA = r"(?:\s|#[^\n]*(?:\n|$))*"
_ARGUMENT_PREFIX = r"(?:\s|#[^\n]*(?:\n|$)|[()+-])*"
_NUMBER_PREFIX = r"(?:[0-9]|\.[0-9])"
# Closing parentheses may separate the method from its call: (event.wait)(1).
_METHOD_PREFIXES = {kind: rf"\.{_TRIVIA}{kind}(?:{_TRIVIA}\))*{_TRIVIA}\(" for kind in TIMING_METHODS}
_POSITIONAL_SUBJECTS = re.compile("|".join((
    _METHOD_PREFIXES["wait"],
    _METHOD_PREFIXES["join"] + _ARGUMENT_PREFIX + _NUMBER_PREFIX,
    _METHOD_PREFIXES["get"] + _ARGUMENT_PREFIX + r"(?:True|False)\b",
)))
_JOIN_PREFIX = re.compile(_METHOD_PREFIXES["join"])
_LATER_NUMBER = re.compile("," + _ARGUMENT_PREFIX + _NUMBER_PREFIX)


def timing_source_needs_ast(source):
    """Keep every possible subject or pragma; skipped files have zero findings."""
    if any(marker in source for marker in _TIMING_MARKERS):
        return True
    if not source.isascii():
        source = unicodedata.normalize("NFKC", source)
    source = source.replace("\\\r\n", "").replace("\\\n", "")
    if any(marker in source for marker in _TIMING_MARKERS):
        return True
    if _POSITIONAL_SUBJECTS.search(source):
        return True
    # The classifier also accepts a numeric literal in a later join position.
    # This deliberately broad fallback preserves that subject without parsing.
    return bool(_JOIN_PREFIX.search(source) and _LATER_NUMBER.search(source))


def _timing_call_kind(call, scope):
    callee = call.func
    imported_sleep = (
        isinstance(callee, ast.Name) and scope.resolve(callee.id) == "time.sleep"
        or isinstance(callee, ast.Attribute) and callee.attr == "sleep"
        and isinstance(callee.value, ast.Name) and scope.resolve(callee.value.id) == "time")
    if imported_sleep:
        duration = next((kw.value for kw in call.keywords if kw.arg == "secs"),
                        call.args[0] if call.args else None)
        zero = (isinstance(duration, ast.Constant)
                and type(duration.value) in {int, float} and duration.value == 0)
        return None if zero else "sleep"
    if not isinstance(callee, ast.Attribute) or callee.attr not in TIMING_METHODS:
        return None
    timed = any(kw.arg == "timeout" and not (
        isinstance(kw.value, ast.Constant) and kw.value.value is None) for kw in call.keywords)
    if timed:
        return callee.attr
    if callee.attr == "wait" and call.args:
        return "wait"
    if callee.attr == "join":
        for arg in call.args:
            if isinstance(arg, ast.UnaryOp) and isinstance(arg.op, (ast.USub, ast.UAdd)):
                arg = arg.operand
            if isinstance(arg, ast.Constant) and type(arg.value) in {int, float, complex}:
                return "join"
    if callee.attr == "get" and len(call.args) == 2:
        first = call.args[0]
        if isinstance(first, ast.Constant) and type(first.value) is bool:
            return "get"
    return None


def _timing_import_names(tree):
    """Find candidate alias spellings through statement bodies, not expressions."""
    names = set()
    pending = [tree]
    containers = (ast.stmt, ast.ExceptHandler, ast.match_case)
    while pending:
        node = pending.pop()
        if isinstance(node, ast.Import):
            names.update(alias.asname or alias.name for alias in node.names if alias.name == "time")
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == "time":
            names.update(alias.asname or alias.name for alias in node.names if alias.name == "sleep")
        for field in node._fields:
            value = getattr(node, field)
            if isinstance(value, list):
                pending.extend(child for child in value if isinstance(child, containers))
            elif isinstance(value, containers):
                pending.append(value)
    return names


def scan_timing_source(source, path="tests/snippet.py"):
    """Return counts by identity, checking actual comment tokens for pragmas."""
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        raise ValueError(f"{path}:{exc.lineno}: syntax error: {exc.msg}") from exc
    # Skip walking subtrees whose source cannot
    # contain a subject method or imported alias spelling. Alias spellings also
    # keep every binding that could shadow them. Python normalizes identifiers
    # with NFKC. Non-ASCII aliases and explicit continuations use the full walk.
    relevant_lines = None
    if "\\\n" not in source and "\\\r\n" not in source:
        names = _timing_import_names(tree)
        if all(name.isascii() for name in names):
            words = re.compile(r"\b(?:" + "|".join(sorted(TIMING_KINDS | names)) + r")\b") if names else TIMING_WORDS
            relevant_lines = [0]
            for line in unicodedata.normalize("NFKC", source).split("\n"):
                relevant_lines.append(relevant_lines[-1] + bool(words.search(line)))
    scopes = _TimingScopes()
    scopes.visit(tree, relevant_lines)
    calls = [(scope.qualname, call.lineno, kind)
             for scope in scopes.scopes for call in scope.calls
             if (kind := _timing_call_kind(call, scope)) is not None]
    pragmas = {}
    errors = []
    if "pb-timing-ok" in source:
        for token in tokenize.generate_tokens(StringIO(source).readline):
            if token.type != tokenize.COMMENT or not re.match(r"#\s*pb-timing-ok\b", token.string):
                continue
            line = token.start[0]
            match = re.fullmatch(r"#\s*pb-timing-ok:\s*(\S.*)", token.string)
            if not match:
                errors.append(f"{path}:{line}: pb-timing-ok requires a reason")
            pragmas[line] = match is not None
        for line in pragmas.keys() - {line for _, line, _ in calls}:
            errors.append(f"{path}:{line}: stale pb-timing-ok; no in-scope call on this line")
    if errors:
        raise ValueError("\n".join(errors))
    return Counter(TimingIdentity(path, qualname, kind) for qualname, line, kind in calls
                   if not pragmas.get(line))


def _scan_timing_chunk(files):
    counts = Counter()
    skipped = 0
    for path, relative in files:
        source = path.read_text(encoding="utf-8")
        if timing_source_needs_ast(source):
            counts.update(scan_timing_source(source, relative))
        else:
            skipped += 1
    return TimingScan(counts, len(files), skipped)


def scan_timing_tests(repository):
    files = [(path, path.relative_to(repository).as_posix())
             for path in sorted((repository / "tests").rglob("*.py"))]
    # The single-process pre-filtered x86 scan still exceeds the budget. Use
    # at most two assigned CPUs without changing affinity or native threads.
    workers = min(2, len(os.sched_getaffinity(0)))
    if len(files) < 32 or workers == 1:
        return _scan_timing_chunk(files)
    counts = Counter()
    skipped = 0
    chunks = [files[index:index + 32] for index in range(0, len(files), 32)]
    with ProcessPoolExecutor(max_workers=workers,
                             mp_context=multiprocessing.get_context("spawn")) as pool:
        for result in pool.map(_scan_timing_chunk, chunks):
            counts.update(result.findings)
            skipped += result.skipped
    return TimingScan(counts, len(files), skipped)


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
        "event.wait(0)", "event.wait(None)", "event.wait(delay)",
        "event.wait(timeout=0)", "event.wait(timeout=False)", "event.wait(timeout=delay)")],
    *[("", expression, "join") for expression in (
        "thread.join(5)", "thread.join(0.0)", "thread.join(-1)", "thread.join(+1)",
        "thread.join(timeout=0)", "thread.join(timeout=delay)", "thread.join(timeout=60)",
        "thread.join(value, 5)", "thread.join((+.5))", "thread.join(0x10)",
        "thread.join(2j)", "thread.join(\n# argument\n(-1))")],
    *[("", expression, "get") for expression in (
        "q.get(timeout=0)", "q.get(timeout=False)", "q.get(timeout=delay)",
        "q.get(True, 5)", "q.get(False, delay)", "q.get((True), 5)",
        "q.get(\n# blocking\nFalse, delay)")],
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
        "time.sleep(0)", "time.sleep(0.0)", "time.sleep(secs=0)",
        "clock.sleep(5)", "event.wait()", "event.wait(timeout=None)",
        "thread.join()", "thread.join(None)", "thread.join(timeout=None)",
        "thread.join(delay)", "', '.join(items)", "sep.join(parts)",
        "d.get('k', 5)", "q.get(True)", "q.get(timeout=None)",
        "asyncio.sleep(1)", "asyncio.wait_for(coro(), timeout=1)",
        "subprocess.run(command, timeout=1)", "subprocess.Popen(command, timeout=1)",
        "process.communicate(timeout=1)", "select.select(read, write, error, 1)",
        "poller.poll(1000)", "functools.partial(time.sleep, 1)",
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
        ("<module>", 1), ("Example", 1), ("Example.test_one", 2),
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
    path.write_text("assert 1 + 1 == 2\n", encoding="utf-8")
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
    for expression, kind in (("(event.wait)(delay)", "wait"), ("(thread.join)(5)", "join"),
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
