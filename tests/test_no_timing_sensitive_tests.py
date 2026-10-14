import ast
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import re


@dataclass(frozen=True, order=True)
class TimingCall:
    path: str
    line: int
    kind: str


METHODS = {
    "threading.Event": "wait",
    "threading.Thread": "join",
    "queue.Queue": "get",
}
KINDS = {"time.sleep", *(f"{kind}.{method}" for kind, method in METHODS.items())}
ISSUE = re.compile(r"https://github\.com/[\w.-]+/[\w.-]+/issues/[1-9][0-9]*\Z")


class _Scope:
    def __init__(self, kind, body):
        self.kind = kind
        self.nodes = []
        self.children = []
        pending = list(body)
        while pending:
            node = pending.pop()
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                kind = "class" if isinstance(node, ast.ClassDef) else "function"
                self.children.append(_Scope(kind, node.body))
                for field, value in ast.iter_fields(node):
                    if field != "body":
                        if isinstance(value, ast.AST):
                            pending.append(value)
                        elif isinstance(value, list):
                            pending.extend(item for item in value if isinstance(item, ast.AST))
            elif isinstance(node, ast.Lambda):
                self.children.append(_Scope("function", [node.body]))
                pending.append(node.args)
            else:
                if isinstance(node, (ast.Import, ast.ImportFrom, ast.Assign, ast.AnnAssign,
                                     ast.Global, ast.Nonlocal, ast.Call)):
                    self.nodes.append(node)
                pending.extend(ast.iter_child_nodes(node))


def _imports(scope, inherited):
    aliases = {name: set(kinds) for name, kinds in inherited.items()}
    for node in scope.nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in {"time", "threading", "queue"}:
                    aliases.setdefault(alias.asname or alias.name, set()).add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            if node.module in {"time", "threading", "queue"}:
                for alias in node.names:
                    qualified = f"{node.module}.{alias.name}"
                    if qualified in METHODS or qualified == "time.sleep":
                        aliases.setdefault(alias.asname or alias.name, set()).add(qualified)
    return aliases


def _qualified(node, aliases):
    if isinstance(node, ast.Name):
        return aliases.get(node.id, set())
    if isinstance(node, ast.Attribute):
        return {f"{name}.{node.attr}" for name in _qualified(node.value, aliases)}
    return set()


def _receiver(node):
    if isinstance(node, ast.Name):
        return node.id
    if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
            and node.value.id in {"self", "cls"}):
        return (node.value.id, node.attr)
    return None


def _bindings(scope, aliases):
    bindings = defaultdict(set)
    external = {name for node in scope.nodes
                if isinstance(node, (ast.Global, ast.Nonlocal)) for name in node.names}
    for node in scope.nodes:
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and isinstance(node.value, ast.Call):
            kinds = _qualified(node.value.func, aliases) & METHODS.keys()
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                key = _receiver(target)
                if key is not None and key not in external:
                    bindings[key].update(kinds)
    return bindings


def _argument(call, position, keyword):
    for item in call.keywords:
        if item.arg == keyword:
            return item.value
    return call.args[position] if len(call.args) > position else None


def _nonpositive(node):
    sign = 1
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        sign, node = -1, node.operand
    return (isinstance(node, ast.Constant) and type(node.value) in {int, float}
            and sign * node.value <= 0)


def scan_source(source, path="tests/snippet.py"):
    """Inspect source text only; no scanned module is imported or executed."""
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        raise ValueError(f"{path}:{exc.lineno}: syntax error: {exc.msg}") from exc
    offenders = set()

    def inspect(scope, inherited, attributes):
        aliases = _imports(scope, inherited)
        bindings = _bindings(scope, aliases) if scope.kind != "class" else {}
        if scope.kind == "class":
            attributes = defaultdict(set)
            for child in scope.children:
                if child.kind == "function":
                    for key, kinds in _bindings(child, _imports(child, aliases)).items():
                        if isinstance(key, tuple):
                            attributes[key].update(kinds)
        for call in scope.nodes:
            if not isinstance(call, ast.Call):
                continue
            if "time.sleep" in _qualified(call.func, aliases):
                duration = _argument(call, 0, "secs")
                if duration is not None and not _nonpositive(duration):
                    offenders.add(TimingCall(path, call.lineno, "time.sleep"))
            if not isinstance(call.func, ast.Attribute):
                continue
            receiver = call.func.value
            if isinstance(receiver, ast.Call):
                kinds = _qualified(receiver.func, aliases) & METHODS.keys()
            else:
                key = _receiver(receiver)
                kinds = attributes.get(key, set()) if isinstance(key, tuple) else bindings.get(key, set())
            for kind in kinds:
                if call.func.attr != METHODS[kind]:
                    continue
                position = 1 if kind == "queue.Queue" else 0
                timeout = _argument(call, position, "timeout")
                if timeout is not None and not (isinstance(timeout, ast.Constant) and timeout.value is None):
                    offenders.add(TimingCall(path, call.lineno, f"{kind}.{METHODS[kind]}"))
        for child in scope.children:
            child_attributes = attributes if scope.kind == "class" and child.kind == "function" else {}
            inspect(child, aliases, child_attributes)

    inspect(_Scope("module", tree.body), {}, {})
    return offenders


def scan_tests(repository):
    offenders = set()
    errors = []
    for path in sorted((repository / "tests").rglob("*.py")):
        relative = path.relative_to(repository).as_posix()
        try:
            offenders.update(scan_source(path.read_bytes(), relative))
        except ValueError as exc:
            errors.append(str(exc))
    if errors:
        raise ValueError("\n".join(errors))
    return offenders


def parse_allowlist(contents):
    """Return exact call-to-issue entries, rejecting ambiguous exemptions."""
    entries = {}
    errors = []
    for number, row in enumerate(contents.splitlines(), 1):
        if not row.strip() or row.lstrip().startswith("#"):
            continue
        fields = row.split()
        label = f"timing_allowlist.txt:{number}"
        if len(fields) != 4:
            errors.append(f"{label}: expected path line kind GitHub-issue-link")
            continue
        path, line, kind, issue = fields
        parts = PurePosixPath(path).parts
        if (not path.startswith("tests/") or not path.endswith(".py")
                or PurePosixPath(path).as_posix() != path
                or any(part in {".", ".."} for part in path.split("/"))
                or any(ord(char) < 32 or ord(char) == 127 for char in path)
                or any(char in path for char in "*?[]\\:") or len(parts) < 2):
            errors.append(f"{label}: malformed repository-relative Python path {path!r}")
        elif not re.fullmatch(r"[1-9][0-9]*", line):
            errors.append(f"{label}: malformed positive line number {line!r}")
        elif kind not in KINDS:
            errors.append(f"{label}: unknown construct kind {kind!r}")
        elif not ISSUE.fullmatch(issue):
            errors.append(f"{label}: missing or malformed GitHub issue link {issue!r}")
        else:
            call = TimingCall(path, int(line), kind)
            if call in entries:
                errors.append(f"{label}: duplicate entry for {path}:{line} {kind}")
            else:
                entries[call] = issue
    if errors:
        raise ValueError("\n".join(errors))
    return entries


def validate_allowlist(offenders, contents):
    entries = parse_allowlist(contents)
    errors = []
    for call in sorted(set(offenders) - entries.keys()):
        errors.append(f"{call.path}:{call.line}: unlisted {call.kind}; remove the timing dependency")
    for call in sorted(entries.keys() - set(offenders)):
        errors.append(f"{call.path}:{call.line}: stale {call.kind}; delete the allowlist entry")
    if errors:
        raise ValueError("\n".join(errors))


def compare_allowlists(previous_contents, candidate_contents):
    """Trusted previous contents must come from outside the candidate checkout."""
    previous = parse_allowlist(previous_contents)
    candidate = parse_allowlist(candidate_contents)
    errors = []
    for call, issue in sorted(candidate.items()):
        if call not in previous:
            errors.append(f"{call.path}:{call.line}: added or retargeted {call.kind} exemption; only deletions are permitted")
        elif previous[call] != issue:
            errors.append(f"{call.path}:{call.line}: replaced issue link for {call.kind}; only deletions are permitted")
    if errors:
        raise ValueError("\n".join(errors))


def test_repository_has_no_unallowlisted_timing_calls():
    repository = Path(__file__).resolve().parents[1]
    validate_allowlist(scan_tests(repository), (repository / "tests/timing_allowlist.txt").read_text())


def test_scanner_detects_sleep_and_timed_synchronization_forms():
    source = """import time, threading, queue
time.sleep(1)
time.sleep(secs=delay)
time.sleep(0)
time.sleep(-1)
time.sleep(0.0)
time.sleep(1 - 1)
time.sleep(None)
threading.Event().wait(0)
threading.Event().wait(timeout=False)
threading.Thread().join(timeout=1)
queue.Queue().get(True, 1)
queue.Queue().get(timeout=0)
queue.Queue().get(False)
threading.Thread().join(None)
time.sleep(-0.1)
time.sleep(+0)
time.sleep(False)
time.sleep(settings.delay)
time.sleep(duration())
"""
    assert {(call.line, call.kind) for call in scan_source(source)} == {
        (2, "time.sleep"), (3, "time.sleep"), (7, "time.sleep"), (8, "time.sleep"),
        (9, "threading.Event.wait"), (10, "threading.Event.wait"),
        (11, "threading.Thread.join"), (12, "queue.Queue.get"), (13, "queue.Queue.get"),
        (17, "time.sleep"), (18, "time.sleep"), (19, "time.sleep"), (20, "time.sleep"),
    }


def test_scanner_resolves_aliases_and_constructor_assigned_receivers():
    source = """import threading as t
import queue as q
from time import sleep as nap
from threading import Event as E, Thread as T
from queue import Queue as Q
event: object = E()
event.wait(1)
thread = T()
thread.join(timeout=1)
items = q.Queue()
items.get(True, 1)
nap(1)
def example():
    receiver.wait(1)
    import threading as lib
    receiver = lib.Event()
    receiver = lib.Thread()
    receiver.join(1)
    import queue as lib
    receiver = lib.Queue()
    receiver.get(timeout=1)
    receiver = unrelated()
    receiver.wait(1)
    local: object = Q()
    local.get(timeout=1)
class Example:
    def use(self):
        self.event.wait(1)
        self.shared.join(1)
        self.shared.get(timeout=1)
    def setup(self):
        self.event: object = t.Event()
        self.shared = t.Thread()
        self.shared = q.Queue()
    @classmethod
    def create(cls):
        cls.ready = E()
    @classmethod
    def check(cls):
        cls.ready.wait(timeout=1)
def more_aliases():
    import time as clock
    clock.sleep(secs=1)
    from threading import Event as Factory
    first = second = Factory()
    first.wait(timeout=1)
    second.wait(1)
    from queue import Queue as Factory
    first: object = Factory()
    first.get(True, 1)
    from threading import Thread as Factory
    first = Factory()
    first.join(timeout=1)
"""
    assert {(call.line, call.kind) for call in scan_source(source)} == {
        (7, "threading.Event.wait"), (9, "threading.Thread.join"), (11, "queue.Queue.get"),
        (12, "time.sleep"), (14, "threading.Event.wait"), (18, "threading.Thread.join"),
        (21, "queue.Queue.get"), (23, "threading.Event.wait"), (25, "queue.Queue.get"),
        (28, "threading.Event.wait"), (29, "threading.Thread.join"), (30, "queue.Queue.get"),
        (40, "threading.Event.wait"),
        (43, "time.sleep"), (46, "threading.Event.wait"), (47, "threading.Event.wait"),
        (50, "queue.Queue.get"), (53, "threading.Thread.join"),
    }


def test_scanner_ignores_untimed_calls_and_unrelated_methods():
    source = '''import time, threading, queue
# time.sleep(2)
"time.sleep(2)"
threading.Event().wait()
threading.Thread().join(timeout=None)
queue.Queue().get(timeout=None)
{}.get("key", 1)
" ".join(["a", "b"])
prismabuild.queue.get(timeout=1)
event = threading.Event()
def other():
    event.wait(1)
def outer():
    local = threading.Event()
    def inner():
        local.wait(1)
def unsupported():
    a, b = threading.Event(), threading.Event()
    a.wait(1)
    (named := threading.Event()).wait(1)
    named.wait(1)
    augmented += threading.Event()
    augmented.wait(1)
    events = [threading.Event()]
    events[0].wait(1)
    returned = factory()
    returned.wait(1)
def external():
    global event
    event = threading.Event()
    event.wait(1)
def closure():
    nonlocal local
    local = threading.Event()
    local.wait(1)
class Base:
    def setup(self):
        self.ready = threading.Event()
class Child(Base):
    def check(self):
        self.ready.wait(1)
class Other:
    def setup(self):
        def nested():
            self.ready = threading.Event()
    def check(self):
        self.ready.wait(1)
class ClassBody:
    ready = threading.Event()
    ready.wait(1)
class Separate:
    def check(self):
        self.ready.wait(1)
'''
    assert scan_source(source) == set()


def test_scanner_covers_nested_python_files_and_reports_syntax_errors(tmp_path):
    import pytest

    nested = tmp_path / "tests/helpers/fixtures"
    nested.mkdir(parents=True)
    (nested / "helper.py").write_text("raise RuntimeError('must not execute')\nimport time\ntime.sleep(1)\n")
    (nested / "ignored.txt").write_text("invalid Python !!!")
    assert scan_tests(tmp_path) == {TimingCall("tests/helpers/fixtures/helper.py", 3, "time.sleep")}
    (nested / "broken.py").write_text("def broken(:\n")
    with pytest.raises(ValueError, match=r"tests/helpers/fixtures/broken.py:1: syntax error"):
        scan_tests(tmp_path)


def test_allowlist_rejects_malformed_duplicate_and_stale_entries():
    import pytest

    issue = "https://github.com/RobTand/prismabuild/issues/1771"
    row = f"tests/example.py 2 time.sleep {issue}\n"
    call = TimingCall("tests/example.py", 2, "time.sleep")
    validate_allowlist({call}, row)
    malformed = [
        row.replace("tests/example.py", path) for path in (
            "/tests/example.py", "tests/../example.py", "tests/*.py", "tests/x?.py",
            "tests/[x].py", "tests//example.py", "tests/./example.py", "tests/example.txt",
            "tests\\example.py", "other/example.py",
            "tests/null\x00.py",
        )
    ] + [row.replace(" 2 ", f" {line} ") for line in ("0", "-1", "1.5", "02", "two")]
    malformed += [row.replace(issue, "1771"), row.replace(issue, issue.replace("/issues/", "/pull/")),
                  "tests/example.py 2 time.sleep\n", row.replace("time.sleep", "get")]
    for contents in malformed:
        with pytest.raises(ValueError, match="malformed|expected|unknown"):
            parse_allowlist(contents)
    with pytest.raises(ValueError, match="duplicate"):
        parse_allowlist(row + row.replace(issue, issue.replace("1771", "1772")))
    with pytest.raises(ValueError, match=r"tests/example.py:2: stale.*delete"):
        validate_allowlist(set(), row)
    with pytest.raises(ValueError, match=r"tests/example.py:2: unlisted.*remove"):
        validate_allowlist({call}, "")


def test_allowlist_comparison_accepts_only_deletions():
    import pytest

    issue = "https://github.com/RobTand/prismabuild/issues/1771"
    first = f"tests/example.py 2 time.sleep {issue}\n"
    second = f"tests/other.py 4 threading.Event.wait {issue}\n"
    previous = first + second
    for candidate in (previous, second + first, first, second, "", "# all removed\n"):
        compare_allowlists(previous, candidate)
    for candidate in (previous + first.replace(" 2 ", " 3 "), first.replace(" 2 ", " 3 "),
                      first.replace("example.py", "replacement.py"),
                      first.replace("time.sleep", "threading.Thread.join"),
                      first.replace("1771", "1772")):
        inventory = set(parse_allowlist(candidate))
        validate_allowlist(inventory, candidate)
        with pytest.raises(ValueError, match="only deletions"):
            compare_allowlists(previous, candidate)
