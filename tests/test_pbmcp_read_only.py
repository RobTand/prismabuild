"""``pbmcp`` cannot write to the queue, proved three ways.

"Read-only" is the property an agent is being asked to trust when it points
a tool at the fleet's live store, and a docstring is not evidence for it.
Three readings, because each catches what the others cannot.

*   **The import surface.**  Read with ``ast``, never by importing: a name
    that mutates the queue must not be reachable from this module at all.
    This is the reading that survives a refactor -- it fails on the line that
    introduces ``q.claim(...)``, before anybody runs it.
*   **A queue root with no write bits.**  The functional half.  Every tool has
    to answer completely against it, which is what says the module does not
    merely avoid writing but does not *need* to.
*   **Writes that raise.**  Modes alone are not the whole of it: ``flock``
    succeeds on a read-only descriptor, and a root owned by this user can have
    its modes changed back by the code under test.  So the syscalls that
    could mutate the store are replaced with ones that raise, inside the
    fixture's roots only, and every tool is run again.  The replacement is
    inherited by ``bounded``'s forked reader, so it covers the child too.
"""

from __future__ import annotations

import ast
import io
import os
from pathlib import Path
import stat
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

import pbmcp  # noqa: E402
import pbmcp_fixture as fx  # noqa: E402

SOURCE = REPOSITORY / "tools" / "fleet" / "pbmcp.py"

#: ``os`` calls that change something, matched on the whole dotted name.
#: Whole rather than tail because ``os.write`` and ``self.stdout.write`` are
#: the same last component and opposite acts: one writes to the store, the
#: other writes the JSON-RPC reply this server exists to send.
FORBIDDEN_OS = frozenset({
    "os.rename", "os.replace", "os.unlink", "os.remove", "os.rmdir",
    "os.removedirs", "os.mkdir", "os.makedirs", "os.symlink", "os.link",
    "os.truncate", "os.ftruncate", "os.chmod", "os.chown", "os.utime",
    "os.mkfifo", "os.mknod", "os.write", "os.writev", "os.pwrite",
})

#: Methods that mutate, lock, or read a whole log, matched on the last
#: component so that ``pool._write_json_atomic`` and a bare
#: ``_write_json_atomic`` are both caught. ``attempt_outcomes`` and
#: ``adopted_attempt_summary`` are here for the log reason rather than the
#: write reason: they read every stdout and stderr whole to verify a digest,
#: which is right for a verifier and ruinous for a status call.
FORBIDDEN_METHODS = frozenset({
    "flock", "lockf",
    "write_text", "write_bytes", "mkdir", "touch", "rmdir", "unlink",
    "symlink_to", "hardlink_to", "chmod", "rename", "replace",
    "ensure_layout", "_transition_locked", "record_pass", "claim", "publish",
    "withdraw", "finish", "announce", "serve_once", "reap_stale",
    "_write_json_atomic", "_atomic_publish", "_publish_immutable",
    "_atomic_write_new_json", "publish_action_request", "publish_result",
    "ingest_input", "attempt_outcomes", "adopted_attempt_summary",
    "archived_preemption_outcomes",
})

#: ``str.replace`` and ``Path.replace`` share a name, so a call on a value
#: this module built from text is not the rename this is looking for. Nothing
#: in the module calls either; the exemption exists so that adding a string
#: edit does not read as adding a rename.
TEXTUAL = ("str", '"', "'")

#: Modes for ``open`` that create or change a file.
WRITING_MODES = ("w", "a", "x", "+")


def _calls(tree: ast.AST) -> list[tuple[str, int]]:
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            found.append((ast.unparse(node.func), node.lineno))
    return found


def test_no_call_in_the_module_can_change_the_store() -> None:
    tree = ast.parse(SOURCE.read_text(), filename=str(SOURCE))
    offending = [
        f"{SOURCE.name}:{line} {name}()"
        for name, line in _calls(tree)
        if name in FORBIDDEN_OS or name.rsplit(".", 1)[-1] in FORBIDDEN_METHODS
    ]
    assert offending == [], (
        "these calls can mutate the queue, take a transition lock, or read a "
        "whole log into memory, and a read-only status server may do none of "
        "them: " + ", ".join(offending))


def test_nothing_is_opened_for_writing() -> None:
    tree = ast.parse(SOURCE.read_text(), filename=str(SOURCE))
    offending = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call)
                and ast.unparse(node.func).rsplit(".", 1)[-1] in ("open",)):
            continue
        for index, argument in enumerate(node.args):
            named = ast.unparse(argument)
            if index == 1 and isinstance(argument, ast.Constant):
                if any(mode in str(argument.value) for mode in WRITING_MODES):
                    offending.append(f"{node.lineno} open(mode={argument.value!r})")
            if "O_WRONLY" in named or "O_RDWR" in named or "O_CREAT" in named:
                offending.append(f"{node.lineno} open({named})")
        for keyword in node.keywords:
            if keyword.arg == "mode" and isinstance(keyword.value, ast.Constant):
                if any(mode in str(keyword.value.value) for mode in WRITING_MODES):
                    offending.append(f"{node.lineno} open(mode=...)")
    assert offending == [], (
        "every path this opens must be opened for reading: " + ", ".join(offending))


def test_the_reader_would_see_a_write_if_one_were_added() -> None:
    """The scan is not vacuously empty."""

    probe = ast.parse(
        "import os\nos.replace('a', 'b')\nq.claim()\npath.write_text('x')\n")
    names = [name for name, _line in _calls(probe)]
    assert "os.replace" in set(names) & FORBIDDEN_OS
    tails = {name.rsplit(".", 1)[-1] for name in names}
    assert {"claim", "write_text"} <= tails & FORBIDDEN_METHODS


def _every_tool(session: pbmcp.Session, fleet: fx.Fleet) -> list[dict]:
    return [
        session.call("pb_status"),
        session.call("pb_action", {"key_prefix": fx.DONE_KEY[:12]}),
        session.call("pb_actions", {"limit": 20}),
        session.call("pb_verify_claim", {"sha256": fx.claim_digest(fleet)}),
        session.call("pb_log", {"key_prefix": fx.DONE_KEY[:12]}),
        session.call("pb_runtime"),
    ]


def test_every_tool_answers_against_a_store_with_no_write_bits(
    tmp_path: Path,
) -> None:
    fleet = fx.build(tmp_path)
    roots = (fleet.queue_root, fleet.cas_root)
    try:
        for root in roots:
            for directory, _sub, files in os.walk(root, topdown=False):
                for name in files:
                    _drop_write(Path(directory) / name)
                _drop_write(Path(directory))
        session = pbmcp.Session(queue_root=fleet.queue_root,
                                cas_root=fleet.cas_root,
                                repo_link=fleet.repo_link)
        for body in _every_tool(session, fleet):
            assert body["complete"] is True, (body["tool"], body["timed_out"],
                                              body["unavailable"])
    finally:
        for root in roots:
            for directory, _sub, files in os.walk(root):
                _add_write(Path(directory))
                for name in files:
                    _add_write(Path(directory) / name)


def _drop_write(path: Path) -> None:
    path.chmod(stat.S_IMODE(path.stat().st_mode) & ~0o222)


def _add_write(path: Path) -> None:
    path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o200)


class _Refused(Exception):
    """The sentinel a mutating syscall raises while the guard is installed."""


def test_every_tool_answers_with_every_write_replaced_by_a_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Modes are not the whole proof: ``flock`` works on a read-only file.

    Scoped to the fixture's roots so that pytest's own capture, the fork's
    ``/dev/null`` and the temporary directory machinery keep working; a guard
    that broke those would have to be relaxed until it proved nothing.
    """

    fleet = fx.build(tmp_path)
    roots = (fleet.queue_root.resolve(), fleet.cas_root.resolve())

    def guarded(name, original, index=0):
        def replacement(*args, **kwargs):
            if args and _inside(args[index], roots):
                raise _Refused(f"pbmcp called {name} on {args[index]}")
            return original(*args, **kwargs)
        return replacement

    for name in ("rename", "replace", "unlink", "remove", "rmdir", "mkdir",
                 "makedirs", "symlink", "truncate", "chmod", "chown", "utime",
                 "mkfifo"):
        original = getattr(os, name, None)
        if original is not None:
            monkeypatch.setattr(os, name, guarded(f"os.{name}", original))
    _install_open_guards(monkeypatch, roots)

    def refuse(*args, **kwargs):
        raise _Refused("pbmcp reached a store writer")

    monkeypatch.setattr(pbmcp.pool, "_write_json_atomic", refuse)
    monkeypatch.setattr(pbmcp.pb, "_atomic_publish", refuse)

    session = pbmcp.Session(queue_root=fleet.queue_root,
                           cas_root=fleet.cas_root, repo_link=fleet.repo_link)
    for body in _every_tool(session, fleet):
        assert body["complete"] is True, (body["tool"], body["unavailable"])
        assert not any("_Refused" in str(entry) for entry in body["unavailable"])


def _install_open_guards(monkeypatch: pytest.MonkeyPatch,
                         roots: tuple[Path, ...]) -> None:
    """Refuse a writing open inside ``roots``, at both doors into the kernel.

    Two doors, because they do not share one: ``pathlib`` reaches ``io.open``
    and never the ``os.open`` Python object, while a descriptor-level reader
    like ``pbmcp``'s log tail reaches ``os.open`` and never ``io.open``.
    Guarding one and calling it proof would leave the other unwatched.
    """

    real_os_open = os.open
    real_io_open = io.open

    def guarded_os_open(path, flags, *rest, **kwargs):
        writes = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
        if flags & writes and _inside(path, roots):
            raise _Refused(f"opened {path} for writing")
        return real_os_open(path, flags, *rest, **kwargs)

    def guarded_io_open(file, mode="r", *rest, **kwargs):
        if any(character in str(mode) for character in WRITING_MODES):
            if _inside(file, roots):
                raise _Refused(f"opened {file} with mode {mode!r}")
        return real_io_open(file, mode, *rest, **kwargs)

    monkeypatch.setattr(os, "open", guarded_os_open)
    monkeypatch.setattr(io, "open", guarded_io_open)


def _inside(path: object, roots: tuple[Path, ...]) -> bool:
    try:
        resolved = Path(os.fsdecode(path)).resolve()
    except (TypeError, ValueError, OSError):
        return False
    return any(resolved == root or root in resolved.parents for root in roots)


def test_the_write_guard_bites(tmp_path: Path,
                               monkeypatch: pytest.MonkeyPatch) -> None:
    """A guard nothing can trip is a guard that proves nothing.

    Tripped with the two writers an accidental line would actually use --
    ``Path.write_text`` and a descriptor opened for writing -- and then shown
    to leave a path outside the roots alone, so that a guard which refused
    everything could not pass for one that refuses the right thing.
    """

    fleet = fx.build(tmp_path)
    roots = (fleet.queue_root.resolve(),)
    _install_open_guards(monkeypatch, roots)

    with pytest.raises(_Refused):
        (fleet.queue_root / "proof.json").write_text("x")
    with pytest.raises(_Refused):
        os.open(fleet.queue_root / "proof.json", os.O_WRONLY | os.O_CREAT, 0o600)
    (tmp_path / "elsewhere.json").write_text("x")
    descriptor = os.open(fleet.queue_root, os.O_RDONLY)
    os.close(descriptor)


def test_the_queue_is_byte_identical_after_every_tool_has_run(
    tmp_path: Path,
) -> None:
    """The end-to-end statement, in the terms an operator would check it in."""

    fleet = fx.build(tmp_path)
    before = _listing(fleet.queue_root) | _listing(fleet.cas_root)
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link)
    _every_tool(session, fleet)
    assert _listing(fleet.queue_root) | _listing(fleet.cas_root) == before


def _listing(root: Path) -> set[tuple[str, int, int]]:
    found = set()
    for directory, _sub, files in os.walk(root):
        for name in files:
            path = Path(directory) / name
            info = path.stat()
            found.add((str(path.relative_to(root)), info.st_size,
                       info.st_mtime_ns))
    return found
