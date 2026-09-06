"""The fleet runtime is one identified generation, never a bag of live copies."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "publish_runtime", ROOT / "tools" / "fleet" / "publish_runtime.py"
)
publish_runtime = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(publish_runtime)  # type: ignore[union-attr]


def _checkout(path: Path, generation: str) -> Path:
    package = path / "src" / "prismabuild"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "core.py").write_text(f"GENERATION = {generation!r}\n")
    (package / "pool.py").write_text(f"GENERATION = {generation!r}\n")
    return path


def _fake_git_and_probe(commit: str):
    def run(argv, **_kwargs):
        words = [str(part) for part in argv]
        if words and words[0] == "git":
            if "rev-parse" in words:
                return SimpleNamespace(returncode=0, stdout=commit + "\n", stderr="")
            if "status" in words:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout="import ok\n", stderr="")

    return run


def test_publish_refuses_an_unproved_commit_before_touching_the_mirror(
    tmp_path, monkeypatch,
) -> None:
    """A blank version is not a receipt and must never accompany live bytes."""

    checkout = _checkout(tmp_path / "checkout", "new")
    mirror = tmp_path / "mirror"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(
        publish_runtime.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=128, stdout="", stderr="fatal: not a git repository\n"
        ),
    )
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py"])

    with pytest.raises(SystemExit, match="cannot prove.*40-hex Git commit"):
        publish_runtime.main()

    assert not mirror.exists(), "an unidentified generation reached the live path"


def test_untracked_published_files_make_the_runtime_tree_dirty(monkeypatch) -> None:
    """A commit cannot identify a source member Git does not track."""

    def git_result(*argv: str):
        output = "" if "--untracked-files=no" in argv else "?? src/prismabuild/new.py\n"
        return SimpleNamespace(returncode=0, stdout=output, stderr="")

    monkeypatch.setattr(publish_runtime, "_git_result", git_result)

    assert publish_runtime._working_tree_dirty() is True


def test_published_skill_companion_documents_resolve_inside_the_generation(
    tmp_path, monkeypatch,
) -> None:
    """Following the installed skill must not require a mutable checkout."""
    import hashlib
    import json
    import re
    from urllib.parse import unquote, urlsplit

    mirror = tmp_path / "fleet" / "repo"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", ROOT)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40))
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py"])
    assert publish_runtime.main() == 0
    generation = mirror.resolve()
    receipt = json.loads((generation / "RUNTIME_VERSION.json").read_text())
    skill = generation / "skills/prismabuild/SKILL.md"
    text = skill.read_text()
    references = {generation / name for name in re.findall(r"`(docs/[^`]+\.md)`", text)}
    references.update(
        (skill.parent / link).resolve()
        for link in re.findall(r"\]\(([^)]+\.md)\)", text)
    )
    required = {generation / "docs/agent_execution_policy.md",
                generation / "docs/operating_prismabuild.md"}
    assert required <= references, "the skill no longer identifies both companion policies"
    pending = list(references)
    visited = set()
    while pending:
        document = pending.pop().resolve()
        if document in visited:
            continue
        visited.add(document)
        assert document.is_relative_to(generation), document
        name = document.relative_to(generation).as_posix()
        assert document.is_file(), f"published guide references an absent file: {name}"
        assert hashlib.sha256(document.read_bytes()).hexdigest() == receipt["files"][name]
        assert document.stat().st_mode & 0o222 == 0, f"published guide is writable: {name}"
        for link in re.findall(r"\]\(([^)]+)\)", document.read_text()):
            parsed = urlsplit(link)
            if not parsed.scheme and not parsed.netloc and parsed.path.endswith(".md"):
                pending.append(document.parent / unquote(parsed.path))


def test_publish_never_exposes_a_mixed_generation(tmp_path, monkeypatch) -> None:
    commit = "a" * 40
    checkout = _checkout(tmp_path / "checkout", "new")
    old = _checkout(tmp_path / "old-generation", "old")
    mirror = tmp_path / "mirror"
    mirror.symlink_to(old, target_is_directory=True)
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(publish_runtime.subprocess, "run", _fake_git_and_probe(commit))
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py"])
    real_copy = shutil.copy2
    real_replace = publish_runtime.os.replace
    copies = []
    activations = []

    def pair(root):
        return tuple((root / "src" / "prismabuild" / name).read_text()
                     for name in ("core.py", "pool.py"))

    def checked_copy(source, target, *args, **kwargs):
        target = Path(target)
        assert not target.is_relative_to(mirror)
        assert not target.resolve().is_relative_to(old.resolve())
        copies.append(target)
        return real_copy(source, target, *args, **kwargs)

    def checked_replace(source, target, *args, **kwargs):
        if Path(target) == mirror:
            assert Path(source).is_symlink()
            assert pair(mirror) == ("GENERATION = 'old'\n",) * 2
            assert pair(Path(source)) == ("GENERATION = 'new'\n",) * 2
            activations.append(Path(source))
        return real_replace(source, target, *args, **kwargs)

    monkeypatch.setattr(publish_runtime.shutil, "copy2", checked_copy)
    monkeypatch.setattr(publish_runtime.os, "replace", checked_replace)
    assert publish_runtime.main() == 0
    assert copies
    assert len(activations) == 1
    assert pair(mirror) == ("GENERATION = 'new'\n",) * 2
    assert pair(old) == ("GENERATION = 'old'\n",) * 2


def test_a_failure_after_sealing_still_removes_the_staging_tree(
    tmp_path, monkeypatch
) -> None:
    """The seal precedes the rename; cleanup must be able to undo it (issue #34).

    main: the rename succeeds and the sealed tree becomes the generation.
    Branch: the rename fails; the sealed, read-only staging tree must not be
    left under ``runtime-generations``, and the publication error must stay
    the exception the caller sees.
    """

    commit = "a" * 40
    checkout = _checkout(tmp_path / "checkout", "new")
    mirror = _checkout(tmp_path / "mirror", "old")
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(
        publish_runtime.subprocess, "run", _fake_git_and_probe(commit)
    )
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--migrate-directory"])
    real_replace = publish_runtime.os.replace
    sealed_before_failure: list[bool] = []

    def failing_replace(source, target, *args, **kwargs):
        if Path(source).name.endswith(".staging"):
            root = Path(source)
            sealed_before_failure.append(not root.stat().st_mode & 0o200)
            raise OSError("injected failure after the seal")
        return real_replace(source, target, *args, **kwargs)

    monkeypatch.setattr(publish_runtime.os, "replace", failing_replace)

    with pytest.raises(OSError, match="injected failure after the seal"):
        publish_runtime.main()

    assert sealed_before_failure == [True]
    store = mirror.parent / "runtime-generations"
    assert [p.name for p in store.iterdir()] == []
    assert (mirror / "src" / "prismabuild" / "core.py").read_text() == (
        "GENERATION = 'old'\n"
    )


def test_every_fleet_tool_is_published_or_excluded_on_purpose() -> None:
    """A tool nobody added to the list is a tool no box can run.

    Neither dl380g10 nor sparklina has a checkout, so the published
    generation is the only place a command exists for them. pool_reset.py:582
    tells the operator to run ``pbwait.py <key>``, and pbwait.py was not
    published, so that instruction named nothing on two of the three boxes.

    Both directions are checked. A name in FLEET_SCRIPTS with no file behind
    it is a script quietly not published: ``_publication_manifest`` skips a
    missing source and says nothing.
    """

    fleet = ROOT / "tools" / "fleet"
    on_disk = {source.name for source in fleet.glob("*.py")}
    published = set(publish_runtime.FLEET_SCRIPTS)
    excluded = {name for name, _reason in publish_runtime.EXCLUDED}

    assert not published & excluded, sorted(published & excluded)
    assert on_disk <= published | excluded, sorted(
        on_disk - (published | excluded)
    )
    for name in published:
        assert (fleet / name).exists() or (ROOT / "tools" / name).exists(), name
    for name, reason in publish_runtime.EXCLUDED:
        assert reason.strip(), name


def _generation_store(tmp_path: Path) -> Path:
    """A mirror whose sibling holds the published generations."""

    mirror = tmp_path / "fleet" / "repo"
    store = mirror.parent / "runtime-generations"
    store.mkdir(parents=True)
    return store


def test_observability_runs_from_only_the_published_generation(tmp_path, monkeypatch):
    """Workers without a checkout can import the collector and dashboard tool."""
    import shutil
    import subprocess
    import sys

    monkeypatch.setattr(publish_runtime, "CHECKOUT", ROOT)
    release = tmp_path / "release"
    for name in publish_runtime._publication_manifest():
        target = release / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(publish_runtime._source_for(name), target)
    result = subprocess.run([
        sys.executable, "-c",
        "import sys,json,importlib.util; from pathlib import Path; "
        "root=Path(sys.argv[1]); sys.path.insert(0,str(root/'tools')); "
        "import pbmetrics; "
        "spec=importlib.util.spec_from_file_location('dashboard',root/'fleet/observability/deploy_dashboard.py'); "
        "module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); "
        "assert json.loads((root/'fleet/observability/prismabuild.json').read_text())['uid']=='prismabuild-fleet'",
        str(release),
    ], cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_a_staging_tree_is_not_a_generation(tmp_path, monkeypatch) -> None:
    """An interrupted publish can leave one behind, receipt and all.

    The staging tree gets its receipt before it is sealed and renamed, and the
    cleanup that removes it swallows an OSError with a warning (:440-442). So
    a survivor holds a RUNTIME_VERSION.json and passes the receipt check, and
    the name check admits it: it has no "/" and it is not "." or "..".
    Activating it would point the live runtime at bytes no publish ever
    finished proving.
    """

    store = _generation_store(tmp_path)
    monkeypatch.setattr(publish_runtime, "MIRROR", store.parent / "repo")
    for name in (".abc123-1788600000-def.staging", ".hidden"):
        staging = store / name
        staging.mkdir()
        (staging / "RUNTIME_VERSION.json").write_text('{"commit": "' + "a" * 40 + '"}')
        with pytest.raises(SystemExit, match="not a generation name"):
            publish_runtime._activate_existing(name, dry_run=True)


def test_a_damaged_receipt_is_a_refusal_and_not_a_traceback(
    tmp_path, monkeypatch
) -> None:
    """Rollback is the one command that runs when everything else has failed."""

    store = _generation_store(tmp_path)
    monkeypatch.setattr(publish_runtime, "MIRROR", store.parent / "repo")
    generation = store / "abc123-1788600000-def"
    generation.mkdir()
    (generation / "RUNTIME_VERSION.json").write_text('{"commit": "aaaa')

    with pytest.raises(SystemExit, match="receipt is not readable"):
        publish_runtime._activate_existing(generation.name, dry_run=True)


def test_a_generation_store_that_cannot_be_written_is_a_refusal(
    tmp_path, monkeypatch
) -> None:
    """Not a PermissionError traceback after "publishing N files"."""

    commit = "a" * 40
    checkout = _checkout(tmp_path / "checkout", "new")
    fleet = tmp_path / "fleet"
    fleet.mkdir()
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", fleet / "repo")
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(
        publish_runtime.subprocess, "run", _fake_git_and_probe(commit)
    )
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py"])
    fleet.chmod(0o555)
    try:
        with pytest.raises(SystemExit, match="cannot write the generation store"):
            publish_runtime.main()
    finally:
        fleet.chmod(0o755)
