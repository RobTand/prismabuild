"""The fleet runtime is one identified generation, never a bag of live copies."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
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
    for leaf in ("generation_receipt.py", "digest_primitives.py"):
        (package / leaf).write_bytes((ROOT / "src/prismabuild" / leaf).read_bytes())
    # Phase-2 default-ON: every published generation is verified, so a plain
    # green canary driver ships with every test checkout.  Tests that care
    # about a verdict overwrite it with _write_canary_driver; tests that care
    # about skipping pass --no-canary.
    driver = path / "tools" / "fleet" / "pbcanary.py"
    driver.parent.mkdir(parents=True, exist_ok=True)
    driver.write_text("def run_canary(*, generation):\n    return 0\n")
    return path


def _index_listing(entries: dict[str, int]) -> str:
    """``git ls-files --stage -z`` output for the given path -> mode map."""

    return "".join(
        f"{mode:06o} {'0' * 40} 0\t{path}\0" for path, mode in entries.items()
    )


def test_real_import_probe_preserves_the_runtime_file_roster(tmp_path, monkeypatch):
    """Importing a staged generation must not publish unlisted bytecode."""
    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    monkeypatch.delenv("PYTHONPYCACHEPREFIX", raising=False)
    root = _checkout(tmp_path / "generation", "new")
    (root / "src/prismabuild/pool.py").write_text(
        "class PoolQueue:\n    def claim(self):\n        return None\n")
    before = {p.relative_to(root): p.read_bytes()
              for p in root.rglob("*") if p.is_file()}
    publish_runtime._probe(root)
    after = {p.relative_to(root): p.read_bytes()
             for p in root.rglob("*") if p.is_file()}
    assert after == before


def _fake_git_and_probe(commit: str, index: dict[str, int] | None = None):
    """Answer the three Git questions publication asks, then the import probe.

    ``index`` is the ``git ls-files --stage`` answer: the mode Git records for
    each checkout-relative path.  A fake checkout is not a repository, so the
    default is an empty index, and published files then fall back to their own
    owner-execute bit exactly as an untracked file does.
    """

    listing = _index_listing(index or {})

    def run(argv, **_kwargs):
        words = [str(part) for part in argv]
        if words and words[0] == "git":
            if "rev-parse" in words:
                return SimpleNamespace(returncode=0, stdout=commit + "\n", stderr="")
            if "status" in words:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if "ls-files" in words:
                return SimpleNamespace(returncode=0, stdout=listing, stderr="")
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
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--rollout", "rolling",
                                        "--rollout-reason", "fixture publication",
                                        "--shape-gate-waiver", "fixture publication"])

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


@pytest.fixture(scope="module")
def published_generation(tmp_path_factory):
    mirror = tmp_path_factory.mktemp("published-content") / "fleet" / "repo"
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(publish_runtime, "CHECKOUT", ROOT)
        patch.setattr(publish_runtime, "MIRROR", mirror)
        patch.setattr(publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40))
        patch.setattr(sys, "argv", ["publish_runtime.py", "--stage-only", "--rollout", "rolling",
                                        "--rollout-reason", "fixture publication",
                                        "--shape-gate-waiver", "fixture publication"])
        assert publish_runtime.main() == 0
    return next((mirror.parent / "runtime-generations").iterdir())


def test_published_skill_companion_documents_resolve_inside_the_generation(
    published_generation,
) -> None:
    """Following the installed skill must not require a mutable checkout."""
    import hashlib
    import json
    import re
    from urllib.parse import unquote, urlsplit

    generation = published_generation
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
    # AGENTS also requires the machine-readable contract ledgers. Follow
    # their links as immutable publication members, like the prose guides.
    pending = [*references, generation / "AGENTS.md"]
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
            if (not parsed.scheme and not parsed.netloc
                    and parsed.path.endswith((".md", ".json"))):
                pending.append(document.parent / unquote(parsed.path))


def test_published_torch_helper_can_be_copied_without_a_source_checkout(
    tmp_path, monkeypatch, published_generation,
) -> None:
    """The published torch-mode guide's copyable helper must actually travel."""
    import hashlib
    import json

    generation = published_generation
    helper = generation / "tools/profile_torch.py"
    assert helper.is_file(), "published torch guide names an absent helper"
    receipt = json.loads((generation / "RUNTIME_VERSION.json").read_text())
    assert helper.read_bytes() == (ROOT / "tools/profile_torch.py").read_bytes()
    assert hashlib.sha256(helper.read_bytes()).hexdigest() == receipt["files"]["tools/profile_torch.py"]
    assert stat.S_IMODE(helper.stat().st_mode) == 0o444

    # Follow the guide: copy into the consumer's own tree. An unprofiled
    # consumer must be able to import and use it even with no torch installed.
    consumer = tmp_path / "consumer"
    consumer.mkdir()
    copied = consumer / "profile_torch.py"
    shutil.copyfile(helper, copied)
    monkeypatch.chdir(consumer)
    monkeypatch.delenv("PRISMABUILD_PROFILE_TORCH_OUT", raising=False)
    monkeypatch.setitem(sys.modules, "torch", None)
    monkeypatch.setitem(sys.modules, "torch.profiler", None)
    spec = importlib.util.spec_from_file_location("copied_torch_helper", copied)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with module.prismabuild_torch_profile() as profiler:
        assert profiler is None


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
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--rollout", "rolling",
                                        "--rollout-reason", "fixture publication",
                                        "--shape-gate-waiver", "fixture publication"])
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
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--migrate-directory",
                                        "--rollout", "rolling", "--rollout-reason",
                                        "fixture publication", "--shape-gate-waiver", "fixture publication"])
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
    # The shell installers are fleet tools too: one nobody listed left no host
    # able to enroll in automatic movement publication (#1659).
    on_disk = {source.name for source in [*fleet.glob("*.py"), *fleet.glob("*.sh")]}
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
    # The publisher refuses this one itself (#1284): a published script that
    # imports a tools module the publication does not carry.
    assert publish_runtime._unshipped_imports(
        publish_runtime._publication_manifest()) == []


def test_qualified_scratch_recorder_sources_travel_with_the_generation() -> None:
    """A published loader must have every source byte it independently binds."""
    from prismabuild import local_scratch

    manifest = publish_runtime._publication_manifest()
    for member in local_scratch.PRODUCER_FILES:
        assert member in manifest, f"qualified scratch producer source missing: {member}"
    for layout in ("tools", "tools/fleet"):
        assert f"{layout}/local_scratch_profile.py" in manifest


def _generation_store(tmp_path: Path) -> Path:
    """A mirror whose sibling holds the published generations."""

    mirror = tmp_path / "fleet" / "repo"
    store = mirror.parent / "runtime-generations"
    store.mkdir(parents=True)
    return store


def test_observability_runs_from_only_the_published_generation(tmp_path, published_generation):
    """Workers without a checkout can import the collector and dashboard tool."""
    import subprocess
    import sys

    release = published_generation
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


def test_the_storage_helper_travels_with_the_generation(tmp_path, monkeypatch):
    """``pbstatus`` reads the NFS readahead window through it (#523).

    A worker has no checkout, so a generation published without this file
    makes the readiness reading permanently ``unavailable`` on every box in
    the fleet -- the check would ship inert. Publishing it installs nothing:
    it is stdlib-only, reads unless given ``--apply``, and the host unit
    beside it is still an operator's to install.
    """

    monkeypatch.setattr(publish_runtime, "CHECKOUT", ROOT)
    manifest = publish_runtime._publication_manifest()
    name = "fleet/storage/nfs_readahead.py"
    assert name in manifest
    source = publish_runtime._source_for(name)
    assert source == ROOT / name and source.is_file()
    assert manifest[name] == publish_runtime._sha256(source)


def test_the_residency_plan_retire_tool_travels_with_the_generation(
    tmp_path, monkeypatch,
) -> None:
    """#1381: an operator on a box without a checkout runs the backfill.

    ``retire_residency_plans.py`` reaps plans whose consumer concluded before
    ``PoolQueue.finish`` started retiring them.  The queue it names lives on
    the shared mount, and the two boxes that most need it have no checkout, so
    it is published rather than excluded.  The flat ``tools/`` spelling is the
    one such a box runs, and its ``parents[2] / "src"`` bootstrap named the
    parent of the generation store there; the entry point binds through
    ``runtime_paths.generation_root`` instead.
    """

    assert "retire_residency_plans.py" in publish_runtime.FLEET_SCRIPTS
    assert "retire_residency_plans.py" not in dict(publish_runtime.EXCLUDED)
    monkeypatch.setattr(publish_runtime, "CHECKOUT", ROOT)
    manifest = publish_runtime._publication_manifest()
    name = "tools/retire_residency_plans.py"
    assert name in manifest
    assert "tools/fleet/retire_residency_plans.py" in manifest
    source = publish_runtime._source_for(name)
    assert source == ROOT / "tools" / "fleet" / "retire_residency_plans.py"
    assert manifest[name] == publish_runtime._sha256(source)


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
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--rollout", "rolling",
                                        "--rollout-reason", "fixture publication",
                                        "--shape-gate-waiver", "fixture publication"])
    fleet.chmod(0o555)
    try:
        with pytest.raises(SystemExit, match="cannot (write the generation store|open publication lock)"):
            publish_runtime.main()
    finally:
        fleet.chmod(0o755)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def _publish_one_generation(
    tmp_path: Path, monkeypatch, checkout: Path, index: dict[str, int],
) -> Path:
    """Publish ``checkout`` into a private mirror and return the generation."""

    mirror = tmp_path / "mirror"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(
        publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40, index)
    )
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--rollout", "rolling",
                                        "--rollout-reason", "  fixture publication  ",
                                        "--shape-gate-waiver", "fixture publication"])
    assert publish_runtime.main() == 0
    return mirror


def test_rolling_receipt_records_the_normalized_safety_reason(tmp_path, monkeypatch):
    checkout = _checkout(tmp_path / "checkout", "new")
    mirror = _publish_one_generation(tmp_path, monkeypatch, checkout, {})
    receipt = json.loads((mirror.resolve() / "RUNTIME_VERSION.json").read_text())
    assert receipt["rollout"] == "rolling"
    assert receipt["rollout_reason"] == "fixture publication"


def test_a_published_program_is_executable_by_a_uid_that_is_not_the_owner(
    tmp_path, monkeypatch,
) -> None:
    """Netdata runs the published mount collector as its own uid (issue #316).

    ``docs/mount_measurement.md`` installs the collector as a symlink into the
    live generation on purpose, so a later publication re-points it and no
    re-link is owed.  That makes a non-owner uid a first-class reader of
    published bytes, and an owner-only mode denies it both execute and read.

    Before the fix, with the publishing checkout at 0700/0600 as a umask-077
    worktree leaves it:

        AssertionError: assert 320 == 365
    """

    checkout = _checkout(tmp_path / "checkout", "new")
    package = checkout / "src" / "prismabuild"
    (package / "pool.py").chmod(0o700)
    (package / "core.py").chmod(0o600)
    index = {
        "src/prismabuild/__init__.py": 0o100644,
        "src/prismabuild/pool.py": 0o100755,
        "src/prismabuild/core.py": 0o100644,
    }

    mirror = _publish_one_generation(tmp_path, monkeypatch, checkout, index)

    assert _mode(mirror / "src" / "prismabuild" / "pool.py") == 0o555
    assert _mode(mirror / "src" / "prismabuild" / "core.py") == 0o444
    # Write stays denied to everyone, owner included: a generation is
    # append-only history and widening read must not widen that.
    for path in (mirror / "src" / "prismabuild").rglob("*"):
        assert _mode(path) & 0o222 == 0


def test_the_published_program_bit_is_the_one_git_records(
    tmp_path, monkeypatch,
) -> None:
    """Which members are programs is a repository fact, not a local mode.

    ``shutil.copy2`` preserves the publishing worktree's mode, so the same
    commit published different modes from different worktrees.  Here the
    filesystem contradicts the index in both directions, and the index wins.

    Before the fix:

        AssertionError: assert 256 == 365
    """

    checkout = _checkout(tmp_path / "checkout", "new")
    package = checkout / "src" / "prismabuild"
    # A program whose worktree copy lost its execute bit ...
    (package / "pool.py").chmod(0o600)
    # ... and a plain module whose worktree copy gained one.
    (package / "core.py").chmod(0o700)
    index = {
        "src/prismabuild/__init__.py": 0o100644,
        "src/prismabuild/pool.py": 0o100755,
        "src/prismabuild/core.py": 0o100644,
    }

    mirror = _publish_one_generation(tmp_path, monkeypatch, checkout, index)

    assert _mode(mirror / "src" / "prismabuild" / "pool.py") == 0o555
    assert _mode(mirror / "src" / "prismabuild" / "core.py") == 0o444


def test_the_publishers_umask_does_not_decide_who_can_read_a_generation(
    tmp_path, monkeypatch,
) -> None:
    """Directories and the receipt come from the umask, not from a source file.

    The generation's directories are created by ``mkdir`` and its receipt by
    ``open("w")``, so under umask 077 both closed to every non-owner reader
    while every published file was closed by ``copy2`` -- one accident with
    two spellings.  Published under a hostile umask, the whole tree must still
    be traversable and readable.

    Before the fix, on the generation directory itself:

        AssertionError: <generation> published 0500, wanted 0555
        assert 320 == 365
    """

    checkout = _checkout(tmp_path / "checkout", "new")
    index = {
        f"src/prismabuild/{name}": 0o100644
        for name in ("__init__.py", "core.py", "pool.py")
    }
    previous = os.umask(0o077)
    try:
        mirror = _publish_one_generation(tmp_path, monkeypatch, checkout, index)
        members = sorted(mirror.rglob("*"))
    finally:
        os.umask(previous)

    assert (mirror / "RUNTIME_VERSION.json").is_file()
    assert members
    for path in [mirror.resolve(), *members]:
        mode = _mode(path)
        wanted = 0o555 if path.is_dir() else 0o444
        assert mode == wanted, f"{path} published {mode:04o}, wanted {wanted:04o}"


def _write_canary_driver(checkout: Path, body: str) -> None:
    """Install a fake crew-A driver in the checkout the publisher loads from."""

    driver_dir = checkout / "tools" / "fleet"
    driver_dir.mkdir(parents=True, exist_ok=True)
    (driver_dir / "pbcanary.py").write_text(body)


def _run_publish(tmp_path: Path, monkeypatch, checkout: Path, extra: list[str],
                 gate: tuple[str, ...] = ("--shape-gate-waiver", "fixture publication"),
                 ) -> tuple[int, Path]:
    """Publish ``checkout`` into a private mirror with extra argv; return (exit, mirror).

    Phase-2 default-ON: an unflagged publication verifies its generation, so
    ``_checkout`` ships a green canary driver with every test checkout.  Tests
    that exercise the canary's own verdicts overwrite it with
    ``_write_canary_driver``; the skip path passes ``--no-canary``.
    """

    mirror = tmp_path / "mirror"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(
        publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40)
    )
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--rollout", "rolling",
                                        "--rollout-reason", "fixture publication",
                                        *gate, *extra])
    return publish_runtime.main(), mirror


def _canary_record(mirror: Path) -> dict:
    store = mirror.parent / "runtime-generations"
    records = list(store.glob("*.canary.json"))
    assert len(records) == 1, [p.name for p in store.iterdir()]
    return json.loads(records[0].read_text())


def test_default_publish_runs_the_canary(tmp_path, monkeypatch) -> None:
    """Phase-2 default-ON: an unflagged publication verifies the generation.

    Flipped 2026-09-19 after the first verified live 4-leg run
    (pb-canary/20260919T173543Z, exit 0, cross-box envelopes equal); the
    default is no longer a skip.
    """

    checkout = _checkout(tmp_path / "checkout", "new")
    _write_canary_driver(checkout,
        "from pathlib import Path\n"
        "def run_canary(*, generation):\n"
        "    (Path(__file__).parent / 'seen.txt').write_text(generation)\n"
        "    return 0\n")
    exit_code, mirror = _run_publish(tmp_path, monkeypatch, checkout, [])
    assert exit_code == 0
    generation = mirror.resolve()
    assert (generation / "RUNTIME_VERSION.json").is_file()
    assert (checkout / "tools" / "fleet" / "seen.txt").read_text() == generation.name
    record = _canary_record(mirror)
    assert record["canary_status"] == "verified"
    assert record["canary_exit"] == 0


def test_no_canary_flag_records_not_run(tmp_path, monkeypatch) -> None:
    """The escape hatch still skips, and says so in the record."""

    checkout = _checkout(tmp_path / "checkout", "new")
    exit_code, mirror = _run_publish(tmp_path, monkeypatch, checkout, ["--no-canary"])
    assert exit_code == 0
    record = _canary_record(mirror)
    assert record["canary_status"] == "not_run"
    assert "--no-canary" in record["detail"]


def test_verified_canary_resolves_against_the_activated_generation(
    tmp_path, monkeypatch,
) -> None:
    """The driver is imported (not duplicated) and told which generation to verify."""

    checkout = _checkout(tmp_path / "checkout", "new")
    _write_canary_driver(checkout,
        "from pathlib import Path\n"
        "def run_canary(*, generation):\n"
        "    (Path(__file__).parent / 'seen.txt').write_text(generation)\n"
        "    return 0\n")
    exit_code, mirror = _run_publish(tmp_path, monkeypatch, checkout, ["--canary"])
    assert exit_code == 0
    generation = mirror.resolve().name
    assert (checkout / "tools" / "fleet" / "seen.txt").read_text() == generation
    record = _canary_record(mirror)
    assert record["canary_status"] == "verified"
    assert record["canary_exit"] == 0
    assert record["generation"] == generation


def test_failed_canary_marks_and_exits_nonzero_without_rollback(
    tmp_path, monkeypatch,
) -> None:
    """Campaign-primacy: a failed canary leaves the activation exactly as it was."""

    checkout = _checkout(tmp_path / "checkout", "new")
    _write_canary_driver(checkout, "def run_canary(*, generation):\n    return 1\n")
    exit_code, mirror = _run_publish(tmp_path, monkeypatch, checkout, ["--canary"])
    assert exit_code == 1
    generation = mirror.resolve()
    assert (generation / "RUNTIME_VERSION.json").is_file(), \
        "a failed canary rolled back the activation it was meant only to mark"
    record = _canary_record(mirror)
    assert record["canary_status"] == "failed"
    assert record["canary_exit"] == 1
    assert record["generation"] == generation.name


def test_crashing_driver_is_a_failed_verdict_not_a_traceback(
    tmp_path, monkeypatch,
) -> None:
    """A driver that raises verifies nothing; the record says so and the exit is 1."""

    checkout = _checkout(tmp_path / "checkout", "new")
    _write_canary_driver(checkout,
        "def run_canary(*, generation):\n    raise RuntimeError('boom')\n")
    exit_code, mirror = _run_publish(tmp_path, monkeypatch, checkout, ["--canary"])
    assert exit_code == 1
    record = _canary_record(mirror)
    assert record["canary_status"] == "failed"
    assert "raised instead of returning a verdict" in record["detail"]


def test_canary_driver_return_drift_is_a_failed_verdict(
    tmp_path, monkeypatch,
) -> None:
    """The assumed contract is int-only; a friendlier shape is still drift."""

    checkout = _checkout(tmp_path / "checkout", "new")
    _write_canary_driver(checkout,
        "def run_canary(*, generation):\n    return 'verified'\n")
    exit_code, mirror = _run_publish(tmp_path, monkeypatch, checkout, ["--canary"])
    assert exit_code == 1
    record = _canary_record(mirror)
    assert record["canary_status"] == "failed"
    assert "run_canary(generation=...) -> int" in record["detail"]


def test_canary_without_a_driver_refuses_before_touching_the_mirror(
    tmp_path, monkeypatch,
) -> None:
    """Fail-closed pre-flight: no driver, no publication, no rollout record."""

    checkout = _checkout(tmp_path / "checkout", "new")
    # Phase-2 ships a green driver with every test checkout; this test is the
    # fail-closed pre-flight, so it removes the driver first.
    (checkout / "tools" / "fleet" / "pbcanary.py").unlink()
    mirror = tmp_path / "mirror"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(
        publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40)
    )
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--rollout", "rolling",
                                        "--rollout-reason", "fixture publication",
                                        "--canary"])
    with pytest.raises(SystemExit, match="no driver"):
        publish_runtime.main()
    assert not mirror.exists()
    assert not (tmp_path / "runtime-generations").exists()


def test_canary_and_no_canary_conflict(tmp_path, monkeypatch) -> None:
    """Two opposing flags are a usage error, not a coin toss."""

    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--canary", "--no-canary"])
    with pytest.raises(SystemExit) as caught:
        publish_runtime.main()
    assert caught.value.code == 2


def test_canary_flags_are_refused_on_rollback(tmp_path, monkeypatch) -> None:
    """Rollback restores bytes; it neither runs the canary nor rewrites its record."""

    mirror = tmp_path / "mirror"
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--rollout", "rolling",
                                        "--rollout-reason", "fixture publication",
                                        "--activate-generation", "somename",
                                        "--canary"])
    with pytest.raises(SystemExit, match="only on fresh publication"):
        publish_runtime.main()


def test_canary_flags_are_refused_with_stage_only(monkeypatch) -> None:
    """A staged generation is not live, so there is nothing to verify yet."""

    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", "--stage-only", "--canary"])
    with pytest.raises(SystemExit) as caught:
        publish_runtime.main()
    assert caught.value.code == 2


# -- the pre-publish shape gate (#987) -------------------------------------


def _fake_shape_gate(monkeypatch, verdict=None, refusal: str | None = None) -> list[dict]:
    """Stand in for the checkout's shape gate module; record what it was asked."""

    asked: list[dict] = []

    class ShapeGateFailure(Exception):
        def __init__(self, reason: str, detail: str) -> None:
            super().__init__(f"{reason}: {detail}")
            self.reason, self.detail = reason, detail

    def verify_gate_receipt(**kwargs):
        asked.append(kwargs)
        if refusal is not None:
            raise ShapeGateFailure("receipt_refused", refusal)
        return verdict

    module = SimpleNamespace(ShapeGateFailure=ShapeGateFailure,
                             verify_gate_receipt=verify_gate_receipt)
    monkeypatch.setattr(publish_runtime, "_load_shape_gate", lambda: module)
    return asked


def test_a_publication_without_the_shape_gate_refuses_and_writes_nothing(
        tmp_path, monkeypatch) -> None:
    """No receipt and no waiver: the dry run refuses, and says how to run the gate."""

    checkout = _checkout(tmp_path / "checkout", "new")
    with pytest.raises(SystemExit) as caught:
        _run_publish(tmp_path, monkeypatch, checkout, ["--dry-run"], gate=())
    message = str(caught.value)
    assert "pre-publish shape gate" in message
    assert "tests/gate_campaign_shape.py" in message
    assert "--shape-gate-waiver" in message
    assert not (tmp_path / "mirror").exists()
    assert not (tmp_path / "runtime-generations").exists()


def test_a_dry_run_with_a_waiver_prints_its_reason(tmp_path, monkeypatch, capsys) -> None:
    checkout = _checkout(tmp_path / "checkout", "new")
    exit_code, mirror = _run_publish(
        tmp_path, monkeypatch, checkout, ["--dry-run"],
        gate=("--shape-gate-waiver", "  the gate host is down  "))
    assert exit_code == 0
    assert "shape gate: WAIVED: the gate host is down" in capsys.readouterr().err
    assert not mirror.exists()


def test_a_waiver_is_recorded_in_the_receipt_and_by_the_canary(
        tmp_path, monkeypatch, capsys) -> None:
    """A waived generation says so in RUNTIME_VERSION.json, the canary record and output."""

    checkout = _checkout(tmp_path / "checkout", "new")
    exit_code, mirror = _run_publish(
        tmp_path, monkeypatch, checkout, [],
        gate=("--shape-gate-waiver", "gate run lost to a reboot"))
    assert exit_code == 0
    waived = {"verdict": "waived", "reason": "gate run lost to a reboot"}
    receipt = json.loads((mirror / "RUNTIME_VERSION.json").read_text())
    assert receipt["shape_gate"] == waived
    record = _canary_record(mirror)
    assert record["canary_status"] == "verified"
    assert record["shape_gate"] == waived
    err = capsys.readouterr().err
    assert f"{mirror.resolve().name} shape gate: WAIVED: gate run lost to a reboot" in err


def test_a_passing_gate_receipt_is_verified_against_the_commit_and_recorded(
        tmp_path, monkeypatch, capsys) -> None:
    verdict = {"verdict": "passed", "action_key": "f" * 64, "tables": ["t1", "t2"],
               "finished_host": "sparky", "snapshot": "b" * 40,
               "snapshot_parent": "a" * 40}
    asked = _fake_shape_gate(monkeypatch, verdict=verdict)
    checkout = _checkout(tmp_path / "checkout", "new")
    exit_code, mirror = _run_publish(
        tmp_path, monkeypatch, checkout, ["--no-canary"],
        gate=("--shape-gate-action", "ffffffffffff"))
    assert exit_code == 0
    assert len(asked) == 1
    assert asked[0]["action_key"] == "ffffffffffff"
    assert asked[0]["commit"] == "a" * 40
    assert asked[0]["checkout"] == checkout
    assert asked[0]["queue_root"] == mirror.parent / "pb-queue"
    assert asked[0]["cas_root"] == mirror.parent / "cas"
    receipt = json.loads((mirror / "RUNTIME_VERSION.json").read_text())
    assert receipt["shape_gate"] == verdict
    assert _canary_record(mirror)["shape_gate"] == verdict
    assert "shape gate: passed by ffffffffffff on sparky (tables t1, t2)" in capsys.readouterr().out


def test_a_refused_gate_receipt_refuses_the_publication(tmp_path, monkeypatch) -> None:
    _fake_shape_gate(monkeypatch, refusal="ffffffffffff ran a tree that differs")
    checkout = _checkout(tmp_path / "checkout", "new")
    with pytest.raises(SystemExit, match="does not cover aaaaaaaaaaaa: ffffffffffff ran a tree"):
        _run_publish(tmp_path, monkeypatch, checkout, [],
                     gate=("--shape-gate-action", "ffffffffffff"))
    assert not (tmp_path / "mirror").exists()


def test_a_dirty_tree_cannot_ride_a_gate_receipt(tmp_path, monkeypatch) -> None:
    """The receipt covers committed bytes; a dirty publish needs a waiver instead."""

    asked = _fake_shape_gate(monkeypatch, verdict={"verdict": "passed"})
    checkout = _checkout(tmp_path / "checkout", "new")
    monkeypatch.setattr(publish_runtime, "_working_tree_dirty", lambda: True)
    with pytest.raises(SystemExit, match="dirty tree is not the commit the shape gate ran"):
        _run_publish(tmp_path, monkeypatch, checkout, ["--allow-dirty", "--dry-run"],
                     gate=("--shape-gate-action", "ffffffffffff"))
    assert asked == []


@pytest.mark.parametrize("argv", [
    ["--shape-gate-action", "ffffffffffff", "--shape-gate-waiver", "both"],
    ["--shape-gate-waiver", " \t"],
    ["--activate-generation", "somename", "--shape-gate-waiver", "rollback"],
    ["--resume-barrier", "epoch", "--shape-gate-waiver", "recovery"],
])
def test_shape_gate_flags_are_usage_errors_where_they_mean_nothing(
        monkeypatch, argv) -> None:
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py", *argv])
    with pytest.raises(SystemExit) as caught:
        publish_runtime.main()
    assert caught.value.code == 2


def test_the_shape_gate_is_a_checkout_tool_not_a_generation_member() -> None:
    excluded = dict(publish_runtime.EXCLUDED)
    assert "shape_gate.py" in excluded
    assert "shape_gate.py" not in publish_runtime.FLEET_SCRIPTS
    assert publish_runtime._shape_gate_path() == (
        publish_runtime.CHECKOUT / "tools" / "fleet" / "shape_gate.py")


def test_publish_refuses_a_script_whose_import_it_does_not_carry(
    tmp_path, monkeypatch,
) -> None:
    """#1284: the publisher refuses the roster, not only the suite.

    Generation 9098f84c872b shipped a tier loop importing a module it did not
    carry while this file's roster test was red on main.  A red test that
    nobody ran is not a gate; the publication itself refuses now.
    """

    checkout = _checkout(tmp_path / "checkout", "new")
    fleet = checkout / "tools" / "fleet"
    (fleet / "tier_loop.py").write_text("import pbtest_outcomes\n")
    (fleet / "pbtest_outcomes.py").write_text("")
    mirror = tmp_path / "mirror"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ("tier_loop.py",))
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(publish_runtime, "EXCLUDED", (
        ("pbtest_outcomes.py", "fixture: excluded, yet imported"),
        ("pbcanary.py", "fixture: the canary driver"),
    ))
    monkeypatch.setattr(
        publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40))
    monkeypatch.setattr(sys, "argv", [
        "publish_runtime.py", "--rollout", "rolling",
        "--rollout-reason", "fixture publication",
        "--shape-gate-waiver", "fixture publication", "--dry-run"])

    with pytest.raises(SystemExit, match="tier_loop.py imports pbtest_outcomes"):
        publish_runtime.main()
    assert not mirror.exists()


def test_the_automatic_signer_does_not_approve_a_replaced_generation(
        tmp_path, monkeypatch, publication_store):
    """A store owner cannot change the receipt that the trusted publisher approves."""
    import hashlib
    from movement_publication_support import unseal
    from prismabuild import runtime_publication as publication

    checkout = _checkout(tmp_path / "checkout", "trusted")
    mirror = tmp_path / "fleet" / "repo"
    store = mirror.parent / "runtime-generations"
    home = tmp_path / "publisher-home"
    key = home / ".config" / "prismabuild" / "movement-approval.key"
    key.parent.mkdir(parents=True)
    secret = bytes.fromhex("ab" * 32)
    key.write_text(secret.hex() + "\n")
    key.chmod(0o600)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    real_stat = Path.stat

    def principal_stat(path, *args, **kwargs):
        info = real_stat(path, *args, **kwargs)
        if path in (key, store):
            return SimpleNamespace(st_uid=50001 if path == key else 50002,
                                   st_mode=info.st_mode)
        return info

    monkeypatch.setattr(Path, "stat", principal_stat)
    monkeypatch.setattr(os, "geteuid", lambda: 50001)
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40))
    monkeypatch.setattr(sys, "argv", [
        "publish_runtime.py", "--stage-only", "--rollout", "rolling",
        "--rollout-reason", "fixture publication", "--shape-gate-waiver", "fixture publication"])
    real_sync = publish_runtime._fsync_directory
    trusted = {}

    def replace_exposed_generation(path):
        real_sync(path)
        if path != store:
            return
        generation = next(p for p in store.iterdir() if not p.name.startswith("."))
        original = generation.with_name("." + generation.name + ".original")
        generation.rename(original)
        raw = (original / "RUNTIME_VERSION.json").read_bytes()
        trusted["digest"] = hashlib.sha256(raw).hexdigest()
        trusted["generation"] = generation
        shutil.copytree(original, generation)
        unseal(generation)
        member = generation / "src" / "prismabuild" / "core.py"
        member.chmod(0o644)
        member.write_text("raise SystemExit(73)\n")
        receipt = json.loads(raw)
        receipt["files"]["src/prismabuild/core.py"] = hashlib.sha256(member.read_bytes()).hexdigest()
        receipt_path = generation / "RUNTIME_VERSION.json"
        receipt_path.chmod(0o644)
        receipt_path.write_text(json.dumps(receipt))
        publish_runtime._seal_generation(generation)

    monkeypatch.setattr(publish_runtime, "_fsync_directory", replace_exposed_generation)
    assert publish_runtime.main() == 0
    generation = trusted["generation"]
    approval = store / f"{generation.name}.approval"
    assert approval.read_text().strip() == publication.approval_hmac(trusted["digest"], secret)
    mirror.symlink_to(generation)
    monkeypatch.setattr(publication, "_read_approval_key", lambda: secret)
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    result = publication.converge({
        "runtime": str(mirror), "generation_store": str(store)})
    assert result["state"] == "error", result
    assert "no valid publisher approval" in result["error"]
    assert not (publication_store / generation.name).exists()


def test_pending_bootstrap_excludes_ordinary_publication(tmp_path, monkeypatch):
    mirror = tmp_path / 'repo'
    monkeypatch.setattr(publish_runtime, 'MIRROR', mirror)
    pending = tmp_path / 'rollout/pending-bootstrap.json'
    pending.parent.mkdir()
    pending.write_text('{"state":"reserved"}')
    with pytest.raises(SystemExit, match='pending installed bootstrap'):
        publish_runtime._require_no_epoch()


@pytest.mark.parametrize('canary_code', [0, 2, 1])
def test_installed_bootstrap_activates_and_runs_canary_under_lock(tmp_path, monkeypatch, canary_code):
    from bootstrap_fixture import bootstrap_case, assess_case
    from prismabuild import installed_bootstrap
    from prismabuild.digest_primitives import canonical_sha256
    case = bootstrap_case(tmp_path, monkeypatch)
    plan, observations, _, designation = case
    mirror = tmp_path / 'repo'
    source, target = (Path(plan[name]['directory']) for name in ('source', 'target'))
    mirror.symlink_to(source)
    monkeypatch.setattr(publish_runtime, 'MIRROR', mirror)
    monkeypatch.setattr(publish_runtime, '_bootstrap_roster', lambda *a: plan['roster'])
    monkeypatch.setattr(publish_runtime.socket, 'gethostname', lambda: 'sparky')
    assessment = tmp_path / 'assessment.json'
    assessment.write_text(json.dumps({'schema': 'prismabuild.installed_bootstrap.assessment.v1',
        'plan': plan, 'qualification_actions': ['1' * 64], 'rollback_designation': designation}))
    events = []
    def assess(*a, **kw):
        assert publish_runtime._PUBLISH_LOCK_DEPTH > 0
        events.append('fresh-root-assessment')
        return assess_case(case), observations
    def convergence(*a, **kw):
        assert publish_runtime._PUBLISH_LOCK_DEPTH > 0
        assert mirror.resolve() == target
        events.append('convergence')
        return observations
    def canary(**kw):
        assert publish_runtime._PUBLISH_LOCK_DEPTH > 0
        assert kw['enabled'] is True
        events.append('canary')
        if canary_code == 0:
            path = publish_runtime._canary_record_path(target.parent, target.name)
            path.write_text(json.dumps({'generation': target.name, 'commit': plan['target']['commit'], 'canary_status': 'verified'}))
        return canary_code
    monkeypatch.setattr(installed_bootstrap, 'assess', assess)
    monkeypatch.setattr(installed_bootstrap, 'require_convergence', convergence)
    monkeypatch.setattr(publish_runtime, '_run_rollout_canary', canary)
    result = publish_runtime._activate_installed_bootstrap('final', assessment)
    assert result == canary_code
    assert events[:3] == ['fresh-root-assessment', 'convergence', 'canary']
    assert mirror.resolve() == target
    pending = publish_runtime._bootstrap_pending_path()
    if canary_code:
        assert pending.exists()
        # Resumption keeps the original source, even with a final pointer.
        assert json.loads(pending.read_bytes())['source'] == plan['source']
    else:
        assert not pending.exists()
        assert events[-1] == 'convergence'


def test_installed_bootstrap_refuses_substituted_activation_name(tmp_path, monkeypatch):
    monkeypatch.setattr(publish_runtime, 'MIRROR', tmp_path / 'repo')
    from bootstrap_fixture import bootstrap_case
    case = bootstrap_case(tmp_path, monkeypatch)
    plan, _, _, designation = case
    path = tmp_path / 'assessment.json'
    path.write_text(json.dumps({'schema': 'prismabuild.installed_bootstrap.assessment.v1',
        'plan': plan, 'qualification_actions': ['1' * 64], 'rollback_designation': designation}))
    with pytest.raises(SystemExit, match='target differs'):
        publish_runtime._activate_installed_bootstrap('old', path)


def test_bootstrap_ordinary_canary_does_not_receive_a_publication_grant(monkeypatch):
    captured = []
    def driver(*, generation, publication_canary_authorizer=None):
        captured.append(publication_canary_authorizer)
        return 0
    monkeypatch.setattr(publish_runtime, '_load_canary_driver', lambda: driver)
    assert publish_runtime._invoke_canary_driver('generation', publication_slot=False)[0] == 0
    assert captured == [None]
    assert publish_runtime._invoke_canary_driver('generation')[0] == 0
    assert callable(captured[-1])


def test_bootstrap_unreadable_queue_cannot_authorize_activation(tmp_path, monkeypatch):
    from bootstrap_fixture import bootstrap_case, assess_case
    from prismabuild import installed_bootstrap
    from prismabuild.compatibility_window import WindowError
    case = bootstrap_case(tmp_path, monkeypatch)
    plan, observations, _, designation = case
    mirror = tmp_path / 'repo'
    source = Path(plan['source']['directory'])
    mirror.symlink_to(source)
    monkeypatch.setattr(publish_runtime, 'MIRROR', mirror)
    monkeypatch.setattr(publish_runtime, '_bootstrap_roster', lambda *a: plan['roster'])
    monkeypatch.setattr(publish_runtime.socket, 'gethostname', lambda: 'sparky')
    monkeypatch.setattr(installed_bootstrap, 'assess', lambda *a, **kw: (assess_case(case), observations))
    monkeypatch.setattr(installed_bootstrap, 'require_convergence', lambda *a, **kw: observations)
    monkeypatch.setattr(publish_runtime, '_run_rollout_canary', lambda **kw: 1)
    path = tmp_path / 'assessment.json'
    path.write_text(json.dumps({'schema': 'prismabuild.installed_bootstrap.assessment.v1',
        'plan': plan, 'qualification_actions': ['1' * 64], 'rollback_designation': designation}))
    original = os.scandir
    def unreadable(directory):
        if isinstance(directory, (str, os.PathLike)) and Path(directory) == tmp_path / 'pb-queue/ready':
            raise PermissionError('queue census unavailable')
        return original(directory)
    monkeypatch.setattr(os, 'scandir', unreadable)
    with pytest.raises((PermissionError, WindowError), match='queue'):
        publish_runtime._activate_installed_bootstrap('final', path)
    assert mirror.resolve() == source


def test_bootstrap_intent_and_transcript_link_every_new_directory(tmp_path, monkeypatch):
    import os
    from types import SimpleNamespace
    writer = publish_runtime._write_bootstrap_pending
    monkeypatch.setattr(publish_runtime, 'MIRROR', tmp_path / 'fleet/repo')
    fsync = os.fsync
    observed = []
    def syncing(fd):
        observed.append(str(Path('/proc/self/fd', str(fd)).resolve()))
        fsync(fd)
    monkeypatch.setattr(os, 'fsync', syncing)
    path = tmp_path / 'fleet/rollout/pending-bootstrap.json'
    writer(path, {'id': 'durable'})
    assert all(str(p) in observed for p in (tmp_path, tmp_path / 'fleet', path.parent))
    observed.clear()
    recorder = writer.__globals__['_record_bootstrap_root_responses']
    result = recorder({'id': 'durable'}, SimpleNamespace(nonce='a' * 64, started_unix=1, rows={}), 'before')
    target = Path(result['path'])
    assert all(str(p) in observed for p in (path.parent, target.parent.parent, target.parent))


@pytest.mark.parametrize('interrupt_assessment', [False, True])
@pytest.mark.parametrize('pointer', ['source', 'target'])
def test_pending_forward_can_reverse_after_reboot_with_original_owner(tmp_path, monkeypatch, pointer, interrupt_assessment):
    from bootstrap_fixture import bootstrap_case, assess_case
    from prismabuild import installed_bootstrap
    from prismabuild.digest_primitives import canonical_sha256
    case = bootstrap_case(tmp_path, monkeypatch)
    plan, observations, _, designation = case
    source, target = (Path(plan[name]['directory']) for name in ('source', 'target'))
    mirror = tmp_path / 'repo'; mirror.symlink_to(source if pointer == 'source' else target)
    monkeypatch.setattr(publish_runtime, 'MIRROR', mirror)
    monkeypatch.setattr(publish_runtime, '_bootstrap_roster', lambda *a: plan['roster'])
    monkeypatch.setattr(publish_runtime.socket, 'gethostname', lambda: 'sparky')
    forward_hash = canonical_sha256(plan)
    prior = {'schema': 'prismabuild.pending_bootstrap.v1', 'id': plan['id'],
        'direction': 'forward', 'source': plan['source'], 'target': plan['target'],
        'forward_plan_sha256': forward_hash, 'state': 'awaiting-convergence',
        'root_transcript': {'sha256': '9' * 64}}
    pending = publish_runtime._bootstrap_pending_path()
    publish_runtime._write_bootstrap_pending(pending, prior)
    plan['direction'] = 'restore'
    for host, row in observations.rows.items():
        row['boot_id'] = 'new-' + host
        row['broker_process'] = {'pid': 999, 'starttime': '99'}
        row['boot_reconciliation'] = {'original_boot_id': plan['boots'][host],
            'boot_id': row['boot_id'], 'broker_process': row['broker_process'],
            'plan_sha256': forward_hash, 'approved_unix': row['observed_unix'] - .001}
        row['plan_sha256'] = canonical_sha256(plan)
        row['files']['upgrade_client.py'] = plan['source_files']['upgrade_client.py']
        row['captured_sha256'] = row['executing_sha256'] = plan['source_files']['upgrade_client.py']
    assessment = tmp_path / 'reverse.json'
    assessment.write_text(json.dumps({'schema': 'prismabuild.installed_bootstrap.assessment.v1',
        'plan': plan, 'qualification_actions': ['1' * 64], 'rollback_designation': designation}))
    attempts = []
    def assess(*a, **kw):
        assert publish_runtime._PUBLISH_LOCK_DEPTH > 0
        attempts.append(True)
        if interrupt_assessment and len(attempts) == 1:
            raise RuntimeError('fresh observation interrupted')
        return assess_case(case), observations
    def canary(**kw):
        assert mirror.resolve() == source
        decision = json.loads(pending.read_bytes())['direction_decision']
        assert decision['previous_intent'] == prior
        assert decision['plan_sha256'] == canonical_sha256(plan)
        assert decision['from'] == 'forward' and decision['to'] == 'restore'
        path = publish_runtime._canary_record_path(source.parent, source.name)
        path.write_text(json.dumps({'generation': source.name, 'commit': plan['source']['commit'], 'canary_status': 'verified'}))
        return 0
    monkeypatch.setattr(installed_bootstrap, 'assess', assess)
    monkeypatch.setattr(installed_bootstrap, 'require_convergence', lambda *a, **kw: observations)
    monkeypatch.setattr(publish_runtime, '_run_rollout_canary', canary)
    if interrupt_assessment:
        with pytest.raises(RuntimeError, match='observation interrupted'):
            publish_runtime._activate_installed_bootstrap(source.name, assessment)
        selected = json.loads(pending.read_bytes())
        assert selected['direction'] == 'restore' and selected['state'] == 'restore-selected'
        forward = tmp_path / 'forward.json'
        forward.write_text(json.dumps({**json.loads(assessment.read_bytes()), 'plan': {**plan, 'direction': 'forward'}}))
        with pytest.raises(SystemExit, match='direction cannot change'):
            publish_runtime._activate_installed_bootstrap(target.name, forward)
        assert json.loads(pending.read_bytes()) == selected
    assert publish_runtime._activate_installed_bootstrap(source.name, assessment) == 0
    archive = pending.parent / ('bootstrap-' + plan['id'] + '-restore.accepted.json')
    assert json.loads(archive.read_bytes())['direction_decision']['previous_intent'] == prior
    assert not pending.exists()


@pytest.mark.parametrize('foreign', ['id', 'source', 'target', 'forward_plan_sha256', 'accepted', 'direction', 'empty'])
def test_reverse_refuses_foreign_or_unintended_pending_owner(tmp_path, monkeypatch, foreign):
    from bootstrap_fixture import bootstrap_case
    from prismabuild.digest_primitives import canonical_sha256
    plan, _, _, designation = bootstrap_case(tmp_path, monkeypatch)
    mirror = tmp_path / 'repo'; mirror.symlink_to(Path(plan['target']['directory']))
    monkeypatch.setattr(publish_runtime, 'MIRROR', mirror)
    monkeypatch.setattr(publish_runtime, '_bootstrap_roster', lambda *a: plan['roster'])
    monkeypatch.setattr(publish_runtime.socket, 'gethostname', lambda: 'sparky')
    prior = {'schema': 'prismabuild.pending_bootstrap.v1', 'id': plan['id'],
        'source': plan['source'], 'target': plan['target'], 'direction': 'forward',
        'forward_plan_sha256': canonical_sha256(plan), 'state': 'reserved'}
    if foreign == 'empty': prior = {}
    elif foreign == 'accepted': prior['state'] = 'accepted'
    elif foreign == 'direction': prior['direction'] = 'unrelated'
    else: prior[foreign] = 'foreign'
    pending = publish_runtime._bootstrap_pending_path()
    publish_runtime._write_bootstrap_pending(pending, prior)
    plan['direction'] = 'restore'
    path = tmp_path / 'reverse.json'
    path.write_text(json.dumps({'schema': 'prismabuild.installed_bootstrap.assessment.v1',
        'plan': plan, 'qualification_actions': ['1' * 64], 'rollback_designation': designation}))
    with pytest.raises(SystemExit, match='owns publication|direction cannot change'):
        publish_runtime._activate_installed_bootstrap(plan['source']['generation'], path)
    assert json.loads(pending.read_bytes()) == prior
    assert mirror.resolve() == Path(plan['target']['directory'])
