"""#2108: selected class requirements belong to the sealed worker generation.

Production image identities and store roots still need operator approval; an
explicit ``classes={}`` is safe (class_requirements_missing), not legacy mode.
The synthetic declarations below exercise mechanics, not production approvals.
Native availability on broken configuration is covered by
``test_class_image_config_failure.py``; this file tests packaging and loading.
"""
from __future__ import annotations

import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys

import pytest

from prismabuild.container_images import CLASS_REQUIREMENTS_SCHEMA
from prismabuild.digest_primitives import raw_sha256
from test_publish_runtime import ROOT, _fake_git_and_probe, publish_runtime

CONTENT = "content:sha256:" + "a" * 64
OTHER = "content:sha256:" + "b" * 64
# Deliberately synthetic: never assert that the real fleet uses these values.
DECLARATION = {"schema": CLASS_REQUIREMENTS_SCHEMA, "classes": {
    "gb10": {"store_root": "/fixture/docker-gb10", "images": {"fixture-gb10:test": CONTENT}},
    "x86": {"store_root": "/fixture/docker-x86", "images": {"fixture-x86:test": OTHER}},
}}


def _selected_policy_paths():
    roster = json.loads((ROOT / "tools/fleet/fleet_boxes.json").read_text())
    selections = {}
    for name, box in roster["boxes"].items():
        argv = box.get("args", [])
        if "--class" not in argv:
            continue
        index = argv.index("--class")
        assert index + 1 < len(argv), f"{name}: --class has no value"
        if argv[index + 1] != "gb10":
            continue
        assert argv.count("--class-image-config") == 1, (
            f"{name}: GB10 must explicitly select one generation-bound class policy; "
            "absence silently leaves legacy container admission"
        )
        index = argv.index("--class-image-config")
        assert index + 1 < len(argv), f"{name}: --class-image-config has no value"
        value = argv[index + 1]
        selected = PurePosixPath(value)
        assert (not selected.is_absolute() and str(selected) == value
                and selected.parts and not {".", ".."}.intersection(selected.parts)), (
            f"{name}: class policy must be a canonical generation-relative path: {value!r}"
        )
        assert selected.parent in (PurePosixPath("tools"), PurePosixPath("tools/fleet")), (
            f"{name}: selected policy must use a shipped fleet layout: {value!r}"
        )
        assert selected.name in publish_runtime.FLEET_DATA, (
            f"{name}: {selected.name} is absent from hashed FLEET_DATA membership"
        )
        assert (ROOT / "tools/fleet" / selected.name).is_file(), (
            f"{name}: selected policy source is missing: tools/fleet/{selected.name}"
        )
        selections[name] = Path(value)
    assert selections, "the current fleet must explicitly select its GB10 class policy"
    return selections


def _stage(monkeypatch, checkout, mirror):
    # The shared helper mocks Git and the publisher's import probe only for this
    # call. It is permissive, so the actual isolated import below is essential.
    # Real copying, sealing, receipt construction and main's stage-only path run.
    with monkeypatch.context() as patch:
        patch.setattr(publish_runtime, "CHECKOUT", checkout)
        patch.setattr(publish_runtime, "MIRROR", mirror)
        patch.setattr(publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40))
        patch.setattr(sys, "argv", [
            "publish_runtime.py", "--stage-only", "--rollout", "rolling",
            "--rollout-reason", "fixture only", "--shape-gate-waiver", "fixture only",
        ])
        return publish_runtime.main()


def _stage_generation(monkeypatch, checkout, mirror):
    assert _stage(monkeypatch, checkout, mirror) == 0
    assert not mirror.exists(), "staging must not activate the live fleet mirror"
    generations = [path for path in (mirror.parent / "runtime-generations").iterdir()
                   if (path / "RUNTIME_VERSION.json").is_file()]
    assert len(generations) == 1, "expected exactly one successfully staged generation"
    return generations[0]


def _assert_sealed_policy(generation, selections):
    receipt = json.loads((generation / "RUNTIME_VERSION.json").read_text())
    for relative in set(selections.values()):
        shipped = generation / relative
        assert shipped.is_file(), f"selected generation path does not exist: {relative}"
        for layout in (Path("tools") / relative.name, Path("tools/fleet") / relative.name):
            member = generation / layout
            assert member.is_file(), f"selected policy missing from layout: {layout}"
            assert member.read_bytes() == shipped.read_bytes()
            assert raw_sha256(member.read_bytes()) == receipt["files"][layout.as_posix()]
            assert member.stat().st_mode & 0o222 == 0, f"unsealed policy: {layout}"


# -I excludes the caller's import paths; imports and policy bytes must come from
# the generation even with the mutable checkout advertised through PYTHONPATH.
_ISOLATED_IMPORTS = '''import json, sys
from pathlib import Path
root = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root / "src"))
from prismabuild import container_images as images, core
assert Path(images.__file__).resolve().is_relative_to(root)
assert Path(core.__file__).resolve().is_relative_to(root)
'''


def _isolated(generation, tmp_path, script, *args):
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", _ISOLATED_IMPORTS + script,
         str(generation), *args], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _copy_checkout(tmp_path):
    """Private fleet data, real package sources, no Git tree or live mutation."""
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    for child in ROOT.iterdir():
        if child.name not in {".git", "tools"}:
            (checkout / child.name).symlink_to(child, target_is_directory=child.is_dir())
    shutil.copytree(ROOT / "tools", checkout / "tools")
    return checkout


def test_rostered_class_policy_is_sealed_and_loads_without_checkout(
        tmp_path, monkeypatch, synthetic_generation):
    selections = _selected_policy_paths()
    generation = _stage_generation(monkeypatch, ROOT, tmp_path / "fleet/repo")
    _assert_sealed_policy(generation, selections)
    _isolated(generation, tmp_path, '''
for name in json.loads(sys.argv[2]):
    policy = images.ClassImagePolicy.from_file(name, source_root=root, klass="gb10")
    assert policy is not None, "explicit configuration must not select legacy mode"
    verdict = policy.evaluate(None, now=1000.)
    assert verdict["status"] == "unknown"
    assert verdict["container_work_eligible"] is False
    declaration = json.loads((root / name).read_text())
    expected = ("inventory_unknown" if "gb10" in declaration["classes"]
                else "class_requirements_missing")
    assert verdict["reason"] == expected, verdict
    assert len(verdict["requirements_sha256"]) == 64
''', json.dumps(sorted({p.as_posix() for p in selections.values()})))
    _assert_generation_policy_checks_only_selected_class_not_all_inventory(
        tmp_path, synthetic_generation)


@pytest.fixture
def synthetic_generation(tmp_path, monkeypatch):
    # Mechanics support the roster acceptance test, not standalone acceptance.
    # Add synthetic data membership and roster selection in a private checkout;
    # the test must still assert the real roster/member exists.
    checkout = _copy_checkout(tmp_path)
    relative = Path("tools/fleet/fixture-class-images.json")
    (checkout / relative).write_text(json.dumps(DECLARATION))
    roster_path = checkout / "tools/fleet/fleet_boxes.json"
    roster = json.loads(roster_path.read_text())
    selections = {}
    for name, box in roster["boxes"].items():
        argv = box.get("args", [])
        if "--class" in argv and argv[argv.index("--class") + 1] == "gb10":
            if "--class-image-config" in argv:
                index = argv.index("--class-image-config")
                del argv[index:index + 2]
            argv.extend(["--class-image-config", relative.as_posix()])
            selections[name] = relative
    assert selections, "mechanics fixture needs a GB10 roster entry"
    roster_path.write_text(json.dumps(roster))
    with monkeypatch.context() as patch:
        patch.setattr(publish_runtime, "FLEET_DATA",
                      (*publish_runtime.FLEET_DATA, relative.name))
        generation = _stage_generation(patch, checkout, tmp_path / "synthetic/fleet/repo")
    _assert_sealed_policy(generation, selections)
    return generation, selections


def _assert_generation_policy_checks_only_selected_class_not_all_inventory(
        tmp_path, synthetic_generation):
    generation, selections = synthetic_generation
    # Exercise both compatibility layouts, regardless of the roster's choice.
    paths = sorted({(parent / path.name).as_posix()
                    for path in selections.values()
                    for parent in (Path("tools"), Path("tools/fleet"))})
    _isolated(generation, tmp_path, '''
declaration = json.loads(sys.argv[3])
for name in json.loads(sys.argv[2]):
    policy = images.ClassImagePolicy.from_file(name, source_root=root, klass="gb10")
    assert policy.requirements_sha256 == core.canonical_sha256(declaration)
    required = declaration["classes"]["gb10"]
    content = required["images"]["fixture-gb10:test"]
    extra = "content:sha256:" + "c" * 64
    inventory = {"schema": images.INVENTORY_SCHEMA, "observed_unix": 1000.,
                 "entries": [content, extra], "store_root": required["store_root"],
                 "image_contents": {**required["images"], "irrelevant:test": extra}}
    def check(snapshot, status, reason, eligible=False, now=1000.):
        verdict = policy.evaluate(snapshot, now=now)
        assert (verdict["status"], verdict["reason"], verdict["container_work_eligible"]) == (
            status, reason, eligible), verdict
        assert verdict["requirements_sha256"] == policy.requirements_sha256
        return verdict
    check(inventory, "satisfied", "class_images_present", True)
    # Extra local images are irrelevant; the other class's required image is
    # absent and its store differs, yet neither can poison GB10 admission.
    check({**inventory, "entries": [content], "image_contents": required["images"]},
          "satisfied", "class_images_present", True)
    other = images.ClassImagePolicy.from_file(name, source_root=root, klass="x86")
    other_inventory = {**inventory, "store_root": declaration["classes"]["x86"]["store_root"]}
    verdict = other.evaluate(other_inventory, now=1000.)
    assert (verdict["status"], verdict["reason"], verdict["missing_images"]) == (
        "refused", "required_image_missing", ["fixture-x86:test"]), verdict
    missing = images.ClassImagePolicy.from_file(name, source_root=root, klass="unapproved")
    verdict = missing.evaluate(inventory, now=1000.)
    assert verdict["reason"] == "class_requirements_missing"
    assert verdict["status"] == "unknown" and verdict["container_work_eligible"] is False
    check(None, "unknown", "inventory_unknown")
    check(inventory, "unknown", "inventory_unknown", now=1000. + images.INVENTORY_TTL_S + 1.)
    check({**inventory, "store_root": "/fixture/wrong-store"}, "refused", "image_store_mismatch")
    verdict = check({**inventory, "image_contents": {"irrelevant:test": extra}},
                    "refused", "required_image_missing")
    assert verdict["missing_images"] == ["fixture-gb10:test"]
    check({**inventory, "image_contents": {"fixture-gb10:test": extra}},
          "refused", "image_content_mismatch")
''', json.dumps(paths), json.dumps(DECLARATION))


@pytest.mark.parametrize("failure", ["missing", "malformed", "invalid_schema", "unreadable"])
def test_selected_policy_member_cannot_be_silently_omitted(
        tmp_path, monkeypatch, capsys, failure):
    """A selected member is mandatory even though optional fleet data may skip."""
    selections = _selected_policy_paths()
    checkout = _copy_checkout(tmp_path)
    # Establish that this exact checkout and mocked main path can stage before
    # injecting the policy failure. Generic setup/API errors are not acceptance.
    _stage_generation(monkeypatch, checkout, tmp_path / "baseline/repo")
    capsys.readouterr()
    member = checkout / "tools/fleet" / sorted(set(selections.values()))[0].name
    if failure == "missing":
        member.unlink()
    elif failure == "malformed":
        member.write_text('{"schema":')
    elif failure == "invalid_schema":
        member.write_text(json.dumps({"schema": CLASS_REQUIREMENTS_SCHEMA, "classes": []}))
    else:
        # chmod is not a reliable unreadability fixture under a root test arm.
        original_open = Path.open

        def blocked_open(path, *args, **kwargs):
            if path == member:
                raise PermissionError(f"fixture selected policy unreadable: {member.name}")
            return original_open(path, *args, **kwargs)

        monkeypatch.setattr(Path, "open", blocked_open)
    mirror = tmp_path / "rejected/repo"
    diagnostic = ""
    try:
        result = _stage(monkeypatch, checkout, mirror)
    except (OSError, ValueError) as error:
        result, diagnostic = 1, str(error)
    except SystemExit as error:
        result, diagnostic = error.code, str(error)
    captured = capsys.readouterr()
    diagnostic += captured.out + captured.err
    assert result not in (0, None), "publisher accepted an unusable selected policy member"
    assert any(label in diagnostic.lower() for label in (
        member.name.lower(), "class image", "class-image",
    )), f"refusal must identify the selected policy, not an unrelated failure: {diagnostic}"
    assert not mirror.exists(), "refused policy must not activate a mirror"
    generations = mirror.parent / "runtime-generations"
    if generations.exists():
        assert not list(generations.glob("*/RUNTIME_VERSION.json")), (
            "refused policy must not leave a publishable generation receipt"
        )
