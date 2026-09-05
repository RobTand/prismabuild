"""The sealed checkout bundle is a function of the tree, not of the box.

Every pbrun action key contains the sha256 of the Git bundle the submitter
seals, twice: once in ``params.checkout_snapshot.input`` and once in
``inputs``.  The bundle was byte-nondeterministic, so an unchanged tree sealed
to a new action key on every submission and no CAS hit, campaign resume or
singleton hold could ever apply to a real repository.

Two independent causes were measured on sparky (git 2.43.0, 20 cores) against
this repository, 4253 loose objects:

* Git's delta search runs one thread per core and the thread that wins a
  candidate decides the delta base.  Three seals gave three digests and three
  action keys.  Below roughly a thousand delta-able objects the search is
  single-threaded and the old code looked correct, which is why a suite built
  on fifteen-object fixtures never saw it -- so the fixture here is sized past
  that threshold, measured on this box, and doubled.
* A delta already sitting in a source pack is reused verbatim, so the same
  tree sealed to different bytes before and after a ``git gc``.  No ``git -c``
  key reaches that; ``--no-reuse-delta``/``--no-reuse-object`` do, and
  ``git bundle create`` accepts neither, which is why the sealer now writes the
  bundle header itself and runs ``pack-objects`` directly.

Pre-fix rates, ten trials each on this box: two seals of the 2000-file fixture
disagreed in 8 of 10 trials, so the four seals this file asks for fail on the
old sealer with near certainty rather than merely usually.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as core_module  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

#: Twice the measured threshold at which Git's delta search starts using more
#: than one thread on this box: 500 files sealed identically eight times out
#: of eight, 1000 gave three distinct digests in eight seals, 2000 gave four.
THREADED_FILE_COUNT = 2000

MAX_BYTES = 64 * 1024 * 1024


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args], capture_output=True, text=True,
        check=False,
    )


def _repository(root: Path, *, files: int) -> Path:
    """A checkout whose blobs delta well, so the delta search has work to do.

    Distinct random files would defeat the search entirely and the old sealer
    would look deterministic at any size: the objects have to be similar for
    threads to have candidates to steal from one another.
    """

    root.mkdir(parents=True)
    assert _git(root, "init", "-q", "-b", "main").returncode == 0
    assert _git(root, "config", "user.email", "test@example.invalid").returncode == 0
    assert _git(root, "config", "user.name", "PrismaBuild test").returncode == 0
    body = "\n".join(
        f"line {index} of a file that is mostly like its neighbours"
        for index in range(40)
    )
    for index in range(files):
        (root / f"f{index:05d}.txt").write_text(f"{body}\nunique marker {index}\n")
    assert _git(root, "add", "-A").returncode == 0
    assert _git(root, "commit", "-qm", "sealed tree").returncode == 0
    return root


def _seal(checkout: Path, cas_root: Path, **kwargs: object) -> dict[str, object]:
    cas = core_module.PrismaBuildCAS(cas_root)
    return pbrun.build_git_checkout_snapshot(
        checkout, cas=cas, max_bytes=MAX_BYTES, **kwargs   # type: ignore[arg-type]
    )


def _action_key(snapshot: dict[str, object], checkout: Path) -> str:
    """The action a submission would seal, reduced to what the bundle moves."""

    return str(
        core_module.seal_action(
            {
                "schema": core_module.ACTION_SCHEMA_V2,
                "task": {
                    "definition_id": "fleet/pbrun",
                    "definition_version": "v1",
                    "task_class": "generation",
                    "determinism": "deterministic",
                    "artifact_family": "generic",
                    "artifact_kind": "generic",
                    "argv": ["/bin/true"],
                    "working_directory": ".",
                    "result_path": "result.txt",
                },
                "inputs": [snapshot["input"]],
                "code_closure": core_module.build_code_closure(
                    checkout, ["f00000.txt"]
                ),
                "params": {
                    "command": ["/bin/true"],
                    "cwd": str(checkout),
                    "demand": {"cpu": 1},
                    "checkout_snapshot": snapshot,
                },
                "environment": {"variables": {}, "toolchain": {}},
                "execution_scope": {
                    "portability": "portable",
                    "platform_key": None,
                    "host_class": None,
                },
            }
        )["action_key"]
    )


def test_the_pack_command_pins_every_setting_that_moves_its_bytes(
    tmp_path: Path, monkeypatch,
) -> None:
    """The argv, not a comment, is what the next box has to agree with.

    Recorded from a real seal rather than read off the constant, so a pin that
    is declared but not passed fails here.
    """

    checkout = _repository(tmp_path / "checkout", files=4)
    recorded: list[list[str]] = []
    real_run = pbrun.subprocess.run

    def record(argv, *args, **kwargs):
        if isinstance(argv, list):
            recorded.append([str(part) for part in argv])
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(pbrun.subprocess, "run", record)
    _seal(checkout, tmp_path / "cas")

    packs = [argv for argv in recorded if "pack-objects" in argv]
    assert len(packs) == 1, "the seal builds exactly one pack"
    argv = packs[0]
    pinned = {
        argv[index + 1] for index, part in enumerate(argv) if part == "-c"
    }
    for key, value in pbrun.BUNDLE_PACK_CONFIGURATION:
        assert f"{key}={value}" in pinned, (
            f"{key} is not pinned on the pack command line"
        )
    # The two that no configuration key can express, and the reason this is
    # not ``git bundle create`` any more.
    assert "--no-reuse-delta" in argv
    assert "--no-reuse-object" in argv
    # Pinning has to happen before the subcommand or Git ignores it.
    assert argv.index("pack-objects") > max(
        index for index, part in enumerate(argv) if part == "-c"
    )
    assert "bundle" not in argv


def test_a_thread_sized_tree_seals_to_one_digest_and_one_action_key(
    tmp_path: Path,
) -> None:
    """The defect itself: an unchanged tree, sealed four times over.

    Four seals rather than two because the old failure is a race; see the
    module docstring for the measured rate.
    """

    checkout = _repository(tmp_path / "checkout", files=THREADED_FILE_COUNT)
    digests = set()
    keys = set()
    for attempt in range(4):
        snapshot = _seal(checkout, tmp_path / f"cas{attempt}")
        digests.add(str(snapshot["input"]["sha256"]))   # type: ignore[index]
        keys.add(_action_key(snapshot, checkout))

    assert len(digests) == 1, f"one tree sealed to {len(digests)} bundles"
    assert len(keys) == 1, f"one tree sealed to {len(keys)} action keys"


def test_the_digest_survives_a_gc_of_the_source_objects(tmp_path: Path) -> None:
    """Storage layout is not part of the tree, so it may not move the key.

    ``git gc`` runs on its own schedule inside anybody's checkout.  A sealer
    that reuses the deltas it finds would move every action key each time one
    fired, which is the same defect on a slower clock.
    """

    checkout = _repository(tmp_path / "checkout", files=THREADED_FILE_COUNT)
    loose = _seal(checkout, tmp_path / "cas-loose")
    assert _git(checkout, "gc", "-q").returncode == 0
    assert _git(checkout, "count-objects", "-v").stdout.startswith("count: 0")
    packed = _seal(checkout, tmp_path / "cas-packed")

    assert loose["input"] == packed["input"]
    assert _action_key(loose, checkout) == _action_key(packed, checkout)


def test_the_sealed_bundle_is_a_bundle_git_can_read(tmp_path: Path) -> None:
    """The header is written here now, so prove Git still accepts the file."""

    checkout = _repository(tmp_path / "checkout", files=4)
    assert _git(checkout, "branch", "gate-base").returncode == 0
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")
    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, cas=cas, max_bytes=MAX_BYTES, snapshot_refs=["gate-base"],
    )
    bundle = cas.input_path(snapshot["input"])

    heads = _git(checkout, "bundle", "list-heads", str(bundle))
    assert heads.returncode == 0, heads.stderr
    advertised = dict(
        (fields[1], fields[0])
        for line in heads.stdout.splitlines()
        if len(fields := line.split(maxsplit=1)) == 2
    )
    sealed_ref = f"refs/heads/{core_module.PBRUN_CHECKOUT_SNAPSHOT_REF_NAME}"
    assert advertised[sealed_ref] == snapshot["commit"]
    assert advertised["refs/heads/gate-base"] == snapshot["refs"]["gate-base"]

    materialized = tmp_path / "materialized"
    materialized.mkdir()
    assert _git(materialized, "init", "-q").returncode == 0
    assert _git(
        materialized, "fetch", "-q", "--no-tags", str(bundle), sealed_ref,
        "refs/heads/gate-base:refs/heads/gate-base",
    ).returncode == 0
    assert _git(
        materialized, "checkout", "-q", "--detach", str(snapshot["commit"])
    ).returncode == 0
    assert (materialized / "f00000.txt").exists()
    verified = _git(materialized, "bundle", "verify", str(bundle))
    assert verified.returncode == 0, verified.stderr
