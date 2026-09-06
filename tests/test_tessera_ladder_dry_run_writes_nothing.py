"""A dry run of the ladder dispatcher leaves the shared checkout alone.

``dispatch_tessera_ladder`` staged its wrapper into the shared checkout before
it looked at ``--dry-run``, so the one flag that promises nothing will change
copied a file into a tree both boxes execute. A dry run now reads the selected
source and constructs the same content-addressed closure as a submission,
without staging anything. Without an explicit --wrapper, the shared checkout
wrapper is the source.

The evidence is a full listing of the checkout with sizes, modification times
and content digests, taken before the run and after it. "The file is not
there" would not have caught ``copy2`` overwriting a wrapper that was already
staged, which is the case the shared checkout is actually in.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import types

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402

import dispatch_tessera_ladder as ladder  # noqa: E402
import fleet_submit  # noqa: E402


def _listing(root: Path) -> dict[str, tuple[int, int, str]]:
    """Every path under ``root``, with its size, mtime and content digest."""

    found: dict[str, tuple[int, int, str]] = {}
    for path in sorted(root.rglob("*")):
        name = str(path.relative_to(root))
        if path.is_dir():
            found[name] = (-1, -1, "dir")
            continue
        stat = path.stat()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        found[name] = (stat.st_size, stat.st_mtime_ns, digest)
    return found


@pytest.fixture()
def checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A shared checkout with a wrapper already staged in it.

    The staged wrapper differs from the local one on purpose. That is the
    state the real checkout is in between runs, and it is what makes a write
    visible as a changed digest rather than only as a changed mtime.
    """

    root = tmp_path / "checkout"
    encoder = root / "tessera" / "src" / "audit_encoder.py"
    encoder.parent.mkdir(parents=True)
    encoder.write_text("VALUE = 1\n", encoding="utf-8")
    (root / ladder.WRAPPER).write_text("# staged earlier\n", encoding="utf-8")

    local = tmp_path / "local" / ladder.WRAPPER
    local.parent.mkdir()
    local.write_text("# the box-local copy, which differs\n", encoding="utf-8")

    monkeypatch.setattr(ladder, "CHECKOUT", root)
    monkeypatch.setattr(ladder, "PYTHON", sys.executable)
    monkeypatch.setattr(ladder, "SOURCE", str(tmp_path / "unused-model"))
    return root


def test_a_dry_run_changes_nothing_in_the_shared_checkout(
    checkout: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """The defect: ``--dry-run`` staged the wrapper before it read the flag."""

    before = _listing(checkout)
    monkeypatch.setattr(
        sys, "argv",
        ["dispatch_tessera_ladder", "--shards", "1-3", "--dry-run"])

    assert ladder.main() in (None, 0)

    assert _listing(checkout) == before
    printed = capsys.readouterr().out
    assert "(dry run)" in printed


def test_a_dry_run_with_nothing_staged_refuses_rather_than_previewing(
    checkout: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """No staged wrapper means no closure this run could seal."""

    (checkout / ladder.WRAPPER).unlink()
    before = _listing(checkout)
    monkeypatch.setattr(
        sys, "argv", ["dispatch_tessera_ladder", "--shards", "1", "--dry-run"])

    assert ladder.main() == 1

    assert _listing(checkout) == before
    assert "ladder wrapper:" in capsys.readouterr().err


def test_a_real_run_still_stages_the_wrapper(
    checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dry run stopped writing; the run that submits did not."""

    local = checkout.parent / "local" / ladder.WRAPPER
    published: list[dict] = []

    class _CAS:
        def __init__(self, root):
            self.root = Path(root)

        def publish_action_request(self, action):
            published.append(action)
            return self.root / "requests" / "unused.json"

    monkeypatch.setattr(pb, "PrismaBuildCAS", _CAS)
    monkeypatch.setattr(
        fleet_submit, "submit",
        lambda action, **_kwargs: types.SimpleNamespace(
            action_key=str(action["action_key"]), describe=lambda: "queued"))
    monkeypatch.setattr(
        sys, "argv", ["dispatch_tessera_ladder", "--shards", "2", "--wrapper", str(local)])

    assert ladder.main() in (None, 0)

    assert (checkout / ladder.WRAPPER).read_text() == "# staged earlier\n"
    assert len(published) == 1
    staged = checkout / published[0]["params"]["wrapper_source"]["staged_path"]
    assert staged.read_bytes() == local.read_bytes()
