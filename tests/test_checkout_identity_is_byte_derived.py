"""The action key follows the tree's bytes, not Git's rendering of them.

Issue #57: ``git_checkout_identity`` hashed ``git diff --binary HEAD``.  That
is porcelain output, so the submitter's own Git configuration reached into the
key.  A ``diff`` driver, ``diff.noprefix`` or ``core.abbrev`` rewrote the patch
text for the same bytes, so two people with identical working trees derived two
action keys and the store ran the work twice.  ``GIT_EXTERNAL_DIFF`` was worse:
it emptied the patch, so a dirty tree derived the clean tree's key and the CAS
answered a dirty submission with a result the dirty code never produced.

The fix reads the same delta through ``git diff-index``, which is plumbing and
reads none of that configuration.  The key itself must not move, so the first
two tests pin today's derivation: one against the legacy command over a matrix
of ordinary dirty trees, one against a literal digest measured on both Git
versions the fleet runs (2.43.0 on the Sparks, 2.53.0 on dl380g10).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb  # noqa: E402

#: ``git_checkout_identity``'s ``dirty_sha256`` for the tree ``_dirty_tree``
#: builds.  Measured on git 2.43.0 and 2.53.0, which agree.  A change here is
#: a change to every action key the fleet has ever minted, so this constant is
#: not to be refreshed to match new output without deciding that deliberately.
ANCHOR_DIRTY_SHA256 = (
    "7e6bfe8a4734e2bb52b080477435de221fb60313d64d200fda82f4b61a86a18c"
)

#: ``sha256(b"")``: the digest a tree with no delta against HEAD derives.
CLEAN_DIRTY_SHA256 = hashlib.sha256(b"").hexdigest()

LEGACY_DIFF = ("diff", "--binary", "HEAD")


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        errors="surrogateescape",
        check=True,
    )
    return completed.stdout


def _repository(root: Path) -> Path:
    root.mkdir(parents=True)
    _git(root, "init", "-q", ".")
    _git(root, "config", "user.email", "test@example.invalid")
    _git(root, "config", "user.name", "Test")
    return root


def _legacy_dirty_sha256(root: Path) -> str:
    """Derive the key the way the unfixed code did, for comparison."""

    return hashlib.sha256(
        _git(root, *LEGACY_DIFF).encode("utf-8", "surrogateescape")
    ).hexdigest()


def _dirty_tree(root: Path) -> Path:
    """One tree exercising text, binary and rename deltas at once."""

    _repository(root)
    (root / "big.txt").write_text(
        "".join(f"{number}\n" for number in range(1, 201))
    )
    (root / "a.txt").write_text("aaa\nbbb\n")
    (root / "b.bin").write_bytes(b"\x00\x01\x02binary\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "init")
    lines = (root / "big.txt").read_text().splitlines(keepends=True)
    lines[9] = "TEN\n"
    lines[99] = "HUNDRED\n"
    (root / "big.txt").write_text("".join(lines))
    (root / "b.bin").write_bytes(b"\x00\x09\x02changed\n")
    _git(root, "mv", "a.txt", "c.txt")
    return root


def _text_delta(root: Path) -> Path:
    _repository(root)
    (root / "doc.dat").write_text("aaa\nbbb\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "init")
    (root / "doc.dat").write_text("aaa\nCCC\n")
    return root


def test_the_anchor_tree_still_derives_the_key_it_always_did(
    tmp_path: Path,
) -> None:
    """A known tree keeps its known key, so nothing in the store is orphaned."""

    root = _dirty_tree(tmp_path / "checkout")

    assert pb.git_checkout_identity(root)["dirty_sha256"] == ANCHOR_DIRTY_SHA256


@pytest.mark.parametrize(
    "state",
    [
        "clean",
        "modified",
        "staged",
        "deleted",
        "renamed",
        "binary",
        "mode",
        "added",
        "everything",
    ],
)
def test_an_ordinary_tree_derives_the_legacy_key(
    tmp_path: Path, state: str
) -> None:
    """Every ordinary working-tree shape hashes to what it hashed before."""

    root = _repository(tmp_path / "checkout")
    (root / "one.txt").write_text("".join(f"{n}\n" for n in range(1, 61)))
    (root / "two.txt").write_text("second\n")
    (root / "three.bin").write_bytes(b"\x00\x01\x02\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "init")
    if state in {"modified", "everything"}:
        (root / "one.txt").write_text("changed\n")
    if state in {"staged", "everything"}:
        (root / "two.txt").write_text("staged\n")
        _git(root, "add", "two.txt")
    if state in {"deleted", "everything"}:
        _git(root, "rm", "-q", "three.bin")
    if state == "renamed":
        _git(root, "mv", "one.txt", "moved.txt")
    if state == "binary":
        (root / "three.bin").write_bytes(b"\x03\x04\x05\n")
    if state == "mode":
        (root / "two.txt").chmod(0o755)
    if state == "added":
        (root / "four.txt").write_text("new\n")
        _git(root, "add", "four.txt")

    identity = pb.git_checkout_identity(root)

    assert identity["dirty_sha256"] == _legacy_dirty_sha256(root)


def test_an_external_diff_driver_no_longer_empties_the_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``GIT_EXTERNAL_DIFF`` used to make a dirty tree derive the clean key."""

    root = _text_delta(tmp_path / "checkout")
    expected = pb.git_checkout_identity(root)["dirty_sha256"]
    assert expected != CLEAN_DIRTY_SHA256
    monkeypatch.setenv("GIT_EXTERNAL_DIFF", "/bin/true")

    identity = pb.git_checkout_identity(root)

    assert identity["dirty_sha256"] == expected


def test_a_textconv_driver_does_not_move_the_key(tmp_path: Path) -> None:
    """A per-clone attributes file is not part of the tree's bytes."""

    root = _text_delta(tmp_path / "checkout")
    expected = pb.git_checkout_identity(root)["dirty_sha256"]
    attributes = root / ".git" / "info" / "attributes"
    attributes.parent.mkdir(parents=True, exist_ok=True)
    attributes.write_text("*.dat diff=upper\n")
    _git(root, "config", "diff.upper.textconv", "tr a-z A-Z <")

    identity = pb.git_checkout_identity(root)

    assert identity["dirty_sha256"] == expected


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("core.abbrev", "12"),
        ("diff.noprefix", "true"),
        ("diff.mnemonicPrefix", "true"),
        ("diff.renames", "false"),
        ("diff.external", "/bin/true"),
        ("color.ui", "always"),
        ("diff.context", "7"),
    ],
)
def test_presentation_config_does_not_move_the_key(
    tmp_path: Path, name: str, value: str
) -> None:
    """Config that only changes how a diff reads must not change the key."""

    root = _dirty_tree(tmp_path / "checkout")
    expected = pb.git_checkout_identity(root)["dirty_sha256"]
    _git(root, "config", name, value)

    identity = pb.git_checkout_identity(root)

    assert identity["dirty_sha256"] == expected


@pytest.mark.parametrize("ignored_path", ["secret.txt", "nested/"])
def test_personal_excludes_do_not_hide_untracked_bytes(
    tmp_path: Path, ignored_path: str
) -> None:
    root = _text_delta(tmp_path / "checkout")
    (root / "nested").mkdir()
    (root / "nested" / "data.txt").write_text("nested payload\n")
    (root / "secret.txt").write_text("secret payload\n")
    expected = pb.git_checkout_identity(root)
    personal = tmp_path / "personal-ignore"
    personal.write_text(ignored_path + "\n")
    _git(root, "config", "core.excludesFile", str(personal))
    assert pb.git_checkout_identity(root) == expected


@pytest.mark.parametrize("rule_location", [".gitignore", ".git/info/exclude"])
def test_repository_ignore_rules_still_apply(tmp_path: Path, rule_location: str) -> None:
    root = _text_delta(tmp_path / "checkout")
    (root / rule_location).write_text("ignored.txt\n")
    before = pb.git_checkout_identity(root)
    (root / "ignored.txt").write_text("ignored bytes\n")
    assert pb.git_checkout_identity(root) == before


@pytest.mark.parametrize("ignored_path", ["pipe", "nested/"])
def test_personal_excludes_cannot_hide_unsupported_inodes(
    tmp_path: Path, ignored_path: str
) -> None:
    import os

    root = _text_delta(tmp_path / "checkout")
    (root / "nested").mkdir()
    os.mkfifo(root / ("nested/pipe" if ignored_path.endswith("/") else "pipe"))
    personal = tmp_path / "personal-ignore"
    personal.write_text(ignored_path + "\n")
    _git(root, "config", "core.excludesFile", str(personal))
    with pytest.raises(pb.ActionContractError, match="unsupported file type"):
        pb.git_checkout_identity(root)
