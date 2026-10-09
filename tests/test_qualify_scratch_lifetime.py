"""The scratch-lifetime qualifier refuses unsafe input before any effect (#1360)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import qualify_scratch_lifetime as qualification


def test_unknown_scenario_exits_without_effect():
    with pytest.raises(SystemExit) as caught:
        qualification.main(["--scenario", "no-such-path",
                            "--temp-root-env", "TEMP_ROOT", "--temp-name", "row-temp",
                            "--cache-root-env", "CACHE_ROOT", "--cache-name", "compile"])
    assert caught.value.code == 2


def test_missing_action_identity_refuses(tmp_path, monkeypatch):
    monkeypatch.delenv("PRISMABUILD_ACTION_KEY", raising=False)
    with pytest.raises(SystemExit, match="PRISMABUILD_ACTION_KEY is absent"):
        qualification.main(["--scenario", "normal",
                            "--temp-root-env", "TEMP_ROOT", "--temp-name", "row-temp",
                            "--cache-root-env", "CACHE_ROOT", "--cache-name", "compile",
                            "--queue-root", str(tmp_path)])


def test_launcher_roles_require_a_rendezvous_id(tmp_path, monkeypatch):
    from argparse import Namespace
    args = Namespace(rendezvous_id="", rendezvous_root="/mnt/shared/pb-qualification")
    with pytest.raises(SystemExit, match="rendezvous-id"):
        qualification._rendezvous_dir(args)
    args = Namespace(rendezvous_id="../escape", rendezvous_root="/mnt/shared/pb-qualification")
    with pytest.raises(SystemExit, match="rendezvous-id"):
        qualification._rendezvous_dir(args)


def test_rendezvous_root_cannot_escape_qualification(tmp_path):
    from argparse import Namespace
    args = Namespace(rendezvous_id="case-1", rendezvous_root=str(tmp_path))
    with pytest.raises(SystemExit, match="beneath /mnt/shared/pb-qualification"):
        qualification._rendezvous_dir(args)


def test_killer_requires_the_victim_key():
    from argparse import Namespace
    args = Namespace(rendezvous_id="case-1", rendezvous_root="/mnt/shared/pb-qualification",
                     victim_key="short")
    with pytest.raises(SystemExit, match="victim action key"):
        qualification._launcher_killer(args, "k" * 64, {"host": "sparky"})
