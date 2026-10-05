"""A gang member keeps the whole ``pbrun`` submission contract (#1517).

``pbgang`` submits each member through ``pbrun``.  It used to forward only a
tag, argv, demand, env, timeout, priority and cpus, so a member that needs a
GPU subset, an exclusive measurement window, a container image or a data
manifest could not be expressed.  Each such member field is now one ``pbrun``
flag, and ``pbrun`` itself stays the judge of every value.

The produced command line is fed to ``pbrun``'s real argument parser, so a
flag the table names wrongly fails here and not on a live window.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbgang  # noqa: E402
import pbrun  # noqa: E402

IMAGE = "content:sha256:" + "a" * 64
GROUP = "0" * 32


def _manifest(tmp_path: Path, member: dict, *, other: dict | None = None) -> Path:
    members = [{"tag": "sparky", "argv": ["/bin/true"], **member},
               {"tag": "sparklina", "argv": ["/bin/true"], **(other or {})}]
    path = tmp_path / "gang.json"
    path.write_text(json.dumps({"priority": 10, "timeout_s": 600, "members": members}))
    return path


def _command(tmp_path: Path, member: dict, index: int = 0) -> list[str]:
    manifest = pbgang.load(_manifest(tmp_path, member))
    args = SimpleNamespace(cwd=tmp_path)
    return pbgang.member_command(args, manifest, manifest["members"][index],
                                 group=GROUP, index=index)


WINDOW_MEMBER = {
    "gpu": True, "gpu_memory_gb": 102, "exclusive": True, "measurement": True,
    "host_class": "gb10", "container_image": [IMAGE],
    "data_manifest": "/mnt/shared/manifests/window.json", "residency": "stage",
    "residency_mover_mem_gb": 2, "progress_phase": ["load=600", "run=1200"],
    "progress_cycle": True, "deterministic": True,
}


def test_a_full_member_reaches_pbruns_own_parser_with_every_option(tmp_path):
    command = _command(tmp_path, WINDOW_MEMBER)
    assert command.count("--") == 1
    flags = command[:command.index("--")]
    args = pbrun.parse_args([*flags[2:], "--", "/bin/true"])
    assert args.gpu is True and args.exclusive is True and args.measurement is True
    assert args.gpu_memory_gb == 102.0
    assert args.host_class == "gb10"
    assert args.container_image == [IMAGE]
    assert args.data_manifest == "/mnt/shared/manifests/window.json"
    assert args.residency == "stage" and args.residency_mover_mem_gb == 2
    assert args.progress_phase == ["load=600", "run=1200"]
    assert args.progress_cycle is True and args.deterministic is True
    assert (args.gang_group, args.gang_size, args.gang_index) == (GROUP, 2, 0)
    assert args.priority == 10 and args.tag == ["sparky"]


def test_a_member_with_only_the_old_fields_builds_the_same_command(tmp_path):
    member = {"demand": "gpu=1,mem_gb=100", "cpus": 4, "env": ["K=V"]}
    command = _command(tmp_path, member)
    assert command[2:] == [
        "--cwd", str(tmp_path), "--detach", "--tag", "sparky",
        "--gang-group", GROUP, "--gang-size", "2", "--gang-index", "0",
        "--priority", "10", "--timeout-s", "600",
        "--demand", "gpu=1,mem_gb=100", "--cpus", "4", "--env", "K=V",
        "--", "/bin/true"]


def test_a_false_switch_adds_no_flag(tmp_path):
    command = _command(tmp_path, {"gpu": False, "exclusive": False})
    assert "--gpu" not in command and "--exclusive" not in command


def test_a_repeated_field_repeats_its_flag(tmp_path):
    other = "content:sha256:" + "b" * 64
    command = _command(tmp_path, {"container_image": [IMAGE, other]})
    pairs = [command[i + 1] for i, token in enumerate(command)
             if token == "--container-image"]
    assert pairs == [IMAGE, other]


@pytest.mark.parametrize("member", [
    {"gpu": "yes"}, {"exclusive": 1}, {"gpu_memory_gb": True},
    {"gpu_memory_gb": [102]}, {"gpu_memory_gb": ""},
    {"container_image": IMAGE}, {"container_image": []}, {"container_image": [""]},
    {"progress_phase": "load=600"}, {"data_manifest": None},
], ids=["gpu-string", "exclusive-int", "memory-bool", "memory-list", "memory-empty",
        "image-string", "image-empty-list", "image-empty-entry", "phase-string",
        "manifest-none"])
def test_a_badly_typed_option_is_refused_by_name(tmp_path, member):
    with pytest.raises(SystemExit, match="field"):
        pbgang.load(_manifest(tmp_path, member))


@pytest.mark.parametrize("name", [
    "gang_group", "gang_size", "gang_index", "detach", "cwd", "withdraw",
    "max_attempts", "retry_safe", "after",
])
def test_a_driver_owned_flag_cannot_be_smuggled_in_as_a_member_field(tmp_path, name):
    with pytest.raises(SystemExit, match="allowed fields"):
        pbgang.load(_manifest(tmp_path, {name: 1}))
