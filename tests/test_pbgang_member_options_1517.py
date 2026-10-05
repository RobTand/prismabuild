"""A gang member carries the ``pbrun`` options a measurement window declares (#1517).

``pbgang`` submits each member through ``pbrun``.  It used to forward only a
tag, argv, demand, env, timeout, priority and cpus, so a member that needs a
GPU subset, an exclusive measurement window, a container image or one declared
attempt could not be expressed.  Exactly those existing options are now member
fields, one ``pbrun`` flag each, and ``pbrun`` itself stays the judge of every
value.  Anything else is refused by name.

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
    "gpu_memory_gb": 102, "exclusive": True, "measurement": True,
    "host_class": "gb10", "max_attempts": 1, "priority_reason": "Window 4 pair",
    "container_images": [IMAGE],
}


def test_a_full_member_reaches_pbruns_own_parser_with_every_option(tmp_path):
    command = _command(tmp_path, WINDOW_MEMBER)
    assert command.count("--") == 1
    flags = command[:command.index("--")]
    args = pbrun.parse_args([*flags[2:], "--", "/bin/true"])
    assert args.exclusive is True and args.measurement is True
    assert args.gpu_memory_gb == 102.0
    assert args.host_class == "gb10"
    assert args.container_image == [IMAGE]
    assert args.max_attempts == 1 and args.priority_reason == "Window 4 pair"
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
    command = _command(tmp_path, {"measurement": False, "exclusive": False})
    assert "--measurement" not in command and "--exclusive" not in command


def test_a_repeated_field_repeats_its_flag(tmp_path):
    other = "content:sha256:" + "b" * 64
    command = _command(tmp_path, {"container_images": [IMAGE, other]})
    pairs = [command[i + 1] for i, token in enumerate(command)
             if token == "--container-image"]
    assert pairs == [IMAGE, other]


@pytest.mark.parametrize("member", [
    {"exclusive": 1}, {"measurement": "yes"}, {"gpu_memory_gb": True},
    {"gpu_memory_gb": [102]}, {"gpu_memory_gb": ""}, {"host_class": ["gb10"]},
    {"container_images": IMAGE}, {"container_images": []}, {"container_images": [""]},
    {"priority_reason": None},
], ids=["exclusive-int", "measurement-string", "memory-bool", "memory-list", "memory-empty",
        "class-list", "image-string", "image-empty-list", "image-empty-entry",
        "reason-none"])
def test_a_badly_typed_option_is_refused_by_name(tmp_path, member):
    with pytest.raises(SystemExit, match="field"):
        pbgang.load(_manifest(tmp_path, member))


@pytest.mark.parametrize("name", [
    "gang_group", "gang_size", "gang_index", "detach", "withdraw", "retry_safe",
    "after", "gpu", "gpu_capacity",
    "progress_phases", "progress_cycle", "deterministic", "profile", "transport",
])
def test_a_driver_owned_or_undeclared_option_is_refused_by_name(tmp_path, name):
    with pytest.raises(SystemExit, match=f"'{name}'"):
        pbgang.load(_manifest(tmp_path, {name: 1}))


def test_a_member_may_declare_one_attempt_and_no_more(tmp_path):
    assert "--max-attempts" in _command(tmp_path, {"max_attempts": 1})
    with pytest.raises(SystemExit, match="one attempt"):
        pbgang.load(_manifest(tmp_path, {"max_attempts": 2}))


def test_a_residency_member_reaches_pbruns_own_parser_with_every_option(tmp_path):
    member = {"data_manifest": "/data/window4/manifest.json", "residency": "stage",
              "residency_tier": "prismabuild-stage:dl380g10", "residency_ram": "auto",
              "residency_share": "auto", "residency_mover_mem_gb": 2,
              "residency_mover_readers": 4, "residency_prefetch_depth_gib": 40,
              "residency_read_mb_s": 800, "residency_mover_max_attempts": 3}
    command = _command(tmp_path, member)
    assert command.count("--") == 1
    flags = command[:command.index("--")]
    assert flags[-20:] == [
        "--data-manifest", "/data/window4/manifest.json",
        "--residency", "stage",
        "--residency-tier", "prismabuild-stage:dl380g10",
        "--residency-ram", "auto",
        "--residency-share", "auto",
        "--residency-mover-mem-gb", "2",
        "--residency-mover-readers", "4",
        "--residency-prefetch-depth-gib", "40",
        "--residency-read-mb-s", "800",
        "--residency-mover-max-attempts", "3"]
    args = pbrun.parse_args([*flags[2:], "--", "/bin/true"])
    assert args.data_manifest == "/data/window4/manifest.json"
    assert args.residency == "stage"
    assert args.residency_tier == "prismabuild-stage:dl380g10"
    assert args.residency_ram == "auto" and args.residency_share == "auto"
    assert args.residency_mover_mem_gb == 2.0 and args.residency_mover_readers == 4
    assert args.residency_prefetch_depth_gib == 40.0 and args.residency_read_mb_s == 800.0
    assert args.residency_mover_max_attempts == 3


def test_the_residency_options_forward_in_table_order_after_the_window_options(tmp_path):
    command = _command(tmp_path, {"measurement": True, "residency": "stage",
                                  "data_manifest": "/m.json"})
    flags = command[:command.index("--")]
    assert flags.index("--measurement") < flags.index("--data-manifest") \
        < flags.index("--residency") < len(flags)


@pytest.mark.parametrize("member", [
    {"data_manifest": ""}, {"data_manifest": True}, {"data_manifest": ["m"]},
    {"residency": ""}, {"residency": True}, {"residency": ["stage"]},
    {"residency_ram": {}}, {"residency_mover_readers": None},
    {"residency_read_mb_s": False},
])
def test_a_badly_typed_residency_option_is_refused_by_name(tmp_path, member):
    with pytest.raises(SystemExit, match="field"):
        pbgang.load(_manifest(tmp_path, member))


@pytest.mark.parametrize("name", [
    "data_manifests", "residency_other", "residency_mover",
    "residency_mover_mem", "residency_prefetch_depth",
])
def test_a_misspelled_residency_option_stays_an_unknown_field(tmp_path, name):
    with pytest.raises(SystemExit, match=f"'{name}'"):
        pbgang.load(_manifest(tmp_path, {name: 1}))


def test_a_priority_reason_is_a_member_field_and_a_manifest_default(tmp_path):
    path = tmp_path / "gang.json"
    path.write_text(json.dumps({
        "priority": 10, "priority_reason": "window default",
        "members": [{"tag": "sparky", "argv": ["/bin/true"]},
                    {"tag": "sparklina", "argv": ["/bin/true"],
                     "priority_reason": "member own"}]}))
    manifest = pbgang.load(path)
    args = SimpleNamespace(cwd=tmp_path)
    reasons = []
    for index, member in enumerate(manifest["members"]):
        command = pbgang.member_command(args, manifest, member, group=GROUP, index=index)
        reasons.append([command[i + 1] for i, token in enumerate(command)
                        if token == "--priority-reason"])
    assert reasons == [["window default"], ["member own"]]


def test_a_manifest_priority_reason_must_be_text(tmp_path):
    path = tmp_path / "gang.json"
    path.write_text(json.dumps({"priority_reason": ["x"], "members": [
        {"tag": "a", "argv": ["x"]}, {"tag": "b", "argv": ["x"]}]}))
    with pytest.raises(SystemExit, match="priority_reason"):
        pbgang.load(path)


# The prepared Window 4 pair, as the flat list its owner wrote: two members, each
# with its own host, resource demand, GPU subset, exclusive measurement class,
# one attempt, priority and reason, image, timeout and a mapping environment.
WINDOW4_CWD = "/mnt/shared/tessera-measurements/window4-953-approved-b5e154-20261005"
WINDOW4_IMAGE = ("localhost/prismaquant/spark-vllm-nccl230@sha256:"
                 "5be13705acaecc7b4aaf342a84f80d67844c9970ff8375bf9fbeecc9c98ce84a")
WINDOW4_ENV = {
    "TS": "/mnt/shared/tessera-runs/worktrees/ts-d13-public-2dbac191",
    "FABRIC": "socket", "WINDOW_MODE": "window4-eager-2048-4096",
    "GRAPH_PEER_WAIT_SECONDS": "3600", "OMP_NUM_THREADS": "1", "MAX_JOBS": "1",
}


def _window4_member(host: str, cpus: int) -> dict:
    return {
        "argv": ["/usr/bin/python3", "tools/window4.py", "--arm", "eager"],
        "cwd": WINDOW4_CWD, "tags": [host],
        "demand": {"cpu": cpus, "mem_gb": 104, "gpu": 1},
        "gpu_memory_gb": 102, "exclusive": True, "measurement": True,
        "host_class": "gb10", "max_attempts": 1, "priority": 10,
        "priority_reason": "Goal: exact reviewed A8S eager Window4 MNBT2048/4096",
        "container_images": [WINDOW4_IMAGE], "timeout_s": 5400, "env": dict(WINDOW4_ENV),
    }


def test_the_window4_pair_reaches_pbruns_argv_intact(tmp_path):
    path = tmp_path / "window4.json"
    path.write_text(json.dumps([_window4_member("sparklina", 8), _window4_member("sparky", 6)]))
    manifest = pbgang.load(path)
    args = SimpleNamespace(cwd=None)
    for index, (host, cpus) in enumerate([("sparklina", 8), ("sparky", 6)]):
        command = pbgang.member_command(args, manifest, manifest["members"][index],
                                        group=GROUP, index=index)
        flags = command[2:command.index("--")]
        parsed = pbrun.parse_args([*flags, "--", *command[command.index("--") + 1:]])
        assert str(parsed.cwd) == WINDOW4_CWD
        assert parsed.tag == [host]
        assert parsed.demand == f"cpu={cpus},mem_gb=104,gpu=1"
        assert parsed.gpu_memory_gb == 102.0
        assert parsed.exclusive is True and parsed.measurement is True
        assert parsed.host_class == "gb10" and parsed.max_attempts == 1
        assert parsed.priority == 10
        assert parsed.priority_reason == "Goal: exact reviewed A8S eager Window4 MNBT2048/4096"
        assert parsed.container_image == [WINDOW4_IMAGE]
        assert parsed.timeout_s == 5400
        assert parsed.env == [f"{key}={value}" for key, value in WINDOW4_ENV.items()]
        assert (parsed.gang_group, parsed.gang_size, parsed.gang_index) == (GROUP, 2, index)
        assert command[command.index("--") + 1:] == [
            "/usr/bin/python3", "tools/window4.py", "--arm", "eager"]


@pytest.mark.parametrize("member", [
    {"tag": "sparky", "tags": ["sparky"]}, {"tags": []}, {"tags": [""]},
    {"demand": {"cpu": True}}, {"demand": {"": 1}}, {"demand": 7},
    {"env": {"K": ["v"]}}, {"env": [1]}, {"cwd": ""},
], ids=["tag-and-tags", "tags-empty", "tags-blank-entry", "demand-bool", "demand-blank-key",
        "demand-number", "env-list-value", "env-number-entry", "cwd-empty"])
def test_a_malformed_native_shape_is_refused_by_name(tmp_path, member):
    base = {"tag": "sparky", "argv": ["/bin/true"]}
    if "tag" in member or "tags" in member:
        base.pop("tag")
    path = tmp_path / "gang.json"
    path.write_text(json.dumps([{**base, **member}, {"tag": "sparklina", "argv": ["/bin/true"]}]))
    with pytest.raises(SystemExit, match="member 0"):
        pbgang.load(path)


def test_cwd_is_required_unless_every_member_names_one(tmp_path, capsys):
    path = tmp_path / "gang.json"
    path.write_text(json.dumps([_window4_member("sparky", 6),
                                {"tag": "sparklina", "argv": ["/bin/true"]}]))
    with pytest.raises(SystemExit):
        pbgang.main(["--manifest", str(path), "--queue", str(tmp_path / "q")])
    assert "--cwd is required" in capsys.readouterr().err
