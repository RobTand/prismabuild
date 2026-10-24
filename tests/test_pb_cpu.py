"""Provisioning decisions use fakes; fleet receipts cover real fresh venvs."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "maintenance"))
import pb_cpu


@pytest.fixture
def cpu_fixture():
    package = {"name": "torch", "version": "2.11.0+cpu", "reason": "Project CPU tests",
               "sha256": ["a" * 64]}
    definition = {"machine": "x86_64", "python": [3, 14], "packages": [package]}
    observed = {"implementation": "CPython", "machine": "x86_64", "python": [3, 14],
                "distributions": [{"name": "Torch", "version": "2.11.0+cpu"}]}
    return definition, observed


@pytest.fixture
def cpu_roster(tmp_path, monkeypatch):
    roster = tmp_path / "checkout" / "tools" / "fleet" / "fleet_boxes.json"
    roster.parent.mkdir(parents=True)
    live = tmp_path / "relocated-environment"
    live.mkdir()
    roster.write_text(json.dumps({"pb_cpu_live_roots": [str(live)]}))
    monkeypatch.setattr(pb_cpu, "CPU_ROOT", tmp_path / "checkout")
    return roster, live


def test_cpu_drift_preserves_local_versions_and_reports_all_changes(cpu_fixture):
    definition, observed = cpu_fixture
    definition["packages"].append({"name": "jsonschema", "version": "4.26.0"})
    observed["distributions"] = [{"name": "torch", "version": "2.11.0"},
                                  {"name": "unexpected", "version": "1"}]
    assert pb_cpu.cpu_compare(definition, observed) == {
        "ok": False, "missing": {"jsonschema": "4.26.0"}, "extra": {"unexpected": "1"},
        "different": {"torch": {"expected": "2.11.0+cpu", "installed": "2.11.0"}},
        "duplicates": []}


def test_cpu_names_follow_distribution_normalization(cpu_fixture):
    definition, observed = cpu_fixture
    definition["packages"][0]["name"] = "Example_pkg.name"
    observed["distributions"][0]["name"] = "example-pkg-name"
    assert pb_cpu.cpu_compare(definition, observed)["ok"]
    observed["distributions"] *= 2
    assert pb_cpu.cpu_compare(definition, observed)["duplicates"] == ["example-pkg-name"]


@pytest.mark.parametrize("machine,minor", [("x86_64", [3, 14]), ("aarch64", [3, 12])])
def test_cpu_committed_definitions_are_hashed_and_include_project_dependencies(machine, minor):
    path, definition = pb_cpu.cpu_read_definition(
        {"implementation": "CPython", "machine": machine, "python": minor})
    packages = {p["name"].lower(): p for p in definition["packages"]}
    assert packages["jsonschema"]["version"] == "4.26.0"
    assert packages["torch"]["version"] == "2.11.0+cpu"
    assert path.name.startswith(machine)
    assert "--hash=sha256:" in pb_cpu.cpu_requirements(definition)


@pytest.mark.parametrize("change", [{"python": [3, 13]}, {"machine": "riscv64"},
                                  {"implementation": "PyPy"}])
def test_cpu_unsupported_interpreters_refuse(cpu_fixture, change):
    _, observed = cpu_fixture
    observed.update(change)
    with pytest.raises(ValueError, match="unsupported interpreter"):
        pb_cpu.cpu_read_definition(observed)


@pytest.mark.parametrize("corruption", ["duplicate", "hash", "reason", "version", "host"])
def test_cpu_definition_input_is_validated(tmp_path, cpu_fixture, corruption):
    definition, observed = cpu_fixture
    if corruption == "duplicate":
        definition["packages"] *= 2
    elif corruption == "host":
        definition["python"] = [3, 12]
    else:
        definition["packages"][0][{"hash": "sha256", "reason": "reason",
                                    "version": "version"}[corruption]] = {
            "hash": [], "reason": "", "version": "1\n--index-url=evil"}[corruption]
    (tmp_path / "x86_64-cp314.json").write_text(json.dumps(definition))
    with pytest.raises(ValueError):
        pb_cpu.cpu_read_definition(observed, tmp_path)


def test_cpu_build_uses_exact_hashes_and_verifies_result(tmp_path, cpu_fixture, cpu_roster):
    definition, observed = cpu_fixture
    calls = []

    def run(arguments, **options):
        calls.append(arguments)
        assert options["check"]
        if "-r" in arguments:
            text = Path(arguments[-1]).read_text()
            assert text == "torch==2.11.0+cpu --hash=sha256:" + "a" * 64 + "\n"
            assert "--require-hashes" in arguments and "--no-deps" in arguments
        return subprocess.CompletedProcess(arguments, 0, stdout=json.dumps(observed))

    assert pb_cpu.cpu_build(Path("/base/python"), tmp_path / "new", definition, run)["ok"]
    assert calls[0] == ["/base/python", "-I", "-m", "venv", str(tmp_path / "new")]
    assert calls[-1][0] == str(tmp_path / "new" / "bin" / "python")


def test_cpu_build_propagates_install_failure(tmp_path, cpu_fixture, cpu_roster):
    def fail(arguments, **options):
        raise subprocess.CalledProcessError(1, arguments)

    with pytest.raises(subprocess.CalledProcessError):
        pb_cpu.cpu_build(Path("/base/python"), tmp_path / "new", cpu_fixture[0], fail)


def test_cpu_build_reports_post_install_drift(tmp_path, cpu_fixture, cpu_roster):
    definition, observed = cpu_fixture
    observed["distributions"].clear()

    def run(arguments, **options):
        return subprocess.CompletedProcess(arguments, 0, stdout=json.dumps(observed))

    result = pb_cpu.cpu_build(Path("/base/python"), tmp_path / "new", definition, run)
    assert not result["ok"]
    assert result["missing"] == {"torch": "2.11.0+cpu"}


@pytest.mark.parametrize("configured_alias", [False, True])
@pytest.mark.parametrize("flat_roster", [False, True])
def test_cpu_build_refuses_relocated_roots_and_symlink_aliases(
        tmp_path, cpu_fixture, cpu_roster, configured_alias, flat_roster):
    roster, live = cpu_roster
    link = tmp_path / "alias"
    link.symlink_to(live, target_is_directory=True)
    other = tmp_path / "another-live-environment"
    other.mkdir()
    if flat_roster:
        roster = roster.rename(roster.parent.parent / "fleet_boxes.json")
    roster.write_text(json.dumps({"pb_cpu_live_roots": [
        str(link if configured_alias else live), str(other)]}))
    calls = []
    for root in (live, link, other):
        for destination in (root, root / "new", root / "new" / "nested"):
            with pytest.raises(ValueError, match="live pb-cpu"):
                pb_cpu.cpu_build(Path("/base/python"), destination, cpu_fixture[0],
                                 lambda *args, **kwargs: calls.append(args))
    assert not calls
    assert list(live.iterdir()) == list(other.iterdir()) == []
    with pytest.raises(ValueError, match="already exists"):
        pb_cpu.cpu_build_destination(tmp_path)
    sibling = live.with_name(live.name + "-fresh")
    assert pb_cpu.cpu_build_destination(sibling) == sibling
    outside = tmp_path / "outside-alias"
    outside.symlink_to(sibling, target_is_directory=True)
    assert pb_cpu.cpu_build_destination(outside / "new") == sibling / "new"


@pytest.mark.parametrize("document", [
    {}, {"pb_cpu_live_roots": []}, {"pb_cpu_live_roots": None},
    {"pb_cpu_live_roots": "/live"}, {"pb_cpu_live_roots": ["relative/live"]},
    {"pb_cpu_live_roots": [""]}, {"pb_cpu_live_roots": [12]},
    {"pb_cpu_live_roots": {"/live": True}}, {"pb_cpu_live_roots": ["/bad\0path"]},
])
def test_cpu_build_requires_valid_protection_config(tmp_path, cpu_roster, document):
    roster, _ = cpu_roster
    roster.write_text(json.dumps(document))
    destination = tmp_path / "fresh"
    with pytest.raises(ValueError, match="pb_cpu_live_roots"):
        pb_cpu.cpu_build_destination(destination)
    assert not destination.exists()


def test_cpu_build_refuses_unreadable_or_malformed_roster(tmp_path, cpu_roster):
    roster, _ = cpu_roster
    for raw in ("{", '{"pb_cpu_live_roots": [], "pb_cpu_live_roots": []}'):
        roster.write_text(raw)
        with pytest.raises(ValueError):
            pb_cpu.cpu_build_destination(tmp_path / "fresh")
    roster.unlink()
    with pytest.raises(FileNotFoundError):
        pb_cpu.cpu_build_destination(tmp_path / "fresh")
    assert not (tmp_path / "fresh").exists()


def test_cpu_cli_machine_report_and_exit_codes(tmp_path, monkeypatch, capsys, cpu_fixture):
    definition, observed = cpu_fixture
    monkeypatch.setattr(pb_cpu, "CPU_ROOT", tmp_path / "no-roster")
    monkeypatch.setattr(pb_cpu, "cpu_observe", lambda _: observed)
    monkeypatch.setattr(pb_cpu, "cpu_read_definition", lambda _: (Path("fixture.json"), definition))
    assert pb_cpu.cpu_main(["--python", "/named/python", "check"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"]
    observed["distributions"].clear()
    assert pb_cpu.cpu_main(["--python", "/named/python", "check"]) == 1
    assert json.loads(capsys.readouterr().out)["missing"] == {"torch": "2.11.0+cpu"}


def test_cpu_cli_probe_failure_is_json(monkeypatch, capsys):
    def fail(_):
        raise FileNotFoundError("missing interpreter")

    monkeypatch.setattr(pb_cpu, "cpu_observe", fail)
    assert pb_cpu.cpu_main(["--python", "/missing", "check"]) == 2
    assert "missing interpreter" in json.loads(capsys.readouterr().out)["error"]
