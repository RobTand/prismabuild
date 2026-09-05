"""Docker's global options must not hide the subcommand from the shim.

``tools/fleet/docker`` is what every action's ``docker`` resolves to, and it
decided what a command was by reading ``argv[0]``.  Every documented spelling
that puts an option first therefore walked past it: ``docker --context default
run -d IMAGE`` reached the daemon with no owner label, no job label and no
marker, so ``pool.cleanup_action_containers`` read the absent marker as "this
action started nothing" and the Epilog's label query found nothing to remove.
A detached container created that way keeps the GPU and the memory after the
reservation ends.  ``docker --context default start`` and ``docker compose -f
f.yml up -d`` escaped the two refusals for the same reason.

Nothing here contacts a Docker daemon: the shim's own testing gate points it
at a fake CLI that records the argv it was handed.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
SHIM = ROOT / "tools" / "fleet" / "docker"
OWNER = "1" * 64


def _shim(tmp_path: Path, argv: list[str], *, cgroup: str | None = None,
          affinity: set[int] | None = None,
          endpoint: str = "unix:///var/run/docker.sock", docker_env: dict | None = None):
    """Run the real shim against a recording fake CLI, under its testing gate."""

    real = tmp_path / "fake-docker"
    real.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib, sys\n"
        "if 'context' in sys.argv and 'inspect' in sys.argv:\n"
        "    pathlib.Path(os.environ['INSPECTED']).write_text(json.dumps(sys.argv[1:]))\n"
        "    print(json.dumps(os.environ['FAKE_ENDPOINT']))\n"
        "    sys.exit(0)\n"
        "pathlib.Path(os.environ['CALLED']).write_text(json.dumps(sys.argv[1:]))\n"
    )
    real.chmod(0o755)
    called = tmp_path / "called.json"
    marker = tmp_path / "owner.used"
    environment = dict(os.environ)
    environment.update({
        "CALLED": str(called),
        "INSPECTED": str(tmp_path / "inspected.json"),
        "FAKE_ENDPOINT": endpoint,
        "PRISMABUILD_CONTAINER_OWNER": OWNER,
        "PRISMABUILD_CONTAINER_MARKER": str(marker),
        "PRISMABUILD_DOCKER_REAL": str(real),
        "PRISMABUILD_DOCKER_TESTING": "1",
    })
    if docker_env:
        environment.update(docker_env)
    if cgroup is not None:
        path = tmp_path / "cgroup"
        path.write_text(cgroup, encoding="utf-8")
        environment["PRISMABUILD_CGROUP_FILE"] = str(path)
    result = subprocess.run(
        (["taskset", "--cpu-list", ",".join(map(str, sorted(affinity)))]
         if affinity is not None else []) + [str(SHIM), *argv],
        env=environment, capture_output=True, text=True,
        check=False,
    )
    forwarded = json.loads(called.read_text()) if called.exists() else None
    return result, marker.exists(), forwarded


def _labels(forwarded: list[str]) -> list[str]:
    return [forwarded[index + 1] for index, token in enumerate(forwarded)
            if token == "--label" and index + 1 < len(forwarded)]


@pytest.mark.parametrize("argv,at", [
    (["run", "--rm", "example:image"], 1),
    (["--context", "default", "run", "-d", "example:image"], 3),
    (["--context=default", "run", "-d", "example:image"], 2),
    (["-cdefault", "run", "-d", "example:image"], 2),
    (["--debug", "container", "create", "example:image"], 3),
    (["-D", "-H", "unix:///var/run/docker.sock", "run", "example:image"], 4),
    (["--log-level", "debug", "run", "--rm", "example:image"], 3),
])
def test_a_creation_behind_global_options_is_still_labelled(
    tmp_path: Path, argv: list[str], at: int
) -> None:
    """The labels go in after the subcommand, wherever the subcommand is."""

    result, marked, forwarded = _shim(tmp_path, argv)

    assert result.returncode == 0, result.stderr
    assert marked, "the action's container marker was never written"
    # Docker resolves the original globals before the shim pins the selected
    # local endpoint; unrelated options survive, while context/host selectors
    # become one explicit --host to prevent a context-switch race.
    relative = 2 if argv[at - 2:at - 1] == ["container"] else 1
    global_count = at - relative
    inspected = json.loads((tmp_path / "inspected.json").read_text())
    assert inspected[:global_count] == argv[:global_count]
    created = next(i for i, token in enumerate(forwarded) if token in {"run", "create"}) + 1
    assert forwarded[created:created + 2] == ["--label", f"prismabuild.action={OWNER}"]
    assert forwarded[created + 2] == "--cpuset-cpus"
    assert forwarded[created + 4:] == argv[at:]


def test_the_job_label_also_survives_a_global_option(tmp_path: Path) -> None:
    """Both labels, so one action's two SLURM jobs stay distinguishable."""

    result, _marked, forwarded = _shim(
        tmp_path, ["--context", "default", "run", "--rm", "example:image"],
        cgroup="0::/system.slice/slurmstepd.scope/job_4242/step_batch/user\n",
    )

    assert result.returncode == 0, result.stderr
    assert _labels(forwarded) == [f"prismabuild.action={OWNER}",
                                  "prismabuild.job=4242"]


@pytest.mark.parametrize("argv", [
    ["--context", "default", "start", "existing-container"],
    ["--debug", "container", "start", "existing-container"],
    ["compose", "-f", "compose.yml", "up", "-d"],
    ["--context", "default", "compose", "-f", "compose.yml", "up", "-d"],
    ["compose", "--project-name", "p", "up"],
    ["compose", "-f", "compose.yml", "run", "web", "true"],
    ["compose", "-f", "compose.yml", "create"],
    ["compose", "-f", "compose.yml", "start"],
])
def test_an_unownable_lifecycle_form_is_refused_wherever_the_options_sit(
    tmp_path: Path, argv: list[str]
) -> None:
    """Compose owns the labels on what it creates, so none of its starting
    verbs can carry this action's owner label; ``compose up`` was only the
    spelling that had been noticed."""

    result, marked, forwarded = _shim(tmp_path, argv)

    assert result.returncode == 125, result.stdout
    assert "cannot attach PrismaBuild's owner label" in result.stderr
    assert forwarded is None, "a refused command must not reach Docker"
    assert not marked


@pytest.mark.parametrize("argv", [
    ["ps", "-a"],
    ["--context", "default", "ps", "-a"],
    ["--debug", "inspect", "example"],
    ["compose", "-f", "compose.yml", "ps"],
    ["compose", "-f", "compose.yml", "down"],
    ["--context", "default", "rm", "-f", "example"],
])
def test_everything_that_starts_no_container_passes_through_untouched(
    tmp_path: Path, argv: list[str]
) -> None:
    result, marked, forwarded = _shim(tmp_path, argv)

    assert result.returncode == 0, result.stderr
    assert forwarded == argv
    assert not marked, "nothing was created, so nothing is marked"


@pytest.mark.parametrize("argv", [
    ["run", "--label=prismabuild.action", "example:image"],
    ["run", "-lprismabuild.job=9999", "example:image"],
    ["run", "-l=prismabuild.action=x", "example:image"],
    ["--context", "default", "run", "--label", "prismabuild.job=1", "img"],
])
def test_every_spelling_of_a_reserved_label_is_refused(
    tmp_path: Path, argv: list[str]
) -> None:
    """A caller that can set these labels can hand its container to another
    job's Epilog, or hide it from its own.  The fixed-token read saw only
    ``--label prismabuild.x=y`` and ``--label=prismabuild.x=y``."""

    result, marked, forwarded = _shim(tmp_path, argv)

    assert result.returncode == 125, result.stdout
    assert "may not set the reserved" in result.stderr
    assert forwarded is None and not marked


def test_a_global_log_level_is_not_read_as_a_label(tmp_path: Path) -> None:
    """``-l`` is the log level before the subcommand and the label after it."""

    result, marked, forwarded = _shim(
        tmp_path, ["-l", "debug", "run", "--rm", "example:image"])

    assert result.returncode == 0, result.stderr
    assert marked
    assert _labels(forwarded) == [f"prismabuild.action={OWNER}"]


def test_an_unknown_global_option_is_refused_rather_than_guessed(
    tmp_path: Path,
) -> None:
    """Guessing an unknown option's arity is the fixed-position read again:
    one token out and a ``start`` reads as an option's value."""

    result, marked, forwarded = _shim(
        tmp_path, ["--nonesuch", "value", "run", "example:image"])

    assert result.returncode == 125, result.stdout
    assert "--nonesuch" in result.stderr
    assert forwarded is None and not marked
