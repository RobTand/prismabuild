"""A fleet tool reads its default root when it is called, not when it is defined.

``tests/conftest.py`` repoints every module's live-store attribute, ``SH`` and
its siblings, at a directory under ``tmp_path`` for every test.  That reaches
code that reads the attribute when it runs.  It cannot reach a parameter
default, because a default is evaluated once, when the ``def`` executes, and
``fleet_submit.submit`` built its ``queue_root`` default from ``SH`` right
there.  A test that called ``submit`` on the pool transport without naming a
``queue_root`` therefore published into the fleet's live queue on the shared
mount, and a pull-queue worker on another box claimed the item.

The sweep here holds every fleet tool to the rule; the second test drives the
call that leaked.
"""
from __future__ import annotations

import importlib
import inspect
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

import fleet_submit  # noqa: E402

from test_slurm_lane import _paper_action  # noqa: E402

#: Where the fleet's store is mounted on every box.
LIVE_MOUNT = "/mnt/shared"

#: The fleet tools whose module attributes ``conftest`` repoints, plus every
#: other tool an operator runs.  Each is already imported by its own tests.
FLEET_TOOLS = (
    "fleet_submit", "pool_reset", "pbrun", "pbstatus", "pbsweep", "pbwait",
    "pbcampaign",
    "pbtest", "worker_loop", "supervise", "require_pool", "seal_and_publish",
    "tessera_status",
)


def definition_time_live_defaults(module) -> list[str]:
    """Every parameter of ``module``'s own functions that defaults under the mount."""

    found = []
    for name, obj in sorted(vars(module).items()):
        if not inspect.isfunction(obj) or obj.__module__ != module.__name__:
            continue
        for parameter in inspect.signature(obj).parameters.values():
            default = parameter.default
            if isinstance(default, (str, Path)) and str(default).startswith(
                LIVE_MOUNT
            ):
                found.append(f"{module.__name__}.{name}({parameter.name}={default})")
    return found


@pytest.mark.parametrize("name", FLEET_TOOLS)
def test_no_fleet_tool_binds_a_live_root_into_a_default(name: str) -> None:
    module = importlib.import_module(name)
    assert definition_time_live_defaults(module) == []


def test_submit_without_a_queue_root_lands_under_the_repointed_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The call that leaked, made against a repointed ``SH``.

    The sweep runs first so that, on a tree where the default is still bound
    to the mount, this test fails before it publishes anything.
    """

    assert definition_time_live_defaults(fleet_submit) == []
    root = tmp_path / "fleet"
    monkeypatch.setattr(fleet_submit, "SH", root)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "pool")
    request = cas.publish_action_request(action)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="pool",
        checkout_root=checkout,
    )

    key = str(action["action_key"])
    assert submission.transport == "pool"
    assert (root / "pb-queue" / pool.READY / f"{key}.json").is_file()
    assert Path(submission.where).is_relative_to(root)
