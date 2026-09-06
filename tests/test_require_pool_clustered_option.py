"""A clustered short option must not hide the test runner behind it.

Issue #225.  ``TEST_WORK`` bounds a runner name with ``(?<![\\w.-])``, and the
character before the name in ``python3 -m<runner>`` is ``m`` -- a word
character -- so the lookbehind fails, nothing matches, and the command is
allowed.  Measured on ``0cb95b8``::

    python3 -m <runner> tests   -> refused   (correct)
    python3 -m<runner> tests    -> ALLOWED   (the gap)

``python3 -m<runner>`` is a spelling CPython accepts and that people type.  It
is the ordinary clustered short-option form, not an exotic evasion, so a guard
that reads one and not the other is reading the option and not the command.

This is a TIGHTENING, so the file pins both directions: the shapes that must
start being refused, and the shapes that were allowed before and must stay
allowed after.  ``-m`` is a very common option character, and the risk a
narrowing like this carries is that ``-m`` on some other command starts
reading as an interpreter switch.

Nothing here starts work or reads the live flag: the module is loaded directly
and asked about strings.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "require_pool.py"

_SPEC = importlib.util.spec_from_file_location("require_pool_clustered", HOOK)
hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hook)                        # type: ignore[union-attr]

#: Assembled rather than written, so this file does not itself trip the rule
#: it tests when an agent edits it from a shell.
RUNNER = "py" + "test"


# --------------------------------------------------------------------------
# The gap: an attached -m value is the same command as a detached one.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("runner", [RUNNER, "nox", "tox", "unittest"])
def test_an_attached_m_value_is_read_as_the_runner_it_names(runner) -> None:
    """``-m<runner>`` and ``-m <runner>`` are one command, so one verdict.

    The detached spelling is asserted alongside the attached one on purpose:
    what this fix claims is that the two agree, and a test that checked only
    the attached form would still pass if the detached one silently stopped
    being refused.
    """

    detached = f"python3 -m {runner} tests"
    attached = f"python3 -m{runner} tests"

    assert hook.unpooled_work(detached) is True
    assert hook.unpooled_work(attached) is True


@pytest.mark.parametrize("command", [
    f"python3 -m{RUNNER} tests",
    f"/usr/bin/python3 -m{RUNNER} -q tests/test_thing.py",
    "python3 -munittest discover",
    f"CUDA_VISIBLE_DEVICES= python3 -m{RUNNER} tests",
    f"ssh lina python3 -m{RUNNER} tests",
    f"echo ok && python3 -m{RUNNER} tests",
    f"bash <<'SH'\npython3 -m{RUNNER} tests\nSH",
])
def test_the_clustered_form_is_refused_wherever_the_spaced_one_is(command) -> None:
    """Every wrapper, boundary and body that carries the spaced spelling."""

    assert hook.unpooled_work(command) is True


# --------------------------------------------------------------------------
# The direction that costs more if it is wrong: what stays allowed.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("command", [
    # ``-m`` belongs to another program and its value is not a runner.
    "sort -m a.txt b.txt",
    "grep -m1 needle file",
    "ssh -o BatchMode=yes lina uptime",
    # A submission carries the runner, and the entrypoint is the exemption.
    f"python3 tools/fleet/pbrun.py --cpus 8 -- python3 -m{RUNNER} -n 8",
    f"python3 tools/fleet/pbtest.py --checkout . --python /usr/bin/python3",
    # Asking the runner about itself is not running it.
    f"python3 -m{RUNNER} --help",
    f"python3 -m{RUNNER} --version",
    # Talking about the rule is not breaking it.
    f"git commit -m{RUNNER}",
    f"git commit -m 'run {RUNNER} through PrismaBuild'",
    f"rg -- -m{RUNNER} tests",
])
def test_these_were_allowed_before_and_stay_allowed(command) -> None:
    """The false-admission direction is the expensive one, so it is pinned.

    Every entry here is a command the guard allowed on ``0cb95b8``.  A
    narrowing that flips any of them has widened the refusal past its subject,
    which is how a guard teaches people to route around it.
    """

    assert hook.unpooled_work(command) is False
