"""What the Tessera dispatchers accept for ``--shards``.

Both dispatchers parsed the flag with a bare ``int()`` after ``parse_args``,
which fails two ways. An operator who typed a range wrong got a ``ValueError``
traceback instead of the usage message argparse exists to print. And the
parse accepted values that name no work: ``0`` and ``500`` seal actions for
shards outside the run, and ``9-4`` seals nothing while the dispatcher reports
success.

The domain is one number, or an inclusive LO-HI range, within 1 to
``OF_SHARDS``. ``argparse`` renders an ``ArgumentTypeError`` as a usage error
and exits 2, so the domain is stated once and every wrong value gets it.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

import dispatch_tessera_ladder as ladder  # noqa: E402
import dispatch_tessera_shards as shards  # noqa: E402

DISPATCHERS = (shards, ladder)


@pytest.mark.parametrize("dispatcher", DISPATCHERS, ids=lambda d: d.__name__)
@pytest.mark.parametrize("text", ["abc", "", "1-", "-3", "1,2", "1-2-3", "1 2"])
def test_a_value_that_is_not_a_shard_range_is_refused(dispatcher, text) -> None:
    with pytest.raises(argparse.ArgumentTypeError) as refusal:
        dispatcher.shard_range(text)
    assert "within 1-120" in str(refusal.value)


@pytest.mark.parametrize("dispatcher", DISPATCHERS, ids=lambda d: d.__name__)
@pytest.mark.parametrize("text", ["0", "121", "5-3", "0-4", "119-121"])
def test_a_range_that_names_no_shard_of_this_run_is_refused(
    dispatcher, text,
) -> None:
    """The half a bare ``int()`` accepted: parseable, and still not work."""

    with pytest.raises(argparse.ArgumentTypeError) as refusal:
        dispatcher.shard_range(text)
    assert "within 1-120" in str(refusal.value)


@pytest.mark.parametrize("dispatcher", DISPATCHERS, ids=lambda d: d.__name__)
@pytest.mark.parametrize("text,expected", [
    ("1", range(1, 2)),
    ("61", range(61, 62)),
    ("1-120", range(1, 121)),
    ("7-9", range(7, 10)),
])
def test_the_range_an_operator_means_is_what_the_loop_gets(
    dispatcher, text, expected,
) -> None:
    assert dispatcher.shard_range(text) == expected
    assert dispatcher.shard_range(text).stop - 1 <= dispatcher.OF_SHARDS


@pytest.mark.parametrize("dispatcher", DISPATCHERS, ids=lambda d: d.__name__)
def test_a_bad_shards_value_is_a_usage_error_and_not_a_traceback(
    dispatcher, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture,
) -> None:
    """The whole command line, so the exit status is argparse's own.

    ``sys.argv`` rather than an ``argv`` parameter: neither ``main`` takes one,
    and a test that added one would pass against the unfixed code for the
    wrong reason.
    """

    monkeypatch.setattr(sys, "argv", [dispatcher.__name__, "--shards", "abc"])
    with pytest.raises(SystemExit) as exit_status:
        dispatcher.main()
    assert exit_status.value.code == 2
    assert "within 1-120" in capsys.readouterr().err
