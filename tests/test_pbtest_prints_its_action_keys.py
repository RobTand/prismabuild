"""pbtest names every shard's full action key, when it is queued and when it ends (#1012).

A worker recording red-first evidence had to recover each shard's key from the
queue afterwards: ``pbrun`` printed twelve characters of it (``pbrun: queued
ae8764488cc3``), and the ``--json`` receipt carried no key at all, only the
full one buried as text inside the action's own output.  A prefix is what
``pbrun --withdraw`` accepts, but it is not an identity, so a report that
cites receipts could cite prefixes only.

Two halves:

* ``pbrun``'s submission line names the full key it published (or attached
  to).  Every later line still names it by prefix.
* ``pbtest`` reads that line as the shard runs and prints the key with the
  shard's test files; its ending line repeats the key; and each shard record
  in ``--json`` carries ``action_key`` and ``receipt_path`` beside
  ``returncode``.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

from pbtest_shard_output import ShardProcess, shard_output_for
from test_offer_refusal_retry import _announce_x86, _submit
from test_pbcampaign import fleet_paths  # noqa: F401

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

_SPEC = importlib.util.spec_from_file_location(
    "pbtest", REPOSITORY / "tools" / "fleet" / "pbtest.py")
pbtest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbtest)                      # type: ignore[union-attr]

import pbrun  # noqa: E402
from prismabuild import core as pb, pool  # noqa: E402


# --- pbrun ------------------------------------------------------------------

def test_pbrun_names_the_full_key_it_queued(
    tmp_path, fleet_paths, monkeypatch, capsys,  # noqa: F811
):
    """main: ``pbrun: queued <12 hex>``; the key has to be dug out of ready/.

    branch: the submission line carries the key the ready record is named by.
    """

    work, queue = fleet_paths
    _announce_x86(queue)

    assert _submit(monkeypatch, work, "dl380g10") == 75   # queued; nothing claims it

    (record,) = list(queue.dir(pool.READY).glob("*.json"))
    key = record.stem
    err = capsys.readouterr().err
    assert f"pbrun: queued {key} tags=" in err, err


# --- pbtest -----------------------------------------------------------------

def _key(command) -> str:
    return hashlib.sha256(json.dumps(list(command)).encode()).hexdigest()


class _Queued(ShardProcess):
    """A shard whose pbrun queued it under a full key, and whose tests passed."""

    returncode = 0

    def __init__(self, command) -> None:
        self.key = _key(command)
        self.output = (f"pbrun: queued {self.key} tags=['x86'] "
                       "demand={'cpu': 2, 'mem_gb': 3}\n"
                       + shard_output_for(command)
                       + "pbrun: executed on dl380g10 in 1s\n")

    def communicate(self):
        return self.output, None


class _Refused(ShardProcess):
    """A shard whose pbrun refused before publishing anything."""

    returncode = 2

    def __init__(self, command) -> None:
        self.command = command

    def communicate(self):
        return "pbrun: no recorded worker can run this action\n", None


def _run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, shard_class, *, shards=2):
    checkout = tmp_path / "checkout"
    for index in range(shards):
        test_file = checkout / "tests" / f"test_{index}.py"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("def test_one():\n    assert True\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    launched: list[_Queued] = []

    def _popen(command, **_kwargs):
        shard = shard_class(command)
        launched.append(shard)
        return shard

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    report = tmp_path / "shards.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
        "--tag", "x86", "--shards", str(shards), "--json", str(report), "tests"])
    code = pbtest.main()
    return code, launched, json.loads(report.read_text())


def test_a_two_shard_run_prints_both_keys_at_submission_and_at_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """main: neither key is printed by pbtest, and the receipt has none.

    branch: each key is printed with its shard's files when pbrun queues it,
    again on the shard's ending line, and recorded in the shard's record.
    """

    code, launched, records = _run(tmp_path, monkeypatch, _Queued)
    lines = capsys.readouterr().out.splitlines()

    assert code == 0 and len(launched) == 2
    for index, (shard, record) in enumerate(zip(launched, records)):
        submitted = [position for position, line in enumerate(lines)
                     if line.startswith(f"shard {index:>3} action {shard.key}")]
        ended = [position for position, line in enumerate(lines)
                 if line.startswith(f"shard {index:>3} ok")]
        assert submitted and ended, lines
        assert submitted[0] < ended[0], "the key is printed before the ending"
        assert all(name in lines[submitted[0]] for name in record["files"])
        assert shard.key in lines[ended[0]]
        assert record["action_key"] == shard.key
    assert records[0]["action_key"] != records[1]["action_key"]


def test_a_shard_record_names_its_receipt_only_when_one_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``receipt_path`` is where the CAS keeps the key's receipt; ``null`` when
    there is none there, so a path in the record is a file a reader can open."""

    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    # The receipt of whichever shard is launched first; the other has none.
    receipt: list[Path] = []

    class _FirstReceipted(_Queued):
        def __init__(self, command):
            super().__init__(command)
            if not receipt:
                path = pb.PrismaBuildCAS(pbrun.SH / "cas").receipt_path(self.key)
                path.parent.mkdir(parents=True)
                path.write_text("{}")
                receipt.append(path)

    code, launched, records = _run(tmp_path, monkeypatch, _FirstReceipted)

    assert code == 0
    assert records[0]["receipt_path"] == str(receipt[0])
    assert receipt[0].parts[-2:] == (launched[0].key[:2], f"{launched[0].key}.json")
    assert records[1]["receipt_path"] is None


def test_a_shard_pbrun_refused_says_it_has_no_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """Nothing was published, so there is no key to print, and that is said."""

    code, _launched, records = _run(tmp_path, monkeypatch, _Refused, shards=1)
    out = capsys.readouterr().out

    assert code == 1
    assert records[0]["action_key"] is None
    assert records[0]["receipt_path"] is None
    (ending,) = [line for line in out.splitlines() if line.startswith("shard   0 rc=2")]
    assert "no action key" in ending


class _QueuedThenInner(_Queued):
    """A shard whose own output later quotes another pbrun's queued line.

    ``pbrun`` relays the action's output when it ends, and a failing test that
    drove ``pbrun`` prints its captured stderr there, at the start of a line.
    """

    def __init__(self, command) -> None:
        super().__init__(command)
        self.output += f"pbrun: queued {'e' * 64} tags=['x86'] demand={{}}\n"


def test_a_later_queued_line_in_the_shards_output_does_not_replace_its_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    code, launched, records = _run(tmp_path, monkeypatch, _QueuedThenInner, shards=1)

    assert code == 0
    assert records[0]["action_key"] == launched[0].key
