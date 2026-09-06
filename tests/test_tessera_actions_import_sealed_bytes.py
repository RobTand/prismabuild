"""A sealed Tessera action imports the tree it sealed, or it is refused.

Sealing a checkout makes an action portable; it does not by itself make the
action relocatable.  Both Tessera producers named the shared encoder as an
absolute ``PYTHONPATH`` into the submitter's checkout, and the SLURM lane's
re-seal carried that string through untouched.  The worker then did exactly
what it promised and nothing that helped: it materialized the snapshot,
verified the sealed encoder bytes in its private checkout, and imported the
shared copy -- whatever the shared copy held by the time the scheduler started
the job -- then published those bytes to the CAS under the sealed key.

Two halves, and both are needed.  The producers address the encoder relative
to the tree the action runs in, so the import lands on sealed bytes under
either transport.  And ``fleet_submit`` runs ``pbrun``'s own relocation guard
over argv and environment while it seals, so an action that escapes the
snapshot is refused before anything is queued rather than trusted because it
was sealed.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import materialize  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

import dispatch_tessera_ladder as ladder  # noqa: E402
import dispatch_tessera_shards as shards  # noqa: E402
import fleet_submit  # noqa: E402

from test_slurm_lane import _submissions, fleet  # noqa: E402

__all__ = ["fleet"]

#: The stand-in for the external Tessera installation: a CPU-only wrapper that
#: imports the staged encoder and writes what it read to the declared result.
#: Nothing here needs a GPU, a model or a plan, and the import is the whole
#: subject -- which copy of the encoder the executing process loaded.
_WRAPPER = (
    "import pathlib, sys\n"
    "import audit_encoder\n"
    "out = pathlib.Path(sys.argv[sys.argv.index('--result') + 1])\n"
    "out.parent.mkdir(parents=True, exist_ok=True)\n"
    "out.write_text(audit_encoder.VALUE)\n"
)


def _shared_checkout(root: Path, producer) -> Path:
    """The shared checkout both boxes execute, with a staged encoder in it."""

    root.mkdir()
    for argv in (
        ["init", "-q"],
        ["config", "user.name", "PrismaBuild test"],
        ["config", "user.email", "t@example.invalid"],
    ):
        subprocess.run(["git", "-C", str(root), *argv], check=True)
    encoder = root / "tessera" / "src" / "audit_encoder.py"
    encoder.parent.mkdir(parents=True)
    encoder.write_text('VALUE = "SEALED_ORIGINAL"\n', encoding="utf-8")
    (root / producer.WRAPPER).write_text(_WRAPPER, encoding="utf-8")
    subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "commit", "-qm", "staged encoder"], check=True)
    return root


def _producer(producer, checkout: Path, tmp_path: Path,
              monkeypatch: pytest.MonkeyPatch) -> None:
    """Point one dispatcher's constants at this test's own tree."""

    monkeypatch.setattr(producer, "CHECKOUT", checkout)
    monkeypatch.setattr(producer, "PYTHON", sys.executable)
    monkeypatch.setattr(producer, "SOURCE", str(tmp_path / "unused-model"))
    if producer is shards:
        monkeypatch.setattr(producer, "PLAN", str(tmp_path / "unused-plan"))
        monkeypatch.setattr(producer, "PARTS", str(tmp_path / "unused-parts"))


def _action(producer, checkout: Path) -> dict:
    closure = pb.build_code_closure(checkout, producer.closure_files())
    if producer is ladder:
        return producer.build_action(1, closure, 4, 32)
    return producer.build_action(1, closure, "0" * 64)


@pytest.mark.parametrize("producer", [ladder, shards], ids=["ladder", "shards"])
def test_a_sealed_shard_executes_the_encoder_it_sealed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, producer,
) -> None:
    """The shared encoder changes after submit; the run still reads the seal."""

    checkout = _shared_checkout(tmp_path / "shared-checkout", producer)
    _producer(producer, checkout, tmp_path, monkeypatch)
    action = _action(producer, checkout)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    fleet_submit._SNAPSHOT_CACHE.clear()

    action, snapshot = fleet_submit.seal_checkout_into_action(
        action, cas=cas, checkout_root=checkout)

    # What the scheduler's delay looks like: the submitter's tree moves on
    # while the job waits for a node.
    encoder = checkout / "tessera" / "src" / "audit_encoder.py"
    encoder.write_text('VALUE = "UNSEALED_CHANGED_AFTER_SUBMIT"\n',
                       encoding="utf-8")

    item = {
        "action_key": action["action_key"],
        "cas_root": str(cas.root),
        "checkout_snapshot": snapshot,
    }
    with materialize._execution_checkout(
        item, local_checkout_root=tmp_path / "local"
    ) as private:
        pb.verify_code_closure(action["code_closure"], private)
        result = pb.run_local_action(
            action, cas_root=cas.root, checkout_root=private)

    assert result["status"] == "published"
    assert Path(result["payload_path"]).read_text() == "SEALED_ORIGINAL"
    assert cas.lookup(action) is not None


@pytest.mark.parametrize("producer", [ladder, shards], ids=["ladder", "shards"])
def test_neither_producer_addresses_its_checkout_absolutely(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, producer,
) -> None:
    """The property, read off the action rather than off the source."""

    checkout = _shared_checkout(tmp_path / "shared-checkout", producer)
    _producer(producer, checkout, tmp_path, monkeypatch)
    action = _action(producer, checkout)

    escaping = [
        f"{name}={value}"
        for name, value in action["environment"]["variables"].items()
        if str(checkout) in str(value)
    ]
    escaping += [
        token for token in action["task"]["argv"] if str(checkout) in str(token)
    ]
    assert escaping == []
    assert action["environment"]["variables"]["PYTHONPATH"] == "tessera/src"


def test_an_action_that_escapes_its_snapshot_is_refused_before_submission(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``fleet_submit`` seals and validates, and the validation is pbrun's.

    ``pbrun`` already refuses an interactive submission whose argv or
    environment names the submitter's repository.  The producer conversion
    never asked it, so the one shape it exists to catch went to the scheduler
    sealed and unrelocatable.
    """

    checkout = _shared_checkout(tmp_path / "shared-checkout", shards)
    _producer(shards, checkout, tmp_path, monkeypatch)
    monkeypatch.setattr(shards, "CHECKOUT", checkout)
    action = _action(shards, checkout)
    # The exact string both producers shipped: the shared encoder, absolute.
    body = {name: value for name, value in action.items() if name != "action_key"}
    body["environment"] = {
        "variables": {
            **action["environment"]["variables"],
            "PYTHONPATH": str(checkout / "tessera" / "src"),
        },
        "toolchain": action["environment"]["toolchain"],
    }
    escaping = pb.seal_action(body)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    request = cas.publish_action_request(escaping)
    fleet_submit._SNAPSHOT_CACHE.clear()

    with pytest.raises(fleet_submit.SubmitRefused) as refusal:
        fleet_submit.submit(
            escaping, cas=cas, request_path=request, transport="slurm",
            checkout_root=checkout, tags=["gb10"],
            resources={"gpu": 1, "mem_gb": 16},
            queue_root=tmp_path / "pb-queue",
        )

    assert "PYTHONPATH" in str(refusal.value)
    assert str(checkout) in str(refusal.value)
    assert _submissions(fleet) == []
    assert sl.resolve_recorded(str(escaping["action_key"])[:12]) == []


@pytest.mark.parametrize('producer', [ladder, shards], ids=['ladder', 'shards'])
def test_dispatchers_seal_and_submit_the_same_bounded_cpu_demand(tmp_path, monkeypatch, producer):
    import types
    checkout = _shared_checkout(tmp_path / 'shared-checkout', producer)
    _producer(producer, checkout, tmp_path, monkeypatch)
    monkeypatch.setattr(producer, 'SH', tmp_path / 'fleet')
    if producer is shards:
        Path(producer.PLAN).write_text('{}')
    captured = []
    def submit(action, **kwargs):
        captured.append((action, kwargs))
        return types.SimpleNamespace(action_key=action['action_key'], describe=lambda: 'queued')
    monkeypatch.setattr(producer.fleet_submit, 'submit', submit)
    monkeypatch.setattr(sys, 'argv', ['dispatch', '--shards', '1', '--transport', 'pool'])
    producer.main()
    action, submission = captured[0]
    assert action['params']['demand'] == submission['resources']
    assert submission['resources']['cpu'] == 1
    assert submission['resources']['gpu'] == 1
    for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'):
        assert action['environment']['variables'][name] == '1'
