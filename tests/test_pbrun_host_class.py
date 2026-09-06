"""``pbrun --measurement --host-class CLASS`` seals a measurement action.

Before this, ``pbrun`` hard-coded ``task_class="generation"`` and a portable
scope, so no measurement action -- a probe, a KL sweep -- could be submitted
from the CLI at all.  The class is a node Feature name; it rides the placement
axis exactly as ``--tag`` does, so the action key moves with it and the SLURM
lane sends it as ``--constraint``.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402
import pbrun  # noqa: E402


class _NoReceipt:
    """A CAS that holds no receipt, so pbrun goes on to the scheduler."""

    def lookup(self, action):
        return None

#: The real sealer, taken before any test replaces it on the shared module.
_seal_action = pb.seal_action


class _Stop(Exception):
    pass


def _sealed_body(argv, monkeypatch, tmp_path):
    """The body ``pbrun`` hands to ``seal_action`` for ``argv``."""

    captured = []
    if not (tmp_path / ".git").exists():
        assert subprocess.run(
            ["git", "init", "-q", str(tmp_path)], check=False
        ).returncode == 0
        assert subprocess.run(
            [
                "git", "-C", str(tmp_path),
                "-c", "user.name=PrismaBuild test",
                "-c", "user.email=test@example.invalid",
                "commit", "--allow-empty", "-qm", "fixture",
            ],
            check=False,
        ).returncode == 0

    def _stop(body, *_a, **_kw):
        captured.append(body)
        raise _Stop()

    monkeypatch.setattr(pbrun.pb, "seal_action", _stop)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(pbrun, "git_repository_root", lambda _cwd: tmp_path)
    monkeypatch.setattr(
        pbrun,
        "build_git_checkout_snapshot",
        lambda *_args, **_kwargs: {"input": {"id": "test"}},
    )
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--cwd", str(tmp_path), *argv])
    with pytest.raises(_Stop):
        pbrun.main()
    return captured[0]


def _sealable(body):
    """The captured body with its placeholder snapshot input removed."""

    return {**body, "inputs": []}


def test_measurement_with_a_host_class_seals_the_scope_and_the_constraint(
    monkeypatch, tmp_path
):
    body = _sealed_body(
        ["--transport", "slurm", "--measurement", "--host-class", "gb10",
         "--", "true"],
        monkeypatch, tmp_path,
    )
    assert body["task"]["task_class"] == "measurement"
    assert body["execution_scope"] == {
        "portability": "host_class_keyed",
        "platform_key": None,
        "host_class": "gb10",
    }
    assert "gb10" in body["params"]["placement"]["required_tags"]
    # Nonportable, so the toolchain binds argv[0] and this box's ABI: what
    # the core requires of a nonportable action, and what a worker of the
    # class verifies before it runs.
    toolchain = body["environment"]["toolchain"]
    assert {"argv0.sha256", "argv0.bytes", "system", "machine", "libc"} <= set(toolchain)
    assert toolchain["argv0.sha256"] == pb.executable_toolchain_contract(
        pbrun.SEALED_ARGV0
    )["argv0.sha256"]
    # And the core accepts what pbrun built.
    sealed = _seal_action(_sealable(body))
    assert sealed["execution_scope"]["host_class"] == "gb10"


def test_the_class_moves_the_action_key(monkeypatch, tmp_path):
    plain = _sealed_body(["--transport", "slurm", "--", "true"], monkeypatch, tmp_path)
    keyed = _sealed_body(
        ["--transport", "slurm", "--measurement", "--host-class", "gb10",
         "--", "true"],
        monkeypatch, tmp_path,
    )
    assert plain["task"]["task_class"] == "generation"
    assert plain["execution_scope"]["portability"] == "portable"
    assert plain["environment"]["toolchain"] == {}
    plain_key = _seal_action(_sealable(plain))["action_key"]
    keyed_key = _seal_action(_sealable(keyed))["action_key"]
    assert plain_key != keyed_key
    # Repeating the keyed command reproduces its key: the class is identity,
    # not a per-submission nonce.
    again = _sealed_body(
        ["--transport", "slurm", "--measurement", "--host-class", "gb10",
         "--", "true"],
        monkeypatch, tmp_path,
    )
    assert _seal_action(_sealable(again))["action_key"] == keyed_key


def test_the_class_is_a_union_with_the_tags_that_landed(monkeypatch, tmp_path):
    body = _sealed_body(
        ["--transport", "slurm", "--host-class", "gb10", "--tag", "sparky",
         "--tag", "gb10", "--", "true"],
        monkeypatch, tmp_path,
    )
    assert body["params"]["placement"]["required_tags"] == ["gb10", "sparky"]
    # A class without --measurement is a generation action keyed on the class.
    assert body["task"]["task_class"] == "generation"
    assert body["execution_scope"]["portability"] == "host_class_keyed"


def test_a_measurement_without_a_class_is_refused_with_the_design_reason(
    monkeypatch, tmp_path
):
    with pytest.raises(SystemExit) as raised:
        _sealed_body(["--transport", "slurm", "--measurement", "--", "true"],
                     monkeypatch, tmp_path)
    assert "do not transfer across architectures" in str(raised.value)


def test_a_class_needs_the_slurm_transport(monkeypatch, tmp_path):
    with pytest.raises(SystemExit) as raised:
        _sealed_body(["--host-class", "gb10", "--", "true"], monkeypatch, tmp_path)
    assert "--transport slurm" in str(raised.value)


class _Captured(Exception):
    def __init__(self, kwargs):
        super().__init__("captured")
        self.kwargs = kwargs


def test_the_slurm_lane_receives_the_class_as_placement(monkeypatch):
    """What ``slurm_lane.run`` gets is what becomes ``--constraint``."""

    def record(action, **kwargs):
        raise _Captured(kwargs)

    monkeypatch.setattr(pbrun.slurm_lane, "run", record)
    with pytest.raises(_Captured) as raised:
        pbrun.slurm_outcome(
            {"action_key": "0" * 64}, cas=_NoReceipt(), request_path="request.json",
            tags=["gb10"], demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
            timeout_s=None, wait_s=60.0, retry_safe=False, max_attempts=1,
        )
    assert raised.value.kwargs["placement"] == ["gb10"]
    # Partition stays the resource axis: no GPU demand and a tag means the
    # default partition, where the constraint picks the node.
    assert raised.value.kwargs["partition"] is None
    assert sl.partition_for(
        sl.LaneResources.from_demand({"gpu": 1, "mem_gb": 16}, exclusive=False),
        ["gb10"],
    ) == sl.GPU_PARTITION


def test_pool_measurement_seals_verified_platform_and_local_placement(monkeypatch, tmp_path):
    import socket
    body = _sealed_body(['--transport', 'pool', '--measurement', '--', 'true'],
                        monkeypatch, tmp_path)
    assert body['task']['task_class'] == 'measurement'
    assert body['execution_scope'] == {
        'portability': 'platform_keyed',
        'platform_key': pb._platform_key_from_evidence(pb._collect_worker_evidence()),
        'host_class': None,
    }
    assert socket.gethostname() in body['params']['placement']['required_tags']
    assert {'argv0.sha256', 'argv0.bytes', 'system', 'machine', 'libc'} <= set(
        body['environment']['toolchain'])
    sealed = _seal_action(_sealable(body))
    assert sealed['execution_scope']['portability'] == 'platform_keyed'


def test_pool_measurement_cannot_claim_anywhere_placement(monkeypatch, tmp_path):
    with pytest.raises(SystemExit, match='pool measurements.*submitting host'):
        _sealed_body(['--transport', 'pool', '--measurement', '--anywhere', '--', 'true'],
                     monkeypatch, tmp_path)


def test_pool_measurement_scope_passes_local_preflight_and_rejects_different_platform(tmp_path, monkeypatch):
    from test_core import _body
    scope, toolchain = pbrun.host_class_scope(None, measurement=True, transport='pool')
    body = _body(tmp_path, task_class='measurement',
                 argv=[pbrun.SEALED_ARGV0, '--noprofile', '--norc', '-c', 'true'])
    body['inputs'] = []
    body['execution_scope'] = scope
    body['environment']['toolchain'] = toolchain
    action = _seal_action(body)
    attestation = pb.preflight_action(action, cas_root=tmp_path / 'cas', checkout_root=tmp_path)
    assert attestation['platform_key'] == scope['platform_key']
    assert attestation['host_class'] is None
    assert attestation['evidence']['source'] == 'local'
    evidence = pb._collect_worker_evidence()
    monkeypatch.setattr(pb, '_collect_worker_evidence', lambda **kwargs: dict(
        evidence, machine='incompatible-architecture'))
    with pytest.raises(pb.ActionContractError):
        pb.preflight_action(action, cas_root=tmp_path / 'cas', checkout_root=tmp_path)
