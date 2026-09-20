"""Admitted reader launch identity reaches the payload (PB #749).

The broker-owned attempt tuple (nonce/scope) plus the immutable helper
generation root must travel resource_exec -> run-local launcher ->
payload through the existing residency environment contract. Sealed
conflicts still refuse; partial, stale, or mismatched identity fails
closed; legacy absence (none present) preserves established behavior.
Run via published pbtest at -10.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb  # noqa: E402

KEY = "c" * 64
NONCE = "b" * 32
SCOPE = ("prismabuild-job"
         + hashlib.sha256((KEY + NONCE).encode()).hexdigest()[:32]
         + ".slice")
#: Exact cross-repo contract the container adapter consumes
#: (tools/tessera_campaign_container.READER_CONTEXT_ENV).
ADAPTER_NAMES = ("PRISMABUILD_ACTION_KEY", "PRISMABUILD_ACTION_NONCE",
                 "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT")


def _action():
    return {"action_key": KEY}


def _executing_root() -> str:
    """This tree's generation root, the only helper core will forward."""

    return str(Path(pb.__file__).resolve().parents[2])


def _launch(monkeypatch, tmp_path, *, nonce=NONCE, scope=SCOPE,
            helper="__executing__", with_map=True):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT",
                 "PRISMABUILD_RESIDENCY_MAP"):
        monkeypatch.delenv(name, raising=False)
    if with_map:
        monkeypatch.setenv("PRISMABUILD_RESIDENCY_MAP",
                           str(tmp_path / "consumer.map.json"))
    if nonce is not None:
        monkeypatch.setenv("PRISMABUILD_ACTION_NONCE", nonce)
    if scope is not None:
        monkeypatch.setenv("PRISMABUILD_ACTION_SCOPE", scope)
    if helper == "__executing__":
        helper = _executing_root()
    monkeypatch.setenv("PRISMABUILD_READER_HELPER_ROOT", str(helper))
    return str(helper)


def test_complete_tuple_forwarded_to_payload_env(tmp_path, monkeypatch) -> None:
    """The exact attempt tuple plus helper reach the payload env."""

    helper = _launch(monkeypatch, tmp_path)
    env = pb._residency_environment(_action(), {})
    assert env["PRISMABUILD_ACTION_KEY"] == KEY
    assert env["PRISMABUILD_ACTION_NONCE"] == NONCE
    assert env["PRISMABUILD_ACTION_SCOPE"] == SCOPE
    assert env["PRISMABUILD_READER_HELPER_ROOT"] == helper
    assert env["PRISMABUILD_RESIDENCY_MAP"].endswith("consumer.map.json")


def test_actual_child_sees_only_the_tuple(tmp_path, monkeypatch) -> None:
    """A real child process sees the tuple and no capability material."""

    helper = _launch(monkeypatch, tmp_path)
    monkeypatch.setenv("PRISMABUILD_BROKER_TOKEN_DO_NOT_FORWARD", "t" * 64)
    monkeypatch.setenv("PRISMABUILD_BROKER_SOCKET_PATH", "/run/x.sock")
    env = pb._residency_environment(_action(), {})
    probe = subprocess.run(
        [sys.executable, "-c",
         "import json, os; print(json.dumps(dict(os.environ)))"],
        capture_output=True, text=True, env=dict(env),
        timeout=60)
    assert probe.returncode == 0
    seen = json.loads(probe.stdout)
    assert seen["PRISMABUILD_ACTION_NONCE"] == NONCE
    assert seen["PRISMABUILD_ACTION_SCOPE"] == SCOPE
    assert seen["PRISMABUILD_READER_HELPER_ROOT"] == helper
    assert seen["PRISMABUILD_ACTION_KEY"] == KEY
    assert "PRISMABUILD_BROKER_TOKEN_DO_NOT_FORWARD" not in seen
    assert "PRISMABUILD_BROKER_SOCKET_PATH" not in seen
    assert not any(name.startswith("PRISMABUILD_BROKER_") for name in seen)


@pytest.mark.parametrize("drop", ["PRISMABUILD_ACTION_NONCE",
                                  "PRISMABUILD_ACTION_SCOPE",
                                  "PRISMABUILD_READER_HELPER_ROOT"])
def test_partial_identity_refuses_launch(tmp_path, monkeypatch, drop) -> None:
    """Any strict signal without the full tuple fails closed, never partial."""

    _launch(monkeypatch, tmp_path)
    monkeypatch.delenv(drop)
    with pytest.raises(pb.ActionContractError):
        pb._residency_environment(_action(), {})


def test_wrong_scope_refuses_launch(tmp_path, monkeypatch) -> None:
    """A scope bound to another action is not this attempt's identity."""

    other = ("prismabuild-job"
             + hashlib.sha256(("d" * 64 + NONCE).encode()).hexdigest()[:32]
             + ".slice")
    _launch(monkeypatch, tmp_path, scope=other)
    with pytest.raises(pb.ActionContractError):
        pb._residency_environment(_action(), {})


def test_wrong_nonce_refuses_launch(tmp_path, monkeypatch) -> None:
    """A tampered nonce breaks the scope binding and refuses."""

    _launch(monkeypatch, tmp_path, nonce="a" * 32)
    with pytest.raises(pb.ActionContractError):
        pb._residency_environment(_action(), {})


def test_stale_helper_refuses_launch(tmp_path, monkeypatch) -> None:
    """A helper root naming nothing on disk is not forwarded."""

    _launch(monkeypatch, tmp_path, helper=tmp_path / "no-such-generation")
    with pytest.raises(pb.ActionContractError):
        pb._residency_environment(_action(), {})


def test_unrelated_existing_helper_refuses_launch(tmp_path, monkeypatch) -> None:
    """An older-but-existing (or merely unrelated) tree is not the helper."""

    _launch(monkeypatch, tmp_path, helper=tmp_path)
    assert Path(tmp_path).is_dir()
    with pytest.raises(pb.ActionContractError):
        pb._residency_environment(_action(), {})


def test_run_local_action_forwards_production_identity(
        tmp_path, monkeypatch) -> None:
    """The production launch carries the proxy-seeded tuple to the child.

    Seeds through the existing resource_exec.payload_identity_env (never
    a hand-built bundle) and traverses run_local_action end to end, so
    removing the integration call fails this test.
    """

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import resource_exec  # noqa: E402
    from test_core import _action as _seal  # noqa: E402
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    code = ("import json, os; open('result.bin','w').write(json.dumps("
            "{k: os.environ.get(k) for k in "
            "('PRISMABUILD_ACTION_KEY', 'PRISMABUILD_ACTION_NONCE', "
            "'PRISMABUILD_ACTION_SCOPE', "
            "'PRISMABUILD_READER_HELPER_ROOT')}))")
    action = _seal(checkout, argv=[sys.executable, "-c", code])
    # Seed for the action in hand (its sealed key), never a fixed key.
    seeded = resource_exec.payload_identity_env(
        {}, action_key=str(action["action_key"]), nonce=NONCE)
    assert set(seeded) == {"PRISMABUILD_ACTION_NONCE",
                           "PRISMABUILD_ACTION_SCOPE",
                           "PRISMABUILD_READER_HELPER_ROOT"}
    for name, value in seeded.items():
        monkeypatch.setenv(name, value)
    result = pb.run_local_action(
        action, cas_root=tmp_path / "cas", checkout_root=checkout)
    assert result["status"] == "published"
    seen = json.loads(Path(result["payload_path"]).read_text())
    assert seen == {"PRISMABUILD_ACTION_KEY": str(action["action_key"]),
                    "PRISMABUILD_ACTION_NONCE": NONCE,
                    "PRISMABUILD_ACTION_SCOPE": seeded[
                        "PRISMABUILD_ACTION_SCOPE"],
                    "PRISMABUILD_READER_HELPER_ROOT": seeded[
                        "PRISMABUILD_READER_HELPER_ROOT"]}


def test_legacy_absence_preserves_behavior(tmp_path, monkeypatch) -> None:
    """No strict signal: key (+map) only, exactly as before."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT",
                 "PRISMABUILD_RESIDENCY_MAP"):
        monkeypatch.delenv(name, raising=False)
    env = pb._residency_environment(_action(), {})
    assert env == {"PRISMABUILD_ACTION_KEY": KEY}


def test_sealed_identity_still_refuses(tmp_path, monkeypatch) -> None:
    """A sealed nonce is a refusal, never an overwrite."""

    _launch(monkeypatch, tmp_path)
    with pytest.raises(pb.ActionContractError):
        pb._residency_environment(
            _action(), {"PRISMABUILD_ACTION_NONCE": NONCE})


def test_container_adapter_contract_names(tmp_path, monkeypatch) -> None:
    """Forwarded names are exactly what the container adapter consumes."""

    _launch(monkeypatch, tmp_path)
    env = pb._residency_environment(_action(), {})
    for name in ADAPTER_NAMES:
        assert name in env, name
