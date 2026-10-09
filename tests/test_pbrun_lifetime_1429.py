"""``pbrun --lifetime-s`` seals the lifetime contract and nothing else (#1429).

The flag is opt-in. Absent, the sealed action is byte-identical to what it
was before the flag existed. Present, it adds ``params.lifetime`` and the
capability tag that only an enforcing box offers.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun  # noqa: E402
from prismabuild import core as pb  # noqa: E402


class _Stop(Exception):
    pass


def _sealed_body(argv, monkeypatch, tmp_path):
    """The action body ``pbrun`` hands to the sealer for ``argv``."""

    captured = []
    for command in (["git", "init", "-q", str(tmp_path)],
                    ["git", "-C", str(tmp_path), "-c", "user.name=PrismaBuild test",
                     "-c", "user.email=test@example.invalid",
                     "commit", "--allow-empty", "-qm", "fixture"]):
        assert subprocess.run(command, check=False).returncode == 0

    def _stop(body, *_args, **_kwargs):
        captured.append(body)
        raise _Stop()

    monkeypatch.setattr(pbrun.pb, "seal_action", _stop)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(pbrun, "git_repository_root", lambda _cwd: tmp_path)
    monkeypatch.setattr(pbrun, "build_git_checkout_snapshot",
                        lambda *_args, **_kwargs: {"input": {"id": "test"}})
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--cwd", str(tmp_path), *argv])
    with pytest.raises(_Stop):
        pbrun.main()
    return captured[0]


def test_the_flag_seals_the_contract_and_an_unfenced_twin_is_unchanged(
        monkeypatch, tmp_path):
    fenced = _sealed_body(["--lifetime-s", "300", "--", "true"], monkeypatch, tmp_path)
    assert fenced["params"]["lifetime"] == {
        "schema": pb.LIFETIME_SCHEMA_V1, "fence_s": 300.0}
    plain = _sealed_body(["--", "true"], monkeypatch, tmp_path)
    assert "lifetime" not in plain["params"]
    # The payload budget is a separate, independent parameter.
    both = _sealed_body(["--lifetime-s", "300", "--timeout-s", "60", "--", "true"],
                        monkeypatch, tmp_path)
    assert both["params"]["execution_timeout_s"] == 60.0
    assert both["params"]["lifetime"]["fence_s"] == 300.0


@pytest.mark.parametrize("seconds", [
    "179", "0", "-5", "nan", "inf", str(pb.LIFETIME_MAX_FENCE_S + 1)])
def test_a_fence_outside_the_bounds_is_refused_before_anything_is_sealed(
        monkeypatch, tmp_path, seconds):
    with pytest.raises(SystemExit, match="--lifetime-s must lie within"):
        _sealed_body(["--lifetime-s", seconds, "--", "true"], monkeypatch, tmp_path)


def test_the_slurm_lane_refuses_the_contract(monkeypatch, tmp_path, capsys):
    with pytest.raises(SystemExit) as refused:
        _sealed_body(["--transport", "slurm", "--lifetime-s", "300", "--", "true"],
                     monkeypatch, tmp_path)
    assert refused.value.code != 0
    assert "--lifetime-s requires pool transport" in capsys.readouterr().err


def test_help_states_the_contract_and_its_limits(monkeypatch, capsys):
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--help"])
    with pytest.raises(SystemExit):
        pbrun.main()
    text = " ".join(capsys.readouterr().out.split())
    assert "--lifetime-s" in text
    assert f"{pb.LIFETIME_MIN_FENCE_S:g} s" in text
    assert f"payload {pb.LIFETIME_RELEASE_RESERVE_S:g} s before that deadline" in text
    assert "The payload budget (--timeout-s) is separate and unchanged" in text
    assert "never on a timer" in text
