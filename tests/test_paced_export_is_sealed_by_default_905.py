"""New spool declarations seal pacing; retained requests keep their old contract."""
from __future__ import annotations

import pytest

from prismabuild import produced_spool, submission
from test_spool_declaration_is_charged_by_default_905 import _build, _fixture, MAX

PACED = produced_spool.PACED_EXPORT_ENV


@pytest.mark.parametrize("switch", [None, "", "1"])
def test_declared_spool_seals_the_explicit_paced_action(tmp_path, monkeypatch, switch):
    work, fleet = _fixture(tmp_path)
    extra = [] if switch is None else ["--env", f"{PACED}={switch}"]
    actual = submission.seal(_build(
        work, fleet, monkeypatch, "--env", f"{MAX}=1024", *extra))
    explicit = submission.seal(_build(
        work, fleet, monkeypatch, "--env", f"{MAX}=1024", "--env", f"{PACED}=1"))

    assert actual["environment"]["variables"][PACED] == "1"
    assert actual["params"]["demand"]["spool_gb"] == 1
    assert actual["action_key"] == explicit["action_key"]


@pytest.mark.parametrize("switch", ["0", "yes", " 1"])
def test_declared_spool_refuses_unpaced_or_malformed_switch(tmp_path, monkeypatch, switch):
    work, fleet = _fixture(tmp_path)
    with pytest.raises(submission.SubmissionRefused, match=PACED):
        _build(work, fleet, monkeypatch,
               "--env", f"{MAX}=1024", "--env", f"{PACED}={switch}")


@pytest.mark.parametrize("switch", [None, "0", "1"])
def test_no_spool_declaration_preserves_pacing_environment(tmp_path, monkeypatch, switch):
    work, fleet = _fixture(tmp_path)
    extra = [] if switch is None else ["--env", f"{PACED}={switch}"]
    action = submission.seal(_build(work, fleet, monkeypatch, *extra))

    assert action["environment"]["variables"].get(PACED) == switch
    assert "spool_gb" not in action["params"]["demand"]
