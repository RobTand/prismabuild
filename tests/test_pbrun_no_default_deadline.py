"""An unspecified submission deadline stays absent from the sealed action."""
import json
from test_pbrun_detach import _checkout, _queue, _run_pbrun


def test_the_timeout_flag_defaults_to_no_deadline(tmp_path, monkeypatch, capsys):
    work = _checkout(tmp_path)
    _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    key = json.loads(capsys.readouterr().out)["action_key"]
    action = json.loads((tmp_path / "cas" / "requests" / key[:2] / f"{key}.json").read_text())
    assert "execution_timeout_s" not in action["params"]
