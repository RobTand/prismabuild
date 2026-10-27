"""Qualify PR fields against the deployed coordinator's real gh 2.46 parser."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

FLEET = Path(__file__).resolve().parents[1] / "tools" / "fleet"
sys.path.insert(0, str(FLEET))
import pbmergeq as mq

pytestmark = pytest.mark.pbtest_capability("mergeq-gh-cli")
SHA = "0123456789abcdef0123456789abcdef01234567"


def test_pr_metadata_uses_fields_supported_by_installed_gh(tmp_path, monkeypatch):
    env = {**os.environ, "GH_TOKEN": "parser-only", "GH_CONFIG_DIR": str(tmp_path)}
    parser = subprocess.run(["/usr/bin/gh", "pr", "view", "--json"], env=env,
                            capture_output=True, text=True, timeout=30)
    diagnostic = parser.stdout + parser.stderr
    supported = {line.strip() for line in diagnostic.splitlines()
                 if line.startswith("  ") and line.strip().isidentifier()}
    assert {"number", "headRefOid"} <= supported
    metadata = {"number": 1, "state": "OPEN", "isDraft": False,
                "author": {"login": "RobTand"}, "headRefOid": SHA,
                "headRefName": "fix/example", "isCrossRepository": False,
                "headRepositoryOwner": {"login": "RobTand"},
                "baseRefName": "main", "title": "Fix", "body": ""}
    linked = [{"number": 101, "url": "https://github.com/RobTand/proj/issues/101"},
              {"number": 102, "url": "https://github.com/RobTand/proj/issues/102"}]
    reads = []

    def github_response(command, **kwargs):
        reads.append(command)
        if command[:3] == ["gh", "pr", "view"]:
            requested = set(command[command.index("--json") + 1].split(","))
            assert requested <= supported, f"unsupported gh PR fields: {requested - supported}"
            return subprocess.CompletedProcess(command, 0, json.dumps(metadata))
        assert command[:3] == ["gh", "api", "graphql"]
        assert "--paginate" in command
        query = next(arg for arg in command if arg.startswith("query="))
        assert "$endCursor" in query and "hasNextPage" in query and "endCursor" in query
        return subprocess.CompletedProcess(command, 0, "\n".join(map(json.dumps, linked)))

    monkeypatch.setattr(mq, "call_tool", github_response)
    cfg = mq.Config(repo="RobTand/proj", context="pb-tests",
                    state_dir=tmp_path / "state", pbtest=FLEET / "pbtest.py",
                    client_python=sys.executable, test_python="/py",
                    pbtest_args=("--priority", "0", "--timeout-s", "60"))
    pr = mq.GitHub(cfg, mq.Store(cfg.state_dir), dry=False).pr(1)
    assert pr == {**metadata, "closingIssuesReferences": linked}
    assert mq.issue_numbers(pr, cfg.repo) == [101, 102]
    assert len(reads) == 2
