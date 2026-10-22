"""Exercise binding propagation through the actual bootstrap gate in PB."""
import json
from pathlib import Path

import pytest

from test_bootstrap_qualification import qualification, owner, core, pbtest_outcomes
from prismabuild.compatibility_window import WindowError


@pytest.mark.parametrize("subset", [False, True], ids=["foreign-completion", "forged-full-population"])
def test_bootstrap_keeps_sealed_binding_through_final_judgment(qualification, tmp_path, subset):
    assessment, index, requests, logs, seal = qualification
    nonce = "c" * 64
    for key, action in list(requests.items()):
        action["environment"]["variables"]["PRISMABUILD_PBTEST_RECORD_BINDING"] = "1"
        requests[key] = core.seal_action({k:v for k,v in action.items() if k != "action_key"})
        lines = logs[key].splitlines()
        record = json.loads(lines[0][len(pbtest_outcomes.PREFIX):])
        full = dict(record)
        if key == "corpus" and subset:
            record = dict(record, collected=record["collected"][::2], reports=record["reports"][::2])
            lines[1] = str(len(record["collected"])) + " passed in 0.01s"
        nodes = record["collected"]
        completion = dict(schema=pbtest_outcomes.COMPLETION_SCHEMA,
            collection_complete=True, collect_only=False, collection_errors=False,
            duplicate_teardown=False, collected=nodes, teardown_finished=nodes,
            outcome_nodeids=nodes, run_nonce=nonce)
        lines[0] = pbtest_outcomes.PREFIX + json.dumps(dict(record, run_nonce=nonce))
        logs[key] = (pbtest_outcomes.preflight_record(0, None, run_nonce=nonce) + "\n"
                     + pbtest_outcomes.COMPLETION_PREFIX + json.dumps(completion) + "\n"
                     + "\n".join(lines) + "\n")
        if key == "corpus":
            if subset:
                full_nodes = full["collected"]
                forged_completion = dict(completion, collected=full_nodes,
                    teardown_finished=full_nodes, outcome_nodeids=full_nodes)
                forged_completion.pop("run_nonce")
                logs[key] += (pbtest_outcomes.PREFIX + json.dumps(full) + "\n"
                              + pbtest_outcomes.COMPLETION_PREFIX + json.dumps(forged_completion) + "\n"
                              + str(len(full_nodes)) + " passed in 0.01s\n")
            else:
                logs[key] += pbtest_outcomes.COMPLETION_PREFIX + "{}\n"
    if subset:
        with pytest.raises(WindowError, match="qualification is incomplete"):
            owner.verify_qualification(assessment, checkout=Path(__file__).resolve().parents[1], fleet_root=tmp_path)
    else:
        result = owner.verify_qualification(assessment, checkout=Path(__file__).resolve().parents[1], fleet_root=tmp_path)
        assert list(result.action_keys) == ["arm", "x86", "corpus"]
