"""A child of a decomposition is a ``pbrun`` action with two more inputs.

That is the whole claim #517 rests on.  If a child were a new kind of action
-- its own sealer, its own hashing rules, its own result contract -- then the
fleet would have two definitions of what an action key means, and the older
one owns every receipt in the CAS.  So the decomposer seals its children
through ``pbrun``'s own two stages: the template is frozen once off one source
tree, and each child differs from an ordinary action only in its resolved
command, its two extra inputs, its sealed membership and its declared result.

What is tested here is that those four are the *only* difference, that they
reach the key, and that the parts a child must not vary are shared byte for
byte.  The batch envelopes and the roster are ingested into a real CAS,
because the path a child is handed has to be one a worker can open, and a
submitter that named it some other way would seal a command that resolves to
nothing.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import decomposition as dc  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

EVIDENCE = "cas:sha256:" + "0" * 64
COMMIT_DATE = "2026-09-11T00:00:00+00:00"


def _checkout(root: Path) -> Path:
    work = root / "work"
    work.mkdir(parents=True)
    (work / "collect.py").write_text("print('ok')\n", encoding="utf-8")
    environment = {
        "GIT_AUTHOR_DATE": COMMIT_DATE,
        "GIT_COMMITTER_DATE": COMMIT_DATE,
        "GIT_AUTHOR_NAME": "PrismaBuild test",
        "GIT_COMMITTER_NAME": "PrismaBuild test",
        "GIT_AUTHOR_EMAIL": "test@example.invalid",
        "GIT_COMMITTER_EMAIL": "test@example.invalid",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(root),
    }
    for args in (
        ("init", "-q", "-b", "main"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "PrismaBuild test"),
        ("add", "collect.py"),
        ("commit", "-qm", "sealed tree"),
    ):
        done = subprocess.run(
            ["git", "-C", str(work), *args], capture_output=True, text=True,
            env=environment,
        )
        assert done.returncode == 0, done.stderr
    return work


def _request(count: int = 66) -> dict[str, Any]:
    return {
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": ["python", "collect.py", "--batch",
                     dc.TASK_BATCH_PLACEHOLDER],
            "cwd": "/checkout",
            "demand": {"cpu": 1, "mem_gb": 4},
            "gpu_memory_gb": None,
            "data_manifest": None,
            "env": {},
        },
        "roster": {
            "schema": dc.LOGICAL_TASK_ROSTER_SCHEMA_V1,
            "tasks": [
                {
                    "id": f"t{index:04d}",
                    "payload": {"rate": 832 + index},
                    "residency_key": "r",
                    "estimated_seconds": 8.2,
                    "estimate_evidence": EVIDENCE,
                    "output_id": f"t{index:04d}",
                }
                for index in range(count)
            ],
        },
        "batch_policy": {
            "schema": dc.ROSTER_BATCH_POLICY_SCHEMA_V1,
            "residencies": [
                {"key": "r", "setup_seconds": 45.0, "setup_evidence": EVIDENCE}
            ],
            "max_setup_fraction": 0.20,
            "max_estimated_wall_seconds": 300.0,
        },
    }


def _ingest(cas, root: Path, name: str, value: object, *, input_id: str):
    path = root / "blobs-in" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    dc.write_document(path, value)
    row, _ = cas.ingest_input(path, input_id=input_id)
    return row


class Decomposed:
    """One frozen template, one plan, and the children sealed from them."""

    def __init__(self, root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        work = _checkout(root)
        monkeypatch.setattr(pbrun, "SH", root)
        self.request = dc.validate_logical_request(_request())
        self.template = pbrun.freeze_action_template(
            command=self.request["common"]["argv"],
            cwd=work,
            logical_cwd=".",
            demand=dict(self.request["common"]["demand"]),
            placement={"required_tags": ["sparky"]},
            variables={"PATH": "/usr/local/bin:/usr/bin:/bin"},
            determinism="stochastic",
            retry_policy={"max_attempts": 1, "retry_safe": False},
            host_class=None,
            measurement=False,
            transport="pool",
            pool_measurement_class=False,
            data_manifest_path=None,
            checkout_snapshot_max_bytes=pbrun.CHECKOUT_SNAPSHOT_MAX_BYTES,
            snapshot_refs=[],
            exclusive=False,
            gpu_memory_gb=None,
            execution_timeout_s=None,
            progress=None,
            profile=None,
        )
        self.cas = self.template["cas"]
        self.frozen = dc.freeze_common(
            self.request["common"],
            logical_cwd=str(self.template["params"]["cwd"]),
            checkout_snapshot_sha256=str(
                self.template["params"]["checkout_snapshot"]["input"]["sha256"]
            ),
        )
        self.plan = dc.build_plan(self.request, self.frozen)
        self.roster_input = _ingest(
            self.cas, root, "roster.json", self.request["roster"],
            input_id=dc.TASK_ROSTER_INPUT_ID,
        )

    def child(self, ordinal: int) -> dict[str, Any]:
        envelope = dc.batch_envelope(
            self.request, self.plan, child_ordinal=ordinal
        )
        batch_input = _ingest(
            self.cas, Path(str(self.cas.root)).parent, f"batch-{ordinal}.json",
            envelope, input_id=dc.TASK_BATCH_INPUT_ID,
        )
        command = dc.resolve_task_batch(
            self.template["params"]["command"],
            batch_path=self.cas.blob_path(str(batch_input["sha256"])),
        )
        return pbrun.seal_action_from_template(
            self.template,
            command=command,
            result_path=dc.child_result_manifest_path(ordinal),
            extra_inputs=[self.roster_input, batch_input],
            extra_params={dc.LOGICAL_BATCH_PARAM: dc.logical_batch_param(
                self.request, self.plan, child_ordinal=ordinal
            )},
        )


@pytest.fixture
def decomposed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Decomposed:
    plan = Decomposed(tmp_path, monkeypatch)
    assert len(plan.plan["partitions"]) >= 3, "fixture needs several children"
    return plan


def test_a_childs_key_is_the_ordinary_hash_of_its_own_body(decomposed) -> None:
    """No second sealer: the child key is ``seal_action`` over the child body."""

    child = decomposed.child(0)
    key = child.pop("action_key")
    assert pbrun.pb.seal_action(child)["action_key"] == key


def test_children_share_everything_the_template_froze(decomposed) -> None:
    """One source tree, one closure, one environment -- across the campaign."""

    first, second = decomposed.child(0), decomposed.child(1)
    for field in ("code_closure", "execution_scope"):
        assert first[field] == second[field], field
    assert (first["environment"]["toolchain"]
            == second["environment"]["toolchain"])
    for field in ("cwd", "demand", "placement", "retry_policy",
                  "checkout_snapshot"):
        assert first["params"][field] == second["params"][field], field
    assert first["inputs"][0] == second["inputs"][0]
    assert (first["inputs"][0]["id"]
            == pbrun.pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID), (
        "the worker's materialization reads the snapshot at inputs[0]"
    )


def test_a_child_differs_from_its_siblings_in_exactly_four_places(
    decomposed,
) -> None:
    first, second = decomposed.child(0), decomposed.child(1)
    differing = {
        field for field in set(first) | set(second)
        if first.get(field) != second.get(field)
    }
    assert differing == {"action_key", "task", "inputs", "params",
                         "environment"}
    assert first["task"]["argv"] != second["task"]["argv"]
    assert first["task"]["result_path"] != second["task"]["result_path"]
    assert first["inputs"][:2] == second["inputs"][:2], (
        "only the batch input is this child's own"
    )
    assert first["inputs"][2] != second["inputs"][2]
    assert (first["params"][dc.LOGICAL_BATCH_PARAM]
            != second["params"][dc.LOGICAL_BATCH_PARAM])
    ours, theirs = (dict(first["environment"]["variables"]),
                    dict(second["environment"]["variables"]))
    assert {name for name in set(ours) | set(theirs)
            if ours.get(name) != theirs.get(name)} == {
        pbrun.CONTAINER_OWNER_ENV, pbrun.CONTAINER_MARKER_ENV}, (
        "the environment varies in ownership and in nothing else"
    )


def test_each_child_owns_its_own_container_lifecycle(decomposed) -> None:
    """Siblings can land on one box; one label would make cleanup kill both.

    Withdrawal and finish reap by the Docker ownership label and then wait on
    the ``<owner>.used`` marker, so two concurrent children under one owner
    would have the first to finish remove the other's live payload and the
    other's marker block its reclaim.  The owner is therefore a property of
    the action, re-derived from the child's own command, and each child's
    marker path follows the owner it actually carries.
    """

    first, second = decomposed.child(0), decomposed.child(1)
    owners = [child["environment"]["variables"][pbrun.CONTAINER_OWNER_ENV]
              for child in (first, second)]
    assert owners[0] != owners[1]
    for child, owner in zip((first, second), owners):
        variables = dict(child["environment"]["variables"])
        assert variables.pop(pbrun.CONTAINER_MARKER_ENV).endswith(
            f"{owner}.used")
        assert variables.pop(pbrun.CONTAINER_OWNER_ENV) == owner
        assert owner == pbrun.container_owner(
            child["params"]["command"],
            child["params"]["cwd"],
            child["params"]["demand"],
            variables,
            determinism=child["task"]["determinism"],
            retry_policy=child["params"]["retry_policy"],
            marker_root=decomposed.template["marker_root"],
            identity=decomposed.template["checkout_identity"],
            logical_cwd=child["params"]["cwd"],
            placement=child["params"]["placement"],
        ), "a child's owner is the ordinary digest over its own body"


def test_the_same_child_seals_the_same_key_twice(decomposed) -> None:
    """Recovery re-seals a missing child rather than re-planning the campaign."""

    assert decomposed.child(2) == decomposed.child(2)


def test_the_batch_reaches_the_child_as_a_path_a_worker_can_open(
    decomposed,
) -> None:
    """A CAS blob path, not a shell expansion and not a submitter-local name."""

    child = decomposed.child(1)
    command = child["params"]["command"]
    assert dc.TASK_BATCH_PLACEHOLDER not in command
    batch_path = Path(command[-1])
    assert batch_path.is_absolute()
    envelope = json.loads(batch_path.read_text(encoding="utf-8"))
    assert envelope["schema"] == dc.BATCH_ENVELOPE_SCHEMA_V1
    assert envelope["child_ordinal"] == 1
    assert [task["id"] for task in envelope["tasks"]] == (
        decomposed.plan["partitions"][1]
    )
    assert child["inputs"][2]["sha256"] == dc.document_sha256(envelope), (
        "the child's sealed input must be the digest of the bytes it reads"
    )
    assert command[:-1] == decomposed.template["params"]["command"][:-1], (
        "the producer's own arguments are untouched"
    )


def test_the_declared_result_is_the_manifest_and_the_log_is_still_teed(
    decomposed,
) -> None:
    """An exit status is not an answer; the merge reads what was measured."""

    child = decomposed.child(1)
    assert child["task"]["result_path"] == dc.child_result_manifest_path(1)
    envelope_path = Path(child["params"]["command"][-1])
    envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
    assert envelope["result_manifest_path"] == child["task"]["result_path"], (
        "the child is told the name the action already declared"
    )
    assert str(decomposed.template["log_name"]) in child["task"]["argv"][-1]


def test_a_child_may_not_restate_what_the_template_froze(decomposed) -> None:
    """Otherwise a child could wear a template's identity and run elsewhere."""

    with pytest.raises(SystemExit, match="restate the frozen template's: cwd"):
        pbrun.seal_action_from_template(
            decomposed.template, extra_params={"cwd": "/somewhere-else"},
        )


def test_the_placeholder_is_substituted_whole_even_when_the_path_is_odd() -> None:
    """One argument in, one argument out; never shell text."""

    argv = ["python", "collect.py", "--batch", dc.TASK_BATCH_PLACEHOLDER]
    resolved = dc.resolve_task_batch(
        argv, batch_path="/mnt/shared/a dir/$HOME; rm -rf /.json"
    )
    assert resolved == [
        "python", "collect.py", "--batch",
        "/mnt/shared/a dir/$HOME; rm -rf /.json",
    ]


def test_a_command_with_no_slot_for_a_batch_is_refused() -> None:
    with pytest.raises(dc.ActionContractError, match="exactly once"):
        dc.resolve_task_batch(["python", "collect.py"], batch_path="/x.json")
