"""Every child passing is not yet the request being answered.

A decomposed campaign's claim is the cover: that between them the children
answered each roster task once, under the plan that fixed their membership.
The children here really run -- a small producer reads the batch it was handed
and writes the manifest its action declared -- so what is checked is the whole
chain the merge rests on: the declared result is the manifest, the manifest is
read back through the receipt that fixed its digest, and the group receipt
appears only when the cover closes.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import contracts
from prismabuild import core as pb, decomposition as dc, pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbcampaign  # noqa: E402
import pbwait  # noqa: E402

from test_pbrun_detach import _checkout, _queue  # noqa: E402
from file_open_observer import observe_file_opens  # noqa: E402

EVIDENCE = "cas:sha256:" + "0" * 64
CHILD_RESULT_LIMIT_BYTES = 1024 * 1024

#: A producer, in the only sense this test needs one: it reads the envelope it
#: was handed, answers for exactly the tasks in it, and writes the manifest at
#: the name the envelope names -- which is the name its action already sealed
#: as its result path.  ``OUTPUT`` is what a real adapter would take from the
#: roster; spelling it wrong is how the cover is made to fail.
PRODUCER = """
import hashlib, json, sys
batch = json.load(open(sys.argv[1]))
open(batch["result_manifest_path"], "w").write(json.dumps({
    "schema": "prismabuild.child_result_manifest.v1",
    "parent_key": batch["parent_key"],
    "plan_key": batch["plan_key"],
    "child_ordinal": batch["child_ordinal"],
    "results": [
        {
            "task_id": task["id"],
            "output_id": OUTPUT,
            "value_sha256": hashlib.sha256(
                json.dumps(task["payload"], sort_keys=True).encode()
            ).hexdigest(),
        }
        for task in batch["tasks"]
    ],
}))
"""


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    return _checkout(tmp_path), _queue(tmp_path)


def _request(work: Path, *, output: str = 'task["output_id"]') -> dict:
    return {
        "schema": dc.LOGICAL_REQUEST_SCHEMA_V1,
        "common": {
            "argv": [sys.executable, "-c", PRODUCER.replace("OUTPUT", output),
                     dc.TASK_BATCH_PLACEHOLDER],
            "cwd": str(work),
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
                    "output_id": f"out{index:04d}",
                }
                for index in range(66)
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


def _decompose(request) -> tuple[list[dict], dict]:
    return pbcampaign.decompose(
        dc.validate_logical_request(request), transport="pool", priority=0)


def _serve(queue: pool.PoolQueue, *, limit: int | None = None) -> int:
    """Run the queued children here, and say how many actually executed."""

    served = 0
    while limit is None or served < limit:
        # The children run under the fleet's own interpreter, so placement
        # requires the interpreter-path tag as well as the host pin; a worker
        # that offers only ``sparky`` claims none of them (#1263).
        one = queue.serve_once(
            tags=["sparky", contracts.INTERPRETER_TAG], python=sys.executable, timeout_s=120.0,
            capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
        )
        if one is None:
            break
        assert one["status"] == "executed", one
        served += 1
    return served


def _group_path(tmp_path: Path, group: dict) -> Path:
    return (tmp_path / "cas" / dc.DECOMPOSITIONS
            / group["plan"]["parent_key"][:2] / group["plan"]["parent_key"]
            / "group.json")


def _small_request(work: Path, producer: str) -> dict:
    request = _request(work)
    request["common"]["argv"][2] = producer
    request["roster"]["tasks"] = request["roster"]["tasks"][:3]
    request["batch_policy"]["residencies"][0]["setup_seconds"] = 20.0
    request["batch_policy"]["max_setup_fraction"] = 0.5
    return request


def _producer_rewriting_result(expression: str) -> str:
    return PRODUCER.replace("OUTPUT", 'task["output_id"]') + (
        '\nfrom pathlib import Path\n'
        'result = Path(batch["result_manifest_path"])\n'
        'raw = result.read_bytes()\n'
        f'result.write_bytes({expression})\n')


# --------------------------------------------------------------------------
# The cover closes
# --------------------------------------------------------------------------

def test_children_that_answered_their_own_batches_earn_one_group_receipt(
    tmp_path: Path, fleet, capsys,
) -> None:
    work, queue = fleet
    records, group = _decompose(_request(work))
    assert len(group["children"]) >= 2, "one child is not a decomposition"
    assert {one["status"] for one in records} == {"submitted"}

    assert _serve(queue) == len(group["children"])

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert pbcampaign.close_group(group, cas=cas) == 0

    receipt = json.loads(_group_path(tmp_path, group).read_text())
    assert receipt["schema"] == dc.GROUP_RECEIPT_SCHEMA_V1
    assert receipt["parent_key"] == group["plan"]["parent_key"]
    assert receipt["plan_key"] == group["plan"]["plan_key"]
    assert receipt["task_count"] == 66
    assert receipt["child_count"] == len(group["children"])
    assert receipt["children"] == [
        {
            "action_key": child["action_key"],
            "receipt_sha256": dc.document_sha256(cas.lookup(child)),
        }
        for child in group["children"]
    ], "the group receipt must name the exact child receipts it verified"
    # The merge is over every answer in plan order, so it is a statement about
    # the whole roster and not about whichever child finished last.
    assert receipt["merged_result_sha256"] == pb.canonical_sha256([
        [f"t{index:04d}", f"out{index:04d}",
         hashlib.sha256(
             json.dumps({"rate": 832 + index}, sort_keys=True).encode()
         ).hexdigest()]
        for index in range(66)
    ])
    assert "group receipt" in capsys.readouterr().err


def test_closing_the_same_group_twice_publishes_the_same_bytes(
    tmp_path: Path, fleet,
) -> None:
    """Nothing in the receipt can vary, so a second close is a re-read."""

    work, queue = fleet
    _, group = _decompose(_request(work))
    _serve(queue)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    assert pbcampaign.close_group(group, cas=cas) == 0
    first = _group_path(tmp_path, group).read_bytes()
    assert pbcampaign.close_group(group, cas=cas) == 0
    assert _group_path(tmp_path, group).read_bytes() == first


@pytest.mark.parametrize("extra_byte", [0, 1], ids=["at-limit", "over-limit"])
def test_campaign_child_result_byte_limit(
    tmp_path: Path, fleet, capsys, extra_byte,
) -> None:
    work, queue = fleet
    size = CHILD_RESULT_LIMIT_BYTES + extra_byte
    producer = _producer_rewriting_result(f'raw + b" " * ({size} - len(raw))')
    _, group = _decompose(_small_request(work, producer))
    assert len(group["children"]) == 1
    assert _serve(queue) == 1
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    child = group["children"][0]
    child_receipt = cas.lookup(child)
    assert child_receipt is not None
    assert child_receipt["result"]["bytes"] == size

    assert pbcampaign.close_group(group, cas=cas) == extra_byte
    assert _group_path(tmp_path, group).exists() is (extra_byte == 0)
    if extra_byte:
        assert "byte bound" in capsys.readouterr().err


def test_campaign_refuses_an_oversized_result_before_opening_its_blob(
    tmp_path: Path, fleet, monkeypatch, capsys,
) -> None:
    work, queue = fleet
    size = CHILD_RESULT_LIMIT_BYTES + 1
    producer = _producer_rewriting_result(f'raw + b" " * ({size} - len(raw))')
    _, group = _decompose(_small_request(work, producer))
    assert _serve(queue) == 1
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    child_receipt = cas.lookup(group["children"][0])
    assert child_receipt is not None
    blob = cas.blob_path(child_receipt["result"]["sha256"])
    assert blob.stat().st_size == size
    opened = observe_file_opens(monkeypatch, blob)

    status = pbcampaign.close_group(group, cas=cas)
    assert opened == [], "an over-limit declared result must refuse before its first open"
    assert status == 1
    assert "byte bound" in capsys.readouterr().err
    assert not _group_path(tmp_path, group).exists()


@pytest.mark.parametrize("damage", [
    "schema", "unknown-field", "missing-digests", "invalid-digest", "digest-count",
])
def test_campaign_refuses_a_malformed_publication_with_matching_children(
    tmp_path: Path, fleet, damage,
) -> None:
    work, queue = fleet
    producer = PRODUCER.replace("OUTPUT", 'task["output_id"]')
    _, group = _decompose(_small_request(work, producer))
    assert _serve(queue) == 1
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    path = dc.record_dir(cas.root, group["plan"]["parent_key"]) / "publication.json"
    publication = json.loads(path.read_bytes())
    if damage == "schema":
        publication["schema"] = "wrong-schema"
    elif damage == "unknown-field":
        publication["unexpected"] = True
    elif damage == "missing-digests":
        del publication["batch_input_sha256"]
    elif damage == "invalid-digest":
        publication["batch_input_sha256"][0] = "not-a-digest"
    else:
        publication["batch_input_sha256"] = []
    damaged = dc.document_bytes(publication)
    path.unlink()
    path.write_bytes(damaged)

    assert pbcampaign.close_group(group, cas=cas) == 1
    assert path.read_bytes() == damaged
    assert not _group_path(tmp_path, group).exists()


@pytest.mark.parametrize("representation", ["duplicate-key", "utf16"])
def test_campaign_requires_strict_utf8_child_manifest_json(
    tmp_path: Path, fleet, representation,
) -> None:
    work, queue = fleet
    if representation == "duplicate-key":
        expression = (
            'b\'{"schema":"prismabuild.child_result_manifest.v1",\' + raw[1:]')
    else:
        expression = 'raw.decode("utf-8").encode("utf-16")'
    _, group = _decompose(_small_request(work, _producer_rewriting_result(expression)))
    assert _serve(queue) == 1
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    child_receipt = cas.lookup(group["children"][0])
    assert child_receipt is not None
    assert child_receipt["result"]["bytes"] < CHILD_RESULT_LIMIT_BYTES

    assert pbcampaign.close_group(group, cas=cas) == 1
    assert not _group_path(tmp_path, group).exists()


@pytest.mark.parametrize("binding", ["parent_key", "plan_key", "child_action_keys"])
def test_campaign_still_refuses_a_publication_for_another_group(
    tmp_path: Path, fleet, binding,
) -> None:
    work, queue = fleet
    producer = PRODUCER.replace("OUTPUT", 'task["output_id"]')
    _, group = _decompose(_small_request(work, producer))
    assert _serve(queue) == 1
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    path = dc.record_dir(cas.root, group["plan"]["parent_key"]) / "publication.json"
    publication = json.loads(path.read_bytes())
    publication[binding] = ["f" * 64] if binding == "child_action_keys" else "f" * 64
    path.unlink()
    path.write_bytes(dc.document_bytes(publication))

    assert pbcampaign.close_group(group, cas=cas) == 1
    assert not _group_path(tmp_path, group).exists()


def test_campaign_accepts_retained_publication_formatting(
    tmp_path: Path, fleet,
) -> None:
    work, queue = fleet
    producer = PRODUCER.replace("OUTPUT", 'task["output_id"]')
    _, group = _decompose(_small_request(work, producer))
    assert _serve(queue) == 1
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    path = dc.record_dir(cas.root, group["plan"]["parent_key"]) / "publication.json"
    publication = json.loads(path.read_bytes())
    retained = json.dumps(dict(reversed(list(publication.items()))), indent=3).encode()
    assert retained != dc.document_bytes(publication)
    path.unlink()
    path.write_bytes(retained)

    assert pbcampaign.close_group(group, cas=cas) == 0
    assert path.read_bytes() == retained
    assert _group_path(tmp_path, group).exists()


# --------------------------------------------------------------------------
# The cover does not close
# --------------------------------------------------------------------------

def test_a_receipted_json_null_keeps_the_missing_child_verdict(
    tmp_path: Path, fleet, capsys,
) -> None:
    work, queue = fleet
    producer = _producer_rewriting_result('b"null\\n"')
    _, group = _decompose(_small_request(work, producer))
    assert len(group["children"]) == 1
    assert _serve(queue) == 1
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    child = group["children"][0]
    receipt = cas.lookup(child)
    assert receipt is not None
    assert cas.result_path(receipt, child).read_bytes() == b"null\n"

    with pytest.raises(dc.MissingChildResult) as missing:
        dc.complete_group(group["request"], group["plan"], group["children"], cas=cas)
    assert (missing.value.ordinal, missing.value.count) == (0, 1)
    capsys.readouterr()

    assert pbcampaign.close_group(group, cas=cas) == 1
    assert capsys.readouterr().err == (
        "pbcampaign: no group receipt: child 0 of 1 has no receipt\n")
    assert not _group_path(tmp_path, group).exists()
    assert cas.lookup(child) == receipt


def test_a_child_without_a_receipt_is_never_a_group_success(
    tmp_path: Path, fleet, capsys,
) -> None:
    """An incomplete set stops here even though every child that ran passed."""

    work, queue = fleet
    _, group = _decompose(_request(work))
    assert _serve(queue, limit=len(group["children"]) - 1) \
        == len(group["children"]) - 1

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert pbcampaign.close_group(group, cas=cas) == 1
    assert "no group receipt" in capsys.readouterr().err
    assert not _group_path(tmp_path, group).exists()


def test_a_receipted_manifest_for_another_ordinal_fails_the_cover(
    tmp_path: Path, fleet, capsys,
) -> None:
    work, queue = fleet
    producer = PRODUCER.replace("OUTPUT", 'task["output_id"]')
    producer = producer.replace(
        '"child_ordinal": batch["child_ordinal"],',
        '"child_ordinal": batch["child_ordinal"] + 1,')
    _, group = _decompose(_small_request(work, producer))
    assert _serve(queue) == 1
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    child = group["children"][0]
    receipt = cas.lookup(child)
    assert receipt is not None
    assert json.loads(cas.result_path(receipt, child).read_bytes())["child_ordinal"] == 1

    assert pbcampaign.close_group(group, cas=cas) == 1
    assert "ordinal" in capsys.readouterr().err
    assert not _group_path(tmp_path, group).exists()


def test_a_receipt_for_another_child_cannot_close_the_group(
    tmp_path: Path, fleet,
) -> None:
    work, queue = fleet
    _, group = _decompose(_request(work))
    assert len(group["children"]) >= 2
    assert _serve(queue) == len(group["children"])
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    first, second = group["children"][:2]
    assert cas.lookup(first) is not None
    assert cas.lookup(second) is not None
    path = cas._receipt_path(first["action_key"])
    foreign = cas._receipt_path(second["action_key"]).read_bytes()
    path.unlink()
    path.write_bytes(foreign)
    path.chmod(0o444)

    assert pbcampaign.close_group(group, cas=cas) == 1
    assert path.read_bytes() == foreign
    assert not _group_path(tmp_path, group).exists()


def test_a_child_answering_under_the_wrong_output_id_fails_the_cover(
    tmp_path: Path, fleet, capsys,
) -> None:
    """Every child exits zero; the roster is still unanswered.

    This is the whole reason the group receipt is not just "did they all
    pass".  A producer that answers for its own tasks under a name the roster
    never asked for has produced something, and nothing that was wanted.
    """

    work, queue = fleet
    _, group = _decompose(_request(work, output='"elsewhere"'))
    assert _serve(queue) == len(group["children"])

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert pbcampaign.close_group(group, cas=cas) == 1
    assert "not the roster's" in capsys.readouterr().err
    assert not _group_path(tmp_path, group).exists()


def test_a_group_receipt_is_refused_rather_than_replaced(
    tmp_path: Path, fleet, capsys,
) -> None:
    """Two runs cannot honestly disagree here, so a difference is the finding."""

    work, queue = fleet
    _, group = _decompose(_request(work))
    _serve(queue)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert pbcampaign.close_group(group, cas=cas) == 0

    path = _group_path(tmp_path, group)
    stored = json.loads(path.read_bytes())
    damaged = dc.document_bytes({**stored, "task_count": stored["task_count"] + 1})
    path.unlink()
    path.write_bytes(damaged)

    assert pbcampaign.close_group(group, cas=cas) == 1
    assert "not the one this run verified" in capsys.readouterr().err
    assert path.read_bytes() == damaged


# --------------------------------------------------------------------------
# The cover decides the campaign, at the boundary an operator actually uses
# --------------------------------------------------------------------------

def _manifest(tmp_path: Path, request) -> str:
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    return str(path)


def _group_receipts(tmp_path: Path) -> list[Path]:
    return sorted(
        (tmp_path / "cas" / dc.DECOMPOSITIONS).rglob("group.json"))


def _detached_keys(out: str) -> list[str]:
    return [json.loads(line)["action_key"] for line in out.splitlines() if line]


def test_the_group_receipt_is_what_a_re_run_without_detach_adds(
    tmp_path: Path, fleet, capsys,
) -> None:
    """The path to a decomposed campaign's verdict, as it is actually walked.

    ``--detach`` returns before anything has run, so it proves no cover and
    publishes no group receipt.  Re-running the same request once the children
    are done submits nothing -- every child is a cache hit -- and that second
    run is where the roster is finally shown to have been answered.
    """

    work, queue = fleet
    manifest = _manifest(tmp_path, _request(work))

    assert pbcampaign.main(["--transport", "pool", "--detach", manifest]) == 0
    children = _detached_keys(capsys.readouterr().out)
    assert len(children) >= 2, "one child is not a decomposition"
    assert _group_receipts(tmp_path) == []

    assert _serve(queue) == len(children)

    assert pbcampaign.main(
        ["--transport", "pool", "--wait-s", "60", manifest]) == 0
    table = capsys.readouterr().out.splitlines()
    assert len(table) == 1 + len(children)
    assert all("cache_hit" in line for line in table[1:])

    receipts = _group_receipts(tmp_path)
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_text())
    assert receipt["task_count"] == 66
    assert receipt["child_count"] == len(children)


def test_a_campaign_whose_every_row_passed_still_fails_on_an_open_cover(
    tmp_path: Path, fleet, capsys,
) -> None:
    """The exit status follows the cover, not the rows.

    Every child ran, exited zero and is a cache hit, so the table this prints
    is a table of successes and ``pbwait``'s own verdict on it is 0.  The
    campaign still exits 1, because the roster it was given is not answered.
    """

    work, queue = fleet
    manifest = _manifest(tmp_path, _request(work, output='"elsewhere"'))

    assert pbcampaign.main(["--transport", "pool", "--detach", manifest]) == 0
    children = _detached_keys(capsys.readouterr().out)
    assert _serve(queue) == len(children)

    assert pbcampaign.main(
        ["--transport", "pool", "--wait-s", "60", manifest]) == 1
    printed = capsys.readouterr()
    table = printed.out.splitlines()
    assert len(table) == 1 + len(children)
    assert all("cache_hit" in line for line in table[1:])
    assert pbwait.verdict([
        {"status": "cache_hit", "succeeded": True} for _ in children
    ]) == 0, "the rows this campaign failed on are rows pbwait passes"
    assert "not the roster's" in printed.err
    assert _group_receipts(tmp_path) == []
