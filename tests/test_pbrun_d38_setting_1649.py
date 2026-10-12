"""Reviewed fleet configuration selects D38 enforcement, with private evidence."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

import test_pbrun_d38_gate as h

d38_gate = h.d38_gate
pbrun = h.pbrun
fleet = h.fleet
REPOSITORY = h.REPOSITORY


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(d38_gate, "utc_now", lambda: h.NOW)
    monkeypatch.setattr(h.pool, "is_box_local_path", lambda _path: False)
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)
    decisions = tmp_path / "decisions"
    decisions.mkdir()
    monkeypatch.setattr(d38_gate, "DECISION_DIR", decisions)
    return decisions


@pytest.mark.parametrize("images", [["x", 0], {}, [None], None, [{"x": 1}]])
def test_malformed_grant_images_are_failed_evidence_checks(
        tmp_path, monkeypatch, isolated, images):
    _, _, target, cas = h._gpu_target(tmp_path, monkeypatch)
    path = h._grant(isolated, "dec-shape", target)
    decision = json.loads(path.read_text())
    decision["grant"]["d38_exception"]["image_digest"] = images
    path.write_text(json.dumps(decision))
    args = SimpleNamespace(d38_receipt=None, d38_exception="dec-shape")
    check = d38_gate.evaluate(args, target, cas=cas, at=h.NOW)
    assert check["result"] == "failed"
    assert "image_digest" in check["reason"]


@pytest.mark.parametrize("value,expected", [({}, False),
    ({"d38_enforce": False}, False), ({"d38_enforce": True}, True)])
def test_only_reviewed_json_booleans_select_enforcement(d38_config, value, expected):
    d38_config.write_text(json.dumps(value))
    assert d38_gate.enforcement_enabled() is expected


@pytest.mark.parametrize("raw", ["{", "[]", "null", "0", "false",
    '{"d38_enforce": 0}', '{"d38_enforce": 1}',
    '{"d38_enforce": "false"}', '{"d38_enforce": null}',
    '{"d38_enforce": []}', '{"d38_enforce": {}}'])
def test_invalid_configuration_is_an_explicit_error(d38_config, raw):
    d38_config.write_text(raw)
    with pytest.raises(SystemExit, match="D38 configuration error"):
        d38_gate.enforcement_enabled()


def test_unreadable_configuration_is_an_explicit_error(d38_config):
    d38_config.unlink()
    with pytest.raises(SystemExit, match="D38 configuration error"):
        d38_gate.enforcement_enabled()


def test_configuration_follows_the_resolved_module(monkeypatch, tmp_path):
    resolved = tmp_path / "resolved"
    alias = tmp_path / "alias"
    resolved.mkdir()
    alias.mkdir()
    gate = resolved / "d38_gate.py"
    gate.touch()
    (resolved / "fleet_boxes.json").write_text('{"d38_enforce": false}')
    (alias / "fleet_boxes.json").write_text('{"d38_enforce": true}')
    link = alias / "d38_gate.py"
    link.symlink_to(gate)
    monkeypatch.setattr(d38_gate, "__file__", str(link))
    assert d38_gate.enforcement_enabled() is False


@pytest.mark.parametrize("field,value", [
    ("host_class", []), ("host_class", {}), ("host_class", None),
    ("image_digest", ["x", 0]), ("image_digest", {}),
    ("created_at", []), ("expires", {}), ("job_identity_hash", 0)])
def test_malformed_receipt_fields_raise_refusal(field, value):
    body = h._body({"action_key": "a" * 64}, key="b" * 64)
    body[field] = value
    with pytest.raises(d38_gate.Refusal):
        d38_gate._parse_receipt(json.dumps(body).encode())


def _submission(tmp_path, monkeypatch, transport):
    work = h._checkout(tmp_path)
    h._queue(tmp_path)
    ns = h._namespace_file(tmp_path)
    options = ["--gpu", "--detach", "--transport", transport,
               "--d38-namespace", str(ns)]
    target, cas = h._seal(tmp_path, monkeypatch, work, *options)
    return work, options, target, cas


def _assert_publication(tmp_path, target, transport, fleet, published=True):
    if transport == "pool":
        rows = h._ready(tmp_path)
        assert len(rows) == int(published)
        if rows:
            assert json.loads(rows[0].read_text())["action_key"] == target["action_key"]
    else:
        submissions = h._submissions(fleet)
        assert len(submissions) == int(published)
        if submissions:
            assert any(target["action_key"] in token
                       for token in submissions[0]["argv"])


@pytest.mark.parametrize("transport", ["pool", "slurm"])
@pytest.mark.parametrize("failure,reason", [
    ("missing", "no preflight receipt"), ("conflicting", "not both"),
    ("invalid", "not valid JSON"), ("expired", "expired"),
    ("mismatched", "different job identity")])
def test_record_only_failures_audit_and_publish(
        tmp_path, monkeypatch, capsys, isolated, fleet, transport, failure, reason):
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    work, options, target, cas = _submission(tmp_path, monkeypatch, transport)
    if failure != "missing":
        path = h._grant(isolated, "dec-check", target,
                        expires=-1 if failure == "expired" else 3600)
        options += ["--d38-exception", "dec-check"]
        if failure == "conflicting":
            options += ["--d38-receipt", h._publish_receipt(tmp_path, cas, target)]
        elif failure == "invalid":
            path.write_text("{broken")
        elif failure == "mismatched":
            other = {key: value for key, value in target.items() if key != "action_key"}
            other["params"] = {**target["params"], "other_job": True}
            other = h.pb.seal_action(other)
            assert other["action_key"] != target["action_key"]
            h._grant(isolated, "dec-check", other)
    assert h._run(tmp_path, monkeypatch, work, *options) == 0, capsys.readouterr().err
    _assert_publication(tmp_path, target, transport, fleet)
    events = h._audit(tmp_path, target["action_key"])
    assert len(events) == 1
    assert events[0]["mode"] == "record_only"
    assert events[0]["transport"] == transport
    assert events[0]["job_identity_hash"] == target["action_key"]
    assert events[0]["result"] == "failed"
    assert reason in events[0]["reason"]


@pytest.mark.parametrize("transport", ["pool", "slurm"])
@pytest.mark.parametrize("enforce", [False, True])
def test_mixed_grant_images_follow_publication_policy(
        tmp_path, monkeypatch, capsys, isolated, d38_config, fleet, transport, enforce):
    d38_config.write_text(json.dumps({"d38_enforce": enforce}))
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    work, options, target, _ = _submission(tmp_path, monkeypatch, transport)
    path = h._grant(isolated, "dec-mixed", target)
    decision = json.loads(path.read_text())
    decision["grant"]["d38_exception"]["image_digest"] = ["x", 0]
    path.write_text(json.dumps(decision))
    assert h._run(tmp_path, monkeypatch, work, *options,
                  "--d38-exception", "dec-mixed") == (2 if enforce else 0)
    _assert_publication(tmp_path, target, transport, fleet, published=not enforce)
    events = h._audit(tmp_path, target["action_key"])
    if enforce:
        assert events == []
        assert "image_digest" in capsys.readouterr().err
    else:
        event, = events
        assert event["mode"] == "record_only"
        assert event["result"] == "failed"
        assert "image_digest" in event["reason"]


@pytest.mark.parametrize("transport", ["pool", "slurm"])
@pytest.mark.parametrize("enforce", [False, True])
@pytest.mark.parametrize("authorization", ["receipt", "exception"])
def test_authorized_evidence_is_retained_in_both_modes(
        tmp_path, monkeypatch, capsys, isolated, d38_config, fleet,
        transport, enforce, authorization):
    d38_config.write_text(json.dumps({"d38_enforce": enforce}))
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    work, options, target, cas = _submission(tmp_path, monkeypatch, transport)
    if authorization == "receipt":
        key = h._publish_receipt(tmp_path, cas, target)
        options += ["--d38-receipt", key]
    else:
        h._grant(isolated, "dec-pass", target)
        options += ["--d38-exception", "dec-pass"]
    assert h._run(tmp_path, monkeypatch, work, *options) == 0, capsys.readouterr().err
    _assert_publication(tmp_path, target, transport, fleet)
    event, = h._audit(tmp_path, target["action_key"])
    assert event["result"] == "authorized"
    assert event["authorization"] == authorization
    assert event["mode"] == ("enforce" if enforce else "record_only")
    if authorization == "receipt":
        assert event["preflight_action_key"] == key
        assert event["receipt_sha256"]
    else:
        assert event["decision_id"] == "dec-pass"
        assert event["decision_sha256"]


@pytest.mark.parametrize("transport", ["pool", "slurm"])
@pytest.mark.parametrize("enforce", [False, True])
def test_audit_oserror_obeys_the_setting(
        tmp_path, monkeypatch, capsys, isolated, d38_config, fleet, transport, enforce):
    d38_config.write_text(json.dumps({"d38_enforce": enforce}))
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    work, options, target, _ = _submission(tmp_path, monkeypatch, transport)
    h._grant(isolated, "dec-pass", target)
    options += ["--d38-exception", "dec-pass"]
    attempted = []

    def broken(root, event):
        attempted.append(event)
        raise OSError("private audit is read-only")

    monkeypatch.setattr(d38_gate, "write_audit", broken)
    assert h._run(tmp_path, monkeypatch, work, *options) == (2 if enforce else 0)
    assert "audit event cannot be written" in capsys.readouterr().err
    assert len(attempted) == 1
    _assert_publication(tmp_path, target, transport, fleet, published=not enforce)


def test_disabled_deferred_release_audits_the_completed_identity(
        tmp_path, monkeypatch, capsys):
    de, queue, work, template, producer = h._deferred_setup(tmp_path, monkeypatch)
    assert h._submit_deferred(work, monkeypatch, de, producer, template) == 0, \
        capsys.readouterr().err
    assert not list((queue.root / d38_gate.AUDIT_DIR_NAME).rglob("*.json"))
    instance = de._start(queue, template, producer)
    de._commit(queue, template, instance, "b1", b"d38 release bytes")
    queue.finish(producer, status="executed")
    released = de._released(de.dr.release_tick(queue, clock=lambda: 0.0))
    assert len(released) == 1, released
    row, = h._ready(tmp_path)
    key = json.loads(row.read_text())["action_key"]
    event, = h._audit(tmp_path, key)
    assert event["job_identity_hash"] == key
    assert event["mode"] == "record_only"
    assert event["result"] == "failed"
    assert event["transport"] == "pool"


def _generation(tmp_path, layout):
    import publish_runtime

    root = tmp_path / "generation"
    directory = root / layout
    directory.mkdir(parents=True)
    shutil.copytree(REPOSITORY / "src", root / "src",
                    ignore=shutil.ignore_patterns("__pycache__"))
    for name in (*publish_runtime.FLEET_SCRIPTS, *publish_runtime.FLEET_DATA):
        source = REPOSITORY / "tools" / "fleet" / name
        if not source.is_file():
            source = REPOSITORY / "tools" / name
        if source.is_file():
            shutil.copy2(source, directory / name)
    for name in publish_runtime.TOP_LEVEL_SCRIPTS:
        shutil.copy2(REPOSITORY / "tools" / name, root / "tools" / name)
    config = json.loads((directory / "fleet_boxes.json").read_text())
    config.pop("d38_enforce", None)
    (directory / "fleet_boxes.json").write_text(json.dumps(config))
    return root, directory


FRESH_SUBMISSION = """
import datetime, socket, sys
from pathlib import Path
root, directory, private, work, transport, intent = sys.argv[1:]
sys.path[:0] = [str(Path(root) / 'src'), directory]
import pbrun, d38_gate
from prismabuild import pool
pbrun.SH = Path(private)
pool.is_box_local_path = lambda _path: False
socket.gethostname = lambda: 'sparky' if transport == 'pool' and intent == 'demand' else 'dl380g10'
d38_gate.utc_now = lambda: datetime.datetime(2026, 10, 8, 16, 30, tzinfo=datetime.timezone.utc)
assert d38_gate.INVOCATIONS == {}
assert d38_gate.enforcement_enabled() is False
queue = pool.PoolQueue(Path(private) / 'pb-queue')
queue.announce(host='sparky', tags=['sparky', 'gb10'], has_gpu=True,
               capacity={'cpu': 4, 'mem_gb': 16, 'gpu': 1})
queue.announce(host='dl380g10', tags=['dl380g10', 'x86'], has_gpu=False,
               capacity={'cpu': 4, 'mem_gb': 16})
options = {'demand': ['--demand', 'gpu=1,mem_gb=1'],
           'tag': ['--tag', 'gb10'], 'class': ['--host-class', 'gb10']}[intent]
sys.argv = ['pbrun.py', '--cwd', work, '--cpus', '1', '--detach',
            '--transport', transport, *options, '--', '/bin/cat', 'seed.txt']
raise SystemExit(pbrun.main())
"""


@pytest.mark.parametrize("layout", ["tools", "tools/fleet"])
@pytest.mark.parametrize("transport,intent", [
    ("pool", "demand"), ("pool", "tag"), ("slurm", "demand"),
    ("slurm", "tag"), ("slurm", "class")])
def test_absent_key_in_fresh_generations_publishes_and_audits(
        tmp_path, monkeypatch, fleet, layout, transport, intent):
    root, directory = _generation(tmp_path, layout)
    work = h._checkout(tmp_path)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    completed = subprocess.run(
        [sys.executable, "-c", FRESH_SUBMISSION, str(root), str(directory),
         str(tmp_path), str(work), transport, intent],
        cwd=work, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    line = json.loads(completed.stdout)
    assert line["status"] == "submitted"
    key = line["action_key"]
    _assert_publication(tmp_path, {"action_key": key}, transport, fleet)
    event, = h._audit(tmp_path, key)
    assert event["mode"] == "record_only"
    assert event["result"] == "failed"
    assert event["reason"]
    assert event["job_identity_hash"] == key
    assert event["transport"] == transport


def test_no_caller_override_selects_enforcement(tmp_path, monkeypatch, d38_config):
    for name in ("D38_ENFORCE", "PRISMABUILD_D38_ENFORCE", "ENFORCE"):
        monkeypatch.setenv(name, "true")
    assert d38_gate.enforcement_enabled() is False
    for option in ("--d38-enforce", "--no-d38-enforce"):
        with pytest.raises(SystemExit):
            pbrun.parse_args([option, "--", "/bin/true"])
    action = {"action_key": "a" * 64, "params": {
        "demand": {"gpu": 1}, "command": ["/bin/true"]}}
    args = SimpleNamespace(d38_enforce=True, host_class=None)
    assert d38_gate.judge_publication(args, action, cas=None,
        queue_root=tmp_path / "queue", transport="pool") is None
    d38_config.write_text('{"d38_enforce": true}')
    for name in ("D38_ENFORCE", "PRISMABUILD_D38_ENFORCE", "ENFORCE"):
        monkeypatch.setenv(name, "false")
    args.d38_enforce = False
    assert d38_gate.judge_publication(args, action, cas=None,
        queue_root=tmp_path / "queue", transport="pool") is not None
