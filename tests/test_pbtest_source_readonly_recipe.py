"""Portable W4 recipe guards, independent of shared-source worker admission."""
from __future__ import annotations

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import action_result, core, movement_actions
from prismabuild.submission_types import SubmissionRequest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest_source_readonly_recipe", ROOT / "tools/fleet/pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)


def _action(checkout, *, collection=True, arguments=()):
    command = pbtest.shard_entry(sys.executable, checkout, collection=collection)
    if collection:
        command.append(json.dumps({"files": ["tests/test_one.py"],
                                   "roots": [["tests", "directory"]]}))
    command.extend(["-q", *arguments, "tests/test_one.py"])
    log = "declared ' logical.log"
    return core.seal_action({
        "schema": core.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/readonly-recipe", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": movement_actions.standard_capture_argv(command, log, path_prefix="/usr/bin"),
                 "working_directory": ".", "result_path": log},
        "inputs": [], "code_closure": core.build_code_closure(checkout, ["tests/test_one.py"]),
        "params": {"command": command},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None, "host_class": None},
    })


@pytest.fixture
def checkout(tmp_path):
    root = tmp_path / "source"
    (root / "tests").mkdir(parents=True)
    (root / "tests/test_one.py").write_text(
        "import sys\ndef test_one(tmp_path):\n"
        "    assert sys.dont_write_bytecode\n"
        "    (tmp_path / 'allowed').write_text('private')\n")
    return root


def test_reseal_is_explicit_and_preserves_source_and_logical_result(checkout):
    private = _action(checkout)
    before = copy.deepcopy(private)
    sealed = pbtest._source_readonly_action(private)
    assert private == before
    assert sealed["action_key"] != private["action_key"]
    assert sealed["code_closure"] == private["code_closure"]
    assert sealed.get("code_snapshot") == private.get("code_snapshot")
    assert sealed["task"]["result_path"] == private["task"]["result_path"]
    assert sealed["params"]["execution_layout"] == "pbtest-source-readonly-v1"
    assert action_result.bind_standard_capture_command(sealed) == sealed["task"]["argv"]
    assert action_result.bind_standard_capture_command(private) == private["task"]["argv"]
    assert "PRISMABUILD_RESULT_PATH" in sealed["task"]["argv"][4]
    assert pbtest._Templates(None)._source_readonly is False


@pytest.mark.parametrize("change", [
    "private-recipe", "legacy-recipe", "missing-layout", "unknown-layout",
    "null-layout", "tampered-recipe", "changed-command", "changed-prefix",
])
def test_cross_layout_and_nonexact_recipes_refuse(checkout, change):
    private = _action(checkout)
    sealed = pbtest._source_readonly_action(private)
    if change == "private-recipe":
        sealed["task"]["argv"] = private["task"]["argv"]
    elif change == "legacy-recipe":
        sealed["task"]["argv"] = movement_actions._capture_argv(
            sealed["params"]["command"], sealed["task"]["result_path"],
            path_prefix="/usr/bin", legacy=True)
    elif change == "missing-layout":
        sealed["params"].pop("execution_layout")
    elif change == "unknown-layout":
        sealed["params"]["execution_layout"] = "future-layout"
    elif change == "null-layout":
        sealed["params"]["execution_layout"] = None
    elif change == "changed-command":
        sealed["params"]["command"].append("--verbose")
    elif change == "changed-prefix":
        sealed["environment"]["variables"]["PATH"] = "/custom/bin:/usr/bin:/bin"
    else:
        sealed["task"]["argv"][4] += "; true"
    sealed.pop("action_key")
    with pytest.raises(action_result.ActionResultError, match="standard captured-log"):
        action_result.bind_standard_capture_command(core.seal_action(sealed))


@pytest.mark.parametrize("collection", [True, False])
def test_real_shard_writes_only_to_worker_private_paths(checkout, tmp_path, collection):
    (checkout / "conftest.py").write_text(
        "from pathlib import Path\n"
        "def pytest_addoption(parser):\n"
        "    parser.addoption('--surface-json')\n"
        "def pytest_sessionfinish(session):\n"
        "    config = session.config\n"
        "    config.cache.set('probe/value', 1)\n"
        "    Path(config.getoption('--surface-json')).write_text('surface')\n")
    private = _action(checkout, collection=collection,
                      arguments=["--surface-json", "reports/probe.shard-0.json",
                                 "-o", "cache_dir=forbidden-source-cache"])
    sealed = pbtest._source_readonly_action(private)
    scratch = tmp_path / "worker"
    scratch.mkdir()
    result_path = scratch / "physical ' $ log"
    environment = {**os.environ, "TMPDIR": str(scratch), "PYTEST_ADDOPTS": "",
                   "PRISMABUILD_ACTION_KEY": sealed["action_key"],
                   "PRISMABUILD_ACTION_NONCE": "ab12" * 8,
                   "PRISMABUILD_RESULT_PATH": str(result_path),
                   "PRISMABUILD_PBTEST_HELPER_DIR": str(scratch / "helpers"),
                   "PRISMABUILD_PBTEST_CACHE_DIR": str(scratch / "cache"),
                   "PRISMABUILD_PBTEST_SURFACE_DIR": str(scratch / "surface")}
    before = {str(path.relative_to(checkout)): path.read_bytes()
              for path in checkout.rglob('*') if path.is_file()}
    completed = subprocess.run(sealed["task"]["argv"], cwd=checkout, env=environment,
                               capture_output=True, text=True, timeout=90)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "1 passed" in completed.stdout
    assert result_path.read_text() == completed.stdout
    assert (scratch / "cache/v/probe/value").read_text().strip() == "1"
    assert (scratch / "surface/probe.shard-0.json").read_text() == "surface"
    assert (scratch / "helpers").is_dir()
    assert not (checkout / private["task"]["result_path"]).exists()
    after = {str(path.relative_to(checkout)): path.read_bytes()
             for path in checkout.rglob('*') if path.is_file()}
    assert before == after


@pytest.mark.parametrize("result_path", [None, "relative.log"])
def test_capture_requires_an_absolute_worker_result_path(tmp_path, result_path):
    environment = dict(os.environ)
    environment.pop("PRISMABUILD_RESULT_PATH", None)
    if result_path is not None:
        environment["PRISMABUILD_RESULT_PATH"] = result_path
    command = [sys.executable, "-c", "print('must not launch')"]
    completed = subprocess.run(movement_actions._source_readonly_capture_argv(
        command, path_prefix="/usr/bin"), env=environment, cwd=tmp_path,
        capture_output=True, text=True, timeout=30)
    assert completed.returncode == 125
    assert completed.stdout == ""
    assert "worker result path must be absolute" in completed.stderr


@pytest.mark.parametrize("overrides,supported", [
    ({}, True), ({"transport": "slurm"}, False),
    ({"profile": "time"}, False), ({"container_image": "image"}, False),
    ({"transport": "custom-transport"}, False),
])
def test_unsupported_requests_keep_private_layout(overrides, supported):
    request = SubmissionRequest(command=("true",), **overrides)
    assert pbtest._source_readonly_supported(request) is supported


@pytest.mark.parametrize("opt_in", [False, True])
def test_cli_requires_explicit_opt_in_even_with_host_tags(tmp_path, monkeypatch, opt_in):
    from test_pbtest_identifies_its_checkout_once import _counted_run, _sealed

    extra = ("--source-readonly",) if opt_in else ()
    code, counts, root = _counted_run(tmp_path, monkeypatch, shards=1, extra=extra)
    actions = _sealed(root)
    assert len(actions) == 1
    assert counts == {"identity": 4, "bundle": 1}
    if opt_in:
        assert actions[0]["params"]["execution_layout"] == "pbtest-source-readonly-v1"
    else:
        assert "execution_layout" not in actions[0]["params"]
    # There is deliberately no worker in this publication-only fixture.
    assert code == 1


@pytest.mark.parametrize("name", sorted(movement_actions._WORKER_PATH_ENV_KEYS))
def test_pbtest_rejects_caller_worker_path_overrides(name):
    with pytest.raises(ValueError, match=name):
        pbtest.validate_shard_env([name + "=/caller/path"])


@pytest.mark.parametrize("name", sorted(movement_actions._WORKER_PATH_ENV_KEYS))
def test_resealing_refuses_caller_worker_path_overrides(checkout, name):
    private = _action(checkout)
    private.pop("action_key")
    private["environment"]["variables"][name] = "/caller/path"
    private = core.seal_action(private)
    with pytest.raises(ValueError, match="supplied by the worker"):
        pbtest._source_readonly_action(private)


@pytest.mark.parametrize("name", sorted(movement_actions._WORKER_PATH_ENV_KEYS))
def test_submission_refuses_worker_path_overrides_before_snapshot(
        tmp_path, monkeypatch, name):
    from types import SimpleNamespace
    from prismabuild import submission
    from prismabuild.submission_types import SubmissionRefused
    import test_pbtest_identifies_its_checkout_once as plan_fixture

    root = plan_fixture._checkout(tmp_path, files=1)

    def unexpected(*args, **kwargs):
        pytest.fail("reserved environment reached snapshot construction")

    monkeypatch.setattr(submission, "snapshot_checkout", unexpected)
    request = SubmissionRequest(command=(sys.executable, "-c", "print('probe')"),
                                cwd=str(root), env=(name + "=/caller/path",))
    with pytest.raises(SubmissionRefused, match=name):
        submission.prepare(request, store=SimpleNamespace(wrapper_dir=root), display=None)


@pytest.mark.parametrize("request_overrides,action_defect", [
    ({"profile": "time"}, None), ({"transport": "slurm"}, None),
    ({"container_image": "image"}, None),
    ({}, "no-snapshot"), ({}, "cwd"), ({}, "missing-cwd"), ({}, "empty-cwd"),
    ({}, "working-directory"), ({}, "profile"),
    ({}, "container_images"), ({}, "container_owner"), ({}, "definition"),
    ({}, "absolute-surface"), ({}, "parent-surface"),
    ({}, "equals-surface"), ({}, "rebound-surface"), ({}, "sealed-surface"),
])
def test_publication_falls_back_without_resealing(
        checkout, monkeypatch, request_overrides, action_defect):
    from types import SimpleNamespace
    from prismabuild.submission_types import SubmissionRefused

    # Only publication selection is under test; no snapshot is materialized.
    private = _action(checkout)
    original_command = copy.deepcopy(private["params"]["command"])
    private.pop("action_key")
    private["task"]["definition_id"] = "fleet/pbrun"
    private["params"].update(cwd=".", checkout_snapshot={"fixture": "metadata-only"})
    if action_defect == "no-snapshot":
        private["params"].pop("checkout_snapshot")
    elif action_defect == "cwd":
        private["params"]["cwd"] = "tests"
    elif action_defect == "missing-cwd":
        private["params"].pop("cwd")
    elif action_defect == "empty-cwd":
        private["params"]["cwd"] = ""
    elif action_defect == "working-directory":
        private["task"]["working_directory"] = "tools"
    elif action_defect == "definition":
        private["task"]["definition_id"] = "custom/producer"
    elif action_defect in {"absolute-surface", "parent-surface", "equals-surface",
                           "rebound-surface", "sealed-surface"}:
        destination = ("reports/rebound.json" if action_defect == "rebound-surface"
                       else "../external-report.json" if action_defect == "parent-surface"
                       else str(checkout.parent / "external-report.json"))
        command = private["params"]["command"]
        command += (["--surface-json=" + destination] if action_defect == "equals-surface"
                    else ["--surface-json", destination])
        private["task"]["argv"] = movement_actions.standard_capture_argv(
            command, private["task"]["result_path"],
            path_prefix=private["environment"]["variables"]["PATH"].split(":", 1)[0])
    elif action_defect:
        private["params"][action_defect] = "enabled"
    private = core.seal_action(private)
    request_command = private["params"]["command"]
    if action_defect == "rebound-surface":
        request_command = original_command + [
            "--surface-json", str(checkout / "reports/rebound.json")]
    elif action_defect == "sealed-surface":
        request_command = original_command + ["--surface-json", "reports/relative.json"]
    request = SubmissionRequest(command=tuple(request_command), **request_overrides)
    record = SimpleNamespace(lines=[], refusal=None, template=object())
    templates = SimpleNamespace(
        _source_readonly=True, store=object(),
        sealing=lambda *args, **kwargs: (record, list(request.command)))
    seen = []

    def submit(action, **kwargs):
        seen.append(action)
        return SubmissionRefused("publication-only fixture")

    def unexpected(*args, **kwargs):
        pytest.fail("unsupported mode was resealed for worker-private paths")

    monkeypatch.setattr(pbtest.submission, "seal", lambda *args, **kwargs: private)
    monkeypatch.setattr(pbtest.submission, "submit_action", submit)
    monkeypatch.setattr(pbtest, "_source_readonly_action", unexpected)
    import time
    result = {}
    pbtest.publish_shard(templates, 0, request, plan=object(), owner=request,
                         shared=False, result=result, deadline=time.monotonic() + 30)
    assert "using the private recipe" in "".join(result["lines"])
    assert seen == [private]
    assert "execution_layout" not in seen[0]["params"]
    assert action_result.bind_standard_capture_command(seen[0]) == private["task"]["argv"]


@pytest.mark.parametrize("destination", ["/external/report.json", "../report.json",
                                         "reports/../report.json"])
@pytest.mark.parametrize("equals", [False, True])
def test_custom_surface_paths_are_not_resealed(checkout, destination, equals):
    arguments = (["--surface-json=" + destination] if equals
                 else ["--surface-json", destination])
    private = _action(checkout, arguments=arguments)
    before = copy.deepcopy(private)
    with pytest.raises(ValueError, match="surface"):
        pbtest._source_readonly_action(private)
    assert private == before
    # Inspect the original request too: preparation may rebind an absolute
    # argument into a relative input path, but that does not authorize sharing.
    request = SubmissionRequest(command=tuple(private["params"]["command"]))
    assert not pbtest._source_readonly_supported(request)


@pytest.mark.parametrize("equals", [False, True])
def test_private_generated_shard_preserves_external_surface_report(checkout, tmp_path, equals):
    import shutil

    destination = tmp_path / "external-report.json"
    (checkout / "conftest.py").write_text(
        "from pathlib import Path\n"
        "def pytest_addoption(parser):\n"
        "    parser.addoption('--surface-json')\n"
        "def pytest_sessionfinish(session):\n"
        "    Path(session.config.getoption('--surface-json')).write_text('external')\n")
    arguments = (["--surface-json=" + str(destination)] if equals
                 else ["--surface-json", str(destination)])
    private = _action(checkout, arguments=arguments)
    request = SubmissionRequest(command=tuple(private["params"]["command"]))
    assert not pbtest._source_readonly_supported(request)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    environment = {**os.environ, "TMPDIR": str(scratch), "PYTHONDONTWRITEBYTECODE": "1",
                   "PYTEST_ADDOPTS": "", "PRISMABUILD_ACTION_KEY": private["action_key"],
                   "PRISMABUILD_ACTION_ATTEMPT_NONCE": "f" * 32}
    completed = subprocess.run(private["task"]["argv"], cwd=checkout, env=environment,
                               capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    shutil.rmtree(scratch)
    assert destination.read_text() == "external"


@pytest.mark.parametrize("outcome,reader,retained", [
    ({"status": "failed"}, False, True),
    (None, False, True),
    ({"status": "executed", "returncode": 0}, True, True),
    ({"status": "executed", "returncode": 0}, False, False),
])
def test_generated_shard_scratch_follows_final_attempt_verdict(
        checkout, tmp_path, monkeypatch, outcome, reader, retained):
    import fcntl
    from prismabuild import _pbtest_layout

    # tmp_path's own failed-only policy is independent of wrapper cleanup.
    # Keep pytest-managed data so this assertion exercises PB's boundary.
    (checkout / "tests/test_one.py").write_text(
        "def test_one(tmp_path):\n"
        "    (tmp_path / 'final-verdict-evidence').write_text('retained')\n"
        "    frozen = tmp_path / 'readonly'\n"
        "    frozen.mkdir()\n"
        "    (frozen / 'data').write_text('owned')\n"
        "    frozen.chmod(0o555)\n")
    # A second lifetime owner must not register/sweep the inner root, even
    # after pytest succeeds. Only the outer attempt knows the final verdict.
    monkeypatch.setattr(pbtest, "ROOTS_PROGRAM", pbtest.ROOTS_PROGRAM + '''

def _unexpected_inner_retirement(*args, **kwargs):
    raise AssertionError("shared inner scratch entered standalone retirement")
_pb_temp_roots["register_pbtest_root"] = _unexpected_inner_retirement
_pb_owned_tree["reap_dead_trees"] = _unexpected_inner_retirement
''')
    action = pbtest._source_readonly_action(_action(
        checkout, arguments=("-o", "tmp_path_retention_policy=all")))

    descriptor = None
    try:
        with _pbtest_layout._attempt_environment(
                action, checkout_root=checkout,
                local_checkout_root=tmp_path / "local") as attempt:
            scratch = attempt.attempt
            env = {**os.environ, **attempt,
                   "PYTHONPATH": os.pathsep.join([str(ROOT / "src"),
                                                  str(ROOT / "tools/fleet")]),
                   "PRISMABUILD_ACTION_KEY": action["action_key"],
                   "PRISMABUILD_ACTION_ATTEMPT_NONCE": "f" * 32,
                   "PYTHONDONTWRITEBYTECODE": "1"}
            completed = subprocess.run(action["task"]["argv"], cwd=checkout, env=env,
                                       capture_output=True, text=True, timeout=30)
            assert completed.returncode == 0, completed.stdout + completed.stderr
            evidence = list(scratch.rglob("final-verdict-evidence"))
            assert len(evidence) == 1, "pytest success erased data before final verdict"
            assert evidence[0].read_text() == "retained"
            if reader:
                descriptor = os.open(scratch / ".readers", os.O_RDONLY)
                fcntl.flock(descriptor, fcntl.LOCK_SH)
            if outcome is not None:
                attempt.record_outcome(outcome)
        assert scratch.exists() is retained
        assert evidence[0].exists() is retained
        if outcome is not None:
            assert outcome["attempt_scratch"]["retained"] is retained
    finally:
        if descriptor is not None:
            os.close(descriptor)


@pytest.mark.parametrize("mode", ["tmpdir", "basetemp", "profile"])
def test_cli_custom_modes_warn_and_keep_the_private_recipe(
        tmp_path, monkeypatch, capsys, mode):
    from test_pbtest_identifies_its_checkout_once import _counted_run, _sealed

    custom = str(tmp_path / "caller-private")
    extra = {
        "tmpdir": ("--tmpdir", custom),
        "basetemp": ("--basetemp", custom),
        "profile": ("--profile", "sample"),
    }[mode]
    code, _, root = _counted_run(
        tmp_path, monkeypatch, shards=1, extra=("--source-readonly", *extra))
    actions = _sealed(root)
    assert code == 1  # publication-only fixture: no worker consumes the shard
    assert len(actions) == 1
    assert "execution_layout" not in actions[0]["params"]
    assert "using the private recipe" in capsys.readouterr().err


@pytest.mark.parametrize("destination", [None, "relative.log"])
def test_worker_capture_refuses_missing_or_relative_result_before_producer(
        tmp_path, destination):
    marker = tmp_path / "producer-ran"
    command = [sys.executable, "-c",
               "from pathlib import Path; Path(__import__('sys').argv[1]).touch()",
               str(marker)]
    argv = movement_actions._source_readonly_capture_argv(command, path_prefix="/usr/bin")
    env = dict(os.environ)
    env.pop(movement_actions._RESULT_PATH_ENV, None)
    if destination is not None:
        env[movement_actions._RESULT_PATH_ENV] = destination
    result = subprocess.run(argv, cwd=tmp_path, env=env, text=True, capture_output=True)
    assert result.returncode == 125
    assert "worker result path must be absolute" in result.stderr
    assert not marker.exists()
    assert not (tmp_path / "relative.log").exists()


@pytest.mark.parametrize("name", [
    movement_actions._PBTEST_HELPER_DIR_ENV,
    movement_actions._PBTEST_CACHE_DIR_ENV,
    movement_actions._PBTEST_SURFACE_DIR_ENV,
])
@pytest.mark.parametrize("kind", ["missing", "relative", "inside-source", "source-symlink"])
def test_bootstrap_refuses_nonprivate_directories_before_pytest(
        checkout, tmp_path, name, kind):
    env = dict(os.environ)
    env.update({
        movement_actions._PBTEST_HELPER_DIR_ENV: str(tmp_path / "helpers"),
        movement_actions._PBTEST_CACHE_DIR_ENV: str(tmp_path / "cache"),
        movement_actions._PBTEST_SURFACE_DIR_ENV: str(tmp_path / "surface"),
    })
    if kind == "missing":
        env.pop(name)
    elif kind == "relative":
        env[name] = "relative-worker-directory"
    elif kind == "inside-source":
        env[name] = str(checkout / "would-mutate-source")
    else:
        alias = tmp_path / "source-alias"
        alias.symlink_to(checkout, target_is_directory=True)
        env[name] = str(alias / "would-mutate-source")
    program = pbtest._SOURCE_READONLY_PROGRAM.replace("@ARG_START@", "1")
    program += '\nraise AssertionError("pytest would have started")\n'
    result = subprocess.run([sys.executable, "-c", program], cwd=checkout, env=env,
                            text=True, capture_output=True)
    assert result.returncode != 0
    assert "missing or non-private worker directory: " + name in result.stderr
    assert "pytest would have started" not in result.stderr
    assert not (checkout / "would-mutate-source").exists()
    assert not (checkout / "relative-worker-directory").exists()
