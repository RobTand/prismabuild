"""Opus review r4: run the branch's own GPU suite against one mutant.

Each mutant is applied to a private copy of this checkout (src, tools and tests
copied; everything else symlinked). The suite the builder ships must fail for
a live second judgment route. Mutants call ``_r4_mark`` when they changed an
outcome, so a survivor that changed outcomes is told apart from a mutant the
suite never exercised. Children of an admitted action run directly.
"""
import os
import warnings
import shutil
import subprocess
import sys
from pathlib import Path
import prismabuild.core as pb
REPO = Path(pb.__file__).resolve().parents[2]
SUITE = ['tests/test_gpu_review_r4.py', 'tests/test_gpu_frozen_record_1744.py', 'tests/test_gpu_review_r3.py', 'tests/test_gpu_review_r2.py', 'tests/test_gpu_closure_1744.py', 'tests/test_gpu_attempt_history_1744.py', 'tests/test_pbstatus.py', 'tests/test_pbmcp_tools.py', 'tests/test_gpu_review_r5.py']
COPIED = {'src', 'tools', 'tests', 'conftest.py', 'pyproject.toml'}
MARK = '\n\ndef _r4_mark(name):\n    import os as _os\n    path = _os.environ.get("R4_MUTANT_FIRED")\n    if path:\n        with open(path, "a") as out:\n            out.write(name + "\\n")\n'
LIFECYCLE = 'src/prismabuild/lifecycle.py'
ENDINGS = 'src/prismabuild/endings.py'
CENSUS = 'src/prismabuild/census.py'
POOL = 'src/prismabuild/pool.py'
CORE = 'src/prismabuild/core.py'
BROKER = 'tools/fleet/resource_broker.py'
PBSTATUS = 'tools/fleet/pbstatus.py'
LIFECYCLE_ANCHOR = '    observation = pb.GpuUseJudgment.require(observation)\n'
ENDINGS_ANCHOR = '        gpu_judgment = pb.GpuUseJudgment.require(pb.gpu_execution_judgment(observation.get("gpu_record"),\n            nonce=observation.get("nonce"), scope_id=observation.get("scope_id")))\n'
ENDINGS_OVERWRITE = '        detail.update(resource_telemetry=gpu_judgment.as_dict(), gpu_use=gpu_judgment["gpu_use"],\n                      gpu_use_reason=gpu_judgment["gpu_use_reason"])\n'
ENDINGS_REFUSAL = '    gpu_refusal_final = (gpu_judgment is not None and gpu_judgment["gpu_use"] == "zero")\n'
CENSUS_ANCHOR = '        "gpu_use": judgment["gpu_use"] if judgment is not None else None,\n'
CENSUS_JUDGMENT = '                    if isinstance(observation, dict) and ("gpu_use" in observation or "gpu_record" in observation)\n                    else None)\n'
POOL_ANCHOR = '            if status.get("gpu_record") is not None:\n                telemetry["gpu_record"] = status["gpu_record"]\n'
DEMAND_ANCHOR = '        return pb.normalized_resource_demand(raw)\n'
RETAIN_ANCHOR = "current.get('gpu_record') is not None"
PBSTATUS_ANCHOR = '    judgment = pb.GpuUseJudgment.require(judgment)\n'
CORE_USED = '        state, reason = "used", "attributed_gpu_uuid"\n'
ZERO_EVIDENCE = '{"sample_failed": False, "live_sample": True, "final_succeeded": True}'
PLAIN_ZERO = '{**observation, "gpu_use": "zero", "gpu_use_reason": "successful_payload_sample", "gpu_devices_used": 0}'

def _lifecycle_condition(condition, body):
    return (LIFECYCLE, LIFECYCLE_ANCHOR, '    if ' + condition + ':\n' + body + LIFECYCLE_ANCHOR)
LAUNDER_LIFECYCLE = '        try:\n            _new = pb.gpu_execution_judgment({"action_key": str(record["action_key"]),\n                "nonce": scope.get("nonce"), "scope_id": scope.get("scope_id"), "deadline_monotonic": 0.0,\n                "resource_telemetry": {"gpu_uuids": [], "gpu_evidence": ' + ZERO_EVIDENCE + '}})\n            if _new["gpu_use"] != observation["gpu_use"]:\n                _r4_mark("lifecycle")\n            observation = _new\n        except Exception:\n            pass\n'
PLAIN_LIFECYCLE = '        _r4_mark("lifecycle_plain")\n        observation = ' + PLAIN_ZERO + '\n'
MARK_LIFECYCLE = (LIFECYCLE, None, MARK)
CONDITIONS = {'live_key_local_variable': 'observation["gpu_use"] == "unobserved" and telemetry.get("gpu_devices_used") == 0', 'live_key_getattr': 'observation["gpu_use"] == "unobserved" and getattr(telemetry, "get")("gpu_devices_used") == 0', 'computed_banned_keys': 'observation["gpu_use"] == "unobserved" and not telemetry.get("gpu_" + "uuids") and (telemetry.get("gpu_" + "evidence") or {}).get("live_" + "sample")'}
MUTANTS = {}
for _name, _condition in CONDITIONS.items():
    MUTANTS[_name + '-laundered'] = [_lifecycle_condition(_condition, LAUNDER_LIFECYCLE), MARK_LIFECYCLE]
    MUTANTS[_name + '-plain'] = [_lifecycle_condition(_condition, PLAIN_LIFECYCLE), MARK_LIFECYCLE]
MUTANTS['second_judgment_function-laundered'] = [(ENDINGS, ENDINGS_ANCHOR, ENDINGS_ANCHOR + '        if gpu_judgment["gpu_use"] == "unobserved" and not observation.get("gpu_uuids"):\n            try:\n                _new = pb.gpu_execution_judgment({"action_key": str(adopted.get("action_key")),\n                    "nonce": observation.get("nonce"), "scope_id": observation.get("scope_id"),\n                    "deadline_monotonic": 0.0, "resource_telemetry": {"gpu_uuids": [],\n                    "gpu_evidence": ' + ZERO_EVIDENCE + '}})\n                if _new["gpu_use"] != gpu_judgment["gpu_use"]:\n                    _r4_mark("endings")\n                gpu_judgment = _new\n            except Exception:\n                pass\n'), (ENDINGS, None, MARK)]
MUTANTS['second_judgment_function-plain'] = [(ENDINGS, ENDINGS_ANCHOR, ENDINGS_ANCHOR + '        if gpu_judgment["gpu_use"] == "unobserved" and not observation.get("gpu_uuids"):\n            _r4_mark("endings_plain")\n            gpu_judgment = {**gpu_judgment.as_dict(), "gpu_use": "zero", "gpu_devices_used": 0}\n'), (ENDINGS, None, MARK)]
MUTANTS['unscanned_pool_local_receiver'] = [(POOL, POOL_ANCHOR, POOL_ANCHOR + '            elif status.get("resource_telemetry"):\n                _live = status["resource_telemetry"]\n                _evidence = _live.get("gpu_evidence") or {}\n                telemetry["gpu_record"] = {"action_key": scope.action_key, "nonce": scope.nonce,\n                    "scope_id": getattr(scope, "scope_id", None), "deadline_monotonic": 0.0,\n                    "resource_telemetry": {"gpu_uuids": sorted(set(_live.get("gpu_uuids") or [])),\n                    "gpu_evidence": {"sample_failed": bool(_evidence.get("sample_failed")),\n                    "live_sample": bool(_evidence.get("live_sample")), "final_succeeded": True}}}\n                _r4_mark("pool_local")\n'), (POOL, None, MARK)]
MUTANTS['unscanned_pool_self_receiver'] = [(POOL, DEMAND_ANCHOR, DEMAND_ANCHOR + '\n    def _r4_live_record(self, record, telemetry):\n        scope = record.get("resource_scope") or {}\n        return {"action_key": str(record["action_key"]), "nonce": scope.get("nonce"),\n                "scope_id": scope.get("scope_id"), "deadline_monotonic": 0.0,\n                "resource_telemetry": {"gpu_uuids": sorted(set((telemetry or {}).get("gpu_uuids") or [])),\n                "gpu_evidence": ' + ZERO_EVIDENCE + '}}\n'), _lifecycle_condition('observation["gpu_use"] == "unobserved" and hasattr(queue, "_r4_live_record")', '        try:\n            _new = pb.gpu_execution_judgment(queue._r4_live_record(record, telemetry))\n            if _new["gpu_use"] != observation["gpu_use"]:\n                _r4_mark("pool_self")\n            observation = _new\n        except Exception:\n            pass\n'), MARK_LIFECYCLE]
MUTANTS['census_unbanned_key-plain'] = [(CENSUS, CENSUS_ANCHOR, '        "gpu_use": ((_r4_mark("census_plain") or ("used" if observation.get("gpu_devices_used") else "zero"))\n                    if judgment is not None else None),\n'), (CENSUS, None, MARK)]
MUTANTS['census_unbanned_key-laundered'] = [(CENSUS, CENSUS_JUDGMENT, CENSUS_JUDGMENT + '        if judgment is not None and judgment["gpu_use"] == "unobserved" and observation.get("nonce"):\n            try:\n                _new = pb.gpu_execution_judgment({"action_key": str(record.get("action_key")),\n                    "nonce": observation["nonce"], "scope_id": observation["scope_id"], "deadline_monotonic": 0.0,\n                    "resource_telemetry": {"gpu_uuids": [], "gpu_evidence": ' + ZERO_EVIDENCE + '    }})\n                if _new["gpu_use"] != judgment["gpu_use"]:\n                    _r4_mark("census")\n                judgment = _new\n            except Exception:\n                pass\n'), (CENSUS, None, MARK)]
MUTANTS['retain_check_satisfied_by_comment'] = [(BROKER, RETAIN_ANCHOR, 'False  # gpu_record: see freeze')]
MUTANTS['leaked_authority_type_of_instance'] = [_lifecycle_condition(CONDITIONS['live_key_local_variable'], '        _r4_mark("leaked_type")\n        observation = type(observation)({**observation.as_dict(), "gpu_use": "zero",\n            "gpu_use_reason": "successful_payload_sample", "gpu_devices_used": 0},\n            _authority=observation._authority)\n'), MARK_LIFECYCLE]
MUTANTS['leaked_authority_object_new'] = [(ENDINGS, ENDINGS_ANCHOR, ENDINGS_ANCHOR + '        if gpu_judgment["gpu_use"] == "unobserved" and not observation.get("gpu_uuids"):\n            _forged = object.__new__(type(gpu_judgment))\n            object.__setattr__(_forged, "_encoded", pb._canonical_bytes({**gpu_judgment.as_dict(),\n                "gpu_use": "zero", "gpu_devices_used": 0}))\n            object.__setattr__(_forged, "_authority", gpu_judgment._authority)\n            _r4_mark("leaked_object_new")\n            gpu_judgment = _forged\n'), (ENDINGS, None, MARK)]
MUTANTS['duck_pbstatus_accepts_any_mapping'] = [(PBSTATUS, PBSTATUS_ANCHOR, '')]
MUTANTS['duck_census_projects_stored_string'] = [(CENSUS, CENSUS_ANCHOR, '        "gpu_use": detail.get("gpu_use"),\n')]
MUTANTS['duck_endings_keeps_stored_projection'] = [(ENDINGS, ENDINGS_OVERWRITE, '        pass\n')]
MUTANTS['duck_endings_routes_on_stored_reason'] = [(ENDINGS, ENDINGS_REFUSAL, '    gpu_refusal_final = raw_detail.get("termination_reason") == "zero_gpu_devices_used"\n')]
CANARY = [(CORE, CORE_USED, '        state, reason = "zero", "successful_payload_sample"\n')]

def _tree(tmp):
    root = tmp / 'mut'
    root.mkdir()
    for entry in REPO.iterdir():
        if entry.name in {'.git', '.pytest_cache'}:
            continue
        target = root / entry.name
        if entry.name not in COPIED:
            target.symlink_to(entry)
        elif entry.is_dir():
            ignore = shutil.ignore_patterns('__pycache__', *(['compat'] if entry.name == 'tests' else []))
            shutil.copytree(entry, target, symlinks=True, ignore=ignore)
        else:
            shutil.copy2(entry, target)
    (root / 'tests' / 'compat').symlink_to(REPO / 'tests' / 'compat')
    return root

def _apply(root, edits):
    for rel, anchor, replacement in edits:
        path = root / rel
        text = path.read_text()
        if anchor is None:
            text += replacement
        else:
            assert text.count(anchor) == 1, f'stale mutant anchor in {rel}: {anchor[:70]!r}'
            text = text.replace(anchor, replacement)
        compile(text, rel, 'exec')
        path.write_text(text)

def run_suite(tmp, edits, *, timeout=1500):
    root = _tree(tmp)
    _apply(root, edits)
    fired = tmp / 'fired'
    env = {key: value for key, value in os.environ.items() if not key.startswith('PYTEST_')}
    env.update(R4_MUTANT_FIRED=str(fired), PYTHONDONTWRITEBYTECODE='1', PYTHONPATH=os.pathsep.join([str(root / 'src'), str(root / 'tools' / 'fleet')]))
    started = __import__('time').monotonic()
    proc = subprocess.run([sys.executable, '-m', 'pytest', '-x', '-p', 'no:cacheprovider', '--tb=line', '-rf', '-o', 'tmp_path_retention_policy=none', '--basetemp', str(tmp / 'bt'), *SUITE], cwd=root, env=env, capture_output=True, text=True, timeout=timeout)
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    summary = [line for line in lines if line.startswith(('FAILED', 'ERROR'))][:3] + lines[-1:]
    marks = sorted(set(fired.read_text().split())) if fired.exists() else []
    elapsed = round(__import__('time').monotonic() - started, 1)
    return (proc.returncode, summary, marks, elapsed)

def check_caught(tmp, name):
    rc, summary, marks, elapsed = run_suite(tmp, MUTANTS[name])
    warnings.warn(f'R4-MUTANT {name}: rc={rc} elapsed={elapsed}s marks={marks} summary={summary}')
    assert rc == 1, f'mutant {name} SURVIVED the branch GPU suite (rc={rc}); outcome-changing marks={marks}; summary={summary}'
