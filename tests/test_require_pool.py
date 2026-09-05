"""The hook's two failure modes are both "it refused something it must not".

It has now had two of them in one day -- it refused the commit whose message
quoted its own pattern, and it refused the command that *starts the pool* --
so the carve-outs get tests rather than trust.
"""
import json
import subprocess
import sys
from pathlib import Path

HOOK = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "require_pool.py"
CUDA = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python"


def _run(command, *, flag: Path):
    event = json.dumps({"tool_input": {"command": command}})
    proc = subprocess.run(
        [sys.executable, str(HOOK)], input=event, text=True,
        capture_output=True, timeout=30,
        env={"PATH": "/usr/bin:/bin", "REQUIRE_POOL_FLAG": str(flag)},
    )
    return proc.returncode, proc.stderr


def _armed(tmp_path, monkeypatch):
    """The hook reads a fixed flag path; point the module at a temp one."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("require_pool", HOOK)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    flag = tmp_path / "on"
    flag.write_text("")
    module.FLAG = flag
    return module


def _verdict(module, command):
    import io
    import contextlib
    stdin = io.StringIO(json.dumps({"tool_input": {"command": command}}))
    err = io.StringIO()
    old = sys.stdin
    sys.stdin = stdin
    try:
        with contextlib.redirect_stderr(err):
            return module.main()
    finally:
        sys.stdin = old


def test_disarmed_hook_allows_everything(tmp_path):
    module = _armed(tmp_path, None)
    module.FLAG = tmp_path / "absent"
    assert _verdict(module, f"{CUDA} -m pytest tests") == 0


def test_armed_hook_refuses_the_cuda_interpreter(tmp_path):
    module = _armed(tmp_path, None)
    assert _verdict(module, f"{CUDA} -m pytest tests") == 2


def test_armed_hook_refuses_a_bare_flock_on_the_gpu_lock(tmp_path):
    # The jam re-formed from direct flock calls naming no wrapper, so the
    # wrapper names alone would have left the observed path open.
    module = _armed(tmp_path, None)
    assert _verdict(module, "flock /home/rob/tmp/arb/.gpu.lock some-command") == 2


def test_it_never_refuses_the_pool_being_started(tmp_path):
    # A worker is configured with --python <the CUDA venv>, so it names the
    # refused pattern by construction.  Refusing it means the hook blocks the
    # only alternative it offers.
    module = _armed(tmp_path, None)
    command = (
        "cd /mnt/shared/prismabuild-fleet && setsid nohup /usr/bin/python3 "
        f"repo/tools/worker_loop.py --gpu-slots 1 --python {CUDA} &"
    )
    assert _verdict(module, command) == 0


def test_it_never_refuses_a_submission_to_the_pool(tmp_path):
    module = _armed(tmp_path, None)
    assert _verdict(
        module,
        f"/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py "
        f"--gpu -- {CUDA} -m pytest tests",
    ) == 0


def test_it_never_refuses_prose_about_the_rule(tmp_path):
    # A commit message quoting the pattern is prose ABOUT the rule, not an
    # instance of it.  This is the lockout that already happened once.
    module = _armed(tmp_path, None)
    assert _verdict(module, f"git commit -m 'stop calling {CUDA} directly'") == 0
    assert _verdict(module, f"gh issue comment 5 -b 'we ran {CUDA} -m pytest'") == 0


def test_leading_env_assignments_do_not_hide_the_command(tmp_path):
    module = _armed(tmp_path, None)
    assert _verdict(module, f"TMPDIR=/home/rob/tmp git commit -m '{CUDA}'") == 0
    assert _verdict(module, f"TMPDIR=/home/rob/tmp {CUDA} -m pytest") == 2


def test_read_only_inspection_stays_allowed(tmp_path):
    # Refusing nvidia-smi would only teach agents to route around the hook.
    module = _armed(tmp_path, None)
    assert _verdict(module, "nvidia-smi --query-gpu=memory.used --format=csv") == 0


def test_a_compound_command_is_judged_segment_by_segment(tmp_path):
    # The hole: judging by the first token alone lets an exempt leader vouch
    # for everything after it.  This is how a CUDA pytest ran unrefused while
    # the hook was armed.
    module = _armed(tmp_path, None)
    assert _verdict(module, f"cat notes.txt && {CUDA} -m pytest tests") == 2
    assert _verdict(module, f"echo starting; {CUDA} -m pytest tests") == 2
    assert _verdict(module, f"git log -1 | {CUDA} -m pytest tests") == 2


def test_an_exempt_segment_is_still_exempt_beside_a_refused_shape(tmp_path):
    # The converse must hold too, or the fix trades one over-broad rule for
    # another: a git commit quoting the pattern stays allowed even when it
    # shares a line with other commands.
    module = _armed(tmp_path, None)
    assert _verdict(module, f"cd /tmp && git commit -m 'do not call {CUDA}'") == 0
    assert _verdict(
        module,
        f"cd /mnt/shared && /usr/bin/python3 repo/tools/pbrun.py --gpu -- "
        f"{CUDA} -m pytest && echo done",
    ) == 0


def test_a_line_continuation_is_not_a_command_boundary(tmp_path):
    # Splitting on the raw newline tears one command into pieces and strips
    # each piece of the context that exempts it.  This shape -- a worker
    # launch whose --python argument sits on its own continued line -- was
    # refused by the first version of the segment fix.
    module = _armed(tmp_path, None)
    command = (
        "cd /mnt/shared/prismabuild-fleet\n"
        "setsid nohup /usr/bin/python3 repo/tools/worker_loop.py \\\n"
        "  --gpu-slots 1 --mem-gb 40 \\\n"
        f"  --python {CUDA} \\\n"
        "  >> /home/rob/tmp/worker.log 2>&1 &"
    )
    assert _verdict(module, command) == 0


def test_a_continuation_does_not_smuggle_gpu_work_past_the_hook(tmp_path):
    # The converse: joining continuations must not make a refused command
    # look exempt by gluing it onto a permitted neighbour.
    module = _armed(tmp_path, None)
    command = f"echo starting\n{CUDA} \\\n  -m pytest tests"
    assert _verdict(module, command) == 2


def test_pool_payload_is_not_segmented_away_from_its_entrypoint(tmp_path, monkeypatch):
    """A pbrun submission whose payload chains commands is still a submission.

    Splitting on ``&&`` tore the payload's interpreter away from the ``pbrun.py``
    that vouches for it, so the hook refused the exact command it tells callers
    to run -- the third time it has locked out its own repair.
    """
    module = _armed(tmp_path, monkeypatch)
    cuda = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python"
    submit = (
        "/usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py --gpu -- "
        f"bash -lc 'cd /home/rob/tmp/ts50 && {cuda} -m pytest tests/ -q'"
    )
    assert _verdict(module, submit) == 0


def test_a_chain_after_a_pool_command_with_no_payload_is_still_scanned(tmp_path, monkeypatch):
    """The cut needs BOTH an entrypoint and a ``--``; without one, nothing is exempt."""
    module = _armed(tmp_path, monkeypatch)
    cuda = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python"
    assert _verdict(module, f"pbrun.py --help && {cuda} train.py") != 0


def test_hook_process_reads_stdin_flag_and_reports_refusal(tmp_path):
    flag = tmp_path / "armed"
    flag.write_text("")
    code, stderr = _run(f"{CUDA} train.py", flag=flag)
    assert code == 2
    assert "Refused: GPU work goes through" in stderr
    assert "pbrun.py" in stderr
    code, stderr = _run("nvidia-smi", flag=flag)
    assert (code, stderr) == (0, "")


def test_hook_process_with_absent_flag_allows_work(tmp_path):
    assert _run(f"{CUDA} train.py", flag=tmp_path / "absent") == (0, "")


def test_hook_process_ignores_malformed_event(tmp_path):
    flag = tmp_path / "armed"
    flag.write_text("")
    proc = subprocess.run(
        [sys.executable, str(HOOK)], input="{broken", text=True,
        capture_output=True,
        env={"PATH": "/usr/bin:/bin", "REQUIRE_POOL_FLAG": str(flag)},
        timeout=30,
    )
    assert (proc.returncode, proc.stdout, proc.stderr) == (0, "", "")
