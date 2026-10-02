"""#1430: missing seal verbs and the distinct binary-streaming pack boundary."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import pytest  # noqa: E402

import pbrun  # noqa: E402
from prismabuild import core as pb  # noqa: E402


@pytest.mark.parametrize("argv", (
    ["config", "--get", "core.autocrlf"],
    ["ls-tree", "-r", "-l", "-z", "fixture-tree"],
    ["hash-object", "-w", "--stdin", "--no-filters"],
    ["update-index", "--add", "--cacheinfo", "100644,fixture-blob,stamp"],
    ["init", "-q", "--bare", "fixture.git"],
    ["check-ref-format", "--branch", "fixture"],
    ["--git-dir=fixture.git", "update-ref", "refs/heads/fixture", "fixture-commit"],
))
@pytest.mark.parametrize("transport", (False, True))
def test_remaining_snapshot_verbs_identify_operation(monkeypatch, tmp_path, argv, transport):
    def failed(*args, **kwargs):
        if transport:
            raise OSError("controlled fault")
        return subprocess.CompletedProcess(args, 17, stdout="", stderr="controlled fault\n")

    monkeypatch.setattr(pb, "_git_run", failed)
    with pytest.raises(SystemExit) as caught:
        pbrun._snapshot_git(tmp_path, argv)
    assert str(caught.value) == f"pbrun: cannot snapshot checkout: Git {' '.join(argv)} failed: controlled fault"


@pytest.mark.parametrize("failure,detail", (
    ("nonzero", "pack fault\ufffd"),
    ("empty", "17"),
    ("transport", "pack transport fault"),
    ("timeout", None),
    ("success", None),
))
@pytest.mark.parametrize("object_format", ("sha1", "sha256"))
def test_deterministic_bundle_streams_bytes_and_names_pack_failure(
    monkeypatch, tmp_path, failure, detail, object_format,
):
    git_dir = tmp_path / "private.git"
    bundle = tmp_path / "checkout.bundle"
    environment = {"GIT_OBJECT_DIRECTORY": "private-objects"}
    refs = (("refs/heads/prismabuild-snapshot", "a" * 40), ("refs/heads/extra", "b" * 40))
    argv = pbrun.deterministic_bundle_argv(git_dir)
    header = ("# v2 git bundle\n" if object_format == "sha1" else
              f"# v3 git bundle\n@object-format={object_format}\n")
    header += "".join(f"{oid} {name}\n" for name, oid in refs) + "\n"
    pack = b"PACK\x00\xff\x80\x00binary\n"
    transport_error = (
        OSError(detail) if failure == "transport" else
        subprocess.TimeoutExpired(argv, pbrun.BUNDLE_PACK_TIMEOUT_S)
        if failure == "timeout" else None
    )

    def format_read(root, *args, **kwargs):
        assert root == tmp_path
        assert args == (f"--git-dir={git_dir}", "rev-parse", "--show-object-format")
        return subprocess.CompletedProcess(args, 0, stdout=object_format + "\n", stderr="")

    def run_pack(actual_argv, **kwargs):
        assert actual_argv == argv
        assert kwargs.keys() == {"cwd", "env", "input", "stdout", "stderr", "timeout"}
        assert kwargs["cwd"] == str(tmp_path)
        assert kwargs["env"] == environment
        assert kwargs["input"] == b"a" * 40 + b"\n" + b"b" * 40 + b"\n"
        assert kwargs["stderr"] == subprocess.PIPE
        assert kwargs["timeout"] == pbrun.BUNDLE_PACK_TIMEOUT_S == 1800
        assert bundle.read_bytes() == header.encode("utf-8")  # header flushed first
        kwargs["stdout"].write(pack)  # never text-capture the binary pack
        if transport_error is not None:
            raise transport_error
        return subprocess.CompletedProcess(actual_argv, 0 if failure == "success" else 17,
                                           stdout=None, stderr=b"pack fault\xff\n" if failure == "nonzero" else b"")

    monkeypatch.setattr(pb, "_git_run", format_read)
    monkeypatch.setattr(subprocess, "run", run_pack)
    if failure == "success":
        pbrun.write_deterministic_bundle(tmp_path, bundle, refs, git_dir=git_dir, environment=environment)
    else:
        with pytest.raises(SystemExit) as caught:
            pbrun.write_deterministic_bundle(tmp_path, bundle, refs, git_dir=git_dir, environment=environment)
        assert caught.value.__cause__ is transport_error
        assert caught.value.__suppress_context__ is (transport_error is not None)
        message = str(caught.value)
        assert message.startswith(f"pbrun: cannot snapshot checkout: Git {' '.join(argv[1:])} failed: ")
        assert ("timed out after 1800" if failure == "timeout" else detail) in message
    assert bundle.read_bytes() == header.encode("utf-8") + pack


@pytest.mark.parametrize("timeout", (False, True))
@pytest.mark.parametrize("inside_handler", (False, True))
def test_snapshot_text_transport_preserves_explicit_cause(
    monkeypatch, tmp_path, timeout, inside_handler,
):
    argv = ["rev-parse", "HEAD"]
    transport_error = (
        subprocess.TimeoutExpired(["git", *argv], 120) if timeout else
        OSError("text transport fault")
    )

    def failed(*args, **kwargs):
        raise transport_error

    def invoke():
        with pytest.raises(SystemExit) as caught:
            pbrun._snapshot_git(tmp_path, argv)
        return caught.value

    monkeypatch.setattr(pb, "_git_run", failed)
    if inside_handler:
        try:
            raise OSError("unrelated caller context")
        except OSError:
            refusal = invoke()
    else:
        refusal = invoke()
    assert refusal.__cause__ is transport_error
    assert refusal.__context__ is transport_error
    assert refusal.__suppress_context__ is True
    assert str(refusal) == (
        f"pbrun: cannot snapshot checkout: Git {' '.join(argv)} failed: {transport_error}"
    )


def test_snapshot_nonzero_does_not_promote_callers_exception_to_cause(monkeypatch, tmp_path):
    def failed(*args, **kwargs):
        return subprocess.CompletedProcess(args, 17, stdout="", stderr="nonzero fault\n")

    monkeypatch.setattr(pb, "_git_run", failed)
    caller_error = OSError("unrelated caller context")
    try:
        raise caller_error
    except OSError:
        with pytest.raises(SystemExit) as caught:
            pbrun._snapshot_git(tmp_path, ["rev-parse", "HEAD"])
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is caller_error
    assert caught.value.__suppress_context__ is False
    assert str(caught.value) == "pbrun: cannot snapshot checkout: Git rev-parse HEAD failed: nonzero fault"
