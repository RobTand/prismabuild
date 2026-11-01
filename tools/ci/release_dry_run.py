"""Build and inspect a release candidate in a disposable, remote-free repository."""
from __future__ import annotations

import argparse
import base64
import configparser
import csv
from email.parser import BytesParser
import importlib.metadata
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.digest_primitives import new_sha256, raw_sha256


def candidate_version(candidate: str, metadata: dict) -> str:
    version = metadata["project"]["version"]
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", candidate):
        raise ValueError("Candidate must be a stable vMAJOR.MINOR.PATCH tag")
    if candidate != f"v{version}":
        raise ValueError(f"Candidate {candidate} does not match project version {version}")
    return version


class _EntryPointsText(importlib.metadata.Distribution):
    """An entry_points.txt read the way importlib.metadata, and so pip, reads it."""

    def __init__(self, text: str) -> None:
        self._text = text

    def read_text(self, filename: str) -> str | None:
        return self._text if filename == "entry_points.txt" else None

    def locate_file(self, path):
        return path


def console_script(entry_points: str) -> str | None:
    """The target of the ``prismabuild`` command in ``[console_scripts]``, if any.

    Only that group installs a command. Installers read the file with different
    parsers, so both must find exactly one directly declared target:
    importlib.metadata's line reader (used by pip), and a strict ConfigParser
    that rejects duplicates. ConfigParser's [DEFAULT] inheritance is disabled
    with a default section name no header can declare: a header is one line.
    """
    try:
        installed = [entry.value for entry in _EntryPointsText(entry_points).entry_points
                     if entry.group == "console_scripts" and entry.name == "prismabuild"]
    except (TypeError, ValueError):
        return None
    parser = configparser.ConfigParser(interpolation=None, delimiters=("=",), default_section="\n")
    parser.optionxform = str
    try:
        parser.read_string(entry_points)
    except configparser.Error:
        return None
    if not parser.has_option("console_scripts", "prismabuild"):
        return None
    target = parser.get("console_scripts", "prismabuild").strip()
    return target if installed == [target] else None


def validate_wheel(wheel: Path, version: str) -> dict:
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(set(names)) != len(names):
            raise ValueError("Wheel contains duplicate paths")
        metadata_paths = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise ValueError("Wheel must contain exactly one METADATA file")
        metadata_path = metadata_paths[0]
        metadata = BytesParser().parsebytes(archive.read(metadata_path))
        if metadata["Name"] != "prismabuild" or metadata["Version"] != version:
            raise ValueError("Wheel name or version does not match the candidate")
        prefix = metadata_path.removesuffix("METADATA")
        if console_script(archive.read(prefix + "entry_points.txt").decode()) != "prismabuild.core:main":
            raise ValueError("Wheel is missing the prismabuild CLI entry point")
        if not {"prismabuild/__init__.py", "prismabuild/core.py"}.issubset(names):
            raise ValueError("Wheel is missing the PrismaBuild package")
        record_path = prefix + "RECORD"
        rows = list(csv.reader(io.StringIO(archive.read(record_path).decode())))
        if any(len(row) != 3 for row in rows):
            raise ValueError("Malformed wheel RECORD")
        if len({row[0] for row in rows}) != len(rows) or {row[0] for row in rows} != set(names):
            raise ValueError("Wheel RECORD does not cover every file exactly once")
        for name, digest, size in rows:
            if name == record_path:
                if digest or size:
                    raise ValueError("Wheel RECORD must not hash itself")
                continue
            content = archive.read(name)
            expected = "sha256=" + base64.urlsafe_b64encode(new_sha256(content).digest()).rstrip(b"=").decode()
            if digest != expected or size != str(len(content)):
                raise ValueError(f"Wheel RECORD mismatch: {name}")
    return {"wheel": wheel.name, "wheel_sha256": raw_sha256(wheel.read_bytes()),
            "wheel_files": len(names)}


def dry_run_release(source: Path, revision: str, candidate: str) -> dict:
    with tempfile.TemporaryDirectory(prefix="prismabuild-release-") as temporary:
        scratch = Path(temporary)
        home = scratch / "home"
        home.mkdir()
        environment = {
            "PATH": os.defpath, "HOME": str(home), "LANG": "C.UTF-8",
            "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0", "OMP_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
        }

        def command(arguments: list[str], cwd: Path) -> bytes:
            result = subprocess.run(arguments, cwd=cwd, env=environment, capture_output=True, timeout=180)
            if result.returncode:
                raise ValueError(f"Command failed ({result.returncode}): {arguments[0]}\n"
                                 + result.stderr.decode(errors="replace"))
            return result.stdout

        source_sha = command(["git", "rev-parse", "--verify", "--end-of-options", f"{revision}^{{commit}}"], source).decode().strip()
        contents = command(["git", "archive", "--format=tar", source_sha], source)
        checkout = scratch / "candidate"
        checkout.mkdir()
        with tarfile.open(fileobj=io.BytesIO(contents)) as archive:
            archive.extractall(checkout, filter="data")
        metadata = tomllib.loads((checkout / "pyproject.toml").read_text())
        version = candidate_version(candidate, metadata)
        command(["git", "init", "--quiet"], checkout)
        command(["git", "add", "."], checkout)
        command(["git", "-c", "user.name=Release dry-run", "-c", "user.email=dry-run@invalid",
                 "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", f"Source {source_sha}"], checkout)
        command(["git", "-c", "tag.gpgsign=false", "tag", candidate], checkout)
        tag_sha = command(["git", "rev-parse", f"refs/tags/{candidate}^{{commit}}"], checkout).decode().strip()
        if command(["git", "remote"], checkout).strip():
            raise ValueError("Dry-run repository must have no remotes")
        output = scratch / "dist"
        output.mkdir()
        command([sys.executable, "-I", "-c",
                 "import setuptools.build_meta, sys; setuptools.build_meta.build_wheel(sys.argv[1])",
                 str(output)], checkout)
        wheels = list(output.glob("*.whl"))
        if len(wheels) != 1:
            raise ValueError("Build must produce exactly one wheel")
        return {"outcome": "PASS", "source_sha": source_sha, "candidate": candidate,
                "version": version, "local_tag_commit": tag_sha, "remotes": [],
                **validate_wheel(wheels[0], version)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate")
    parser.add_argument("--source", type=Path, default=Path.cwd())
    parser.add_argument("--revision", default="HEAD")
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    try:
        report = dry_run_release(arguments.source.resolve(), arguments.revision, arguments.candidate)
    except (ValueError, OSError, KeyError, zipfile.BadZipFile, subprocess.TimeoutExpired) as error:
        report = {"outcome": "FAIL", "candidate": arguments.candidate, "reason": str(error)}
    rendered = json.dumps(report, indent=2) + "\n"
    if arguments.report:
        arguments.report.write_text(rendered)
    print(rendered, end="")
    return 0 if report["outcome"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
