"""Standalone, CAS-bound D38 producer. Executes under Python isolated mode."""
from __future__ import annotations

import argparse
import datetime
import json
import runpy
from types import SimpleNamespace
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys

MAX_BYTES = 64 * 1024


def strict_json(raw: bytes):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result
    def constant(value):
        raise ValueError(f"non-finite JSON value: {value}")
    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)


def bounded_json(path: Path) -> tuple[object, bytes]:
    with path.open("rb") as handle:
        raw = handle.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("D38 evidence exceeds 64 KiB")
    return strict_json(raw), raw


def run_preflight(plan_path: str, plan_sha256: str, result_path: str, *, digests=None) -> dict:
    if digests is None:
        from . import digest_primitives as digests
    output = Path(result_path)
    if result_path != ".d38/receipt.json":
        raise ValueError("the D38 receipt path is not the sealed producer path")
    if output.parent.is_symlink() or not output.parent.resolve().is_relative_to(Path.cwd().resolve()):
        raise ValueError("the D38 receipt directory leaves the checkout")
    output.unlink(missing_ok=True)
    plan, raw = bounded_json(Path(plan_path))
    if digests.raw_sha256(raw) != plan_sha256:
        raise ValueError("the D38 plan bytes differ from the sealed digest")
    if not isinstance(plan, dict) or plan.get("schema") != "fleet.d38.plan.v1":
        raise ValueError("the D38 plan schema is invalid")
    target = plan["target_action"]
    if (target["action_key"] != digests.canonical_sha256({key: value for key, value in target.items() if key != "action_key"})
            or plan["target"]["job_identity_hash"] != target["action_key"]):
        raise ValueError("the target identity does not cover the sealed target")
    descriptor = plan["descriptor"]
    overrides = sorted(name for name in os.environ
        if name in {"PRISMABUILD_DOCKER_TESTING", "BASH_ENV", "ENV"}
        or name.startswith("BASH_FUNC_") or name.startswith("LD_"))
    if overrides:
        raise ValueError("the CPU producer forbids execution overrides: " + ", ".join(overrides))
    docker_name = shutil.which("docker")
    if docker_name is None:
        raise ValueError("the reviewed Docker shim is unavailable")
    docker = Path(docker_name).resolve(strict=True)
    if (not docker.is_file() or not os.access(docker, os.X_OK)
            or digests.raw_sha256(docker.read_bytes()) != descriptor["docker_sha256"]):
        raise ValueError("the Docker executable differs from the reviewed shim")
    source = Path(descriptor["source"])
    if (source.is_symlink() or not source.is_file()
            or digests.raw_sha256(source.read_bytes()) != descriptor["source_sha256"]):
        raise ValueError("the harness source differs from the reviewed source")
    owner = Path(descriptor["digest_source"])
    if (owner.is_symlink() or not owner.is_file()
            or digests.raw_sha256(owner.read_bytes()) != descriptor["digests_sha256"]):
        raise ValueError("the snapshot digest owner differs from the reviewed source")
    if os.environ.get("CUDA_VISIBLE_DEVICES") != "" or os.environ.get("NVIDIA_VISIBLE_DEVICES") != "none":
        raise ValueError("the CPU producer did not hide CUDA devices")
    machine = platform.machine().lower()
    if machine not in ("aarch64", "arm64"):
        raise ValueError("the ARM64 canary image needs a GB10 CPU host")
    key = os.environ.get("PRISMABUILD_ACTION_KEY", "")
    if re.fullmatch(r"[0-9a-f]{64}", key) is None:
        raise ValueError("the producer has no executing action identity")
    checks_path = Path(descriptor["checks_path"])
    checks_path.unlink(missing_ok=True)
    completed = subprocess.run(plan["cpu_command"], check=False)
    if completed.returncode:
        raise ValueError(f"the CPU canary failed with exit {completed.returncode}")
    checks, _ = bounded_json(checks_path)
    expected = {**descriptor["completion"],
                "inputs": [item["id"] for item in plan["namespace_descriptor"]["prerequisites"]],
                "image": plan["target"]["image_digest"][0],
                "source_sha256": descriptor["source_sha256"]}
    if checks != expected:
        raise ValueError("the CPU canary did not complete every required source-owned check")
    print(digests.sorted_json({"d38_checks": checks}), flush=True)
    body = {"schema": "fleet.d38.preflight.v1", **plan["target"],
            "preflight_action_key": key, "host_class": "gb10", "result": "pass",
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "expires": None}
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    temporary.unlink(missing_ok=True)
    with temporary.open("x", encoding="utf-8") as handle:
        handle.write(digests.sorted_json(body, allow_nan=False) + "\n")
    temporary.replace(output)
    return body


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--digests", required=True)
    args = parser.parse_args(argv)
    try:
        digests = SimpleNamespace(**runpy.run_path(args.digests))
        run_preflight(args.plan, args.plan_sha256, args.result, digests=digests)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"D38 preflight refused: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
