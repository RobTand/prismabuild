#!/usr/bin/python3
"""Make nested SLURM describe the hardware and respect its parent cpuset."""
from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path


def configure(text: str, hardware: str) -> str:
    fields = dict(re.findall(r"(\w+)=(\S+)", hardware.splitlines()[0]))
    names = ("CPUs", "Boards", "SocketsPerBoard", "CoresPerSocket", "ThreadsPerCore")
    if any(not fields.get(name, "").isdigit() or int(fields[name]) < 1 for name in names):
        raise ValueError("slurmd -C did not report a complete positive CPU topology")
    lines = []
    text = re.sub(r"\\\n\s*", " ", text)
    for line in text.splitlines():
        if line.startswith("NodeName="):
            for name in names:
                line = re.sub(rf"\s+{name}=\S+", "", line)
            line += " " + " ".join(f"{name}={fields[name]}" for name in names)
        if line.startswith("TaskPluginParam="):
            values = line.split("=", 1)[1].split(",")
            if "SlurmdSpecOverride" not in values:
                values.append("SlurmdSpecOverride")
            line = "TaskPluginParam=" + ",".join(values)
        lines.append(line)
    if not any(line.startswith("TaskPluginParam=") for line in lines):
        # SLURM maps the parent cgroup's unavailable OS CPUs to its abstract
        # topology itself, including SMT. Never widen the PB allocation.
        lines.append("TaskPluginParam=SlurmdSpecOverride")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="Generated smoke slurm.conf to update")
    path = parser.parse_args().config
    hardware = subprocess.check_output(["/usr/sbin/slurmd", "-C"], text=True)
    path.write_text(configure(path.read_text(), hardware))
    print("smoke: nested topology " + hardware.splitlines()[0])
    print("smoke: SlurmdSpecOverride excludes CPUs unavailable in the PB parent cgroup")


if __name__ == "__main__":
    main()
