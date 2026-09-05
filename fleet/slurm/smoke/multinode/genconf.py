#!/usr/bin/python3
"""Turn the fleet's own SLURM configuration into the three-node smoke's.

The one-node harness writes its `slurm.conf` from scratch, which is honest for
one container called `pbsmoke` but useless for the claims this harness exists
to settle: the partition routing rule, the node Features a `--tag` becomes,
and the shard counts are all things the *fleet's* file says, and a smoke that
restates them from memory tests the restatement.

So this reads `fleet/slurm/slurm.conf`, `gres.conf` and `cgroup.conf` and
edits them.  Every edit is one entry in `DEVIATIONS`, printed at the top of
the run beside the results.  What is not in that list is the fleet's file
unchanged, byte for byte -- including `SlurmctldHost=dl380g10`, the three
`NodeName` lines' Gres and Features, and all three `PartitionName` lines.

One file is generated and copied to all three containers rather than generated
per container, because slurmd sends a config hash at registration and a
controller that sees a different one logs `appears to have a different
slurm.conf`.  Identical bytes is the only way to be sure that message is
absent because nothing differs.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

#: What the container cannot have, and what it gets instead.  Each entry is
#: (setting, fleet value, smoke value, why) and is printed verbatim.
DEVIATIONS: list[tuple[str, str, str, str]] = []


def note(setting: str, fleet: str, here: str, why: str) -> None:
    DEVIATIONS.append((setting, fleet, here, why))


def logical_lines(text: str) -> list[str]:
    """SLURM continues a line with a trailing backslash; join those first.

    The fleet's three `NodeName=` lines are each written over several physical
    lines, so any edit that matched a physical line would match a fragment.
    """

    joined: list[str] = []
    buffer = ""
    for line in text.splitlines():
        stripped = line.rstrip()
        if stripped.endswith("\\"):
            buffer += stripped[:-1].rstrip() + " "
            continue
        joined.append((buffer + stripped).strip() if buffer else line)
        buffer = ""
    if buffer:
        joined.append(buffer.strip())
    return joined


def rewrite_slurm_conf(text: str, *, cpus: int) -> str:
    out: list[str] = []
    for line in logical_lines(text):
        bare = line.strip()

        if bare.startswith("SlurmctldHost="):
            # The fleet pins the controller and every node to a LAN address
            # because the boxes do not resolve each other's names.  Inside a
            # docker network the opposite holds: the names resolve, by the
            # container aliases, and 192.168.1.x is a different fleet
            # altogether -- the real one, which is not running SLURM.  A
            # controller configured with those addresses binds and dials into
            # nothing and answers no RPC at all.
            note("SlurmctldHost", "dl380g10(192.168.1.107)", "dl380g10",
                 "docker's own DNS resolves the node names; the fleet's LAN "
                 "addresses are not on this network")
            out.append(re.sub(r"\([^)]*\)", "", line))
            continue

        if bare.startswith("SlurmUser="):
            note("SlurmUser", "slurm", "root",
                 "no slurm user in the image, and creating one would test useradd")
            out.append("SlurmUser=root")
            continue

        if bare.startswith("KillWait="):
            note("KillWait", "30", "10",
                 "the node-failure row would otherwise spend it waiting")
            out.append("KillWait=10")
            continue

        if bare.startswith("NodeName=") and "NodeAddr=" in bare:
            if not any(name == "NodeAddr" for name, _, _, _ in DEVIATIONS):
                note("NodeAddr", "each box's LAN address", "absent",
                     "the containers resolve each other's node names; "
                     "whether NodeAddr is the right remedy on the fleet is "
                     "not a question this can answer")
            line = re.sub(r"\s*NodeAddr=\S+", "", line)
            bare = line.strip()

        if bare.startswith("NodeName=dl380g10"):
            # The controller container runs on a GB10 like the other two, so
            # the x86 box's socket/core/thread counts describe hardware that is
            # not under it.  A node whose configured topology exceeds what
            # slurmd reports comes up DRAINED with "Low socket*core*thread
            # count", which would be a fact about this box and not about the
            # fleet's file.
            note("NodeName=dl380g10 topology",
                 "CPUs=80 SocketsPerBoard=2 CoresPerSocket=20 ThreadsPerCore=2",
                 f"CPUs={cpus} SocketsPerBoard=2 CoresPerSocket={cpus // 2} "
                 "ThreadsPerCore=1",
                 "all three containers are on one GB10; the x86 topology is "
                 "not under them")
            line = re.sub(r"CPUs=\d+", f"CPUs={cpus}", line)
            line = re.sub(r"CoresPerSocket=\d+", f"CoresPerSocket={cpus // 2}", line)
            line = re.sub(r"ThreadsPerCore=\d+", "ThreadsPerCore=1", line)
            out.append(line)
            continue

        if bare.startswith("ReturnToService="):
            # Unchanged, but named here because a row depends on it: the
            # returning node in the node-failure row comes back by this rule
            # and by nothing an operator types.
            out.append(line)
            continue

        out.append(line)

    # SlurmdTimeout is absent from the fleet's file, so it is SLURM's 300 s
    # there.  The node-failure row waits for the controller to notice a dead
    # slurmd, and five minutes of waiting is not a stronger claim than thirty
    # seconds of it.
    note("SlurmdTimeout", "absent (SLURM's 300)", "30",
         "the node-failure row waits for the controller to notice; the value "
         "under test is ReturnToService, not this")
    out.append("")
    out.append("# -- added by fleet/slurm/smoke/multinode/genconf.py ---------")
    out.append("SlurmdTimeout=30")
    return "\n".join(out) + "\n"


def rewrite_cgroup_conf(text: str) -> str:
    out: list[str] = []
    for line in text.splitlines():
        bare = line.strip()
        if bare.startswith("ConstrainDevices="):
            note("ConstrainDevices", "yes", "no",
                 "the GRES binds a mknod'd character device nothing opens; "
                 "device containment stays unverified here")
            out.append("ConstrainDevices=no")
            continue
        if bare.startswith("AllowedDevicesFile="):
            note("AllowedDevicesFile", "cgroup_allowed_devices_file.conf",
                 "absent",
                 "it only has meaning with ConstrainDevices=yes")
            continue
        out.append(line)
    note("IgnoreSystemd", "absent", "yes",
         "there is no systemd in the container to ask for a cgroup scope")
    out.append("")
    out.append("# -- added by fleet/slurm/smoke/multinode/genconf.py ---------")
    out.append("IgnoreSystemd=yes")
    return "\n".join(out) + "\n"


def rewrite_gres_conf(text: str) -> str:
    """The fleet's `gres.conf`, unchanged.

    `File=/dev/nvidia0` stays because slurmd refuses a SHARED gres whose
    SHARING gres has no `File=`; what differs is the device, which `boot.sh`
    creates with `mknod` on the two Spark containers.  That is a deviation in
    the container, not in the file, so it is recorded against the device.
    """

    note("gres.conf File=/dev/nvidia0", "the GB10's device",
         "a mknod'd character device",
         "the file is the fleet's unchanged; nothing in the smoke opens it")
    return text


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: genconf.py <fleet slurm dir> <out dir> <cpus>",
              file=sys.stderr)
        return 2
    source, out, cpus = Path(argv[1]), Path(argv[2]), int(argv[3])
    out.mkdir(parents=True, exist_ok=True)

    (out / "slurm.conf").write_text(
        rewrite_slurm_conf(
            (source / "slurm.conf").read_text(encoding="utf-8"), cpus=cpus
        ),
        encoding="utf-8",
    )
    (out / "cgroup.conf").write_text(
        rewrite_cgroup_conf((source / "cgroup.conf").read_text(encoding="utf-8")),
        encoding="utf-8",
    )
    (out / "gres.conf").write_text(
        rewrite_gres_conf((source / "gres.conf").read_text(encoding="utf-8")),
        encoding="utf-8",
    )

    width = max(len(name) for name, _, _, _ in DEVIATIONS)
    print("deviations from fleet/slurm/*.conf "
          "(everything else is the fleet's file unchanged):")
    for name, fleet, here, why in DEVIATIONS:
        print(f"  {name.ljust(width)}  {fleet}  ->  {here}")
        print(f"  {' ' * width}  because {why}")
    print("  RealMemory, CPUs on the two Sparks, Gres, Features, Weight, all")
    print("  three PartitionName lines, the controller's node name,")
    print("  ReturnToService=2, MinJobAge, the scheduler and cgroup plugin")
    print("  choices and the Epilog: unchanged.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
