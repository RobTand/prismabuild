# What PR 1584's per-host enrollment needs (pb-integrator, 2026-10-10)

Read from `tools/fleet/install_movement_publisher.sh` and the `publish_runtime.py` diff in the gen12 tree. I ran none of it and enrolled no host.
CEO decision (`rep-1010-030745-900d`): recommend Rob defer; enroll no host and create no publisher account until Rob decides.

**Once, a person.** Pick a publisher account that does not own the runtime store. The publisher refuses a key owned by the store owner, so `rob` cannot be it. Make a 64-hex secret. Put it at `~/.config/prismabuild/movement-approval.key` under that account, mode 0600.

**On each host, once, as root.** The hosts with worker loops are dl380g10, sparky and sparklina. Copy `install_movement_publisher.sh`, `runtime_publication.py` and `digest_primitives.py` from the live generation to a local temp directory. Run `sudo bash install_movement_publisher.sh --approval-key-file PATH`. Use the same secret on every host.

**What the installer creates (root-owned).** `/opt/prismabuild` with the two modules and `movement-generations`. `/etc/prismabuild/movement-approval.key` (mode 0400) and `movement-publish.json`. `/var/lib/prismabuild-movement-publish`. A systemd service and timer `prismabuild-movement-publish` that run every 60 seconds. It refuses to run if `/opt`, `/etc` or `/var/lib` lack root custody.

**State on 2026-10-10.** dl380g10 has none of this (checked read-only). The Sparks were not checked.

**Process point for Rob.** Signing runs inside `publish_runtime.py`, as the user who runs it. A publication by `rob` writes no approval file, so enrolled hosts then make no protected copy. The reservation turns on only if the publisher account runs the publications. D64 has pb-integrator publish as `rob`.

**Not enrolling costs nothing.** Hosts keep their old behaviour and nothing is refused. Gen12 publishes without any key (signing is best effort).
