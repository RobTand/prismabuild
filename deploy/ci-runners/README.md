# PrismaBuild CI runners

PB has separate rootless Podman runners on sparky, sparklina, and dl380g10.
The host registers an ephemeral runner with GitHub, pipes its JIT config into
one container, and lets systemd replace the container after one job. There is
no polling loop. Nothing here installs or changes PB workers or TL3 services.

## Capacity and isolation

| Host | Slots | Role | CPUs per slot | Memory per slot |
| --- | --- | --- | --- | --- |
| sparky | 2 | pb-gate | slot 1: 0-4; slot 2: 10-14 | 8 GiB |
| sparklina | 2 | pb-gate | slot 1: 0-4; slot 2: 10-14 | 8 GiB |
| dl380g10 | 2 | pb-suite | 40-79, quota of 4 CPUs | 6 GiB |

The Sparks use efficiency cores outside PB's declared ten performance CPUs.
Their user managers delegate cpu, memory, and pids, but no cpuset controller.
Per-slot systemd `CPUAffinity=` places the processes without changing delegation.
The canary checks the actual affinity inside a job. Each Spark declares 104 GB
to PB out of about 121 GB. Check `free -g` before enabling slots. If 16 GiB of
additional memory does not fit, enable only slot 1 and report the reduced count.

dl380g10's two slots add eight CPU equivalents on SMT siblings 40-79.
TL3's eight existing slots use 32 CPU equivalents on that set. Together they
stay within 40. PB's admission can still use SMT overflow. CI is external load,
not a reservation in PB's ledger. Each container limits total memory plus swap
to the memory limit and permits at most 1,024 processes.

Containers have private process, user, IPC, and network namespaces. They have
no host mounts, shared storage, device access, container sockets, or host
credentials. All capabilities are dropped and privilege escalation is disabled.
Host proxy variables are not forwarded. The image runs as the unprivileged
`runner` user. Rootless networking can still reach LAN services. This design
does not enforce a network destination allowlist.

Labels are `self-hosted`, `linux`, `x64` or `arm64`, `pb-fleet`, and either
`pb-gate` or `pb-suite`. Runner names are `pb-HOST-slot-N-UUID`. Units are
`pb-ci-runner@N.service`, containers are `pb-ci-runner-N`, and configuration is
under `~/.config/pb-ci-runners/`. No TL3 image, unit, environment, or registration
is reused.

## Image inputs

The Containerfile pins each architecture's official Actions runner manifest.
Python 3.14.10 is baked in from python-build-standalone release 20261026.
Each archive has its own SHA-256 from the release API. Both `python` and
`python3` resolve to 3.14. Package versions and wheel hashes, including pytest
9.1.1, pytest-xdist 3.8.0, and PyYAML 6.0.3, are in `requirements.lock`.
The image also contains git, bash, and coreutils. Jobs do not download Python.
Native library thread counts default to one.

The base and Python archives are immutable. Ubuntu packages come from the
base's configured apt repositories during the build. Record the resulting
local image ID on every host. Retain the previous ID for rollback.

## Install one canary slot

Run as the rootless owner with Podman, a lingering systemd user manager, and
cpu, memory, and pids delegation. dl380g10 also needs its existing GitHub CLI
login with repository runner administration permission. Let `gh` load it.
Never open a token file or enable shell tracing or `GH_DEBUG`.

1. Check memory with `free -g` on each host.
2. Copy this directory to a PB-specific staging directory on each host.
3. On each host, run `bash deploy/ci-runners/install.sh 1` from the staged tree.
4. Read the PB unit journal and the repository's REST runners list.

The installer builds `localhost/pb-ci-runner:py314` locally with two CPUs and
six GiB. Image provisioning runs directly on the hosts. It writes immutable
image IDs to mode-600 per-slot environment files. Rerunning the installer
updates files and enables requested slots without restarting active jobs.
`install.sh 0` installs without enabling more slots. It does not disable slots.
Old images remain available. Never prune the shared Podman image store.

The Sparks call `ssh -o BatchMode=yes dl380g10 gh api ...` for registration.
Ensure that this route is available before enabling their units. A host with
no SSH identity can use a PB-specific key under `~/.config/pb-ci-runners/`.
Generate it locally without displaying the private key, and install its public
key on dl380g10 with `restrict,command="/home/rob/.local/lib/pb-ci-runners/jit-broker.py"`.
Install `jit-broker.py` there with mode 700. Its forced command accepts only
PB gate registrations for the two Spark hosts and slots. Configure that host's
SSH client to use the PB key for dl380g10. Never copy the private key.

The JIT config stays in shell memory and a pipe. It never appears in argv,
container configuration, a host environment file, or the journal. The entrypoint
exports it only inside the container as `ACTIONS_RUNNER_INPUT_JITCONFIG` before
executing the listener. The Actions listener removes that input before job
processes start. Every canary asserts that it is unset and prints only `set` or
`unset`. Runner diagnostic files live only in the ephemeral container.

Registration failures retain systemd's backoff from 30 seconds to five minutes.
Valid registration sends `RESTART_RESET=1`. Start limits are disabled. Systemd
versions that support the reset notification reset the delay after registration.
Older versions may keep a longer restart delay after an outage. The canary
measures observed job pickup separately from this notification contract.

## Prove the canary and expand

Open the PR. `runner-canary.yml` runs on changes to this directory and itself,
and supports manual dispatch. It has one gate job and one suite job. Its
same-repository condition excludes fork pull requests. Repository approval
policy still applies to outside contributors. Keep that condition in every
workflow targeting these runners.

The jobs print the runner name, architecture, Python and package versions,
affinity, `Cpus_allowed_list`, `nproc`, the cgroup memory limit, and the JIT
input status. They fail if shared storage is visible or placement is wrong.
Save run and job REST records, selected job logs, and image IDs under
`evidence/`. Job `created_at` to `started_at` is the pickup interval. Distinguish
that interval from workflow queue time and execution duration.

After both jobs pass, enable slot 2 on each host:

```sh
systemctl --user enable --now pb-ci-runner@2.service
gh api repos/RobTand/prismabuild/actions/runners
```

Capture runner names, labels, status, and busy flags. Repeat the canary enough
to record several pickup intervals and cover both Spark placements. Do not
claim host coverage from a role label alone. Record the actual runner name.

## Roll out an image or roll back

Run `install.sh 0` on each host to build and stage new image IDs. Running jobs
keep their current container. Confirm a runner's `busy=false` REST record before
restarting its PB slot. If all slots are busy, use the service's exit event or
wait for the next operator action. Do not add a status-polling loop.

To roll back, restore the previous immutable image ID in that host's PB slot
environment files, then restart idle PB slots. To withdraw capacity, disable
only the intended PB units:

```sh
systemctl --user disable --now pb-ci-runner@1.service pb-ci-runner@2.service
```

Stopping a busy slot cancels its job. Remove only stale PB registrations through
the PB repository's REST API. Do not change TL3 registrations or units, PB
fleet services, shared storage, delegation, or repository protection settings.

## Validate script changes

`tests/test_ci_runner_boundaries.py` substitutes gh, ssh, Podman, notification,
and systemd commands. It uses synthetic JIT input without credentials or a
container daemon. Tests check transport, labels, stdin handoff, isolation,
registration failure, invalid input, installer convergence, CPU placement, and
the constrained SSH broker.

Commit tests first, submit from a clean detached worktree through the published
`pbtest.py` at priority -10, and record red then green action keys. Run five
fresh repeats per architecture, including two with two xdist workers. Inspect
terminal status, actual pytest outcomes, and CAS receipts. The CI canary runs
directly on its runners under Rob's CI policy. Agent-run script tests always
use PrismaBuild admission.
