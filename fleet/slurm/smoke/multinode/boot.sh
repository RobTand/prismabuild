#!/bin/bash
# Bring one of the three nodes up, then stay up.
#
#   boot.sh ctld    dl380g10 runs munged, slurmctld and slurmd
#   boot.sh node    sparky and gx10-6b77 run munged and slurmd
#
# Unlike the one-node harness's `inside.sh`, this does not run the rows: the
# rows have to `docker exec` into a container other than the one they are in
# (submit from sparky, kill sparky's slurmd, restart dl380g10's slurmctld), and
# the image's `docker` is the Epilog's fake one.  So the rows run on the host
# and this ends by waiting.
#
# The configuration is NOT written here.  `run.sh` generates one copy from
# fleet/slurm/*.conf with `genconf.py` and drops it on the shared volume; each
# container copies the same bytes, so a `different slurm.conf` complaint in
# slurmctld.log would mean a real mismatch rather than three files that were
# written three times.
set -u

ROLE="${1:?boot.sh needs a role: ctld or node}"
REPO="${PB_SMOKE_REPO:-/repo}"
VOL="${PB_SMOKE_VOL:-/mnt/shared}"
NODE="$(hostname -s)"
ETC="$VOL/etc"

say() { printf '%s\n' "boot[$NODE]: $*"; }
die() { say "FATAL: $*"; exit 1; }

# Prebuilt mode uses an immutable base image and performs this small setup
# inside the accounted container, where apt/ssh work shares its PB budget.
if [ -d /pb-smoke-keys ]; then
    if [ ! -x /usr/sbin/sshd ]; then
        apt-get update && apt-get install -y --no-install-recommends openssh-server openssh-client \
            || die "could not install SSH inside the smoke container"
    fi
    install -o munge -g munge -m 0400 /pb-smoke-keys/munge.key /etc/munge/munge.key || die "munge key install failed"
    ssh-keygen -A || die "SSH host key generation failed"
    install -d -m 0755 /run/sshd
    install -d -o rob -g rob -m 0700 /home/rob/.ssh
    install -o rob -g rob -m 0600 /pb-smoke-keys/id_smoke /home/rob/.ssh/id_ed25519
    install -o rob -g rob -m 0600 /pb-smoke-keys/id_smoke.pub /home/rob/.ssh/authorized_keys
    printf 'StrictHostKeyChecking no\nUserKnownHostsFile /dev/null\nLogLevel ERROR\n' > /home/rob/.ssh/config
    chown rob:rob /home/rob/.ssh/config
    chmod 0600 /home/rob/.ssh/config
    usermod -p '*' rob || die "could not enable smoke SSH identity"
fi

# The daemons log as root onto a volume the host reads back as `rob`, and a
# transcript nobody outside the container can read is not evidence.  The log
# directory is a symlink into the volume rather than a config change, which is
# what lets SlurmctldLogFile and SlurmdLogFile stay the fleet's own paths.
mkdir -p "$VOL/logs/$NODE"
chmod -R a+rX "$VOL/logs" 2>/dev/null || true
rm -rf /var/log/slurm
ln -s "$VOL/logs/$NODE" /var/log/slurm
mkdir -p /etc/slurm /var/spool/slurm/ctld /var/spool/slurm/d

# -- cgroup v2 delegation ----------------------------------------------------
#
# Three containers, three independent instances of the arrangement the one-node
# harness already needs, for the reason it needs it: core._collect_worker_evidence
# refuses a job whose processes are not in a `job_<id>` cgroup, so
# proctrack/cgroup is not optional and a fallback would fail every action for a
# reason unrelated to what is under test.  --cgroupns=private gives each
# container its own cgroup root; the processes still have to be moved out of it
# before a controller can be delegated.
prepare_cgroups() {
    local root=/sys/fs/cgroup
    [ -f "$root/cgroup.controllers" ] || { say "no cgroup v2 at $root"; return 1; }
    mkdir -p "$root/init" 2>/dev/null || return 1
    if [ -f "$root/cgroup.procs" ]; then
        while read -r pid; do
            [ -n "$pid" ] && echo "$pid" >"$root/init/cgroup.procs" 2>/dev/null
        done <"$root/cgroup.procs"
    fi
    echo "+cpuset +cpu +memory +pids" >"$root/cgroup.subtree_control" 2>/dev/null || return 1
    # 23.11's cgroup/v2 plugin creates its stepd scope under system.slice and
    # refuses to initialize when that directory is absent; on a real box systemd
    # owns it.  25.11 does not need it, so it is created unconditionally rather
    # than branched on the version.
    mkdir -p "$root/system.slice" 2>/dev/null
    echo "+cpuset +cpu +memory +pids" >"$root/system.slice/cgroup.subtree_control" 2>/dev/null
    say "cgroup v2 delegated [$(cat "$root/cgroup.subtree_control")]"
    return 0
}
prepare_cgroups || die "cgroup delegation failed; the lane cannot work without it"

# -- configuration -----------------------------------------------------------
for name in slurm.conf gres.conf cgroup.conf; do
    [ -f "$ETC/$name" ] || die "$ETC/$name is missing; run.sh generates it"
    install -m 0644 "$ETC/$name" "/etc/slurm/$name"
done
cp "$REPO/fleet/slurm/epilog.sh" /etc/slurm/epilog.sh
chmod 0755 /etc/slurm/epilog.sh
say "config sha256 $(sha256sum /etc/slurm/slurm.conf | cut -d' ' -f1)"
say "epilog sha256 $(sha256sum /etc/slurm/epilog.sh | cut -d' ' -f1)"

# The two Spark nodes declare `Gres=gpu:1,shard:N` bound to /dev/nvidia0, which
# is the fleet's gres.conf unchanged.  slurmd refuses to start when a SHARED
# gres has no SHARING gres bound to a File, so the device has to exist; nothing
# opens it and ConstrainDevices is off.
if grep -q "^NodeName=$NODE .*Name=gpu" /etc/slurm/gres.conf; then
    [ -e /dev/nvidia0 ] || mknod /dev/nvidia0 c 195 0 || \
        die "could not create /dev/nvidia0; this node offers a GRES bound to it"
    say "mknod /dev/nvidia0 for the node's Gres"
fi

# -- ssh ---------------------------------------------------------------------
# Only so that verify.sh row 0b can read the other boxes' installed slurm.conf
# the way it will on the fleet.  Nothing else in the harness uses it.
/usr/sbin/sshd
say "sshd started"

# -- munge -------------------------------------------------------------------
# The key came from the image, which is what makes it one key across the three
# containers with no ordering between them.
[ -f /etc/munge/munge.key ] || die "no munge key in the image"
runuser -u munge -- /usr/sbin/munged --force >>"/var/log/slurm/munged.log" 2>&1 \
    || die "munged did not start"
sleep 1
munge -n | unmunge >/dev/null 2>&1 || die "munge is not answering"
say "munge key sha256 $(sha256sum /etc/munge/munge.key | cut -d' ' -f1)"

# -- start scripts -----------------------------------------------------------
#
# Written as files rather than run inline because two rows restart a daemon and
# a restart that is typed by hand is a different start.  The Epilog's
# environment comes from slurmd's, so the exports belong in the script that
# starts slurmd.
cat >/usr/local/bin/pb-start-slurmd <<SCRIPT
#!/bin/bash
export PRISMABUILD_SLURM_LANE_ROOT="$VOL/prismabuild-fleet/slurm"
export PB_SMOKE_DOCKER_LOG="$VOL/docker.log"
exec /usr/sbin/slurmd >>/var/log/slurm/slurmd.stdout 2>&1
SCRIPT
cat >/usr/local/bin/pb-start-slurmctld <<SCRIPT
#!/bin/bash
exec /usr/sbin/slurmctld >>/var/log/slurm/slurmctld.stdout 2>&1
SCRIPT
chmod 0755 /usr/local/bin/pb-start-slurmd /usr/local/bin/pb-start-slurmctld

if [ "$ROLE" = ctld ]; then
    : >"$VOL/docker.log"; chmod 0666 "$VOL/docker.log"
    /usr/local/bin/pb-start-slurmctld & sleep 2
    say "slurmctld started"
fi
/usr/local/bin/pb-start-slurmd & sleep 1
say "slurmd started, $(sinfo --version 2>&1 | head -n 1)"

# The rows run as `rob` through `docker exec`, and the git materialization they
# drive needs an identity and a safe.directory rule.  Done here so that every
# container is ready to be a submitter without the rows setting three of them up.
# `env HOME=`, because `runuser` without `-l` keeps root's HOME and the config
# would land in /root/.gitconfig where the job's user never looks.
as_rob() { runuser -u rob -- env HOME=/home/rob "$@"; }
as_rob git config --global user.name "PrismaBuild smoke" || true
as_rob git config --global user.email "smoke@example.invalid" || true
as_rob git config --global --add safe.directory '*' || true

chmod -R a+rX "$VOL/logs" 2>/dev/null || true
# The last thing, and the only thing run.sh waits for besides `sinfo`.  A node
# reaches `idle` the moment slurmd registers, which is several steps before
# this script has finished setting the container up -- and the first row to
# submit through a half-set-up container reads as a lane defect.
: >"$VOL/logs/$NODE/ready"
chmod 0644 "$VOL/logs/$NODE/ready"
say "up"
# The rows drive this container from the host; there is nothing left to do here
# but stay alive and keep the logs readable.
while true; do
    sleep 5
    chmod -R a+rX "$VOL/logs" 2>/dev/null || true
done
