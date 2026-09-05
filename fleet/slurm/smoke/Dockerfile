# A one-node SLURM controller, in a container, so the lane can be run rather
# than described.  Everything in `fleet/slurm/` had been tested only against
# fake `sbatch`/`sacct`/`scontrol`/`squeue` binaries; this image is where a real
# controller answers instead.
#
# Two package sources, one image.  With an empty `debs/` in the build context
# the image installs Ubuntu 24.04's own `slurm-wlm` (23.11.4), which is enough
# to exercise everything the lane asks of a scheduler.  With `debs/` populated
# it installs those instead -- the 25.11.2 rebuild the fleet will actually run,
# so the smoke can be taken twice and the two answers compared.  `debs/` is
# always present in the context and usually empty; `run.sh` fills it.
#
# NOT a model of the fleet's install.  There is no systemd here, no `slurm`
# user, no NFS and no GPU: the daemons run as root in the foreground and the
# node's GRES is declared with no device file.  What it does model exactly is
# the scheduler's *behaviour* -- select/cons_tres, GRES shards, Features,
# `--time` enforcement, the Epilog, and an `sacct` with no accounting behind it.
FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive

# `debs/` decides which SLURM this is.  Copied before the install so that the
# choice is made inside the build and the caller only has to fill a directory.
COPY debs /debs

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates git python3 python3-minimal procps iproute2 \
        munge libmunge2 sudo bzip2 xz-utils; \
    if ls /debs/*.deb >/dev/null 2>&1; then \
        echo "installing SLURM from the packages in debs/"; \
        apt-get install -y --no-install-recommends /debs/*.deb; \
    else \
        echo "installing SLURM from the Ubuntu 24.04 archive"; \
        apt-get install -y --no-install-recommends slurm-wlm; \
    fi; \
    rm -rf /var/lib/apt/lists/*

# The sealed action environment `pbrun` builds names `/home/rob` and
# `/home/rob/tmp` literally, and `materialize.LOCAL_CHECKOUT_ROOT` defaults
# under the second.  uid 1000 so a bind-mounted volume stays owned by the user
# who ran the harness rather than by root.
RUN set -eux; \
    userdel -r ubuntu 2>/dev/null || true; \
    id -u rob >/dev/null 2>&1 || useradd -m -u 1000 -s /bin/bash rob; \
    install -d -o rob -g rob /home/rob/tmp /home/rob/tmp/prismabuild-checkouts; \
    install -d /etc/slurm /var/log/slurm /var/spool/slurm/ctld /var/spool/slurm/d; \
    install -d -o munge -g munge -m 0700 /etc/munge /var/lib/munge /var/log/munge; \
    install -d -o munge -g munge -m 0755 /run/munge

# The fake `docker` the Epilog is pointed at.  The Epilog's whole job is to
# remove containers by the action's ownership label, and a smoke that started
# real containers would be testing Docker rather than the Epilog.  This records
# the argv it was called with, which is the fact under test.
#
# Installed as `docker` on the PATH, not behind PRISMABUILD_EPILOG_DOCKER.
# Measured reason: SLURM builds the Epilog's environment itself, out of its own
# SLURM_* variables, so nothing exported to slurmd reaches it -- an Epilog run
# with PRISMABUILD_EPILOG_DOCKER exported to slurmd still ran plain `docker`.
# The fleet's Epilog will find the real `docker` on PATH the same way, so this
# is also the faithful arrangement.
COPY fake_docker.sh /usr/local/lib/pb-smoke/docker
RUN set -eux; \
    chmod 0755 /usr/local/lib/pb-smoke/docker; \
    ln -sf /usr/local/lib/pb-smoke/docker /usr/local/bin/docker; \
    ln -sf /usr/local/lib/pb-smoke/docker /usr/bin/docker

CMD ["/bin/bash", "-lc", "echo 'run fleet/slurm/smoke/inside.sh from the mounted repo'"]
