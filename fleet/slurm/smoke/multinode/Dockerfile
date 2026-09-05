# The three-node smoke's image: the one-node image, a shared munge key, and an
# ssh route between the boxes.
#
# `FROM` the image `fleet/slurm/smoke/run.sh` already builds, so the SLURM
# under test is chosen the same way it is chosen there -- by what `debs/` held
# when that image was built -- and this layer adds what three boxes need that
# one did not.
#
# The munge key is baked in rather than mounted because that is what makes it
# the SAME key in all three containers before any daemon starts, with no
# ordering between them.  The ssh key is here for the same reason and for one
# more: `fleet/slurm/verify.sh` row 0b reads /etc/slurm/slurm.conf on the other
# two boxes over ssh, and a harness that stubbed that out would be testing the
# stub.  Both keys are generated fresh for every run and the image is removed
# when the run ends.  Keys in an image are a test-only arrangement; on the
# fleet the munge key is installed per box at mode 0400 and ssh keys are rob's.
ARG BASE_IMAGE=prismabuild-slurm-smoke:25.11
FROM ${BASE_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive

COPY munge.key /keys/munge.key
COPY id_smoke /keys/id_smoke
COPY id_smoke.pub /keys/id_smoke.pub

RUN set -eux; \
    install -o munge -g munge -m 0400 /keys/munge.key /etc/munge/munge.key; \
    apt-get update; \
    apt-get install -y --no-install-recommends openssh-server openssh-client; \
    rm -rf /var/lib/apt/lists/*; \
    ssh-keygen -A; \
    install -d -m 0755 /run/sshd; \
    install -d -o rob -g rob -m 0700 /home/rob/.ssh; \
    install -o rob -g rob -m 0600 /keys/id_smoke /home/rob/.ssh/id_ed25519; \
    install -o rob -g rob -m 0600 /keys/id_smoke.pub /home/rob/.ssh/authorized_keys; \
    printf 'StrictHostKeyChecking no\nUserKnownHostsFile /dev/null\nLogLevel ERROR\n' \
        > /home/rob/.ssh/config; \
    chown rob:rob /home/rob/.ssh/config; chmod 0600 /home/rob/.ssh/config; \
    rm -rf /keys; \
    : "a locked account refuses public-key login too; * is no password, not locked"; \
    usermod -p '*' rob

CMD ["/bin/bash", "-lc", "echo 'run fleet/slurm/smoke/multinode/boot.sh from the mounted repo'"]
