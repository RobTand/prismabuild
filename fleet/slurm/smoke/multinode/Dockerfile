# The three-node smoke's image: the one-node image plus a shared munge key.
#
# `FROM` the image `fleet/slurm/smoke/run.sh` already builds, so the SLURM
# under test is chosen the same way it is chosen there -- by what `debs/` held
# when that image was built -- and this layer adds one file.
#
# The key is baked into the image rather than mounted because that is what
# makes it the SAME key in all three containers before any daemon starts, with
# no ordering between them.  It is generated fresh for every run and the image
# is removed when the run ends.  A munge key in an image is a test-only
# arrangement; on the fleet the key is installed per box with mode 0400 and
# never travels through a build context.
ARG BASE_IMAGE=prismabuild-slurm-smoke:25.11
FROM ${BASE_IMAGE}

COPY munge.key /etc/munge/munge.key
RUN set -eux; \
    chown munge:munge /etc/munge/munge.key; \
    chmod 0400 /etc/munge/munge.key

CMD ["/bin/bash", "-lc", "echo 'run fleet/slurm/smoke/multinode/boot.sh from the mounted repo'"]
