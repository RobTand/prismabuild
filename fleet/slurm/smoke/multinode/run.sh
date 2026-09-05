#!/bin/bash
# Three SLURM nodes, in three containers, on this box.
#
#   fleet/slurm/smoke/multinode/run.sh                    # the fleet's 25.11.2
#   PB_SMOKE3_SLURM=24.04 fleet/slurm/smoke/multinode/run.sh   # Ubuntu's 23.11.4
#   PRISMABUILD_SLURM_SMOKE=1 PYTHONPATH=src pytest -q tests/test_slurm_smoke_multinode.py
#
# The one-node harness settles what a scheduler does for one action.  This one
# settles the four things that need more than one box: a controller talking to
# a remote slurmd over munge, an action landing somewhere other than where it
# was submitted, the partition routing rule against three real nodes, and a
# node leaving and coming back.
#
# The nodes' hostnames are the fleet's own NodeNames, and the configuration is
# generated from fleet/slurm/*.conf by `genconf.py`, which prints every
# deviation at the top of the run.  The repository is mounted read-only in all
# three containers; one run volume is mounted at /mnt/shared in all three, so
# the CAS, `pb-queue` and the lane root are one filesystem as they are on the
# fleet.  The host's real /mnt/shared is never touched.
#
# The rows run on the HOST, not in a container: three of them have to reach
# into a container other than the submitter's (submit from sparky, kill
# sparky's slurmd, restart dl380g10's slurmctld), and the image's `docker` is
# the Epilog's fake one.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
RUN_ROOT="${PB_SMOKE_RUN_ROOT:-/home/rob/slurm-build/smoke}"
KEEP="${PB_SMOKE_KEEP:-0}"
#: The fleet's own packages are the primary; the archive's 23.11.4 is the
#: secondary.  Both are images `fleet/slurm/smoke/run.sh` already knows how to
#: build, and this harness only adds a munge key to one of them.
FLAVOUR="${PB_SMOKE3_SLURM:-25.11}"
BASE_IMAGE="${PB_SMOKE3_BASE_IMAGE:-prismabuild-slurm-smoke:$FLAVOUR}"
DEB_DIR="${DEB_DIR:-}"
if [ -z "$DEB_DIR" ] && [ "$FLAVOUR" = "25.11" ]; then
    DEB_DIR=/home/rob/slurm-build/arm64-24.04
fi

command -v docker >/dev/null 2>&1 || { echo "smoke3: docker is not on PATH" >&2; exit 2; }

# A nonce in every docker name.  Two other agents may be running the one-node
# harness at the same time and a collision would look like a defect in whichever
# run lost.
stamp="$(date +%Y%m%dT%H%M%S)-$$"
run="$RUN_ROOT/multinode-$stamp"
ctx="$run/ctx"
vol="$run/vol"
net="pb-smoke3-net-$stamp"
image="prismabuild-slurm-smoke3:$stamp"
NODES=(dl380g10 sparky gx10-6b77)
declare -A ROLE=([dl380g10]=ctld [sparky]=node [gx10-6b77]=node)

mkdir -p "$vol" "$ctx" || exit 2
echo "smoke3: disk at $(df --output=pcent "$RUN_ROOT" | tail -n 1 | tr -d ' ') used on $(dirname "$RUN_ROOT")"
echo "smoke3: run directory $run"

cleanup() {
    for node in "${NODES[@]}"; do
        docker rm -f "pb-smoke3-$node-$stamp" >/dev/null 2>&1
    done
    docker network rm "$net" >/dev/null 2>&1
    # Only the image this run built.  The base image belongs to the one-node
    # harness and is left alone.
    docker rmi "$image" >/dev/null 2>&1
}
trap cleanup EXIT

# -- the base image ----------------------------------------------------------
if ! docker image inspect "$BASE_IMAGE" >/dev/null 2>&1; then
    echo "smoke3: building the base image $BASE_IMAGE via the one-node harness"
    base_ctx="$RUN_ROOT/ctx-smoke3-$stamp"
    mkdir -p "$base_ctx/debs" || exit 2
    cp "$HERE/../Dockerfile" "$HERE/../fake_docker.sh" "$base_ctx/" || exit 2
    if [ -n "$DEB_DIR" ]; then
        [ -d "$DEB_DIR" ] || { echo "smoke3: DEB_DIR $DEB_DIR does not exist" >&2; exit 2; }
        cp "$DEB_DIR"/*.deb "$base_ctx/debs/" 2>/dev/null
        cp "$DEB_DIR"/full-set/slurmctld_*.deb "$base_ctx/debs/" 2>/dev/null
    fi
    docker build -q -t "$BASE_IMAGE" "$base_ctx" >"$run/base-build.log" 2>&1 || {
        echo "smoke3: base image build failed; see $run/base-build.log" >&2
        tail -n 40 "$run/base-build.log" >&2
        exit 2
    }
    rm -rf "$base_ctx"
fi

# -- the run's image: the base plus one munge key ----------------------------
cp "$HERE/Dockerfile" "$ctx/" || exit 2
dd if=/dev/urandom of="$ctx/munge.key" bs=1024 count=1 status=none || exit 2
chmod 0400 "$ctx/munge.key"
echo "smoke3: building $image from $BASE_IMAGE"
docker build -q --build-arg "BASE_IMAGE=$BASE_IMAGE" -t "$image" "$ctx" \
    >"$run/build.log" 2>&1 || {
    echo "smoke3: image build failed; see $run/build.log" >&2
    tail -n 40 "$run/build.log" >&2
    exit 2
}
rm -f "$ctx/munge.key"

# -- the configuration, generated once from the fleet's files ----------------
python3 "$HERE/genconf.py" "$REPO/fleet/slurm" "$vol/etc" "$(nproc)" \
    | tee "$run/deviations.txt" || exit 2
echo

# -- the network and the three containers ------------------------------------
docker network create "$net" >/dev/null || exit 2
for node in "${NODES[@]}"; do
    docker run -d --name "pb-smoke3-$node-$stamp" \
        --privileged --cgroupns=private \
        --network "$net" --network-alias "$node" --hostname "$node" \
        -v "$REPO":/repo:ro \
        -v "$vol":/mnt/shared \
        -e PB_SMOKE_REPO=/repo \
        -e PB_SMOKE_VOL=/mnt/shared \
        "$image" bash /repo/fleet/slurm/smoke/multinode/boot.sh "${ROLE[$node]}" \
        >/dev/null || { echo "smoke3: could not start $node" >&2; exit 2; }
    echo "smoke3: started $node as ${ROLE[$node]}"
done

# -- wait for all three to register -------------------------------------------
ctld="pb-smoke3-dl380g10-$stamp"
ready=0
for _ in $(seq 1 120); do
    # -p all, because `sinfo -N` prints one line per node PER PARTITION and
    # every node here is in two of them.
    idle="$(docker exec "$ctld" sinfo -h -N -p all -o '%T' 2>/dev/null | grep -c '^idle$')"
    marks=0
    for node in "${NODES[@]}"; do
        [ -f "$vol/logs/$node/ready" ] && marks=$((marks + 1))
    done
    if [ "${idle:-0}" = "3" ] && [ "$marks" = 3 ]; then ready=1; break; fi
    sleep 1
done
if [ "$ready" != 1 ]; then
    echo "smoke3: the three nodes did not all come up (idle=${idle:-0}/3, boot.sh finished on ${marks:-0}/3)" >&2
    docker exec "$ctld" sinfo -N -l 2>&1 | sed 's/^/smoke3: /' >&2
    for node in "${NODES[@]}"; do
        echo "smoke3: --- $node boot log ---" >&2
        docker logs "pb-smoke3-$node-$stamp" 2>&1 | tail -n 20 >&2
    done
    exit 2
fi
docker exec "$ctld" sinfo -N -h -p all -o 'node %N %T gres=%G features=%f' | sed 's/^/smoke3: /'
docker exec "$ctld" sinfo --version | sed 's/^/smoke3: slurm /'

# -- the rows ----------------------------------------------------------------
PB_SMOKE3_STAMP="$stamp" \
PB_SMOKE3_VOL="$vol" \
PB_SMOKE3_REPO="$REPO" \
python3 "$HERE/rows_multinode.py" 2>&1 | tee "$run/smoke.log"
status="${PIPESTATUS[0]}"

echo "smoke3: transcript $run/smoke.log (exit $status)"
if [ "$KEEP" != "0" ]; then
    trap - EXIT
    echo "smoke3: PB_SMOKE_KEEP is set; containers, network and image $image kept"
fi
exit "$status"
