#!/bin/bash
# Run the SLURM lane against a real controller, in a container, on this box.
#
#   fleet/slurm/smoke/run.sh                             # Ubuntu 24.04's 23.11.4
#   DEB_DIR=/home/rob/slurm-build/arm64-24.04 .../run.sh # the fleet's 25.11.2
#
# DEB_DIR names a directory of .deb files to install instead of the archive's
# `slurm-wlm`.  `full-set/` beneath it is picked up too, because the node
# packages alone have no `slurmctld` and a one-node cluster needs one.
#
# The repository is mounted read-only: the smoke must not be able to change the
# tree it is testing.  Everything writable -- the CAS, `pb-queue`, the lane
# root, the actions' own checkouts -- lives in a per-run directory under
# RUN_ROOT, mounted at /mnt/shared inside the container so that `pbrun`'s
# hard-coded fleet paths resolve without being patched.  The host's real
# /mnt/shared is never touched.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
RUN_ROOT="${PB_SMOKE_RUN_ROOT:-/home/rob/slurm-build/smoke}"
DEB_DIR="${DEB_DIR:-}"
KEEP="${PB_SMOKE_KEEP:-0}"

if [ -n "$DEB_DIR" ]; then
    IMAGE="${PB_SMOKE_IMAGE:-prismabuild-slurm-smoke:25.11}"
else
    IMAGE="${PB_SMOKE_IMAGE:-prismabuild-slurm-smoke:24.04}"
fi

command -v docker >/dev/null 2>&1 || { echo "smoke: docker is not on PATH" >&2; exit 2; }

free_pct="$(df --output=pcent "$RUN_ROOT" 2>/dev/null || df --output=pcent /home/rob)"
echo "smoke: disk at $(echo "$free_pct" | tail -n 1 | tr -d ' ') used on $(dirname "$RUN_ROOT")"

stamp="$(date +%Y%m%dT%H%M%S)"
run="$RUN_ROOT/run-$stamp"
ctx="$RUN_ROOT/ctx"
vol="$run/vol"
mkdir -p "$vol" "$ctx/debs" || exit 2
rm -f "$ctx"/debs/*.deb
cp "$HERE/Dockerfile" "$HERE/fake_docker.sh" "$ctx/" || exit 2

if [ -n "$DEB_DIR" ]; then
    [ -d "$DEB_DIR" ] || { echo "smoke: DEB_DIR $DEB_DIR does not exist" >&2; exit 2; }
    cp "$DEB_DIR"/*.deb "$ctx/debs/" 2>/dev/null
    # slurmctld lives in full-set/ in the fleet's build; the node packages do
    # not carry it and a single-node cluster is still a cluster.
    for extra in slurmctld; do
        cp "$DEB_DIR"/full-set/${extra}_*.deb "$ctx/debs/" 2>/dev/null
    done
    echo "smoke: installing $(ls "$ctx/debs" | wc -l) packages from $DEB_DIR"
else
    echo "smoke: installing slurm-wlm from the Ubuntu 24.04 archive"
fi

echo "smoke: building $IMAGE"
docker build -q -t "$IMAGE" "$ctx" >"$run/build.log" 2>&1 || {
    echo "smoke: image build failed; see $run/build.log" >&2
    tail -n 40 "$run/build.log" >&2
    exit 2
}

name="pb-slurm-smoke-$stamp"
echo "smoke: running $name (repo read-only, volume $vol)"
docker run --rm --name "$name" \
    --privileged --cgroupns=private \
    --hostname pbsmoke \
    -v "$REPO":/repo:ro \
    -v "$vol":/mnt/shared \
    -e PB_SMOKE_REPO=/repo \
    -e PB_SMOKE_VOL=/mnt/shared \
    "$IMAGE" bash /repo/fleet/slurm/smoke/inside.sh 2>&1 | tee "$run/smoke.log"
status="${PIPESTATUS[0]}"

echo "smoke: transcript $run/smoke.log (exit $status)"
if [ "$KEEP" = "0" ]; then
    # The container is --rm; the run directory is what a failing row is read
    # out of afterwards, so it stays.  Only the build context is disposable.
    :
fi
exit "$status"
