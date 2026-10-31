#!/bin/bash
set +x
set -euo pipefail
umask 077

slots=${1:-1}
[[ "$slots" =~ ^[012]$ ]] || { echo 'Usage: install.sh [0|1|2]' >&2; exit 1; }
[[ $(id -u) != 0 ]] || { echo 'Install as the rootless runner owner' >&2; exit 1; }
host=$(hostname -s)
case "$host:$(uname -m)" in
    dl380g10:x86_64) arch=amd64; affinity=(40-79 40-79); registration_tool=gh ;;
    sparky:aarch64|sparklina:aarch64)
        arch=arm64; affinity=(0-4 10-14); registration_tool=ssh
        free -g
        ;;
    *) echo 'Unsupported PB runner host or architecture' >&2; exit 1 ;;
esac
for tool in podman systemctl systemd-notify "$registration_tool"; do
    command -v "$tool" >/dev/null
done
[[ $(podman info --format '{{.Host.Security.Rootless}}') == true ]] || exit 1
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# Image builds are infrastructure provisioning. Do not use this for test runs.
podman build --cpu-period=100000 --cpu-quota=200000 --memory=6g \
    --build-arg "TARGETARCH=$arch" --tag localhost/pb-ci-runner:py314 "$here"
image=$(podman image inspect localhost/pb-ci-runner:py314 --format '{{.Id}}')
image="sha256:${image#sha256:}"
[[ "$image" =~ ^sha256:[a-f0-9]{64}$ ]] || exit 1
install -d -m 700 "$HOME/.local/lib/pb-ci-runners" "$HOME/.config/pb-ci-runners"
install -d -m 755 "$HOME/.config/systemd/user"
install -C -m 700 "$here/runner-slot.sh" "$HOME/.local/lib/pb-ci-runners/runner-slot.sh"
install -C -m 644 "$here/pb-ci-runner@.service" "$HOME/.config/systemd/user/pb-ci-runner@.service"
for slot in 1 2; do
    config="$HOME/.config/pb-ci-runners/slot-$slot.env"
    temporary=$(mktemp "$HOME/.config/pb-ci-runners/.slot-$slot.XXXXXX")
    printf 'PB_CI_IMAGE=%s\n' "$image" > "$temporary"
    chmod 600 "$temporary"
    mv -f "$temporary" "$config"
    dropin="$HOME/.config/systemd/user/pb-ci-runner@$slot.service.d"
    install -d -m 755 "$dropin"
    printf '[Service]\nCPUAffinity=%s\n' "${affinity[$((slot-1))]}" > "$dropin/placement.conf"
done
systemctl --user daemon-reload
for ((slot=1; slot<=slots; slot++)); do
    systemctl --user enable --now "pb-ci-runner@$slot.service"
done
# Keep old images available for rollback. Never prune this shared image store.
printf 'Installed PB image %s on %s; requested %s active slots\n' "$image" "$host" "$slots"
