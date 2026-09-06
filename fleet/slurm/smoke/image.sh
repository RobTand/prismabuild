#!/bin/bash
# Shared image identity check for admitted smoke harnesses. Source this file.
pb_smoke_verify_image() {
    local wanted="$1" actual
    [[ "$wanted" =~ ^sha256:[0-9a-f]{64}$ ]] || {
        echo 'smoke: prebuilt image must be a full immutable sha256 image ID' >&2
        return 2
    }
    actual="$(docker image inspect --format '{{.Id}}' "$wanted")" || return 2
    [ "$actual" = "$wanted" ] || {
        echo "smoke: image inspection returned $actual, expected $wanted" >&2
        return 2
    }
    printf '%s\n' "$actual"
}
