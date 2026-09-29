# Fleet canary timer (PB #688, #978)

## Why a timer

The fleet end-to-end canary (`tools/fleet/pbcanary.py`) needs the fleet
queue and `/mnt/shared`, which GitHub-hosted runners cannot reach. It used
to run from a self-hosted GitHub Actions workflow, `canary.yml`. That
workflow was informational and could not see a generation that was staged
in PrismaBuild and activated outside it, so an activated generation could
sit without a verdict. It is removed.

A systemd timer on the dl380 replaces it. The dl380 (`dl380g10` in
`tools/fleet/fleet_boxes.json`) is always on and holds `/mnt/shared` as a
local dataset.

## What runs

`tools/fleet/pbcanary_watch.py` resolves the live `repo` link to the active
generation and reads the generation's verdict sidecar,
`runtime-generations/<generation>.canary.json`. The publisher writes the
same sidecar.

- With no verdict, a verdict that never reported (a `pending` record older
  than six hours), or an unreadable record, it runs `pbcanary.py
  --generation <generation>` and records the result.
- With a verdict and no `--nightly`, it exits 0 and runs nothing.
- With `--nightly`, it runs the canary regardless.

The driver's exit status is the watcher's exit status: 0 verified, 1 a leg
failed, 2 the canary could not run. The sidecar records `verified`,
`failed` or `not_run` to match.

`pbstatus` shows the state in a `== canary` section, and as a `canary` key
in `--json`. An active generation with no verdict prints
`missing canary verdict for generation <name>`.

Units in `fleet/maintenance/`:

- `prismabuild-canary.timer` runs `prismabuild-canary.service` at boot and
  every 10 minutes.
- `prismabuild-canary-nightly.timer` runs
  `prismabuild-canary-nightly.service` daily at 05:30 UTC-local time.

Both services set `PBCANARY_GPU_IMAGE` to a `name@sha256` digest pin. The
host must be able to `docker run --entrypoint "" <that ref>` with
`--gpus all`.

## Install

The units are not installed by merging. On the dl380, as the user that owns
the `prismabuild` checkout:

```
mkdir -p ~/.config/systemd/user
cp ~/prismabuild/fleet/maintenance/prismabuild-canary*.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now prismabuild-canary.timer prismabuild-canary-nightly.timer
systemctl --user list-timers 'prismabuild-canary*'
```

Proof: `python3 tools/fleet/pbstatus.py` shows `== canary` with the active
generation, and `~/prismabuild/tools/fleet/pbcanary_watch.py` run by hand
writes the sidecar.

## Maintenance

A missing canary verdict in `pbstatus` is an ops alert, not a canary pass.
The checkout the timer runs from must be at or ahead of the active
generation's commit, because it supplies `pbcanary.py`.
