# Self-hosted runner setup for pbcanary (PB #688, crew E)

## Why this runner exists

`.github/workflows/canary.yml` runs the fleet end-to-end canary
(`tools/fleet/pbcanary.py`, crew A). GitHub-hosted runners cannot reach
the fleet queue or `/mnt/shared`, so one self-hosted runner provides
the `fleet` label. Its only job is to check out the repo and **submit**
the canary through the real queue interfaces (`pbrun`); the legs
execute on fleet workers (sparky/sparklina), never as local processes
on the runner host.

## Host: dl380

- The dl380 (`dl380g10` in `tools/fleet/fleet_boxes.json`) is always on
  and is the file server: `/mnt/shared` is a local ZFS dataset here, so
  the queue roots, CAS, and canary namespaces are local paths, not
  network mounts.
- Run the GitHub Actions runner service on this host under an
  unprivileged user (e.g. `pb-runner`) that is a member of whatever
  group owns `/mnt/shared/prismabuild-fleet` writes. The canary writes
  only under `/mnt/shared/prismabuild-fleet/pb-canary/<run-id>/`.

## Registration

Repo-level runner for `RobTand/prismabuild` (repo scope is enough; no
org runner needed):

1. Repo → Settings → Actions → Runners → New self-hosted runner.
   Pick the Linux image matching the dl380 OS.
2. On the dl380, as the runner user:
   - `mkdir -p ~/actions-runner && cd ~/actions-runner`
   - Download and verify the runner package per GitHub's shown steps.
   - `./config.sh --url https://github.com/RobTand/prismabuild \
       --token <just-issued-token> \
       --name pb-canary-dl380 --labels fleet --unattended`
     - The `self-hosted` label is implicit; `--labels fleet` adds the
       `fleet` label the workflow's `runs-on: [self-hosted, fleet]`
       selects on. Do not add other labels.
     - Registration tokens are single-use and expire; re-issue from the
       same settings page when adding a replacement host.
3. `./svc.sh install` then `./svc.sh start` (or the equivalent
   `systemctl` unit). Verify the runner shows Idle (green) on the
   settings page.
4. Proof: Actions → pbcanary → Run workflow (leave generation empty) →
   the run leaves Queued, starts on `pb-canary-dl380`, checks out main,
   submits the canary, archives `pbcanary-summary-<run-id>`.

## Host prerequisites

- The canary's GPU leg reads `PBCANARY_GPU_IMAGE` from the job environment
  (set in `canary.yml` as versioned config — do not shadow it with a
  different value in the runner's service environment unless you also
  update the workflow). The host must be able to `docker run
  --entrypoint "" <that ref>` with `--gpus all`: the image must exist
  locally or be pullable, and the Docker daemon must have GPU device
  plumbing (`nvidia-container-toolkit`). Verified 2026-09-19 on sparky
  against `prismaquant-glm-derivative@sha256:c0e532d2…` (1 CUDA device).


- `git`, `python3` (whatever `tools/fleet/pbcanary.py` needs beyond the
  stdlib comes from the repo/fleet environment, not the runner image).
- `/mnt/shared/prismabuild-fleet` reachable at the same path the fleet
  uses (local dataset on dl380 — nothing to mount).
- Egress to github.com (checkout, artifact upload) and to the fleet
  queue. No ingress: the runner polls outbound only.

## Pre-registration behavior

The workflow file may merge before this runner registers. That is safe:
runs with unmatched `runs-on` labels stay Queued server-side until a
matching runner appears, then proceed normally. Nothing backfills and
nothing times out while queued (`timeout-minutes` starts at pickup).
Delete or re-run stale queued runs only if they predate the driver's
landing (crew A) — a queued run against a tree without
`tools/fleet/pbcanary.py` fails at the `Run pbcanary` step.

## Fork hardening

- Repo → Settings → Actions → General → **Fork pull request workflows**:
  require approval for first-time contributors (minimum; "require
  approval for all outside collaborators" is also acceptable). This
  workflow has no `pull_request` trigger, but the setting guards the
  repo's other workflows sharing the same runner pool.
- **No secrets on this runner beyond checkout + fleet mounts.** The
  workflow declares `permissions: contents: read`, checks out with
  `persist-credentials: false`, and configures no environment secrets.
  Do not add deployment keys, tokens, or cloud credentials to the
  runner host or the workflow; the canary needs none (low-priority
  queue submission is unauthenticated local-path work).
- The `GITHUB_TOKEN` available to the job is read-scoped; artifact
  upload uses that token, not a PAT.
- Keep this workflow informational: never add `pbcanary` as a required
  status check in branch protection, and never add a `pull_request`
  trigger to `canary.yml` (PR CI stays exactly as-is per #688
  non-goals).

## Maintenance

- Upgrade the runner package when GitHub marks it out of date
  (settings page shows the version); `./svc.sh stop`, replace package,
  `./svc.sh start`.
- If the runner goes offline, canary runs queue (see above) and the
  nightly verdict goes missing — treat a missing nightly verdict as an
  ops alert, not a canary pass.
