#!/bin/bash
# Phase 1 of docs/slurm_runbook_2026-09-04.md, as one script per box.
#
# Run it as root on each of the three fleet boxes, controller first:
#
#     sudo bash fleet/slurm/install.sh                 # on dl380g10
#     scp /home/rob/.munge-key.b64 sparky:/home/rob/   # as rob, not as root
#     sudo bash fleet/slurm/install.sh                 # on sparky
#     scp /home/rob/.munge-key.b64 sparklina:/home/rob/
#     ssh -t sparklina sudo bash .../fleet/slurm/install.sh
#
# One script rather than three because the three boxes differ in exactly two
# places -- where SLURM's packages come from, and whether a controller runs --
# and a per-box script is three places for one runbook step to rot.  What the
# box is, is asked of the box: `hostname -s`, which is also what NodeName= in
# slurm.conf must equal, so a box this script does not recognize is a box
# slurm.conf does not describe either.
#
# It is idempotent.  Every step asks whether it is already done and says so
# rather than redoing it, because the realistic way this is run is twice: once
# to the first refusal, and again after the refusal is fixed.
#
# It refuses rather than repairs.  A `slurm` user at the wrong uid, a SLURM
# older than the controller's 25.11, a topology the box does not report -- each
# stops the run naming the step, because each is a decision somebody has to
# make and none of them is one a script should make quietly.  A wrong uid
# silently breaks controller/node state exchange; a wrong topology brings the
# node up DRAINED with a message naming neither file.
#
# --dry-run prints every command it would run, verbatim, and runs none of them.
# It needs no root and no packages: the already-done checks are reported and
# treated as not-yet-done, so the output is the complete list for a fresh box.

set -uo pipefail

DRY_RUN=0
STEP="startup"

usage() {
    cat <<'USAGE'
usage: install.sh [--dry-run]

Installs SLURM 25.11 and munge on this box, per its hostname:

  dl380g10    controller and CPU node; SLURM from apt; creates the munge key
              and leaves a base64 copy at /home/rob/.munge-key.b64 for the
              operator to scp to the Sparks
  sparky      GPU node; SLURM from the prebuilt debs in
  gx10-6b77   /home/rob/slurm-build/arm64-24.04; installs the munge key from
              /home/rob/.munge-key.b64 and shreds it

Run as root.  --dry-run needs no root and changes nothing.
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'install.sh: unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

# -- saying and doing --------------------------------------------------------
#
# Every line this prints is either a command it ran (or would run) or a comment
# starting with `#`.  That is the contract --dry-run rests on and the one the
# tests read: an operator can paste the output of a dry run and get the same
# install.

#: The transcript goes to fd 3, a dup of stdout taken before anything runs.
#: Without it a command read back with `$(capture ...)` would swallow its own
#: printed line into the variable, and a dry run would silently omit exactly
#: the commands whose output the script reads.
exec 3>&1

say() { printf '%s\n' "$*" >&3; }

die() {
    printf 'install.sh: FAILED at step %s: %s\n' "$STEP" "$*" >&2
    exit 1
}

step() {
    STEP="$1"
    shift
    printf '\n# step %s: %s\n' "$STEP" "$*" >&3
}

#: One argument, quoted only if it needs it, so the printed line is pasteable.
quote_one() {
    case "$1" in
        # Anything outside this set gets single-quoted.  `*` is inside it on
        # purpose: the only argument that carries one is the deb glob, and a
        # quoted glob is not the command that runs.
        *[!A-Za-z0-9_@%+=:,./*-]*) printf "'%s'" "${1//\'/\'\\\'\'}" ;;
        "") printf "''" ;;
        *) printf '%s' "$1" ;;
    esac
}

quote() {
    local first=1 token
    for token in "$@"; do
        [ "$first" = 1 ] || printf ' '
        first=0
        quote_one "$token"
    done
}

#: Print a command, then run it unless this is a dry run.  A failure aborts the
#: install naming the step, which is the whole error-handling policy here.
run() {
    { quote "$@"; printf '\n'; } >&3
    if [ "$DRY_RUN" = 0 ]; then
        "$@" || die "command failed: $(quote "$@")"
    fi
}

#: The same, for a line that needs a shell: a redirect, or the deb glob.  What
#: is printed is exactly what is executed.
run_shell() {
    printf '%s\n' "$1" >&3
    if [ "$DRY_RUN" = 0 ]; then
        bash -c "$1" || die "command failed: $1"
    fi
}

#: Read something back from the box.  Prints the command, returns its stdout.
#: In a dry run it prints and returns nothing, so every caller has to treat an
#: empty answer as "not checkable here" rather than as a fact.
capture() {
    { quote "$@"; printf '\n'; } >&3
    [ "$DRY_RUN" = 0 ] || return 0
    "$@" 2>/dev/null
}

#: An idempotence gate.  True when the step is already done, and always false
#: in a dry run so the output is the full list for a fresh box.
already() {
    { printf '# already-done check: '; quote "$@"; printf '\n'; } >&3
    [ "$DRY_RUN" = 0 ] || return 1
    "$@" >/dev/null 2>&1
}

# -- what box is this --------------------------------------------------------

NODE="$(hostname -s)"
case "$NODE" in
    dl380g10) ROLE=controller ;;
    sparky|gx10-6b77) ROLE=spark ;;
    *)
        printf 'install.sh: %s is not a fleet box (expected dl380g10, sparky or gx10-6b77)\n' \
            "$NODE" >&2
        exit 2
        ;;
esac

#: The configuration files this installs are the ones sitting next to this
#: script, not a path relative to a checkout: no box but sparky has a
#: prismabuild checkout, and `fleet/` is not among the files publish_runtime
#: mirrors to /mnt/shared, so the realistic way this reaches dl380g10 and
#: sparklina is a copy of this one directory.
CONF_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SLURM_UID=64030
SLURM_GID=64030
SLURM_VERSION_PREFIX="25.11."
DEB_GLOB="/home/rob/slurm-build/arm64-24.04/*.deb"
#: The munge key travels as base64 in rob's home directory and nowhere else.
#: Not /mnt/shared -- it is the fleet's shared secret and an NFS export is the
#: wrong place for one.  Not /tmp -- an out-of-memory event cleared that on
#: this fleet once already.
KEY_B64="/home/rob/.munge-key.b64"
KEY_OWNER="rob"
LANE_JOBS="/mnt/shared/prismabuild-fleet/slurm/jobs"
CONFIGS_644="slurm.conf gres.conf cgroup.conf"

say "# prismabuild SLURM install"
say "# box        : $NODE ($ROLE)"
say "# configs    : $CONF_SRC"
say "# mode       : $([ "$DRY_RUN" = 1 ] && echo 'dry run, nothing is executed' || echo 'live')"

if [ "$DRY_RUN" = 0 ] && [ "$(id -u)" -ne 0 ]; then
    STEP="preflight"
    die "this installs system packages and writes /etc/slurm; run it as root (sudo bash $0)"
fi

for name in $CONFIGS_644 epilog.sh; do
    [ -f "$CONF_SRC/$name" ] || die "checkout is missing $CONF_SRC/$name"
done

# -- step 1: the slurm user, its group, and its directories -------------------

step 1 "the slurm user and group at uid/gid $SLURM_UID, and the spool and log directories"

if already getent group slurm; then
    say "# the slurm group exists"
else
    run groupadd -g "$SLURM_GID" slurm
fi

if already id -u slurm; then
    say "# the slurm user exists"
else
    run useradd -u "$SLURM_UID" -g slurm -s /usr/sbin/nologin -M \
        -d /var/spool/slurm slurm
fi

run install -d -m 755 -o slurm -g slurm /var/spool/slurm
run install -d -m 755 -o slurm -g slurm /var/spool/slurm/ctld
run install -d -m 755 -o slurm -g slurm /var/spool/slurm/d
run install -d -m 755 -o slurm -g slurm /var/log/slurm

# -- step 2: packages --------------------------------------------------------
#
# munge is installed in the same apt invocation as SLURM on both routes rather
# than in a step of its own: the Sparks' debs Recommend munge instead of
# depending on it (see /home/rob/slurm-build/BUILD.md), so it has to be named
# on that command line anyway, and one apt run is one place for apt to fail.

step 2 "munge and SLURM 25.11 packages"

run apt-get update

case "$ROLE" in
    controller)
        # 25.11.2 is what Ubuntu 26.04 carries, which is why this box is the
        # controller and why the Sparks had to be given a matching build.
        run env DEBIAN_FRONTEND=noninteractive apt-get install -y \
            slurm-wlm slurmctld slurmd munge
        ;;
    spark)
        # Ubuntu 24.04's own slurm-wlm is 23.11.4, which a 25.11 controller
        # refuses to talk to.  These are the 25.11.2 rebuild of Ubuntu 26.04's
        # source package, built on sparky and present on both Sparks.
        if [ "$DRY_RUN" = 0 ] && ! compgen -G "$DEB_GLOB" >/dev/null; then
            die "no packages at $DEB_GLOB; see /home/rob/slurm-build/BUILD.md"
        fi
        run_shell "env DEBIAN_FRONTEND=noninteractive apt-get install -y $DEB_GLOB munge"
        ;;
esac

# -- step 3: the munge key ---------------------------------------------------
#
# Installing the `munge` package already put a key on this box.  Measured
# 2026-09-05 in an ubuntu:24.04 container: after step 2, /etc/munge/munge.key
# exists, 128 bytes, munge:munge, generated by the package's own postinst.  It
# is a perfectly good key and it is the wrong one -- it is this box's, and the
# fleet needs one key on all three.  So "a key is present" is not evidence that
# the fleet's key is installed, and this step does not treat it as any.
#
# What is evidence is the stamp below: the sha256 of the key this script
# installed, written beside it.  A key with a matching stamp is the fleet's; a
# key without one is the package's, and on a Spark that is a refusal rather
# than a skip.

step 3 "the fleet's shared munge key"

MUNGE_KEY=/etc/munge/munge.key
MUNGE_STAMP=/etc/munge/prismabuild-fleet-key.sha256

#: True when the key on this box is the one this script installed.
fleet_key_stamped() {
    {
        printf '# already-done check: '
        printf 'sha256sum %s matches %s\n' "$MUNGE_KEY" "$MUNGE_STAMP"
    } >&3
    [ "$DRY_RUN" = 0 ] || return 1
    [ -f "$MUNGE_STAMP" ] && [ -f "$MUNGE_KEY" ] || return 1
    [ "$(sha256sum < "$MUNGE_KEY" | cut -d' ' -f1)" = "$(cat "$MUNGE_STAMP")" ]
}

stamp_fleet_key() {
    run_shell "umask 077 && sha256sum < $MUNGE_KEY | cut -d' ' -f1 > $MUNGE_STAMP"
    run chmod 600 "$MUNGE_STAMP"
}

case "$ROLE" in
    controller)
        # This box defines the fleet's key, so whatever key is here is it --
        # including the one the package generated.  Only a box with no key at
        # all needs one made.
        if already test -f "$MUNGE_KEY"; then
            say "# using the key already on this box as the fleet's key"
        else
            run install -d -m 700 -o munge -g munge /etc/munge
            run /usr/sbin/mungekey --create
            run chown munge:munge "$MUNGE_KEY"
            run chmod 400 "$MUNGE_KEY"
        fi
        stamp_fleet_key
        # Exported on every run, including one that kept an existing key: the
        # reason to be here again may be that a Spark needs the copy.
        say "# the operator copies this to each Spark as rob:"
        say "#     scp $KEY_B64 sparky:$KEY_B64"
        say "#     scp $KEY_B64 sparklina:$KEY_B64"
        run_shell "umask 077 && base64 $MUNGE_KEY > $KEY_B64"
        run chown "$KEY_OWNER:$KEY_OWNER" "$KEY_B64"
        run chmod 600 "$KEY_B64"
        ;;
    spark)
        if [ "$DRY_RUN" = 0 ] && [ ! -f "$KEY_B64" ]; then
            if fleet_key_stamped; then
                say "# the fleet's key is already installed; nothing to copy"
            else
                die "no munge key at $KEY_B64, and the key on this box is not one this script installed (it is the munge package's own). Run install.sh on dl380g10 first, then, as rob: scp $KEY_B64 $NODE:$KEY_B64"
            fi
        else
            # The b64 copy wins over whatever is on disk.  It is the fleet's
            # key by construction and the key here may be the package's.
            run install -d -m 700 -o munge -g munge /etc/munge
            run_shell "umask 077 && base64 -d $KEY_B64 > $MUNGE_KEY"
            if [ "$DRY_RUN" = 0 ] && [ ! -s "$MUNGE_KEY" ]; then
                die "the key decoded from $KEY_B64 is empty; re-copy it from dl380g10"
            fi
            run chown munge:munge "$MUNGE_KEY"
            run chmod 400 "$MUNGE_KEY"
            stamp_fleet_key
            # The secret does not stay lying around in a home directory once
            # it is where it belongs.
            run shred -u "$KEY_B64"
        fi
        ;;
esac

# -- step 4: refuse a SLURM or a slurm user that cannot join this fleet -------

step 4 "refuse unless SLURM is ${SLURM_VERSION_PREFIX}x and the slurm user is uid $SLURM_UID"

installed_version="$(capture slurmd -V)"
if [ "$DRY_RUN" = 0 ]; then
    case "$installed_version" in
        *" ${SLURM_VERSION_PREFIX}"*) say "# $installed_version" ;;
        "") die "slurmd -V printed nothing; the packages did not install" ;;
        *) die "this box has '$installed_version'; the controller is ${SLURM_VERSION_PREFIX}x and 25.11 accepts slurmd from 25.05, 24.11 and 24.05 only" ;;
    esac
fi

installed_uid="$(capture id -u slurm)"
if [ "$DRY_RUN" = 0 ] && [ "$installed_uid" != "$SLURM_UID" ]; then
    die "the slurm user is uid '$installed_uid', not $SLURM_UID; the controller and the nodes exchange state as that user and every box must agree"
fi

# -- step 5: does the box answer to the stanza that describes it? -------------
#
# Asked before anything is written to /etc/slurm, so a box that does not match
# its stanza is refused rather than configured and then refused.  The four
# numbers are the ones task/affinity binds against, and the address is the one
# the controller will dial; RealMemory and Gres are
# deliberately not compared, because RealMemory here is the fleet's admission
# budget rather than the box's physical memory, and Gres is declared in
# gres.conf rather than detected.

step 5 "cross-check slurmd -C and this box's addresses against its NodeName= stanza"

#: One NodeName stanza with its backslash continuations joined.
node_stanza() {
    awk '
        /\\$/ { sub(/\\$/, "", $0); buf = buf $0 " "; next }
        { print buf $0; buf = "" }
    ' "$2" \
        | sed 's/^[[:space:]]*//' \
        | grep "^NodeName=$1[[:space:]]" \
        | head -n 1
}

#: One `Key=value` out of such a line.
stanza_field() {
    printf '%s\n' "$1" | tr ' ' '\n' | sed -n "s/^$2=//p" | head -n 1
}

declared="$(node_stanza "$NODE" "$CONF_SRC/slurm.conf" | tr -s ' ')"
[ -n "$declared" ] || die "$CONF_SRC/slurm.conf declares no NodeName=$NODE"
say "# declared: $declared"

detected="$(capture slurmd -C | grep '^NodeName=' | head -n 1)"
if [ "$DRY_RUN" = 0 ]; then
    [ -n "$detected" ] || die "slurmd -C printed no NodeName line"
    say "# detected: $detected"
    for key in CPUs SocketsPerBoard CoresPerSocket ThreadsPerCore; do
        want="$(stanza_field "$declared" "$key")"
        got="$(stanza_field "$detected" "$key")"
        # slurmd -C spells a single-board machine's socket count
        # SocketsPerBoard; older and future spellings say Sockets.
        if [ -z "$got" ] && [ "$key" = SocketsPerBoard ]; then
            got="$(stanza_field "$detected" Sockets)"
        fi
        [ -n "$want" ] || die "slurm.conf's NodeName=$NODE declares no $key"
        [ -n "$got" ] || die "slurmd -C reported no $key"
        [ "$want" = "$got" ] || die "$key: slurm.conf says $want, this box reports $got. Fix fleet/slurm/slurm.conf and republish it to every box; a node whose stanza and hardware disagree comes up DRAINED with 'Low socket*core*thread count'"
    done
    say "# CPUs, SocketsPerBoard, CoresPerSocket and ThreadsPerCore all agree"
fi

# The address half of the same question.  slurm.conf carries an explicit
# NodeAddr for every node and an address on SlurmctldHost, because on this
# fleet the names do not resolve to what SLURM needs -- see that file's
# Addresses note for the measurement.  They are DHCP leases rather than
# reservations, so this is the place where a lease that moved becomes a
# refusal naming the step, instead of a node that quietly never registers.

declared_address="$(stanza_field "$declared" NodeAddr)"
[ -n "$declared_address" ] || die "slurm.conf's NodeName=$NODE declares no NodeAddr. The controller cannot resolve either Spark by name, so an address is not optional here"

box_addresses="$(capture ip -4 -o addr show scope global)"
if [ "$DRY_RUN" = 0 ]; then
    if ! printf '%s\n' "$box_addresses" \
        | awk '{print $4}' | cut -d/ -f1 | grep -qx "$declared_address"; then
        die "slurm.conf gives NodeName=$NODE the address $declared_address, and this box does not hold it:
$(printf '%s\n' "$box_addresses" | awk '{printf "  %s %s\n", $2, $4}')
A DHCP lease probably moved.  Fix NodeAddr in fleet/slurm/slurm.conf and
install it on all three boxes; the NFS export in fleet/slurm/epilog.sh pins
the same addresses, so check that too."
    fi
    say "# NodeAddr $declared_address is one of this box's addresses"
fi

# -- step 6: the configuration -----------------------------------------------
#
# Four files.  There is no cgroup_allowed_devices_file.conf: on cgroup v2 SLURM
# parses AllowedDevicesFile only to warn about it (25.11.2,
# src/interfaces/cgroup.c:416-419), and containment is an eBPF program that
# denies exactly the GRES File= devices this job was not allocated and admits
# everything else.  A file listing the NVIDIA control interfaces was answering
# a question the kernel no longer asks.

step 6 "install the four configuration files into /etc/slurm"

run install -d -m 755 /etc/slurm
for name in $CONFIGS_644; do
    run install -m 644 "$CONF_SRC/$name" "/etc/slurm/$name"
done
run install -m 755 "$CONF_SRC/epilog.sh" /etc/slurm/epilog.sh

# -- step 7: the lane's job-state directory ----------------------------------
#
# On the shared mount, and therefore NOT as root: dl380g10 exports
# /storage_pool/shared without no_root_squash, so root is `nobody` there and
# its mkdir fails.  Every write below the lane root is the job user's, which is
# the same reason epilog.sh deletes its state file with runuser.

step 7 "the SLURM lane's job-state directory on the shared mount"

if already test -d "$LANE_JOBS"; then
    say "# $LANE_JOBS exists"
else
    run runuser -u "$KEY_OWNER" -- mkdir -p "$LANE_JOBS"
fi
run runuser -u "$KEY_OWNER" -- chmod 1777 "$LANE_JOBS"

# -- step 8: the daemons -----------------------------------------------------
#
# enable then restart, rather than `enable --now`: a re-run after a
# configuration change has to pick the new configuration up, and `--now` on an
# already-running unit does nothing at all.

step 8 "enable and start munge, then SLURM"

run systemctl enable munge
run systemctl restart munge
run_shell "munge -n | unmunge | head -n 5"

if [ "$ROLE" = controller ]; then
    run systemctl enable slurmctld
    run systemctl restart slurmctld
fi

run systemctl enable slurmd
run systemctl restart slurmd

say ""
say "# done on $NODE."
case "$ROLE" in
    controller)
        say "# next: copy $KEY_B64 to each Spark as rob and run this script there."
        ;;
    spark)
        say "# next: when all three boxes are installed, run fleet/slurm/verify.sh as rob."
        ;;
esac
