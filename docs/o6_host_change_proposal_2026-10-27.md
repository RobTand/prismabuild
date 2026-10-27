# O6 dedicated scratch host proposal

Owner: PB #2383. This proposal does not authorize host changes, publication,
activation, a standing VM, or an O6 launch. Rob's 2026-10-27 assignment requires
operator authorization for this dedicated dataset before mutation.

## Decisions already approved

TL2 D-0503/D-0511 approve a per-action QEMU/KVM VM with 8 vCPUs, 32 GiB RAM,
a 48-GiB OS overlay and a 256-GiB guest data disk. The outer action declares
9 CPUs and 36 GiB, including its driver, and a 320-GiB scratch ceiling.
It requests no GPU or exclusive claim. No standing VM, bridge, TAP, libvirt
domain or new group is needed. D-0501/D-0507's production `tl2` identity and
three production datasets are separate approvals and remain untouched.

## Read-only host inventory

Inspected dl380g10 over SSH on 2026-10-27, without tests or mutation:

- `storage_pool/tl2-qualification-vm` does not exist.
- `storage_pool` reports 12,934,889,613,952 available bytes. This observation
  is not an allocation or a future capacity guarantee.
- The parent has `sync=standard`, `canmount=on`, and no quota or reservation.
- `/storage_pool` is root:root 0755.
- `/tmp/pbscratch` is rob:rob 0700 on btrfs, backed by
  `/dev/nvme1n1p4[/var/lib/prismabuild-scratch]`.
- A complete PB observation reports dl380g10 `spool_gb=8`. That backing and
  offer remain unchanged.
- `/dev/kvm` is root:kvm 0660; the existing kvm group includes rob.
  QEMU executables are root:root 0755. Admitted read-only action
  `8003f734600e30dd6523029d793a5940f6980864293ab580cb30c010214daf00`
  opened KVM and observed API version 12. Its overall result was blocked
  because the dedicated root and dataset were absent. It booted no VM and
  made no disk writes. No software-emulation fallback is accepted.

The supervisor is not a `prismabuild-supervisor.service` unit on this host.
An empty result from inspecting that name proves no supervisor permissions.
The existing resource-broker service is active. Runtime and loaded-role
convergence require their own selected-field evidence.

## Requested operator change

Create exactly one dedicated dataset with a 384-GiB quota and reservation,
mounted at `/storage_pool/tl2-qualification-vm`. Keep host execution, setuid
and device nodes disabled there; QEMU accesses disk images as ordinary files.
Use the parent's standard sync behavior explicitly. Prepare only PB's private
namespace beneath the root. The commands below are a proposal, not executed:

```sh
zfs create -o mountpoint=/storage_pool/tl2-qualification-vm \
  -o quota=384G -o reservation=384G -o sync=standard \
  -o exec=off -o setuid=off -o devices=off \
  storage_pool/tl2-qualification-vm
chown root:root /storage_pool/tl2-qualification-vm
chmod 0755 /storage_pool/tl2-qualification-vm
install -d -o rob -g rob -m 0700 \
  /storage_pool/tl2-qualification-vm/prismabuild-ephemeral
```

384 GiB is 412,316,860,416 bytes. The planned PB root-specific offer is at most
320 GiB, 343,597,383,680 bytes, with a 32-GiB minimum free-space floor and an
actual descriptor-based capacity observation. The nominal margin is 64 GiB;
the smaller floor leaves room for the dataset's own metadata without rounding
the usable offer below 320 GiB. Quota/reservation readback and
the measured root identity must agree before enabling that offer. Capacity
is independent of ordinary spool; measured I/O profiles do not supply it.
This is reservation accounting for trusted O6 code. It does not claim an
arbitrary-write per-action quota or a hostile-code guarantee.

## Verification and lifetime

After approval, read back exact dataset properties, mount, owner and mode.
An admitted PB action verifies local ZFS identity, immutable creation time,
available capacity, sync/flush behavior, protected namespace, and contained
KVM access. The dedicated-root admission proof must also show a 320-GiB
reservation, refusal of overcommit, ordinary spool still at 8 GiB, exact
stopped-attempt deletion and retained capacity on ambiguous cleanup.

All VM files belong in the registered ephemeral leaf addressed through PB's
scratch helper. The root and TMPDIR are not automatically redirected. PB
owns stop proof and deletion after success, failure and interruption. TL2
supplies no sweeper. A cleanup error retains the claim and its capacity.

Before runtime activation, PB also registers this local root and the exact
member `reservations/dl380g10:scratch_gb:61f4329524dcd3e9624832f8cabb7f1bdd656cd64cc9d77b7663aa5d2a713865`
with the used-filesystem floor, through an admitted action on dl380g10.
The registration interface is
`python -m prismabuild.filesystem_floor register /storage_pool/tl2-qualification-vm --member MEMBER --coverage-json PATH`.
Enforce mode requires an authority-supplied coverage snapshot for this exact
binding. Its inputs and activation approval are not supplied by this dataset
proposal; diagnostic registration alone does not satisfy that gate. PB must
retain the current floor policy and record an unbound/unknown refusal if
coverage is missing. See the operating guide's floor section and coverage
reference. The measured I/O profile, capacity config and floor binding are
three separate inputs.

Dataset provisioning, source merge, runtime activation and native O6
qualification are separate gates. No large O6 action may be submitted until
backing and deployed PB admission are ready. Rollback requires proof that
no action or cleanup debt owns this root; this proposal authorizes no
automatic dataset destruction.
