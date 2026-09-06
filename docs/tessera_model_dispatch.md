# Full-model Tessera exports

`dispatch_tessera_model.py` accepts a complete source checkpoint, explicit plan,
optional static input scales, immutable encoder commit and qualified producer
image. It derives whole-layer work units using Tessera's `serving_parts`
contract. A one-file checkpoint with 24 contiguous layers produces 24 encode
actions; an application does not choose partition indices, counts or hosts.

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/dispatch_tessera_model.py \
  --source /mnt/shared/models/LFM2.5-8B-A1B-BF16 \
  --plan /path/to/full-plan.json --input-scales /path/to/scales.safetensors \
  --encoder-checkout /path/to/tessera --encoder-revision FULL_COMMIT_SHA \
  --image REPOSITORY@sha256:DIGEST \
  --out /mnt/shared/models/lfm-mixed-export \
  --workspace /home/rob/tmp/lfm-export-dispatch
```

The initial implementation targets pool transport and the producer's existing
whole-layer, weights-only serving-part interface. It preserves fused groups,
expert stacks, passthrough ownership and the complete explicit plan. Hessian,
cached-expert and partial-layer experiments need explicit additional contracts;
they are not silently inferred from extra flags. The default dense fallback is
E4M3/q1024; explicit plan entries override it. NVFP4 plan members require real
static scales. No serving gate is bypassed.

The coordinator archives the requested encoder commit and snapshots the plan,
scales and PB adapter into a private Git workspace. Preparation actions verify
the exact image and source on every currently eligible GPU worker, and must
agree on source identity. Those CPU checks use host tags solely to verify that
host's dependencies. Encode actions retain the ordinary eligible class tags;
PrismaBuild's existing campaign, queue and admission own distribution. A missing
image or differing source stops preparation with the worker's explicit error.
New workers still check their image and source before encoding.

Source hashes, plan/scales, producer revision, image and partition domain bind
the export contract. Within a worker, a private locked cache reuses a source
hash only while its device/inode/size/mtime/ctime tuple and expected digest are
unchanged. Source inventory is checked and file identities are checked again
after export. This avoids rehashing a full checkpoint for every layer; Tessera
already materializes only tensors belonging to that layer. It does not provide
hostile-writer filesystem immutability or keep tensor residency across actions.

Sources and output must be shared paths below `/mnt/shared`. Ancestor directories
must be traversable by Docker's daemon, including under NFS root squashing.
Kernel compilation caches are retained per producer commit/image on each worker;
native and build threads are bounded. Containers bind source and sealed code
read-only, retain PB's assigned affinity
and resource scope, and use the exact locally installed image digest. The
adapter does not pull images or launch an independent scheduler.

Every action writes a private attempt directory, hashes its files, and publishes
a completed part. Failed attempt directories are removed by their own owner;
PB remains responsible for exact-scope process/container cleanup. Preparation,
encode and assembly submissions/endings/verified receipts are retained under
`WORKSPACE/.pb-state/`. Reports are excluded from the encoder checkout snapshot,
so recording a result cannot change the action identity. Assembly has a separate
checkout containing the exact receipt-derived barrier.

Assembly is submitted only after every partition has a successful verified CAS
receipt. It rehashes part payloads and invokes the producer's complete-set merge,
which verifies source, plan, encoder/image identities, tensor ownership, output
coverage, explicit-plan obligations and population before publishing a checkpoint.
An incomplete part is never exposed as a loadable model.

Resume a stopped dispatcher with the same workspace:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/dispatch_tessera_model.py \
  --workspace /home/rob/tmp/lfm-export-dispatch --resume
```

Completed actions reuse CAS receipts; unfinished actions use the existing PB
resubmission contract. A failure or wait leaves assembly blocked and preserves
the exact action keys and worker errors. Inspect the queue, terminal records,
resource telemetry and receipts before diagnosing idle GPUs. A passing unit test
or successful submission alone is not evidence of a completed model export.
