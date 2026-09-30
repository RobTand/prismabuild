# Experimental checkout-entry timestamp refusal

This record covers the private diagnostic verifier in
`tools/maintenance/diag_811_e1_checkout_cache.py`, not production checkout
admission, cache integration, runtime deployment, or performance.

## Finding and repair

`verify_entry` checks `created_unix` with `math.isfinite`. A valid JSON integer
can exceed the floating-point conversion range and raise `OverflowError`
before the verifier returns its structured publication refusal. This also
happens after a previously valid publication has populated the positive memo.

The repair catches `OverflowError` only around the timestamp predicate. It
then follows the existing named `created_unix` refusal, including memo
invalidation. It preserves the accepted types, excludes booleans, and retains
the existing publication schema, generation binding, pack/index checks, and
finite-value admission. It introduces no numeric cutoff or fallback.

## RED witness

PrismaBuild action
`c062c178130c6fd692a2ab0fbfc6f853a7d8c6bedd6cecbfdf855d5eac86297e`
terminated failed, exit 1, after one attempt on sparky: **4 failed, 12 passed,
0 skipped**. All 16 collected tests ran; reconciliation reported no missing
files, uncounted outcomes, or collection gaps. CPU-only, CUDA hidden; this
population makes no CUDA-surface claim.

All four added cases failed before the repair at:

```text
tools/maintenance/diag_811_e1_checkout_cache.py:399: OverflowError: int too large to convert to float
```

The input is `sign * (1 << sys.float_info.max_exp)`, derived from the existing
floating-point timestamp check's exponent range. The matrix covers both signs
and cold/primed memo states. Synthetic pack/index bytes and a Git double
isolate publication admission; they do not qualify a real Git pack.

## GREEN witness

PrismaBuild action
`09e7e090a11c34e879e48f4ad74ea01d9243522ed737b88b304fb045c8b842c5`
terminated done, exit 0, after one worker attempt on sparklina: **20 passed,
0 failed, 0 skipped**, 1.17 seconds of pytest. All 20 collected tests ran;
reconciliation reported no missing files or collection/outcome gaps. This
CPU-only run covered all three `tests/test_diag811_*.py` files, including
publication, generation and paired-boundary controls; CUDA was hidden.

Execution receipt:
`6ba86098ed1439ec615e6ff626d961668347acc88f2146c351f4046d3fe8ff62`.
The 5,635-byte result matches SHA-256
`43657df78f13797d1d7ec92b43bd8668d21307507fceb0fbe16fca800241ee8a`.
Local-result claim
`e99a59a2d3b9b7ce8f57b450389a89b1441b7545f2d9c53b8f7dfbaad1d46639`
passed all nine executed `pb_verify_claim(hash_payload=true)` checks.
Full worker attestation was not independently checked.

The client first timed out discovering worker offers and published nothing.
Its supported retry produced this single action; the terminal worker record
and immutable attempt record show one execution attempt. The client's two
submission attempts are not two worker executions. No agent resubmission or
placement override occurred.

## Scope and execution

The run reserves two CPUs and 2 GiB, one shard, two pytest-xdist workers with
`--dist worksteal`, native threads one, priority -10, and a 600-second PB
execution timeout. `--durations 20` records slow cases. The U4 window sentinel
is checked before each client. The coordinator's explicit GB10/pb-cpu self-test
route is used; no GPU is requested and no local tests are run.

This is a bounded refusal repair under PB #811 and staged-read ID-05. The
staged-read ledger and all other acceptance axes remain unchanged. The earlier
real-pack measurement remains historical evidence for its exact source;
there is no new benchmark or speedup claim. Production retention, reader
lifetime, retirement, crash recovery, and deployed claim-path before/after
acceptance remain open. PB #940's live holder-bound witness and the held
Tessera GPU arms are unaffected.
