# Issue 201 dispatcher validation — 2026-09-06

39 CPU tests passed through PrismaBuild across four shards, with no skips or
missing tools. Tests cover partition derivation, verified source cache reuse,
source mutation, output/receipt mismatch, the failure barrier, campaign and
runtime publication behavior. All terminal records, returned receipts and
actual CAS payload hashes were checked.

| Suite | Result | Action key |
|---|---|---|
| tests/test_tessera_model_dispatch.py | 9 passed in 1.42s | `c7ba6b2f8324a728c180738c76cbc732dc84f056625be2ac116081e471d20a07` |
| tests/test_pbcampaign.py | 15 passed in 2.67s | `c6cf2c5bdb3f64c19bda4784776a06a0167c1fefc55aa3237e81a2d6f812f815` |
| tests/test_publish_runtime.py | 10 passed in 1.92s | `fa78126a7a5b98dc4c5e68abd9dbf91c2d23f97fc9d8e41f969766e51c81660a` |
| tests/test_main_push_guard.py | 5 passed in 1.30s | `fe191cc6e99744fbfe52e4a404b74e30be021e74743985f224b66647087c41e1` |

GPU qualification used a deliberately tiny, two-layer LFM-shaped checkpoint
with two 128×128 BF16 source matrices, encoded as E4M3/q1024 using Tessera
commit `8c3a38935cb105c49e896edd67d193998717f203` and
`eugr/spark-vllm@sha256:0afec8d4f79f44685a1ddf758659d33aef3b0f3ec9068e5a7cd1108d30e5581c`.
The final integrated smoke workspace is `/home/rob/tmp/pb201-model-smoke-final`;
the output is `/mnt/shared/pb-issue201-smoke-lfm/output-final`.

| Stage | Worker | Elapsed seconds | Action key |
|---|---|---:|---|
| prepare | sparklina | 0.4 | `2e89dd7829ec94b76bf8611b65649b433f6a535a12d25ade56dbb788d2ae707a` |
| prepare | sparky | 0.4 | `7d7e0f75774c26c12678990d2e75ddb8d1a0b5b32b3befca26539277ac5db0ec` |
| encode | sparklina | 68.8 | `d6980e99f299caf01ca21ebfa33158eb3a780f412e3769c1abe6e584a29a5d36` |
| encode | sparky | 71.1 | `e34f2e56581b72147e8eb9424abe829cc991030e9b3c969ed01a80b574c8985d` |
| assemble | sparklina | 1.3 | `4a52bac29aadbd3fc6ef394feef36d0baadd5a5c8af56aaf8a20a367913aa27d` |

The encode claims overlapped for 54.8 seconds on separate GB10 workers. Each
reserved one CPU, one GPU and 16 GiB. Broker accounting recorded about 4.42 GiB
peak memory per encode and complete exact-scope cleanup. CPU preparation checked
the exact image/source on both workers; assembly ran only after both encode receipts.
All five stage receipts and payload hashes were independently verified. Resume
reported CAS hits for both encodes and assembly, with unchanged action keys and
no new GPU execution.

The final source adds explicit one-job native build limits and fail-closed final
artifact-metadata verification after this GPU run; the CPU release checks above
cover the final source. The smoke validates export/distribution/assembly plumbing,
not the full 8B model, serving quality, GPU saturation or a performance improvement.

Negative evidence and retained artifacts:

- The legacy GLM dispatcher refused the full-source interface in admitted action
  `3ac3b59241e5b985dd791aee80b820b6e222bb6b5dbe96f5bd9a03c6bf0897a8`. It requires
  caller-provided GLM shard indices and has no full-model contract.
- Initial smoke mounts failed because private ancestor directories on the test
  NFS path could not be traversed by Docker. Fixture paths were corrected; no
  fleet-wide permissions or source-model permissions were weakened.
- A Llama-shaped fixture was refused by the producer construction census. The
  qualification was corrected to a supported LFM-shaped fixture; no serving
  gate or qualification override was added.
- Preparation detected a source file-identity change on one worker. A resumed
  preparation obtained matching stable identities; the other worker reused its
  receipt. The first refusal remains in the attempt history.
- Failed workspaces under `/home/rob/tmp/pb201-model-smoke*` and their small
  shared fixture outputs are retained as bounded debugging evidence.

Full-workload acceptance remains open: the final mixed LFM plan/static scales
and qualified producer reference have not been supplied. The image digest in
the issue was not installed on Sparky when inspected. The published entrypoint
does not infer calibration data, replace the producer image, or claim that the
full export has completed.
