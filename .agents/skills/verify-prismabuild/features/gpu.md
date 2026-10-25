# NVIDIA qualification

## Sub-features

Admitted CUDA visibility, device allocation, fixed-work matrix multiplication, and a numerical result returned to the CPU.

## How to get to it (user POV)

From a task checkout, run `python3 tools/verify.py gpu`.

## Driving it with the CLI

Run the skill's GPU command. Require `PASS`, `cas_verified: true`, and no skips. Retain the `prismabuild-gpu-qualification` line from `pbtest.json`, the device identity, and equal actual and expected checksums.

## Gotchas

A GPU host tag alone grants no GPU reservation. The command declares GPU demand through PB. Missing CUDA, missing PyTorch, or an invisible GPU fails the check. Plain CPU pytest skips this explicitly opt-in module. This gate qualifies this fixed CUDA workload and PB transport; it does not qualify a model, serving, performance, or a deployment rollout. PB accounts for external GPU load, including vLLM.
