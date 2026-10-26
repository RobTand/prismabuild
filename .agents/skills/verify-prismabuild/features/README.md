# PrismaBuild feature map

- [Fast CPU verdict](fast.md): deterministic identities, CAS boundaries, manifest validation, and test-runner contracts.
- [CPU integration](integration.md): worker subprocesses, queue and broker behavior, filesystem lifecycles, and CLI composition.
- [NVIDIA qualification](gpu.md): admitted CUDA visibility and real tensor execution with numerical checks.

Tier membership is authoritative in `tools/verify.py`. New CPU test files enter integration automatically. The fast tier is a bounded gate, not a claim that every CPU path was tested.
