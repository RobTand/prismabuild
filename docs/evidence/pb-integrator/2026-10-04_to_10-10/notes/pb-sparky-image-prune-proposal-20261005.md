# Sparky docker images: read-only inventory and prune proposal (pb-integrator, 2026-10-05 ~19:50Z)

NOTHING WAS PUSHED OR PRUNED. This is a proposal for the CEO. Sources: `docker images`, `docker system df -v` on sparky, the registry at 192.168.1.107, Docker Hub and ghcr digest checks, and a scan of every PrismaBuild action request from the last 7 days (15,519 scanned; 3,068 name container images).

## The headline: the 175 GB estimate is the whole store, not what idle pruning frees
- Docker reports the whole image store as **186.2 GB** (22 images, 4 in use), of which **28.85 GB is reclaimable**. The big images share three huge base layers (about 30.6, 36.4 and 39.5 GB). Deleting one image frees only its unshared layers, usually under 1 GB; space comes back only when a whole family goes.
- Pruning images with no PB use in 7 days frees about **27.5 GB**. Adding the older registry-held nccl230 family frees about **67 GB**. Neither comes near the 120-150 GiB needed for the full 598.5 GiB BF16 source.
- The 78 GiB G3 set (18 GiB source + 60 GiB teacher logits) does NOT need any pruning: sparky had 236 GiB free at 18:57Z (the other agent is still archiving about 340 GiB more).

## Option A (recommended): prune only what nothing has used for 7 days, all re-pullable. Frees about 27.5 GB
| image id | frees | why it is safe |
|---|---|---|
| e813795a18ea | 26.01 GB | eugr/spark-vllm, untagged, built 5 days ago; 0 containers; no PB action named it in 7 days; digest exists on Docker Hub |
| 155ce16b4900 | 0.04 MB | eugr/spark-vllm, untagged; shares every layer with the current nightly; pullable |
| 0afec8d4f79f | 0.04 MB | eugr/spark-vllm:pinned-0afec8d4; shares layers with kept images; pullable |
| fc120ece0a38 | 0.04 MB | vllm/vllm-openai:qwen38-flash-next; shares the 30.6 GB base kept by a running job; pullable |
| 905c02933be6 | 0.04 MB | vllm/vllm-openai, untagged; same base; pullable |
| e932bd6ed0e0 | 0.95 GB | grafana/grafana:12.4.1, 7 months old; pullable |
| 63805ebb8d2b | 0.42 GB | prom/prometheus:v3.5.0, 14 months old; pullable |
| c82fe42504fb | 0.08 GB | koalaman/shellcheck-alpine:stable, 14 months old; pullable |

Rollback for each: `docker pull <repo>@<digest>`. Digests:

- e813795a18ea: `eugr/spark-vllm@sha256:e813795a18ea115211fd46b4fbbbb0be5e76d49d26c8f4b8c0763ac5b9f9f07c`
- 155ce16b4900: `eugr/spark-vllm@sha256:155ce16b49007c8b00690dd4c7637c0c1eafbe909709c3175c4498325c01c1d8`
- 0afec8d4f79f: `eugr/spark-vllm@sha256:0afec8d4f79f44685a1ddf758659d33aef3b0f3ec9068e5a7cd1108d30e5581c`
- fc120ece0a38: `vllm/vllm-openai@sha256:fc120ece0a388cc0aa1caad4a9f1cd92113484ab7ec2fd0efadd62585be05bf8`
- 905c02933be6: `vllm/vllm-openai@sha256:905c02933be6021301db2dc284e24e3727467aa3a0f63b41d609885778a07bce`
- e932bd6ed0e0: `grafana/grafana@sha256:e932bd6ed0e026595b08483cd0141e5103e1ab7ff8604839ff899b8dc54cabcb`
- 63805ebb8d2b: `prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996`
- c82fe42504fb: `koalaman/shellcheck-alpine@sha256:c82fe42504fbc9fc68f15d36638e5ee2324ebb8b94e96a3c4e395bf361c49183`

## Option B (your call): also drop the older nccl230 family that the registry already holds. Frees about 39.5 GB more
Images a5424378 (7 exited containers, 2 weeks old), f8dbe1a0 (2 exited containers, used 21:15Z yesterday) and c2e75e03 (used 00:08Z today) share one 39.5 GB base. The registry holds all three (tags `a5424378-mtpmap1`, `f8dbe1a0-kpooltail1`, `0afec8d4`). The 11 exited containers must be removed first. **This breaks the 7-day idle rule**: two of these were named by PB actions less than a day ago, and bringing them back is a 39.5 GB pull from the registry host (the same HDD pool the G3 arms read). Rollback: `docker pull 192.168.1.107/prismaquant/spark-vllm-nccl230:<tag>`.

## Do NOT push the local-only images (my disagreement with the plan as stated)
The local builds with no registry copy (prismaquant-glm-derivative, prismaquant-glm-producer, prismaquant-qwen38-producer, qwen38-flash-dgx, tessera/lfm25-teacher, prismaquant/glm53-mia-sm121, glm-tp2-diag, prismabuild-slurm-smoke) sit on the 30.6 GB vLLM base that the registry does not hold. Pushing the first one uploads that whole base to dl380g10, and pruning them frees only 0.3-0.5 GB each (about 1.4 GB in total). That is a 30 GB write to the loaded HDD pool to save 1.4 GB. cf3f7f83 (prismaquant-glm-producer) is also the image of the container running right now. Recommend: keep them all.

## What would close the full-BF16 gap
Images cannot. The 120-150 GiB must come from elsewhere on sparky (the other agent measured /home/rob at 1.1 TB at 02:37Z: venvs 206G, ~/tmp 165G, HF cache 52G, tessera-runs 39G, dq-runs mostly archived now) or from a smaller resident target. Suggested order: stage the 78 GiB G3 set first; re-measure free space after the archive finishes; then decide the full BF16 with real numbers.

## Caveats
- "Idle" means no PB action request named the image digest in 7 days; manual `docker run` use is not visible to PB. Exited containers also hold images (a5424378: 7, f8dbe1a0: 2, the exl3 image: 1).
- Unique sizes are Docker's own accounting with the containerd store; actual reclaim after deletion can differ by layer rounding.
- Sparky has a running container (sleepy_napier) on the 30.6 GB family and two Created containers on the current nightly image; none of them is touched by either option.
