#!/usr/bin/env bash
# Re-run the fixture qualification through the published PrismaBuild client.
set -euo pipefail
checkout=$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)
lane=${1:?usage: verify_gpu_runtime_1895.sh x86|arm REPORT.json}
report=${2:?supply an absolute report path outside the checkout}
case "$lane" in
  x86) interpreter=/home/rob/venvs/pb-cpu/bin/python3.14; tags=(--tag x86) ;;
  arm) interpreter=/home/rob/venvs/pb-cpu/bin/python3.12; tags=() ;;
  *) exit 2 ;;
esac
exec python3 /mnt/shared/prismabuild-fleet/repo/tools/fleet/pbtest.py \
  --checkout "$checkout" --python "$interpreter" "${tags[@]}" \
  --shards 2 --max-clients 2 --workers-per-shard 1 --threads-per-shard 1 \
  --cpus-per-shard 1 --mem-gb 4 --json "$report" \
  tests/test_gpu_container_runtime.py tests/test_gpu_runtime_integration.py \
  tests/test_gpu_capacity.py tests/test_adaptive_gpu.py tests/test_box_capacity.py \
  tests/test_resource_broker.py tests/test_resource_scope.py tests/test_pool_resource_scope.py \
  tests/test_pool_scope_creation_recovery.py tests/test_broker_scope_contract_1301.py \
  tests/test_worker_loop_offers_what_is_free.py tests/test_claim_denials.py \
  tests/test_pbstatus.py tests/test_pbstatus_pool.py \
  tests/test_docker_shim_global_options.py tests/test_docker_shim_cpu_affinity.py \
  tests/test_docker_shim_memory.py tests/test_docker_shim_relays_termination_1599.py \
  tests/test_sample_docker_coverage.py tests/test_nsys_docker_refusal.py \
  tests/test_duplication_baseline.py tests/test_design_doc_line_references.py
