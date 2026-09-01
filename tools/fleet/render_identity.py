"""Is a production render bit-identical across two GB10 boxes?

This is the gate on whether PrismaBuild can carry quantization work at all.  A
CAS receipt keyed by an action key promises that the same action yields the same
bytes anywhere.  If a render differs between sparky and sparklina, that promise
is false and the cache would silently serve one box's result for the other's.

Renders the real production path (GPTQ + JSO, the shipping levers) on real
weights and prints a digest per tensor.  Run on both boxes, diff the output.
"""
import sys, os, json, hashlib, socket, pathlib
os.environ.setdefault("PYTHONHASHSEED", "0")
sys.path.insert(0, "/home/rob/prismaquant")
import torch
from safetensors import safe_open
from prismaquant.production_weight_cache import render_production_weight

MODEL = "/mnt/shared/models/GLM-5.3-Flash-4layer"
LEVERS = {"gptq": True, "static_act_order": True, "joint_scale_opt": True}
torch.manual_seed(0)

def digest(t: torch.Tensor) -> str:
    # view as uint8 to hash the exact bits: bfloat16 has no numpy dtype, and
    # casting to float32 would hide a low-bit difference, which is the entire
    # thing this script exists to detect.
    flat = t.detach().cpu().contiguous().view(torch.uint8)
    return hashlib.sha256(flat.numpy().tobytes()).hexdigest()

import glob
shards = sorted(glob.glob(f"{MODEL}/*.safetensors"))
rows = []
picked = 0
for path in shards:
    with safe_open(path, "pt") as f:
        for key in sorted(f.keys()):
            if not key.endswith(".weight") or ".layers.0." not in key:
                continue
            if not any(r in key for r in ("q_proj", "gate_proj", "down_proj")):
                continue
            W = f.get_tensor(key)
            if W.ndim != 2 or min(W.shape) < 256:
                continue
            W = W[:512, :1024].to("cuda").to(torch.bfloat16).contiguous()
            # Deterministic synthetic activations: same seed -> same bytes on
            # both boxes, so any digest difference is the RENDER, not the input.
            g = torch.Generator(device="cuda"); g.manual_seed(1234)
            X = torch.randn(256, W.shape[1], generator=g, device="cuda", dtype=torch.bfloat16)
            acts = {"input": X}
            for fmt in ("NVFP4", "FP8_E4M3"):
                try:
                    out = render_production_weight(
                        W, fmt, qname=key, activations=acts, levers=LEVERS
                    )
                    rows.append({"qname": key, "fmt": fmt, "digest": digest(out),
                                 "shape": list(out.shape), "dtype": str(out.dtype)})
                except Exception as exc:
                    rows.append({"qname": key, "fmt": fmt, "error": repr(exc)[:200]})
            picked += 1
            if picked >= 3:
                break
    if picked >= 3:
        break

payload = json.dumps({"host": socket.gethostname(),
                  "torch": torch.__version__,
                  "device": torch.cuda.get_device_name(0),
                  "input_digest": digest(X),
                  "rows": rows}, indent=1)
out = pathlib.Path(f"/mnt/shared/prismabuild-fleet/render_{socket.gethostname()}.json")
out.write_text(payload)
print(f"wrote {out}")
