"""Create a tiny two-layer checkpoint for admitted dispatcher GPU qualification."""
import json
import math
from pathlib import Path
import struct
import sys

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=False)
source = root / 'source'
source.mkdir()
header, data = {}, bytearray()
plan = {}
for layer in range(2):
    name = f'model.layers.{layer}.short_conv.out_proj.weight'
    offset = len(data)
    for i in range(128 * 128):
        bits = struct.unpack('<I', struct.pack('<f', math.sin(i + layer) * .05))[0]
        data.extend(struct.pack('<H', bits >> 16))
    header[name] = {'dtype': 'BF16', 'shape': [128, 128], 'data_offsets': [offset, len(data)]}
    plan[name] = {'grid': 'E4M3', 'q256': 1024}
raw = json.dumps(header).encode()
raw += b' ' * (-len(raw) % 8)
(source / 'model.safetensors').write_bytes(struct.pack('<Q', len(raw)) + raw + data)
(source / 'config.json').write_text(json.dumps({'architectures': ['Lfm2MoeForCausalLM'],
    'model_type': 'lfm2_moe', 'layer_types': ['conv', 'conv'], 'num_dense_layers': 2, 'hidden_size': 128, 'intermediate_size': 256,
    'num_hidden_layers': 2, 'num_attention_heads': 1, 'num_key_value_heads': 1,
    'vocab_size': 8, 'torch_dtype': 'bfloat16'}))
(root / 'plan.json').write_text(json.dumps(plan))
print(root)
