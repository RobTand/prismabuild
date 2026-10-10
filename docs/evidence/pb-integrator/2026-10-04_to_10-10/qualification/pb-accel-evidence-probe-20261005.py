import os,sys,socket
sys.path.insert(0,'/mnt/shared/prismabuild-fleet/repo/src')
from prismabuild import core as pb
ev=pb._collect_worker_evidence(attest_accelerator_identity=True)
tc=pb.live_platform_toolchain_contract(evidence=ev)
print(socket.gethostname(),'| has cuda_compute_capability:', 'cuda_compute_capability' in tc,'| has nvidia_driver:', 'nvidia_driver' in tc,'| toolchain keys:',sorted(tc)[:8])
acc=ev.get('accelerator') or ev.get('accelerators') or {k:v for k,v in ev.items() if 'cuda' in k or 'nvidia' in k or 'gpu' in k}
print('   evidence accelerator-ish:',str(acc)[:260])
