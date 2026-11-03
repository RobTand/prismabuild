"""Shared real pbrun pipeline setup against a scratch queue and CAS."""
from prismabuild import core, pool
from queue_space_support import QUEUE_SPACE_BOOTSTRAP
from verified_action_support import _standard_action

BOOTSTRAP = r"""
import json, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(os.environ['CLIENT_SOURCE']) / 'tools' / 'fleet'))
import pbrun
from prismabuild import submission as submit_core
base = Path(os.environ['CLIENT_ROOT'])
pbrun.configured_store = lambda: submit_core.FleetStore(Path(base), pbrun.RUNTIME_ROOT, pbrun.fleet_roster.Roster.beside(pbrun.__file__))
cas = submit_core.pb.PrismaBuildCAS(base / 'cas')
action = cas.read_action_request(os.environ['CLIENT_KEY'])
mode = os.environ['CLIENT_MODE']
def prepare(args, **_submission_context):
    if mode == 'refused':
        raise submit_core.SubmissionRefused('controlled refusal')
    if mode == 'offer_timeout':
        raise submit_core.OfferDiscoveryTimedOut('controlled offer timeout')
    if mode == 'interrupted':
        raise SystemExit(143)
    return dict(template=dict(cas=cas, params=dict(placement=dict(required_tags=[]),
        demand=dict(cpu=1, mem_gb=1))), cwd=base / 'checkout', portable_checkout=True,
        offer_queue=lambda: submit_core.pool.PoolQueue(base / 'pb-queue'))
submit_core.prepare = prepare
submit_core.seal = lambda *args, **kwargs: action
submit_core.announce_placement = lambda *args, **kwargs: None
submit_core.d38_gate.judge_publication = lambda *args, **kwargs: None
submit_core.publish_consumer_row = lambda q, *args, **kwargs: (
    q.records.item_path('ready', action['action_key']) if mode != 'attached' else None, None)
fd = sys.argv[sys.argv.index('--client-events-fd') + 1]
sys.argv = ['pbrun.py', '--client-events-fd', fd, '--wait-s', '0', '--transport', 'pool',
    *(['--detach'] if mode == 'cached' else []), '--', '/bin/echo', 'hello']
""" + QUEUE_SPACE_BOOTSTRAP + r"""
raise SystemExit(pbrun.main())
"""



def _setup(tmp_path):
    (tmp_path / 'checkout').mkdir()
    action = _standard_action(tmp_path / 'checkout', command=['/bin/echo', 'hello'])
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / 'pb-queue')
    queue.records.ensure_layout()
    return action, cas, queue

