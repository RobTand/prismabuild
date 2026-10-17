"""Verify an actual receipted scratch profile with the current consumer.

Run only as an admitted PB action on the profile's host, whose local root
identity is part of this check. The config and materialized checkout are private.
No shared profile configuration, worker or runtime setting is changed.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile


def qualify_scratch_source_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--producer-key', required=True)
    parser.add_argument('--profile-root', required=True)
    parser.add_argument('--cas-root', default='/mnt/shared/prismabuild-fleet/cas')
    args = parser.parse_args()
    admitted = os.environ.get('PRISMABUILD_ACTION_KEY', '')
    if len(admitted) != 64 or any(c not in '0123456789abcdef' for c in admitted):
        raise SystemExit('scratch source qualification requires an admitted PB action')
    source = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(source / 'src'))
    from prismabuild import core as pb, local_scratch as scratch

    cas = pb.PrismaBuildCAS(args.cas_root)
    action = cas.read_action_request(args.producer_key)
    assert action is not None
    receipt = cas.lookup(action)
    assert receipt is not None
    raw = pb.immutable_files.read_immutable_file(
        cas.result_path(receipt, action), where='actual scratch profile', max_bytes=65536)
    body = pb.digest_primitives.decode_strict_json(raw, where='actual scratch profile')
    assert body['completed'] is True and body['errors'] == []
    assert body['producer_action_key'] == args.producer_key
    assert body['root'] == args.profile_root
    assert body['write_bytes'] == body['read_bytes'] == 134217728
    ref = dict(cas_root=args.cas_root, action_key=args.producer_key,
               artifact_sha256=pb.canonical_sha256(body), root=args.profile_root,
               profile_contract=scratch.PROFILE_CONTRACT)
    with tempfile.TemporaryDirectory(prefix='final-profile-consumer-') as directory:
        private = Path(directory)
        config = private / 'profiles.json'
        config.write_bytes(pb._canonical_file_bytes(dict(schema=scratch.CONFIG_SCHEMA, profiles=[ref])))
        reader = scratch.ProfileInputs(config, source_root=source,
            checkout_root=private / 'materialized',
            producer_python='/home/rob/venvs/pb-cpu/bin/python3.12')
        # Surface the concrete source/receipt refusal before observe's normal
        # containment turns a failed observation into an empty profile list.
        verified = reader._load(ref)
        assert verified == {**body, 'artifact_sha256': ref['artifact_sha256']}
        assert len(reader.verified) == 1
        first = reader.observe()
        second = reader.observe()
        assert first == second
        assert first[scratch.PROFILES] == [verified]
        assert first[scratch.DEVICES] == {args.profile_root: scratch.root_identity(args.profile_root)}
        assert len(reader.verified) == 1
        sources = {name: dict(bytes=len((source / name).read_bytes()),
                             sha256=pb.raw_sha256((source / name).read_bytes()))
                   for name in scratch.PRODUCER_FILES}
        assert reader.expected_sources == tuple(sources[name]['sha256'] for name in scratch.PRODUCER_FILES)
    result = dict(qualified=True, producer_action=args.producer_key,
                  qualification_action=admitted, actual_python=sys.version,
                  affinity=sorted(os.sched_getaffinity(0)), observed=first,
                  exact_source_members=sources, immutable_source_verifications=1,
                  observation_count=2, shared_configuration_changed=False,
                  scope='Actual buffered write/fdatasync/warm-read artifact, current production consumer, sealed producer closure and current local root identity. No representative spill, pressure, speedup or deployment claim.')
    print('scratch-source-qualification: ' + json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    qualify_scratch_source_main()
