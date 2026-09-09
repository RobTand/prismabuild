"""A host has to be able to say which agent it is running, fleet-wide.

`upgrade_client.py` is installed by a copy step rather than by the runtime
symlink, so none of the fleet-visible records answer for it.  The loops'
`runtime_commit` in `pb-queue/workers/<host>.json` answers for the loops.  The
generation receipt says what a host is *supposed* to install.  What it actually
installed lives in root-owned host-local state.

#458 cannot arm a coordinated rollout without that answer: one host still
running an older agent never posts its drain marker, and its older code
releases the drain under the rest of the fleet.

So each agent posts `rollout/agents/<host>.<sha256 of its own bytes>.json`
once per version of itself.  Three properties are load bearing and each has a
test here.

The tree is write-once.  Content lands in a `.tmp-<uuid4>` sibling and is
*linked* onto its final name, never renamed onto it, so a name that already
exists refuses the write instead of replacing a statement somebody else made.

The write goes through a dropped-credential child, because root is squashed on
the shared mount.  The read does not: the fleet root is `drwxrwxr-x rob rob`
and the squashed uid can stat it, so a host that has already posted for the
version it runs costs one `stat` and no subprocess.

A poster that cannot write must not stop the agent from converging its
members.  Attestation is a precondition somebody else checks, not a step of
the upgrade.

Nothing here touches the real shared mount, a real broker, or a real process.
"""
import importlib.util
import io
import json
import os
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
CLIENT = REPO / 'tools/fleet/upgrade_client.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


upgrade = load('upgrade_client_attestation', CLIENT)


def agent(tmp_path, *, poster=None, store=None):
    """An agent whose rollout tree is a private directory.

    `generation_store` is the fleet's `runtime-generations`; the rollout tree
    is its sibling, so pointing the store at a private path moves the whole
    tree with it.
    """
    store = store if store is not None else tmp_path / 'fleet' / 'runtime-generations'
    return upgrade.Upgrader(dict(install_dir=str(tmp_path / 'installed'),
                                 state_dir=str(tmp_path / 'state'),
                                 generation_store=str(store),
                                 reader_uid=os.getuid()),
                            rpc=lambda *a, **k: {'ok': True},
                            sleep=lambda _: None,
                            poster=poster)


def io_of(data):
    """The child reads its content from stdin, the way the parent hands it over."""
    return io.BytesIO(data)


class Recorder:
    """A poster that records rather than spawning a child."""

    def __init__(self, root, *, fail=None):
        self.root = Path(root)
        self.fail = fail
        self.calls = []

    def __call__(self, relpath, content):
        self.calls.append((relpath, content))
        if self.fail is not None:
            raise self.fail
        upgrade.post_marker(self.root, relpath, content)


# --- where the tree lives ---------------------------------------------------

def test_the_rollout_tree_sits_beside_the_generation_store():
    """`rollout/` is a sibling of `runtime-generations/`, per the design tree."""
    root = upgrade.rollout_root({'generation_store': '/mnt/shared/pb-fleet/runtime-generations'})
    assert root == Path('/mnt/shared/pb-fleet/rollout')


def test_an_explicit_rollout_root_wins():
    """A harness must be able to point the tree somewhere it can write."""
    root = upgrade.rollout_root({'generation_store': '/mnt/shared/pb-fleet/runtime-generations',
                                 'rollout_root': '/home/rob/private/rollout'})
    assert root == Path('/home/rob/private/rollout')


# --- what a marker may be called -------------------------------------------

@pytest.mark.parametrize('relpath', [
    '../escape',
    '/absolute/name',
    'agents/../../escape',
    'agents/.hidden',
    '.tmp-deadbeef',
    'agents/name with space',
    'agents/name\nsecond',
    '',
    'a/b/c/d/e',
])
def test_a_marker_path_that_is_not_plain_descent_is_refused(tmp_path, relpath):
    with pytest.raises(ValueError):
        upgrade.marker_path(tmp_path, relpath)


@pytest.mark.parametrize('relpath', [
    'agents/sparky.abc123.json',
    'agents/gx10-6b77.abc123.json',
    'agents/host.example.com.abc123.json',
    '1788947072.4/sparky.drained',
    'terminal.json',
])
def test_a_plain_descent_resolves_under_the_root(tmp_path, relpath):
    resolved = upgrade.marker_path(tmp_path, relpath)
    assert resolved == Path(tmp_path).joinpath(*relpath.split('/'))


# --- write-once -------------------------------------------------------------

def test_a_marker_is_created_with_its_content(tmp_path):
    assert upgrade.post_marker(tmp_path / 'rollout', 'agents/one.json', b'{"a": 1}\n') is True
    assert (tmp_path / 'rollout/agents/one.json').read_bytes() == b'{"a": 1}\n'


def test_a_second_post_of_one_name_keeps_the_first_content(tmp_path):
    """The name carries the claim, so a repeat is the same statement, not a new one."""
    root = tmp_path / 'rollout'
    upgrade.post_marker(root, 'agents/one.json', b'first\n')
    assert upgrade.post_marker(root, 'agents/one.json', b'second\n') is False
    assert (root / 'agents/one.json').read_bytes() == b'first\n'


def test_no_temporary_file_survives_a_write(tmp_path):
    root = tmp_path / 'rollout'
    upgrade.post_marker(root, 'agents/one.json', b'first\n')
    upgrade.post_marker(root, 'agents/one.json', b'second\n')
    assert sorted(entry.name for entry in (root / 'agents').iterdir()) == ['one.json']


def test_no_temporary_file_survives_a_refused_link(tmp_path, monkeypatch):
    """A failure mid-write must not leave a `.tmp-` sibling for a listing to trip on."""
    root = tmp_path / 'rollout'

    def refuse(source, target):
        raise OSError(28, 'No space left on device')

    monkeypatch.setattr(upgrade.os, 'link', refuse)
    with pytest.raises(OSError):
        upgrade.post_marker(root, 'agents/one.json', b'first\n')
    assert list((root / 'agents').iterdir()) == []


def test_an_oversized_marker_is_refused(tmp_path):
    with pytest.raises(ValueError):
        upgrade.post_marker(tmp_path, 'agents/one.json', b'x' * (upgrade.MAX_MARKER + 1))


# --- the unprivileged child -------------------------------------------------

def test_the_child_refuses_to_run_as_the_wrong_uid(tmp_path):
    """This is a mode of a root-owned program; the uid check is what makes it safe."""
    config = {'generation_store': str(tmp_path / 'runtime-generations'),
              'reader_uid': os.getuid() + 1}
    with pytest.raises(SystemExit):
        upgrade.post_child(config, 'agents/one.json', io_of(b'{}'))


def test_the_child_refuses_uid_zero(tmp_path):
    config = {'generation_store': str(tmp_path / 'runtime-generations'), 'reader_uid': 0}
    with pytest.raises(SystemExit):
        upgrade.post_child(config, 'agents/one.json', io_of(b'{}'))


def test_the_child_writes_what_it_is_given(tmp_path):
    config = {'generation_store': str(tmp_path / 'fleet' / 'runtime-generations'),
              'reader_uid': os.getuid()}
    assert upgrade.post_child(config, 'agents/one.json', io_of(b'{"a": 1}\n')) == 0
    assert (tmp_path / 'fleet/rollout/agents/one.json').read_bytes() == b'{"a": 1}\n'


def test_the_child_refuses_an_oversized_stream(tmp_path):
    config = {'generation_store': str(tmp_path / 'fleet' / 'runtime-generations'),
              'reader_uid': os.getuid()}
    with pytest.raises(SystemExit):
        upgrade.post_child(config, 'agents/one.json', io_of(b'x' * (upgrade.MAX_MARKER + 1)))


# --- the attestation itself -------------------------------------------------

def test_the_attested_hash_is_this_files_own_bytes():
    """A version claim nothing derived from the running bytes is not a claim."""
    import hashlib
    assert upgrade.self_digest() == hashlib.sha256(CLIENT.read_bytes()).hexdigest()


def test_the_name_carries_the_host_and_the_hash():
    name = upgrade.attestation_name('gx10-6b77', 'a' * 64)
    assert name == f'gx10-6b77.{"a" * 64}.json'
    host, sha, suffix = name.rsplit('.', 2)
    assert (host, sha, suffix) == ('gx10-6b77', 'a' * 64, 'json')


def test_a_hostname_with_dots_still_parses_back():
    """The coordinator splits from the right, so an FQDN costs nothing."""
    name = upgrade.attestation_name('box.example.com', 'b' * 64)
    host, sha, _ = name.rsplit('.', 2)
    assert (host, sha) == ('box.example.com', 'b' * 64)


def test_a_hostname_that_could_escape_the_directory_cannot():
    name = upgrade.attestation_name('../../etc/passwd', 'c' * 64)
    assert '/' not in name


def test_an_agent_posts_once_and_the_marker_names_its_hash(tmp_path):
    recorder = Recorder(tmp_path / 'fleet' / 'rollout')
    client = agent(tmp_path, poster=recorder)
    result = client.attest()
    assert result['marker'].startswith('agents/')
    assert upgrade.self_digest() in result['marker']
    posted = json.loads((tmp_path / 'fleet' / 'rollout' / result['marker']).read_text())
    assert posted['client_sha256'] == upgrade.self_digest()
    assert posted['schema'] == upgrade.ATTESTATION_SCHEMA
    assert posted['client_upgrade_protocol'] == upgrade.CLIENT_UPGRADE_PROTOCOL


def test_a_second_tick_costs_no_write(tmp_path):
    """One write per host per version of this file, not one per tick."""
    recorder = Recorder(tmp_path / 'fleet' / 'rollout')
    client = agent(tmp_path, poster=recorder)
    client.attest()
    client.attest()
    client.attest()
    assert len(recorder.calls) == 1


def test_a_poster_that_fails_does_not_stop_the_agent(tmp_path):
    """Attestation is a precondition somebody else checks, not a step of the upgrade."""
    recorder = Recorder(tmp_path / 'fleet' / 'rollout', fail=RuntimeError('child failed'))
    client = agent(tmp_path, poster=recorder)
    result = client.attest()
    assert 'error' in result and 'marker' not in result


def test_an_agent_with_no_poster_attests_nothing(tmp_path):
    assert agent(tmp_path, poster=None).attest() is None


def test_the_attestation_travels_in_the_status_report(tmp_path):
    """Whatever the agent decided is readable where every other agent state is."""
    recorder = Recorder(tmp_path / 'fleet' / 'rollout')
    client = agent(tmp_path, poster=recorder)
    (tmp_path / 'state').mkdir(parents=True, exist_ok=True)
    client.attest()
    value = client.report('current')
    assert value['attestation']['marker'].endswith('.json')


def test_a_report_before_attesting_carries_no_claim(tmp_path):
    client = agent(tmp_path, poster=None)
    (tmp_path / 'state').mkdir(parents=True, exist_ok=True)
    assert 'attestation' not in client.report('current')


def test_the_mode_is_reachable_from_the_command_line(tmp_path, monkeypatch):
    """The parent invokes the child by argv, so the dispatch is part of the contract.

    `trusted` is stood down because it demands a root-owned enrollment file and
    this runs unprivileged; everything it guards is exercised where it is used.
    """
    import sys

    config = tmp_path / 'client-upgrade.json'
    config.write_text(json.dumps({'generation_store': str(tmp_path / 'fleet' / 'runtime-generations'),
                                  'reader_uid': os.getuid()}))
    monkeypatch.setattr(upgrade, 'trusted', Path)
    monkeypatch.setattr(sys, 'argv', ['upgrade_client.py', '--config', str(config),
                                      '--post-rollout-marker', 'agents/two.json'])
    monkeypatch.setattr(sys, 'stdin', type('S', (), {'buffer': io_of(b'{"b": 2}\n')})())
    assert upgrade.main() == 0
    assert (tmp_path / 'fleet/rollout/agents/two.json').read_bytes() == b'{"b": 2}\n'


def test_the_command_line_refuses_a_path_that_is_not_plain_descent(tmp_path, monkeypatch):
    import sys

    config = tmp_path / 'client-upgrade.json'
    config.write_text(json.dumps({'generation_store': str(tmp_path / 'fleet' / 'runtime-generations'),
                                  'reader_uid': os.getuid()}))
    monkeypatch.setattr(upgrade, 'trusted', Path)
    monkeypatch.setattr(sys, 'argv', ['upgrade_client.py', '--config', str(config),
                                      '--post-rollout-marker', '../../escape'])
    monkeypatch.setattr(sys, 'stdin', type('S', (), {'buffer': io_of(b'{}')})())
    with pytest.raises(ValueError):
        upgrade.main()


def test_the_tree_is_readable_under_a_strict_umask(tmp_path):
    """A directory nobody else can enter turns one stat per tick into one write.

    The tree is read by every host as the squashed uid. A service unit that
    carries a strict umask would otherwise make the existence check miss every
    time, silently, while the status still reported a posted marker.
    """
    previous = os.umask(0o077)
    try:
        upgrade.post_marker(tmp_path / 'fleet' / 'rollout', 'agents/one.json', b'{}\n')
    finally:
        os.umask(previous)
    for directory in ('fleet/rollout', 'fleet/rollout/agents'):
        assert (tmp_path / directory).stat().st_mode & 0o777 == 0o755


def test_a_directory_that_was_already_there_keeps_its_mode(tmp_path):
    """Only a directory this write created is given a mode."""
    root = tmp_path / 'fleet' / 'rollout'
    (root / 'agents').mkdir(parents=True)
    (root / 'agents').chmod(0o700)
    upgrade.post_marker(root, 'agents/one.json', b'{}\n')
    assert (root / 'agents').stat().st_mode & 0o777 == 0o700


def test_the_parent_hands_the_child_a_dropped_credential_invocation(tmp_path, monkeypatch):
    """The other half of reachable: the parent has to produce that argv.

    The call itself needs CAP_SETGID for `extra_groups`, so what is checked
    here is the shape of the invocation and not its effect.
    """
    seen = {}

    def record(argv, **kwargs):
        seen['argv'] = argv
        seen.update(kwargs)
        # The parent hands over a temporary file it closes on return, so the
        # content is read here, where the child would read it.
        seen['content'] = kwargs['stdin'].read()
        return type('R', (), {'returncode': 0, 'stderr': b''})()

    config_path = tmp_path / 'client-upgrade.json'
    config = {'generation_store': str(tmp_path / 'fleet' / 'runtime-generations'),
              'reader_uid': os.getuid()}
    monkeypatch.setattr(upgrade, 'trusted', Path)
    monkeypatch.setattr(upgrade.subprocess, 'run', record)
    upgrade.post_as_reader(config_path, config, 'agents/one.json', b'{}\n')

    assert '-I' in seen['argv']
    assert seen['argv'][seen['argv'].index('--post-rollout-marker') + 1] == 'agents/one.json'
    assert seen['argv'][seen['argv'].index('--config') + 1] == str(config_path)
    assert seen['user'] == os.getuid()
    assert seen['extra_groups'] == []
    assert seen['env'] == {'PATH': '/usr/bin:/bin'}
    assert seen['cwd'] == '/'
    assert seen['content'] == b'{}\n'


def test_the_parent_refuses_a_path_before_it_spawns_anything(tmp_path, monkeypatch):
    """A name that cannot be written is not worth a subprocess."""
    def refuse(*args, **kwargs):
        raise AssertionError('spawned a child for a path it should have refused')

    monkeypatch.setattr(upgrade, 'trusted', Path)
    monkeypatch.setattr(upgrade.subprocess, 'run', refuse)
    with pytest.raises(ValueError):
        upgrade.post_as_reader(tmp_path / 'c.json',
                               {'generation_store': str(tmp_path / 'rg'),
                                'reader_uid': os.getuid()},
                               '../../escape', b'{}\n')


def test_a_child_that_fails_is_reported_not_swallowed(tmp_path, monkeypatch):
    def fail(*args, **kwargs):
        return type('R', (), {'returncode': 1, 'stderr': b'permission denied'})()

    monkeypatch.setattr(upgrade, 'trusted', Path)
    monkeypatch.setattr(upgrade.subprocess, 'run', fail)
    with pytest.raises(RuntimeError, match='permission denied'):
        upgrade.post_as_reader(tmp_path / 'c.json',
                               {'generation_store': str(tmp_path / 'rg'),
                                'reader_uid': os.getuid()},
                               'agents/one.json', b'{}\n')
