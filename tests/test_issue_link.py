"""Issue policy decisions use recorded API data, never the network."""
from copy import deepcopy

import pytest

from tools.ci.issue_link import check_pull_request

REPO = 'RobTand/prismabuild'


def pull(branch='prismabuild-2406', body='Refs #2406', number=10, **fields):
    return {
        'number': number, 'body': body, 'created_at': '2026-10-10T00:00:00Z',
        'head': {'ref': branch, 'repo': {'full_name': REPO}},
        'user': {'login': 'RobTand', 'id': 24923402, 'type': 'User'},
        'draft': False, **fields,
    }


def metadata(entries='  - number: 11\n    scopes: []\n  - number: 12\n    scopes: []'):
    return ('<details>\n<summary>Metadata for CI tooling, read by '
            '<code>mergify ci</code></summary>\n\n```yaml\n---\n'
            'all_scopes: false\npull_requests:\n' + entries
            + '\nscopes: []\n...\n\n```\n</details>')


def batch(body=None):
    return pull('mergify/merge-queue/batch-id', metadata() if body is None else body,
                draft=True, user={'login': 'mergify[bot]', 'id': 37929162, 'type': 'Bot'})


def check(subject, *, issues=None, members=None, sidebar=()):
    issues = {2406: {'number': 2406}} if issues is None else issues
    members = {11: pull(number=11), 12: pull('prismabuild-2406-tl3-2', number=12)} if members is None else members
    return check_pull_request(subject, REPO, issues.get, members.get, lambda number: sidebar)


@pytest.mark.parametrize('branch', [
    'prismabuild-2406', 'prismabuild-2406-gpu', 'prismabuild-2406-gpu-2',
    'prismabuild-2406-tl3-1', 'prismabuild-2406-0',
])
def test_valid_ordinary_branches(branch):
    verdict = check(pull(branch))
    assert verdict.allowed
    assert verdict.reason == 'Accepted the pull request body reference to issue #2406.'


@pytest.mark.parametrize('keyword', [
    'close', 'closes', 'closed', 'fix', 'fixes', 'fixed', 'resolve', 'resolves',
    'resolved', 'ref', 'refs', 'reference', 'references', 'REFS',
])
@pytest.mark.parametrize('reference', ['#2406', REPO + '#2406', 'https://github.com/' + REPO + '/issues/2406'])
def test_supported_references(keyword, reference):
    assert check(pull(body=keyword + '\n' + reference)).allowed


@pytest.mark.parametrize('branch,body', [
    ('fix/something', 'Refs #2406'), ('prismabuild-2407', 'Refs #2406'),
    ('prismabuild-2406-', 'Refs #2406'), ('prismabuild-2406-GPU', 'Refs #2406'),
    ('prismabuild-2406-_gpu', 'Refs #2406'), ('prismabuild-02406', 'Refs #2406'),
    ('prismabuild-2406', 'Mentions #2406'), ('prismabuild-2406', None),
    ('prismabuild-2406', 'Refs RobTand/other#2406'),
    ('prismabuild-2406', 'Refs #02406'), ('prismabuild-2406', 'prefrefs #2406'),
])
def test_bad_ordinary_reference_or_branch(branch, body):
    verdict = check(pull(branch, body))
    assert not verdict.allowed
    assert 'branch naming rule (rule 12)' in verdict.reason


def test_references_are_checked_until_matching_actual_issue():
    assert check(pull(body='Refs #99. Refs #2406')).allowed
    assert not check(pull(body='Refs #99. Refs #2406'), issues={99: {'number': 99}}).allowed


@pytest.mark.parametrize('issues', [{}, {2406: {'number': 2406, 'pull_request': {}}}])
def test_missing_issue_and_pull_request_references_are_rejected(issues):
    assert not check(pull(), issues=issues).allowed


@pytest.mark.parametrize('branch', ['ig/legacy', 'pg/x', 'pg-train/x', 'tl/release-op', 'release', 'release/1', 'release-1'])
def test_exempt_branches_accept_body_or_sidebar_but_still_need_issue(branch):
    assert check(pull(branch)).allowed
    assert check(pull(branch, ''), sidebar=[{'number': 2406, 'repository': {'nameWithOwner': REPO}}]).allowed
    verdict = check(pull(branch, ''), sidebar=[{'number': 2406, 'repository': {'nameWithOwner': 'RobTand/other'}}])
    assert not verdict.allowed
    assert 'Refs #123' in verdict.reason and 'sidebar' in verdict.reason


@pytest.mark.parametrize('created_at,allowed', [
    ('2026-10-09T16:59:59Z', True), ('2026-10-09T17:00:00Z', False),
    ('2026-10-09T17:00:01Z', False),
])
def test_creation_cutoff(created_at, allowed):
    assert check(pull('legacy', created_at=created_at)).allowed is allowed


def test_sidebar_is_not_sufficient_for_ordinary_branch():
    verdict = check(pull(body=''), sidebar=[{'number': 2406, 'repository': {'nameWithOwner': REPO}}])
    assert not verdict.allowed
    assert 'prismabuild-2406' in verdict.reason


@pytest.mark.parametrize('spoof', ['type', 'id', 'author', 'fork', 'not-draft', 'branch'])
def test_spoofed_batches_follow_ordinary_rule(spoof):
    subject = batch()
    if spoof == 'type':
        subject['user']['type'] = 'User'
    elif spoof == 'id':
        subject['user']['id'] = 1
    elif spoof == 'author':
        subject['user'] = {'login': 'ordinary', 'id': 1, 'type': 'User'}
    elif spoof == 'fork':
        subject['head']['repo']['full_name'] = 'other/prismabuild'
    elif spoof == 'not-draft':
        subject['draft'] = False
    else:
        subject['head']['ref'] = 'mergify/other/batch'
    verdict = check(subject)
    assert not verdict.allowed
    assert 'branch naming rule' in verdict.reason


@pytest.mark.parametrize('body', [
    '', 'pull_requests:\n  - number: 11', metadata('  - number: nope'),
    metadata('  - number: 0'), metadata('  - number: -1'), metadata('  - scopes: []'),
    metadata('  - number: 11\n    number: 12'), metadata('  - number: 11\n  - invalid'),
    metadata('[]'), metadata(''), metadata('  - number: 11\n  - number: 11'),
    metadata().replace('pull_requests:', 'pull_requests: [11]'),
    metadata() + '\n' + metadata(), metadata().replace('```yaml', '```json'),
])
def test_invalid_batch_metadata_fails_closed(body):
    verdict = check(batch(body))
    assert not verdict.allowed
    assert 'batch' in verdict.reason.lower() and 'metadata' in verdict.reason.lower()


def test_valid_batch_checks_every_member():
    verdict = check(batch())
    assert verdict.allowed
    assert '#11' in verdict.reason and '#12' in verdict.reason


@pytest.mark.parametrize('invalid', ['branch', 'pr-reference', 'missing-issue', 'missing-member', 'nested-batch'])
def test_invalid_member_names_the_member(invalid):
    members = {11: pull(number=11), 12: pull(number=12)}
    if invalid == 'branch':
        members[12]['head']['ref'] = 'unrelated-branch'
    elif invalid == 'pr-reference':
        members[12] = pull('prismabuild-99', 'Refs #99', number=12)
    elif invalid == 'missing-issue':
        members[12] = pull('prismabuild-98', 'Refs #98', number=12)
    elif invalid == 'missing-member':
        del members[12]
    else:
        members[12] = batch()
    verdict = check(batch(), members=members, issues={2406: {'number': 2406}, 99: {'pull_request': {}}})
    assert not verdict.allowed
    assert '#12' in verdict.reason


def test_batch_members_keep_ordinary_exemptions():
    assert check(batch(), members={11: pull(number=11), 12: pull('pg/legacy', number=12)}).allowed


def test_api_errors_do_not_turn_into_acceptance():
    def unavailable(number):
        raise RuntimeError('API unavailable')
    with pytest.raises(RuntimeError, match='API unavailable'):
        check_pull_request(pull(), REPO, unavailable, lambda number: None, lambda number: ())
