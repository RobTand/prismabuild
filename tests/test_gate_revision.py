"""PR selection and exact-head queue runs must use coherent Git trees."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('gate_revision', ROOT / 'tools/ci/gate_revision.py')
revision = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(revision)
AUTHENTICATED_FULL = (
    "github.event.pull_request.head.repo.full_name == github.repository && "
    "((startsWith(github.head_ref, 'mergify/merge-queue/') && "
    "github.event.pull_request.user.login == 'mergify[bot]' && "
    "github.event.pull_request.user.id == 37929162) || "
    "contains(github.event.pull_request.labels.*.name, 'gate-canary'))"
)


@pytest.fixture
def histories(tmp_path):
    repository = tmp_path / 'repository'
    repository.mkdir()
    environment = dict(os.environ, GIT_AUTHOR_NAME='Gate fixture',
                       GIT_AUTHOR_EMAIL='gate@example.invalid',
                       GIT_COMMITTER_NAME='Gate fixture',
                       GIT_COMMITTER_EMAIL='gate@example.invalid')

    def git(*args):
        return subprocess.run(['git', *args], cwd=repository, env=environment,
                              capture_output=True, text=True, check=True).stdout.strip()

    git('init', '-q', '-b', 'target')
    (repository / 'common.txt').write_text('common tree\n')
    git('add', '.')
    git('commit', '-qm', 'Common tree')
    ancestor = git('rev-parse', 'HEAD')
    # pb-history-ok: Creates a PR branch only in the generated temporary fixture repository.
    git('switch', '-qc', 'pr')
    (repository / 'pr-change.txt').write_text('PR change\n')
    git('add', '.')
    git('commit', '-qm', 'PR change')
    head = git('rev-parse', 'HEAD')
    # pb-history-ok: Switches only to the fixture target branch created above.
    git('switch', '-q', 'target')
    (repository / 'target-tooling-change.txt').write_text('New target tooling\n')
    git('add', '.')
    git('commit', '-qm', 'Target advances without changing PR head')
    target = git('rev-parse', 'HEAD')
    # pb-history-ok: Builds the synthetic merge exclusively from fixture-created commits.
    git('merge', '--no-ff', '-qm', 'Synthetic PR merge', 'pr')
    merge = git('rev-parse', 'HEAD')
    return repository, git, ancestor, target, head, merge


def test_pr_selection_uses_merge_first_parent_and_excludes_unrelated_target_changes(histories):
    repository, git, ancestor, target, head, merge = histories
    result = revision.gate_revisions(repository, 'selection', ancestor, merge)
    assert result['head_sha'] == merge
    assert result['base_sha'] == target
    assert result['base_source'] == 'merge_first_parent'
    # pb-history-ok: Compares only generated fixture commits to prove the selection diff.
    changed = git('diff', '--name-only', result['base_sha'], result['head_sha'])
    assert changed.splitlines() == ['pr-change.txt']
    assert head != merge


def test_full_gate_retains_exact_draft_head_and_event_base(histories):
    repository, git, ancestor, target, head, merge = histories
    # pb-history-ok: Checks out only the generated fixture draft head, never project history.
    git('switch', '--detach', '-q', head)
    result = revision.gate_revisions(repository, 'full', ancestor, head)
    assert result['base_sha'] == ancestor
    assert result['head_sha'] == head
    assert result['base_source'] == 'pr_event'
    assert target != ancestor and merge != head


def test_wrong_checkout_cannot_claim_a_plan_for_another_commit(histories):
    repository, _, ancestor, _, head, _ = histories
    with pytest.raises(ValueError, match='checkout'):
        revision.gate_revisions(repository, 'full', ancestor, head)


def test_revision_cli_records_immutable_selection_identity(histories, tmp_path):
    repository, _, ancestor, target, _, merge = histories
    destination = tmp_path / 'revision.json'
    run = subprocess.run([sys.executable, str(ROOT / 'tools/ci/gate_revision.py'),
                          '--root', str(repository), '--mode', 'selection',
                          '--base', ancestor, '--head', merge, '--output', str(destination)],
                         text=True, capture_output=True, check=False)
    assert run.returncode == 0, run.stderr
    assert f'base_sha={target}\n' in run.stdout
    assert f'head_sha={merge}\n' in run.stdout
    assert json.loads(destination.read_text())['base_sha'] == target


def test_full_gate_rejects_an_unavailable_event_base(histories):
    repository, _, _, _, _, merge = histories
    with pytest.raises((ValueError, subprocess.CalledProcessError)):
        revision.gate_revisions(repository, 'full', '1' * 40, merge)


