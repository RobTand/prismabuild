"""Real immutable records preserve the campaign's original publication protocol."""
import ast
from copy import deepcopy
from pathlib import Path
from history_inputs import source_bytes
from types import SimpleNamespace

import pytest

import pbcampaign
from prismabuild import decomposition as dc, immutable_files, residency_plan
from prismabuild.submission_types import SubmissionRequest


ORIGINAL = 'fe416690b87aba99ad30c7df4f95ac8da47055d5'
POLICY = {'task_data_manifest': {'residency_tier': 'tier', 'residency_ram': 'off',
                               'mover_readers': 1, 'mover_mem_gb': 1}}


@pytest.fixture(params=['retained-original', 'current'])
def freeze_data_plan_control(request):
    if request.param == 'current':
        return pbcampaign.frozen_child_data_plans
    source = source_bytes(ORIGINAL, 'tools/fleet/pbcampaign.py')
    nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
             and node.name in {'_stored_document', 'frozen_child_data_plans'}]
    assert len(nodes) == 2
    namespace = dict(vars(pbcampaign), immutable_files=immutable_files)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), ORIGINAL + '/pbcampaign.py', 'exec'), namespace)
    return namespace['frozen_child_data_plans']


@pytest.fixture
def data_plan_control(tmp_path):
    plan = {'parent_key': 'a' * 64, 'plan_key': 'b' * 64}
    children = [{'action_key': 'c' * 64, 'params': {}}]
    cas = SimpleNamespace(root=tmp_path / 'cas')
    path = dc.record_dir(cas.root, plan['parent_key']) / 'data-plans.json'
    path.parent.mkdir(parents=True)
    record = {'schema': 'prismabuild.decomposed_data_plans.v1', **plan,
              'children': [{'action_key': children[0]['action_key'], 'plan': None}]}
    return dict(plan=plan, children=children, cas=cas, path=path, record=record)


def run_data_plan_control(freeze, context, request=POLICY):
    return freeze(request, context['plan'], context['children'], template={},
                  args=SubmissionRequest(), queue=None, cas=context['cas'])


def test_first_data_plan_publication_uses_exact_original_bytes(
        data_plan_control, freeze_data_plan_control):
    context = data_plan_control
    assert run_data_plan_control(freeze_data_plan_control, context) == [None]
    assert context['path'].read_bytes() == dc.document_bytes(context['record'])


def test_existing_data_plan_never_rebuilds_rows(data_plan_control, freeze_data_plan_control):
    context = data_plan_control
    assert run_data_plan_control(freeze_data_plan_control, context) == [None]
    before = context['path'].read_bytes()
    assert run_data_plan_control(freeze_data_plan_control, context, request={}) == [None]
    assert context['path'].read_bytes() == before


def test_lost_data_plan_publication_reads_the_actual_first_writer(
        data_plan_control, freeze_data_plan_control, monkeypatch):
    context = data_plan_control
    winner = b' ' + dc.document_bytes(context['record']) + b'\n'
    publish = immutable_files.publish_immutable
    read = Path.read_bytes
    events = []

    def first_writer(path, proposed):
        assert proposed == dc.document_bytes(context['record'])
        events.append('publish')
        assert publish(path, winner)
        return False

    def observe(path):
        if path == context['path']:
            events.append('read')
        return read(path)

    monkeypatch.setattr(immutable_files, 'publish_immutable', first_writer)
    monkeypatch.setattr(Path, 'read_bytes', observe)
    assert run_data_plan_control(freeze_data_plan_control, context) == [None]
    assert events == ['read', 'publish', 'read']
    assert read(context['path']) == winner


@pytest.mark.parametrize('damage', ['schema', 'parent', 'plan', 'length',
                                   'child_identity', 'child_fields', 'unexpected_plan', 'json'])
def test_damaged_data_plan_keeps_the_original_refusal(
        data_plan_control, freeze_data_plan_control, damage):
    context = data_plan_control
    record = deepcopy(context['record'])
    message = 'decomposed data-plan record is corrupt or belongs to another plan'
    if damage == 'schema':
        record['schema'] = 'foreign'
    elif damage == 'parent':
        record['parent_key'] = 'd' * 64
    elif damage == 'plan':
        record['plan_key'] = 'd' * 64
    elif damage == 'length':
        record['children'] = []
    elif damage == 'child_identity':
        record['children'][0]['action_key'] = 'd' * 64
        message = 'decomposed data-plan child identity differs from the frozen publication'
    elif damage == 'child_fields':
        record['children'][0]['extra'] = True
        message = 'decomposed data-plan child identity differs from the frozen publication'
    elif damage == 'unexpected_plan':
        record['children'][0]['plan'] = {}
        message = 'a no-read child unexpectedly has a staging plan'
    else:
        message = 'is published but is not JSON:'
    context['path'].write_bytes(b'{broken' if damage == 'json' else dc.document_bytes(record))
    before = context['path'].read_bytes()
    with pytest.raises(pbcampaign.ManifestError, match=message) as failure:
        run_data_plan_control(freeze_data_plan_control, context, request={})
    assert failure.value.__cause__ is None
    if damage == 'json':
        assert failure.value.__suppress_context__ is True
    assert context['path'].read_bytes() == before


@pytest.mark.parametrize('first_has_reads', [False, True])
def test_first_child_plan_refusal_precedes_a_later_wrong_child_identity(
        data_plan_control, freeze_data_plan_control, first_has_reads):
    context = data_plan_control
    context['children'].append({'action_key': 'd' * 64, 'params': {}})
    record = context['record']
    record['children'][0]['plan'] = {}
    record['children'].append({'action_key': 'e' * 64, 'plan': None})
    if first_has_reads:
        context['children'][0]['params']['data_manifest'] = {'input': {'sha256': 'f' * 64, 'bytes': 1}}
        error, message = residency_plan.ResidencyPlanError, 'plan schema must be'
    else:
        error, message = pbcampaign.ManifestError, 'a no-read child unexpectedly has a staging plan'
    context['path'].write_bytes(dc.document_bytes(record))
    with pytest.raises(error, match=message):
        run_data_plan_control(freeze_data_plan_control, context, request={})


def test_unreadable_data_plan_keeps_the_original_boundary_error(
        data_plan_control, freeze_data_plan_control):
    context = data_plan_control
    context['path'].symlink_to(context['path'].name)
    with pytest.raises(pbcampaign.ManifestError, match='cannot read .*data-plans.json:'):
        run_data_plan_control(freeze_data_plan_control, context, request={})
    assert context['path'].is_symlink()


def test_data_plan_record_construction_is_pure(data_plan_control, monkeypatch):
    def refuse(path):
        raise AssertionError('construction attempted record I/O')
    monkeypatch.setattr(Path, 'read_bytes', refuse)
    context = data_plan_control
    record = dc.DataPlans(context['cas'].root, context['plan'])
    assert record.path == context['path']
    assert record.plan is context['plan']
