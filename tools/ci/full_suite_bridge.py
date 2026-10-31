#!/usr/bin/env python3
"""Fetch public CircleCI JUnit as data; publish issues with the host's GitHub token."""
from __future__ import annotations

import argparse
import importlib.util
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile

_spec = importlib.util.spec_from_file_location('full_suite_report', Path(__file__).with_name('full_suite_report.py'))
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)
JOB = 'full-suite-report-inputs'
MAX_DOWNLOAD = 64 * 1024 * 1024


def event_target(event: dict, repository: str) -> tuple[str, int, str] | None:
    check = event.get('check_run')
    if check:
        if check.get('name') != JOB or check.get('status') != 'completed':
            return None
        url, sha = check.get('details_url', ''), check.get('head_sha', '')
    elif 'inputs' in event:
        number, sha = event['inputs'].get('job_number', ''), event['inputs'].get('sha', '')
        return ('job', int(number), sha) if re.fullmatch(r'[1-9][0-9]*', str(number)) and re.fullmatch(r'[a-f0-9]{40}', sha) else None
    else:
        if event.get('context') not in {'CircleCI Pipeline', 'ci/circleci: ' + JOB} or event.get('state') == 'pending':
            return None
        url, sha = event.get('target_url') or '', event.get('sha', '')
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != 'https' or parsed.netloc not in {'circleci.com', 'app.circleci.com'} or not re.fullmatch(r'[a-f0-9]{40}', sha):
        return None
    repo = re.escape(repository)
    if match := re.fullmatch(rf'/gh/{repo}/([1-9][0-9]*)/?', parsed.path):
        return 'job', int(match[1]), sha
    if match := re.fullmatch(rf'/pipelines/(?:github|gh)/{repo}/[1-9][0-9]*/workflows/[^/]+/jobs/([1-9][0-9]*)/?', parsed.path):
        return 'job', int(match[1]), sha
    if match := re.fullmatch(rf'/pipelines/(?:github|gh)/{repo}/([1-9][0-9]*)/?', parsed.path):
        return 'pipeline', int(match[1]), sha
    return None


def valid_pipeline(pipeline: dict, repository: str, sha: str) -> bool:
    return (pipeline.get('project_slug') in {'gh/' + repository, 'github/' + repository}
            and pipeline.get('vcs', {}).get('branch') == 'main'
            and pipeline.get('vcs', {}).get('revision') == sha)


def manifest_files(manifest: dict, sha: str) -> list[str]:
    if (manifest.get('schema') != 1 or manifest.get('sha') != sha
            or manifest.get('branch') != 'main' or manifest.get('complete') is not True):
        raise ValueError('expected a complete main manifest for the exact SHA')
    files = manifest.get('junit')
    if not isinstance(files, list) or not files or len(files) != len(set(files)):
        raise ValueError('manifest needs a nonempty unique JUnit list')
    for name in files:
        path = PurePosixPath(name)
        if path.is_absolute() or '..' in path.parts or not name.startswith('full-suite/junit/') or path.suffix != '.xml':
            raise ValueError('JUnit paths must be relative under full-suite/junit/')
    return files


def newer_than_history(number: int, history: dict) -> bool:
    return number > history.get('circleci_pipeline_number', 0)


def public_bytes(url: str) -> bytes:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != 'https' or parsed.username or parsed.password:
        raise ValueError('public downloads require HTTPS without credentials')
    # No authorization headers are attached here, including after redirects.
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read(MAX_DOWNLOAD + 1)
    if len(data) > MAX_DOWNLOAD:
        raise ValueError('download exceeds 64 MiB')
    return data


class CircleCI:
    def __init__(self, repository: str):
        self.slug = 'gh/' + repository

    def get(self, path: str):
        return json.loads(public_bytes('https://circleci.com/api/v2/' + path))

    def pages(self, path: str) -> list[dict]:
        items, token = [], None
        for _ in range(1000):
            suffix = '' if token is None else ('&' if '?' in path else '?') + urllib.parse.urlencode({'page-token': token})
            value = self.get(path + suffix)
            items.extend(value['items'])
            token = value.get('next_page_token')
            if not token:
                return items
        raise ValueError('CircleCI pagination exceeded 1000 pages')

    def resolve(self, kind: str, number: int) -> tuple[dict, dict] | None:
        if kind == 'job':
            job = self.get(f'project/{self.slug}/job/{number}')
            if job['name'] != JOB or job['status'] not in {'success', 'failed'}:
                return None
            pipeline = self.get('pipeline/' + urllib.parse.quote(job['pipeline']['id'], safe=''))
            return job, pipeline
        pipeline = next((p for p in self.pages(f'project/{self.slug}/pipeline?branch=main') if p['number'] == number), None)
        if pipeline is None:
            raise ValueError('CircleCI pipeline was not found')
        jobs = []
        for workflow in self.pages(f"pipeline/{pipeline['id']}/workflow"):
            jobs.extend(j for j in self.pages(f"workflow/{workflow['id']}/job")
                        if j['name'] == JOB and j['status'] in {'success', 'failed'})
        if not jobs:
            return None
        job = max(jobs, key=lambda j: j['job_number'])
        return self.resolve('job', job['job_number'])

    def junit(self, number: int, sha: str, directory: Path) -> None:
        rows = self.pages(f'project/{self.slug}/{number}/artifacts')
        artifacts = {row['path']: row['url'] for row in rows}
        if len(artifacts) != len(rows):
            raise ValueError('duplicate CircleCI artifact paths')

        def fetch(name):
            url = artifacts[name]
            host = urllib.parse.urlparse(url).hostname or ''
            if not (host == 'output.circle-artifacts.com' or host.endswith('.circle-artifacts.com')):
                raise ValueError('unexpected CircleCI artifact host')
            return public_bytes(url)

        manifest = json.loads(fetch('full-suite/run.json'))
        total = 0
        for name in manifest_files(manifest, sha):
            data = fetch(name)
            total += len(data)
            if total > 256 * 1024 * 1024:
                raise ValueError('JUnit set exceeds 256 MiB')
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)


def github_artifact(api: report.GitHub, artifact_id: int) -> dict:
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    request = urllib.request.Request(api.root + f'actions/artifacts/{artifact_id}/zip',
                                     headers={'Authorization': f'Bearer {api.token}'})
    try:
        urllib.request.build_opener(NoRedirect).open(request, timeout=60)
    except urllib.error.HTTPError as exc:
        if exc.code != 302:
            raise
        location = exc.headers['Location']
    else:
        raise ValueError('GitHub artifact did not redirect')
    archive = public_bytes(location)
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        info = zipped.getinfo('results.json')
        if info.file_size > 256 * 1024 * 1024:
            raise ValueError('results exceed 256 MiB')
        return json.loads(zipped.read(info))


def previous_results(api: report.GitHub, run_number: int) -> dict:
    runs = api.pages('actions/workflows/full-suite-report.yml/runs?branch=main&status=completed', 'workflow_runs')
    for run in sorted(runs, key=lambda r: r['run_number'], reverse=True):
        if run['run_number'] >= run_number:
            continue
        artifacts = api.pages(f"actions/runs/{run['id']}/artifacts", 'artifacts')
        artifact = next((a for a in artifacts if a['name'] == 'full-suite-results' and not a['expired']), None)
        if artifact:
            result = github_artifact(api, artifact['id'])
            if result['complete'] and not result.get('dry_run'):
                return result
    return {}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    repository = os.environ['GITHUB_REPOSITORY']
    target = event_target(json.loads(args.event.read_text()), repository)
    if target is None:
        print('No eligible CircleCI completion event.')
        return 0
    kind, number, sha = target
    circle = CircleCI(repository)
    resolved = circle.resolve(kind, number)
    if resolved is None:
        print('CircleCI has no completed full-suite input job for this event.')
        return 0
    job, pipeline = resolved
    if not valid_pipeline(pipeline, repository, sha):
        raise ValueError('CircleCI project, branch or revision does not match the event')
    api = report.GitHub.from_environment(repository)
    history = previous_results(api, int(os.environ['GITHUB_RUN_NUMBER']))
    if not newer_than_history(pipeline['number'], history):
        print('CircleCI pipeline already reported or superseded; preserving the newer baseline.')
        return 0
    number = job['number']
    directory = args.output.parent / 'junit'
    directory.mkdir(parents=True, exist_ok=False)
    circle.junit(number, sha, directory)
    previous = args.output.parent / 'previous.json'
    previous.write_text(json.dumps(history) + '\n')
    cli = ['--input', str(directory), '--output', str(args.output), '--previous', str(previous),
           '--repository', repository, '--sha', sha,
           '--run-url', f'https://circleci.com/gh/{repository}/{number}']
    if args.dry_run:
        cli.append('--dry-run')
    code = report.main(cli)
    result = json.loads(args.output.read_text())
    result['circleci_pipeline_number'] = pipeline['number']
    args.output.write_text(json.dumps(result, sort_keys=True) + '\n')
    return code


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile, report.ET.ParseError) as exc:
        # Exception strings can contain a signed artifact URL. Never log them.
        print(f'Full-suite bridge could not complete ({type(exc).__name__}).', file=sys.stderr)
        raise SystemExit(2)
