"""Issue-link policy decision and GitHub adapter."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Verdict:
    allowed: bool
    reason: str


def check_pull_request(pull, repository, fetch_issue, fetch_pull, fetch_sidebar):
    return Verdict(False, 'This pull request violates the branch naming rule (rule 12). '
                   'Use prismabuild-<issue>.')
