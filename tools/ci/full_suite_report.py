"""Compare full-suite outcomes and report regressions."""

def classify(*args):
    raise NotImplementedError('failure classification')

def owners(*args):
    raise NotImplementedError('issue ownership')

def issue_plan(*args):
    raise NotImplementedError('issue decisions')

def previous_run(*args):
    raise NotImplementedError('previous main run')

def collect_results(*args):
    raise NotImplementedError('shard completeness')
