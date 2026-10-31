"""Host-independent CircleCI artifact bridge for GitHub issue reporting."""

def event_target(*args):
    raise NotImplementedError('CircleCI event target')

def valid_pipeline(*args):
    raise NotImplementedError('CircleCI identity')

def manifest_files(*args):
    raise NotImplementedError('complete artifact manifest')

def newer_than_history(*args):
    raise NotImplementedError('monotonic baseline')
