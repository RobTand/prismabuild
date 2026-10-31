"""Private scratch for pytest launched inside an admitted action."""
from contextlib import contextmanager
from pathlib import Path
import tempfile


@contextmanager
def pytest_basetemp(root=None):
    """Yield a private base outside the checkout, then remove its attempt."""
    attempt = Path(tempfile.mkdtemp(prefix="pb-", dir=root))
    yield attempt / "pytest"
