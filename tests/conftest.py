# === MODULE PURPOSE ===
# Pytest configuration and shared fixtures for tests.

import pytest


# Marker for tests that require live API access
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "live: marks tests as requiring live API access (may be slow/flaky)"
    )


@pytest.fixture
def anyio_backend():
    """Use asyncio as the async backend for pytest-asyncio."""
    return "asyncio"
