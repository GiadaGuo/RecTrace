"""
conftest.py
-----------
Shared pytest fixtures: fakeredis client injected into all tests.
"""

import pytest
import fakeredis


@pytest.fixture
def fake_redis():
    """A fresh fakeredis instance for each test."""
    return fakeredis.FakeRedis(decode_responses=True)
