"""Shared pytest fixtures for framework tests."""

import pytest

from tests.helpers import install_optional_dependency_stubs, run_dispatcher_once


install_optional_dependency_stubs()


@pytest.fixture
def run_pipeline():
    """Factory fixture that returns a helper to run pipelines for testing."""
    return run_dispatcher_once
