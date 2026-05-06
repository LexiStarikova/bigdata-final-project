"""Shared fixtures for the big data final project test suite."""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

sys.path.insert(0, str(REPO_ROOT / "scripts"))


@pytest.fixture
def repo_root():
    return REPO_ROOT


@pytest.fixture
def scripts_dir(repo_root):
    return repo_root / "scripts"


@pytest.fixture
def sql_dir(repo_root):
    return repo_root / "sql"
