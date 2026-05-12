"""Tests for security validation checks."""

from pathlib import Path

import pytest

from static_validate import (
    ValidationResult,
    check_no_hardcoded_secrets,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def result():
    return ValidationResult()


class TestNoHardcodedSecrets:
    def test_no_hardcoded_secrets(self, result):
        check_no_hardcoded_secrets(REPO_ROOT, result)
        assert result.success, (
            "Hardcoded secrets detected:\n"
            + "\n".join(f"  - {e}" for e in result.errors)
        )


class TestSecretPatternDetection:
    """Unit tests for the secret detection regex logic."""

    def test_safe_environ_not_flagged(self):
        r = ValidationResult()
        check_no_hardcoded_secrets(REPO_ROOT, r)
        for err in r.errors:
            assert "os.environ" not in err, \
                f"os.environ usage should not be flagged: {err}"

    def test_safe_file_read_not_flagged(self):
        r = ValidationResult()
        check_no_hardcoded_secrets(REPO_ROOT, r)
        for err in r.errors:
            assert "secrets/" not in err or "hardcoded" not in err.lower(), \
                f"Reading from secrets/ file should not be flagged: {err}"
