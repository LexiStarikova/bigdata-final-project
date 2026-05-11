"""Integration tests: run the full static_validate suite and assert overall result."""

from pathlib import Path

import pytest

from static_validate import ValidationResult, run_all_checks

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestFullValidation:
    """Run all checks and verify the aggregate result."""

    @pytest.fixture(scope="class")
    def result(self) -> ValidationResult:
        return run_all_checks(REPO_ROOT)

    def test_no_critical_errors(self, result: ValidationResult):
        """The project must pass all critical validations."""
        assert result.success, (
            f"Validation failed with {len(result.errors)} error(s):\n"
            + "\n".join(f"  - {e}" for e in result.errors)
        )

    def test_passed_checks_nonzero(self, result: ValidationResult):
        assert len(result.passed) > 0, "No checks passed — validator may be broken"

    def test_summary_format(self, result: ValidationResult):
        lines = result.summary_lines()
        assert any("Summary:" in line for line in lines)


class TestValidationResultAPI:
    """Unit tests for the ValidationResult accumulator."""

    def test_ok(self):
        r = ValidationResult()
        r.ok("good")
        assert r.success
        assert len(r.passed) == 1

    def test_warn_does_not_fail(self):
        r = ValidationResult()
        r.warn("minor")
        assert r.success

    def test_error_fails(self):
        r = ValidationResult()
        r.error("bad")
        assert not r.success

    def test_summary_lines(self):
        r = ValidationResult()
        r.ok("a")
        r.warn("b")
        r.error("c")
        lines = r.summary_lines()
        summary = "\n".join(lines)
        assert "1 passed" in summary
        assert "1 warnings" in summary
        assert "1 errors" in summary
