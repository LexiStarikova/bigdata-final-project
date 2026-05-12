"""Tests for repository structure, .gitignore, secrets, and README.

Script existence and syntax are already tested in test_shell_scripts.py.
This module covers repo-level structure requirements.
"""

from pathlib import Path

import pytest

from static_validate import (
    ValidationResult,
    check_gitignore,
    check_no_committed_secrets,
    check_no_large_files,
    check_required_dirs,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def result():
    return ValidationResult()


class TestRequiredDirs:
    def test_required_dirs(self, result):
        check_required_dirs(REPO_ROOT, result)
        assert result.success, f"Missing directories: {result.errors}"

    def test_scripts_dir(self):
        assert (REPO_ROOT / "scripts").is_dir()

    def test_sql_dir(self):
        assert (REPO_ROOT / "sql").is_dir()

    def test_output_dir(self):
        """output/ should exist (created by stages)."""
        assert (REPO_ROOT / "output").is_dir()


class TestGitignore:
    def test_gitignore_exists(self):
        assert (REPO_ROOT / ".gitignore").is_file()

    def test_gitignore_content(self, result):
        check_gitignore(REPO_ROOT, result)
        assert result.success, f"Gitignore errors: {result.errors}"

    def test_gitignore_data(self):
        content = (REPO_ROOT / ".gitignore").read_text().lower()
        assert any(p in content for p in ("data/", "data/*", "*.parquet")), \
            ".gitignore should cover data files"

    def test_gitignore_pycache(self):
        content = (REPO_ROOT / ".gitignore").read_text().lower()
        assert "__pycache__" in content

    def test_gitignore_venv(self):
        content = (REPO_ROOT / ".gitignore").read_text().lower()
        assert any(p in content for p in (".venv", "venv/", "venv"))

    def test_gitignore_ipynb_checkpoints(self):
        content = (REPO_ROOT / ".gitignore").read_text().lower()
        assert ".ipynb_checkpoints" in content

    def test_gitignore_psql_pass(self):
        content = (REPO_ROOT / ".gitignore").read_text()
        assert ".psql.pass" in content, ".gitignore should cover secrets/.psql.pass"


class TestNoCommittedSecrets:
    def test_no_committed_secrets(self, result):
        check_no_committed_secrets(REPO_ROOT, result)
        assert result.success, f"Secret files found: {result.errors}"

    def test_no_env_file(self):
        assert not (REPO_ROOT / ".env").is_file(), ".env should not be committed"

    def test_no_pem_files(self):
        exclude = {".git", ".venv", "venv", "node_modules"}
        pem_files = [
            f for f in REPO_ROOT.rglob("*.pem")
            if not any(part in exclude for part in f.parts)
        ]
        assert not pem_files, f"Found .pem files: {pem_files}"

    def test_no_key_files(self):
        exclude = {".git", ".venv", "venv", "node_modules"}
        key_files = [
            f for f in REPO_ROOT.rglob("*.key")
            if not any(part in exclude for part in f.parts)
        ]
        assert not key_files, f"Found .key files: {key_files}"


class TestNoLargeFiles:
    def test_no_large_files(self, result):
        check_no_large_files(REPO_ROOT, result)
        assert result.success, f"Large files: {result.errors}"


class TestReadme:
    def test_readme_exists(self):
        candidates = [REPO_ROOT / "README.md", REPO_ROOT / "README.MD"]
        assert any(c.is_file() for c in candidates), "README not found"


class TestRequirementsTxt:
    """requirements.txt must list cluster dependencies."""

    def test_file_exists(self):
        assert (REPO_ROOT / "requirements.txt").is_file()

    def test_pyspark_listed(self):
        content = (REPO_ROOT / "requirements.txt").read_text()
        assert "pyspark" in content

    def test_pylint_listed(self):
        content = (REPO_ROOT / "requirements.txt").read_text()
        assert "pylint" in content


class TestOutputArtifacts:
    """Check that output/ contains expected Stage I/II/III artifacts."""

    def test_avsc_file_in_output(self):
        """Rubric: .avsc file should be in output/."""
        avsc = list((REPO_ROOT / "output").glob("*.avsc"))
        assert avsc, "output/ should contain .avsc file from Sqoop/Stage 1"

    def test_java_file_in_output(self):
        """Rubric: .java file should be in output/."""
        java = list((REPO_ROOT / "output").glob("*.java"))
        assert java, "output/ should contain .java file from Sqoop codegen"

    def test_evaluation_csv_in_output(self):
        """Rubric: evaluation.csv from Stage 3."""
        assert (REPO_ROOT / "output" / "evaluation.csv").is_file()

    def test_eda_csv_files(self):
        """Rubric: at least 6 q*.csv files."""
        csv_files = list((REPO_ROOT / "output").glob("q*.csv"))
        assert len(csv_files) >= 6, f"Only {len(csv_files)} q*.csv files, need >=6"

    def test_hive_results_txt(self):
        assert (REPO_ROOT / "output" / "hive_results.txt").is_file()
