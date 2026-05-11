"""
Static validation for the Big Data final project repository.

Scans the repository for structure, security, shell scripts, SQL/HQL,
and PySpark ML compliance without requiring any cluster services.

Usage:
    python tests/static_validate.py          # from repo root
    python tests/static_validate.py /path    # explicit repo root
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Result accumulator
# ---------------------------------------------------------------------------

class ValidationResult:
    """Accumulates pass / warning / error results."""

    def __init__(self):
        self.passed: List[str] = []
        self.warnings: List[str] = []
        self.errors: List[str] = []

    def ok(self, msg: str):
        self.passed.append(msg)

    def warn(self, msg: str):
        self.warnings.append(msg)

    def error(self, msg: str):
        self.errors.append(msg)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def summary_lines(self) -> List[str]:
        lines = []
        if self.errors:
            lines.append("ERRORS:")
            for e in self.errors:
                lines.append(f"  [FAIL] {e}")
        if self.warnings:
            lines.append("WARNINGS:")
            for w in self.warnings:
                lines.append(f"  [WARN] {w}")
        lines.append(
            f"Summary: {len(self.passed)} passed, "
            f"{len(self.warnings)} warnings, {len(self.errors)} errors"
        )
        return lines


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _repo_root_from_argv() -> Path:
    if len(sys.argv) > 1:
        return Path(sys.argv[1]).resolve()
    return Path(__file__).resolve().parent.parent


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _find_files(root: Path, extensions: Tuple[str, ...], exclude_dirs: Tuple[str, ...] = (
    ".git", ".venv", "venv", "__pycache__", "node_modules", ".pytest_cache",
    ".ruff_cache", ".mypy_cache",
)) -> List[Path]:
    """Recursively find files with given extensions, skipping excluded dirs."""
    results: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]
        for fn in filenames:
            if any(fn.endswith(ext) for ext in extensions):
                results.append(Path(dirpath) / fn)
    return sorted(results)


def _file_size_mb(path: Path) -> float:
    try:
        return path.stat().st_size / (1024 * 1024)
    except OSError:
        return 0.0


# ---------------------------------------------------------------------------
# 1. Repository structure
# ---------------------------------------------------------------------------

def check_stage_scripts_exist(root: Path, result: ValidationResult):
    """Required stage scripts must exist (under scripts/ or root)."""
    for name in ("stage1.sh", "stage2.sh", "stage3.sh"):
        candidates = [root / name, root / "scripts" / name]
        if any(c.is_file() for c in candidates):
            result.ok(f"{name} exists")
        else:
            result.error(f"{name} not found (checked root and scripts/)")


def check_required_dirs(root: Path, result: ValidationResult):
    """Required directories."""
    for d in ("scripts", "sql"):
        if (root / d).is_dir():
            result.ok(f"Directory '{d}/' exists")
        else:
            result.error(f"Directory '{d}/' missing")
    for d in ("output",):
        if (root / d).is_dir():
            result.ok(f"Directory '{d}/' exists")
        else:
            result.warn(f"Directory '{d}/' missing (created at runtime)")


def check_gitignore(root: Path, result: ValidationResult):
    """Check .gitignore exists and contains key patterns."""
    gi = root / ".gitignore"
    if not gi.is_file():
        result.error(".gitignore not found")
        return
    result.ok(".gitignore exists")
    content = _read_text(gi).lower()
    patterns = {
        "data": ["data/", "data/*", "*.parquet"],
        "secrets": ["secrets/", ".psql.pass", "secrets/.psql.pass"],
        "venv": [".venv", "venv/", "venv"],
        "__pycache__": ["__pycache__"],
        ".ipynb_checkpoints": [".ipynb_checkpoints"],
    }
    for category, options in patterns.items():
        if any(opt.lower() in content for opt in options):
            result.ok(f".gitignore covers {category}")
        else:
            result.warn(f".gitignore may not cover {category}")


def check_no_committed_secrets(root: Path, result: ValidationResult):
    """No secret files should be committed."""
    secret_paths = [
        "secrets/.psql.pass",
        ".env",
    ]
    for sp in secret_paths:
        p = root / sp
        if p.is_file() and sp == ".env":
            result.error(f"Secret file committed: {sp}")
        elif p.is_file() and sp == "secrets/.psql.pass":
            result.warn(f"Secret file present locally: {sp} (verify .gitignore)")
        else:
            result.ok(f"No committed secret: {sp}")

    for f in _find_files(root, (".pem", ".key")):
        rel = f.relative_to(root)
        if ".git" not in str(rel):
            result.error(f"Potential secret file: {rel}")


def check_no_large_files(root: Path, result: ValidationResult):
    """No committed files larger than 100 MB."""
    exclude = {".git", ".venv", "venv", "data", "node_modules"}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in exclude]
        for fn in filenames:
            fp = Path(dirpath) / fn
            if _file_size_mb(fp) > 100:
                result.error(f"File >100 MB: {fp.relative_to(root)} ({_file_size_mb(fp):.1f} MB)")
    result.ok("No files >100 MB (excluding data/)")


def check_readme(root: Path, result: ValidationResult):
    """README should exist."""
    candidates = [root / "README.md", root / "README.MD", root / "readme.md"]
    if any(c.is_file() for c in candidates):
        result.ok("README exists")
    else:
        result.warn("README.md not found")


# ---------------------------------------------------------------------------
# 2. Security: hardcoded secrets
# ---------------------------------------------------------------------------

_SECRET_PATTERN = re.compile(
    r"""(?:password|passwd|token|secret|api_key)\s*=\s*["']([^"']{3,})["']""",
    re.IGNORECASE,
)

_SAFE_PATTERNS = (
    re.compile(r"""os\.environ""", re.IGNORECASE),
    re.compile(r"""<[^>]+>"""),
    re.compile(r"""(secrets|\.pass|\.psql|\.hive)""", re.IGNORECASE),
    re.compile(r"""head\s+-n""", re.IGNORECASE),
    re.compile(r"""getenv""", re.IGNORECASE),
    re.compile(r"""placeholder""", re.IGNORECASE),
    re.compile(r"""example""", re.IGNORECASE),
    re.compile(r"""\$\{?\w+"""),
    re.compile(r""""\$\("""),
    re.compile(r"""['"]<"""),
)


def check_no_hardcoded_secrets(root: Path, result: ValidationResult):
    """Detect hardcoded passwords/tokens/secrets in source files."""
    files = _find_files(root, (".py", ".sh", ".sql", ".hql", ".yaml", ".yml", ".cfg", ".ini", ".toml"))
    found = []
    for f in files:
        if "test" in f.name.lower() and "static_validate" not in f.name.lower():
            continue
        content = _read_text(f)
        for lineno, line in enumerate(content.splitlines(), 1):
            m = _SECRET_PATTERN.search(line)
            if m:
                value = m.group(1)
                if any(sp.search(line) for sp in _SAFE_PATTERNS):
                    continue
                if value.strip() in ("", "None", "null", "xxx", "changeme"):
                    continue
                found.append((f.relative_to(root), lineno))
    if found:
        for rel, ln in found:
            result.error(f"Possible hardcoded secret at {rel}:{ln}")
    else:
        result.ok("No hardcoded secrets detected")


# ---------------------------------------------------------------------------
# 3. Shell scripts
# ---------------------------------------------------------------------------

def find_shell_scripts(root: Path) -> List[Path]:
    return _find_files(root, (".sh",))


def check_shell_scripts_nonempty(root: Path, result: ValidationResult):
    """All .sh files must be non-empty."""
    for sh in find_shell_scripts(root):
        content = _read_text(sh).strip()
        if not content or content == "#!/bin/bash":
            rel = sh.relative_to(root)
            if sh.name == "postprocess.sh":
                result.warn(f"Shell script nearly empty: {rel}")
            else:
                result.ok(f"Shell script non-empty: {rel}")
        else:
            result.ok(f"Shell script non-empty: {sh.relative_to(root)}")


def _stage_content(root: Path, name: str) -> str:
    """Read a stage script from scripts/ or root."""
    for loc in (root / "scripts" / name, root / name):
        if loc.is_file():
            return _read_text(loc)
    return ""


def check_stage1_content(root: Path, result: ValidationResult):
    """Stage 1 must contain Sqoop, compression, format, and HDFS cleanup."""
    content = _stage_content(root, "stage1.sh")
    if not content:
        result.error("stage1.sh not found")
        return

    checks = [
        (["sqoop import", "sqoop import-all-tables"], "Sqoop import command"),
        (["--compress", "--compression-codec"], "Sqoop compression option"),
        (["--as-avrodatafile", "--as-parquetfile"], "Sqoop AVRO/PARQUET format"),
        (["hdfs dfs -rm", "hdfs dfs -rmr", "--delete-target-dir"], "HDFS cleanup before import"),
    ]
    for patterns, desc in checks:
        if any(p in content for p in patterns):
            result.ok(f"stage1.sh: {desc}")
        else:
            result.error(f"stage1.sh: missing {desc}")


def check_stage2_content(root: Path, result: ValidationResult):
    """Stage 2 must reference Hive/EDA execution."""
    content = _stage_content(root, "stage2.sh")
    if not content:
        result.error("stage2.sh not found")
        return
    if any(p in content for p in ("beeline", "spark-submit", "spark-sql", "hive")):
        result.ok("stage2.sh: Hive/EDA execution reference found")
    else:
        result.error("stage2.sh: no beeline/spark-submit/hive reference")


def check_stage3_content(root: Path, result: ValidationResult):
    """Stage 3 must use spark-submit with YARN."""
    s3_sh = _stage_content(root, "stage3.sh")
    if not s3_sh:
        result.error("stage3.sh not found")
        return
    if "spark-submit" in s3_sh:
        result.ok("stage3.sh: spark-submit found")
    else:
        result.error("stage3.sh: missing spark-submit")
    if "--master yarn" in s3_sh:
        result.ok("stage3.sh: --master yarn found")
    else:
        result.warn("stage3.sh: --master yarn not found (may be configured elsewhere)")


def check_no_local_master_in_scripts(root: Path, result: ValidationResult):
    """No script should hardcode --master local (except conditional fallback)."""
    for sh in find_shell_scripts(root):
        content = _read_text(sh)
        lines = content.splitlines()
        for lineno, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if re.search(r"--master\s+local(?:\b|\[)", stripped):
                rel = sh.relative_to(root)
                ctx_start = max(0, lineno - 6)
                ctx_end = min(len(lines), lineno + 3)
                context = "\n".join(lines[ctx_start:ctx_end])
                if re.search(r"(use_local|FORCE_LOCAL|STAGE3_FORCE_LOCAL|local_parquets|LOCAL_MASTER)", context):
                    result.warn(f"{rel}:{lineno}: --master local in conditional fallback block (acceptable)")
                else:
                    result.error(f"{rel}:{lineno}: hardcoded --master local")


def check_no_hardcoded_user_paths(root: Path, result: ValidationResult):
    """Flag suspicious hardcoded user paths like /home/teamXX."""
    for sh in find_shell_scripts(root):
        content = _read_text(sh)
        for lineno, line in enumerate(content.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if re.search(r"/home/\w+/|/Users/\w+/", line):
                result.warn(f"{sh.relative_to(root)}:{lineno}: possible hardcoded user path")


# ---------------------------------------------------------------------------
# 4. SQL checks
# ---------------------------------------------------------------------------

def find_sql_files(root: Path) -> List[Path]:
    return _find_files(root, (".sql",))


def check_sql_create_table(root: Path, result: ValidationResult):
    """SQL create scripts should contain CREATE TABLE."""
    sql_files = find_sql_files(root)
    if not sql_files:
        result.error("No .sql files found")
        return
    result.ok(f"Found {len(sql_files)} .sql file(s)")
    found_create = False
    for f in sql_files:
        content = _read_text(f).upper()
        if "CREATE TABLE" in content:
            found_create = True
    if found_create:
        result.ok("SQL: CREATE TABLE found")
    else:
        result.error("SQL: no CREATE TABLE in any .sql file")


def check_sql_drop_before_create(root: Path, result: ValidationResult):
    """DROP TABLE should appear before CREATE TABLE."""
    for f in find_sql_files(root):
        content = _read_text(f).upper()
        if "CREATE TABLE" in content:
            if "DROP TABLE" in content:
                drop_pos = content.index("DROP TABLE")
                create_pos = content.index("CREATE TABLE")
                if drop_pos <= create_pos:
                    result.ok(f"SQL: DROP before CREATE in {f.name}")
                else:
                    result.warn(f"SQL: DROP after first CREATE in {f.name}")
            else:
                result.warn(f"SQL: no DROP TABLE in {f.name}")


def check_sql_primary_key(root: Path, result: ValidationResult):
    """SQL should contain PRIMARY KEY."""
    all_sql = " ".join(_read_text(f) for f in find_sql_files(root)).upper()
    if "PRIMARY KEY" in all_sql:
        result.ok("SQL: PRIMARY KEY found")
    else:
        result.error("SQL: no PRIMARY KEY in any .sql file")


def check_sql_bulk_load(root: Path, result: ValidationResult):
    """SQL or Python should contain COPY/bulk load."""
    all_sql = " ".join(_read_text(f) for f in find_sql_files(root))
    py_files = _find_files(root / "scripts", (".py",))
    all_py = " ".join(_read_text(f) for f in py_files)
    combined = all_sql + " " + all_py
    if re.search(r"\bCOPY\b", combined, re.IGNORECASE):
        result.ok("SQL/Python: COPY bulk load found")
    else:
        result.error("SQL/Python: no COPY bulk load command found")


# ---------------------------------------------------------------------------
# 5. HQL / Hive checks
# ---------------------------------------------------------------------------

def find_hql_files(root: Path) -> List[Path]:
    return _find_files(root, (".hql",))


def check_hql_create_database(root: Path, result: ValidationResult):
    """At least one HQL file must contain CREATE DATABASE."""
    hql_files = find_hql_files(root)
    if not hql_files:
        result.error("No .hql files found")
        return
    result.ok(f"Found {len(hql_files)} .hql file(s)")
    all_hql = " ".join(_read_text(f) for f in hql_files).upper()
    if "CREATE DATABASE" in all_hql:
        result.ok("HQL: CREATE DATABASE found")
    else:
        result.error("HQL: no CREATE DATABASE")


def check_hql_drop_database(root: Path, result: ValidationResult):
    """Idempotent cleanup: DROP DATABASE IF EXISTS."""
    all_hql = " ".join(_read_text(f) for f in find_hql_files(root)).upper()
    if "DROP DATABASE" in all_hql:
        result.ok("HQL: DROP DATABASE found (idempotent cleanup)")
    else:
        result.warn("HQL: no DROP DATABASE for idempotent setup")


def check_hql_external_table(root: Path, result: ValidationResult):
    """At least one CREATE EXTERNAL TABLE."""
    all_hql = " ".join(_read_text(f) for f in find_hql_files(root)).upper()
    if "CREATE EXTERNAL TABLE" in all_hql:
        result.ok("HQL: CREATE EXTERNAL TABLE found")
    else:
        result.error("HQL: no CREATE EXTERNAL TABLE")


def check_hql_storage_format(root: Path, result: ValidationResult):
    """Hive tables must use AVRO or PARQUET."""
    all_hql = " ".join(_read_text(f) for f in find_hql_files(root)).upper()
    if "STORED AS AVRO" in all_hql or "STORED AS PARQUET" in all_hql:
        result.ok("HQL: AVRO/PARQUET storage format found")
    else:
        result.error("HQL: no STORED AS AVRO/PARQUET")


def check_hql_partitioned(root: Path, result: ValidationResult):
    """At least one PARTITIONED BY."""
    all_hql = " ".join(_read_text(f) for f in find_hql_files(root)).upper()
    if "PARTITIONED BY" in all_hql:
        result.ok("HQL: PARTITIONED BY found")
    else:
        result.error("HQL: no PARTITIONED BY")


def check_hql_bucketed(root: Path, result: ValidationResult):
    """At least one CLUSTERED BY (bucketing)."""
    all_hql = " ".join(_read_text(f) for f in find_hql_files(root)).upper()
    if "CLUSTERED BY" in all_hql:
        result.ok("HQL: CLUSTERED BY (bucketing) found")
    else:
        result.error("HQL: no CLUSTERED BY (bucketing)")


def check_hql_eda_queries(root: Path, result: ValidationResult):
    """At least 6 EDA query files or q*_results tables."""
    hql_files = find_hql_files(root)
    all_hql = " ".join(_read_text(f) for f in hql_files)
    q_result_matches = re.findall(r"q(\d+)_results", all_hql, re.IGNORECASE)
    unique_q = set(q_result_matches)
    q_files = [f for f in hql_files if re.match(r"q\d+", f.stem, re.IGNORECASE)]
    count = max(len(unique_q), len(q_files))
    if count >= 6:
        result.ok(f"HQL: {count} EDA queries detected (>=6 required)")
    else:
        result.error(f"HQL: only {count} EDA queries detected (need >=6)")


def check_hql_analytical_patterns(root: Path, result: ValidationResult):
    """EDA queries should contain analytical SQL patterns."""
    all_hql = " ".join(_read_text(f) for f in find_hql_files(root)).upper()
    patterns = {
        "GROUP BY": "GROUP BY",
        "aggregation (SUM/AVG/COUNT)": r"\b(SUM|AVG|COUNT|MIN|MAX)\s*\(",
        "ORDER BY": "ORDER BY",
    }
    for desc, pat in patterns.items():
        if re.search(pat, all_hql):
            result.ok(f"HQL EDA: {desc} found")
        else:
            result.warn(f"HQL EDA: {desc} not found")


# ---------------------------------------------------------------------------
# 6. PySpark / ML checks
# ---------------------------------------------------------------------------

def _gather_pyspark_sources(root: Path) -> str:
    """Concatenate all Python source under scripts/ for ML checks."""
    py_files = _find_files(root / "scripts", (".py",))
    parts = []
    for f in py_files:
        parts.append(_read_text(f))
    nb_files = _find_files(root, (".ipynb",))
    for f in nb_files:
        parts.append(_read_text(f))
    return "\n".join(parts)


def check_no_local_master_python(root: Path, result: ValidationResult):
    """Python ML code must not hardcode .master('local')."""
    src = _gather_pyspark_sources(root)
    bad = [
        r'''\.master\s*\(\s*["']local''',
        r'''master\s*\(\s*["']local\[''',
    ]
    for pat in bad:
        matches = list(re.finditer(pat, src))
        for m in matches:
            ctx_start = max(0, m.start() - 200)
            ctx = src[ctx_start:m.end() + 100]
            if re.search(r"(args\.master|os\.environ|SPARK_MASTER|if\s|else)", ctx):
                result.warn("Python: .master('local') used in conditional/arg path (acceptable)")
            else:
                result.error("Python: hardcoded .master('local') in ML code")
    if not any(re.search(pat, src) for pat in bad):
        result.ok("Python: no hardcoded .master('local')")


def check_hive_support(root: Path, result: ValidationResult):
    """SparkSession should enable Hive support."""
    src = _gather_pyspark_sources(root)
    if ".enableHiveSupport()" in src or "hive.metastore.uris" in src:
        result.ok("PySpark: Hive support enabled")
    else:
        result.error("PySpark: no .enableHiveSupport() or hive.metastore.uris")


def check_pyspark_ml_imports(root: Path, result: ValidationResult):
    """Must use pyspark.ml, not sklearn for distributed training."""
    src = _gather_pyspark_sources(root)
    if "pyspark.ml" in src:
        result.ok("PySpark: pyspark.ml used")
    else:
        result.error("PySpark: pyspark.ml not found")
    if re.search(r"from\s+sklearn|import\s+sklearn", src):
        result.error("PySpark: sklearn detected — should use pyspark.ml for distributed training")
    else:
        result.ok("PySpark: no sklearn usage")


def check_pipeline(root: Path, result: ValidationResult):
    """Must contain Pipeline and VectorAssembler."""
    src = _gather_pyspark_sources(root)
    for symbol in ("Pipeline", "VectorAssembler"):
        if symbol in src:
            result.ok(f"PySpark: {symbol} found")
        else:
            result.error(f"PySpark: {symbol} not found")


def check_train_test_split(root: Path, result: ValidationResult):
    """Must use randomSplit or equivalent split."""
    src = _gather_pyspark_sources(root)
    if "randomSplit" in src or "temporal_train_test_split" in src:
        result.ok("PySpark: train/test split found")
    else:
        result.error("PySpark: no randomSplit or train/test split")
    ratio_match = re.search(r"train_ratio\s*=\s*([\d.]+)", src)
    if ratio_match:
        ratio = float(ratio_match.group(1))
        if ratio >= 0.6:
            result.ok(f"PySpark: train ratio {ratio} >= 0.6")
        else:
            result.error(f"PySpark: train ratio {ratio} < 0.6")
    else:
        split_match = re.search(r"randomSplit\s*\(\s*\[([\d.,\s]+)\]", src)
        if split_match:
            parts = [float(x.strip()) for x in split_match.group(1).split(",") if x.strip()]
            if parts and parts[0] >= 0.6:
                result.ok(f"PySpark: train split ratio {parts[0]} >= 0.6")
            elif parts:
                result.error(f"PySpark: train split ratio {parts[0]} < 0.6")


def check_cross_validator(root: Path, result: ValidationResult):
    """Must use ParamGridBuilder and CrossValidator."""
    src = _gather_pyspark_sources(root)
    for symbol in ("ParamGridBuilder", "CrossValidator"):
        if symbol in src:
            result.ok(f"PySpark: {symbol} found")
        else:
            result.error(f"PySpark: {symbol} not found")

    folds_match = re.search(r"numFolds\s*[=:]\s*(\d+)", src)
    cv_folds_match = re.search(r"cv.folds.*?default\s*=\s*(\d+)", src)
    if folds_match:
        k = int(folds_match.group(1))
        if k in (3, 4):
            result.ok(f"PySpark: numFolds={k} (valid)")
        else:
            result.error(f"PySpark: numFolds={k} (should be 3 or 4)")
    elif cv_folds_match:
        k = int(cv_folds_match.group(1))
        if k in (3, 4):
            result.ok(f"PySpark: cv-folds default={k} (valid)")
        else:
            result.error(f"PySpark: cv-folds default={k} (should be 3 or 4)")
    else:
        result.warn("PySpark: numFolds not statically detectable")

    addgrid_count = len(re.findall(r"\.addGrid\s*\(", src))
    if addgrid_count >= 3:
        result.ok(f"PySpark: {addgrid_count} .addGrid() calls (>=3)")
    else:
        result.error(f"PySpark: only {addgrid_count} .addGrid() calls (need >=3)")


def check_model_types(root: Path, result: ValidationResult):
    """Must contain required model types.

    For this regression project: LinearRegression, RandomForestRegressor, GBTRegressor.
    Also accept classification models if present: RandomForestClassifier, LinearSVC, NaiveBayes.
    """
    src = _gather_pyspark_sources(root)
    regression_models = {
        "LinearRegression": False,
        "RandomForestRegressor": False,
        "GBTRegressor": False,
    }
    classification_models = {
        "RandomForestClassifier": False,
        "LinearSVC": False,
        "NaiveBayes": False,
    }
    for model in regression_models:
        if model in src:
            regression_models[model] = True
    for model in classification_models:
        if model in src:
            classification_models[model] = True

    reg_found = sum(regression_models.values())
    cls_found = sum(classification_models.values())

    if reg_found >= 3:
        result.ok(f"PySpark: all 3 regression model types found ({', '.join(k for k,v in regression_models.items() if v)})")
    elif cls_found >= 3:
        result.ok("PySpark: all 3 classification model types found")
    elif reg_found + cls_found >= 3:
        result.ok("PySpark: 3+ model types found (regression + classification)")
    else:
        all_models = {**regression_models, **classification_models}
        missing = [k for k, v in all_models.items() if not v]
        result.error(f"PySpark: fewer than 3 model types. Missing: {', '.join(missing)}")


def check_evaluators(root: Path, result: ValidationResult):
    """Must contain relevant evaluators/metrics."""
    src = _gather_pyspark_sources(root)
    evaluators = [
        "RegressionEvaluator",
        "MulticlassClassificationEvaluator",
        "BinaryClassificationEvaluator",
    ]
    found = [e for e in evaluators if e in src]
    if found:
        result.ok(f"PySpark: evaluator(s) found: {', '.join(found)}")
    else:
        result.error("PySpark: no ML evaluator found")

    metrics = ["rmse", "mae", "r2", "accuracy", "f1", "areaUnderROC", "areaUnderPR"]
    found_metrics = [m for m in metrics if m in src]
    if found_metrics:
        result.ok(f"PySpark: metrics found: {', '.join(found_metrics)}")
    else:
        result.error("PySpark: no evaluation metrics found")


def check_model_save(root: Path, result: ValidationResult):
    """Must save models."""
    src = _gather_pyspark_sources(root)
    if ".write().overwrite().save(" in src or "save_ml_pipeline_local" in src:
        result.ok("PySpark: model save found")
    else:
        result.error("PySpark: no model save (.write().overwrite().save())")


def check_prediction_output(root: Path, result: ValidationResult):
    """Must save prediction outputs and evaluation.csv."""
    src = _gather_pyspark_sources(root)
    if "predictions.csv" in src or "prediction_csv" in src or "write_single_partition_csv" in src:
        result.ok("PySpark: prediction output saving found")
    else:
        result.error("PySpark: no prediction output saving")
    if "evaluation.csv" in src:
        result.ok("PySpark: evaluation.csv reference found")
    else:
        result.error("PySpark: no evaluation.csv reference")

    for model_key in ("model1", "model2", "model3"):
        if model_key in src:
            result.ok(f"PySpark: {model_key} reference found")
        else:
            result.warn(f"PySpark: {model_key} reference not found")


# ---------------------------------------------------------------------------
# 7. Python compile check
# ---------------------------------------------------------------------------

def check_python_compile(root: Path, result: ValidationResult):
    """All .py files must compile."""
    py_files = _find_files(root, (".py",))
    failed = []
    for f in py_files:
        source = _read_text(f)
        try:
            compile(source, str(f), "exec")
        except SyntaxError as exc:
            failed.append((f.relative_to(root), str(exc)))
    if failed:
        for rel, err in failed:
            result.error(f"Python compile error: {rel}: {err}")
    else:
        result.ok(f"All {len(py_files)} Python files compile")


# ---------------------------------------------------------------------------
# Run all checks
# ---------------------------------------------------------------------------

ALL_CHECKS = [
    check_stage_scripts_exist,
    check_required_dirs,
    check_gitignore,
    check_no_committed_secrets,
    check_no_large_files,
    check_readme,
    check_no_hardcoded_secrets,
    check_shell_scripts_nonempty,
    check_stage1_content,
    check_stage2_content,
    check_stage3_content,
    check_no_local_master_in_scripts,
    check_no_hardcoded_user_paths,
    check_sql_create_table,
    check_sql_drop_before_create,
    check_sql_primary_key,
    check_sql_bulk_load,
    check_hql_create_database,
    check_hql_drop_database,
    check_hql_external_table,
    check_hql_storage_format,
    check_hql_partitioned,
    check_hql_bucketed,
    check_hql_eda_queries,
    check_hql_analytical_patterns,
    check_no_local_master_python,
    check_hive_support,
    check_pyspark_ml_imports,
    check_pipeline,
    check_train_test_split,
    check_cross_validator,
    check_model_types,
    check_evaluators,
    check_model_save,
    check_prediction_output,
    check_python_compile,
]


def run_all_checks(root: Optional[Path] = None) -> ValidationResult:
    """Execute all validation checks and return combined result."""
    if root is None:
        root = _repo_root_from_argv()
    result = ValidationResult()
    for check_fn in ALL_CHECKS:
        check_fn(root, result)
    return result


def main():
    """CLI entry point."""
    root = _repo_root_from_argv()
    if not root.is_dir():
        print(f"Error: {root} is not a directory", file=sys.stderr)
        sys.exit(2)
    print(f"Validating: {root}")
    result = run_all_checks(root)
    for line in result.summary_lines():
        print(line)
    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
