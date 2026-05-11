#!/bin/bash
# Cluster-only smoke test — NOT run by GitHub Actions CI.
# Run manually from repo root: bash tests/cluster_smoke.sh
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass_count=0
warn_count=0
fail_count=0

ok()   { echo -e "${GREEN}[PASS]${NC} $1"; pass_count=$((pass_count + 1)); }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; warn_count=$((warn_count + 1)); }
fail() { echo -e "${RED}[FAIL]${NC} $1"; fail_count=$((fail_count + 1)); }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Cluster Smoke Test"
echo "Repo root: $ROOT"
echo ""

# Check required commands
echo "Checking commands"
for cmd in hdfs sqoop beeline spark-submit; do
    if command -v "$cmd" &>/dev/null; then
        ok "$cmd available"
    else
        fail "$cmd not found in PATH"
    fi
done

# Check secrets files (never print contents)
echo ""
echo "Checking secrets"
for secret in secrets/.psql.pass secrets/.hive.pass; do
    if [[ -f "$secret" ]]; then
        if [[ -s "$secret" ]]; then
            ok "$secret exists and is non-empty"
        else
            fail "$secret exists but is empty"
        fi
    else
        fail "$secret not found"
    fi
done

# Check local output files
echo ""
echo "--- Checking local output files ---"

core_files=(
    "output/hive_results.txt"
    "output/evaluation.csv"
    "output/q1.csv"
    "output/q2.csv"
    "output/q3.csv"
    "output/q4.csv"
    "output/q5.csv"
    "output/q6.csv"
)

optional_files=(
    "output/q7.csv"
    "output/q8.csv"
    "output/q9.csv"
    "output/q10.csv"
    "output/q11.csv"
    "output/q12.csv"
    "output/model1_predictions.csv"
    "output/model2_predictions.csv"
    "output/model3_predictions.csv"
)

for f in "${core_files[@]}"; do
    if [[ -s "$f" ]]; then
        ok "$f exists"
    elif [[ -f "$f" ]]; then
        warn "$f exists but is empty"
    else
        fail "$f missing (core output)"
    fi
done

for f in "${optional_files[@]}"; do
    if [[ -s "$f" ]]; then
        ok "$f exists"
    elif [[ -f "$f" ]]; then
        warn "$f exists but is empty"
    else
        warn "$f missing (optional output)"
    fi
done

echo ""
echo "Checking HDFS paths"

hdfs_paths=(
    "project/warehouse"
    "project/hive/warehouse"
)

if command -v hdfs &>/dev/null; then
    for hp in "${hdfs_paths[@]}"; do
        if hdfs dfs -test -d "$hp" 2>/dev/null; then
            ok "HDFS: $hp exists"
        else
            warn "HDFS: $hp not found"
        fi
    done
else
    warn "hdfs command not available; skipping HDFS checks"
fi

# Summary
echo ""
echo "Smoke test summary"
echo -e "${GREEN}Passed: ${pass_count}${NC}"
echo -e "${YELLOW}Warnings: ${warn_count}${NC}"
echo -e "${RED}Failures: ${fail_count}${NC}"

if [[ $fail_count -gt 0 ]]; then
    exit 1
fi
exit 0
