.PHONY: lint test ci static-validate cluster-smoke clean-local

lint:
	ruff check scripts/ tests/
	pylint scripts/ tests/ --disable=C,R --fail-under=0 || true

test:
	pytest tests/ -v --tb=short

static-validate:
	python tests/static_validate.py

ci: lint test static-validate

cluster-smoke:
	bash tests/cluster_smoke.sh

clean-local:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/ *.egg-info/
