.PHONY: install lint test test-cov run clean

install:
	pip install -e ".[dev]"

lint:
	ruff check src/ tests/
	pylint src/nl_to_sql --score=yes
	mypy src/nl_to_sql

test:
	pytest tests/unit/ -v

test-cov:
	pytest tests/unit/ \
		--cov=nl_to_sql \
		--cov-report=term-missing \
		--cov-fail-under=80 \
		-v

test-all:
	pytest tests/ -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -f coverage.xml .coverage
