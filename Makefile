PY := .venv/bin/python
DAYS ?= 3
TXNS ?= 1000000

.PHONY: setup lint test data run benchmark clean

setup:
	python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"

lint:
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

test:
	.venv/bin/pytest -q

data:  ## generate DAYS days of synthetic landing data with TXNS transactions per day
	$(PY) -m lakehouse.generate.synthetic --days $(DAYS) --txns-per-day $(TXNS)

run:  ## run the full pipeline for every generated day, in order
	@for d in $$(ls data/landing/transactions | sed 's/ingest_date=//' | sort); do \
		echo "== $$d"; $(PY) -m lakehouse.pipeline --run-date $$d || exit 1; \
	done

benchmark:
	$(PY) scripts/benchmark_joins.py

clean:
	rm -rf data spark-warehouse
