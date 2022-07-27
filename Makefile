src = pydanticORM
tests_src = tests
all_src = $(src) $(tests_src)


mypy_base = mypy --show-error-codes
mypy = $(mypy_base) $(all_src)

help:
	@echo "Targets:"
	@echo "    make test"
	@echo "    make lint"
	@echo "    make clean"
	@echo "    make static"

lint:
	pre-commit run --all-files

clean:
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name .coverage`
	rm -f `find . -type f -name ".coverage.*"`
	rm -rf `find . -name __pycache__`
	rm -rf `find . -type d -name '*.egg-info' `
	rm -rf `find . -type d -name 'pip-wheel-metadata' `
	rm -rf `find . -type d -name .pytest_cache`
	rm -rf `find . -type d -name .cache`
	rm -rf `find . -type d -name .mypy_cache`
	rm -rf `find . -type d -name htmlcov`
	rm -rf `find . -type d -name "*.egg-info"`
	rm -rf `find . -type d -name build`
	rm -rf `find . -type d -name dist`

test:
	bash scripts/test.sh && make clean

static:
	$(mypy)