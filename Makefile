CHARM_DIR := $(shell pwd)
TEST_TIMEOUT := 900

test: lint unit_test integration_test

unit_test:
	@echo "Unit tests of hooks"
	cd hooks && trial test_hooks.py

integration_test:
	@echo "PostgreSQL integration tests, all versions"
	trial test

integration_test_91:
	@echo "PostgreSQL 9.1 integration tests"
	trial test.PG91Tests

integration_test_92:
	@echo "PostgreSQL 9.2 integration tests"
	trial test.PG92Tests

integration_test_93:
	@echo "PostgreSQL 9.3 integration tests"
	trial test.PG93Tests

lint:
	@echo "Lint check (flake8)"
	@flake8 -v \
	    --exclude hooks/charmhelpers,hooks/_trial_temp \
	    hooks testing tests
