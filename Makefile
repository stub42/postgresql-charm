CHARM_DIR := $(shell pwd)
TEST_TIMEOUT := 900
SERIES := $(juju get-environment default-series)

default:
	@echo "One of:"
	@echo "    make testdep"
	@echo "    make lint"
	@echo "    make test"
	@echo "    make unit_test"
	@echo "    make integration_test"
	@echo "    make integration_test_91"
	@echo "    make integration_test_92"
	@echo "    make integration_test_93"
	@echo "    make integration_test_94"

test: lint unit_test integration_test

testdep:
	tests/00_setup.test

unit_test:
	@echo "Unit tests of hooks"
	cd hooks && trial test_hooks.py

integration_test:
	@echo "PostgreSQL integration tests, all non-beta versions, ${SERIES}"
	trial test.PG91Tests test.PG92Tests test.PG93Tests

integration_test_91:
	@echo "PostgreSQL 9.1 integration tests, ${SERIES}"
	trial test.PG91Tests

integration_test_92:
	@echo "PostgreSQL 9.2 integration tests, ${SERIES}"
	trial test.PG92Tests

integration_test_93:
	@echo "PostgreSQL 9.3 integration tests, ${SERIES}"
	trial test.PG93Tests

integration_test_94:
	@echo "PostgreSQL 9.4 (beta) integration tests, ${SERIES}"
	trial test.PG94Tests

lint:
	@echo "Lint check (flake8)"
	@flake8 -v \
	    --exclude hooks/charmhelpers,hooks/_trial_temp \
	    hooks testing tests test.py
