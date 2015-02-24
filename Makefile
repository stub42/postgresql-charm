CHARM_DIR := $(shell pwd)
TEST_TIMEOUT := 900
SERIES := $(juju get-environment default-series)

default:
	@echo "One of:"
	@echo "    make testdep"
	@echo "    make lint"
	@echo "    make unit_test"
	@echo "    make integration_test"
	@echo "    make integration_test_91"
	@echo "    make integration_test_92"
	@echo "    make integration_test_93"
	@echo "    make integration_test_94"
	@echo
	@echo "There is no 'make test'"

test_bot_tests:
	@echo "Installing dependencies and running automatic-testrunner tests"
	tests/00-setup.sh
	tests/01-lint.sh
	tests/02-unit-tests.sh
	tests/03-basic-amulet.py

testdep:
	tests/00-setup.sh

unit_test:
	@echo "Unit tests of hooks"
	cd hooks && trial test_hooks.py

integration_test:
	@echo "PostgreSQL integration tests, all non-beta versions, ${SERIES}"
	trial test.PG91Tests
	trial test.PG92Tests
	trial test.PG93Tests
	trial test.PG94Tests

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
	@echo "PostgreSQL 9.4 integration tests, ${SERIES}"
	trial test.PG94Tests

lint:
	@echo "Lint check (flake8)"
	@flake8 -v \
	    --exclude hooks/charmhelpers,hooks/_trial_temp \
            --ignore=E402 \
	    hooks testing tests test.py

sync:
	@bzr cat \
	    lp:charm-helpers/tools/charm_helpers_sync/charm_helpers_sync.py \
		> .charm_helpers_sync.py
	@python .charm_helpers_sync.py -c charm-helpers.yaml
	@rm .charm_helpers_sync.py
