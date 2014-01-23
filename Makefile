CHARM_DIR := $(shell pwd)

test: lint unit_test integration_test

auto_test: test

unit_test:
	@echo "Unit tests of hooks"
	cd hooks && trial test_hooks.py

integration_test:
	@echo "Integration tests using Juju deployed units"
	TEST_TIMEOUT=900 trial test.py

lint:
	@echo "Lint check (flake8)"
	@flake8 -v --exclude hooks/charmhelpers,hooks/_trial_temp hooks
