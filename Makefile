CHARM_DIR := $(shell pwd)

test: lint unit_test integration_test

unit_test:
	cd hooks && trial test_hooks.py

integration_test:
	echo "Integration tests using Juju deployed units"
	TEST_TIMEOUT=900 ./test.py -v

lint:
	@flake8 --exclude hooks/charmhelpers hooks # requires python-flakes8
