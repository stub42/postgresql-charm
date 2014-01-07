CHARM_DIR := $(shell pwd)

test:
	cd hooks && CHARM_DIR=$(CHARM_DIR) trial test_hooks.py
	echo "Integration tests using Juju deployed units"
	TEST_TIMEOUT=900 ./test.py -v

lint:
	bzr ls-lint
