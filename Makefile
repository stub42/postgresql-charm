test:
	cd hooks && trial test_hooks.py
	echo "Integration tests using Juju deployed units"
	TEST_TIMEOUT=900 ./test.py -v

lint:
	bzr ls-lint
