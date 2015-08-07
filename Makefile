CHARM_DIR := $(shell pwd)
TEST_TIMEOUT := 900
SERIES := $(juju get-environment default-series)

default:
	@echo "One of:"
	@echo "    make testdeps"
	@echo "    make lint"
	@echo "    make test"
	@echo "    make unit_test"
	@echo "    make integration_test"
	@echo
	@echo "There is no 'make test'"

test: lint unittest integration

testdeps:
	sudo add-apt-repository -y ppa:stub/juju
	sudo apt-get install -y \
	    python3-psycopg2 python3-nose python3-flake8 amulet \
	    python3-jinja2 python3-yaml juju-wait

unittest:
	nosetests3 -sv tests/test_postgresql.py

lint: proof
	@echo "Lint check (flake8)"
	@flake8 -v \
	    --exclude 'hooks/charmhelpers,hooks/hooks.py' \
            --ignore=E402 \
	    hooks actions testing tests

proof:
	@echo "Charm Proof"
	@charm proof

sync:
	@bzr cat \
	    lp:charm-helpers/tools/charm_helpers_sync/charm_helpers_sync.py \
		> .charm_helpers_sync.py
	@python .charm_helpers_sync.py -c charm-helpers.yaml
	@rm .charm_helpers_sync.py
