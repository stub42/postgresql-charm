CHARM_DIR := $(shell pwd)
TEST_TIMEOUT := 900
SERIES := $(shell juju get-environment default-series)

default:
	@echo "One of:"
	@echo "    make testdeps"
	@echo "    make lint"
	@echo "    make test"
	@echo "    make unit_test"
	@echo "    make integration_test"
	@echo
	@echo "There is no 'make test'"

test: testdeps proof lint unittest integration

testdeps:
	sudo add-apt-repository -y ppa:stub/juju
	sudo apt-get install -y \
	    python3-psycopg2 python3-nose python3-flake8 amulet \
	    python3-jinja2 python3-yaml juju-wait bzr \
	    python3-nose-cov python3-nose-timer python-swiftclient

_co=,
_empty=
_sp=$(_empty) $(_empty)

TESTFILES=$(filter-out %/test_integration.py,$(wildcard tests/test_*.py))
PACKAGES=$(subst $(_sp),$(_co),$(notdir $(basename $(wildcard hooks/*.py))))

unittest: lint
	nosetests3 -sv ${TESTFILES} --cover-package=${PACKAGES} \
	    --with-coverage --cover-branches
	@echo OK: Unit tests pass `date`

coverage: lint
	nosetests3 -sv ${TESTFILES} --cover-package=${PACKAGES} \
	    --with-coverage --cover-branches \
	    --cover-erase --cover-html --cover-html-dir=coverage \
	    --cover-min-percentage=100 || \
		(gnome-open coverage/index.html; false)

integration: lint
	nosetests3 -sv tests/test_integration.py --with-timer
	@echo OK: Integration tests pass `date`

lint: proof
	@echo "Lint check (flake8)"
	@flake8 -v \
            --ignore=E402 \
	    --exclude=hooks/charmhelpers,__pycache__ \
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
