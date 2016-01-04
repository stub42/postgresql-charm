CHARM_DIR := $(shell pwd)
TEST_TIMEOUT := 900
SERIES := $(shell juju get-environment default-series)
HOST_SERIES := $(shell lsb_release -sc)

# /!\ Ensure that errors early in pipes cause failures, rather than
# overridden by the last stage of the pipe. cf. 'test.py | ts'
SHELL := /bin/bash
export SHELLOPTS:=errexit:pipefail


default:
	@echo "One of:"
	@echo "    make testdeps"
	@echo "    make lint"
	@echo "    make unittest"
	@echo "    make integration"
	@echo "    make coverage (opens browser)"


_fail_ex:
	false | ts

_success_ex:
	true | ts



test: testdeps lint unittest integration

testdeps:
	sudo add-apt-repository -y ppa:juju/stable
	sudo add-apt-repository -y ppa:stub/juju
	sudo apt-get update
ifeq ($(HOST_SERIES),trusty)
	sudo apt-get install -y \
	    python3-psycopg2 python3-nose python3-flake8 amulet \
	    python3-jinja2 python3-yaml juju-wait bzr python3-amulet \
	    python-swiftclient moreutils
else
	sudo apt-get install -y \
	    python3-psycopg2 python3-nose python3-flake8 amulet \
	    python3-jinja2 python3-yaml juju-wait bzr python3-amulet \
	    python3-nose-cov python3-nose-timer python-swiftclient moreutils
endif

lint:
	@echo "Charm Proof"
	@charm proof
	@echo "Lint check (flake8)"
	@flake8 -v \
            --ignore=E402 \
	    --exclude=lib/charmhelpers,lib/pgclient/hooks/charmhelpers,lib/pypi,__pycache__ \
	    hooks actions testing tests reactive lib

_co=,
_empty=
_sp=$(_empty) $(_empty)

TESTFILES=$(filter-out %/test_integration.py,$(wildcard tests/test_*.py))
PACKAGES=$(subst $(_sp),$(_co),$(notdir $(basename $(wildcard hooks/*.py))))

NOSE := nosetests3 -sv
ifeq ($(HOST_SERIES),trusty)
TIMING_NOSE := nosetests3 -sv
else
TIMING_NOSE := nosetests3 -sv --with-timer
endif

unittest:
	${NOSE} ${TESTFILES} --cover-package=${PACKAGES} \
	    --with-coverage --cover-branches
	@echo OK: Unit tests pass `date`

coverage:
	${NOSE} ${TESTFILES} --cover-package=${PACKAGES} \
	    --with-coverage --cover-branches \
	    --cover-erase --cover-html --cover-html-dir=coverage \
	    --cover-min-percentage=100 || \
		(gnome-open coverage/index.html; false)

integration:
	${TIMING_NOSE} tests/test_integration.py 2>&1 | ts
	@echo OK: Integration tests pass `date`

sync: sync-charmhelpers sync-pypi

# Embed from a branch, as we often will need patches applied.
sync-charmhelpers:
	@bzr cat \
	    lp:charm-helpers/tools/charm_helpers_sync/charm_helpers_sync.py \
		> .charm_helpers_sync.py
	rm -rf lib/charmhelpers
	@python .charm_helpers_sync.py -c charm-helpers.yaml
	@rm .charm_helpers_sync.py
	git add -A lib/charmhelpers

	
# Embed pure python pypi dependencies.
sync-pypi:
	rm -rf lib/pypi
	mkdir lib/pypi
	pip3 install --no-compile --no-deps -t lib/pypi charms.reactive
	git add -A lib/pypi


# These targets are to separate the test output in the Charm CI system
# eg. 'make test_integration.py:PG93Tests'
test_integration.py%:
	${TIMING_NOSE} tests/$@ 2>&1 | ts
	@echo OK: $@ tests pass `date`
