CHARM_DIR := $(shell pwd)
TEST_TIMEOUT := 900
#SERIES := $(shell juju get-environment default-series)
SERIES := trusty
HOST_SERIES := $(shell lsb_release -sc)

BUILD_ROOT=/home/stub/charms/built
BUILD_DIR=${BUILD_ROOT}/${SERIES}/postgresql
export LAYER_PATH=/home/stub/layers
export INTERFACE_PATH=/home/stub/interfaces


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


ifeq ($(HOST_SERIES),xenial)
    # Juju 1.x is now juju-1 under Xenial, not juju. Work around this.
    export PATH := /usr/lib/juju-1.25/bin:$(PATH)
endif


test: testdeps lint unittest integration

testdeps:
ifeq ($(HOST_SERIES),trusty)
	sudo apt-get install -y python-tox python3-psycopg2 bzr moreutils \
	    software-properties-common python3-flake8
else
	sudo apt-get install -y tox python3-psycopg2 bzr moreutils \
	    software-properties-common python3-flake8
endif
	sudo add-apt-repository -y ppa:juju/stable
	sudo apt-get install charm-tools

lint:
	@echo "Charm Proof"
	@charm proof
	@echo "Lint check (flake8)"
	@flake8 -v \
            --ignore=E402 \
	    --exclude=lib/testdeps,lib/pgclient/hooks/charmhelpers,lib/charms,__pycache__,.tox \
	    hooks actions testing tests reactive lib

# Clean crud from running tests etc.
buildclean:
	rm -rf ${BUILD_DIR}/lib/pgclient/hooks/charmhelpers
	rm -rf ${BUILD_DIR}/.tox
	rm -rf ${BUILD_DIR}/.cache
	rm -rf ${BUILD_DIR}/.unit-state.db
	rm -rf ${BUILD_DIR}/.coverage

build: buildclean
	@echo "Building charm"
	charm build -o ${BUILD_ROOT} -s ${SERIES}

fbuild: buildclean
	@echo "Forcefully building charm"
	charm build -o ${BUILD_ROOT} -s ${SERIES} --force

# Build with a custom charms.reactive
custombuild: fbuild
	rm -f ${BUILD_DIR}/wheelhouse/charms.reactive*
	rsync -rav --delete \
	    --exclude='*.pyc' --exclude='__pycache__' --exclude='*~' \
	    ${HOME}/charms/charms.reactive/charms/reactive/ \
	    ${BUILD_DIR}/lib/charms/reactive/

_co=,
_empty=
_sp=$(_empty) $(_empty)

TESTFILES=$(filter-out %/test_integration.py,$(wildcard tests/test_*.py))
PACKAGES=$(subst $(_sp),$(_co),$(notdir $(basename $(wildcard hooks/*.py))))

tox: .tox/testenv/bin/python3

.tox/testenv/bin/python3: requirements.txt
	tox --notest -r

# Put the testenv on the PATH so the juju-wait plugin is found.
export PATH := .tox/testenv/bin:$(PATH)

NOSE := .tox/testenv/bin/nosetests -sv
TIMING_NOSE := ${NOSE} --with-timer

unittest: tox
	${NOSE} ${TESTFILES}
	@echo OK: Unit tests pass `date`

# Coverage broken?
#
# unittest: tox
# 	${NOSE} ${TESTFILES} --cover-package=${PACKAGES} \
# 	    --with-coverage --cover-branches
# 	@echo OK: Unit tests pass `date`
# 
# coverage: tox
# 	${NOSE} ${TESTFILES} --cover-package=${PACKAGES} \
# 	    --with-coverage --cover-branches \
# 	    --cover-erase --cover-html --cover-html-dir=coverage \
# 	    --cover-min-percentage=100 || \
# 		(gnome-open coverage/index.html; false)


# We need to unpack charmhelpers so the old non-reactive test client
# charm works (rather than embed another copy).
client-charmhelpers:
	tar -xz --strip-components 1 \
           -f wheelhouse/charmhelpers-*.tar.gz \
           -C lib/pgclient/hooks --wildcards '*/charmhelpers'

integration-deps: tox client-charmhelpers

integration: integration-deps
	${TIMING_NOSE} tests/test_integration.py 2>&1 | ts

# More overheads, but better progress reporting
integration_breakup: integration-deps
	${NOSE} tests/test_integration.py:PG93Tests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG93MultiTests 2>&1 | ts
	${NOSE} tests/test_integration.py:UpgradedCharmTests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG91Tests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG91MultiTests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG95Tests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG95MultiTests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG94Tests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG94MultiTests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG92Tests 2>&1 | ts
	${NOSE} tests/test_integration.py:PG92MultiTests 2>&1 | ts
	@echo OK: Integration tests pass `date`

# These targets are to separate the test output in the Charm CI system
# eg. 'make test_integration.py:PG93Tests'
test_integration.py%: integration-deps
	${TIMING_NOSE} tests/$@ 2>&1 | ts
	@echo OK: $@ tests pass `date`
