CHARM_DIR := $(shell pwd)
TEST_TIMEOUT := 900
SERIES := $(shell juju get-environment default-series 2> /dev/null | juju get-model-config default-series 2> /dev/null | echo trusty)
HOST_SERIES := $(shell lsb_release -sc)
JUJU := juju

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


# Munge the path so the requested version of Juju is found, and thus used
# by Amulet and juju-deployer.
export PATH := /usr/lib/juju-$(shell $(JUJU) --version | perl -p -e "s/-.*//")/bin:$(PATH)


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


CHARM_NAME := postgresql

LAYER_BRANCH := master
DEVEL_BRANCH := test-built
STABLE_BRANCH := built

JUJU_REPOSITORY := /home/stub/charms/built
BUILD_DIR := ${JUJU_REPOSITORY}/${SERIES}/postgresql
CHARM_STORE_URL := cs:~postgresql-charmers/postgresql

export LAYER_PATH=${HOME}/charms/layers
export INTERFACE_PATH=${HOME}/charms/interfaces

# Create the test-build worktree if it doesn't already exist.
$(BUILD_DIR):
	-git branch $(DEVEL_BRANCH) $(LAYER_BRANCH)
	git worktree add $@ $(DEVEL_BRANCH)

# A quick test build, not to be committed or released. Builds
# from the working tree including all untracked and uncommitted
# updates.
.PHONY: build
build: | $(BUILD_DIR)
	charm build -f -o $(JUJU_REPOSITORY) -n $(CHARM_NAME)

# Generate a fresh development build and commit it to $(TEST_BRANCH).
# Only builds work committed to $(LAYER_BRANCH).
.PHONY: dev-build
build-dev: | $(BUILD_DIR)
	-cd $(BUILD_DIR) && git merge --abort
	cd $(BUILD_DIR) \
	    && git reset --hard $(TEST_BRANCH) \
	    && git clean -ffd \
	    && git merge --log --no-commit -s ours \
		-m "charm-build of $(LAYER_BRANCH)" $(LAYER_BRANCH)
	rm -rf .tmp-repo
	git clone -b $(LAYER_BRANCH) . .tmp-repo
	charm build -f -o $(JUJU_REPOSITORY) -n $(CHARM_NAME) .tmp-repo
	rm -rf .tmp-repo
	cd $(BUILD_DIR) && \
	    if [ -n "`git status --porcelain`" ]; then \
	        git add . ; \
		git commit; \
	    else \
		echo "No changes"; \
	    fi

# Generate and publish a fresh development build.
publish-dev: build-dev
	cd $(BUILD_DIR) \
	    && export rev=`charm push . $(CHARM_NAME) 2>&1 \
		| tee /dev/tty | grep url: | cut -f 2 -d ' '` \
	    && git tag -f -m "$$rev" `echo $$rev | tr -s '~:/' -` \
	    && charm publish -c development $$rev
	git push --tags upstream $(LAYER_BRANCH) $(DEVEL_BRANCH)
	git push --tags github $(LAYER_BRANCH) $(DEVEL_BRANCH)

# Publish the latest development build as the stable release in
# both the charm store and in $(STABLE_BRANCH).
.PHONY: publish-stable
publish-stable:
	-git branch $(STABLE_BRANCH) $(DEVEL_BRANCH)
	rm -rf .tmp-repo
	git clone --no-single-branch -b $(STABLE_BRANCH) . .tmp-repo
	cd .tmp-repo \
	    && git merge --no-ff origin/$(DEVEL_BRANCH) --log \
		-m "charm-build of $(LAYER_BRANCH)" \
	    && export rev=`charm push . $(CHARM_NAME) 2>&1 \
		| tee /dev/tty | grep url: | cut -f 2 -d ' '` \
	    && git tag -f -m "$$rev" `echo $$rev | tr -s '~:/' -` \
	    && git push -f --tags .. $(STABLE_BRANCH) \
	    && charm publish -c stable $$rev
	rm -rf .tmp-repo
	git push --tags upstream master built
	git push --tags github master built
	git push --tags bzr built:master

# Clean crud from running tests etc.
#buildclean:
#	cd ${BUILD_DIR} && git reset --hard && git clean -ffd


# Build with a custom charms.reactive
# custombuild: fbuild
# 	rm -f ${BUILD_DIR}/wheelhouse/charms.reactive*
# 	rsync -rav --delete \
# 	    --exclude='*.pyc' --exclude='__pycache__' --exclude='*~' \
# 	    ${HOME}/charms/charms.reactive/charms/reactive/ \
# 	    ${BUILD_DIR}/lib/charms/reactive/

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


# Push tested git branches. Extract a clean copy of the tested built charm
# and publish it.
# publish-stable:
# 	git push --tags upstream master built
# 	git push --tags github master built
# 	git push --tags bzr built:master
# 	rm -rf .push-built
# 	git clone -b built . .push-built
# 	charm publish -c stable \
# 	    `charm push .push-built cs:~postgresql-charmers/postgresql 2>&1 | \
# 	    grep url: | cut -f 2 -d ' '`
# 	rm -rf .push-built
# 	charm grant cs:~postgresql-charmers/postgresql everyone
