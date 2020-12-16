CHARM_DIR := $(shell pwd)
export SERIES := $(shell juju get-environment default-series 2> /dev/null | juju get-model-config default-series 2> /dev/null | echo xenial)
HOST_SERIES := $(shell lsb_release -sc)
JUJU := juju

# /!\ Ensure that errors early in pipes cause failures, rather than
# overridden by the last stage of the pipe. cf. 'test.py | ts'
SHELL := /bin/bash
export SHELLOPTS:=errexit:pipefail

export AMULET_TIMEOUT := 1800


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


test: lint unittest integration

CHARM_NAME := postgresql

LAYER_BRANCH := master
DEVEL_BRANCH := test-built
STABLE_BRANCH := built

BUILD_ROOT := $(HOME)/charms
BUILD_DIR := $(BUILD_ROOT)/builds/$(CHARM_NAME)
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
	charm build -f -d $(BUILD_DIR)/.. -n $(CHARM_NAME)

# Generate a fresh development build and commit it to $(TEST_BRANCH).
# Only builds work committed to $(LAYER_BRANCH).
.PHONY: dev-build
build-dev: | $(BUILD_DIR)
	-cd $(BUILD_DIR) && git merge --abort
	cd $(BUILD_DIR) \
	    && git reset --hard $(TEST_BRANCH) \
	    && git clean -fxd \
	    && git merge --log --no-commit -s ours \
		-m "charm-build of $(LAYER_BRANCH)" $(LAYER_BRANCH)
	rm -rf .tmp-repo
	git clone -b $(LAYER_BRANCH) . .tmp-repo
	charm build -f -o $(JUJU_REPOSITORY) -n $(CHARM_NAME) --no-local-layers .tmp-repo
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
	    && export rev=`charm push . $(CHARM_STORE_URL) 2>&1 \
		| tee /dev/tty | grep url: | cut -f 2 -d ' '` \
	    && git tag -f -m "$$rev" `echo $$rev | tr -s '~:/' -` \
	    && charm release -c edge $$rev --resource wal-e-0
	git push -f --tags upstream $(LAYER_BRANCH) $(DEVEL_BRANCH)
	git push -f --tags github $(LAYER_BRANCH) $(DEVEL_BRANCH)

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
	    && export rev=`charm push . $(CHARM_STORE_URL) 2>&1 \
		| tee /dev/tty | grep url: | cut -f 2 -d ' '` \
	    && git tag -f -m "$$rev" `echo $$rev | tr -s '~:/' -` \
	    && git push -f --tags .. $(STABLE_BRANCH) \
	    && charm release -c stable $$rev --resource wal-e-0
	rm -rf .tmp-repo
	git push -f --tags upstream master built
	git push -f --tags github master built

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

#_co=,
#_empty=
#_sp=$(_empty) $(_empty)
#TESTFILES=$(filter-out %/test_integration.py,$(wildcard tests/test_*.py))
#PACKAGES=$(subst $(_sp),$(_co),$(notdir $(basename $(wildcard hooks/*.py))))

lint:
	tox -v -e lint


unittest:
	tox -v -e unittest
	@echo OK: Unit tests pass `date`


# We need to unpack charmhelpers so the old non-reactive test client
# charm works (rather than embed another copy).
client-charmhelpers:
	tar -xz --strip-components 1 \
           -f wheelhouse/charmhelpers-*.tar.gz \
           -C lib/pgclient/hooks --wildcards '*/charmhelpers'

integration-deps: client-charmhelpers


NOSE := tox -v -e integration --

integration: integration-deps
	@echo START: $@ tests `date`
	${NOSE} 2>&1
	@echo OK: Integration tests pass `date`

# These targets are to separate the test output in the Charm CI system
# eg. make integration:"PG95 and Multi and replication'
integration\:%: integration-deps
	@echo START: $@ tests `date`
	${NOSE} -k "$(subst integration:,,$@)" 2>&1 | ts
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
