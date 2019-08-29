#!/usr/bin/env python3

# Load modules from $JUJU_CHARM_DIR/lib
import sys

sys.path.append("lib")

from charms.layer import basic

basic.bootstrap_charm_deps()
basic.init_config_states()


# This will load and run the appropriate @hook and other decorated
# handlers from $JUJU_CHARM_DIR/reactive, $JUJU_CHARM_DIR/hooks/reactive,
# and $JUJU_CHARM_DIR/hooks/relations.
#
# See https://jujucharms.com/docs/stable/authors-charm-building
# for more information on this pattern.
from charms.reactive import main

main()
