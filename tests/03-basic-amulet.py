#!/usr/bin/python3

import amulet
import os.path

d = amulet.Deployment()

d.add('postgresql', os.path.join(os.path.dirname(__file__), os.pardir))
d.expose('postgresql')

try:
    d.setup(timeout=900)
    d.sentry.wait()
except amulet.helpers.TimeoutError:
    amulet.raise_status(amulet.SKIP, msg="Environment wasn't stood up in time")
except:
    raise

amulet.raise_status(amulet.PASS)
