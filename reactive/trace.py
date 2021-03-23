from charmhelpers.core import hookenv
from charms.reactive import trace

hookenv.atstart(trace.install_tracer, trace.LogTracer())
