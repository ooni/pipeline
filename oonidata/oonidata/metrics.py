# -*- coding: utf-8 -*-

"""
Metric generation
"""

from os.path import basename, splitext
from functools import wraps

class MockTimer(object):
    def __call__(self, f):
        @wraps(f)
        def _f(*a, **k):
            return f(*a, **k)
        return _f

class MockStatsClient(object):
    """
    API compatible with the statsd client, but does nothing.
    """
    def __init__(self, host=None, port=None, prefix=None, sample_rate=None):
        pass
    def incr(self, stat, count=1, rate=1):
        pass
    def decr(self, stat, count=1, rate=1):
        pass
    def gauge(self, stat, value, rate=1, delta=False):
        pass
    def set(self, stat, value, rate=1):
        pass
    def timer(self, stat, rate):
        return MockTimer()

_STATSD_AVAILABLE = True
try:
    import statsd  # debdeps: python3-statsd
    statsdclient = statsd.StatsClient
except ImportError:
    _STATSD_AVAILABLE = False
    statsdclient = MockStatsClient

def setup_metrics(host="localhost", name=None):
    """Setup metric generation. Use dotted namespaces e.g.
    "pipeline.centrifugation"
    """
    if name is None:
        import __main__

        prefix = splitext(basename(__main__.__file__))[0]
    else:
        prefix = name

    prefix = prefix.strip(".")
    return statsd.StatsClient(host, 8125, prefix=prefix)
