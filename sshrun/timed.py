"""A context manager with a stopwatch"""
from time import time

class Timed:
    def __init__(self, action):
        self.action = action
    def __enter__(self):
        self.t0 = time()
        print 'INFO: starting', self.action
    def __exit__(self, *_):
        deltat = time() - self.t0
        print 'HEADLINE: finished %s in %.3fs' % (self.action, deltat)
