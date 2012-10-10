from time import time, sleep

def retry(fn, description, pace=1.0, timeout=60.0, catch=[Exception], propagate=[]):
    """Run fn, retrying in the event of a exceptions on the catch
    list for up to timeout seconds, waiting pace seconds between attempts"""
    start_time = time()
    count = 0
    while 1:
        count += 1
        delta_t = time() - start_time
        print 'RETRY:', description, 'iteration', count, 'used', delta_t,
        print 'timeout', timeout
        try:
            result = fn()
            print 'RETRY:', description, 'iteration', count, 'succeeded'
            return result
        except Exception, exc:
            matches = [x for x in catch if isinstance(exc, x)]
            propagates = [x for x in propagate if isinstance(exc, x)]
            delta_t = time() - start_time
            if delta_t < timeout and matches and not propagates:
                print 'RETRY:', description, 'iteration', count, \
                    'failed with', repr(exc )
            else:
                raise
        print 'RETRY: sleeping', pace, 'seconds after iteration', count, \
            'of', description, 'failed'
        sleep(pace)
