"""Retry functions"""
from time import time, sleep
from sshrun.time_limit import TimeoutError

class TimeoutStillFalseError(TimeoutError):
    """A function is still returning false after too long"""

def retry(fn, description, pace=1.0, timeout=60.0, 
          catch=[Exception], propagate=[], verbose=False):
    """Run fn, retrying in the event of a exceptions on the catch
    list for up to timeout seconds, waiting pace seconds between attempts"""
    start_time = time()
    count = 0
    while 1:
        count += 1
        delta_t = time() - start_time
        if verbose:
            print 'RETRY:', description, 'iteration', count, 'used', delta_t,
            print 'timeout', timeout
        try:
            result = fn()
            if verbose:
                print 'RETRY:', description, 'iteration', count, 'succeeded'
            return result
        except Exception, exc:
            matches = [x for x in catch if isinstance(exc, x)]
            propagates = [x for x in propagate if isinstance(exc, x)]
            delta_t = time() - start_time
            if delta_t < timeout and matches and not propagates:
                if verbose:
                    print 'RETRY:', description, 'iteration', count,
                    print 'failed with', repr(exc )
            else:
                raise
        if verbose:
            print 'RETRY: sleeping', pace, 'seconds after iteration', count,
            print 'of', description, 'failed'
        sleep(pace)

def retry_until_true(fn, description='seeking truth', pace=1.0, timeout=60.0, 
                     false_callback=lambda: None, verbose=False):
    """Keep running function until it returns a truish value.

    Arguments:

      fn: the function we wish to return true
      description: text describing what we are doing
      pace: number of seconds to pace out attempts to call fn
      timeout: give up aftr this number of seconds 
      false_callback: function to call if fn is false

    Returns:
      
      the result of fn, which will be a true value
       
    Raises:

      TimeoutStillFalseError
    """

    start_time= time()
    count = 0
    while 1:
        count += 1
            
        if verbose:
            print 'RETRY:', description, 'iteration', count, 
            print 'timeout', timeout

        with time_limit(timeout-delta_t, 'run '+description):
            result = fn()
        delta_t = time() - start_time
        if verbose:
            print 'RETRY:', description, 'truish' if result else 'falsish', 
            print 'on iteration', count
        if result:
            print
            return result

        sleep(pace)
        
