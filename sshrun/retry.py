"""Retry functions"""
from time import time, sleep
from sshrun.time_limit import TimeoutError, time_limit

class TimeoutStillFalseError(TimeoutError):
    """A function is still returning false after too long"""

def retry(fn, description, pace=1.0, timeout=60.0, 
          retry_on_false = False, catch=[], propagate=[], verbose=False):
    """Keeping running function until success.

    Arguments:

        fn: the function we wish to run
        retry_on_falsish: if true, retry if we get a falsish result from fn
        catch: exceptions types to ignore and retry
        pace: number of seconds to pace out attempts to call fn
        timeout: give up after this number of seconds
        fail_callback: function to call on failure


    Run fn, retrying in the event of a exceptions on the catch
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
            elapsed = time() - start_time
            with time_limit(timeout-elapsed, 'run '+description):
                result = fn()
            if verbose:
                print 'RETRY:', description, 'iteration', count, 'finished'
            if catch is not None or result:
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
        delta1_t = time() - start_time

        with time_limit(timeout-delta1_t, 'run '+description):
            result = fn()
        delta_t = time() - start_time
        if verbose:
            print 'RETRY:', description, 'truish' if result else 'falsish', 
            print 'on iteration', count
        if result:
            print
            return result

        sleep(pace)
        
