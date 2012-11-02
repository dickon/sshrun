from time import time, sleep
from sshrun.time_limit import TimeoutError

class TimeoutStillFalseError(TimeoutError):
    """A function is still returning false after too long"""

def retry(fn, description, pace=1.0, timeout=60.0, catch=[Exception], 
          propagate=[]):
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

def retry_until_true(fn, description='seeking truth', pace=1.0, timeout=60.0, 
                     false_callback=lambda: pass, verbose=False):
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
            
        print 'RETRY:', description, 'iteration', count, 'timeout', timeout
        
        result = fn()
        delta_t = time() - start_time
        print 'RETRY:', description, 'truish' if result else 'falsish', 'on iteration', count,
        if result:
            print
            return result
        else:
            print 'will', 'RETRY' if delta_t < timeout else 'STOP'
        if delta_t >= timeout:
            raise TimeoutStillFalseError(
        sleep(pace)
        
