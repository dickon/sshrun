"""Launch subprocesses"""

from subprocess import Popen, PIPE
from os import kill, read, close
from time import time
from pipes import quote
from signal import SIGKILL
from tempfile import mkstemp, mkdtemp
from os import unlink, environ
from sshrun.time_limit import time_limit, TimeoutError
from select import POLLIN, POLLPRI, poll, error
from errno import EINTR
from os.path import exists
from sshrun.retry import retry

class SubprocessError(Exception):
    """A subprocess failed"""

class SubprocessFailure(SubprocessError):
    """A subprocess returned a non-zero exit code"""

class UnableToRunCommandsOnHost(SubprocessError):
    """Unable to run commands on host"""

def space_escape(args):
    """Escale spaces in args"""
    return [quote(x) for x in args]

def run(args, timeout=60, host=None, split=False, word_split=False, 
        line_split=False,
        ignore_failure=False, verify=True,
        cwd=None, user=None, env={}, shell=False, stderr=False,  echo=False, 
        verbose=False, announce_interval = 20, wait=True,
        stdin_push='', output_callback=None, 
        error_callback=None):
    """Run command with args, or raise SubprocessTimeout after timeout seconds.

    If host is specified, ssh to that machine. Let's hope your ssh configuration
    works.

    If split is true, convert output to a list of lines, where each
    line is a list of words.
    
    If word_split is true, convert output to a list of whitespace separated words.

    If line_split is true, convert output to a list of lines.

    If ignore_failure is true, do not raise exceptions for non-zero exit codes.
    
    If cwd is true, run commands in that working directory using a shell.

    If env is a dictionary, set those environment variables.

    If shell is true, run through a shell (implied by cwd or env).

    If stderr is true, return stderr as well as stdout. Otherwise or by default
    return just stdout.

    If echo is true, echo stdout/stderr through to sys.stdout/sys.stderr

    If verbose is true, print arguments timing and exit code.
    See http://stackoverflow.com/questions/1191374/subprocess-with-timeout

    If verify and host are set, then make sure the connection to the host
    works before trying it.

    If wait is false, then simply launch the command and return straight away.
    """
    description = ' '.join(args)
    if host and verify:
        verify_connection(host, user, timeout=timeout, verbose=verbose)
    
    spargs = space_escape(args)    
    if host:
        shell_prefixes = []
        if cwd:
            shell_prefixes.extend(['cd', cwd, '&&'])
            cwd = None
        for key, value in env.iteritems():
            shell_prefixes.append("%s=%s" % (key, (space_escape([value])[0])))
        env = None
        shell = False
        args = ['ssh', '-oPasswordAuthentication=no', '-l' + (user if user else 'root'), host] + shell_prefixes + spargs
        description += ' on '+host
    if verbose:
        print 'RUN:', repr(args)
    process = Popen(args, stdout=PIPE, stderr=PIPE, stdin=PIPE, shell=shell,
                    env=env, cwd=cwd)

    fd2output = {}
    fd2file = {}
    poller = poll()

    def register_and_append(file_obj, eventmask):
        """Record file_obj for poll operations"""
        poller.register(file_obj.fileno(), eventmask)
        fd2file[file_obj.fileno()] = file_obj
        out = fd2output[file_obj.fileno()] = []
        return out
    def close_unregister_and_remove(fdes):
        """fdes is finished"""
        poller.unregister(fdes)
        fd2file[fdes].close()
        fd2file.pop(fdes)

    def throw_timeout(delay):
        """Throw exception for after delay"""
        try:
            out = run(['ps', '--no-headers', '-o', 'pid', '--ppid', 
                       str(process.pid)])
        except SubprocessError:
            out = ''
        pids = [process.pid] + [int(p) for p in out.split()]
        for pid in pids:
            try:
                kill(pid, SIGKILL)
            except OSError:
                if verbose:
                    print 'WARNING: unable to kill subprocess', pid
        raise TimeoutError(description, timeout, delay)

    if not wait:
        return

    start = time()
    with time_limit(timeout, 'launch '+' '.join(args), 
                    timeout_callback=throw_timeout):
        if stdin_push:
            process.stdin.write(stdin_push)
            process.stdin.flush()
            process.stdin.close()
        stdout_list = register_and_append(process.stdout, POLLIN | POLLPRI)
        stderr_list = register_and_append(process.stderr, POLLIN | POLLPRI)
        announce = time() + announce_interval
        while fd2file:
            if time() > announce:
                announce = time() + announce_interval
                print 'NOTE: waiting', time() - start, 'of', timeout, \
                    'seconds for', ' '.join(args)
            try:
                ready = poller.poll(20)
            except error, eparam:
                if eparam.args[0] == EINTR:
                    continue
                raise
            for fdes, mode in ready:
                if not mode & (POLLIN | POLLPRI):
                    close_unregister_and_remove(fdes)
                    continue
                if fdes not in fd2file:
                    print 'operation on unexpected FD', fdes
                    continue
                data = read(fdes, 4096)
                if not data:
                    close_unregister_and_remove(fdes)
                fd2output[fdes].append(data)
                fileobj = fd2file[fdes]
                if fileobj == process.stdout:
                    if echo:
                        for line in data.splitlines():
                            print 'STDOUT:', line
                    if output_callback:
                        output_callback(data)
                if fileobj == process.stderr:
                    if echo:
                        for line in data.splitlines():
                            print 'STDERR:', line
                    if error_callback:
                        error_callback(data)
            
        process.wait()
        output = ''.join(stdout_list), ''.join(stderr_list)
        exit_code = process.returncode

    delay = time() - start
    
    if verbose:
        print 'RUN: finished', ' '.join(args), 'rc', exit_code, \
            'in', delay, 'seconds', \
            'output', len(output[0]), 'characters'
    if exit_code != 0 and not ignore_failure:
        raise SubprocessError(description, exit_code, output[0], output[1])
    if word_split:
        outv = [x.split() for x in output]
        assert not split # specifiy one of split and word_split only
    elif split:
        outv = [[line.split() for line in x.split('\n')] for 
            x in output] 
    elif line_split:
        outv = [x.split('\n') for x in output]
    else:
        outv = output
    if stderr:
        return (outv[0], outv[1], exit_code) if ignore_failure else outv
    else:
        return (outv[0], exit_code) if ignore_failure else outv[0]

def statcheck(filename, predicate, **args):
    """Return output of predicate or split up stat output on filename,
    or False if stat fails. Supports arguments of run()"""
    statout, exitcode = run(['stat', filename], split=True, ignore_failure=True,
                      **args)
    if statout == [[]] or exitcode != 0:
        return False
    result = predicate(statout)
    return result
        
def isfile(filename, **args):
    """Is filename a file? Supports arguments of run()"""
    return statcheck(filename, lambda x: x[1][-1] == 'file', **args)
    

def isdir(filename, **args):
    """Is filename a directory? Supports arguments of run()"""
    return statcheck(filename, lambda x: x[1][-1] == 'directory', **args)

def islink(filename, **args):
    """Is filename a symbolic link? Supports arguments of run()"""
    return statcheck(filename, lambda x: x[1][-2:] == ['symbolic', 'link'], 
                     **args)

def readfile(filename, host=None, user='root', **args):
    """Return contents of filename on host"""
    if host == None:
        return file(filename, 'rb').read()
    handle, temp = mkstemp()
    try:
        verify_connection(host, user, timeout=10)
        a2 = dict(args)
        a2.pop('cwd', None)
        run(['scp', user+'@'+host+':'+filename, temp], env=None, **a2)
        return file(temp, 'rb').read()
    finally:
        unlink(temp)
        close(handle)

def writefile(filename, content, host=None, user='root',
              **args):
    """Write contents at filename on host"""
    if host == None:
        temp = filename
    else:
        fd, temp = mkstemp()
        close(fd)
    fobj = file(temp, 'wb')
    fobj.write(content)
    fobj.close()
    if host == None:
        return
    try:
        run(['scp', temp, user+'@'+host+':'+filename], env=None,
            **args)
    finally:
        unlink(temp)
    run(['chmod', 'a+rx', filename], host=host, **args)
        
def verify_connection(host, user, timeout=60, verbose=False):
    """Verify that we can connect to host as user"""
    def go():
        run(['true'], verify=False, host=host, user=user, timeout=5)
    try:
        retry(go, 'run true on '+host, timeout=timeout)
    except Exception,exc:
        if verbose:
            print 'RUN: first stage verify failed with', exc
    else:
        return
    
    for line in run(['ps', 'uaxxwww'], split=True):
        if 'ssh' in line and host in line:
            if verbose:
                print 'RUN: killing ssh process', line
            try:
                kill(int(line[1]), SIGKILL)
            except OSError:
                if verbose:
                    print 'NOTE: unable to kill', line[1]
        sfile = '/tmp/root@'+host+':22'
        if exists(sfile):
            try:
                unlink(sfile)
            except OSError:
                pass
    try:
        go()
    except Exception, exc:
        if verbose:
            print 'RUN: second stage verify failed with', repr(exc)
        raise UnableToRunCommandsOnHost(host, user, exc)

def maketempfile(host=None, postfix='', **kd):
    """Make temporary file on host"""
    return run(['mktemp', '/tmp/tmp'+postfix+'XXXXXXXXXX'],
               host=host, **kd).rstrip('\n')

def maketempdirectory(host=None, postfix='', **kd):
    """Make temporary file on host"""
    return run(['mktemp', '-d', '/tmp/tmp'+postfix+'XXXXXXXXXX'],
               host=host, **kd).rstrip('\n')

FUNCTIONS = [run, statcheck, isfile, islink, isdir, readfile, writefile, 
             verify_connection,  maketempfile, maketempdirectory]

def specify(**options):
    """Return a dictionary mapping function names 
    to versions of the functions in this module with defaults changed
    as specified"""
    def subrun(function):
        def returnfn(*argl, **argd):
            """Run args with options"""
            for key in options:
                argd.setdefault(key, options[key])
            return function(*argl, **argd)
        return returnfn
    return dict( [(x.__name__, subrun(x)) for x in FUNCTIONS])
