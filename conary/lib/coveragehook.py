# Copyright (c) 2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
    Imports our modified version of coverage.py which has support for the COVERAGE_DIR
    directive.  If the COVERAGE_DIR environment variable is set, it will automatically
    start running coverage.  Forks will continue to run coverage, to a different pid.
"""
import atexit
import imp
import os
import signal
import sys

from conary.lib import util

def install():
    """
        Starts the coverage tool if the coverage dir directive is set.
    """
    if os.environ.get('COVERAGE_DIR', None):
        _install()

def save():
    if 'coverage' in sys.modules:
        sys.modules['coverage'].the_coverage.save()

def _save():
    sys.modules['coverage'].the_coverage.save()

def _install():
    coverageLoc = os.environ.get('COVERAGE_TOOL', None)
    if not coverageLoc:
        raise RuntimeError, 'cannot find coverage.py!'

    coverageDir = os.environ.get('COVERAGE_DIR', None)
    if not coverageDir:
        raise RuntimeError, 'COVERAGE_DIR must be set to a path for cache file'
    util.mkdirChain(coverageDir)

    if ('coverage' in sys.modules 
        and (sys.modules['coverage'].__file__ == coverageLoc 
             or sys.modules['coverage'].__file__ == coverageLoc + 'c')):
        coverage = sys.modules['coverage']
    else:
        coverage = imp.load_source('coverage', coverageLoc)
    the_coverage = coverage.the_coverage
    if hasattr(the_coverage, 'pid') and the_coverage.pid == os.getpid():
        _run(coverage)
        return
    elif hasattr(the_coverage, 'pid'):
        _reset(coverage)

    _installOsWrapper()
    _run(coverage)
    return


def _saveState(signal, f):
    save()
    sys.exit(1)

def _run(coverage):
    signal.signal(signal.SIGUSR2, _saveState)
    atexit.register(coverage.the_coverage.save)
    coverage.the_coverage.get_ready()
    coverage.c.enable()

origOsFork = os.fork
origOsExit = os._exit
origExecArray = {}

for exectype in 'l', 'le', 'lp', 'lpe', 'v', 've', 'vp', 'vpe':
    fnName = 'exec' + exectype
    origExecArray[fnName] = getattr(os, fnName)

def _installOsWrapper():
    """
        wrap fork to automatically start a new coverage
        file with the forked pid.
    """
    global origOsFork
    global origOsExit
    def fork_wrapper():
        pid = origOsFork()
        if pid:
            return pid
        else:
            _reset(sys.modules['coverage'])
            _run(sys.modules['coverage'])
            return 0

    def exit_wrapper(*args):
        sys.modules['coverage'].the_coverage.save()
        origOsExit(*args)

    def exec_wrapper(fn):
        def _exec_wrapper(*args, **kw):
            sys.modules['coverage'].the_coverage.save()
            return fn(*args, **kw)
        return _exec_wrapper

    if os.fork is origOsFork:
        os.fork = fork_wrapper

    if os._exit is origOsExit:
        os._exit = exit_wrapper

    for fnName, origFn in origExecArray.iteritems():
        curFn = getattr(os, fnName)
        if curFn is origFn:
            setattr(os, fnName, exec_wrapper(origFn))

def _reset(coverage):
    coverage.c.disable()
    coverage.c.clear()
    coverage.the_coverage = None
    coverage.the_coverage = coverage.coverage()


install()
