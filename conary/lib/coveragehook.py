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

def saveState(signal, f):
    sys.modules['coverage'].the_coverage.save()
    sys.exit(0)


def _install():
    coverageLoc = os.environ.get('COVERAGE_TOOL', None)
    if not coverageLoc:
        raise RuntimeError, 'cannot find coverage.py!'

    coverageDir = os.environ.get('COVERAGE_DIR', None)
    if not coverageDir:
        raise RuntimeError, 'COVERAGE_DIR must be set to a path for cache file'
    util.mkdirChain(coverageDir)

    coverage = imp.load_source('coverage', coverageLoc)
    the_coverage = coverage.the_coverage
    if hasattr(the_coverage, 'pid') and the_coverage.pid == os.getpid():
        return
    elif hasattr(the_coverage, 'pid'):
        _reset(coverage)

    _installOsWrapper()
    _run(coverage)
    return

def _run(coverage):
    signal.signal(signal.SIGQUIT, saveState)
    atexit.register(coverage.the_coverage.save)
    coverage.the_coverage.get_ready()
    sys.settrace(coverage.t)

origOsFork = os.fork
origOsExit = os._exit
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

    if os.fork is origOsFork:
        os.fork = fork_wrapper

    if os._exit is origOsExit:
        os._exit = exit_wrapper

def _reset(coverage):
    sys.settrace(None)
    coverage.c.clear()
    coverage.the_coverage = None
    coverage.the_coverage = coverage.coverage()


install()
