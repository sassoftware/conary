# -*- mode: python -*-
#
# Copyright (c) 2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.

import os, signal

# This is a list of all signals which are reasonable to catch but normally
# terminate the application; SIGSEGV and SIGBUS are excluded because it's not
# clear that letting python keep running after such signals is a good idea!
# Signal names can differ by architecture, so this is a named list as strings,
# and we turn that into signal numbers based on which ones are actually defined
catchableSignalNames = [ 'SIGABRT', 'SIGALRM', 'SIGFPE', 'SIGHUP',
                         'SIGILL', 'SIGINT', 'SIGIO', 'SIGIOT', 'SIGPIPE',
                         'SIGPOLL', 'SIGPROF', 'SIGPWR', 'SIGQUIT', 'SIGRTMAX',
                         'SIGRTMIN', 'SIGSYS', 'SIGTERM', 'SIGTRAP', 'SIGTSTP',
                         'SIGTTIN', 'SIGTTOU', 'SIGURG', 'SIGUSR1', 'SIGUSR2',
                         'SIGVTALRM', 'SIGWINCH', 'SIGXCPU', 'SIGXFSZ' ]
# list(set()) here removes duplicates (SIGIO/SIGPOLL for instance)
catchableSignals = list(set([ signal.__dict__[x] for x in catchableSignalNames
                     if x in signal.__dict__ ]))
del catchableSignalNames

class SignalException(Exception):
    def reraise(self):
        os.kill(os.getpid(), self.sigNum)

    def __str__(self):
        for name, val in signal.__dict__.iteritems():
            if name.startswith('SIG') and val == self.sigNum:
                break

        if val == self.sigNum:
            return 'SignalException: signal %s received' % name
        else:
            return 'SignalException: signal %d received' % self.sigNum

    def __init__(self, sigNum):
        self.sigNum = sigNum

def signalHandler(sigNum, stack):
    raise SignalException(sigNum)

# decorator which allows a function to handle a signel as an exception; if
# the exception passes back on the return path the signal is resent without
# a handler
def sigprotect(*signals):
    # this isn't quite atomic because python doesn't seem to provide
    # sigaction-style signal functions. that shouldn't really be a problem
    # here though

    if not signals:
        signals = catchableSignals

    def decorator(fn):

        def call(*args, **kwargs):
            exception = None
            rekill = False
            try:
                oldHandlers = []
                for sigNum in signals:
                    # set us up to restore the old handler before changing
                    # it in case the signal is raised right here
                    oldHandler = signal.getsignal(sigNum)
                    oldHandlers.append((sigNum, oldHandler))
                    signal.signal(sigNum, signalHandler)

                rc = fn(*args, **kwargs)
            except SignalException, exception:
                rekill = True
                # not clear what else we can return if we make it that
                # far!
                rc = exception
            except Exception, exception:
                # rc isn't set here because we're going to reraise this
                # exception after cleaning up the process's signal
                # handlers
                pass

            # if a signal is received here it could get raised instead
            # of the original signal. it's a shame we don't have proper
            # signal management functions which would help
            for (sigNum, handler) in oldHandlers:
                signal.signal(sigNum, handler)

            if rekill:
                exception.reraise()
            elif exception:
                raise

            return rc

        return call

    return decorator
