#!/usr/bin/python
#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


from testrunner import testhelp

import os, signal

from conary.lib import sigprotect

class CallFlag:

    def __call__(self):
        self.called = True

    def wasCalled(self, reset = True):
        rc = self.called
        if reset:
            self.called = False
        return rc

    def __init__(self):
        self.called = False

class SignalsTest(testhelp.TestCase):

    def testDecorator(self):
        def killself(sig):
            os.kill(os.getpid(), sig)

        def createHandler(c):

            def handler(*args):
                c()

            return handler

        def _test(sigNum, c, reraise = False):
            try:
                killself(sigNum)
            except sigprotect.SignalException, e:
                assert(e.sigNum == sigNum)
                c()
                if reraise:
                    raise

        @sigprotect.sigprotect(signal.SIGTERM, signal.SIGUSR2)
        def catchTermUsr2(*args, **kwargs):
            return _test(*args, **kwargs)

        @sigprotect.sigprotect()
        def catchAll(*args, **kwargs):
            return _test(*args, **kwargs)

        sigCalled = CallFlag()
        handlerCalled = CallFlag()

        catchTermUsr2(signal.SIGUSR2, sigCalled)
        assert(sigCalled.wasCalled())

        signal.signal(signal.SIGTERM, createHandler(handlerCalled))
        catchTermUsr2(signal.SIGTERM, sigCalled, reraise = True)
        assert(handlerCalled.wasCalled())
        assert(sigCalled.wasCalled())
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        for sigNum in sigprotect.catchableSignals:
            catchAll(sigNum, sigCalled)
            assert(sigCalled.wasCalled())

    def testCatchableList(self):
        l = []
        for name, sigNum in sorted(signal.__dict__.items()):
            if not name.startswith('SIG'): continue
            if name.startswith('SIG_'): continue

            # catching SIGCHLD is evil; we don't want death of a child turning
            # into exceptions
            if name in [ 'SIGBUS', 'SIGSEGV', 'SIGCHLD', 'SIGCLD',
                         'SIGCONT' ]:
                catchable = False
            else:
                try:
                    signal.signal(signal.__dict__[name], signal.SIG_IGN)
                except RuntimeError:
                    catchable = False
                else:
                    catchable = True
                    signal.signal(signal.__dict__[name], signal.SIG_DFL)

            if catchable and sigNum not in sigprotect.catchableSignals:
                l.append(name)
                raise AssertionError('signal %s is catchable but is not in '
                            'catchable list' % name)
            elif not catchable and sigNum in sigprotect.catchableSignals:
                raise AssertionError('signal %s is not catchable but is in '
                            'catchable list' % name)

    def testSignalException(self):
        e = sigprotect.SignalException(signal.SIGHUP)
        assert(str(e) == 'SignalException: signal SIGHUP received')

        e = sigprotect.SignalException(signal.NSIG)
        assert(str(e) == 'SignalException: signal %d received' % signal.NSIG)

    def testThreading(self):
        @sigprotect.sigprotect()
        def runme():
            pass

        import threading
        thread = threading.Thread(target = runme)

        thread.start()
        thread.join()
