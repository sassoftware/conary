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
import sys

from testrunner import testcase

from conary import callbacks, errors, trove
from conary.lib import log

class CallbacksTest(testhelp.TestCase):

    def testUncatchableException(self):
        # Tests that uncatchable exceptions are properly passed through

        class TestUncatchableError(errors.ConaryError):
            errorIsUncatchable = True

        class TestCatchableError(errors.ConaryError):
            errorIsUncatchable = False

        class C_(callbacks.ChangesetCallback):
            def fooBuriedErrorCallback1(self):
                raise Exception("This should not be raised")

            def fooBuriedErrorCallback2(self):
                raise TestCatchableError("This should not be raised")

            def fooRaisedErrorCallback1(self):
                raise TestUncatchableError("foo")

            def fooRaisedErrorCallback2(self):
                raise trove.DigitalSignatureVerificationError("real error")

            def fooRaisedErrorCallback3(self):
                sys.exit(124)

        oldDisabled = log.logger.disabled
        log.logger.disabled = 1

        cb = C_()
        cb.fooBuriedErrorCallback1()
        cb.fooBuriedErrorCallback2()

        self.assertRaises(TestUncatchableError, cb.fooRaisedErrorCallback1)
        self.assertRaises(trove.DigitalSignatureVerificationError, 
                              cb.fooRaisedErrorCallback2)
        self.assertRaises(SystemExit, cb.fooRaisedErrorCallback3)

        log.logger.disabled = oldDisabled

    def testDefaultErrorWarning(self):
        cb = callbacks.UpdateCallback()

        msg = "message 1"
        tb = "Traceback: foo\n    bar\n    baz\n"

        logFilter = testcase.LogFilter()

        logFilter.add()
        cb.warning(msg)
        logFilter.compare('warning: ' + msg)
        logFilter.clear()

        logFilter.add()
        cb.error(msg)
        logFilter.compare('error: ' + msg)
        logFilter.clear()

        logFilter.add()
        cb.error(msg)
        logFilter.compare('error: ' + msg)
        logFilter.clear()

        # Tracebacks
        logFilter.add()
        cb.error(msg, exc_text=tb)
        logFilter.compare('error: ' + msg + '\n' + tb)
        logFilter.clear()

        # Errors with formatting
        msg1 = "Some text with %s, %s and %s"
        args = ("one", "two", "three")

        logFilter.add()
        cb.error(msg1, exc_text=tb, *args)
        logFilter.compare('error: ' + (msg1 % args) + '\n' + tb)
        logFilter.clear()
