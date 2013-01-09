#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
