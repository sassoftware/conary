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
import os
import sys
import tempfile


from conary import changelog

class ChangeLogTest(testhelp.TestCase):
    def testBadEditorVariable(self):
        def _tempStream():
            fd, tempf = tempfile.mkstemp()
            os.unlink(tempf)
            return os.fdopen(fd, "w+")

        oldEditor = self._invalidChangeLogEditor()

        cl = changelog.ChangeLog('Test User', 'user@example.com', 'nomsg\n')

        commitMsg = "Line 1\nLine2\nLine3\n"

        newStdin = _tempStream()
        newStdin.write(commitMsg)
        newStdin.write('.')
        newStdin.seek(0)

        oldStderr = sys.stderr
        newStderr = _tempStream()

        # Redirect stdout too - raw_input will print the prompt string
        oldStdout = sys.stdout
        newStdout = _tempStream()

        oldStdin = sys.stdin

        ret = None
        try:
            sys.stdin = newStdin
            sys.stdout = newStdout
            sys.stderr = newStderr
            ret = cl.getMessageFromUser(prompt="ggg")
        finally:
            sys.stdin = oldStdin
            sys.stdout = oldStdout
            sys.stderr = oldStderr
            if oldEditor:
                os.environ['EDITOR'] = oldEditor

        self.assertTrue(ret)
        self.assertEqual(cl.message(), commitMsg)

        expMsg = ("Error executing %s. Please set the EDITOR\n"
            "environment variable to a valid editor, or enter log message,\n"
            "terminated with single '.' (or CTRL+D to cancel)\n" % '/foo/bar')

        newStderr.seek(0)
        self.assertEqual(newStderr.read(), expMsg)

        # Now fail it - EOF instead of single dot
        newStdin.truncate(0)
        newStdin.write("Line1\n")
        newStdin.seek(0)

        newStderr.truncate(0)
        newStdout.truncate(0)

        oldEditor = self._invalidChangeLogEditor()
        ret = None

        try:
            sys.stdin = newStdin
            sys.stdout = newStdout
            sys.stderr = newStderr
            ret = cl.getMessageFromUser(prompt="ggg")
        finally:
            sys.stdin = oldStdin
            sys.stderr = oldStderr
            sys.stdout = oldStdout
            if oldEditor:
                os.environ['EDITOR'] = oldEditor

        self.assertFalse(ret)


    def _invalidChangeLogEditor(self):
        oldEditor = os.environ.get('EDITOR', None)
        os.environ['EDITOR'] = '/foo/bar'
        return oldEditor
