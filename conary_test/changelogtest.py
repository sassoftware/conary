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

        self.failUnless(ret)
        self.failUnlessEqual(cl.message(), commitMsg)

        expMsg = ("Error executing %s. Please set the EDITOR\n"
            "environment variable to a valid editor, or enter log message,\n"
            "terminated with single '.' (or CTRL+D to cancel)\n" % '/foo/bar')

        newStderr.seek(0)
        self.failUnlessEqual(newStderr.read(), expMsg)

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

        self.failIf(ret)


    def _invalidChangeLogEditor(self):
        oldEditor = os.environ.get('EDITOR', None)
        os.environ['EDITOR'] = '/foo/bar'
        return oldEditor
