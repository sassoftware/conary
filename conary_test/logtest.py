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

from conary.lib import log

class LogTest(testhelp.TestCase):

    def testLog(self):
        log.resetErrorOccurred()
        self.logCheck(log.warning, ("a warning",), "warning: a warning")
        self.logCheck(log.debug, ("a debug",), [])
        assert(not log.errorOccurred())
        log.setVerbosity(2)
        self.logCheck(log.warning, ("a warning",), "warning: a warning")
        self.logCheck(log.debug, ("a debug",), "+ a debug")
        log.setVerbosity(0)
        self.logCheck(log.warning, ("a warning",), "warning: a warning")
        self.logCheck(log.debug, ("a debug",), [])
        assert(not log.errorOccurred())
        self.logCheck(log.error, ("an error",), "error: an error")
        assert(log.errorOccurred())

    def testLogMinVerbosity(self):
        log.resetErrorOccurred()
        log.setVerbosity(log.ERROR)
        self.logCheck(log.warning, ("a warning",), [])
        assert(not log.setMinVerbosity(log.ERROR))
        self.logCheck(log.warning, ("a warning",), [])
        assert(log.setMinVerbosity(log.WARNING) == log.ERROR)
        self.logCheck(log.warning, ("a warning",), ["warning: a warning"])
        assert(not log.setMinVerbosity(log.ERROR))
        self.logCheck(log.warning, ("a warning",), ["warning: a warning"])

    def testLogWithObject(self):
        log.resetErrorOccurred()
        log.setVerbosity(2)
        foo = object()
        fooString = str(foo)
        self.logCheck(log.error,   (foo,), "error: %s" % fooString)
        assert(log.errorOccurred())
        self.logCheck(log.warning, (foo,), "warning: %s" % fooString)
        self.logCheck(log.debug,   (foo,), "+ %s" % fooString)
        self.logCheck(log.info,    (foo,), "+ %s" % fooString)

    def testLogWithPercentChar(self):
        log.resetErrorOccurred()
        log.setVerbosity(2)
        fooString = "Some message with a %s char in it"
        self.logCheck(log.error,   (fooString,), "error: %s" % fooString)
        assert(log.errorOccurred())
        self.logCheck(log.warning, (fooString,), "warning: %s" % fooString)
        self.logCheck(log.debug,   (fooString,), "+ %s" % fooString)
        self.logCheck(log.info,    (fooString,), "+ %s" % fooString)

        fooString = "This %s work"
        arg1 = "does"
        efooString = fooString % arg1

        self.logCheck(log.error,   (fooString, arg1), "error: %s" % efooString)
        assert(log.errorOccurred())
        self.logCheck(log.warning, (fooString, arg1), "warning: %s" % efooString)
        self.logCheck(log.debug,   (fooString, arg1), "+ %s" % efooString)
        self.logCheck(log.info,    (fooString, arg1), "+ %s" % efooString)

    def testSysLogCommand(self):
        # Create a temporary file we use for syslog
        fd, tempf = tempfile.mkstemp()
        os.close(fd)
        # Mock sys.argv to have some weird chars in it
        oldArgv = sys.argv
        try:
            sys.argv = ['dummy', 'cmd with %s in it', 'arg with %d in it']

            syslog = log.SysLog('/', tempf)
            syslog.command()
            syslog.close()

            line = open(tempf).readline().strip()
            # Strip timestamp
            line = line[line.find(']') + 2:]

            expected = ' '.join(["version %s:" % log.constants.version,
                sys.argv[1], sys.argv[2]])
            self.assertEqual(line, expected)
        finally:
            sys.argv = oldArgv
            os.unlink(tempf)

        # Make sure syslog has closed the file
        self.assertEqual(syslog.f, None)
