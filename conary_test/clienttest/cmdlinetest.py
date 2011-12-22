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


import sys

from StringIO import StringIO

#testsuite
from conary_test import rephelp

#conary
from conary.conaryclient import cmdline
"""
Tests for functions in the cmdline module
"""


class ClientCmdlineTest(rephelp.RepositoryHelper):
    def testAskYN(self):
        def _test(input, result, default=None):
            oldStdin, oldStdout = sys.stdin, sys.stdout
            try:
                outBuffer = StringIO()
                inBuffer = StringIO()

                sys.stdout = outBuffer
                sys.stdin = inBuffer
                inBuffer.write(input)
                inBuffer.seek(0)
                assert(cmdline.askYn('foo', default=default) == result)
            finally:
                sys.stdin, sys.stdout = oldStdin, oldStdout
        _test('Y\n', True)
        _test('YES\n', True)
        _test('\n', None) # FIXME: should this be Y?
        _test('\n', True, default=True)
        _test('\n', False, default=False)
        _test('n\n', False)
        _test('No\n', False)
        _test('', False) # this will raise an EOFError
        _test('', False, default=True)
        _test('Nein\nnyet\nyes', True)

    def testParseTroveSpec(self):
        tests = [
            (("foo=/bar@baz:1/2-3-4", False, False),
                ("foo", "/bar@baz:1/2-3-4", None)),
            (("   foo=/bar@baz:1/2-3-4\n\n\n", False, False),
                ("foo", "/bar@baz:1/2-3-4", None)),
        ]
        for (specStr, allowEmptyName, withFrozenFlavor), exp in tests:
            ret = cmdline.parseTroveSpec(specStr, allowEmptyName =
                allowEmptyName, withFrozenFlavor = withFrozenFlavor)
            self.failUnlessEqual(ret, exp)
