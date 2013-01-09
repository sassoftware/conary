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
            self.assertEqual(ret, exp)
