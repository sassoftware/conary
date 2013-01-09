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
import StringIO
import tempfile

from conary.lib import util
from conary.lib import debhelper
from conary_test import resources


class DebHelperTest(testhelp.TestCase):
    def setUp(self):
        testhelp.TestCase.setUp(self)
        self.workDir = tempfile.mkdtemp()

    def tearDown(self):
        testhelp.TestCase.tearDown(self)
        util.rmtree(self.workDir, ignore_errors = True)

    def testControlFileParser(self):
        parser = debhelper.ControlFileParser()
        lines = [
            'Version: 1.2-3',
            # Trailing space chars don't get trimmed
            'Package: foo ',
            'Section: "useless"',
            # But leading space chars get trimmed
            'Architecture:   i886',
            'Description: Line1',
            ' Line2',
            ' Line3',
            ' .',
            ' Line4',
        ]
        sio = StringIO.StringIO('\n'.join(lines))
        sio.seek(0)

        ret = parser.parse(sio)
        self.assertEqual(sorted(ret), [
            ('architecture', ['i886']),
            ('description', [ 'Line1', 'Line2', 'Line3', '', 'Line4' ]),
            ('package', ['foo']),
            # We should know how to remove quotes here, for now we just won't
            ('section', ['"useless"']),
            ('version', ['1.2-3']),
        ])

    def testDebHeader(self):
        fobj = file(os.path.join(resources.get_archive(), "bash.deb"))

        fields = [debhelper.NAME, debhelper.VERSION, debhelper.RELEASE,
                      debhelper.SUMMARY, debhelper.DESCRIPTION]

        expected = dict([
            (debhelper.NAME, 'bash'),
            (debhelper.VERSION, '3.1'),
            (debhelper.RELEASE, '2ubuntu10'),
            (debhelper.SUMMARY, 'The GNU Bourne Again SHell'),
            (debhelper.DESCRIPTION, '''\
Bash is an sh-compatible command language interpreter that executes
commands read from the standard input or from a file.  Bash also
incorporates useful features from the Korn and C shells (ksh and csh).

Bash is ultimately intended to be a conformant implementation of the
IEEE POSIX Shell and Tools specification (IEEE Working Group 1003.2).

Included in the bash package is the Programmable Completion Code, by
Ian Macdonald.'''),
        ])

        h = debhelper.DebianPackageHeader(fobj)
        ret = dict((k, h[k]) for k in fields)
        self.assertEqual(ret, expected)
