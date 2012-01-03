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
        self.failUnlessEqual(sorted(ret), [
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
        self.failUnlessEqual(ret, expected)
