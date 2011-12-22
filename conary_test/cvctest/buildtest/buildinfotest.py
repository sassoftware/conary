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


import tempfile
from testrunner import testhelp

from conary.build import buildinfo


class BuildInfoTest(testhelp.TestCase):
    def testBuildInfo(self):
        d = tempfile.mkdtemp()
        f = open(d + '/conary-build-info', 'w')
        f.write(
'''
macros.foo  hello world
bar            here it is
linenum            32
''')
        f.close()
        b = buildinfo.BuildInfo(d)
        b.read()
        assert(b.linenum == '32')
        assert(b.macros['foo'] == 'hello world')
        assert(b.bar == 'here it is')

    def testWriteBuildInfo(self):
        d = tempfile.mkdtemp()
        b = buildinfo.BuildInfo(d)
        b.begin()
        b.foo = 'bar\\nb\nam'
        b.bar = 1
        b.stop()
        b = buildinfo.BuildInfo(d)
        b.read()
        assert(b.foo == 'bar\\nb\nam')
