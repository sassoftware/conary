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


import os


from conary_test import rephelp

testRecipe = """\
class TestRecipe(PackageRecipe):
    name = 'testdirs'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.MakeDirs('/tmp', mode=01777)
"""

# XXX todo: ownership, removal tests

class DirectoryTest(rephelp.RepositoryHelper):
    def setUp(self):
        rephelp.RepositoryHelper.setUp(self)
        self.resetRepository()
        self.build(testRecipe, 'TestRecipe')
        self.tmpdir = os.sep.join((self.rootDir, 'tmp'))

    def testDir1(self):
        """verify that the directory is created with the correct mode"""
        self.resetRoot()
        self.updatePkg(self.rootDir, 'testdirs')
        sb = os.stat(self.tmpdir)
        assert(sb.st_mode == 041777)

    def testDir2(self):
        """
        verify that the directory ends up with the correct mode
        even if it already exists with the wrong mode
        """
        self.resetRoot()
        os.mkdir(self.tmpdir)
        os.chmod(self.tmpdir, 0755)
        self.updatePkg(self.rootDir, 'testdirs')
        sb = os.stat(self.tmpdir)
        assert(sb.st_mode == 041777)
