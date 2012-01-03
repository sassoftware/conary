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


class BuildPackageTest(rephelp.RepositoryHelper):
    def _inodeCheck(self, path1, path2, same=True):
        l1 = os.lstat('/'.join((self.workDir, path1)))
        l2 = os.lstat('/'.join((self.workDir, path2)))
        theSame = (l1.st_rdev, l1.st_ino) == (l2.st_rdev, l2.st_ino)
        return same == theSame

    def testHardlinkGroup(self):
        recipestr1 = """
class HardlinkGroup(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.Create('/asdf/blah')
        r.Link('bar', '/asdf/blah')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "HardlinkGroup")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        assert(self._inodeCheck('/asdf/blah', '/asdf/bar', same=True))
