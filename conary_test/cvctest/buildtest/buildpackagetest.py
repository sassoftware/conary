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
