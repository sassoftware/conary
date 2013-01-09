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
