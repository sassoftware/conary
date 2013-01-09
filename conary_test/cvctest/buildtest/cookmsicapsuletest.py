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
import re
import shutil
import types

from conary_test import rephelp
from conary_test import resources

from conary.repository import changeset

class CookTestWithMSICapsules(rephelp.RepositoryHelper):

    def testCookWithMSICapsule(self):
        recipestr = """
class TestCookWithMSICapsule(CapsuleRecipe):
    name = '%s'
    version = '%s'

    clearBuildReqs()

    def setup(r):
        r.addCapsule(%s)

"""
        pkgName = 'foo'
        version = '1.3.5.7abc'
        msiName = 'Setup2.msi'
        addCapsuleArgs = '\'%s\', msiArgs=\'/q /l*v /i\'' % msiName
        recipestr = recipestr % (pkgName, version, addCapsuleArgs)
        self.cfg.windowsBuildService = '172.16.175.244'

        class fakeWinHelper:
            name = 'foo'
            productName = 'A Super Wonderful MSI'
            version = '1.2.3.4'
            platform = 'ia64'
            productCode = 'pcode'
            upgradeCode = 'ucode'
            msiArgs = None
            components = []
            def __init__(self,*args):
                pass
            def extractMSIInfo(self, *args, **kwargs):
                pass

        from conary.build import source
        self.mock(source, 'WindowsHelper', fakeWinHelper)

        pkgNames, built, cs = self._cookPkgs(recipestr, msiName, pkgName)

        ti = [ tcs.getTroveInfo() for tcs in cs.iterNewTroveList()
               if tcs.name() == 'foo:msi'][0]
        self.assertEqual(ti.capsule.msi.name(),
                             fakeWinHelper.productName)
        self.assertEqual(ti.capsule.msi.version(),
                             fakeWinHelper.version)
        self.assertEqual(ti.capsule.msi.platform(),
                             fakeWinHelper.platform)
        self.assertEqual(ti.capsule.msi.productCode(),
                             fakeWinHelper.productCode)
        self.assertEqual(ti.capsule.msi.upgradeCode(),
                             fakeWinHelper.upgradeCode)
        self.assertEqual(ti.capsule.msi.msiArgs(),
                             '/q /l*v /i')

        self.assertEquals(pkgNames, [pkgName, pkgName +':msi'])

    def _cookAndInstall(self, recipestr, filename, pkgname,
                        builtpkgnames=None, output = ''):

        if builtpkgnames is None:
            builtpkgnames = [pkgname]

        r = self._cookPkgs(recipestr, filename, pkgname, builtpkgnames)
        self._installPkgs(builtpkgnames, output = '')
        return r

    def _cookPkgs(self, recipestr, filename, pkgname, builtpkgnames=None, macros={}, updatePackage=False):
        repos = self.openRepository()
        recipename = pkgname + '.recipe'
        ccsname = pkgname + '.ccs'

        if builtpkgnames is None:
            builtpkgnames = [pkgname]

        origDir = os.getcwd()
        try:
            os.chdir(self.workDir)
            if updatePackage:
                self.checkout(pkgname)
            else:
                self.newpkg(pkgname)
            os.chdir(pkgname)
            self.writeFile(recipename, recipestr)
            if not updatePackage:
                self.addfile(recipename)

            if isinstance(filename, types.StringType):
                filenames = [filename]
            else:
                filenames = filename

            for filename in filenames:
                shutil.copyfile(
                    resources.get_archive() + '/' + filename,
                    filename)
                self.addfile(filename) 

            self.commit()
            built, out = self.cookItem(repos, self.cfg, pkgname, macros=macros)

            self.changeset(repos, builtpkgnames, ccsname)
            cs = changeset.ChangeSetFromFile(ccsname)
        finally:
            os.chdir(origDir)

        return (sorted([x.getName() for x in cs.iterNewTroveList()]), built, cs)

    def _installPkgs(self, builtpkgnames, output = ''):
        rc, str = self.captureOutput(self.updatePkg, self.rootDir,
                                     builtpkgnames, depCheck=False)
        assert re.match(output, str), '%r != %r' %(output, str)
