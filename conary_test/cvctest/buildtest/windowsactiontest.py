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


from conary_test import rephelp

from testrunner import testhelp
from conary_test import resources

import os

from conary.build import build
from conary.build import source


class WindowsActionTest(rephelp.RepositoryHelper):
    def setUp(self):
        rephelp.RepositoryHelper.setUp(self)
        self.archivePath = resources.get_archive()
        self.cfg.macros['targetos'] = 'windows'

    def tearDown(self):
        self.cfg.macros.pop('targetos')
        rephelp.RepositoryHelper.tearDown(self)

    test_recipe1 = """
class WindowsAppTest(CapsuleRecipe):
    name = 'WindowsAppTest'
    version = '1.2.3.4'
    clearBuildReqs()
    def setup(r):
        r.addSource('%(name)s-%(version)s.zip')
        r.BuildMSI('%(name)s-%(version)s.zip',
#            dest='%%ProgramFiles%%\%(name)s',
            dest=r'Program Files\%(name)s',
            manufacturer='rPath',
            description='Test Package',
            package='%(name)s:msi',
        )
"""

    test_recipe2 = """
class WindowsAppTest(CapsuleRecipe):
    name = 'WindowsAppTest'
    version = '1.2.3.6'
    clearBuildReqs()
    def setup(r):
        r.addSource('%(name)s-%(version)s.zip')
        r.BuildMSI('%(name)s-%(version)s.zip',
            manufacturer='rPath',
            description='Test Package',
            package='%(name)s:msi',

            applicationType='webApp',
            defaultDocument='Default.aspx',
            webSite='Default Web Site',
            alias='Omni',
            webSiteDir='Omni',
            applicationName='Omni',
        )
"""

    test_recipe3 = """
class WindowsAppTest(CapsuleRecipe):
    name = 'WindowsAppTest'
    version = '1.2.3.6'
    clearBuildReqs()
    def setup(r):
        r.addSource('%(name)s-%(version)s.zip')
        r.BuildMSI('%(name)s-%(version)s.zip',
            manufacturer='rPath',
            description='Test Package',
            package='%(name)s:msi',
            applicationType='app',
            dest='foo',
            msiArgs='/q /l*v /i',
        )
"""

    def testPolicy(self):
        def newDo(a, macros):
            msiPath = os.path.join(self.archivePath, 'Setup2.msi')
            a.recipe._addCapsule(msiPath, 'msi', a.package % macros)
            a.recipe.winHelper = source.WindowsHelper()
            a.recipe.winHelper.productName = 'WindowsAppTest'
            a.recipe.winHelper.platform = ''
            a.recipe.winHelper.version = '1.2.3.4'
            a.recipe.winHelper.productCode = 'FIXME'
            a.recipe.winHelper.upgradeCode = 'FIXME'

        self.mock(build.BuildMSI, 'do', newDo)

        built, d = self.buildRecipe(self.test_recipe1, 'WindowsAppTest',
            logBuild=True)

        msis = [ x for x in built if x[0].endswith(':msi') ]
        self.assertEqual(len(msis), 1)

        msi = msis[0]

        repos = self.openRepository()
        spec = repos.findTrove(self.cfg.buildLabel, msi)
        self.assertEqual(len(spec), 1)

        trv = repos.getTrove(*spec[0])

        self.assertEqual(trv.troveInfo.capsule.msi.name(), 'WindowsAppTest')
        self.assertEqual(trv.troveInfo.capsule.msi.platform(), '')
        self.assertEqual(trv.troveInfo.capsule.msi.version(), '1.2.3.4')
        self.assertEqual(trv.troveInfo.capsule.msi.productCode(), 'FIXME')
        self.assertEqual(trv.troveInfo.capsule.msi.upgradeCode(), 'FIXME')

    def testGetUpgradeCode(self):
        def mockBuildMSI(version, upCode=None):
            def do(a, macros):
                a.package = a.package % macros
                msiPath = os.path.join(self.archivePath, 'Setup2.msi')
                a.recipe._addCapsule(msiPath, 'msi', a.package % macros)
                a.recipe.winHelper = source.WindowsHelper()
                a.recipe.winHelper.productName = 'WindowsAppTest'
                a.recipe.winHelper.platform = ''
                a.recipe.winHelper.version = version
                a.recipe.winHelper.productCode = 'FIXME'
                upgradeCode = a._getUpgradeCode()
                if not upgradeCode:
                    upgradeCode = 'None'
                if upCode:
                    upgradeCode = upCode
                a.recipe.winHelper.upgradeCode = upgradeCode

            self.mock(build.BuildMSI, 'do', do)


        repos = self.openRepository()
        origDir = os.getcwd()

        self.resetWork()
        os.chdir(self.workDir)

        self.newpkg('WindowsAppTest')
        os.chdir('WindowsAppTest')
        self.writeFile('WindowsAppTest.recipe', self.test_recipe1)
        self.addfile('WindowsAppTest.recipe')
        self.commit()

        os.chdir(origDir)
        self.resetWork()
        os.chdir(self.workDir)

        mockBuildMSI('1.2.3.4', '12345')
        built = self.cookItem(repos, self.cfg, 'WindowsAppTest')

        self.checkout('WindowsAppTest')
        os.chdir('WindowsAppTest')
        self.writeFile('WindowsAppTest.recipe',
            self.test_recipe1.replace('1.2.3.4', '1.2.3.5'))
        self.commit()

        self.resetWork()
        os.chdir(origDir)

        mockBuildMSI('1.2.3.5')
        built = self.cookItem(repos, self.cfg, 'WindowsAppTest')[0]

        msis = [ x for x in built if x[0].endswith(':msi') ]
        self.assertEqual(len(msis), 1)

        msi = msis[0]

        repos = self.openRepository()
        spec = repos.findTrove(self.cfg.buildLabel, msi)
        self.assertEqual(len(spec), 1)

        trv = repos.getTrove(*spec[0])

        self.assertEqual(trv.troveInfo.capsule.msi.name(), 'WindowsAppTest')
        self.assertEqual(trv.troveInfo.capsule.msi.platform(), '')
        self.assertEqual(trv.troveInfo.capsule.msi.version(), '1.2.3.5')
        self.assertEqual(trv.troveInfo.capsule.msi.productCode(), 'FIXME')
        self.assertEqual(trv.troveInfo.capsule.msi.upgradeCode(), '12345')

    def testPersistingComponentInformation(self):
        def mockBuildMSI(version, components=None):
            def do(a, macros):
                a.package = a.package % macros
                msiPath = os.path.join(self.archivePath, 'Setup2.msi')
                a.recipe._addCapsule(msiPath, 'msi', a.package % macros)
                a.recipe.winHelper = source.WindowsHelper()
                a.recipe.winHelper.productName = 'WindowsAppTest'
                a.recipe.winHelper.platform = ''
                a.recipe.winHelper.version = version
                a.recipe.winHelper.productCode = 'foo'
                a.recipe.winHelper.upgradeCode = 'bar'
                if components:
                    a.recipe.winHelper.components = components
                for comp in a._getComponentInfo():
                    if comp not in a.recipe.winHelper.components:
                        a.recipe.winHelper.components.append(comp)

            self.mock(build.BuildMSI, 'do', do)

        repos = self.openRepository()
        origDir = os.getcwd()

        self.resetWork()
        os.chdir(self.workDir)

        self.newpkg('WindowsAppTest')
        os.chdir('WindowsAppTest')
        self.writeFile('WindowsAppTest.recipe', self.test_recipe1)
        self.addfile('WindowsAppTest.recipe')
        self.commit()

        os.chdir(origDir)
        self.resetWork()
        os.chdir(self.workDir)

        mockBuildMSI('1.2.3.4',
            components=[('uuid1', 'path1'), ('uuid2', 'path2')])

        built = self.cookItem(repos, self.cfg, 'WindowsAppTest')

        spec = repos.findTrove(self.cfg.buildLabel,
            ('WindowsAppTest:msi', None, None))[0]
        trv = repos.getTrove(*spec)

        components = [ (x.uuid(), x.path())
            for _, x in trv.troveInfo.capsule.msi.components.iterAll() ]

        self.assertTrue(('uuid1', 'path1') in components)
        self.assertTrue(('uuid2', 'path2') in components)

        self.checkout('WindowsAppTest')
        os.chdir('WindowsAppTest')
        self.writeFile('WindowsAppTest.recipe',
            self.test_recipe1.replace('1.2.3.4', '1.2.3.5'))
        self.commit()

        self.resetWork()
        os.chdir(origDir)

        mockBuildMSI('1.2.3.5', components=[('uuid3', 'path3'), ])
        built = self.cookItem(repos, self.cfg, 'WindowsAppTest')

        spec = repos.findTrove(self.cfg.buildLabel,
            ('WindowsAppTest:msi', None, None))[0]
        trv = repos.getTrove(*spec)

        components = [ (x.uuid(), x.path())
            for _, x in trv.troveInfo.capsule.msi.components.iterAll() ]

        self.assertTrue(('uuid1', 'path1') in components)
        self.assertTrue(('uuid2', 'path2') in components)
        self.assertTrue(('uuid3', 'path3') in components)

    def testMsiArgs(self):
        def mockBuildMSI(version, msiArgs=None):
            def do(a, macros):
                a.package = a.package % macros
                msiPath = os.path.join(self.archivePath, 'Setup2.msi')
                a.recipe._addCapsule(msiPath, 'msi', a.package % macros)
                a.recipe.winHelper = source.WindowsHelper()
                a.recipe.winHelper.productName = 'WindowsAppTest'
                a.recipe.winHelper.platform = ''
                a.recipe.winHelper.version = version
                a.recipe.winHelper.productCode = 'foo'
                a.recipe.winHelper.upgradeCode = 'bar'
                a.recipe.winHelper.msiArgs = msiArgs

            self.mock(build.BuildMSI, 'do', do)


        repos = self.openRepository()
        origDir = os.getcwd()

        self.resetWork()
        os.chdir(self.workDir)

        self.newpkg('WindowsAppTest')
        os.chdir('WindowsAppTest')
        self.writeFile('WindowsAppTest.recipe', self.test_recipe3)
        self.addfile('WindowsAppTest.recipe')
        self.commit()

        os.chdir(origDir)
        self.resetWork()
        os.chdir(self.workDir)

        mockBuildMSI('1.2.3.4', '/q /l*v /i')
        built, _ = self.cookItem(repos, self.cfg, 'WindowsAppTest')

        msis = [ x for x in built if x[0].endswith(':msi') ]
        self.assertEqual(len(msis), 1)

        msi = msis[0]

        repos = self.openRepository()
        spec = repos.findTrove(self.cfg.buildLabel, msi)
        self.assertEqual(len(spec), 1)

        trv = repos.getTrove(*spec[0])

        self.assertEqual(trv.troveInfo.capsule.msi.name(), 'WindowsAppTest')
        self.assertEqual(trv.troveInfo.capsule.msi.platform(), '')
        self.assertEqual(trv.troveInfo.capsule.msi.version(), '1.2.3.4')
        self.assertEqual(trv.troveInfo.capsule.msi.productCode(), 'foo')
        self.assertEqual(trv.troveInfo.capsule.msi.upgradeCode(), 'bar')
        self.assertEqual(trv.troveInfo.capsule.msi.msiArgs(), '/q /l*v /i')

    def testBasics(self):
        raise testhelp.SkipTestException('need to mock out the windows build service')
        built, d = self.buildRecipe(self.test_recipe1, 'WindowsAppTest', logBuild=True)

    def testWebApp(self):
        raise testhelp.SkipTestException('need to mock out the windows build service')
        built, d = self.buildRecipe(self.test_recipe2, 'WindowsAppTest', logBuild=True)
