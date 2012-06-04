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


import sys
import os

import conary_test
from conary_test import rephelp
from conary.deps import deps
from conary import versions
from conary.build.policy import PolicyError
from conary.build.errors import CookError
from conary import rpmhelper
from conary_test import resources

pythonVer = "%s.%s" % sys.version_info[:2]


class RpmCapsuleTest(rephelp.RepositoryHelper):
    @conary_test.rpm
    def testRPMCapsuleEpoch(self):
        recipestr1 = r"""
class TestEpoch(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('epoch-1.0-1.i386.rpm')
"""
        built, d = self.buildRecipe(recipestr1, "TestEpoch")

        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]

        repos = self.openRepository()
        trv = repos.getTrove(*nvf)

        self.assertEquals(trv.troveInfo.capsule.rpm.epoch(), 17)

    @conary_test.rpm
    def testScriptHasLdSoConf(self):
        'test warning on capsule scripts containing "ld.so.conf" (CNP-185)'
        recipestr = """
class TestLdSoConf(CapsuleRecipe):
    name = 'scripts'
    version = '1.0_1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('scripts-1.0-1.x86_64.rpm')
"""
        # note that cookrpmcapsuletest:testCookWithScripts tests success case
        self.assertRaises(PolicyError, self.buildRecipe, recipestr,
            'TestLdSoConf')

    @conary_test.rpm
    def testRPMCapsuleDepPolicy(self):
        """ Make sure that RPMProvide and RPMProvide work"""
        recipestr1 = r"""
class TestEpoch(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('epoch-1.0-1.i386.rpm')
        r.RPMProvides('rpm: nonsenseProvision(FOO BAR)', 'epoch:rpm' )
        r.RPMRequires('rpm: nonsenseRequirement(BAZ QUX)', 'epoch' )
"""
        built, d = self.buildRecipe(recipestr1, "TestEpoch")

        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]

        repos = self.openRepository()
        trv = repos.getTrove(*nvf)

        self.assertEquals(str(trv.provides()),
            '\n'.join(('trove: epoch:rpm',
                       'rpm: epoch',
                       'rpm: epoch[x86-32]',
                       'rpm: nonsenseProvision(BAR FOO)')))

        self.assertEquals(str(trv.requires),
            '\n'.join(('rpm: nonsenseRequirement(BAZ QUX)',
                       'rpmlib: CompressedFileNames',
                       'rpmlib: PayloadFilesHavePrefix')))

    @conary_test.rpm
    def testRPMCapsuleDepPolicy2(self):
        """Make sure that we can't specify non rpm and rpmlib deps using
        RPMProvides"""
        recipestr1 = r"""
class TestEpoch(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('epoch-1.0-1.i386.rpm')
        r.RPMProvides('soname: nonsenseProvision(FOO BAR)', 'epoch' )
"""
        try:
            self.buildRecipe(recipestr1, "TestEpoch")
        except CookError, e:
            err = str(e).split('\n')[1]
            self.assertEqual(
                str(err),
                " PolicyError: RPMProvides cannot "
                "be used to provide the non-rpm dependency: 'soname: "
                "nonsenseProvision(FOO BAR)'")

    @conary_test.rpm
    def testRPMCapsuleDepPolicy3(self):
        """Make sure that we can't specify non rpm and rpmlib deps using
        RPMRequires"""
        recipestr1 = r"""
class TestEpoch(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('epoch-1.0-1.i386.rpm')
        r.RPMRequires('soname: nonsenseProvision(FOO BAR)', 'epoch' )
"""
        try:
            self.buildRecipe(recipestr1, "TestEpoch")
        except CookError, e:
            err = str(e).split('\n')[1]
            self.assertEqual(
                str(err),
                " PolicyError: RPMRequires cannot "
                "be used to provide the non-rpm dependency: 'soname: "
                "nonsenseProvision(FOO BAR)'")

    @conary_test.rpm
    def testRPMProvidesExceptions(self):
        """
        Make sure you can add exceptions for rpm dependencies. You need to be
        able to do this when an RPM that you are installing incorrectly provides
        something that the system provides to avoid installing all RPMs as one
        large job.

        It only makes sense to do exceptDeps for RPMProvides since rpm
        provisions aren't actually attached to files.
        """

        recipe1 = """
class TestRecipe(CapsuleRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('perl-Archive-Tar-1.46-68.fc11.x86_64.rpm')
"""

        recipe2 = recipe1 + "\n        r.RPMProvides(exceptDeps='rpm: perl.*')"

        def getPerlProvides(trv):
            return [ x for x in str(trv.provides()).split('\n')
                     if x.startswith('rpm: perl') ]

        r1trv = self.build(recipe1, 'TestRecipe')
        r1provides = getPerlProvides(r1trv)
        self.assertEqual(len(r1provides), 5)

        r2trv = self.build(recipe2, 'TestRecipe')
        r2provides = getPerlProvides(r2trv)
        self.assertEqual(len(r2provides), 0)

        self.assertTrue([ x for x in str(r2trv.provides()) ])


    @conary_test.rpm
    def testRPMCapsuleKernelModMerging(self):
        '''
        Make sure that RPMRequires passes through mergeKmodSymbols correctly
        '''
        def checkDeps(built, reqExpected, provExpected):
            nvf = built
            nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]
            repos = self.openRepository()
            trv = repos.getTrove(*nvf)
            reqGot = list(trv.requires().iterDepsByClass(deps.RpmDependencies))
            provGot = list(trv.provides().iterDepsByClass(deps.RpmDependencies))
            self.assertEquals(str(reqGot), reqExpected)
            self.assertEquals(str(provGot), provExpected)

        recipestr1 = r"""
class TestKernel(CapsuleRecipe):
    name = 'kernelish'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('kernelish-1.0-1.noarch.rpm')
        r.RPMRequires(mergeKmodSymbols=True)
"""
        built, d = self.buildRecipe(recipestr1, "TestKernel")
        req = "[Dependency('ksym', flags={'bar:123456789abcdef': 1, 'foo:123456789abcdef': 1})]"
        prov = "[Dependency('kernel', flags={'bar:123456789abcdef': 1, 'foo:123456789abcdef': 1}), Dependency('kernelish')]"
        checkDeps(built[0], req, prov)

        recipestr2 = r"""
class TestKernel(CapsuleRecipe):
    name = 'kernelish'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('kernelish-1.0-1.noarch.rpm')
        r.RPMRequires(mergeKmodSymbols=False)
"""
        built, d = self.buildRecipe(recipestr2, "TestKernel")
        req = "[Dependency('ksym[bar:123456789abcdef]'), Dependency('ksym[foo:123456789abcdef]')]"
        prov = "[Dependency('kernel[foo:123456789abcdef]'), Dependency('kernelish'), Dependency('kernel[bar:123456789abcdef]')]"
        checkDeps(built[0], req, prov)

    @conary_test.rpm
    def testRPMCapsuleDepCulling(self):
        """ Make sure that RPMRequires redundent rpm requires are culled"""
        recipestr1 = r"""
class TestDepCulling(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('gnome-main-menu-0.9.10-26.x86_64.rpm')
"""
        self.overrideBuildFlavor('is: x86_64')
        built, d = self.buildRecipe(recipestr1, "TestDepCulling")

        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]

        repos = self.openRepository()
        trv = repos.getTrove(*nvf)
        reqGot = list(trv.requires().iterDepsByClass(deps.RpmDependencies))
        reqExpected = "[Dependency('hal'), Dependency('gnome-main-menu-lang'), Dependency('gnome-panel'), Dependency('tango-icon-theme'), Dependency('coreutils'), Dependency('dbus-1-glib'), Dependency('libssui'), Dependency('eel'), Dependency('wireless-tools')]"
        self.assertEquals(str(reqGot), reqExpected)

    @conary_test.rpm
    def testRPMRequiresExceptions(self):
        """ Make sure that RPMRequires's exceptions argument works"""
        recipestr1 = r"""
class TestRPMRequiresExceptions(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('gnome-main-menu-0.9.10-26.x86_64.rpm')
        r.RPMRequires(exceptions='gnome-main-menu.*rpm')
"""
        self.overrideBuildFlavor('is: x86_64')
        built, d = self.buildRecipe(recipestr1, "TestRPMRequiresExceptions")

        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]

        repos = self.openRepository()
        trv = repos.getTrove(*nvf)
        reqGot = list(trv.requires().iterDepsByClass(deps.RpmDependencies))
        reqExpected = "[]"
        self.assertEquals(str(reqGot), reqExpected)

    @conary_test.rpm
    def testRPMRequiresExceptDeps1(self):
        """ Make sure that RPMRequires's exceptDeps argument works"""
        recipestr1 = r"""
class TestRPMRequiresExceptDeps(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('gnome-main-menu-0.9.10-26.x86_64.rpm')
        r.RPMRequires(exceptDeps='rpmlib: .*')
"""
        self.overrideBuildFlavor('is: x86_64')
        built, d = self.buildRecipe(recipestr1, "TestRPMRequiresExceptDeps")

        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]

        repos = self.openRepository()
        trv = repos.getTrove(*nvf)
        reqGot = list(trv.requires().iterDepsByClass(deps.RpmLibDependencies))
        reqExpected = "[]"
        self.assertEquals(str(reqGot), reqExpected)

    @conary_test.rpm
    def testRPMRequiresExceptDeps2(self):
        """ Make sure that RPMRequires's exceptDeps argument works"""
        recipestr1 = r"""
class TestRPMRequiresExceptDeps(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('gnome-main-menu-0.9.10-26.x86_64.rpm')
        r.RPMRequires(exceptDeps=('gnome-main-menu.*','rpmlib: .*'))
"""
        self.overrideBuildFlavor('is: x86_64')
        built, d = self.buildRecipe(recipestr1, "TestRPMRequiresExceptDeps")

        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]

        repos = self.openRepository()
        trv = repos.getTrove(*nvf)
        reqGot = list(trv.requires().iterDepsByClass(deps.RpmLibDependencies))
        reqExpected = "[]"
        self.assertEquals(str(reqGot), reqExpected)

    @conary_test.rpm
    def testRPMRequiresExceptDeps3(self):
        """ Make sure that RPMRequires's exceptDeps argument works"""
        recipestr1 = r"""
class TestRPMRequiresExceptDeps(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('gnome-main-menu-0.9.10-26.x86_64.rpm')
        r.RPMRequires(exceptDeps=(('gnome-main-menu.*','rpm: .*'),) )
"""
        self.overrideBuildFlavor('is: x86_64')
        built, d = self.buildRecipe(recipestr1, "TestRPMRequiresExceptDeps")

        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]

        repos = self.openRepository()
        trv = repos.getTrove(*nvf)
        reqGot = list(trv.requires().iterDepsByClass(deps.RpmDependencies))
        reqExpected = "[]"
        self.assertEquals(str(reqGot), reqExpected)

    @conary_test.rpm
    def testRPMCapsuleUserGroup(self):
        recipestr1 = r"""
class TestGroup(CapsuleRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('ownerships-1.0-1.i386.rpm')
"""
        built, d = self.buildRecipe(recipestr1, "TestGroup")

        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]

        repos = self.openRepository()
        trv = repos.getTrove(*nvf)

        self.assertEquals(trv.requires(), deps.ThawDependencySet(
                '17#CompressedFileNames|17#PayloadFilesHavePrefix|'
                '17#PayloadIsBzip2'))

    @conary_test.rpm
    def testRPMCapsuleGhost(self):
        recipestr1 = r"""
class TestGhost(CapsuleRecipe):
    name = 'ghost'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('ghost-1.0-1.i386.rpm')
        # ensure that initialContents overrides transient
        r.Transient('/foo/ghost')
"""
        built, d = self.buildRecipe(recipestr1, "TestGhost")
        client = self.getConaryClient()
        repos = client.getRepos()
        nvf = repos.findTrove(None, built[0])
        trv = repos.getTrove(*nvf[0])
        fileList = list(trv.iterFileList())
        fileObjs = repos.getFileVersions([(x[0], x[2], x[3]) for x in fileList
                                          if x[1] == '/foo/ghost'])
        for fileInfo, fileObj in zip(fileList, fileObjs):
            self.assertFalse(fileObj.flags.isConfig(),
                        "Expected config to be unset for %s" % fileInfo[1])
            self.assertFalse(fileObj.flags.isTransient(),
                        "Expected transient to be unset for %s" % fileInfo[1])
            self.assertTrue(fileObj.flags.isInitialContents(),
                            "Expected initialContents for %s" % fileInfo[1])

    @conary_test.rpm
    def testRPMCapsuleDeps(self):
        'make sure that rpm capsule deps are correct'
        recipestr1 = r"""
class TestProvides(CapsuleRecipe):
    name = 'depstest'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('depstest-0.1-1.x86_64.rpm')
"""
        built, d = self.buildRecipe(recipestr1, "TestProvides")
        client = self.getConaryClient()
        repos = client.getRepos()
        nvf = repos.findTrove(None, built[0])
        trv = repos.getTrove(*nvf[0])

        reqExpected = '\n'.join((
            'abi: ELF32(SysV x86)',
            'file: /bin/sh',
            'soname: ELF32/ld-linux.so.2(GLIBC_PRIVATE SysV x86)',
            'soname: ELF32/libc.so.6(GLIBC_2.0 GLIBC_2.1.3 SysV x86)',
            'rpmlib: CompressedFileNames',
            'rpmlib: PayloadFilesHavePrefix'))
        provExpected = '\n'.join((
            'file: /bin/fakebin',
            'trove: depstest:rpm',
            'soname: ELF32/libm.so.6(GLIBC_2.0 GLIBC_2.1 GLIBC_2.2 '
            'GLIBC_2.4 SysV x86)',
            'rpm: depstest',
            'rpm: depstest[x86-64]',
            'rpm: libm.so.6(GLIBC_2.0 GLIBC_2.1 GLIBC_2.2 GLIBC_2.4)'))
        self.assertEqual(str(trv.provides()), provExpected)
        self.assertEqual(str(trv.requires()), reqExpected)

    @conary_test.rpm
    def testRPMCapsuleSharedDeps(self):
        'make sure that rpm capsule deps on shared files are correct'
        recipestr1 = r"""
class TestSharedDep(CapsuleRecipe):
    name = 'sharedfiledep'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        # test dep provides
        r.addCapsule('sharedfiledep-1.0-1.i386.rpm',
                     package='sharedfiledep:rpm')
        r.addCapsule('sharedfiledep-secondary-1.0-1.i386.rpm',
                     package='sharedfiledep-secondary:rpm')
        # test dep requires
        r.Requires('foo:bar', '/usr/lib/libfoo.so.0.0')
"""
        built, d = self.buildRecipe(recipestr1, "TestSharedDep")
        client = self.getConaryClient()
        repos = client.getRepos()
        reqExpected = '\n'.join((
            'abi: ELF32(SysV x86)',
            'file: /bin/sh',
            'trove: foo:bar',
            'soname: ELF32/libc.so.6(GLIBC_2.1.3 SysV x86)',
            'rpm: rtld[GNU_HASH]',
            'rpmlib: CompressedFileNames',
            'rpmlib: PayloadFilesHavePrefix',
        ))
        provExpectedTemplate = '\n'.join((
                'file: /usr/bin/script',
                'trove: TROVENAME:rpm',
                'soname: ELF32/libfoo.so.0(SysV x86)',
                'soname: ELF32/libfoo.so.0.0(SysV x86)',
                'rpm: libfoo.so.0',
                'rpm: TROVENAME',
                'rpm: TROVENAME[x86-32]',
        ))

        for b in built:
            nvf = repos.findTrove(None, b)
            trv = repos.getTrove(*nvf[0])
            provExpected = provExpectedTemplate.replace('TROVENAME',
                                                        b[0].split(':')[0])

            self.assertEqual(str(trv.provides()), provExpected)
            self.assertEqual(str(trv.requires()), reqExpected)

    @conary_test.rpm
    def testNoCapsuleTags(self):
        'Ensure that capule files are not tagged'
        recipestr1 = r"""
class TestTags(CapsuleRecipe):
    name = 'tagfile'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('tagfile-1.0-1.i386.rpm')
        r.TagSpec('info-file', '/usr/share/info/')
"""
        built, d = self.buildRecipe(recipestr1, "TestTags")
        client = self.getConaryClient()
        repos = client.getRepos()
        nvf = repos.findTrove(None, built[0])
        trv = repos.getTrove(*nvf[0])
        fileList = list(trv.iterFileList())
        fileObjs = repos.getFileVersions([(x[0], x[2], x[3])
                                          for x in fileList])
        fileObj = fileObjs[0]
        self.assertEquals(fileObj.tags, [])

    @conary_test.rpm
    def testRPMCapsuleKernelDeps(self):
        '''make sure that rpm kernel deps include symbol set hashes'''
        fPath = os.path.join(resources.get_archive(),
                             'tmpwatch-2.9.1-1.i386.rpm')

        def mockGet(self, item, default):
            if item == rpmhelper.PROVIDENAME or item == rpmhelper.REQUIRENAME:
                return ['kernel(goodVer)', 'ksym(goodVer2)', 'kernel(badver)',
                        'ksym(badver2)']
            if item == rpmhelper.PROVIDEVERSION or \
                    item == rpmhelper.REQUIREVERSION:
                return ['0123456789ABCDEF',
                        'FEDCBA9876543210',
                        '3.2.1',
                        'ABCDE']

        self.mock(rpmhelper._RpmHeader, 'get', mockGet)
        h = rpmhelper.readHeader(file(fPath))
        (req, prov), output = self.captureOutput(h.getDeps)
        expected = '\n'.join((
        "rpm: kernel[badver]",
        "rpm: kernel[goodVer:0123456789ABCDEF]",
        "rpm: ksym[badver2]",
        "rpm: ksym[goodVer2:FEDCBA9876543210]",
        ))
        self.assertEqual(str(req), expected)
        self.assertEqual(str(prov), expected)
        # assert that the two bad versions were each seen twice; once
        # in provides and once in requires
        self.assertEqual(len(output.strip().split('\n')), 4)

        (req, prov), output = self.captureOutput(h.getDeps,
                                                 mergeKmodSymbols=True)
        expected = '\n'.join((
        "rpm: kernel(badver goodVer:0123456789ABCDEF)",
        "rpm: ksym(badver2 goodVer2:FEDCBA9876543210)",
        ))
        self.assertEqual(str(req), expected)
        self.assertEqual(str(prov), expected)
        self.assertEqual(len(output.strip().split('\n')), 4)

    @conary_test.rpm
    def testRemoveCapsuleFiles(self):
        """
        Ensure that the remove capsule files policy actually works. Make
        sure removed files don't leave behind file provides.
        """

        recipe1 = """\
class TestRecipe(CapsuleRecipe):
    name = 'test'
    version = '0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('shareddirs-1.0-1.x86_64.rpm', dir='/',
            package='%(name)s:rpm')
"""


        recipe2 = (recipe1 + "\n        "
            "r.RemoveCapsuleFiles('%(name)s:rpm', '^/usr/bin/script$')")

        trv = self.build(recipe1, 'TestRecipe')
        files1 = [ x[1] for x in trv.iterFileList() ]
        self.assertTrue('/usr/bin/script' in files1)
        assert(trv.provides.getDepClasses()[deps.DEP_CLASS_FILES].hasDep('/usr/bin/script'))

        trv2 = self.build(recipe2, 'TestRecipe')
        files2 = [ x[1] for x in trv2.iterFileList() ]
        self.assertTrue('/usr/bin/script' not in files2)
        assert(deps.DEP_CLASS_FILES not in trv2.provides.getDepClasses())

        recipe3 = (recipe1 + "\n        "
            "r.RemoveCapsuleFiles('%(name)s:foo', '^/usr/bin/script$')")
        e = self.assertRaises(PolicyError, self.build, recipe3, 'TestRecipe')
        self.assertEquals(str(e),
                  'Package Policy errors found:\n'
                  'RemoveCapsuleFiles: Component test:foo does not exist')

    @conary_test.rpm
    def testRPMCapsulePackageNoComponent(self):
        '''
        CNY-3743: make sure just the package name can be specified to
        addCapsule's package arg
        '''
        recipestr1 = r"""
class TestSharedDep(CapsuleRecipe):
    name = 'sharedfiledep'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        # test dep provides
        r.addCapsule('sharedfiledep-1.0-1.i386.rpm',
                     package='sharedfiledep')
"""
        built, d = self.buildRecipe(recipestr1, "TestSharedDep")
