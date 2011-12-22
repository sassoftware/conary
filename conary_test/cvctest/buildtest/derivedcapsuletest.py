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
import conary_test
from conary_test import rephelp

from conary import files, versions
from conary.build import derive

class DerivedPackageTest(rephelp.RepositoryHelper):

    shadowLabel = versions.Label('localhost@rpl:shadowLabel')
    shadowLabel2 = versions.Label('localhost@rpl:shadowLabel2')

    @conary_test.rpm
    def testCapsuleDerivationBasic(self):
        pkgname = 'tmpwatch'
        # create a capsule package
        recipe = """
class TestMultiPackage(CapsuleRecipe):
    name = 'tmpwatch'
    version = '0.1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('tmpwatch-2.9.1-1.i386.rpm')
"""
        repos = self.openRepository()

        os.chdir(self.workDir)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname=pkgname))
        self.addfile(pkgname + '.recipe')
        self.discardOutput(self.commit)
        built = self.cookFromRepository(pkgname)
        self.failUnlessEqual(str(built),
            "(('tmpwatch:rpm', '/localhost@rpl:linux/0.1-1-1', "
            "Flavor('is: x86')),)")
        nvf = repos.findTrove(None, built[0])
        trv = repos.getTrove(*nvf[0])
        fileMap = dict([(x[1], (x[0], x[2], x[3])) for x in
                        trv.iterFileList()])
        fileObjMap = dict(zip(fileMap.keys(),
                           repos.getFileVersions(fileMap.values())))

        # create a derived package
        derive.derive(repos, self.cfg, self.shadowLabel, pkgname)
        os.chdir(self.workDir)
        self.checkout(pkgname, self.shadowLabel.asString(),
                      dir=pkgname + '-derived')
        os.chdir(pkgname + '-derived')
        derivedRecipe = """
class TestMultiPackage(DerivedCapsuleRecipe):
    name = "tmpwatch"
    version = '0.1'

    clearBuildReqs()

    def setup(r):
        # add a new file
        r.Create("/usr/foo", package=':rpm', contents="foo_derived1")
        # change a an existing one
        r.Create("/etc/cron.daily/tmpwatch", contents='tmpwatch_cron_derived1')
"""
        self.writeFile(pkgname + '.recipe', derivedRecipe)
        self.discardOutput(self.commit)
        dbuilt = self.cookFromRepository(pkgname, buildLabel=self.shadowLabel)
        self.assertTrue(len(dbuilt) == 1)
        self.assertTrue(dbuilt[0][0] == 'tmpwatch:rpm')
        dnvf = repos.findTrove(None, dbuilt[0])
        dtrv = repos.getTrove(*dnvf[0])
        dfileMap = dict([(x[1], (x[0], x[2], x[3])) for x in
                         dtrv.iterFileList()])
        dfileObjMap = dict(zip(dfileMap.keys(),
                           repos.getFileVersions(dfileMap.values())))

        # compare the troves
        self.assertEqual(trv.requires(), dtrv.requires())
        self.assertEqual(trv.provides(), dtrv.provides())

        fileAddition = 0
        fileOverride = 0
        for k in dfileMap:
            df = dfileMap[k]
            if k in fileMap:
                if k == '/etc/cron.daily/tmpwatch':
                    self.assertEqual(dfileMap[k][0], fileMap[k][0])
                    self.assertNotEqual(dfileMap[k][1], fileMap[k][1])
                    fileOverride += 1
                else:
                    dfileObj = dfileObjMap[k]
                    fileObj = fileObjMap[k]
                    assert dfileObj.inode == fileObj.inode
                    assert dfileObj.flags() & \
                        ~(files._FILE_FLAG_CAPSULE_ADDITION |
                          files._FILE_FLAG_CAPSULE_OVERRIDE) == fileObj.flags()
                    assert dfileObj.requires() == fileObj.requires()
                    assert dfileObj.provides() == fileObj.provides()

            elif k == '/usr/foo':
                assert k not in fileMap
                fileAddition += 1

        self.assertEqual(fileAddition, 1)
        self.assertEqual(fileOverride, 1)

        # derive from the derived package
        derive.derive(repos, self.cfg, self.shadowLabel2, pkgname)
        os.chdir(self.workDir)
        self.checkout(pkgname, self.shadowLabel2.asString(),
                      dir=pkgname + '-derived2')
        os.chdir(pkgname + '-derived2')
        derivedRecipe2 = """
class TestMultiPackage(DerivedCapsuleRecipe):
    name = "tmpwatch"
    version = '0.1'

    clearBuildReqs()

    def setup(r):
        # change the contents of two existing files
        r.Create("/usr/foo", package=':rpm', contents="foo_derived2")
        r.Create("/etc/cron.daily/tmpwatch", contents='tmpwatch_cron_derived2')
"""
        self.writeFile(pkgname + '.recipe', derivedRecipe2)
        self.discardOutput(self.commit)
        d2built = self.cookFromRepository(pkgname,
                                          buildLabel=self.shadowLabel2)
        self.assertTrue(len(d2built) == 1)
        self.assertTrue(d2built[0][0] == 'tmpwatch:rpm')
        d2nvf = repos.findTrove(None, d2built[0])
        d2trv = repos.getTrove(*d2nvf[0])
        d2fileMap = dict([(x[1], (x[0], x[2], x[3])) for x
                          in d2trv.iterFileList()])
        d2fileObjMap = dict(zip(d2fileMap.keys(),
                           repos.getFileVersions(d2fileMap.values())))

        # compare the troves
        self.assertEqual(dtrv.requires(), d2trv.requires())
        self.assertEqual(dtrv.provides(), d2trv.provides())

        fileOverride = 0
        for k in d2fileMap:
            df = dfileMap[k]
            d2f = d2fileMap[k]
            if d2f[1] != df[1]:
                #self.assertEqual(d2fileMap[k][0],dfileMap[k][0])
                fileOverride += 1

            d2fileObj = d2fileObjMap[k]
            dfileObj = dfileObjMap[k]
            assert d2fileObj.inode == dfileObj.inode
            assert d2fileObj.flags() & \
                ~(files._FILE_FLAG_CAPSULE_ADDITION |
                  files._FILE_FLAG_CAPSULE_OVERRIDE) == \
                  dfileObj.flags() & \
                  ~(files._FILE_FLAG_CAPSULE_ADDITION |
                    files._FILE_FLAG_CAPSULE_OVERRIDE)
            assert d2fileObj.requires() == dfileObj.requires()
            assert d2fileObj.provides() == dfileObj.provides()

        self.assertEqual(fileOverride, 2)

        self.updatePkg(pkgname,
                       depCheck=False)

        self.updatePkg(pkgname + "=" + self.shadowLabel.asString(),
                       depCheck=False)
        foo = open(self.rootDir + '/usr/foo').read().strip()
        self.assertEqual(foo, 'foo_derived1')
        conf = open(self.rootDir + '/etc/cron.daily/tmpwatch').read().strip()
        self.assertEqual(conf, 'tmpwatch_cron_derived1')

        self.updatePkg(pkgname + "=" + self.shadowLabel2.asString(),
                       depCheck=False)
        foo = open(self.rootDir + '/usr/foo').read().strip()
        self.assertEqual(foo, 'foo_derived2')
        conf = open(self.rootDir + '/etc/cron.daily/tmpwatch').read().strip()
        self.assertEqual(conf, 'tmpwatch_cron_derived2')

    @conary_test.rpm
    def testCapsuleDerivationGhost(self):
        pkgname = 'ghost'
        # create a capsule package
        recipe = """
class TestMultiPackage(CapsuleRecipe):
    name = 'ghost'
    version = '0.1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('ghost-1.0-1.i386.rpm')
"""
        repos = self.openRepository()

        os.chdir(self.workDir)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname=pkgname))
        self.addfile(pkgname + '.recipe')
        self.discardOutput(self.commit)
        built = self.cookFromRepository(pkgname)
        self.failUnlessEqual(str(built),
            "(('ghost:rpm', '/localhost@rpl:linux/0.1-1-1', "
            "Flavor('')),)")
        nvf = repos.findTrove(None, built[0])
        trv = repos.getTrove(*nvf[0])
        fileMap = dict([(x[1], (x[0], x[2], x[3])) for x in
                        trv.iterFileList()])
        fileObjMap = dict(zip(fileMap.keys(),
                           repos.getFileVersions(fileMap.values())))

        # create a derived package
        derive.derive(repos, self.cfg, self.shadowLabel, pkgname)
        os.chdir(self.workDir)
        self.checkout(pkgname, self.shadowLabel.asString(),
                      dir=pkgname + '-derived')
        os.chdir(pkgname + '-derived')
        derivedRecipe = """
class TestMultiPackage(DerivedCapsuleRecipe):
    name = "ghost"
    version = '0.1'

    clearBuildReqs()

    def setup(r):
        # add a new file
        r.Create("/usr/foo", package=':rpm', contents="foo_derived1")
"""
        self.writeFile(pkgname + '.recipe', derivedRecipe)
        self.discardOutput(self.commit)
        dbuilt = self.cookFromRepository(pkgname, buildLabel=self.shadowLabel)
        self.assertTrue(len(dbuilt) == 1)
        self.assertTrue(dbuilt[0][0] == 'ghost:rpm')
        dnvf = repos.findTrove(None, dbuilt[0])
        dtrv = repos.getTrove(*dnvf[0])
        dfileMap = dict([(x[1], (x[0], x[2], x[3])) for x in
                         dtrv.iterFileList()])
        dfileObjMap = dict(zip(dfileMap.keys(),
                           repos.getFileVersions(dfileMap.values())))

        # compare the troves
        self.assertEqual(trv.requires(), dtrv.requires())
        self.assertEqual(trv.provides(), dtrv.provides())

        fileAddition = 0
        fileOverride = 0
        for k in dfileMap:
            if k in fileMap:
                dfileObj = dfileObjMap[k]
                fileObj = fileObjMap[k]
                assert dfileObj.inode == fileObj.inode
                assert dfileObj.flags() & \
                    ~(files._FILE_FLAG_CAPSULE_ADDITION |
                      files._FILE_FLAG_CAPSULE_OVERRIDE) == fileObj.flags()
                assert dfileObj.requires() == fileObj.requires()
                assert dfileObj.provides() == fileObj.provides()
            elif k == '/usr/foo':
                assert k not in fileMap
                fileAddition += 1

        self.assertEqual(fileAddition, 1)
        self.assertEqual(fileOverride, 0)

    @conary_test.rpm
    def testCapsuleDerivationSharedDirs(self):
        pkgname = 'shareddirs'
        # create a capsule package
        recipe = """
class TestMultiPackage(CapsuleRecipe):
    name = 'shareddirs'
    version = '0.1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('shareddirs-1.0-1.x86_64.rpm', package='shareddirs:rpm')
        r.addCapsule('shareddirs-secondary-1.0-1.x86_64.rpm', package='shareddirs:rpm2')
"""
        repos = self.openRepository()

        os.chdir(self.workDir)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname=pkgname))
        self.addfile(pkgname + '.recipe')
        self.discardOutput(self.commit)
        built = self.cookFromRepository(pkgname)
        self.failUnlessEqual(set([ x[0] for x in built ]),
            set(['shareddirs:rpm', 'shareddirs:rpm2']))

        # create a derived package
        derive.derive(repos, self.cfg, self.shadowLabel, pkgname)
        os.chdir(self.workDir)
        self.checkout(pkgname, self.shadowLabel.asString(),
                      dir=pkgname + '-derived')
        os.chdir(pkgname + '-derived')
        derivedRecipe = """
class TestMultiPackage(DerivedCapsuleRecipe):
    name = "shareddirs"
    version = '0.1'

    clearBuildReqs()

    def setup(r):
        # add a new file
        r.Create("/usr/foo", package='shareddirs:rpm', contents="foo_derived")
        r.Create("/usr/foo2", package='shareddirs:rpm2', contents="foo2_derived")
"""
        self.writeFile(pkgname + '.recipe', derivedRecipe)
        self.discardOutput(self.commit)
        dbuilt = self.cookFromRepository(pkgname, buildLabel=self.shadowLabel)
        self.failUnlessEqual(set([ x[0] for x in dbuilt ]),
            set(['shareddirs:rpm', 'shareddirs:rpm2']))

    @conary_test.rpm
    def testDerivedCapsuleContentOverride(self):
        pkgname = 'with-config'

        recipe = """
class Test(CapsuleRecipe):
    name = 'with-config'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('with-config-0.1-1.noarch.rpm')
"""
        self.makeSourceTrove(pkgname, recipe, buildLabel=self.defLabel)
        built = self.cookFromRepository(pkgname, buildLabel=self.defLabel)

        repos = self.openRepository()
        derive.derive(repos, self.cfg, self.shadowLabel, pkgname)

        recipe = r"""
class Test(DerivedCapsuleRecipe):
    name = 'with-config'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/usr/share/with-config.txt',
            contents='some other content\n', mode=0755)
        r.Create('/usr/share/new-content.txt',
            contents='some new content\n', mode=0644)
"""

        self.updateSourceTrove(pkgname, recipe, str(self.shadowLabel))
        built = self.cookFromRepository(pkgname, buildLabel=self.shadowLabel)

        self.updatePkg('%s=%s' % (pkgname, self.shadowLabel))
        self.verifyFile(self.rootDir + '/usr/share/with-config.txt',
            'some other content\n', perms=0755)
        self.verifyFile(self.rootDir + '/usr/share/new-content.txt',
            'some new content\n', perms=0644)
