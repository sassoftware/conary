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


import gzip
import os
from conary_test import rephelp
import tempfile

from conary.build import packagerecipe, manifest
from conary.lib import util, magic

renamePolicy = """
import os
from conary.build import policy
from conary.lib import util

class BogusRename(policy.DestdirPolicy):
    def doFile(self, path):
        realPath = util.joinPaths(self.recipe.macros.destdir, path)
        if os.path.isdir(realPath) and os.listdir(realPath):
            return
        newPath = realPath + '-1'
        os.rename(realPath, newPath)
        self.recipe.recordMove(realPath, newPath)
"""

class ManifestTest(rephelp.RepositoryHelper):
    def setUp(self):
        rephelp.RepositoryHelper.setUp(self)
        self.destdir = os.path.join(self.tmpDir, '_ROOT_')
        util.mkdirChain(self.destdir)
        self.policyCount = 0
        self.policyDirs = []

    def tearDown(self):
        for path in self.policyDirs:
            util.rmtree(path)
        util.rmtree(self.destdir)
        rephelp.RepositoryHelper.tearDown(self)

    def touch(self, fn, contents = ''):
        if os.path.exists(fn):
            return
        util.mkdirChain(os.path.dirname(fn))
        f = open(fn, 'w')
        f.write(contents)
        f.close()

    def addPolicy(self, policyStr):
        tmpPolicyDir = tempfile.mkdtemp()
        self.policyDirs.append(tmpPolicyDir)
        policyFn = os.path.join(tmpPolicyDir, 'policy%d.py' % self.policyCount)
        self.policyCount += 1
        f = open(policyFn, 'w')
        f.write(policyStr)
        f.close()
        self.cfg.policyDirs.append(tmpPolicyDir)

    def getRecipe(self):
        class DummyPackageRecipe(packagerecipe.PackageRecipe):
            def __init__(x):
                x.name = 'package'
                x.version = '1.0'
                packagerecipe.PackageRecipe.__init__(x, self.cfg, None, None)
                x._loadSourceActions(lambda y: True)
                x.loadPolicy()
                x.macros.destdir = self.destdir
        return DummyPackageRecipe()

    def getLegacyRecipe(self):
        def recordMove(*args, **kwargs):
            raise AttributeError("recordMove didn't exists in legacy recipes")
        r = self.getRecipe()
        r.recordMove = recordMove
        return r

    def testExplicitRecord(self):
        r = self.getRecipe()
        man = manifest.ExplicitManifest('foo:runtime', r)
        path = '/foo'
        man.recordPaths(path)
        self.assertEquals(man.manifestPaths, set(['/foo']))

    def testExtraSepRecord1(self):
        r = self.getRecipe()
        man = manifest.ExplicitManifest('foo:runtime', r)
        path = '//foo'
        man.recordPaths(path)
        self.assertEquals(man.manifestPaths, set(['/foo']))

    def testExtraSepRecord2(self):
        r = self.getRecipe()
        man = manifest.ExplicitManifest('foo:runtime', r)
        path = '/' + r.macros.destdir + '//foo'
        man.recordPaths(path)
        self.assertEquals(man.manifestPaths, set(['/foo']))

    def testDestDirTranslate(self):
        r = self.getRecipe()
        man = manifest.ExplicitManifest('foo', r)
        path = os.path.join(self.destdir, 'foo')
        man.recordPaths(path)
        self.assertEquals(man.manifestPaths, set(['/foo']))

    def testRemoveSep(self):
        r = self.getRecipe()
        man = manifest.ExplicitManifest('foo', r)
        path = os.path.sep + os.path.join(self.destdir, 'foo')
        man.recordPaths(path)
        self.assertEquals(man.manifestPaths, set(['/foo']))

    def testWalk(self):
        r = self.getRecipe()
        man = manifest.ExplicitManifest('foo', r)
        path = os.path.sep + os.path.join(self.destdir, 'foo')
        man.recordPaths(path)
        man.walk(init = True)
        self.assertEquals(man.fileSet, set(['/foo']))

        # repeat test to ensure behavior is invariant of init param
        man.fileSet = set()
        man.manifestPaths = set()
        man.recordPaths(path)
        man.walk(init = False)
        self.assertEquals(man.fileSet, set(['/foo']))

    def testCreate(self):
        r = self.getRecipe()
        man = manifest.ExplicitManifest('foo', r)
        path = os.path.sep + os.path.join(self.destdir, 'foo')
        man.recordPaths(path)
        man.manifestsDir = tempfile.mkdtemp()
        try:
            man.manifestFile = os.path.join(man.manifestsDir,
                    os.path.basename(man.manifestFile))
            man.create()
            self.assertEquals('/foo\n', open(man.manifestFile).read())
        finally:
            util.rmtree(man.manifestsDir)

    def testLoad(self):
        r = self.getRecipe()
        r._pathTranslations.append(('/foo', '/bar'))
        man = manifest.ExplicitManifest('foo', r)
        path = os.path.sep + os.path.join(self.destdir, 'foo')
        man.recordPaths(path)
        man.manifestsDir = tempfile.mkdtemp()
        try:
            man.manifestFile = os.path.join(man.manifestsDir,
                    os.path.basename(man.manifestFile))
            man.create()
            regexp = man.load()
        finally:
            util.rmtree(man.manifestsDir)
        self.assertFalse(regexp.match('/foo'))
        self.assertFalse(not regexp.match('/bar'))

    def testTranslatePath(self):
        r = self.getRecipe()
        r._pathTranslations.append(('/foo', '/bar'))
        man = manifest.ExplicitManifest('foo', r)
        self.assertEquals(man.translatePath('/foo'), '/bar')

    def testDocRemap(self):
        recipestr1 = """
class RenamedDocs(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that Doc followed a PackageSpec through
        # the path translation process
        self.addSource('sourcefile')
        self.Doc('sourcefile', package = 'foo')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "RenamedDocs")

        # first prove the package spec for Doc worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:supdoc')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileInfo = [x for x in trv.iterFileList()][0]

        # prove the name was changed
        self.assertEquals(fileInfo[1], '/usr/share/doc/test-0/sourcefile-1')

    def testInstallRemap(self):
        recipestr1 = """
class RenamedInstalls(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that Install followed a PackageSpec through
        # the path translation process
        self.addSource('sourcefile')
        self.Install('sourcefile', '/foo', package = 'foo')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "RenamedInstalls")

        # first prove the package spec for Install worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:runtime')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileInfo = [x for x in trv.iterFileList()][0]

        # prove the name was changed
        self.assertEquals(fileInfo[1], '/foo-1')

    def testJavaDocRemap(self):
        recipestr1 = """
class RenamedJavaDocs(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that JavaDoc followed a PackageSpec through
        # the path translation process
        self.addSource('sourcefile')
        self.JavaDoc('sourcefile', package = 'foo')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "RenamedJavaDocs")

        # first prove the package spec for JavaDoc worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:doc')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileInfo = [x for x in trv.iterFileList()][0]

        # prove the name was changed
        self.assertEquals(fileInfo[1], '/usr/share/javadoc/test-0/sourcefile-1')

    def testMakeDirsRemap(self):
        recipestr1 = """
class RenamedDirs(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that MakeDirs followed a PackageSpec through
        # the path translation process
        self.MakeDirs('/foo', package = 'foo')
        self.ExcludeDirectories(exceptions = '/foo-1')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "RenamedDirs")

        # first prove the package spec for MakeDirs worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:runtime')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileInfo = [x for x in trv.iterFileList()][0]

        # prove the name was changed
        self.assertEquals(fileInfo[1], '/foo-1')

    def testMakeFIFORemap(self):
        recipestr1 = """
class MkFIFO(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that MakeFIFO followed a PackageSpec through
        # the path translation process
        self.MakeFIFO('/foo', package = 'foo')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "MkFIFO")

        # first prove the package spec for MakeFIFO worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:runtime')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileInfo = [x for x in trv.iterFileList()][0]

        # prove the name was changed
        self.assertEquals(fileInfo[1], '/foo-1')

    def testSymlinkDirRemap(self):
        recipestr1 = """
class SymlinkDirRemap(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that Symlink followed a PackageSpec through
        # the path translation process
        self.Symlink('/foo', '/foo/symlink', package = 'foo')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "SymlinkDirRemap")

        # first prove the package spec for Symlink worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:runtime')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileInfo = [x for x in trv.iterFileList()][0]

        # prove the name was changed
        self.assertEquals(fileInfo[1], '/foo/symlink-1')

    def testSymlinkFileRemap(self):
        recipestr1 = """
class SymlinkFileRemap(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that Symlink followed a PackageSpec through
        # the path translation process
        self.Create('/foo/file', package = 'foo')
        # we have to know that the target of our symlink is going to be renamed
        self.Symlink('/foo/file-1', '/foo/symlink', package = 'foo')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "SymlinkFileRemap")

        # first prove the package spec for Symlink worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:runtime')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileNames = [x[1] for x in trv.iterFileList()]

        # prove the name was changed
        self.assertFalse('/foo/symlink-1' not in fileNames)

    def testLinkFileRemap(self):
        recipestr1 = """
class LinkFileRemap(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that Link followed a PackageSpec through
        # the path translation process
        self.Create('/foo/file', package = 'foo')
        # we have to know that the target of our link is going to be renamed
        self.Link('/foo/link', '/foo/file', package = 'foo')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "LinkFileRemap")

        # first prove the package spec for Symlink worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:runtime')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileNames = [x[1] for x in trv.iterFileList()]

        # prove the name was changed
        self.assertFalse('/foo/link-1' not in fileNames)

    def testXInetdServiceRemap(self):
        recipestr1 = """
class XInetdServiceRemap(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # these files should end up in the foo package (CNY-1679)
        # we're looking to prove that XInetdService followed a PackageSpec through
        # the path translation process
        self.XInetdService('foo', 'bogus description', type = 'test',
                protocol = 'tcp', package = 'foo')
"""
        self.addPolicy(renamePolicy)
        (built, d) = self.buildRecipe(recipestr1, "XInetdServiceRemap")

        # first prove the package spec for XInetdService worked.
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'foo:config')

        repos = self.openRepository()
        trvNVF = repos.findTrove(None, built[0])
        trv = repos.getTrove(*trvNVF[0])
        fileInfo = [x for x in trv.iterFileList()][0]

        # prove the name was changed
        self.assertEquals(fileInfo[1], '/etc/xinetd.d/foo-1')

    def testFixupManpagePaths(self):
        recipestr0 = """
class TestFilesInMandir(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(self):
        # this test exercises the FixupManpagePaths policy in conjunction with
        # package spec
        self.Create('%(mandir)s/foo.1', package = 'manpage')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr0, "TestFilesInMandir")
        self.assertEquals(built[0][0], 'manpage:doc')

    def testFixupManpagePathsLegacyCompat(self):
        # prove that conary-policy will work with older versions of conary
        r = self.getLegacyRecipe()
        filePath = os.path.join(self.destdir, 'usr', 'share', 'man', 'foo.1')
        newPath = os.path.join(self.destdir,
                'usr', 'share', 'man', 'man1', 'foo.1')
        util.mkdirChain(os.path.dirname(newPath))
        self.touch(filePath)
        r.FixupManpagePaths(filePath)
        # we want to run just this policy
        self.captureOutput(r._policyMap['FixupManpagePaths'].doProcess, r)

        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))

        # we expect that the move wasn't recorded
        self.assertEquals(r._pathTranslations, [])

    def testFixObsoletePathsLegacyCompat(self):
        # prove that conary-policy will work with older versions of conary
        r = self.getLegacyRecipe()
        filePath = os.path.join(self.destdir, 'usr', 'info', 'foo')
        newPath = os.path.join(self.destdir,
                'usr', 'share', 'info', 'foo')
        util.mkdirChain(os.path.dirname(newPath))
        self.touch(filePath)
        r.FixObsoletePaths(filePath)
        # we want to run just this policy
        self.captureOutput(r._policyMap['FixObsoletePaths'].doProcess, r)

        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))

        # we expect that the move wasn't recorded
        self.assertEquals(r._pathTranslations, [])

    def testFixMultilibPathsLegacyCompat(self):
        # prove that conary-policy will work with older versions of conary
        r = self.getLegacyRecipe()
        path = os.path.join(os.path.sep, 'lib', 'foo.so')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(self.destdir, 'libXX', 'foo.so')
        symlinkPath = os.path.join(os.path.sep, 'lib', 'symlink.so')

        self.touch(filePath, contents = '\x7fELF fake of course...')
        os.symlink('foo.so', util.joinPaths(self.destdir, symlinkPath))

        r.macros.lib = 'libXX'
        r.FixupMultilibPaths()
        # we want to run just this policy
        m = magic.Magic(path, self.destdir)
        m.name = 'ELF'
        r.magic = {}
        r.magic[path] = m
        r.magic[symlinkPath] = m
        r._policyMap['FixupMultilibPaths'].test = lambda *args, **kwargs: True
        r._policyMap['FixupMultilibPaths'].dirmap = \
                {'/usr/lib' : '/usr/libXX', '/lib' : '/libXX'}
        self.captureOutput(r._policyMap['FixupMultilibPaths'].doProcess, r)

        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))
        self.assertFalse(os.path.exists(util.joinPaths(self.destdir, symlinkPath)))
        self.assertFalse(not os.path.exists(util.joinPaths(self.destdir,
            'libXX', 'symlink.so')))

        # we expect that the move wasn't recorded
        self.assertEquals(r._pathTranslations, [])

    def testFixMultilibPaths(self):
        # prove that conary-policy will work with older versions of conary
        r = self.getRecipe()
        path = os.path.join(os.path.sep, 'lib', 'foo.so')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(self.destdir, 'libXX', 'foo.so')
        symlinkPath = os.path.join(os.path.sep, 'lib', 'symlink.so')

        self.touch(filePath, contents = '\x7fELF fake of course...')
        os.symlink('foo.so', util.joinPaths(self.destdir, symlinkPath))

        r.macros.lib = 'libXX'
        r.FixupMultilibPaths()
        # we want to run just this policy
        m = magic.Magic(path, self.destdir)
        m.name = 'ELF'
        r.magic = {}
        r.magic[path] = m
        r.magic[symlinkPath] = m
        r._policyMap['FixupMultilibPaths'].test = lambda *args, **kwargs: True
        r._policyMap['FixupMultilibPaths'].dirmap = \
                {'/usr/lib' : '/usr/libXX', '/lib' : '/libXX'}
        self.captureOutput(r._policyMap['FixupMultilibPaths'].doProcess, r)

        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))
        self.assertFalse(os.path.exists(util.joinPaths(self.destdir, symlinkPath)))
        self.assertFalse(not os.path.exists(util.joinPaths(self.destdir,
            'libXX', 'symlink.so')))

        # we expect that the move was recorded
        # the order foo vs symlink get evaluated isn't stable. sorting here
        # doesn't hurt anything
        self.assertEquals(sorted(r._pathTranslations),
                sorted([('/lib/foo.so', '/libXX/foo.so'),
                    ('/lib/symlink.so', '/libXX/symlink.so')]))

    def testNormalizePkgConfigLegacy(self):
        r = self.getLegacyRecipe()
        path = os.path.join(os.path.sep, r.macros.datadir, 'pkgconfig', 'foo')
        filePath = util.joinPaths(self.destdir, path)
        newPath = util.joinPaths(self.destdir,
                r.macros.libdir, 'pkgconfig', 'foo')
        self.touch(filePath)
        r.NormalizePkgConfig()
        self.captureOutput(r._policyMap['NormalizePkgConfig'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))

        # we expect that no move was recorded
        self.assertEquals(r._pathTranslations, [])

    def testNormalizePkgConfig(self):
        r = self.getRecipe()
        path = os.path.join(os.path.sep, r.macros.datadir, 'pkgconfig', 'foo')
        filePath = util.joinPaths(self.destdir, path)
        newPath = util.joinPaths(self.destdir,
                r.macros.libdir, 'pkgconfig', 'foo')
        self.touch(filePath)
        r.NormalizePkgConfig()
        self.captureOutput(r._policyMap['NormalizePkgConfig'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))

        # we expect that no move was recorded
        self.assertEquals(r._pathTranslations,
                [('/usr/share/pkgconfig/foo',
                    r.macros.libdir + '/pkgconfig/foo')])

    def testNormalizeManPages(self):
        r = self.getRecipe()
        path = os.path.join(os.path.sep, r.macros.mandir, 'man1', 'foo.1.gz')
        path2 = os.path.join(os.path.sep,
                r.macros.mandir, 'man1', 'bar.1.bz2')
        filePath = util.joinPaths(self.destdir, path)
        filePath2 = util.joinPaths(self.destdir, path2)
        self.touch(filePath)
        self.touch(filePath2[:-4], contents = 'text')
        g = gzip.open(filePath, 'w')
        g.write('some comrpessed data')
        g.close()
        self.captureOutput(os.system, 'bzip2 %s' % filePath2[:-4])
        r.NormalizeManPages()
        self.captureOutput(r._policyMap['NormalizeManPages'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(not os.path.exists(filePath))
        self.assertFalse(not os.path.exists(filePath2[:-4] + '.gz'))
        self.assertFalse(os.path.exists(filePath2))

        # we expect that moves were recorded
        # the order foo/bar get evaluated isn't stable. we'll just sort for
        # test purposes
        self.assertEquals(sorted(r._pathTranslations),
                sorted([('/usr/share/man/man1/foo.1.gz',
                    '/usr/share/man/man1/foo.1'),
                 ('/usr/share/man/man1/bar.1.bz2',
                     '/usr/share/man/man1/bar.1'),
                 ('/usr/share/man/man1/bar.1',
                     '/usr/share/man/man1/bar.1.gz'),
                 ('/usr/share/man/man1/foo.1',
                     '/usr/share/man/man1/foo.1.gz')]))

    def testNormalizeManPagesLegacy(self):
        r = self.getLegacyRecipe()
        path = os.path.join(os.path.sep, r.macros.mandir, 'man1', 'foo.1.gz')
        path2 = os.path.join(os.path.sep,
                r.macros.mandir, 'man1', 'bar.1.bz2')
        filePath = util.joinPaths(self.destdir, path)
        filePath2 = util.joinPaths(self.destdir, path2)
        self.touch(filePath)
        self.touch(filePath2[:-4], contents = 'text')
        g = gzip.open(filePath, 'w')
        g.write('some comrpessed data')
        g.close()
        self.captureOutput(os.system, 'bzip2 %s' % filePath2[:-4])
        r.NormalizeManPages()
        self.captureOutput(r._policyMap['NormalizeManPages'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(not os.path.exists(filePath))
        self.assertFalse(not os.path.exists(filePath2[:-4] + '.gz'))
        self.assertFalse(os.path.exists(filePath2))

        # we expect that no moves were recorded
        self.assertEquals(r._pathTranslations, [])

    def testNormalizeInfoPagesLegacy(self):
        r = self.getLegacyRecipe()
        path = os.path.join(os.path.sep, r.macros.infodir, 'man1', 'foo.gz')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(os.path.sep, r.macros.infodir, 'foo.gz')
        self.touch(filePath)
        g = gzip.open(filePath, 'w')
        g.write('some comrpessed data')
        g.close()
        r.NormalizeInfoPages()
        m = magic.Magic(path, self.destdir)
        m.name = 'gzip'
        m.contents['compression'] = 5
        r.magic = {}
        r.magic[path] = m
        r.magic[newPath] = m
        r._policyMap['NormalizeInfoPages'].doProcess(r)
        #self.captureOutput(r._policyMap['NormalizeInfoPages'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(os.path.exists(filePath))
        self.assertFalse(not os.path.exists(util.joinPaths(self.destdir, newPath)))

        # we expect that no moves were recorded
        self.assertEquals(r._pathTranslations, [])

    def testNormalizeInfoPages(self):
        r = self.getRecipe()
        path = os.path.join(os.path.sep, r.macros.infodir, 'man1', 'foo.gz')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(os.path.sep, r.macros.infodir, 'foo.gz')
        self.touch(filePath)
        g = gzip.open(filePath, 'w')
        g.write('some comrpessed data')
        g.close()
        r.NormalizeInfoPages()
        m = magic.Magic(path, self.destdir)
        m.name = 'gzip'
        m.contents['compression'] = 5
        r.magic = {}
        r.magic[path] = m
        r.magic[newPath] = m
        r._policyMap['NormalizeInfoPages'].doProcess(r)
        #self.captureOutput(r._policyMap['NormalizeInfoPages'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(os.path.exists(filePath))
        self.assertFalse(not os.path.exists(util.joinPaths(self.destdir, newPath)))

        # we expect that moves were recorded
        self.assertEquals(r._pathTranslations,
                [('/usr/share/info/man1/foo.gz', '/usr/share/info/foo.gz')])

    def testNormalizeBz2InfoPages(self):
        r = self.getRecipe()
        path = os.path.join(os.path.sep, r.macros.infodir, 'man1', 'foo.bz2')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(os.path.sep, r.macros.infodir, 'foo.gz')
        self.touch(filePath[:-4])
        self.captureOutput(os.system, 'bzip2 %s' % filePath[:-4])

        r.NormalizeInfoPages()
        m = magic.Magic(path, self.destdir)
        m.name = 'bzip'
        m.contents['compression'] = 5
        r.magic = {}
        r.magic[path] = m
        r.magic[newPath] = m
        r.magic[newPath[:-3] + '.bz2'] = m
        r._policyMap['NormalizeInfoPages'].doProcess(r)
        #self.captureOutput(r._policyMap['NormalizeInfoPages'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(os.path.exists(filePath))
        self.assertFalse(not os.path.exists(util.joinPaths(self.destdir, newPath)))

        # we expect that moves were recorded
        self.assertEquals(r._pathTranslations,
                [('/usr/share/info/man1/foo.bz2',
                    '/usr/share/info/foo.bz2'),
                 ('/usr/share/info/foo.bz2',
                     '/usr/share/info/foo.gz')])

    def testNormalizeInitscriptLocationLegacy(self):
        r = self.getLegacyRecipe()
        path = os.path.join(os.path.sep, 'etc', 'rc.d', 'init.d', 'foo')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(self.destdir, 'foo', 'foo')
        self.touch(filePath)
        r.macros.initdir = '/foo'
        r.NormalizeInitscriptLocation()
        self.captureOutput( \
                r._policyMap['NormalizeInitscriptLocation'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))

        # we expect that no move was recorded
        self.assertEquals(r._pathTranslations, [])

    def testNormalizeInitscriptLocation(self):
        r = self.getRecipe()
        path = os.path.join(os.path.sep, 'etc', 'rc.d', 'init.d', 'foo')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(self.destdir, 'foo', 'foo')
        self.touch(filePath)
        r.macros.initdir = '/foo'
        r.NormalizeInitscriptLocation()
        self.captureOutput( \
                r._policyMap['NormalizeInitscriptLocation'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))

        # we expect that no move was recorded
        self.assertEquals(r._pathTranslations,
                [('/etc/rc.d/init.d/foo', '/foo/foo')])

    def testNormalizeAppDefaultsLegacy(self):
        r = self.getLegacyRecipe()
        path = os.path.join(os.path.sep, 'etc', 'X11', 'app-defaults', 'foo')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(self.destdir, 'usr', 'X11R6', 'lib', 'X11', 'app-defaults', 'foo')
        self.touch(filePath)
        r.NormalizeAppDefaults()
        self.captureOutput( \
                r._policyMap['NormalizeAppDefaults'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))

        # we expect that no move was recorded
        self.assertEquals(r._pathTranslations, [])

    def testNormalizeAppDefaults(self):
        r = self.getRecipe()
        path = os.path.join(os.path.sep, 'etc', 'X11', 'app-defaults', 'foo')
        filePath = util.joinPaths(self.destdir, path)
        newPath = os.path.join(self.destdir, 'usr', 'X11R6', 'lib', 'X11', 'app-defaults', 'foo')
        self.touch(filePath)
        r.NormalizeAppDefaults()
        self.captureOutput( \
                r._policyMap['NormalizeAppDefaults'].doProcess, r)
        # prove the policy did move the file
        self.assertFalse(not os.path.exists(newPath))
        self.assertFalse(os.path.exists(filePath))

        # we expect that no move was recorded
        self.assertEquals(r._pathTranslations,
                [('/etc/X11/app-defaults/foo',
                    '/usr/X11R6/lib/X11/app-defaults/foo')])
