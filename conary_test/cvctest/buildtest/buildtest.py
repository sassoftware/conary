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


import bz2
import os
import sys
import socket, stat
from testrunner import testhelp

from conary import versions
from conary.lib import util
from conary_test import rephelp
from conary.build import errors
from conary.build import policy, packagepolicy


# for use in many places...
def mockedSaveArgSet(self, real, s, *args):
    for arg in args:
        if type(arg) in (list, tuple, set):
            s.update(arg)
        else:
            s.add(arg)
    if real:
        real(self, *args)

# for use in many places...
def mockedUpdateArgs(self, s, *args):
    for arg in args:
        if type(arg) in (list, tuple, set):
            s.update(arg)
        else:
            s.add(arg)




class XMLCatalogTest(rephelp.RepositoryHelper):
    def testXMLCatalogTest1(self):
        # This recipe should fail because it lacks a buildRequires 
        # entry to libxml2:runtime.
        recipestr1 = """
class TestXMLCatalog(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.XMLCatalogEntry('sgml-entities.xml', 
                          'delegatePublic', 
                          '-//OASIS//DocBk ', 
                          '%(datadir)s/sgml/docbook/master.xml')
"""
        self.assertRaises(policy.PolicyError, self.buildRecipe,
                recipestr1, "TestXMLCatalog")

        recipestr2 = """
class TestXMLCatalog2(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    buildRequires = [ 'libxml2:runtime' ]

    def setup(r):
        r.XMLCatalogEntry('a.xml',
                          'delegatePublic',
                          '-//OASIS//DocBk ',
                          '%(datadir)s/sgml/docbook/master.xml')
        r.XMLCatalogEntry('a.xml',
                          'delegatePublic',
                          '-//OASIS//DocBook ',
                          '%(datadir)s/sgml/docbook/master2.xml')
        r.XMLCatalogEntry('b.xml',
                          'delegatePublic',
                          '-//OASIS//SGML Entities ',
                          '%(datadir)s/sgml/docbook/master3.xml',
                          catalogDir='/ue')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr2, "TestXMLCatalog2", ignoreDeps=True)
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])

        # There's not really a good way to test to see if this file 
        # was created correctly, so we just test to see if an 
        # expected number of lines are present. Note: this line
        # count could change with future versions of libxml2.
        F = file(util.joinPaths(self.workDir, '/etc/xml/a.xml'))
        lines = F.readlines()
        assert(len(lines) == 6)
        F.close()
        
        assert(os.lstat(util.joinPaths(self.workDir, 'ue/b.xml')))




class SGMLCatalogTest(rephelp.RepositoryHelper):
    def testSGMLCatalogTest1(self):
        # This recipe should fail because it lacks a buildRequires 
        # entry to libxml2:runtime.     
        recipestr1 = """
class TestSGMLCatalog(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.SGMLCatalogEntry('sgml-entities.cat', 'foot.cat')
"""
        self.assertRaises(policy.PolicyError, self.buildRecipe,
            recipestr1, "TestSGMLCatalog")

        recipestr2 = """
class TestSGMLCatalog(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    buildRequires = ['libxml2:runtime']

    def setup(r):
        r.SGMLCatalogEntry('sgml-entities.cat', 'foo.cat')
        r.SGMLCatalogEntry('sgml-entities.cat', 'bar.cat')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr2, "TestSGMLCatalog", ignoreDeps=True)
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])  

        F = file(util.joinPaths(self.workDir, '/etc/sgml/sgml-entities.cat'))
        lines = F.readlines()
        list = ['CATALOG "bar.cat"\n',
                'CATALOG "foo.cat"\n']
        lines.sort()
        assert(lines == list)
        F.close()
 
 

class PutFilesTest(rephelp.RepositoryHelper):
    def testPutFilesTest1(self):
        """
        Test _PutFiles
        """

        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Create('this', 'that')
        self.Install('this', 'that', '/foo')
"""
        self.assertRaises(errors.CookError, self.buildRecipe,
            recipestr, "TestPutFiles")

    def testPutFilesTest2(self):
        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Create('this', 'that')
        self.Install('t*', '/foo')
"""
        self.assertRaises(TypeError, self.buildRecipe,
            recipestr, "TestPutFiles")

    def testPutFilesTest3(self):
        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Create('foo', 'bar', 'baz')
        self.MakeDirs('a')
        self.Create('a/{foo,bar,baz}')
        self.Install('foo', '/')
        self.MakeDirs('/b')
        self.Install('foo', 'bar', 'baz', '/b/')
        self.Install('a', '/c/')
        self.Install('a', '/z/')
        self.Move('/z', '/d')
        self.Install('a', '/z/')
        self.Move('/z/*', '/e/')
        self.Copy('/e/*', '/f')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestPutFiles")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        assert(os.lstat(util.joinPaths(self.workDir, 'foo')))
        assert(os.lstat(util.joinPaths(self.workDir, 'b/foo')))
        assert(os.lstat(util.joinPaths(self.workDir, 'b/bar')))
        assert(os.lstat(util.joinPaths(self.workDir, 'b/baz')))
        assert(os.lstat(util.joinPaths(self.workDir, 'c/a/foo')))
        assert(os.lstat(util.joinPaths(self.workDir, 'c/a/bar')))
        assert(os.lstat(util.joinPaths(self.workDir, 'c/a/baz')))
        assert(os.lstat(util.joinPaths(self.workDir, 'd/a/foo')))
        assert(os.lstat(util.joinPaths(self.workDir, 'd/a/bar')))
        assert(os.lstat(util.joinPaths(self.workDir, 'd/a/baz')))
        assert(os.lstat(util.joinPaths(self.workDir, 'e/a/foo')))
        assert(os.lstat(util.joinPaths(self.workDir, 'e/a/bar')))
        assert(os.lstat(util.joinPaths(self.workDir, 'e/a/baz')))
        assert(os.lstat(util.joinPaths(self.workDir, 'f/a/foo')))
        assert(os.lstat(util.joinPaths(self.workDir, 'f/a/bar')))
        assert(os.lstat(util.joinPaths(self.workDir, 'f/a/baz')))
        self.assertRaises(OSError, os.lstat,
                          util.joinPaths(self.workDir, 'z/a/foo'))
        self.assertRaises(OSError, os.lstat,
                          util.joinPaths(self.workDir, 'z/a/bar'))
        self.assertRaises(OSError, os.lstat,
                          util.joinPaths(self.workDir, 'z/a/baz'))

    def testPutFilesTest4(self):
        # test strange names
        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Create('[b]ar', '*ar', 'bar')
        # note -- you can't quote metachars in file names -- \* will not
        # escape the * -- 
        # but we can make sure that metacharacters in file names are not
        # expanded twice when those files are caught by a glob
        self.Install('*', '/strangeNames/')
        self.Doc('*')
        self.Remove('*')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestPutFiles")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        assert(os.lstat(util.joinPaths(self.workDir, '/strangeNames/[b]ar')))
        assert(os.lstat(util.joinPaths(self.workDir, '/strangeNames/*ar')))
        assert(os.lstat(util.joinPaths(self.workDir, '/strangeNames/bar')))
        assert(os.lstat(util.joinPaths(self.workDir, 
                                       '/usr/share/doc/test-0/[b]ar')))
        assert(os.lstat(util.joinPaths(self.workDir, 
                                      '/usr/share/doc/test-0/*ar')))
        assert(os.lstat(util.joinPaths(self.workDir,
                                       '/usr/share/doc/test-0/bar')))

    def testPutFilesTest5(self):
        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('this')
        r.Move('this')
"""
        self.assertRaises(errors.CookError, self.buildRecipe,
            recipestr, "TestPutFiles")

    def testUnmatchedPutFilesTest(self):
        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        self.MakeDirs("/foo")
        self.Copy('/foo/*', '/bar', allowNoMatch = True)
"""
        self.logFilter.add()
        self.buildRecipe(recipestr, "TestPutFiles")
        self.logFilter.remove()
        self.assertEquals(self.logFilter.records[0],
                "warning: Copy: No files matched: '/foo/*'")

    def testUnmatchedPutFilesTest2(self):
        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        self.MakeDirs("/foo")
        self.Copy('/foo/*', '/bar')
"""
        self.assertRaises(RuntimeError,
                self.buildRecipe, recipestr, "TestPutFiles")


    def testPutFilesPermissionOverride(self):
        # CNY-1634
        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.Create('/file', contents = 'old', mode = 0444)
        r.Run('echo new > new')
        r.Install('new', '/file')
"""
        self.buildRecipe(recipestr, "TestPutFiles")
        self.updatePkg('test')
        self.verifyFile(self.rootDir + '/file', 'new\n')

    def testPutSymlinks(self):
        recipestr = """
class TestPutFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/dir1/foo1', contents='abc', mode=0755)
        # Link to an existing file
        r.Symlink('foo1', '/dir1/foo5')
        # Dangling symlink
        r.Symlink('/dir1/foo1', '/dir2/foo2')
        r.Install('/dir2/foo2', '/dir3/', preserveSymlinks=True)
        r.Install('/dir2/foo2', '/dir4/foo4', preserveSymlinks=True)
        # This should simply copy the file
        r.Install('/dir1/foo5', '/dir5/foo5')
        # This is just to test modes for Install
        r.Install('/dir1/foo1', '/dir1/foo600', mode=0600)
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestPutFiles")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        sl755 = util.joinPaths(self.workDir, '/dir1/foo1')
        sl600 = util.joinPaths(self.workDir, '/dir1/foo600')
        sl1 = util.joinPaths(self.workDir, '/dir1/foo5')
        sl2 = util.joinPaths(self.workDir, '/dir2/foo2')
        # Test modes
        self.assertFalse(os.path.islink(sl755))
        self.assertEqual(0755, os.lstat(sl755)[stat.ST_MODE] & 0755)
        self.assertFalse(os.path.islink(sl600))
        self.assertEqual(0600, os.lstat(sl600)[stat.ST_MODE] & 0600)

        self.assertTrue(os.path.islink(sl1))
        self.assertTrue(os.path.islink(sl2))

        sl3 = util.joinPaths(self.workDir, '/dir3/foo2')
        sl4 = util.joinPaths(self.workDir, '/dir4/foo4')
        sl5 = util.joinPaths(self.workDir, '/dir5/foo5')

        self.assertTrue(os.path.islink(sl3))
        self.assertTrue(os.path.islink(sl4))
        self.assertFalse(os.path.islink(sl5))

    def testSymlinkGlobbing(self):
        recipestr = r"""
class TestSymlinkGlobbing(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.Create("/usr/srcfiles/file1", contents="Contents file1\n")
        r.Create("/usr/srcfiles/file2", contents="Contents file2\n")
        r.Symlink("/usr/srcfiles/*", "/usr/symlinks/")
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestSymlinkGlobbing")
        repos = self.openRepository()
        self.updatePkg(self.rootDir, 'test')
        symlinksdir = os.path.join(self.rootDir, 'usr/symlinks')
        contents = os.listdir(symlinksdir)
        contents.sort()
        self.assertEqual(contents, ['file1', 'file2'])
        for f in contents:
            lpath = os.path.join(symlinksdir, f)
            fpath = os.readlink(lpath)
            if fpath[0] != '/':
                # Relative link
                fpath = os.path.join(symlinksdir, fpath)
            self.assertEqual(open(fpath).read(), "Contents %s\n" % f)

    def testUnmatchedSymlinkGlobbing(self):
        recipestr = r"""
class TestSymlinkGlobbing(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.MakeDirs("/usr/srcfiles")
        r.Symlink("/usr/srcfiles/*", "/usr/symlinks/")
"""
        self.assertRaises(RuntimeError, self.buildRecipe, recipestr, "TestSymlinkGlobbing")

    def testUnmatchedSymlinkGlobbing2(self):
        recipestr = r"""
class TestSymlinkGlobbing(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.MakeDirs("/usr/srcfiles")
        r.Symlink("/usr/srcfiles/*", "/usr/symlinks/", allowNoMatch = True)
"""
        self.reset()
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr, "TestSymlinkGlobbing")
        self.logFilter.remove()
        self.assertFalse("warning: Symlink: No files matched: '/usr/srcfiles/*'" \
                not in self.logFilter.records)


class ManifestTest(rephelp.RepositoryHelper):
    def testManifest(self):
        recipestr = r"""
class TestManifest(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        # ensure that ordering is correct by specifying PackageSpec first
        r.PackageSpec('asdf', '/asdf/asdf')
        r.Create('Makefile', contents='\n'.join((
            'install:',
            '\tmkdir -p $(DESTDIR)/foo/blah',
            '\ttouch $(DESTDIR)/foo/blah/bar',
            '\tmkdir -p $(DESTDIR)/asdf',
            '\ttouch $(DESTDIR)/asdf/asdf',
        )))
        r.MakeInstall(package='foo')
        r.ExcludeDirectories(exceptions='/foo')
        r.Create('/blah/test')
        r.Create('/oddcomp', package=':oddcomp')
        r.Run('touch %(destdir)s/asdf/fdsa', package='asdf:fdsa')
        # test empty manifests
        r.Run('true', package=':doesnotexist')
        # test skipping missing subdirectories
        r.MakeInstall(dir='non-existent', skipMissingDir=True, package='nonesuch')
        r.Create('testfileaction')
        r.Install('testfileaction', '/var/', package=':testfileaction')


        # ensure that the manifests are correct, easiest done
        # from within the build
        r.Create('asdf.0.manifest', contents='/asdf/fdsa')
        r.Create('foo.0.manifest', contents='\n'.join((
            '/asdf',
            '/asdf/asdf',
            '/foo',
            '/foo/blah',
            '/foo/blah/bar',
        )))
        r.Create('.0.manifest', contents = '/oddcomp')
        r.Create('nonesuch.0.manifest')
        r.Run('cmp foo.0.manifest ../_MANIFESTS_/foo.0.manifest')
        r.Run('cmp asdf.0.manifest ../_MANIFESTS_/asdf.0.manifest')
        #r.Run('cp .0.manifest /tmp/; cp ../_MANIFESTS_/.0.manifest /tmp/acutal.0.manifest')
        r.Run('cmp .0.manifest ../_MANIFESTS_/.0.manifest')
        r.Run('cmp nonesuch.0.manifest ../_MANIFESTS_/nonesuch.0.manifest')
        r.Remove('asdf.0.manifest', 'foo.0.manifest')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestManifest")
        repos = self.openRepository()
        for troveName, troveVersion, troveFlavor in built:
            troveVersion = versions.VersionFromString(troveVersion)
            trove = repos.getTrove(troveName, troveVersion, troveFlavor)
            for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
                trove.getName(), trove.getVersion(), trove.getFlavor(),
                withFiles=True):
                assert path != '/blah'
                if path == '/foo':
                    assert trove.getName() == 'foo:runtime'
                if path == '/oddcomp':
                    assert trove.getName() == 'test:oddcomp'
                if path == '/foo/blah/bar':
                    assert trove.getName() == 'foo:runtime'
                if path == '/blah/test':
                    assert trove.getName() == 'test:runtime'
                if path == '/asdf/asdf':
                    assert trove.getName() == 'asdf:runtime'
                if path == '/asdf/fdsa':
                    assert trove.getName() == 'asdf:fdsa'
                if path == '/var/testfileaction':
                    assert trove.getName() == 'test:testfileaction'


class LinkTest(rephelp.RepositoryHelper):
    """
    Test creating hard links
    """
    def testLinkTestBad(self):
        recipestr1 = """
class TestLink(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Link('this/is/broken', '')
"""
        self.assertRaises(errors.CookError, self.buildRecipe,
            recipestr1, "TestLink")

    def testLinkTestGood(self):
        recipestr2 = """
class TestLink(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.macros.foo = '/foo'
        r.macros.bar = 'bar'
        r.Create('%(foo)s',
            contents='ABCDEFGABCDEFGABCDEFGABCDEFG%(destdir)s/')
        r.Link('%(bar)s', 'blah', '%(foo)s')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr2, "TestLink")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        a = os.lstat(util.joinPaths(self.workDir, 'foo'))
        b = os.lstat(util.joinPaths(self.workDir, 'bar'))
        c = os.lstat(util.joinPaths(self.workDir, 'blah'))
        assert(a[stat.ST_INO] == b[stat.ST_INO])
        assert(b[stat.ST_INO] == c[stat.ST_INO])

    def testLinkDir(self):
        recipe1 = """
class FooRecipe(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.MakeDirs('/var/foo', '/var/bar/')
        r.Create('/var/foo/testme', contents='arbitrary data')
        r.Link('/var/foo/tested', '/var/foo/testme')
"""
        (built, d) = self.buildRecipe(recipe1, "FooRecipe")
        self.updatePkg(built[0][0])
        assert(os.lstat(self.rootDir + '/var/foo/testme').st_ino ==
               os.lstat(self.rootDir + '/var/foo/tested').st_ino)

class MakeDirsTest(rephelp.RepositoryHelper):
    def testMakeDirsTest1(self):
        """
        Test creating directories
        """

        recipestr1 = """
class TestMakeDirs(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.MakeDirs('foo')
        self.Run('ls foo')
"""
        (built, d) = self.buildRecipe(recipestr1, "TestMakeDirs")

        recipestr2 = """
class TestMakeDirs(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.MakeDirs('/bar/blah')
        self.ExcludeDirectories(exceptions='/bar/blah')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr2, "TestMakeDirs")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        assert(stat.S_ISDIR(
            os.lstat(util.joinPaths(self.workDir, '/bar/blah'))[stat.ST_MODE]))




class SugidTest(rephelp.RepositoryHelper):
    def testSugidTest1(self):
        """
        Test to make sure that setu/gid gets restored.
        Warning: this won't catch variances when running as root!
        """

        recipestr1 = """
class TestSugid(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Create('%(essentialbindir)s/a', mode=06755)
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestSugid")
        self.mimicRoot()
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        self.realRoot()
        a = os.lstat(util.joinPaths(self.workDir, 'bin/a'))
        assert (a.st_mode & 07777 == 06755)





class CreateTest(rephelp.RepositoryHelper):
    def testCreateTest1(self):
        """
        Test creating files directly
        """

        recipestr1 = """
class TestCreate(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Create(('/a', '/b'))
        self.Create('/c', '/d', contents='ABCDEFGABCDEFGABCDEFGABCDEFG')
        self.Create('/e', contents='%(essentialbindir)s')
        self.Create('/f', contents='%(essentialbindir)s', macros=False)
        self.Create('%(essentialbindir)s/{g,h}', mode=0755)
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestCreate")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        a = os.lstat(util.joinPaths(self.workDir, 'a'))
        b = os.lstat(util.joinPaths(self.workDir, 'b'))
        F = file(util.joinPaths(self.workDir, 'c'))
        c = F.read()
        F.close
        F = file(util.joinPaths(self.workDir, 'd'))
        d = F.read()
        F.close
        F = file(util.joinPaths(self.workDir, 'e'))
        e = F.read()
        F.close
        F = file(util.joinPaths(self.workDir, 'e'))
        e = F.read()
        F.close
        F = file(util.joinPaths(self.workDir, 'f'))
        f = F.read()
        F.close
        g = os.lstat(util.joinPaths(self.workDir, '/bin/g'))
        h = os.lstat(util.joinPaths(self.workDir, '/bin/g'))
        assert (a.st_size == 0)
        assert (b.st_size == 0)
        assert (c == 'ABCDEFGABCDEFGABCDEFGABCDEFG\n')
        assert (d == 'ABCDEFGABCDEFGABCDEFGABCDEFG\n')
        assert (e == '/bin\n')
        assert (f == '%(essentialbindir)s\n')
        assert (g.st_mode & 0777 == 0755)
        assert (h.st_mode & 0777 == 0755)



class SymlinkTest(rephelp.RepositoryHelper):
    def testSymlinkTest1(self):
        """
        Test creating files directly
        """

        recipestr1 = """
class TestSymlink(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/a')
        r.Symlink('/one/argument')
"""
        self.assertRaises(errors.CookError, self.buildRecipe,
            recipestr1, "TestSymlink")
            
    def testSymlinkTest2(self):
        recipestr2 = """
class TestSymlink(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Symlink('/asdf/foo', '/bar/blah')
        r.DanglingSymlinks(exceptions='.*')
"""
        self.buildRecipe(recipestr2, "TestSymlink")

class DocTest(rephelp.RepositoryHelper):

    def exists(self, file):
        return os.path.exists(self.workDir + file)


    def testDocs(self):

        recipestr1 = """
class TestDocs(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('README')
        r.Doc('README')
        r.Create('docs/README.too')
        r.Doc('docs/')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestDocs")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        
        docdir = '/usr/share/doc/test-0/'
        for file in 'README', 'docs/README.too':
            assert(self.exists(docdir + file))

class ConfigureTest(rephelp.RepositoryHelper):

    def testConfigure(self):
        recipestr1 = """
class TestConfigure(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.addSource('configure', mode=0755, contents='''#!/bin/sh
exit 0
''')
        r.Configure()
        r.Create('/asdf/foo')
"""
        (built, d) = self.buildRecipe(recipestr1, "TestConfigure")
        # make sure that the package doesn't mention the bootstrap
        # bootstrap flavor
        assert(built[0][2].isEmpty())

    def testConfigureSubDirMissingOK(self):
        recipestr1 = """
class TestConfigure(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.addSource('configure', mode=0755, contents='''#!/bin/sh
touch mustNotExist
exit 0
''')
        r.Configure(subDir='missing', skipMissingSubDir=True)
        r.Run('test -f mustNotExist && exit 1 ; exit 0')
        r.Create('/asdf/foo')
"""
        (built, d) = self.buildRecipe(recipestr1, "TestConfigure")

    def testConfigureSubDirMissingBad(self):
        recipestr1 = """
class TestConfigure(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.addSource('configure', mode=0755, contents='''#!/bin/sh
exit 0
''')
        r.Configure(subDir='missing')
        r.Create('/asdf/foo')
"""
        self.assertRaises(RuntimeError, self.buildRecipe,
                          recipestr1, "TestConfigure")


    def testConfigureLocal(self):
        recipestr1 = """
class TestConfigure(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.addSource('configure', mode=0755, contents='''#!/bin/sh -x
echo "$CONFIG_SITE" > $1
''')
        r.MakeDirs('/make', '/conf')
        r.ManualConfigure('%(destdir)s/conf/target')
        r.ManualConfigure('%(destdir)s/conf/local', local=True)
        r.Make('%(destdir)s/make/target', makeName='./configure')
        r.Make('%(destdir)s/make/local', local=True, makeName='./configure')
        # run again to make sure any state changed by Make was restored.
        r.ManualConfigure('%(destdir)s/conf/target')
        r.ManualConfigure('%(destdir)s/conf/local', local=True)
"""
        self.overrideBuildFlavor('is:x86 target: x86_64')
        (built, d) = self.buildRecipe(recipestr1, "TestConfigure")
        self.updatePkg('test[is:x86 target:x86_64]')
        for dir in ('%s/make/', '%s/conf'):
            dir = dir % self.cfg.root
            self.verifyFile('%s/local' % dir,
                            ' '.join([ '%s/%s' % (self.cfg.siteConfigPath[0], x)
                                        for x in ('x86', 'linux')]) + '\n')
            self.verifyFile('%s/target' % dir,
                            ' '.join([ '%s/%s' % (self.cfg.siteConfigPath[0], x)
                                        for x in ('x86_64', 'linux')]) + '\n')

    def testConfigureMissingReq(self):
        recipestr = """
class TestConfigure(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addSource('configure', mode=0755, contents='''#!/bin/sh
echo "$0: line 2000: foo: command not found"
# exit 1
''')
        r.ManualConfigure()
        r.Create('/opt/foo')
"""
        self.logFilter.add()
        self.assertRaises(RuntimeError, self.buildRecipe, 
                          recipestr.replace('# exit', 'exit'),
                          "TestConfigure", logBuild = True)
        self.logFilter.remove()
        self.logFilter.regexpCompare([
            'error: .*',
            'warning: ./configure: line 2000: foo: command not found',
            'warning: Failed to find possible build requirement for path "foo"',
        ])

        # now repeat with foo in the repository but not installed
        self.addComponent('foo:runtime', '1', fileContents = [
            ('/usr/bin/foo', rephelp.RegularFile(contents="", perms=0755)),])
        repos = self.openRepository()
        self.logFilter.add()
        reportedBuildReqs = set()
        self.mock(packagepolicy.reportMissingBuildRequires, 'updateArgs',
                  lambda *args:
                    mockedSaveArgSet(args[0], None, reportedBuildReqs, *args[1:]))
        (built, d) = self.buildRecipe(recipestr, "TestConfigure",
                logBuild = True, repos=repos)
        self.logFilter.remove()
        self.logFilter.compare([
            'warning: ./configure: line 2000: foo: command not found',
            "warning: Some missing buildRequires ['foo:runtime']",
        ])
        self.assertEquals(reportedBuildReqs, set(['foo:runtime']))
        self.unmock()

        # now test with absolute path in error message
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr.replace(
            'foo: command not found', '/usr/bin/foo: command not found'),
                "TestConfigure",
                logBuild = True)
        self.logFilter.remove()
        self.logFilter.regexpCompare([
            'warning: .*: /usr/bin/foo: command not found',
            r"warning: Some missing buildRequires \['foo:runtime'\]",
        ])
        # test that the logfile got the warning message
        client = self.getConaryClient()
        repos = client.getRepos()
        nvf = [x for x in built if x[0] == 'test:debuginfo'][0]
        nvf = repos.findTrove(self.cfg.buildLabel, nvf)
        fileDict = client.getFilesFromTrove(*nvf[0])
        fileObj = fileDict['/usr/src/debug/buildlogs/test-0-log.bz2']
        b = bz2.BZ2Decompressor()
        buildLog = b.decompress(fileObj.read())
        self.assertFalse( \
                "warning: Suggested buildRequires additions: ['foo:runtime']" \
                not in buildLog)

        # finally repeat with foo installed, not just in repository
        self.updatePkg('foo:runtime')
        self.logFilter.add()
        reportedBuildReqs = set()
        self.mock(packagepolicy.reportMissingBuildRequires, 'updateArgs',
                  lambda *args:
                    mockedSaveArgSet(args[0], None, reportedBuildReqs, *args[1:]))
        (built, d) = self.buildRecipe(recipestr, "TestConfigure",
                logBuild = True)
        self.logFilter.remove()
        self.logFilter.compare([
            'warning: ./configure: line 2000: foo: command not found',
            "warning: Some missing buildRequires ['foo:runtime']",
        ])
        self.assertEquals(reportedBuildReqs, set(['foo:runtime']))


    def testConfigureMissingReq2(self):
        """
        test that regexp matching is not fooled by dir argument
        """
        recipestr1 = """
class TestConfigure(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.Create('configure', mode=0755, contents='''#!/bin/sh
echo "/random/path/configure: line 2000: foo: command not found"
''')
        r.ManualConfigure('')
        r.Create('/opt/foo')
"""
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr1, "TestConfigure",
                logBuild = True)
        self.logFilter.remove()
        self.logFilter.compare([
            'warning: /random/path/configure: line 2000: foo: '
                        'command not found',
            'warning: Failed to find possible build requirement for path "foo"',
        ])


class CMakeTest(rephelp.RepositoryHelper):

    def testCMake(self):
        if not util.checkPath('cmake'):
            raise testhelp.SkipTestException('cmake not installed')
        recipestr1 = """
class TestCMake(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.addSource('CMakeLists.txt', contents = '''\
PROJECT(floo)
ADD_EXECUTABLE(floo floo.c)
''')
        r.addSource('floo.c', contents = '''
int main()
{
    return 0;
}
''')
        r.CMake()
        r.Make()
        r.Copy('floo', '/usr/bin/floo')
"""
        (built, d) = self.buildRecipe(recipestr1, "TestCMake")

    def testCMakeSubDir(self):
        if not util.checkPath('cmake'):
            raise testhelp.SkipTestException('cmake not installed')
        # Same as previous test, but run in a subdir
        recipestr1 = """
class TestCMake(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('floo/CMakeLists.txt', contents = '''\
PROJECT(floo)
''')
        r.CMake(dir = 'floo')
        r.Copy('floo/Makefile', '/usr/share/floo/')
"""
        (built, d) = self.buildRecipe(recipestr1, "TestCMake")

class RemoveTest(rephelp.RepositoryHelper):

    def testRemove(self):

        recipestr1 = """
class TestRemove(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.MakeDirs('a/b')
        r.Create('a/file')
        r.Install('a', '/a')
        r.Remove('/a/*')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestRemove")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])

    def testRemoveRecursive(self):
        # Test for CNY-69
        recipestr1 = """
class TestRemove(PackageRecipe):
    name = 'testr'
    version = '0.1'
    clearBuildReqs()

    def setup(r):
        r.Create("%(datadir)s/%(name)s/dir1/file1", contents="file1")
        r.Create("%(datadir)s/%(name)s/dir1/dir2/file2", contents="file2")
        r.Create("%(datadir)s/%(name)s/dir1/dir2/dir3/file3", contents="file3")
        r.Create("%(datadir)s/%(name)s/dir1/dir2/dir5/file4", contents="file4")
        r.Remove("%(datadir)s/%(name)s/dir1/dir2", recursive=True)
"""
        repos = self.openRepository()
        oldVal = self.cfg.cleanAfterCook
        self.cfg.cleanAfterCook = False
        try:
            (build, d) = self.buildRecipe(recipestr1, "TestRemove")
        finally:
            self.cfg.cleanAfterCook = oldVal

        dr = os.path.join(self.workDir, '../build/testr/_ROOT_',
            'usr/share/testr')
        self.assertEqual(os.listdir(dr), ["dir1"])

    def testUnmatchedRemove(self):
        recipestr = """
class TestRemove(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.MakeDirs('/a')
        """
        self.reset()
        err = self.assertRaises(RuntimeError,
                self.buildRecipe, recipestr + "r.Remove(r.glob('/a/*'))\n", 
                "TestRemove")
        assert(str(err) == "Remove: No files matched: Glob('/a/*')")
        err = self.assertRaises(RuntimeError,
                self.buildRecipe, recipestr + "r.Remove('/a/*')\n", 
                "TestRemove")
        assert(str(err) == "Remove: No files matched: '/a/*'")
        err = self.assertRaises(RuntimeError,
                self.buildRecipe, 
                recipestr + "r.Remove(r.glob('/a/*'), '/b/*')\n", 
                "TestRemove")
        assert(str(err) == "Remove: No files matched: (Glob('/a/*'), '/b/*')")

    def testUnmatchedRemove2(self):
        recipestr = """
class TestRemove(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.MakeDirs('/a')
        r.Remove('/a/*', allowNoMatch = True)
"""
        self.reset()
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr, "TestRemove")
        self.logFilter.remove()
        self.assertEquals(self.logFilter.records[0],
                "warning: Remove: No files matched: '/a/*'")


class BuildLabelTest(rephelp.RepositoryHelper):
    def testBuildLabel(self):
        recipestr1 = """
class TestBuildLabel(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        assert(r.macros.buildlabel == 'localhost@rpl:linux')
        assert(r.macros.buildbranch == '/localhost@rpl:linux')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestBuildLabel")


class ConsoleHelperTest(rephelp.RepositoryHelper):
    def testConsoleHelper(self):
        recipestr1 = """
class TestConsoleHelper(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        # test default values
        r.Create('%(sbindir)s/foo', mode=0755)
        r.ConsoleHelper('%(bindir)s/foo', '%(sbindir)s/foo')
        # test non-default values
        r.Create('%(sbindir)s/bar', mode=0755)
        r.ConsoleHelper('%(bindir)s/bar', '%(sbindir)s/bar',
            consoleuser=True, timestamp=True, targetuser='<user>',
            session=False, fallback=True, noxoption='--haha',
            otherlines=['ONE=one', 'TWO=two'])
        r.Create('%(sbindir)s/blah', mode=0755)
        r.Create('blah.pam', contents='TESTING')
        r.ConsoleHelper('%(bindir)s/blah', '%(sbindir)s/blah',
            pamfile='blah.pam')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestConsoleHelper")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)

        F = file(util.joinPaths(self.workDir, '/etc/security/console.apps/foo'))
        fooC = F.readlines()
        F.close
        assert(len(fooC) == 2)

        F = file(util.joinPaths(self.workDir, '/etc/security/console.apps/bar'))
        barC = F.readlines()
        F.close
        assert(len(barC) == 7)

        F = file(util.joinPaths(self.workDir, '/etc/pam.d/foo'))
        fooP = F.readlines()
        F.close
        assert(len(fooP) == 4)

        F = file(util.joinPaths(self.workDir, '/etc/pam.d/bar'))
        barP = F.readlines()
        F.close
        assert(len(barP) == 8)

        F = file(util.joinPaths(self.workDir, '/etc/pam.d/blah'))
        blahP = F.readlines()
        F.close
        assert(len(blahP) == 1)



class ReplaceTest(rephelp.RepositoryHelper):
    def testReplace(self):
        recipestr = r"""
class TestReplace(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.addSource('bar', contents='abcdef\nabcdef\n')
        r.Create('/asdf/foo', contents='abcdef\nabcdef\n')
        r.Create('/asdf/multiReplace', contents='abcdef\nabcdef\n')
        r.Replace('(a.*f)', r'\1g', 'bar', '/asdf/foo', lines=1)
        r.Install('bar', '/asdf/bar')
        r.addSource('bar2', contents='someotherstuff')
        r.Replace('notmatching', '', 'bar2', allowNoChange=True)
        r.Replace(('a', 'b'), ('b', 'c'), ('c', 'd'), '/asdf/multiReplace')
        r.Replace(('a', 'b'), '/asdf', allowNoChange=True)

        # now test regexp line limiter
        r.Create('/bar3', contents='several1\nseveral2\nseveral3\n')
        r.Replace('several', 'many', '/bar3', lines='1$')
"""

        (built, d) = self.buildRecipe(recipestr, "TestReplace")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.verifyFile(self.workDir + '/asdf/foo', 'abcdefg\nabcdef\n')
        self.verifyFile(self.workDir + '/asdf/bar', 'abcdefg\nabcdef\n')
        self.verifyFile(self.workDir + '/asdf/multiReplace', 'ddddef\nddddef\n')

        self.verifyFile(self.workDir + '/bar3', 'many1\nseveral2\nseveral3\n')

    def testFailReplaceInit(self):
        recipestr = r"""
class TestReplace(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        # test all the init-time failures in the recipe
        try:
            r.Replace('a')
        except TypeError, err:
            if err.args[0].endswith('not enough arguments'):
                r.Create('/asdf/passed1')
        try:
            r.Replace('a', 'b')
        except TypeError, err:
            if err.args[0].endswith('not enough arguments: no file glob supplied'):
                r.Create('/asdf/passed2')
        try:
            r.Replace('a', 'b', 'foo', lines=(0,30))
        except RuntimeError, err:
            if err.args[0].endswith('Replace() line indices start at 1, like sed'):
                r.Create('/asdf/passed3')
        try:
            r.Replace('a', '\1', 'foo')
        except TypeError, msg:
            if msg.args[0].find(
                        'Illegal octal character in substitution string') != -1:
                r.Create('/asdf/passed4')

        try:
            r.Replace('aa', 'bb', '')
        except TypeError, msg:
            if msg.args[0].find(
                        'empty file path specified to Replace') != -1:
                r.Create('/asdf/passed5')

"""
        (built, d) = self.buildRecipe(recipestr, "TestReplace")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        for i in range(1,6):
            assert(os.path.exists(self.workDir + '/asdf/passed%d' % i))

    def testFailReplace(self):
        emptyRecipe = r"""
class TestReplace(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.Create('/asdf/foo', contents='abc\n')
        """

        recipe2 = emptyRecipe + 'r.Replace("a", "b", "nonexistant")'
        self.assertRaises(RuntimeError, self.buildRecipe, 
                            recipe2, "TestReplace")
        recipe3 = emptyRecipe + 'r.Replace(("nosuchpattern", "b"), "/asdf/foo")'
        self.assertRaises(RuntimeError, self.buildRecipe, 
                            recipe3, "TestReplace")


    def testReplaceNonRegular(self):
        recipe = r"""
class TestReplace(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.Create('/bar', contents='abc\n')
        r.Symlink('bar', '/foo')
        r.Replace(("foo", "bar"), "/foo")
        r.Create('build', contents='fdsa\n')
        r.Symlink('build', 'link')
        r.Replace('fdsa', 'f', 'link')
        r.Symlink('/absolute', '/dangling')
        r.Create('/absolute', contents='asd\n')
        r.Replace('asd', 'qwe', '/dangling')
"""
        self.logCheck(self.buildRecipe, (recipe, "TestReplace"), [
            'warning: /foo is not a regular file, not applying Replace',
            'warning: link is not a regular file, not applying Replace',
            'warning: /dangling is not a regular file, not applying Replace'
            ])

class DirSyntaxTest(rephelp.RepositoryHelper):
    def testDirSyntax(self):
        recipestr1 = r"""

class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/asdf/foo', mode=0700)
        r.Create('asdf/foo', mode=0777)
        r.Run('ls foo', dir='/asdf')
        r.Run('ls foo', dir='asdf')
        r.Move('/asdf/foo', 'asdf/bar')
        r.Create('/asdf/foo', mode=0700, component=':comp1')
        r.Run('ls bar', dir='asdf')
        r.Copy('asdf/bar', '/asdf/bar')
        r.Run('ls bar', dir='/asdf')
        r.Copy('/asdf/bar', 'asdf/bar2')
        r.Run('ls bar2', dir='asdf')
        r.Copy('/asdf/bar', '/asdf/bar2', component=':comp2')
        r.Remove('asdf/bar')
        r.Remove('/asdf/bar')
        r.Move('asdf/foo', '/asdf/bar')
        r.Remove('/asdf/bar')
        r.MakeDirs('/asdf/destsubdir', 'asdf/buildsubdir')
        r.MakeDirs('/asdf/destsubdir2', component=':comp3', mode=0700)
        r.Run('ls destsubdir', dir='/asdf')
        r.Run('ls buildsubdir', dir='asdf')
        r.Symlink('destsubdir', '/asdf/symlink', component=':comp4')
        r.Run('ls symlink', dir='/asdf')
        r.Symlink('buildsubdir', 'asdf/symlink')
        r.Run('ls symlink', dir='asdf')
        r.Symlink('%(builddir)s/asdf/bar2', 'asdf/buildsubdir')
        r.Run('ls buildsubdir/bar2', dir='asdf')
        r.SetModes('asdf/bar2', '/asdf/destsubdir', 0700)
        r.Run('''
mkdir %(destdir)s/builddir
cat > %(destdir)s/builddir/hello.c <<'EOF'
#include <stdio.h>

int main(void) {
    return printf("Hello, world.\\\\n");
}
EOF
                        ''')
        r.Make('hello', preMake='LDFLAGS="-static"', subDir='/builddir')
        r.Run('''
mkdir builddir
cat > builddir/hello.c <<'EOF'
#include <stdio.h>

int main(void) {
    return printf("Hello, world.\\\\n");
}
EOF
                        ''')
        r.Make('hello', preMake='LDFLAGS="-static"', subDir='builddir')
        r.Create('/builddir/configure', mode=0755, contents='''\
#!/bin/sh
echo hello > config.out
''')
        r.Configure(subDir='/builddir')
        r.Run('ls config.out', dir='/builddir')
        r.Create('builddir/configure', mode=0755, contents='''\
#!/bin/sh
echo hello > config.out
''')
        r.Configure(subDir='builddir')
        r.Run('ls config.out', dir='builddir')

        r.Install('builddir/config.out', 'builddir/config.out2')
        r.Run('ls config.out2', dir='builddir')
        r.Install('builddir/config.out', '/builddir/config.out2', component='comp5')
        r.Run('ls config.out2', dir='/builddir')
        r.Install('/builddir/config.out', '/builddir/config.out3', component='package2:comp1')
        r.Run('ls config.out3', dir='/builddir')
        r.Install('/builddir/config.out', 'builddir/config.out3')
        r.Run('ls config.out3', dir='builddir')
        r.MakeDirs('/usr/share/%%bar', mode=0700)
        r.Run('ls %(destdir)s/usr/share/%%bar')
        r.Doc('/builddir/config.*', component='package2:comp2')
        r.Run('ls config.out', dir='%(thisdocdir)s')
        r.Doc('builddir/config.*', component='package2:comp2', subdir='subdir')
        r.Run('ls config.out', dir='%(thisdocdir)s/subdir')"""

        (built, d) = self.buildRecipe(recipestr1, "Foo")
        compNames = [x[0] for x in built]
        for i in range(1, 6):
            assert('foo:comp%d' % i in compNames)
        for i in range(1, 3):
            assert('package2:comp%d' % i in compNames)

    def testFailDirSyntax(self):
        # set component on build dir file
        # set bad mode on build dir file
        emptyRecipe = r"""
class TestDirSyntax(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.Create('/asdf/foo', contents='abc\n')
        r.Create('asdf/foo', contents='abc\n')
        """
        recipe2 = emptyRecipe + 'r.Create("asdf/bar", mode=02755)'
        self.assertRaises(RuntimeError, self.buildRecipe, 
                            recipe2, "TestDirSyntax")
        recipe2 = emptyRecipe + 'r.Create("asdf/bar", component=":lib")'
        self.assertRaises(RuntimeError, self.buildRecipe, 
                            recipe2, "TestDirSyntax")
        recipe2 = emptyRecipe + 'r.Link("foo", "asdf/bar")'
        self.assertRaises(TypeError, self.buildRecipe,
                            recipe2, "TestDirSyntax")
 

class DesktopfileTest(rephelp.RepositoryHelper):
    def testDesktopfileTest1(self):
        """
        Test Desktopfile
        """
        if not os.path.exists('/usr/bin/desktop-file-validate'):
            raise testhelp.SkipTestException("desktop-file-utils is not installed")

        recipestr1 = """
class TestDesktopfile(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    #buildRequires = [ 'desktop-file-utils:runtime' ]
    
    def setup(r):
        r.Create('foo.desktop', contents='''\
[Desktop Entry]
Encoding=UTF-8
Name=Foo
Comment=Foo Bar
Exec=foo
Icon=foo
Terminal=false
Type=Application
Categories=Presentation;Java;
''')
        r.Desktopfile('foo.desktop')
        r.Create('%(datadir)s/foo/foo.png')
"""
        self.addComponent("desktop-file-utils:runtime", "1", fileContents = [
            ("/usr/bin/desktop-file-validate", "somecontent"),
            ("/usr/bin/desktop-file-install", "someothercontent")
        ])
        reportedBuildReqs = set()
        self.mock(packagepolicy.reportMissingBuildRequires, 'updateArgs',
                  lambda *args:
                    mockedUpdateArgs(args[0], reportedBuildReqs, *args[1:]))

        self.updatePkg(self.rootDir, ["desktop-file-utils:runtime"])
        self.build(recipestr1, "TestDesktopfile")
        self.assertEquals(reportedBuildReqs, set(('desktop-file-utils:runtime',)))

        reportedBuildReqs = set()
        recipestr2 = recipestr1.replace('#buildR', 'buildR')
        self.build(recipestr2, "TestDesktopfile")
        self.assertEquals(reportedBuildReqs, set())



class XInetdServiceTest(rephelp.RepositoryHelper):
    def testXInetdServiceTest1(self):
        """
        Test XInetdService
        """

        recipestr1 = """
class TestXInetdService(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.XInetdService('foo', 'the foo service')
"""
        self.assertRaises(errors.CookError, self.buildRecipe,
            recipestr1, "TestXInetdService")

        recipestr2 = """
class TestXInetdService(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.XInetdService('foo',
            'the foo service with a really long description to see'
            ' how well textwrap does at formatting the description'
            ' string into a chkconfig-compatible description string.',
            server='/usr/bin/foo', port='1079', protocol='tcp',
            otherlines=['foo=bar', 'baz=blah'])
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr2, "TestXInetdService",
                                      ignoreDeps=True)
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        f = file(self.workDir + '/etc/xinetd.d/foo')
        l = [ x[:-1] for x in f.readlines() ]
        e = ['# default: False',
             '# description: the foo service with a really long description to see \\',
             '#              how well textwrap does at formatting the description \\',
             '#              string into a chkconfig-compatible description string.',
             '',
             'service foo',
             '{',
             '\tprotocol\t= tcp',
             '\tport\t\t= 1079',
             '\tserver\t\t= /usr/bin/foo',
             '\twait\t\t= no',
             '\tdisable\t\t= yes',
             '\tfoo=bar',
             '\tbaz=blah',
             '}'
            ]
        for found, expected in zip(l, e):
            assert(found == expected)



class MakeDevicesTest(rephelp.RepositoryHelper):
    def testMakeDevicesTest1(self):
        recipestr1 = """
class TestMakeDevices(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.MakeDevices('/dev/foo', 'c', 1, 2, 'root', 'root')
        # add a dev to a libdir to make sure it works (CNY-2692)
        r.MakeDevices('/lib/dev/foo', 'c', 1, 2, 'root', 'root')
        r.MakeDevices('/lib64/dev/foo', 'c', 1, 2, 'root', 'root')
"""
        built, d = self.buildRecipe(recipestr1, "TestMakeDevices")





class RunTest(rephelp.RepositoryHelper):
    def testRunTest1(self):
        recipestr1 = """
class TestRun(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/asdf/ajkl', contents='bar')
        r.Run('if cat /asdf/ajkl ; then : else exit 1 ; fi')
        r.Run('cat /asdf/ajkl | grep bar', filewrap=True)
        r.Run('cat /asdf/ajkl | grep bar', wrapdir='/asdf')
        r.Run('if cat /asdf/ajkl ; then : else exit 1 ; fi', wrapdir='/fdsa')
"""
        built, d = self.buildRecipe(recipestr1, "TestRun")


    def testRunReadFromStdin(self):
        recipestr = r"""
class TestPackage(PackageRecipe):
    name = 'test'
    version = '1'

    clearBuildReqs()

    def setup(r):
        r.Create("/usr/foo", contents="some text\n")
        r.Run("cat")
"""
        os.chdir(self.workDir)
        self.newpkg("test")
        os.chdir('test')

        self.writeFile('test.recipe', recipestr)
        self.addfile('test.recipe')
        self.commit()

        self.cookFromRepository('test', logBuild=True)
        # Same deal, without logging - should still work
        self.cookFromRepository('test', logBuild=False)


class PythonSetupTest(rephelp.RepositoryHelper):
    def testPythonSetupS0(self):
        recipestr = r"""
class Test(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('setup.py', contents='\n'.join((
            '#import setuptools',
            'file("%(destdir)s/setuptools", "w").close()',
            '')))
        r.PythonSetup()
"""
        # missing buildreq, but test that it runs anyway
        trv = self.build(recipestr, "Test")
        self.verifyPackageFileList(trv, ['/setuptools'])

    def testPythonSetupS1(self):
        recipestr = r"""
class Test(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('setup.py', contents='\n'.join((
            '#from setuptools import setup',
            'file("%(destdir)s/setuptools", "w").close()',
            '')))
        r.PythonSetup()
"""
        # missing buildreq, but test that it runs anyway
        trv = self.build(recipestr, "Test")
        self.verifyPackageFileList(trv, ['/setuptools'])

    def testPythonSetupD(self):
        try:
            __import__('setuptools')
        except ImportError:
            raise testhelp.SkipTestException('Missing python-setuptools package')
        recipestr = r"""
class Test(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('blah/setupfoo.py', contents='\n'.join((
            '#import disttools',
            'file("%(destdir)s/disttools", "w").close()',
            '')))
        r.PythonSetup(setupName='setupfoo.py', dir='blah', action='whatever',
                      rootDir='')
"""
        # missing buildreq, but test that it runs anyway
        trv = self.build(recipestr, "Test")
        self.verifyPackageFileList(trv, ['/disttools'])

    def testPythonSetupPyVer(self):
        recipestr = r"""
class Test(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.macros.pyver = '%(pversion)s'
        r.Create('setup.py', contents='\n'.join((
            '#from setuptools import setup',
            'file("%%(destdir)s/setuptools", "w").close()',
            '')))
        r.PythonSetup()
"""
        # we should fail with a bogus python version
        self.assertRaises(RuntimeError, self.build, recipestr %
                          {'pversion':'bogus'}, "Test")
        # we should succeed with a the currently running  python version
        trv = self.build(recipestr % {'pversion': sys.version[0:3]}, "Test")
        assert trv is not None


class TestMakeFIFO(rephelp.RepositoryHelper):
    """test CNY-1597"""
    def testMakeFIFOBuildAction(self):
        recipestr = """
class FIFO(PackageRecipe):
    name = 'fifo'
    version = '6.2.2.4'
    clearBuildReqs()
    def setup(r):
        r.MakeFIFO('/path/to/my/spiffy/named/pipe')
        r.Run('test -p %(destdir)s/path/to/my/spiffy/named/pipe')
        r.MakeFIFO('this/is/a/path/relative/to/builddir/pipe')
        r.Run('test -p this/is/a/path/relative/to/builddir/pipe')
"""
        self.buildRecipe(recipestr, 'FIFO')

    def testPythonSetupNonPure(self):
        try:
            __import__('setuptools')
        except ImportError:
            raise testhelp.SkipTestException('Missing python-setuptools package')
        recipestr = r"""
class Test(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.macros.lib = 'lib64'
        # test pre-existing python-specific multilib breakage
        r.Create('%(prefix)s/lib/python2.4/site-packages/a')
        r.Create('%(prefix)s/lib64/python2.4/site-packages/b')
        # test multilib breakage that purePython=False could help
        r.Create('setup.py', contents='\n'.join((
            '#import setuptools',
            'file("%(destdir)s%(prefix)s/lib/python2.4/site-packages/foo", "w").close()',
            'file("%(destdir)s%(prefix)s/lib64/python2.4/site-packages/bar", "w").close()',
            '')))
        r.PythonSetup()
"""
        # missing buildreq, but test that it runs anyway
        self.logFilter.add()
        self.assertRaises(policy.PolicyError, self.buildRecipe,
                          recipestr, "Test")
        self.logFilter.remove()
        assert(os.path.exists(util.joinPaths(self.buildDir,
               'test/_ROOT_/usr/lib/python2.4/site-packages/foo')))
        assert(os.path.exists(util.joinPaths(self.buildDir,
               'test/_ROOT_/usr/lib64/python2.4/site-packages/bar')))
        self.logFilter.regexpCompare([
            r'error: Python and object files detected in different directories before PythonSetup\(\) instance on line 18',
            r'error: Python and object files detected in different directories on line 18; call all instances of PythonSetup\(\) with the purePython=False argument',
            'error: NonMultilibComponent: .*',
            ])



class SetModesTest(rephelp.RepositoryHelper):
    def testSetModesTest1(self):
        """
        Test _PutFiles
        """

        recipestr = """
class TestSetModes(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/usr/bin/foo')
        r.SetModes('/usr/bin/foo', 04751)
        r.Create('/foo/bar')
        r.SetModes('/foo/bar', 07755)
        r.SetModes('/foo/bar', 0755)
"""
        built, d = self.buildRecipe(recipestr, "TestSetModes")
        permMap = { '/usr/bin/foo': 04751,
                    '/foo/bar': 0755 }
        repos = self.openRepository()
        for troveName, troveVersion, troveFlavor in built:
            troveVersion = versions.VersionFromString(troveVersion)
            trove = repos.getTrove(troveName, troveVersion, troveFlavor)
            for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
                trove.getName(), trove.getVersion(), trove.getFlavor(),
                withFiles=True):
                self.assertEquals(fileObj.inode.perms(), permMap[path])

    def testSetModesTest2(self):
        """
        Test _PutFiles
        """

        recipestr = """
class TestSetModes(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/usr/bin/foo')
        r.SetModes('/usr/bin/foo', 755)
"""
        self.logCheck(self.buildRecipe, (recipestr, "TestSetModes"),
            'warning: odd permission 1363 for path /usr/bin/foo, correcting to 0755: add initial "0"?')


class TestIncludeLicense(rephelp.RepositoryHelper):
    """test CNY-1656"""
    def testIncludeLicenseBuildAction(self):
        try:
            socket.gethostbyname('www.rpath.com')
        except:
            raise testhelp.SkipTestException('Test requires networking')

        recipestr= """
class License(PackageRecipe):
    name = 'license'
    version = '1.5.7'
    clearBuildReqs()
    def setup(r):
        r.addSource('CPL-1.0')
        r.IncludeLicense(('CPL-1.0', 'CPL-1.0'))
        # sha1sum of the relevent license
        r.Run('test -f %(destdir)s/%(datadir)s/known-licenses/CPL-1.0/7d2ea178c5858c731bf8a026aeb880b27698b924')
"""
        self.buildRecipe(recipestr, 'License')



class EnvironmentTest(rephelp.RepositoryHelper):
    def testEnvironment(self):
        recipestr = """
class TestEnvironment(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Environment('FOOBAR', 'BAZ')
        r.Run('echo "$FOOBAR" > baz')
        r.Create('foobar', contents='BAZ')
        r.Run('cmp foobar baz')
"""
        self.buildRecipe(recipestr, "TestEnvironment")


class TestSharedLibraryPolicy(rephelp.RepositoryHelper):
    """test CNP-45"""
    def testSharedLibraryPolicy(self):
        recipestr = """
class BadRecipe(AutoPackageRecipe):
    name = 'badrecipe'
    version = '3'
    clearBuildReqs()
    def setup(r):
        r.Create('/path/to/a/file')
        r.SharedLibrary(subtrees = '/path/to/a/file')
"""
        self.logFilter.add()
        self.assertRaises(policy.PolicyError, self.buildRecipe,
                recipestr, "BadRecipe")
        self.logFilter.remove()
        assert('error: NormalizeLibrarySymlinks: The subtrees= argument takes directories only; /path/to/a/file is not a directory') 


class TestMakeFIFO(rephelp.RepositoryHelper):
    """test CNY-1597"""
    def testMakeFIFOBuildAction(self):
        recipestr = """
class FIFO(PackageRecipe):
    name = 'fifo'
    version = '6.2.2.4'
    clearBuildReqs()
    def setup(r):
        r.MakeFIFO('/path/to/my/spiffy/named/pipe')
        r.Run('test -p %(destdir)s/path/to/my/spiffy/named/pipe')
        r.MakeFIFO('this/is/a/path/relative/to/builddir/pipe')
        r.Run('test -p this/is/a/path/relative/to/builddir/pipe')
"""
        self.buildRecipe(recipestr, 'FIFO')

class TestReportMissingBuildReqs(rephelp.RepositoryHelper):
    def testActionSuggestsBuildReqs(self):
        # First, add a trove that provides make
        self.addComponent("fakemake:runtime", "1", fileContents = [
            ("/usr/bin/make", "somecontent")])
        self.updatePkg(self.rootDir, ["fakemake:runtime"])
        # test for Run; work whether usrmove applied or not
        self.addComponent("true:runtime", "2", fileContents = [
            ("/bin/true", "#!/bin/sh\nexit 0"),
            ("/usr/bin/true", "#!/bin/sh\nexit 0") ])
        self.updatePkg(self.rootDir, ["true:runtime"])
        recipestr = """
class ActionSuggests(PackageRecipe):
    name = "suggests"
    version = "1"
    clearBuildReqs()
    placeholder = 1
    def setup(r):
        r.Create("Makefile", contents = '\\n'.join([
            "datadir = $(DESTDIR)/usr/share",
            "INSTALL = $(datadir)/suggests/suggests.txt",
            "all:",
            "install:",
            "\\tinstall -d $(basename $(INSTALL))",
            "\\techo 1 > $(INSTALL)",
        ]))
        r.Make()
        # MakePathsInstall and MakeInstall do the same thing in this case,
        # but we want to exercise both classes
        r.MakePathsInstall()
        r.MakeInstall()

        # test MakeParallelSubdir too, it doesn't really make any difference
        r.MakeParallelSubdir()
        r.Run('ENVVAR="a b"  true') # CNY-3224
"""
        reportedBuildReqs = set()
        self.mock(packagepolicy.reportMissingBuildRequires, 'updateArgs',
                  lambda *args:
                    mockedUpdateArgs(args[0], reportedBuildReqs, *args[1:]))
        self.build(recipestr, 'ActionSuggests')
        self.assertEquals(reportedBuildReqs, set(('fakemake:runtime',
                                                  'true:runtime')))

        # Same deal, with buildRequires added
        recipestr2 = recipestr.replace("placeholder = 1",
           "buildRequires = ['fakemake:runtime', 'true:runtime']")

        reportedBuildReqs.clear()
        self.build(recipestr2, 'ActionSuggests')
        self.assertEquals(reportedBuildReqs, set())

    def testActionSuggestsBuildReqs2(self):
        # First, add a trove that provides tar and gz
        fakeTroves = ['tar', 'gzip']
        for comp in fakeTroves:
            self.addComponent("fake%s:runtime" % comp, "1",
                fileContents = [ ("/bin/%s" % comp, "%scontent" % comp)])
        def checkPath(prog):
            return '/bin/' + prog
        self.mock(util, 'checkPath', checkPath)
        self.updatePkg(self.rootDir, ["fake%s:runtime" % x for x in fakeTroves])
        recipestr = """
class ActionSuggests(PackageRecipe):
    name = "suggests"
    version = "1"
    clearBuildReqs()
    placeholder = 1
    def setup(r):
        r.addArchive("foo-1.0.tar.gz", dir="/usr/share/foo/")
"""
        reportedBuildReqs = set()
        self.mock(packagepolicy.reportMissingBuildRequires, 'updateArgs',
                  lambda *args:
                    mockedUpdateArgs(args[0], reportedBuildReqs, *args[1:]))
        self.build(recipestr, 'ActionSuggests')
        self.assertEquals(reportedBuildReqs, set(['fakegzip:runtime',
                                                  'faketar:runtime']))

        # Same deal, with buildRequires added
        recipestr2 = recipestr.replace("placeholder = 1",
                   "buildRequires = ['fakegzip:runtime', 'faketar:runtime']")

        reportedBuildReqs.clear()
        self.build(recipestr2, 'ActionSuggests')
        self.assertEquals(reportedBuildReqs, set())
