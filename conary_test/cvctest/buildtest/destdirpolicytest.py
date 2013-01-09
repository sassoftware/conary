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
import gzip
import os
import re
import stat
import StringIO
import sys
from testrunner import testhelp
import tempfile


from conary.deps import deps

from conary.lib import fixedglob
from conary.lib import log
from conary.lib import magic
from conary.lib import util
from conary_test import rephelp
from conary.build import action, cook
from conary.build import macros, use
from conary.build import policy, packagepolicy
from conary.build import recipe
from conary import versions

class FixDirModesTest(rephelp.RepositoryHelper):
    def testFixDirModesTest1(self):
        """
        Verify that odd modes on directories do not cause Conary trouble.
        """
        recipestr1 = """
class TestFixDirModes(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        # imitate make install with weird mode:
        # should not break build, but should not show up in directory list
        # as an owned directory
        self.Run('mkdir %(destdir)s/foo; chmod 0111 %(destdir)s/foo')
        # this should be an owned directory with mode 011
        self.MakeDirs('/bar', mode=0111)
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestFixDirModes")
        self.updatePkg(self.workDir, built[0][0], built[0][1])
        self.assertRaises(OSError, os.stat, self.workDir + os.sep + 'foo')
        s = os.stat(self.workDir + os.sep + 'bar')[stat.ST_MODE]
        assert((s & 0777) == 0111)
        # keep resetWork() from complaining in util.rmtree()
        os.chmod(self.workDir + os.sep + 'bar', 0700)


class AutoDocTest(rephelp.RepositoryHelper):
    def testAutoDocTest1(self):
        """
        Test automatic documentation finding...
        """
        recipestr0 = """
class TestAutoDoc(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.Create('README')
"""
        recipestr1 = """
class TestAutoDoc(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.Create('README')
        r.Create('NEWS')
        r.Create('NEWS.foo')
        r.Create('HACKING')
        r.Create('COPYING.lib')
        r.Create('foo/README')
        r.Create('foo/LICENSE.lib')
        r.Create('foo/COPYING')
        r.Create('LICENSE/LICENSE')
        r.Create('asdf/fdsa')
        r.Symlink('asdf', 'READMEs')
        r.Install('README', '%(thisdocdir)s/', mode=0400)
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr0, "TestAutoDoc")
        # should be empty
        assert(not built)
        (built, d) = self.buildRecipe(recipestr1, "TestAutoDoc")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.assertRaises(OSError, os.stat,
            self.workDir+'/usr/share/doc/test-0/NEWS.foo')
        # none of these should raise OSError...
        os.stat(self.workDir+'/usr/share/doc/test-0/README')
        os.stat(self.workDir+'/usr/share/doc/test-0/NEWS')
        os.stat(self.workDir+'/usr/share/doc/test-0/HACKING')
        os.stat(self.workDir+'/usr/share/doc/test-0/COPYING.lib')
        os.stat(self.workDir+'/usr/share/doc/test-0/foo/README')
        os.stat(self.workDir+'/usr/share/doc/test-0/foo/LICENSE.lib')
        os.stat(self.workDir+'/usr/share/doc/test-0/foo/COPYING')



class RemoveNonPackageFilesTest(rephelp.RepositoryHelper):
    def testRemoveNonPackageFilesTest1(self):
        """
        Make sure that "extra" libraries are removed, and nothing
        else.
        """
        recipestr1 = """
class TestRemoveNonPackageFiles(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Create('/foo.la', '/foo.la.foo')
        self.Create('%(libdir)s/python/site-packages/foo.a{,.so,.py}')
        self.Create('%(libdir)s/python/site-packages/pa/foo.py')
        self.Create('/foo{~,.orig}', '/bar{~,.orig}')
        self.Create('/.#asdf')
        self.Create('/tmp/asdf')
        self.Create('/var/tmp/asdf')
        self.Create('%(servicedir)s/www/tmp/somefile.xml')
        self.RemoveNonPackageFiles(exceptions='/bar~')
        self.RemoveNonPackageFiles(exceptions='/bar\..*')
"""
        self.reset()
        m = macros.Macros()
        m.update(recipe.loadMacros(self.cfg.defaultMacros))
        m.update(use.Arch._getMacros())
        (built, d) = self.buildRecipe(recipestr1, "TestRemoveNonPackageFiles")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.assertRaises(OSError, os.stat,
            self.workDir+'foo.la')
        self.assertRaises(OSError, os.stat,
            self.workDir+'%(libdir)s/python/site-packages/foo.a' %m)
        # none of these should raise OSError...
        os.stat(self.workDir+'/foo.la.foo')
        os.stat(self.workDir+'/%(libdir)s/python/site-packages/foo.a.so' %m)
        os.stat(self.workDir+'/%(libdir)s/python/site-packages/foo.a.py' %m)
        os.stat(self.workDir+'/%(libdir)s/python/site-packages/pa/foo.py' %m)
        os.stat(self.workDir+'%(servicedir)s/www/tmp/somefile.xml' %m)
        self.assertRaises(OSError, os.stat, self.workDir+'/foo~')
        self.assertRaises(OSError, os.stat, self.workDir+'/foo.orig')
        self.assertRaises(OSError, os.stat, self.workDir+'/.#asdf')
        self.assertRaises(OSError, os.stat, self.workDir+'/tmp/asdf')
        self.assertRaises(OSError, os.stat, self.workDir+'/var/tmp/asdf')
        # none of these should raise OSError...
        os.stat(self.workDir+'/bar~')
        os.stat(self.workDir+'/bar.orig')




class FixupMultilibPathsTest(rephelp.RepositoryHelper):
    def testFixupMultilibPathsTest1(self):
        """
        Test that libraries in wrong paths on multilib platforms get
        noticed and moved.
        """
        recipestr1 = """
class TestFixupMultilibPaths(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.MakeDirs('/usr/lib')
        r.Run("echo 'void foo(void) { return ; }' > foo.c")
        r.Run('make CFLAGS="-g -fPIC" foo.o')
        r.Run('ar r libfoo.a foo.o')
        r.Run('gcc -g -shared -Wl,-soname,libfoo.so.0 -o libfoo.so.0.0 foo.o -nostdlib')
        r.Install('libfoo*', '/usr/lib/')
        r.Symlink('libfoo.so.0.0', '/usr/lib/libfoo.so.0')
        r.Symlink('/usr/lib/libfoo.so.0.0', '/usr/lib/libfoo.so')
        r.Symlink('../../usr/lib/libfoo.so.0.0', '/usr/lib/libgork.so')

        # and don't make ldconfig noises
        r.Create('true.c', contents='int main() { return 0; }')
        r.Run('%(cc)s %(ldflags)s -static -o true true.c')
        r.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
        r.ComponentSpec('lib', '.*')

        r.Create('/usr/lib/foo.a')
"""
        self.reset()
        self.logFilter.ignore('warning: EnforceSonameBuildRequirements:.*')
        self.logFilter.ignore('warning: Provides:.*')
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr1, "TestFixupMultilibPaths",
            macros={'lib': 'lib64'})
        self.logFilter.remove()
        self.logFilter.compare((
            'warning: back-referenced symlink ../../usr/lib/libfoo.so.0.0 should probably be replaced by absolute symlink (start with "/" not "..")',
            'warning: FixupMultilibPaths: file /usr/lib/libfoo.a found in wrong directory, attempting to fix...',
            'warning: FixupMultilibPaths: file /usr/lib/libfoo.so.0.0 found in wrong directory, attempting to fix...',
            'warning: FixupMultilibPaths: file /usr/lib/libfoo.so.0 found in wrong directory, attempting to fix...',
            'warning: FixupMultilibPaths: file /usr/lib/libfoo.so found in wrong directory, attempting to fix...',
            'warning: FixupMultilibPaths: file /usr/lib/libgork.so found in wrong directory, attempting to fix...',
            'warning: FixupMultilibPaths: non-object file with library name /usr/lib/foo.a',
        ))
        self.mimicRoot()
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.realRoot()
        self.assertRaises(OSError, os.stat,
            self.workDir+os.sep+'usr/lib/libfoo.a')
        self.assertRaises(OSError, os.stat,
            self.workDir+os.sep+'usr/lib/libfoo.so.0.0')
        # none of these should raise OSError...
        os.stat(self.workDir+'/usr/lib64/libfoo.so.0.0')
        os.stat(self.workDir+'/usr/lib64/libfoo.a')
        os.stat(self.workDir+'/usr/lib/foo.a')


        recipestr2 = """
class TestFixupMultilibPaths(PackageRecipe):
    # this one actually installs multilib files...
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.addArchive('multilib-sample.tar.bz2', dir='%(prefix)s')
        r.Symlink('libffi-2.00-beta.so', '/usr/lib/libffi.so')
        r.Symlink('libffi-2.00-beta.so', '/usr/lib64/libffi.so')

        r.Strip(exceptions='.*')
"""
        self.reset()
        self.logFilter.ignore('warning: Provides:.*')
        self.logFilter.add()
        # %(lib)s must be lib64 to enable FixupMultilibPaths policy
        # even though this recipe doesn't reference it explicitly
        (built, d) = self.buildRecipe(recipestr2, "TestFixupMultilibPaths",
            macros={'lib': 'lib64'})
        self.logFilter.remove()
        self.logFilter.compare((
            'warning: CheckSonames: /usr/lib64/libffi-2.00-beta.so is not a symlink but probably should be a link to libffi-2.00-beta.so.something',
            'warning: CheckSonames: /usr/lib/libffi-2.00-beta.so is not a symlink but probably should be a link to libffi-2.00-beta.so.something',
        ))


    def testFixupMultilibPathsTest2(self):
        recipestr1 = """
class TestFixupMultilibPaths(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.MakeDirs('/usr/lib')
        r.Run("echo 'void foo(void) { return ; }' > foo.c")
        r.Run('make CFLAGS="-g -fPIC" foo.o')
        r.Run('ar r libfoo.a foo.o')
        r.Run('gcc -g -shared -Wl,-soname,libfoo.so.0 -o libfoo.so.0.0 foo.o -nostdlib')
        r.Install('libfoo.a', '/usr/lib/subdir/')

        # and don't make ldconfig noises
        r.Create('true.c', contents='int main() { return 0; }')
        r.Run('%(cc)s %(ldflags)s -static -o true true.c')
        r.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
"""
        self.reset()
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr1, "TestFixupMultilibPaths",
            macros={'lib': 'lib64'})
        self.logFilter.remove()
        self.logFilter.compare('warning: FixupMultilibPaths: file /usr/lib/subdir/libfoo.a found in wrong directory, attempting to fix...')
        self.mimicRoot()
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.realRoot()
        assert(os.path.exists(self.workDir + '/usr/lib64/subdir/libfoo.a'))

    def testFixupMultilibPathsPythonScript(self):
        raise testhelp.SkipTestException('CNP-121 waiting for CNP-136')
        recipestr = """
class PythonLocationTest(PackageRecipe):
    name = "test"
    version = "1"

    clearBuildReqs()

    def setup(r):
        r.Create('/usr/lib/python2.4/site-packages/foo/foo.py',
                contents = "#!/usr//bin/python")
        r.CompilePython('/usr/lib/python2.4/site-packages/foo/')
"""
        (built, d) = self.buildRecipe(recipestr, "PythonLocationTest",
                macros = {'libdir': '/usr/lib64', 'lib': 'lib64'})
        repos = self.openRepository()
        nvf = repos.findTrove(self.cfg.buildLabel, built[0])
        client = self.getConaryClient()
        fileDict = client.getFilesFromTrove(*nvf[0])
        self.assertEquals(sorted(fileDict.keys()),
                ['/usr/lib64/python2.4/site-packages/foo/foo.py',
                 '/usr/lib64/python2.4/site-packages/foo/foo.pyc',
                 '/usr/lib64/python2.4/site-packages/foo/foo.pyo'])

    def testPackageSpecMultilibPaths(self):
        raise testhelp.SkipTestException('CNP-121 waiting for CNP-136')
        recipestr = """
class PythonLocationTest(PackageRecipe):
    name = "test"
    version = "1"

    clearBuildReqs()

    def setup(r):
        # prove that FixupMultilibPaths remembers to properly
        # update the package manifest wrt to package spec
        r.Create('/usr/lib/python2.4/site-packages/foo/foo.py',
                contents = "#!/usr//bin/python", package = 'splat')
        # we kind of want the compiled python files to end up in the same
        # package
        r.CompilePython('/usr/lib/python2.4/site-packages/foo/',
                package = 'splat')
"""
        (built, d) = self.buildRecipe(recipestr, "PythonLocationTest",
                macros = {'libdir': '/usr/lib64', 'lib': 'lib64'})
        # prove component is correct
        self.assertEquals(built[0][0], 'splat:python')

        # prove files got moved
        repos = self.openRepository()
        nvf = repos.findTrove(self.cfg.buildLabel, built[0])
        client = self.getConaryClient()
        fileDict = client.getFilesFromTrove(*nvf[0])
        self.assertEquals(sorted(fileDict.keys()),
                ['/usr/lib64/python2.4/site-packages/foo/foo.py',
                 '/usr/lib64/python2.4/site-packages/foo/foo.pyc',
                 '/usr/lib64/python2.4/site-packages/foo/foo.pyo'])

class ExecutableLibrariesTest(rephelp.RepositoryHelper):
    def testExecutableLibrariesTest1(self):
        """
        Test that libraries without executable bits set will be warned
        and fixed, and the linkage between SharedLibrary and
        ExecutableLibraries
        """
        recipestr1 = """
class TestExecutableLibraries(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Run("echo 'void foo(void) { return ; }' > foo.c")
        self.Run('make CFLAGS="-g -fPIC" foo.o')
        self.Run('ar r libfoo.a foo.o')
        self.Run('gcc -g -shared -Wl,-soname,libfoo.so.0 -o libfoo.so.0.0 foo.o')
        self.Install('libfoo.so*', '/usr/foo/', mode=0644)
        self.SharedLibrary(subtrees='/usr/foo')
"""
        self.reset()
        self.logFilter.ignore('warning: EnforceSonameBuildRequirements: The following dependencies are not resolved within the package or in the system database:.*')
        self.logFilter.add()
        trove = self.build(recipestr1, "TestExecutableLibraries", logLevel=log.INFO)
        self.logFilter.remove()
        self.logFilter.compare((
            'warning: ExecutableLibraries: non-executable library /usr/foo/libfoo.so.0.0, changing to mode 0755',
            '+ NormalizeLibrarySymlinks: ldconfig added the following new files in /usr/foo: libfoo.so.0',
        ), allowMissing=True)
        repos = self.openRepository()
        for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
            trove.getName(), trove.getVersion(), trove.getFlavor(),
            withFiles=True):
            if path == '/usr/foo/libfoo.so.0.0':
                assert("shlib" in fileObj.tags)

    def testExecutableLibrariesTest2(self):
        """
        Test that shared lib dirs listed in
        %(destdir)s/etc/ld.so.conf.d/*.conf will be honored.
        """
        recipestr1 = """
class TestExecutableLibraries(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        self.Run("echo 'void foo(void) { return ; }' > foo.c")
        self.Run('make CFLAGS="-g -fPIC" foo.o')
        self.Run('ar r libfoo.a foo.o')
        self.Run('gcc -g -shared -Wl,-soname,libfoo.so.0 -o libfoo.so.0.0 foo.o')
        self.Install('libfoo.so*', '/usr/foo/', mode=0644)
        self.Create('/etc/ld.so.conf.d/foo.conf', contents = '/usr/foo')
"""
        self.reset()
        self.logFilter.ignore('warning: EnforceSonameBuildRequirements: The following dependencies are not resolved within the package or in the system database:.*')
        self.logFilter.add()
        trove = self.build(recipestr1, "TestExecutableLibraries", logLevel=log.INFO)
        self.logFilter.remove()
        self.logFilter.compare((
                'warning: ExecutableLibraries: non-executable library /usr/foo/libfoo.so.0.0, changing to mode 0755',
                '+ NormalizeLibrarySymlinks: ldconfig added the following new files in /usr/foo: libfoo.so.0',
                ), allowMissing=True)
        repos = self.openRepository()
        for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
            trove.getName(), trove.getVersion(), trove.getFlavor(),
            withFiles=True):
            if path == '/usr/foo/libfoo.so.0.0':
                assert("shlib" in fileObj.tags)

    def testExecutableLibrariesTest3(self):
        """
        Test that shared lib dirs listed in /etc/ld.so.conf.d/*.conf will
        be honored if they are managed.
        """
        recipestr1 = """
class TestExecutableLibraries(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        self.Run("echo 'void foo(void) { return ; }' > foo.c")
        self.Run('make CFLAGS="-g -fPIC" foo.o')
        self.Run('ar r libfoo.a foo.o')
        self.Run('gcc -g -shared -Wl,-soname,libfoo.so.0 -o libfoo.so.0.0 foo.o')
        self.Install('libfoo.so*', '/usr/foo/', mode=0644)
"""
        self.reset()
        ldConfPath = os.path.join(os.path.sep, 'etc', 'ld.so.conf.d',
                'foo.conf')
        self.addComponent('fooconf:runtime',
                fileContents = [(ldConfPath, '/usr/foo')])
        self.updatePkg('fooconf:runtime')

        self.logFilter.ignore('warning: EnforceSonameBuildRequirements: The following dependencies are not resolved within the package or in the system database:.*')
        self.logFilter.add()
        trove = self.build(recipestr1, "TestExecutableLibraries", logLevel=log.INFO)
        self.logFilter.remove()
        self.logFilter.compare((
                'warning: ExecutableLibraries: non-executable library /usr/foo/libfoo.so.0.0, changing to mode 0755',
                '+ NormalizeLibrarySymlinks: ldconfig added the following new files in /usr/foo: libfoo.so.0',
                ), allowMissing=True)
        repos = self.openRepository()
        for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
            trove.getName(), trove.getVersion(), trove.getFlavor(),
            withFiles=True):
            if path == '/usr/foo/libfoo.so.0.0':
                assert("shlib" in fileObj.tags)

    def testExecutableLibrariesTest4(self):
        """
        Test that shared lib dirs listed in /etc/ld.so.conf.d/*.conf will
        be ignored if they are not managed.
        """
        recipestr1 = """
class TestExecutableLibraries(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        self.Run("echo 'void foo(void) { return ; }' > foo.c")
        self.Run('make CFLAGS="-g -fPIC" foo.o')
        self.Run('ar r libfoo.a foo.o')
        self.Run('gcc -g -shared -Wl,-soname,libfoo.so.0 -o libfoo.so.0.0 foo.o')
        self.Install('libfoo.so*', '/usr/foo/', mode=0644)
"""
        self.reset()
        ldConfPath = os.path.join(self.cfg.root, 'etc', 'ld.so.conf.d',
                'foo.conf')
        util.mkdirChain(os.path.dirname(ldConfPath))
        f = open(ldConfPath, 'w')
        f.write('/usr/foo')
        f.close()

        self.logFilter.ignore('warning: EnforceSonameBuildRequirements: The following dependencies are not resolved within the package or in the system database:.*')
        self.logFilter.add()
        trove = self.build(recipestr1, "TestExecutableLibraries", logLevel=log.INFO)
        self.logFilter.remove()
        self.assertFalse('+ SharedLibrary: /usr/foo/libfoo.so.0.0' \
                in self.logFilter.records,
                "Conary honored a non-managed file for AutoSharedLibrary")

class ReadableDocsTest(rephelp.RepositoryHelper):
    def testReadableDocs(self):
        recipestr1 = """
class TestReadableDocs(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('%(thisdocdir)s/README', mode=0600)
        r.Create('%(thisdocdir)s/executable', mode=0700)
"""
        self.reset()
        self.logFilter.add()
        trove = self.build(recipestr1, "TestReadableDocs")
        self.logFilter.remove()
        self.logFilter.compare((
            'warning: ReadableDocs: documentation file /usr/share/doc/test-0/README not group and world readable, changing to mode 0644',
            'warning: ReadableDocs: documentation file /usr/share/doc/test-0/executable not group and world readable, changing to mode 0755',
        ))
        repos = self.openRepository()
        for fileid, path, fileId, version, fileObj in repos.iterFilesInTrove(
            trove.getName(), trove.getVersion(), trove.getFlavor(),
            withFiles=True):
            if path == '/usr/share/doc/test-0/README': 
                assert(fileObj.inode.perms() == 0644)
            elif path == '/usr/share/doc/test-0/README-TOO': 
                assert(fileObj.inode.perms() == 0755)



class StripTest(rephelp.RepositoryHelper):
    def testStripBasic(self):
        """
        Test to make sure that all ELF objects are stripped as appropriate.

        XXX: test archive objects when we have magic detection for those
        as well.
        """
        recipestr1 = """
class TestStrip(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('foo.c', contents="int main(void) { return 0 ; }")
        r.Create('foo2.c', contents="int main(void) { return 0 ; }")
        r.Make('CFLAGS="-g -static" LDFLAGS="-g -static" foo')
        r.Make('CFLAGS="-g -static" LDFLAGS="-g -static" foo2')
        r.Remove('foo2.c')
        r.Run('cp foo bar; strip bar')
        r.Create('libblah.c', contents="int foo(void) { return 1 ; }")
        r.Run('%(cc)s -g -fPIC -c libblah.c')
        r.Run('%(cc)s -g -shared -Wl,-soname,libblah.so.0 -o libblah.so.0 libblah.o')
        r.Install('libblah.so.0', '%(libdir)s/')
        r.Install('libblah.so.0', '%(prefix)s/lib/')
        r.Install('foo2', '%(bindir)s/', mode=0555)
        r.Install('foo', '%(bindir)s/', mode=0555)
        r.Install('foo', '%(bindir)s/foo.unstripped')
        r.Install('bar', '%(bindir)s/')
        r.Symlink('bar', '%(bindir)s/baz')
        r.Strip(exceptions='%(bindir)s/foo.unstripped')
        # we can't easily test with this policy active...
        r.FixupMultilibPaths(exceptions='.*')
        r.Requires(exceptions='.*')

        # and don't make ldconfig noises
        r.Create('true.c', contents='int main() { return 0; }')
        r.Run('%(cc)s %(ldflags)s -static -o true true.c')
        r.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
        r.ComponentSpec('lib', '.*')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestStrip",
            macros={'lib': 'lib64'})
        self.mimicRoot()
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.realRoot()
        f = magic.magic('/usr/bin/foo', self.workDir)
        u = magic.magic('/usr/bin/foo.unstripped', self.workDir)
        b = magic.magic('/usr/bin/bar', self.workDir)
        l3 = magic.magic('/usr/lib/libblah.so.0', self.workDir)
        l6 = magic.magic('/usr/lib64/libblah.so.0', self.workDir)
        assert(f.name == 'ELF')
        assert(u.name == 'ELF')
        assert(b.name == 'ELF')
        assert(l3.name == 'ELF')
        assert(l6.name == 'ELF')
        # eu-strip -f (for debuginfo) marks it stripped, (eu-)strip -g doesn't
        # so we can't depend on this for things that aren't stripped
        # explicitly
        #assert(f.contents['stripped'] == 1)
        assert(f.contents['hasDebug'] == 0)
        assert(u.contents['stripped'] == 0)
        assert(u.contents['hasDebug'] == 1)
        assert(b.contents['stripped'] == 1)
        assert(b.contents['hasDebug'] == 0)
        #assert(l3.contents['stripped'] == 1)
        assert(l3.contents['hasDebug'] == 0)
        #assert(l6.contents['stripped'] == 1)
        assert(l6.contents['hasDebug'] == 0)

    def testStripMissing(self):
        'CNP-143'
        recipestr = """
class TestStrip(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.macros.strip = 'eu-strip-missing'
        r.macros.strip_archive = 'strip-missing'
        r.Create('foo.c', contents="int main(void) { return 0 ; }")
        r.Make('CFLAGS="-g -static" LDFLAGS="-g -static" foo')
        r.Install('foo', '%(bindir)s/', mode=0755)

        r.Create('libblah.c', contents="int foo(void) { return 1 ; }")
        r.Make('libblah.o')
        r.Run('ar r libblah.a libblah.o')
        r.Install('libblah.a', '/usr/lib/')
        # we can't easily test with this policy active...
        r.FixupMultilibPaths(exceptions='.*')

        # don't make ldconfig noises
        r.Create('true.c', contents='int main() { return 0; }')
        r.Run('%(cc)s %(ldflags)s -static -o true true.c')
        r.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
        r.ComponentSpec('lib', '.*')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestStrip",
            macros={'lib': 'lib'})
        self.mimicRoot()
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.realRoot()
        f = magic.magic('/usr/bin/foo', self.workDir)
        assert(f.name == 'ELF')
        assert(f.contents['stripped'] == False)
        a = magic.magic('/usr/lib/libblah.a', self.workDir)
        assert(a.name == 'ar')
        assert(f.contents['stripped'] == False)

    def testStripNotAtPrefix(self):
        recipestr = """
class MissingStripPath(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        # point somewhere where strip cannot be found
        r.macros.prefix = '/blah'
        r.addSource('unstripped_archive.o', dest='%(libdir)s/')
        r.addSource('unstripped_binary', dest='%(bindir)s/')
"""
        self.buildRecipe(recipestr, "MissingStripPath")


class NormalizeLibrarySymlinksTest(rephelp.RepositoryHelper):
    def testNormalizeLibrarySymlinksTestFix(self):
        """
        Ensure that NormalizeLibrarySymlinks runs ldconfig when
        appropriate and warns about necessary changes
        """
        recipestr = """
class TestNormalizeLibrarySymlinks(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('libblah.c', contents="int foo(void) { return 1 ; }")
        r.Run('%(cc)s -g -fPIC -c libblah.c')
        r.Run('%(cc)s -g -shared -Wl,-soname,libblah.so.0 -o libblah.so.0.0 libblah.o')
        r.Install('libblah.so.0.0', '/foo/bar/')
        r.SharedLibrary(subtrees='/foo/bar')

        # Need to *really* call ldconfig, but testing that this works
        # with included ldconfig
        r.Create('true.c', contents='''
#include <stdlib.h>
int main() {
    system("/sbin/ldconfig -n %(destdir)s/foo/bar");
    return 0;
}
''')
        r.Run('%(cc)s %(ldflags)s -static -o true true.c')
        r.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
        r.ComponentSpec('lib', '.*')
"""
        self.logFilter.ignore('warning: EnforceSonameBuildRequirements: The following dependencies are not resolved within the package or in the system database:.*')
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeLibrarySymlinks", logLevel=log.INFO)
        self.logFilter.remove()
        assert('+ NormalizeLibrarySymlinks: ldconfig added the following new files in /foo/bar: libblah.so.0'
                in self.logFilter.records)

    def testNormalizeLibrarySymlinksTestBroken(self):
        """
        Ensure that NormalizeLibrarySymlinks runs ldconfig when
        appropriate and warns about files going away
        """
        recipestr = """
class TestNormalizeLibrarySymlinks(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Symlink('libblah.so.0', '%(libdir)s/libblah.so.0.0')
"""
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeLibrarySymlinks")
        self.logFilter.remove()
        macros = {'lib':'lib'}
        macros.update(use.Arch._getMacros())
        self.logFilter.compare((
            'warning: NormalizeLibrarySymlinks: ldconfig removed files in /usr/%(lib)s: libblah.so.0.0' %macros,
            'error: No files were found to add to package test'))
    def testNormalizeLibrarySymlinksTestGood(self):
        """
        Ensure that NormalizeLibrarySymlinks does not warn when there
        are no changes that ldconfig needs to make.
        """
        recipestr = """
class TestNormalizeLibrarySymlinks(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('libblah.c', contents="int foo(void) { return 1 ; }")
        r.Run('%(cc)s -g -fPIC -c libblah.c')
        r.Run('%(cc)s -g -shared -Wl,-soname,libblah.so.0 -o libblah.so.0.0 libblah.o')
        r.Install('libblah.so.0.0', '%(libdir)s/')
        r.Symlink('libblah.so.0.0', '%(libdir)s/libblah.so.0')

        # and don't make ldconfig noises
        r.Create('true.c', contents='int main() { return 0; }')
        r.Run('%(cc)s %(ldflags)s -static -o true true.c')
        r.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
        r.ComponentSpec('lib', '.*')
"""
        self.logFilter.ignore('warning: EnforceSonameBuildRequirements:.*')
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeLibrarySymlinks")
        self.logFilter.remove()
        self.logFilter.compare(())


class NormalizeCompressionTest(rephelp.RepositoryHelper):
    def testNormalizeCompressionTest1(self):
        """
        Test to make sure that all backup files get removed
        """
        recipestr = """
class TestNormalizeCompression(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('a', contents = 'ABCDEFGABCDEFGABCDEFGABCDEFG')
        r.Run('cp a foo ;cp a bar; gzip -1 foo; gzip -9n bar')
        r.Install('a', '%(datadir)s/foo')
        r.Install('a', '%(datadir)s/')
        r.Run('cp a b; bzip2 -1 a; bzip2 -9 b')
        r.Install('foo.gz', '%(datadir)s/', mode=0444)
        r.Install('bar.gz', '%(datadir)s/')
        r.Install('a.bz2', '%(datadir)s/', mode=0440)
        r.Install('b.bz2', '%(datadir)s/')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeCompression")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        foo = self.workDir + '/usr/share/foo.gz'
        bar = self.workDir + '/usr/share/bar.gz'
        a = self.workDir + '/usr/share/a.bz2'
        b = self.workDir + '/usr/share/b.bz2'
        assert(file(foo).read() == file(bar).read())
        assert(file(a).read() == file(b).read())
        assert(os.lstat(foo).st_mode &0777 == 0444)
        assert(os.lstat(a).st_mode &0777 == 0440)

class NormalizeManPagesTest(rephelp.RepositoryHelper):
    def testNormalizeManPagesTest1(self):
        """
        Test all aspects of man page normalization
        """
        recipestr = r"""
class TestNormalizeManPages(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('a.1', contents='ABCDEFGABCDEFGABCDEFGABCDEFG%(destdir)s/')
        r.Create('b.1', contents='.so man1/a.1')
        r.Run("cp a.1 c.1; gzip c.1")
        r.Run("chmod 400 a.1")
        r.Install('*', '%(mandir)s/man1/')
        r.Create('%(mandir)s/man1/f.1', contents='''real content
.so man1/a.1
more content''')
        r.Create('%(mandir)s/man1/g.1',
                 contents=r'''.\" this is a comment
.so man1/a.1

''')
        # create a dangling symlink (that won't dangle after man page
        # compression)
        r.Symlink('a.1.gz', '%(mandir)s/man1/d.1.gz')
        # test rstrip
        r.Create('%(mandir)s/man1/z.1',
                 contents='.so %(mandir)s/man1/a.1 ')
        
        r.ExcludeDirectories(exceptions='%(mandir)s/man1')
        r.Run("chmod 700 %(destdir)s/%(mandir)s/man1")
        
        # exclude two directories at once
        r.MakeDirs('/foo', '/bar')
        r.ExcludeDirectories(exceptions=['/foo', '/bar'])

        # test utf-8 fixups; \253 is legal iso-8859-1 but not utf-8
        r.Create('%(mandir)s/man1/u.1', contents='\253')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeManPages")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        mode = self.getmode('/usr/share/man/man1/a.1.gz')
        assert(mode & 0777 == 0644)
        mode = self.getmode('/usr/share/man/man1')
        assert(mode & 0777 == 0755)
        # make sure destdir is removed:
        f = os.popen("gzip -dc "+self.workDir+"/usr/share/man/man1/a.1.gz")
        assert(f.read() == 'ABCDEFGABCDEFGABCDEFGABCDEFG/\n')
        f.close()
        for link in ('b', 'z'):
            assert(
                os.readlink(self.workDir+"/usr/share/man/man1/%s.1.gz" %link)
                == 'a.1.gz')
        a = magic.magic("/usr/share/man/man1/a.1.gz", self.workDir)
        assert(a.contents['compression'] == '9')
        c = magic.magic("/usr/share/man/man1/c.1.gz", self.workDir)
        assert(c.contents['compression'] == '9')
        # make sure that .so in the middle of a page with other things
        # will not force the page into a symlink
        assert(not os.path.islink(self.workDir + '/usr/share/man/man1/f.1.gz'))
        # and make sure that comments and blank lines alone will not get
        # in the way of turning the page into a symlink
        assert(os.path.islink(self.workDir + '/usr/share/man/man1/g.1.gz'))
        # make sure that excludeDirectories works with two files
        assert(os.path.isdir(self.workDir + '/foo'))
        assert(os.path.isdir(self.workDir + '/bar'))
        # utf-8ification
        f = os.popen("gzip -dc "+self.workDir+"/usr/share/man/man1/u.1.gz")
        assert(f.read() == '\xc2\xab\n')
        f.close()

    def getmode(self, filename):
        return os.stat(self.workDir+filename)[stat.ST_MODE]


class TestFixupManpagePaths(rephelp.RepositoryHelper):
    def test1TestFixupManpagePaths(self):
        """
        Verify that man pages installed into /usr/man get moved correctly
        """
        recipestr = """
class TestStuff(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(self):
        # Tests FixupManpagePaths dependency on FixObsoletePaths
        # as well as contents of FixupManpagePaths
        self.Create('/usr/man/foo.1')
"""
        (built, d) = self.buildRecipe(recipestr, "TestStuff")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(not os.path.exists(self.workDir + '/usr/man/foo.1'))
        # depends on NormalizeManPages
        assert(os.path.exists(self.workDir + '/usr/share/man/man1/foo.1.gz'))



class NormalizeAppDefaultsTest(rephelp.RepositoryHelper):
    def testNormalizeAppDefaultsTest1(self):
        """
        Test to make sure that all backup files get removed
        """
        recipestr = """
class TestNormalizeAppDefaults(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('%(sysconfdir)s/X11/app-defaults/foo')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeAppDefaults")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(not os.path.isdir(self.workDir+'/etc/X11/app-defaults'))
        assert(os.path.isdir(self.workDir+'/usr/X11R6/lib/X11/app-defaults'))
        assert(os.path.exists(self.workDir+'/usr/X11R6/lib/X11/app-defaults/foo'))


class NormalizeInterpreterPathsTest(rephelp.RepositoryHelper):
    def testNormalizeInterpreterPathsTest1(self):
        """
        Test to make sure that #!/bin/env and #!/usr/bin/env get fixed
        """
        recipestr = """
class TestNormalizeInterpreterPaths(PackageRecipe):
    name = 'test++'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/bin/foo', contents='#!/bin/env perl', mode=0755)
        r.Create('/bin/foo2', contents='#!/bin/env /usr/bin/perl', mode=0755)
        # test writing to read-only files
        r.Create('/bin/bar', contents='#!/usr/bin/env perl', mode=0555)
        # test writing to execute-only files
        r.Create('/bin/blah', contents='#!/bin/bash', mode=0111)
        # test writing to files w/ normal permissions
        r.Create('/bin/asdf', contents='#!/usr/bin/env blah', mode=0755)
        r.Create('/bin/asdf1', contents='#!/usr/local/bin/env blah', mode=0755)
        r.Create('/foo/bla', contents='#!/usr/local/bin/perl -w', mode=0755)
        r.Create('/foo/bla1', contents='#!/usr/local/bin/foo', mode=0755)
        r.Create('/foo/bla2', contents='#!/usr/local/sbin/sh', mode=0755)
        # test non-execute files
        r.Create('/foo/blah', contents='#!/bin/env perl', mode=0644)
        r.Create('/foo/bar', contents='#!./bin/env perl', mode=0644)
        # errors in thisdocdir should not be noted
        r.Create('%(thisdocdir)s/asdf', contents='#!/bin/env /nonesuch', mode=0755)
        r.Requires('%(essentialbindir)s/bash', exceptions='.*')
        r.Requires('%(essentialbindir)s/blah', exceptions='.*')
        r.Requires('%(bindir)s/perl', exceptions='.*')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeInterpreterPaths")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self._checkOneFileNoEnv('/bin/foo')
        self._checkOneFileNoEnv('/bin/foo2')
        self._checkOneFileNoEnv('/bin/bar')
        self._checkOneFileLine('/bin/blah', '#!/bin/bash\n')
        self._checkOneFileLine('/bin/asdf', '#!/bin/blah\n')
        self._checkOneFileLine('/bin/asdf1', '#!/bin/blah\n')
        self._checkOneFileLine('/foo/bla', '#!/usr/bin/perl -w\n')
        self._checkOneFileLine('/foo/bla1', '#!/bin/foo\n')
        if os.path.exists("/usr/bin/sh"):
            # SLES-11
            shPath = "/usr/bin/sh"
        else:
            shPath = "/bin/sh"
        self._checkOneFileLine('/foo/bla2', '#!%s\n' % shPath)
        self._checkOneFileLine('/foo/blah', '#!/bin/env perl\n')
        self._checkOneFileLine('/foo/bar', '#!./bin/env perl\n')
        self._checkOneFileLine('/usr/share/doc/test++-0/asdf', '#!/bin/env /nonesuch\n')

    def _checkOneFileNoEnv(self, path):
        workpath = self.workDir + path
        mode = os.lstat(workpath)[stat.ST_MODE]
        os.chmod(workpath, mode | 0400)
        f = file(workpath)
        line = f.readline()
        f.close()
        os.chmod(workpath, mode)
        assert(line.find('env') == -1)

    def _checkOneFileLine(self, path, line):
        workpath = self.workDir + path
        mode = os.lstat(workpath)[stat.ST_MODE]
        os.chmod(workpath, mode | 0400)
        f = file(workpath)
        thisline = f.readline()
        f.close()
        os.chmod(workpath, mode)
        self.assertEqual(thisline, line)

    def testNormalizeInterpreterPathsTest2(self):
        """
        Test case where interpreter doesn't exist
        """
        recipestr = """
class TestNormalizeInterpreterPaths(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/bin/foo', contents='#!/bin/env nosuchfile', mode=0755)
"""
        self.reset()
        self.assertRaises(policy.PolicyError, self.buildRecipe, 
                          recipestr, "TestNormalizeInterpreterPaths")

    def testNormalizeInterpreterPathsTest3(self):
        """
        Test to make sure that interpriter is given for /bin/env
        """
        recipestr = """
class TestNormalizeInterpreterPaths(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/foo/bla', contents='#!/bin/env', mode=0755)
"""
        self.reset()
        self.assertRaises(policy.PolicyError, self.buildRecipe, 
                          recipestr, "TestNormalizeInterpreterPaths")



class NormalizePamConfigTest(rephelp.RepositoryHelper):
    def testNormalizePamConfigTest1(self):
        recipestr = r"""
class TestNormalizePamConfig(PackageRecipe):
    name = 'test++'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/%(sysconfdir)s/pam.d/foo', contents='/lib/security/$ISA/',
                 mode=0400)
        r.Symlink('foo', '%(sysconfdir)s/pam.d/bar')
        r.Create('/%(sysconfdir)s/pam.d/stack', contents='account    required pam_stack.so service=system-auth',
                 mode=0400)

"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizePamConfig")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        path = self.workDir+'/etc/pam.d/foo'
        f = file(path)
        line = f.readline()
        f.close()
        self.assertEqual(line.find('ISA'), -1)
        self.assertEqual(os.stat(path)[stat.ST_MODE] & 0777, 0400)

        path = self.workDir+'/etc/pam.d/stack'
        f = file(path)
        line = f.readline()
        f.close()
        assert line.find('pam_stack') == -1, line


class RelativeSymlinksTest(rephelp.RepositoryHelper):
    def testRelativeSymlinksTest1(self):
        """
        Test conversion of absolute to relative symlinks
        """
        recipestr = """
class TestRelativeSymlinks(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.MakeDirs('/a/b/{c,d,e}', '/b/g')
        self.Create("/a/b/c/foo")
        self.Symlink('foo', '/a/b/c/bar')
        self.Symlink('/a/b/c/foo', '/a/b/d/foo')
        self.Symlink('../c/foo', '/a/b/d/foo')
        self.Symlink('c/foo', '/a/b/foo')
        self.Symlink('/a/b/c/foo', '/b/g/bar')
        self.Symlink('/a/b/f/foo', '/a/b/f/foo/foo')
"""
        self.reset()
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr, "TestRelativeSymlinks")
        self.logFilter.remove()
        self.logFilter.compare((
            'warning: back-referenced symlink ../c/foo should probably be replaced by absolute symlink (start with "/" not "..")',
        ))
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(os.readlink(self.workDir+"/a/b/c/bar") == 'foo')
        assert(os.readlink(self.workDir+"/a/b/d/foo") == '../c/foo')
        assert(os.readlink(self.workDir+"/a/b/foo") == 'c/foo')
        assert(os.readlink(self.workDir+"/b/g/bar") == '../../a/b/c/foo')
        assert(os.readlink(self.workDir+"/a/b/f/foo/foo") == '.')


class FixBuilddirSymlinkTest(rephelp.RepositoryHelper):
    def testFixBuilddirSymlink(self):
        recipestr = r"""
class TestFixBuilddirSymlink(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('subdir/bin/file', contents = 'test text')
        r.Symlink('%(builddir)s/subdir/bin/file', 'subdir/lib/symlink')
        r.Move('subdir', '/')
"""
        self.resetRoot()
        (built, d) = self.buildRecipe(recipestr, "TestFixBuilddirSymlink")
        self.updatePkg(self.workDir, 'test', '0')
        assert(os.readlink(self.workDir+'/subdir/lib/symlink') == '../bin/file')


class NormalizePkgConfigTest(rephelp.RepositoryHelper):
    def testNormalizePkgConfig(self):
        recipestr = """
class TestNormalizePkgConfig(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/usr/lib/pkgconfig/foo.pc')
        r.Create('%(datadir)s/pkgconfig/bar.pc')
"""
        self.reset()
        built, d = self.buildRecipe(recipestr, "TestNormalizePkgConfig",
            macros={'lib': 'lib64'})
        repos = self.openRepository()
        name, version, flavor = built[0]
        version = versions.VersionFromString(version)
        trv = repos.getTrove(name, version, flavor)
        files = sorted([x[1] for x in
                       repos.iterFilesInTrove(
                            trv.getName(), trv.getVersion(), trv.getFlavor(),
                            withFiles=True)])
        assert(files == ['/usr/lib64/pkgconfig/bar.pc',
                         '/usr/lib64/pkgconfig/foo.pc'])


class NormalizePythonInterpreterVersionTest(rephelp.RepositoryHelper):

    def testNormalizePythonInterpreterVersionTest1(self):
        """
        Test to make sure that #!/usr/bin/python[\d.]+ get fixed
        """
        recipestr = """
class TestNormalizePythonInterpreterVersion(PackageRecipe):
    name = 'test++'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        py = file('/usr/bin/python', 'rb')
        pyc = py.read()
        %(interpreter)s
        %(variant)s
        py.close()

        r.NormalizePythonInterpreterVersion(versionMap={
            '%%(bindir)s/python25': '%(longVersion)s'})

        r.Create('/bin/foo0', contents=r'#!/usr/bin/python -o\\n', mode=0755)#not normalized interpriter path
        r.Create('/bin/foo1', contents=r'#!%(longVersion)s -o\\n', mode=0755)#correct interpriter path
        r.Create('/bin/foo2', contents=r'#!/usr/bin/python -o\\n', mode=0555)#test writing to read-only files
        r.Create('/bin/foo3', contents=r'#!/usr/bin/python -o\\nimport time\\n', mode=0111)#test writing to execute-only files
        r.Create('/bin/foo4', contents=r'#!%%(bindir)s/python25 -o\\n', mode=0755)#not normalized interpriter path
"""

        self.interpreter = "r.Create('/usr/bin/python', macros=False, contents=pyc, mode=0755)"
        variants = (
            ("r.Link('%(longVersion)s', '/usr/bin/python')", self.interpreter),
            ("r.Create('%(longVersion)s', macros=False, contents=pyc, mode=0755)", self.interpreter),
            ("", self.interpreter),
            ("", ""),
        )

        self.longVersion = '/usr/bin/python2.'

        for var, interpreter in variants:
            self.reset()
            variant = var % {'longVersion': self.longVersion}
            (built, d) = self.buildRecipe(recipestr % {
                'variant': variant,
                'interpreter': interpreter,
                'longVersion': self.longVersion
                }, 'TestNormalizePythonInterpreterVersion')
            for p in built:
                    self.updatePkg(self.workDir, p[0], p[1], depCheck=False)

            if interpreter and not variant:
                # if only non-version interpreter in destdir, do not modify
                # the interpreter path
                contents = '/usr/bin/python '
            else:
                contents = self.longVersion
            # Disable this test
            if not interpreter:
                if sys.version_info[:2] == (2, 6):
                    sys.stderr.write("\n"
                        "testNormalizePythonInterpreterVersionTest1: "
                        "Skipping tests on python 2.6 on "
                        "foresight, /usr/bin/python and /usr/bin/python2.6 are "
                        "not in the same component\n")
                else:
                    self._checkVersion('/bin/foo0', contents)
                    self._checkVersion('/bin/foo2', contents)
                    self._checkVersion('/bin/foo3', contents)
            self._checkVersion('/bin/foo1', self.longVersion)
            self._checkVersion('/bin/foo4', self.longVersion)



    def testNormalizePythonInterpreterVersionNoConfig(self):
        """
        Test that /usr/bin/python-config is not an interpreter (CNP-194)
        """
        import normalize
        p = normalize.NormalizePythonInterpreterVersion(None)
        p.versionMap = {}
        p.preProcess()
        self.assertEqual(p.interpreterRe.match('/usr/bin/python-2.4') and True,
                          True)
        self.assertEqual(p.interpreterRe.match('/usr/bin/python-config'), None)

    def testNormalizePythonInterpreterVersionMap(self):
        """
        Making sure that the specific CNP-104 use case required is tested
        directly.
        """
        recipestr = """
class TestNormalizePythonInterpreterVersionMap(PackageRecipe):
    name = 'test++'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        py = file('/usr/bin/python', 'rb')
        pyc = py.read()
        py.close()

        r.NormalizePythonInterpreterVersion(versionMap=(
            ('%(bindir)s/python', '%(bindir)s/python2.5'),
            ['%(bindir)s/python25', '%(bindir)s/python2.5']
        ))

        r.Create('/usr/bin/python', macros=False, contents=pyc, mode=0755)
        r.Link('python2.5', '/usr/bin/python')
        r.Create('/bin/foo0', contents='#!/usr/bin/python', mode=0755)
        r.Create('/bin/foo1', contents='#!/usr/bin/python25', mode=0755)
        r.Create('/bin/foo2', contents='#!/usr/bin/python2.5', mode=0755)
        # Make sure ordering with NormalizeInterpreterPaths is correct
        r.Create('/bin/foo3', contents='#!/usr/bin/env python', mode=0755)
        r.Create('/bin/foo4', contents='#!/usr/bin/env python2.5', mode=0755)
"""

        built, d = self.buildRecipe(recipestr, 'TestNormalizePythonInterpreterVersionMap')
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)

        contents='/usr/bin/python2.5'
        self._checkVersion('/bin/foo0', contents)
        self._checkVersion('/bin/foo1', contents)
        self._checkVersion('/bin/foo2', contents)
        self._checkVersion('/bin/foo3', contents)
        self._checkVersion('/bin/foo4', contents)


    def _checkVersion(self, path, contents):
        fullpath = self.workDir + path

        mode = os.lstat(fullpath)[stat.ST_MODE]
        if not mode & 0111:
            return

        os.chmod(fullpath, mode | 0400)

        f = file(fullpath, 'r')
        line = f.readline().strip()
        f.close()
        os.chmod(fullpath, mode)

        assert line.find(contents) != -1, '%s incorrectly contains %s' %(path, line)



class NormalizeInfoPages(rephelp.RepositoryHelper):

    def testNormalizeInfoPages1(self):
        """
        Test to make sure that #!/usr/bin/python[\d.]+ get fixed
        """
        recipestr = """
class TestNormalizeInfoPages(PackageRecipe):
    name = 'test++'
    version = '0'
    clearBuildReqs()
    
    def setup(r):

        r.MakeDirs('%(infodir)s/test0')
        r.Create('%(infodir)s/test0/test0.info', contents='INFO PAGE')
        r.Create('%(infodir)s/test0/test1.info', contents='INFO PAGE')
        
        r.Create('%(infodir)s/test2.info', contents='INFO PAGE')
        
        r.MakeDirs('%(infodir)s/test1')
        r.Create('%(infodir)s/test1/test3.info', contents='INFO PAGE')
        r.Create('%(infodir)s/test1/test4.info', contents='INFO PAGE')
        
        r.MakeDirs('%(infodir)s/test2/test3')
        r.Create('%(infodir)s/test1/test2/test5.info', contents='INFO PAGE')
        r.Create('%(infodir)s/test1/test2/test6.info', contents='INFO PAGE')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeInfoPages")
        for p in built:
                self.updatePkg(self.workDir, p[0], p[1], depCheck=False)

        self._checkInfoFile('test0.info.gz')
        self._checkInfoFile('test1.info.gz')
        self._checkInfoFile('test2.info.gz')
        self._checkInfoFile('test3.info.gz')
        self._checkInfoFile('test4.info.gz')
        self._checkInfoFile('test5.info.gz')
        self._checkInfoFile('test6.info.gz')

    def _checkInfoFile(self, file):
        infofile = '/'.join((self.workDir, '/usr/share/info', file))
        assert os.path.exists(infofile)

    def testNormalizeInfoPagesTest2(self):
        """
        Test to make sure that all info files get compressed. CNP-135 exposed
        a combination of bugs that caused this to not happen.
        """
        recipestr = """
class TestNormalizeInfoPages(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.Create('/%(infodir)s/foo.info',
                contents = 10 * 'ABCDEFGABCDEFGABCDEFGABCDEFG')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr, "TestNormalizeInfoPages",
                logBuild = True)
        client = self.getConaryClient()
        repos = client.getRepos()
        nvf = [x for x in built if x[0] == 'test:doc'][0]
        nvf = repos.findTrove(self.cfg.buildLabel, nvf)
        fileDict = client.getFilesFromTrove(*nvf[0])
        self.assertEquals(fileDict.keys(), ['/usr/share/info/foo.info.gz'])
        # gzip module depends on file-like objects honoring two-param seek.
        fileObj = fileDict.values()[0]
        fileObj.seek(0)
        fileObj = StringIO.StringIO(fileObj.read())

        g = gzip.GzipFile(fileobj = fileObj, mode = 'r')
        self.assertEquals(g.read(), 10 * 'ABCDEFGABCDEFGABCDEFGABCDEFG' + '\n')

class PythonEggsTest(rephelp.RepositoryHelper):
    def testBadPythonEggFile(self):
        recipeStr = """class EggRecipe(PackageRecipe):
        name = 'egg-dir'
        version = '1'

        clearBuildRequires()
        def setup(r):
            # create a file that's not a zipfile, ensure NormalizePythonEggs
            # doesn't attempt to process it
            r.Create('/usr/lib/python2.5/site-packages/splat/splat.egg')"""

        err = self.assertRaises(policy.PolicyError, self.buildRecipe,
                recipeStr, "EggRecipe")
        assert str(err).endswith("splat.egg exists but isn't a valid Python .egg")

    def testPythonEggDir(self):
        recipeStr = """class EggRecipe(PackageRecipe):
        name = 'egg-dir'
        version = '1'

        clearBuildRequires()
        def setup(r):
            # create a dir that violates PythonEggs,
            # but sidestep NormalizePythonEggs
            r.Create('/usr/lib/python2.5/site-packages/splat/splat.egg/file')"""

        self.buildRecipe(recipeStr, "EggRecipe")

    def makeEgg(self, name = 'foo', version = '1'):
        tmpDir = tempfile.mkdtemp()
        try:
            cwd = os.getcwd()
            os.chdir(tmpDir)
            f = open(os.path.join(tmpDir, 'setup.py'), 'w')
            f.write("\n".join(('from setuptools import setup, find_packages',
                    'setup(name = "%s",' % name,
                    '    version = "%s",'  % version,
                    '    packages = find_packages())')))
            f.close()
            self.captureOutput(os.system, 'python setup.py bdist_egg')
            eggFn = os.listdir(os.path.join(tmpDir, 'dist'))[0]
            return open(os.path.join(tmpDir, 'dist', eggFn)).read()
        finally:
            os.chdir(cwd)
            util.rmtree(tmpDir)

    def testExtractEgg(self):
        eggFile = self.makeEgg()
        recipeStr = """class EggRecipe(PackageRecipe):
            name = 'egg-recipe'
            version = '1'

            clearBuildRequires()
            def setup(r):
                r.addSource('foo.egg',
                        dir = '%(libdir)s/python2.5/site-packages/')"""
        self.addComponent('egg-recipe:source=1',
                [('egg-recipe.recipe', recipeStr),
                ('foo.egg', eggFile)])
        client = self.getConaryClient()
        repos = client.getRepos()
        built, csf = self.cookItem(repos, self.cfg, 'egg-recipe')
        nvf = built[0]
        nvf = repos.findTrove(None, nvf)[0]
        fileDict = client.getFilesFromTrove(*nvf)
        ref = ['PKG-INFO', 'SOURCES.txt', 'top_level.txt', 'zip-safe']
        fileList = sorted([os.path.basename(x) for x in fileDict.keys()])
        # rpl:2 python-setuptools is newer, and has an additional file
        if 'dependency_links.txt' in fileList:
            fileList.remove('dependency_links.txt')
        self.assertEquals(fileList, ref)

    def testNonPackagePythonEgg(self):
        eggFile = self.makeEgg()
        recipeStr = """class EggRecipe(PackageRecipe):
        name = 'egg-recipe'
        version = '1'

        clearBuildRequires()
        def setup(r):
            # ensure that removing python eggs via NonPackageFiles
            # isn't tripped by NormalizePythonEggs exploding them
            # (NonPackageFiles should win)

            # create a file so that the build can complete ensure everything
            # ends up in the same component
            r.Create('/opt/splat', package = 'egg-recipe:python')
            r.RemoveNonPackageFiles('.*\.egg')
            r.addSource('foo.egg',
                    dir = '%(libdir)s/python2.5/site-packages/')"""

        self.addComponent('egg-recipe:source=1',
                [('egg-recipe.recipe', recipeStr),
                ('foo.egg', eggFile)])
        client = self.getConaryClient()
        repos = client.getRepos()
        built, csf = self.cookItem(repos, self.cfg, 'egg-recipe')
        nvf = built[0]
        nvf = repos.findTrove(None, nvf)[0]
        fileDict = client.getFilesFromTrove(*nvf)
        ref = ['splat']
        fileList = sorted([os.path.basename(x) for x in fileDict.keys()])
        self.assertEquals(fileList, ref)

    def testExtractEggRequires(self):
        eggFile = self.makeEgg()
        recipeStr = """class EggRecipe(PackageRecipe):
            name = 'egg-recipe'
            version = '1'

            clearBuildRequires()
            def setup(r):
                r.addSource('foo.egg',
                        dir = '%(libdir)s/python2.5/site-packages/')"""
        self.addComponent('egg-recipe:source=1',
                [('egg-recipe.recipe', recipeStr),
                ('foo.egg', eggFile)])
        client = self.getConaryClient()
        repos = client.getRepos()

        _addActionPathBuildRequires = \
                action.RecipeAction._addActionPathBuildRequires
        self.buildReqs = set([])
        def mockedAddActionPathBuildRequires(x, buildReqs):
            self.buildReqs.update(buildReqs)
            return _addActionPathBuildRequires(x, buildReqs)
        self.mock(action.RecipeAction, '_addActionPathBuildRequires',
                mockedAddActionPathBuildRequires)
        output = self.captureOutput(cook.cookItem,
                repos, self.cfg, 'egg-recipe')
        self.assertEquals(self.buildReqs, set(['unzip']))

    def testExtractEggException(self):
        eggFile = self.makeEgg()
        recipeStr = """class EggRecipe(PackageRecipe):
            name = 'egg-recipe'
            version = '1'

            clearBuildRequires()
            def setup(r):
                # this should fail on PythonEggs
                r.addSource('foo.egg',
                        dir = '%(libdir)s/python2.5/site-packages/')
                r.NormalizePythonEggs(exceptions = '.*')"""
        self.addComponent('egg-recipe:source=1',
                [('egg-recipe.recipe', recipeStr),
                ('foo.egg', eggFile)])
        client = self.getConaryClient()
        repos = client.getRepos()
        err = self.assertRaises(policy.PolicyError, self.cookItem,
                repos, self.cfg, 'egg-recipe')
        assert str(err).startswith('Package Policy errors found:\nPythonEggs:')
        assert str(err).endswith('argument to setup.py or use r.PythonSetup()')

class EggRequiresTest(rephelp.RepositoryHelper):
    def testEggRequires(self):
        recipeStr = """
class EggRequiresRecipe(PackageRecipe):
    name = 'egg-requires'
    version = '1'

    clearBuildRequires()
    buildRequires = ['python-setuptools:python']

    def setup(r):
        r.Create('/usr/lib/python2.5/site-packages/eggtest-1-py2.5.egg-info/requires.txt', contents = \"\"\"present1
missing1>=0.3

[optional]
present2>0.4

[extra]
missing2>=1.2,<=1.3\"\"\")
"""
        realGlob = fixedglob.glob
        def dummyGlob(path):
            if not path.startswith(self.buildDir):
                if 'present' in path:
                    return [path]
                elif 'missing' in path:
                    return []
            return realGlob(path)
        self.addDbComponent(self.openDatabase(), 'python-setuptools:python')
        self.mock(fixedglob, 'glob', dummyGlob)

        self.mock(packagepolicy.Requires, '_enforceProvidedPath',
                lambda x, path, *args, **kwargs: \
                        (re.search('present\d', path).group() + ":python"))

        self.logFilter.add()
        (built, d) = self.buildRecipe(recipeStr, 'EggRequiresRecipe')
        self.logFilter.remove()
        self.assertFalse('warning: Requires: Python egg-info for missing1 was not found' not in self.logFilter.records)
        self.assertFalse('warning: Requires: Python egg-info for missing2 was not found' in self.logFilter.records)

        repos = self.openRepository()
        name, ver, flv = built[0]
        ver = versions.VersionFromString(ver)
        trv = repos.getTrove(name, ver, flv)
        pyDeps = deps.ThawDependencySet('4#present1::python|4#present2::python')
        self.assertEquals(trv.getRequires(), pyDeps)

    def testEggRequiresDestdir(self):
        recipeStr = """
class EggRequiresRecipe(PackageRecipe):
    name = 'egg-requires'
    version = '1'

    clearBuildRequires()
    buildRequires = ['python-setuptools:python']

    def setup(r):
        r.Create('/usr/lib/python2.5/site-packages/present-1-py2.5.egg-info/PKG-INFO', component = 'present:python')
        r.Create('/usr/lib/python2.5/site-packages/eggtest-1-py2.5.egg-info/requires.txt', contents = \"\"\"present
missing1>=0.3\"\"\")
"""

        self.addDbComponent(self.openDatabase(), 'python-setuptools:python')
        self.mock(packagepolicy.Requires, '_enforceProvidedPath',
                lambda x, path, *args, **kwargs: \
                        (re.search('present\d', path).group() + ":python"))

        self.logFilter.add()
        (built, d) = self.buildRecipe(recipeStr, 'EggRequiresRecipe')
        self.logFilter.remove()
        self.assertFalse('warning: Requires: Python egg-info for missing1 was not found' not in self.logFilter.records)

        repos = self.openRepository()
        name, ver, flv = built[0]
        ver = versions.VersionFromString(ver)
        trv = repos.getTrove(name, ver, flv)
        pyDeps = deps.ThawDependencySet('4#present::python')
        self.assertEquals(trv.getRequires(), pyDeps)

    def testEggRequiresSymlink(self):
        recipeStr = """
class EggRequiresRecipe(PackageRecipe):
    name = 'egg-requires'
    version = '1'

    clearBuildRequires()
    buildRequires = ['python-setuptools:python']

    def setup(r):
        r.Create('/other/place/PKG-INFO', component = 'symlink:python')
        r.Symlink('/other/place', '/usr/lib/python2.5/site-packages/symlink-1-py2.5.egg-info', component = 'symlink:python')
        r.Create('/usr/lib/python2.5/site-packages/eggtest-1-py2.5.egg-info/requires.txt', contents = "symlink\\n")
"""

        self.mock(packagepolicy.Requires, '_enforceProvidedPath',
                lambda x, path, *args, **kwargs: \
                        (re.search('present\d', path).group() + ":python"))

        self.addDbComponent(self.openDatabase(), 'python-setuptools:python')
        (built, d) = self.buildRecipe(recipeStr, 'EggRequiresRecipe')

        repos = self.openRepository()
        name, ver, flv = built[0]
        ver = versions.VersionFromString(ver)
        trv = repos.getTrove(name, ver, flv)

        pyDeps = deps.ThawDependencySet('4#symlink::python')
        self.assertEqual(trv.getRequires(), pyDeps)
        self.assertEquals(trv.getRequires(), pyDeps)

    def testEggMissingPythonSetuptools(self):
        # when cooking into the repository, an .egg file requires that you
        # have python-setuptools in your buildreqs.
        recipeStr = """
class EggRequiresRecipe(PackageRecipe):
    name = 'egg-requires'
    version = '1'

    clearBuildRequires()
    # Please note, this buildRequires line is modified and used by this
    # test. don't remove it.
    #buildRequires = ['python-setuptools:python']

    def setup(r):
        r.Create('/other/place/PKG-INFO', component = 'symlink:python')
        r.Symlink('/other/place', '/usr/lib/python2.5/site-packages/symlink-1-py2.5.egg-info', component = 'symlink:python')
        r.Create('/usr/lib/python2.5/site-packages/eggtest-1-py2.5.egg-info/requires.txt', contents = "symlink\\n")
"""
        self.mock(packagepolicy.Requires, '_enforceProvidedPath',
                lambda x, path, *args, **kwargs: \
                        (re.search('present\d', path).group() + ":python"))
        self.addComponent('egg-requires:source=1', 
                          [('egg-requires.recipe', recipeStr)])
        self.addDbComponent(self.openDatabase(), 'python-setuptools:python')
        err = self.assertRaises(policy.PolicyError, 
                self.cookItem, self.openRepository(), self.cfg, 'egg-requires')
        self.assertEquals(str(err), "Package Policy errors found:\nRequires: add 'python-setuptools:python' to buildRequires to inspect /usr/lib/python2.5/site-packages/eggtest-1-py2.5.egg-info/requires.txt")

        self.addComponent('egg-requires:source=1-2', 
                          [('egg-requires.recipe', recipeStr.replace('#build', 'build'))])
        self.cookItem(self.openRepository(), self.cfg, 'egg-requires')

class DocDirTest(rephelp.RepositoryHelper):
    def testDocDirAutoDocConflict(self):
        recipeStr = """
class DocRecipe(PackageRecipe):
    name = 'splat'
    version = '1'
    clearBuildReqs()

    def setup(self):
        # this recipe used to fail due to mis-ordering of AutoDoc and
        # FixObsoletePaths. simply building without error proves the problem
        # has been corrected. (CNP-70)
        self.Create('/usr/doc/misfire', contents = "needs to be moved")
        self.Create('README', 'this is an autodoc')
"""
        built, d = self.buildRecipe(recipeStr, 'DocRecipe')

    def testEmptyDocDirExists(self):
        recipeStr = """
class DocRecipe(PackageRecipe):
    name = 'splat'
    version = '1'
    clearBuildReqs()

    def setup(self):
        # the problem with this recipe is that FixObsoletePaths always runs
        # before ExcludeDirectories (different buckets). Test that
        # FixObsoletePaths deletes an empty directory. (CNP-70)
        self.MakeDirs('%(thisdocdir)s')
        self.MakeDirs('/usr/doc')
        # add a file because we're not trying to test an empty package
        self.Create('/opt/junk')
"""
        self.logFilter.add()
        built, d = self.buildRecipe(recipeStr, 'DocRecipe')
        self.logFilter.remove()
        self.assertEquals(self.logFilter.records,
                ['warning: FixObsoletePaths: Path /usr/doc should not exist, '
                 'but is empty. removing.'])

class PolicyOrderTest(rephelp.RepositoryHelper):
    def testNormalizeNonPackageFileOrder(self):
        recipeStr = """
class PolicyOrder(PackageRecipe):
    name = 'splat'
    version = '1'
    clearBuildReqs()

    def setup(self):
        # simply create a file so that we don't be entirely empty
        self.Create('/usr/share/doc/foo')
        self.Create('%(mandir)s/man1/foo.bad~', contents = ('12345' + chr(10)))
"""
        built, d = self.buildRecipe(recipeStr, 'PolicyOrder', logBuild = True)
        nvf = built[0]
        nvf = nvf[0], versions.VersionFromString(nvf[1]), nvf[2]
        repos = self.openRepository()
        trv = repos.getTrove(*nvf)
        self.assertFalse('/usr/share/man/man1/foo.bad~.gz' in \
                [x[1] for x in trv.iterFileList()], "bad pathname was gzipped")
        nvf = 'splat:debuginfo', nvf[1], nvf[2]

        # now let's go one further. make sure no destdir policy that has
        # Normalize in the name runs before RemoveNonPackageFiles.
        # This is because normalizing policies normally rename the file,
        # thus possibly fooling RemoveNonPackageFiles (CNP-122)

        whiteList = ['NormalizeLibrarySymlinks']
        client = self.getConaryClient()
        fileDict = client.getFilesFromTrove(*nvf)
        compData = fileDict['/usr/src/debug/buildlogs/splat-1-xml.bz2'].read()
        bzip = bz2.BZ2Decompressor()
        data = bzip.decompress(compData)
        policyLines = [x for x in data.splitlines() if \
                'DESTDIR_MODIFICATION' in x]
        policies = [x.split('DESTDIR_MODIFICATION')[1][1:].split('<')[0] \
                for x in policyLines]
        leadingNormalizingPolicies = [x for x in \
                policies[:policies.index('RemoveNonPackageFiles')] \
                if 'Normalize' in x and x not in whiteList]
        self.assertFalse(leadingNormalizingPolicies,
                "The following 'Normalize' policies ran before "
                "RemoveNonPackageFiles: %s" % \
                        ', '.join(leadingNormalizingPolicies))

class InitScriptTest(rephelp.RepositoryHelper):
    def testInitScriptContents(self):
        recipeStr = """
class InitTest(PackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()

    def setup(self):
        self.Create('%(initdir)s/bar', contents = chr(10).join((
            '# chkconfig: - 10 99',
            '# description: junk',
            '/etc/rc.d/init.d/bar')), mode = 0755)
"""
        built, d = self.buildRecipe(recipeStr, 'InitTest')
        self.updatePkg('test:runtime')
        f = open(os.path.join(self.rootDir, 'etc', 'init.d', 'bar'))
        data = f.read()
        self.assertFalse('rc.d' in data)

    def testInitScriptFunctionsDep(self):
        'convert sourcing functions to dep'
        repos = self.openRepository()

        funcRecipe = r"""
class Functions(PackageRecipe):
    name = 'func'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.Create('%(initdir)s/functions', mode=0755)
"""
        self.build(funcRecipe, 'Functions')

        initRecipe = r"""
class InitTest(PackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.Create('%(initdir)s/foo', contents='\n'.join((
            '# chkconfig: - 10 99',
            '# description: junk',
            '. %(initdir)s/functions')), mode=0755)
"""
        trove = self.build(initRecipe, 'InitTest')
        # dep on file is file dep
        for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
            'test:runtime', trove.getVersion(), trove.getFlavor(),
            withFiles=True):
            if path == '/etc/init.d/foo':
                self.assertEquals(fileObj.requires(),
                    deps.ThawDependencySet('3#/etc/init.d/functions'))
        # dep on trove represents this change changed
        self.assertEquals(trove.requires(),
            deps.ThawDependencySet('4#func::runtime'))


    def testInitScriptGoodSymlink(self):
        # prove that NormalizeInitscriptContents follows symlinks
        recipeStr = """
class InitTest(PackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()

    def setup(self):
        self.Create('/opt/bar', contents = chr(10).join((
            '# chkconfig: - 10 99',
            '# description: junk',
            '/etc/rc.d/init.d/bar')), mode = 0755)
        self.Symlink('/opt/bar', '%(initdir)s/bar')
"""
        built, d = self.buildRecipe(recipeStr, 'InitTest')
        self.updatePkg('test:runtime')
        f = open(os.path.join(self.rootDir, 'opt', 'bar'))
        data = f.read()
        self.assertFalse('rc.d' in data)

    def testInitScriptBadSymlink(self):
        # prove that NormalizeInitscriptContents doesn't break if the target
        # of the symlink isn't present
        recipeStr = """
class InitTest(PackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()

    def setup(self):
        self.Symlink('/opt/bar', '%(initdir)s/bar')
        self.DanglingSymlinks(exceptions = '.*')
"""
        # this buildRecipe command used to error prior to CNP-129
        self.logFilter.add()
        built, d = self.buildRecipe(recipeStr, 'InitTest')
        self.logFilter.remove()
        self.logFilter.compare(['warning: NormalizeInitscriptContents: /etc/init.d/bar is a symlink to ../../opt/bar, which does not exist.'])

    def testInitScriptDoubleRun(self):
        # prove that NormalizeInitscriptContents can be run twice on the same
        # file safely by symlinking to a file in initdir
        recipeStr = """
class InitTest(PackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()

    def setup(self):
        self.Create('%(initdir)s/foo', contents = chr(10).join((
            '# chkconfig: - 10 99',
            '# description: junk',
            '/etc/rc.d/init.d/bar')), mode = 0755)
        self.Symlink('%(initdir)s/foo', '%(initdir)s/bar')
"""
        built, d = self.buildRecipe(recipeStr, 'InitTest')
        self.updatePkg('test:runtime')
        f = open(os.path.join(self.rootDir, 'etc', 'init.d', 'foo'))
        data = f.read()
        self.assertFalse('rc.d' in data)

    def testInitScriptDirSymlink1(self):
        # prove that NormalizeInitscriptContents does the right thing if
        # initdir is a symlink
        recipeStr = """
class InitTest(PackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()

    def setup(self):
        self.Create('/opt/foo', contents = chr(10).join((
            '# chkconfig: - 10 99',
            '# description: junk',
            '/etc/rc.d/init.d/bar')), mode = 0755)
        self.Symlink('/opt', '%(initdir)s')
"""
        built, d = self.buildRecipe(recipeStr, 'InitTest')
        self.updatePkg('test:runtime')
        f = open(os.path.join(self.rootDir, 'opt', 'foo'))
        data = f.read()
        self.assertFalse('rc.d' in data)

    def testInitScriptDirSymlink2(self):
        # prove that NormalizeInitscriptContents does the right thing if
        # a leading dir to initdir is a symlink
        recipeStr = """
class InitTest(PackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()

    def setup(self):
        self.Create('/opt/init.d/foo', contents = chr(10).join((
            '# chkconfig: - 10 99',
            '# description: junk',
            '/etc/rc.d/init.d/bar')), mode = 0755)
        self.Symlink('/opt', '/etc')
"""
        built, d = self.buildRecipe(recipeStr, 'InitTest')
        self.updatePkg('test:runtime')
        f = open(os.path.join(self.rootDir, 'opt', 'init.d', 'foo'))
        data = f.read()
        self.assertFalse('rc.d' in data)
