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


from testrunner import decorators

import os
import stat
import shutil
import tempfile
from testrunner import testhelp
import SimpleHTTPServer

from conary import files, versions, rpmhelper
from conary import trove as trovemod
VFS = versions.VersionFromString
from conary.lib import log
from conary.lib import openpgpfile, util
from conary_test import rephelp
from conary.build import errors
from conary.build import lookaside
from conary.build import source
from conary_test import resources
from conary_test.cvctest.buildtest import policytest


class SourceTest(rephelp.RepositoryHelper):

    def testSourceManifest(self):
        recipestr = """
class TestSourceManifest(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        # random files we have around in archive...
        r.addArchive('asdf.tar.gz', dir='/', package='asdf')
        r.addSource('sourcefile', dir='/var/test/')
"""
        (built, d) = self.buildRecipe(recipestr, "TestSourceManifest")
        repos = self.openRepository()
        for troveName, troveVersionString, troveFlavor in built:
            for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
                    troveName, versions.VersionFromString(troveVersionString), troveFlavor,
                withFiles=True):
                if 'fdsa' in path or 'bam' in path:
                    assert(troveName == 'asdf:runtime')
                elif 'sourcefile' in path:
                    assert(troveName == 'test:runtime')



    def testISOArchiveJoliet(self):
        recipestr = """
class TestISOArchive(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        r.addArchive('jcd.iso', dir='/')
        r.SetModes('%(bindir)s/touch', 0755)
"""
        trove = self.build(recipestr, "TestISOArchive")
        repos = self.openRepository()
        paths = []
        for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
                trove.getName(), trove.getVersion(), trove.getFlavor(),
                withFiles=True):
            paths.append(path)
        assert sorted(paths) == [
            '/a/b', '/aaa/blah', '/b/bar', '/usr/bin/touch']

    def testISOArchiveRockRidge(self):
        recipestr = """
class TestISOArchive(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        r.addArchive('rrcd.iso', dir='/')
        r.SetModes('%(bindir)s/touch', 0755)
"""
        trove = self.build(recipestr, "TestISOArchive")
        repos = self.openRepository()
        paths = []
        for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
                trove.getName(), trove.getVersion(), trove.getFlavor(),
                withFiles=True):
            paths.append(path)
        assert sorted(paths) == [
            '/a/b', '/aaa/blah', '/b/bar', '/usr/bin/touch']

    @decorators.requireBinary("xz")
    def testSourceTest1(self):
        """
        Test build.source
        """

        recipestr = """
class TestSource(PackageRecipe):
    name = 'tmpwatch'
    version = '2.9.0'
    clearBuildReqs()
    
    def setup(r):
        # test unpacking and extracting from an RPM
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm')

        # test unpacking and extracting from a src bz2 RPM
        r.addSource('mkinitrd.spec', rpm='rpm-with-bzip-5.0.29-1.src.rpm')

        # test unpacking and extracting from a bz2 RPM
        r.addArchive('rpm-with-bzip-5.0.29-1.i386.rpm')

        # test unpacking and extracting from an lzma RPM
        r.addArchive('gnome-main-menu-0.9.10-26.x86_64.rpm')

        # test unpacking from a tar.xz (CNY-3207)
        r.addArchive('foo.tar.xz', dir='/')

        # test applying a patch
        r.addPatch('tmpwatch.fakebug.patch')
        # test taking that patch right back out with a string to extraArgs
        r.addPatch('tmpwatch.fakebug.patch', extraArgs='--reverse')
        # test putting the patch back in again with a list to extraArgs this time
        r.addPatch('tmpwatch.fakebug.patch', extraArgs=['--ignore-whitespace',])
        # test the dest= capability of addSource
        r.addSource('tmpwatch.fakebug.patch', dest='foo')
        r.addSource('tmpwatch.fakebug.patch', dir='/asdf', dest='foo')
        r.addSource('tmpwatch.fakebug.patch', dest='/asdf/foo2%%')
        r.addSource('tmpwatch.fakebug.patch', dest='/asdf/')
        # make sure spaces are OK
        r.addSource('name with spaces')
        r.addSource('localfoo')
        r.addSource('local name with spaces')
        r.addAction('ls foo')
        r.addAction('ls foo', dir='/asdf')
        r.addAction('ls foo2%%', dir='/asdf')
        r.addAction('ls tmpwatch.fakebug.patch', dir='/asdf')
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm',
                        dir='/asdf')
        r.addAction('ls tmpwatch-2.9.0', dir='/asdf')
        # XXX I'm not sure what this was intended to show
        #r.addAction('ls tmpwatch-2.9.0', dir='%(destdir)s/asdf')
        r.addPatch('tmpwatch.fakebug.patch', dir='/asdf/tmpwatch-2.9.0')
"""
        self.resetWork()
        self.resetRepository()
        self.repos = self.openRepository()
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('tmpwatch')
        os.chdir('tmpwatch')
        file('localfoo', 'w').close()
        file('local name with spaces', 'w').close()
        self.writeFile('tmpwatch.recipe', recipestr)
        self.addfile('tmpwatch.recipe')
        self.addfile('localfoo', text=True)
        self.addfile('local name with spaces', text=True)
        self.commit()
        os.chdir('..')
        shutil.rmtree('tmpwatch')
        os.chdir(origDir)
        self.resetCache()

        # ensure that we are testing the /usr/bin/xz path here
        realExists = os.path.exists
        def access_xz(*args):
            if args[0].endswith('/unlzma'):
                return False
            return realExists(*args)
        self.mock(os.path, 'exists', access_xz)

        self.cookItem(self.repos, self.cfg, 'tmpwatch', requireCleanSources=False)

    @decorators.requireBinary("unlzma")
    def testSourceTestUnlzma(self):
        """
        Test build.source
        """

        recipestr = """
class TestSource(PackageRecipe):
    name = 'gnome-main-menu'
    version = '0.9.10'
    clearBuildReqs()
    
    def setup(r):
        # test unpacking and extracting from an lzma RPM if xz not available
        r.addArchive('gnome-main-menu-0.9.10-26.x86_64.rpm', dir='/')
        del r.NonMultilibDirectories
"""
        # ensure that we are testing the older /usr/bin/unlzma path here
        # to make sure this still works on SLES10
        realExists = os.path.exists
        def access_lzma(*args):
            if args[0].endswith('/xz'):
                return False
            return realExists(*args)
        self.mock(os.path, 'exists', access_lzma)
        (built, d) = self.buildRecipe(recipestr, "TestSource")
        assert 'gnome-main-menu:runtime' in [x[0] for x in built]

    def testSourceTestMissinglzma(self):
        """
        Test build.source
        """

        recipestr = """
class TestSource(PackageRecipe):
    name = 'gnome-main-menu'
    version = '0.9.10'
    clearBuildReqs()
    
    def setup(r):
        # test unpacking and extracting from an lzma RPM if xz not available
        r.addArchive('gnome-main-menu-0.9.10-26.x86_64.rpm', dir='/')
"""
        # make sure sane error exists if neither xz nor unlzma provided
        realExists = os.path.exists
        def access_nolzma(*args):
            if args[0].split(os.sep)[-1] in ('xz', 'unlzma'):
                return False
            return realExists(*args)
        self.mock(os.path, 'exists', access_nolzma)
        self.assertRaises(RuntimeError, self.buildRecipe, recipestr,
                        "TestSource")

    def testUnpackOldRpm30(self):
        # CNY-3210
        # Use a very old version of rpm, that does not have PAYLOADCOMPRESSOR
        # set
        # Downloaded from
        # http://ftpsearch.kreonet.re.kr/pub/tools/utils/rpm/rpm/dist/rpm-3.0.x/
        destdir = os.path.join(self.workDir, 'dest')
        util.mkdirChain(destdir)
        rpmfile = os.path.join(self.cfg.sourceSearchDir, 'popt-1.5-4x.i386.rpm')
        source._extractFilesFromRPM(rpmfile, directory = destdir)
        self.assertTrue(os.path.exists(util.joinPaths(destdir,
            '/usr/include/popt.h')))

    def testUnpackRPMWithUnsupportedTag(self):
        # CNY-3404
        destdir = os.path.join(self.workDir, 'dest')
        util.mkdirChain(destdir)
        rpmfile = os.path.join(self.cfg.sourceSearchDir,
            'tags-1.2-3.noarch.rpm')
        # mock with tags that do and do not exist to make sure
        # that tags will be found
        self.mock(source, '_forbiddenRPMTags',
            (
                ('SIG_SHA1', rpmhelper.SIG_SHA1),
                ('BLINKPKGID', rpmhelper.BLINKPKGID),
                ('NAME', rpmhelper.NAME),
            ))
        self.assertRaises(source.SourceError,
            source._extractFilesFromRPM, rpmfile, directory = destdir)

    def testSourcePerms(self):
        recipestr = """
class TestSource(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        # test not preserving world-writeable permissions in builddir
        r.addArchive('worldwriteable.tar.bz2', dir='test-1')
        r.Install('worldwriteable', '/ww/notworldwriteable')
        # test preserving world-writeable permissions in root proxy
        r.addArchive('worldwriteable.tar.bz2', dir='/ww/')
        # test missing intermediate directory in tarball CNY-3060
        r.addArchive('missing.tar', dir='/opt/f', preserveOwnership=True)
"""
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr, "TestSource")
        self.logFilter.remove()
        self.logFilter.compare((
            'warning: WarnWriteable: Possibly inappropriately writeable permission 0666 for file /ww/worldwriteable'
            ))

    def testSourceTestSRPMCache(self):
        """
        Test SRPM lookaside handling (CNY-771)
        """

        recipe1 = """
class foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.macros.release = '1'
        r.macros.srpm = '%(name)s-%(version)s-%(release)s.src.rpm'
        r.addSource('bar', rpm='%(srpm)s')
        r.addSource('baz', rpm='%(srpm)s')
"""

        recipe2 = """
class foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.macros.release = '2'
        r.macros.srpm = '%(name)s-%(version)s-%(release)s.src.rpm'
        r.addSource('bar', rpm='%(srpm)s')
        r.addSource('baz', rpm='%(srpm)s')
"""

        self.resetRepository()
        self.repos = self.openRepository()

        self.resetWork()
        self.resetCache()
        os.chdir(self.workDir)
        self.newpkg('foo')
        os.chdir('foo')
        self.writeFile('foo.recipe', recipe1)
        self.addfile('foo.recipe')
        self.commit()

        self.resetWork()
        # Don't reset the cache
        #self.resetCache()
        os.chdir(self.workDir)
        self.checkout('foo')
        os.chdir('foo')
        self.writeFile('foo.recipe', recipe2)

        # If an IOError is raised here conary failed to find the source that
        # was removed from the SRPM, this is the correct behaviour.
        try:
            self.commit()
        except IOError:
            return

        self.resetWork()
        self.resetCache()
        os.chdir(self.workDir)
        self.cookItem(self.repos, self.cfg, 'foo')

    def testSourceTestSigCheck(self):
        """
        Test signatures
        """
        # XXX use smaller bz2 file than distcc
        recipestr1 = """
class TestSig1(PackageRecipe):
    name = 'distcc'
    version = '2.9'
    clearBuildReqs()
    
    def setup(r):
        r.addArchive('distcc-2.9.tar.bz2', keyid='A0B3E88B')
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm', keyid='sdds', dir='new-subdir')
"""

        def mockedDownloadPublicKey(slf):
            if slf.keyid == 'A0B3E88B':
                f = file(os.path.join(resources.get_archive(), '0xA0B3E88B.pgp'))
                return openpgpfile.parseAsciiArmorKey(f)
            raise source.SourceError("Failed to retrieve PGP key %s" % slf.keyid)

        self.mock(source._Source, '_downloadPublicKey',
                    mockedDownloadPublicKey)

        util.rmtree(self.buildDir, ignore_errors=True)
        self.resetWork()
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr1, "TestSig1")
        self.logFilter.remove()
        self.logFilter.compare((
            'warning: No GPG signature file found for tmpwatch-2.9.0.tar.gz',
            'error: No files were found to add to package distcc'
            ))
        recipestr2 = """
class TestSig2(PackageRecipe):
    name = 'distcc'
    version = '2.9'
    clearBuildReqs()
    def setup(r):
        r.addArchive('distcc-2.9.tar.bz2', keyid='BADBAD')
"""
        self.resetWork()
        self.assertRaises(source.SourceError, self.buildRecipe, recipestr2,
                        "TestSig2")

    def testSourceTestSigCheckFailedDownload(self):
        """
        Test a download failure for the key
        """
        recipestr1 = """
class TestSig1(PackageRecipe):
    name = 'distcc'
    version = '2.9'
    clearBuildReqs()

    def setup(r):
        r.addArchive('distcc-2.9.tar.bz2', keyid='A0B3E88B')
        r.Create("/usr/foo", contents="Bar!!!\\n")
"""

        from conary.repository import transport

        listcounter = []
        def mockedDoDownloadPublicKey(slf, keyServer, lc = listcounter):
            lc.append(None)
            if len(lc) < 7:
                raise transport.TransportError("Blah!")
            f = file(os.path.join(resources.get_archive(), '0xA0B3E88B.pgp'))
            data = openpgpfile.parseAsciiArmorKey(f)

            return data

        self.mock(source._Source, '_doDownloadPublicKey',
                    mockedDoDownloadPublicKey)

        util.rmtree(self.buildDir, ignore_errors=True)
        self.resetWork()
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipestr1, "TestSig1", prep=True)
        self.logFilter.remove()
        self.assertEqual(len(listcounter), 7)

    def testDontCheckKeyOfCommitedSource(self):
        # We choose not to check the public key for sources already committed,
        # instead relying on the check only at the time of commit.
        recipestr1 = """
class TestSig1(PackageRecipe):
    name = 'distcc'
    version = '2.9'
    clearBuildReqs()

    def setup(r):
        r.addArchive('distcc-2.9.tar.bz2', keyid='A0B3E88B')
        r.Create("/usr/foo", contents="Bar!!!\\n")
"""
        listcounter = []
        def _checkSignature(self, file):
            listcounter.append(None)
            return

        self.mock(source._Source, 'checkSignature',
                    _checkSignature)
        os.chdir(self.workDir)
        self.newpkg('distcc')
        os.chdir('distcc')
        self.writeFile('distcc.recipe', recipestr1)
        self.addfile('distcc.recipe')
        self.commit()
        assert(listcounter)
        listcounter[:] = []
        assert(not listcounter)
        self.cookItem(self.openRepository(), self.cfg, 'distcc', requireCleanSources=True)
        assert(not listcounter)

    def testSourceTestApplyMacros(self):
        """
        Test applymacros
        """

        recipestr1 = """
class TestApplyMacrosPatch(PackageRecipe):
    name = 'tmpwatch'
    version = '2.9.0'
    clearBuildReqs()

    def cleanup(r, builddir, destdir):
        pass
    
    def setup(r):
        # avoid cleanup
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm')
        # test applying a patch
        r.macros.bugid = 'BUGID'
        r.addPatch('tmpwatch.fakebug.patch', macros=True)
"""

        self.resetWork()
        (built, d) = self.buildRecipe(recipestr1, "TestApplyMacrosPatch")
        rv = self.findInFile(util.joinPaths(self.buildDir, 'tmpwatch/tmpwatch-2.9.0/tmpwatch.c'), 'BUGID')
        assert(rv != -1)

    def testSourceApplyMacros2(self):
        """
        Test applymacros
        """

        recipestr1 = """
class TestApplyMacrosSource(PackageRecipe):
    name = 'tmpwatch'
    version = '2.9.0'
    clearBuildReqs()

    def cleanup(r, builddir, destdir):
        pass
    
    def setup(r):
        # avoid cleanup
        r.macros.sourcemacros = 'source-apply-macros'
        r.addSource('myfile', contents="%(sourcemacros)s %(destdir)s",
                                                        macros=True)
        r.macros.a = 'XXX'
        r.macros.b = 'YYY'
        r.macros.c = 'ZZZ'
        r.addSource('sourcefile', macros=True, mode=0676)
"""
        self.resetWork()
        (built, d) = self.buildRecipe(recipestr1, "TestApplyMacrosSource")
        rv = self.findInFile(util.joinPaths(self.buildDir, 'tmpwatch/tmpwatch-2.9.0/myfile'), 'source-apply-macros')
        assert(rv != -1)
        rv = self.findInFile(util.joinPaths(self.buildDir, 'tmpwatch/tmpwatch-2.9.0/myfile'), self.cfg.buildPath + '/tmpwatch/_ROOT_')
        assert(rv != -1)
        rv = self.findInFile(util.joinPaths(self.buildDir, 'tmpwatch/tmpwatch-2.9.0/sourcefile'), 'XXX YYY ZZZ')
        assert(rv != -1)
        assert(os.stat(util.joinPaths(self.buildDir, 'tmpwatch/tmpwatch-2.9.0/sourcefile'))[stat.ST_MODE] & 07777 == 0676)


    def testPatchFilter(self):
        recipestr = """
class TestPatchFilter(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        # avoid cleanup
        r.addSource('unpatched1')
        r.addSource('unpatched2')
        r.addPatch('patchToFilter.patch')
"""

        self.assertRaises(source.SourceError, self.buildRecipe, recipestr,
                          'TestPatchFilter')

        recipestr = """
class TestPatchFilter(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        # avoid cleanup
        r.addSource('unpatched1')
        r.addSource('unpatched2')
        r.addPatch('patchToFilter.patch', filter='sed s/Oops// |cat | cat')
"""

        self.buildRecipe(recipestr, 'TestPatchFilter')

    def testAction(self):
        recipestr1 = """
class TestAction(PackageRecipe):
    name = 'tmpwatch'
    version = '2.9.0'
    clearBuildReqs()

    def cleanup(r, builddir, destdir):
        pass

    def setup(r):
        r.addAction('mkdir asdf')
        r.addAction('touch foo', dir='asdf')
"""
        self.resetWork()
        (built, d) = self.buildRecipe(recipestr1, "TestAction")
        # should not raise an error
        os.stat(util.joinPaths(self.buildDir, 'tmpwatch/tmpwatch-2.9.0/asdf/foo'))

    def findInFile(self, filename, key):
        f = open(filename)
        contents = f.read()
        return contents.find(key)


    def testAutoSourcePermissions(self):
        permsRecipe = """\
class TestPerms(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addSource('dc_client.init', rpm='distcache-1.4.5-2.src.rpm')
        r.Run('test -x dc_client.init')
        # get rid of "cowardlily refusing" message
        r.Create('/foo')
"""
        self.resetWork()
        self.resetRepository()
        self.repos = self.openRepository()
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('test')
        os.chdir('test')
        self.writeFile('test.recipe', permsRecipe)
        self.addfile('test.recipe')
        self.commit()
        os.chdir('..')
        shutil.rmtree('test')
        os.chdir(origDir)
        self.resetCache()
        # this should fail if permissions are not restored
        self.cookItem(self.repos, self.cfg, 'test', requireCleanSources=False)
        

    def testAutoMainDir(self):
        """
        Test mainDir automagic guessing.
        """

        recipestr1 = """
class TestSource(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        # asdf.tar.gz contains asdf/fdsa and bam, not test-1/fdsa
        r.addArchive('asdf.tar.gz')
        # the next line will only work if mainDir was auto-set to asdf
        r.Install('fdsa', '/')
"""
        (built, d) = self.buildRecipe(recipestr1, "TestSource")

        recipestr2 = """
class TestSource(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        # asdf.tar.gz contains asdf/fdsa and bam, not test-1/fdsa
        r.addArchive('asdf.tar.gz', dir='blah')
        # the next line will only work if mainDir was auto-set to blah
        r.Install('asdf/fdsa', '/')
"""
        (built, d) = self.buildRecipe(recipestr2, "TestSource")

        recipestr3 = """
class TestSource(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        # foo-1.0.tar.gz has foo-1.0/a and blah/b
        r.addArchive('foo-1.0.tar.gz')
        r.Install('a', '/')
"""
        (built, d) = self.buildRecipe(recipestr3, "TestSource")

        # Test for special characters in the filename
        recipestr4 = """
class TestSource(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        # foo-1.0.tar.gz has foo-1.0/a and blah/b
        r.addArchive('foo-1.0&;.tar.gz')
        r.Install('a', '/')
"""
        (built, d) = self.buildRecipe(recipestr4, "TestSource")


    def testAutoMainDirGuessFailure(self):
        recipestr = """
class TestSource(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        # foo-1.0.tar.gz has foo-1.0/a and blah/b
        r.addSource('macros')
        r.addArchive('distcc-2.9.tar.bz2')
"""
        self.assertRaises(source.SourceError, self.buildRecipe, recipestr,
                        "TestSource")

    def testSourceMagic(self):
        d = tempfile.mkdtemp()
        try:
            # copy setup.tar.gz to a file without .gz extension
            shutil.copyfile(resources.get_archive() + '/asdf.tar.gz',
                            d + '/asdf')
            # look in our new source directory for sources
            self.cfg.sourceSearchDir = d
            r = policytest.DummyRecipe(self.cfg)
            # test the Archive class when the archive does not end in .gz
            os.mkdir('/'.join((r.macros.builddir, r.theMainDir)))
            a = source.Archive(r, 'asdf')
            a.doAction()
            assert(os.path.isdir(r.macros.builddir + '/asdf'))
            assert(os.path.isfile(r.macros.builddir + '/asdf/fdsa'))
        finally:
            shutil.rmtree(d)

    def testAddBadPatch(self):
        # make sure we don't get a Y/N prompt when a patch fails to apply
        recipestr1 = """\
class PatchTest(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm')
        r.addPatch('unrelated.patch')
"""
        self.logFilter.add()
        rc = self.captureOutput(self.buildRecipe, recipestr1, "PatchTest",
                                _returnException=True, logLevel=log.INFO)
        self.logFilter.remove()
        msg = '\n'.join(x for x in self.logFilter.records if 'patch' in x)
        expected = """+ attempting to apply /unrelated.patch to /test/tmpwatch-2.9.0/ with patch level(s) 1, 0, 2, 3
+ patch did not apply with --dry-run, trying level 1 directly
+ patch level 1 FAILED
+ can't find file to patch at input line 3
Perhaps you used the wrong -p or --strip option?
The text leading up to this was:
--------------------------
|--- foo1\t2006-06-19 21:52:47.000000000 -0400
|+++ foo2\t2006-06-19 21:52:50.000000000 -0400
--------------------------
File to patch: 
Skip this patch? [y] 
Skipping patch.
1 out of 1 hunk ignored
+ patch level 0 FAILED
+ can't find file to patch at input line 3
Perhaps you used the wrong -p or --strip option?
The text leading up to this was:
--------------------------
|--- foo1\t2006-06-19 21:52:47.000000000 -0400
|+++ foo2\t2006-06-19 21:52:50.000000000 -0400
--------------------------
File to patch: 
Skip this patch? [y] 
Skipping patch.
1 out of 1 hunk ignored
+ patch level 2 FAILED
+ can't find file to patch at input line 3
Perhaps you used the wrong -p or --strip option?
The text leading up to this was:
--------------------------
|--- foo1\t2006-06-19 21:52:47.000000000 -0400
|+++ foo2\t2006-06-19 21:52:50.000000000 -0400
--------------------------
File to patch: 
Skip this patch? [y] 
Skipping patch.
1 out of 1 hunk ignored
+ patch level 3 FAILED
+ can't find file to patch at input line 3
Perhaps you used the wrong -p or --strip option?
The text leading up to this was:
--------------------------
|--- foo1\t2006-06-19 21:52:47.000000000 -0400
|+++ foo2\t2006-06-19 21:52:50.000000000 -0400
--------------------------
File to patch: 
Skip this patch? [y] 
Skipping patch.
1 out of 1 hunk ignored
error: could not apply patch /unrelated.patch in directory /test/tmpwatch-2.9.0/"""
        # normalize variable paths in the message
        msg = msg.replace(self.buildDir, '')
        msg = msg.replace(self.sourceSearchDir, '')
        # centos behavioral differences
        msg = msg.replace(
                'missing header for unified diff at line 3 of patch\n', '')
        self.assertEqual(msg, expected)
        if rc[0].__class__ != source.SourceError:
            self.fail('expected SourceError exception not raised')
        # make sure no stdout/stderr output was produced
        if rc[1]:
            self.fail('unexpected output: %s' %rc[1])

    def testAddGoodPatch(self):
        recipestr1 = """\
class PatchTest(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm')
        r.addPatch('tmpwatch.fakebug.patch')
"""
        self.logFilter.add()
        rc = self.captureOutput(self.buildRecipe, recipestr1, "PatchTest",
                                logLevel=log.INFO)
        self.logFilter.remove()
        msg = '\n'.join(x for x in self.logFilter.records if 'patch' in x)
        expected = """+ attempting to apply /tmpwatch.fakebug.patch to /test/tmpwatch-2.9.0/ with patch level(s) 1, 0, 2, 3
+ patching file tmpwatch.c
+ applied successfully with patch level 1"""
        # normalize variable paths in the message
        msg = msg.replace(self.buildDir, '')
        msg = msg.replace(self.sourceSearchDir, '')
        self.assertEqual(msg, expected)
        # make sure no stdout/stderr output was produced
        if rc[1]:
            self.fail('unexpected output: %s' %rc[1])

    def testAddGoodLevel0Patch(self):
        recipestr1 = """\
class PatchTest(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm')
        r.addPatch('tmpwatch.fakebug.level0.patch')
"""
        self.logFilter.add()
        rc = self.captureOutput(self.buildRecipe, recipestr1, "PatchTest",
                                logLevel=log.INFO)
        self.logFilter.remove()
        msg = '\n'.join(x for x in self.logFilter.records if 'patch' in x)
        expected = """+ attempting to apply /tmpwatch.fakebug.level0.patch to /test/tmpwatch-2.9.0/ with patch level(s) 1, 0, 2, 3
+ patching file tmpwatch.c
+ applied successfully with patch level 0"""

        # normalize variable paths in the message
        msg = msg.replace(self.buildDir, '')
        msg = msg.replace(self.sourceSearchDir, '')
        self.assertEqual(msg, expected)
        # make sure no stdout/stderr output was produced
        if rc[1]:
            self.fail('unexpected output: %s' %rc[1])

    def testAddGoodButRejectedPatch(self):
        recipestr1 = """\
class PatchTest(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm')
        r.addPatch('tmpwatch.fakebug.rej.patch')
"""
        self.logFilter.add()
        rc = self.captureOutput(self.buildRecipe, recipestr1, "PatchTest",
                                _returnException=True, logLevel=log.INFO)
        self.logFilter.remove()
        msg = '\n'.join(x for x in self.logFilter.records if 'patch' in x)
        expected = """+ attempting to apply /tmpwatch.fakebug.rej.patch to /test/tmpwatch-2.9.0/ with patch level(s) 1, 0, 2, 3
+ patch did not apply with --dry-run, trying level 1 directly
+ patch level 1 FAILED
+ patching file tmpwatch.c
Hunk #1 FAILED at 419.
1 out of 1 hunk FAILED -- saving rejects to file tmpwatch.c.rej
+ patch level 0 failed - probably wrong level
+ patch level 2 failed - probably wrong level
+ patch level 3 failed - probably wrong level
error: could not apply patch /tmpwatch.fakebug.rej.patch in directory /test/tmpwatch-2.9.0/"""
        # normalize variable paths in the message
        msg = msg.replace(self.buildDir, '')
        msg = msg.replace(self.sourceSearchDir, '')
        self.assertEqual(msg, expected)
        # make sure no stdout/stderr output was produced
        if rc[1]:
            self.fail('unexpected output: %s' %rc[1])
        # make sure we've written out the reject file
        assert(os.path.exists(self.buildDir + '/test/tmpwatch-2.9.0/tmpwatch.c.rej'))

    def testAddPartiallyApplicablePatch(self):
        # patch partially applies at level one and completely
        # applies at level 2.
        recipestr1 = """\
class PatchTest(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        contents = '\\n'.join(str(x) for x in range(1, 21)) + '\\n'
        r.addSource('foo', contents=contents)
        r.addSource('bar', contents=contents)
        r.addAction('mkdir subdir; cp foo subdir/; touch subdir/bar')
        r.addPatch('partial.patch')
"""
        self.logFilter.add()
        rc = self.captureOutput(self.buildRecipe, recipestr1, "PatchTest",
                                _returnException=True, logLevel=log.INFO)
        self.logFilter.remove()
        # patch does not partially apply with -p1
        assert('change' not in open(self.buildDir + '/test/test-1.0/subdir/foo').read())
        # patch applies with -p2
        assert('change' in open(self.buildDir + '/test/test-1.0/foo').read())

    def testPatchSameFileTwiceInOnePatch(self):
        # CNY-2142
        recipestr1 = """\
class PatchTest(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        contents = '\\n'.join(str(x) for x in range(1, 11)) + '\\n'
        r.addSource('foo', contents=contents)
        r.addPatch('patches/foo.patch')
"""
        self.logFilter.add()
        rc = self.captureOutput(self.buildRecipe, recipestr1, "PatchTest",
                                logLevel=log.INFO)
        self.logFilter.remove()
        assert('444' in open(self.buildDir + '/test/test-1.0/foo').read())
        recipestr1 = recipestr1.replace("foo.patch'", "foo.patch', level=1")
        shutil.rmtree(self.buildDir + '/test')
        rc = self.captureOutput(self.buildRecipe, recipestr1, "PatchTest",
                                logLevel=log.INFO)
        assert('444' in open(self.buildDir + '/test/test-1.0/foo').read())

    def testLargePatch(self):
        # Test a patch that's large enough to cause our patch pipe
        # to be full - this "patch" would hang if we didn't have good
        # pipe handling.  Done correctly, it just fails.
        recipestr1 = """\
class PatchTest(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm = 'tmpwatch-2.9.0-2.src.rpm')
        r.addPatch('tmpwatch-2.9.0.tar.gz', rpm = 'tmpwatch-2.9.0-2.src.rpm')
"""
        self.assertRaises(source.SourceError, self.captureOutput,
                      self.buildRecipe,
                      recipestr1, "PatchTest")

    def testMissingPatchProgram(self):
        recipestr = """\
class PatchTest(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addPatch('tmpwatch.fakebug.patch', patchName='noSuchPatchProgram')
"""
        self.logFilter.add()
        self.assertRaises(source.SourceError,
              self.buildRecipe, recipestr, "PatchTest")
        self.logFilter.remove()
        self.logFilter.compare([
            'warning: Failed to find possible build requirement for path "noSuchPatchProgram"',
        ])

    def testTarPermissions(self):
        r = policytest.DummyRecipe(self.cfg)
        os.mkdir('/'.join((r.macros.builddir, r.theMainDir)))
        # add an archive that has a file with group write permissions
        a = source.Archive(r, 'group-write.tar.gz', dir='/')
        a.doAction()
        sb = os.stat(r.macros.destdir + '/foo/foo')
        # make sure that the group write bit is set
        assert(sb.st_mode & stat.S_IWGRP)

    def testDeb(self):
        r = policytest.DummyRecipe(self.cfg)
        os.mkdir('/'.join((r.macros.builddir, r.theMainDir)))
        # add an archive that has a file with group write permissions
        a = source.Archive(r, 'bash.deb', dir='/')
        a.doAction()
        self.assertTrue(os.path.isfile(r.macros.destdir + '/bin/bash'))
        self.assertTrue(os.path.islink(r.macros.destdir + '/bin/sh'))

    def testBzipDeb(self):
        r = policytest.DummyRecipe(self.cfg)
        os.mkdir('/'.join((r.macros.builddir, r.theMainDir)))
        # add an archive that has a file with group write permissions
        a = source.Archive(r, 'test.deb', dir='/')
        a.doAction()
        self.assertTrue(os.path.isfile(r.macros.destdir + '/testme'))

    @decorators.requireBinary("xz")
    def testLZMADeb(self):
        r = policytest.DummyRecipe(self.cfg)
        os.mkdir('/'.join((r.macros.builddir, r.theMainDir)))
        a = source.Archive(r, 'testlzma.deb', dir='/')
        a.doAction()
        self.assertTrue(os.path.isfile(r.macros.destdir + '/testme'))

    def testBzipDebControl(self):
        r = policytest.DummyRecipe(self.cfg)
        os.mkdir('/'.join((r.macros.builddir, r.theMainDir)))
        # add an archive that has a file with group write permissions
        a = source.Archive(r, 'test.deb', debArchive='control.tar', dir='/')
        a.doAction()
        self.assertTrue(os.path.isfile(r.macros.destdir + '/controlData'))

    def testEmptyDeb(self):
        r = policytest.DummyRecipe(self.cfg)
        os.mkdir('/'.join((r.macros.builddir, r.theMainDir)))
        # add an archive that has a file with group write permissions
        a = source.Archive(r, 'testempty.deb', dir='/')
        self.assertRaises(source.SourceError, a.doAction)

    def testAllPermissionsRetention(self):
        recipestr = """
class TestTar(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('allperms.tar', dir = '/tar',
                     preserveOwnership = True,
                     preserveSetid = True,
                     preserveDirectories = True)
        r.ExcludeDirectories(exceptions = [ '.*/foo' ] )
"""
        (built, d) = self.buildRecipe(recipestr, "TestTar")
        trvInfo = (built[0][0], VFS(built[0][1]), built[0][2])
        repos = self.openRepository()
        cs = repos.createChangeSet(
                [ (trvInfo[0], (None, None), (trvInfo[1], trvInfo[2]), True) ] )
        trvCs = cs.getNewTroveVersion(*trvInfo)
        trv = trovemod.Trove(trvCs)
        actual = {}
        for pathId, path, fileId, version in trv.iterFileList():
            stream = cs.getFileChange(None, fileId)
            fObj = files.ThawFile(stream, pathId)
            actual[path] = (fObj.inode.owner(),
                            fObj.inode.group(),
                            fObj.inode.permsString())

        expected = {
            '/tar/allperms/empty':
                ('root', 'root', 'rwxr-xr-x'),
            '/tar/allperms/normaldir/normalfile':
                ('root', 'root', 'rw-r--r--'),
            '/tar/allperms/permsdir':
                ('root', 'root', 'rwx------'),
            '/tar/allperms/permsdir/notempty':
                ('root', 'root', 'rw-r--r--'),
            '/tar/allperms/owneddir':
                ('bin', 'daemon', 'rwxr-xr-x'),
            '/tar/allperms/owneddir/ownedfile':
                ('nobody', 'nobody', 'rw-r--r--'),
            '/tar/allperms/setuid':
                ('bin', 'daemon', 'rwsr-xr-x'),
            '/tar/allperms/setgid':
                ('bin', 'daemon', 'rwxr-sr-x'),
            '/tar/allperms/setudir':
                ('root', 'root', 'rwsr-xr-x'),
            '/tar/allperms/setgdir':
                ('root', 'root', 'rwxr-sr-x'),
        }

        if actual != expected:
            l = []
            extra = sorted(list(set(actual.keys()) - set(expected.keys())))
            for m in extra:
                l.append('%s is extra' %m)
            for path in actual.keys():
                if path in expected and actual[path] != expected[path]:
                    l.append('%s should be %s:%s %s but is %s:%s %s'
                             %((path,) + expected[path] + actual[path]))
            missing = sorted(list(set(expected.keys()) - set(actual.keys())))
            for m in missing:
                l.append('%s is missing' %m)
            msg = '\n'.join(l)
            self.fail(msg)

    def testOwnershipRetention(self):
        recipestr = """
class TestTar(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('ownerships.tar', dir = '/tar',
                     preserveOwnership = True)
#        r.addArchive('ownerships.cpio.gz', dir = '/cpio',
#                     preserveOwnership = True)
#        r.addArchive('ownerships.cpio.gz', dir = '/',
#                     preserveOwnership = True)
        r.addArchive('ownerships-1.0-1.i386.rpm', dir = '/rpm',
                     preserveOwnership = True)
        r.addArchive('dotslashownerships.tar', dir = '/dotslash',
                     preserveOwnership = True)
        r.addArchive('ownerships_spaces.tar', dir='/spaces',
                preserveOwnership=True)
        r.ExcludeDirectories(exceptions = [ '.*/foo', '/spaces/.*' ] )
"""
        (built, d) = self.buildRecipe(recipestr, "TestTar")
        trvInfo = (built[0][0], VFS(built[0][1]), built[0][2])
        repos = self.openRepository()
        cs = repos.createChangeSet(
                [ (trvInfo[0], (None, None), (trvInfo[1], trvInfo[2]), True) ] )
        trvCs = cs.getNewTroveVersion(*trvInfo)
        trv = trovemod.Trove(trvCs)
        actual = {}
        for pathId, path, fileId, version in trv.iterFileList():
            stream = cs.getFileChange(None, fileId)
            fObj = files.ThawFile(stream, pathId)
            actual[path] = (fObj.inode.owner(), fObj.inode.group())

        expected = {
#                  '/cpio/foo':        ('bin', 'bin'),
#                  '/cpio/foo/first':  ('bin', 'daemon'),
#                  '/cpio/foo/fourth': ('nobody', 'nobody'),
#                  '/cpio/foo/second': ('postfix', 'rmake'),
#                  '/cpio/foo/third':  ('ident', 'dovecot'),
#                  '/foo':        ('bin', 'bin'),
#                  '/foo/first':  ('bin', 'daemon'),
#                  '/foo/fourth': ('nobody', 'nobody'),
#                  '/foo/second': ('postfix', 'rmake'),
#                  '/foo/third':  ('ident', 'dovecot'),
                  '/rpm/foo':        ('ewt', 'ewt'),
                  '/rpm/foo/first':  ('bin', 'daemon'),
                  '/rpm/foo/fourth': ('nobody', 'nobody'),
                  '/rpm/foo/second': ('postfix', 'rmake'),
                  '/rpm/foo/third':  ('ident', 'dovecot'),
                  '/tar/foo':         ('msw', 'msw'),
                  '/tar/foo/first':   ('bin', 'daemon'),
                  '/tar/foo/fourth':  ('nobody', 'nobody'),
                  '/tar/foo/second':   ('ident', 'dovecot'),
                  '/tar/foo/third':  ('postfix', 'rmake'),
                  '/dotslash/foo':         ('msw', 'msw'),
                  '/dotslash/foo/first':   ('bin', 'daemon'),
                  '/dotslash/foo/fourth':  ('nobody', 'nobody'),
                  '/dotslash/foo/second':  ('postfix', 'rmake'),
                  '/dotslash/foo/third':   ('ident', 'dovecot'),
                  '/spaces/without':                ('nobody', 'nobody'),
                  '/spaces/without/bork bork':      ('nobody', 'nobody'),
                  '/spaces/with spaces':            ('nobody', 'nobody'),
                  '/spaces/with spaces/foo':        ('nobody', 'nobody'),
                  '/spaces/with spaces/bar baz':    ('nobody', 'nobody'),
                }

        if actual != expected:
            l = []
            for path in actual.keys():
                if actual[path] != expected[path]:
                    l.append('%s should be %s:%s but is %s:%s'
                             %((path,) + expected[path] + actual[path]))
            msg = '\n'.join(l)
            self.fail(msg)

        recipestr = """
class TestTar(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('ownerships.tar', preserveOwnership = True)
"""
        try:
            self.buildRecipe(recipestr, "TestTar")
        except source.SourceError, e:
            assert(str(e) == 'preserveOwnership, preserveSetid, and '
                             'preserveDirectories not allowed when '
                             'unpacking into build directory')
        else:
            assert(0)

        recipestr = """
class TestTar(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('rrcd.iso', preserveOwnership = True, dir = '/')
"""

        try:
            self.buildRecipe(recipestr, "TestTar")
        except source.SourceError, e:
            assert(str(e) == 'cannot preserveOwnership, preserveSetid, or '
                             'preserveDirectories for iso images')
        else:
            assert(0)

        recipestr = """
class TestTar(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('ownerships.zip', preserveOwnership = True, dir = '/')
"""

        try:
            self.buildRecipe(recipestr, "TestTar")
        except source.SourceError, e:
            assert(str(e) == 'cannot preserveOwnership, preserveSetid, or '
                             'preserveDirectories for xpi or zip archives')
        else:
            assert(0)

    @decorators.requireBinary("lzop")
    def testSourceLzop(self):
        recipestr = """
class TestSource(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('foo.tar.lzo', dir='/')
"""
        built = self.buildRecipe(recipestr, 'TestSource')[0]
        assert built

class TestBadaddSource(rephelp.RepositoryHelper):
    def testBadaddSource(self):
        recipestr="""
class Yo(PackageRecipe):
    name = 'yo'
    version = '1.3'
    clearBuildReqs()

    def setup(r):
        r.addSource('http://foo.bar.com/directory/', dest='/mumble/')
"""

        try:
            self.buildRecipe(recipestr, "Yo")
        except source.SourceError, e:
            assert(str(e) == 'cannot specify a directory as input to addSource')
        else:
            assert(False)

    def testAddSourceDir(self):
        recipeStr = """
class TestAddSourceDir(PackageRecipe):
    name = 'addsource'
    version = '1.9'
    clearBuildReqs()

    def setup(r):
        r.addSource('test', sourceDir = '')"""

        class Stop(Exception):
            pass

        def mockDoDownload(x):
            f = x._findSource(x.httpHeaders)
            self.assertEquals(f, os.path.join(x.builddir,
                                              x.recipe.macros.maindir,
                                              'test'))
            raise Stop

        def mockFindAll(*args, **kwargs):
            raise AssertionError('lookaside cache should not have been used')

        self.mock(source.addSource, 'doDownload', mockDoDownload)
        self.mock(lookaside, 'findAll', mockFindAll)

        self.assertRaises(Stop, self.buildRecipe,
                recipeStr, "TestAddSourceDir")


    def testAddMaindirPatch(self):
        recipestr = """
class TestSource(PackageRecipe):
    name = 'tmpwatch'
    version = '2.9.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm')
        r.addSource('mkinitrd.spec', rpm='rpm-with-bzip-5.0.29-1.src.rpm')
        r.addArchive('rpm-with-bzip-5.0.29-1.i386.rpm')

        # add patch file so it exists in builddir
        r.addArchive('tmpwatch.fakebug.patch.tgz', dir = '%(maindir)s')
        # test applying a patch using sourceDir
        r.addPatch('tmpwatch.fakebug.patch', sourceDir='')"""

        # this line will fail if the patch cannot be added
        self.buildRecipe(recipestr, "TestSource")

    def testAddPatchGlob(self):
        recipestr = """
class TestSource(PackageRecipe):
    name = 'tmpwatch'
    version = '3.9.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive('tmpwatch-2.9.0.tar.gz', rpm='tmpwatch-2.9.0-2.src.rpm')
        r.addSource('mkinitrd.spec', rpm='rpm-with-bzip-5.0.29-1.src.rpm')
        r.addArchive('rpm-with-bzip-5.0.29-1.i386.rpm')

        # add patch file so it exists in builddir
        r.addArchive('tmpwatch.fakebug.patch.tgz', dir = '%(maindir)s')
        # test applying a patch using globs and sourceDir
        r.addPatch('tmpwatch.fakebug.*', sourceDir='')"""

        # this line will fail if the patch cannot be added
        self.buildRecipe(recipestr, "TestSource")

    def testAddPatchBadGlob(self):
        recipestr = """
class TestSource(PackageRecipe):
    name = 'tmpwatch'
    version = '2.4.0'
    clearBuildReqs()

    def setup(r):
        # test applying a patch using globs without sourceDir. this will fail
        r.addPatch('tmpwatch.fakebug.*')"""

        # globs for addPatch are only used if soruceDir is defined.
        try:
            self.buildRecipe(recipestr, "TestSource")
        except OSError, e:
            if e.errno != 2:
                raise
            self.assertEquals(str(e),
                  "[Errno 2] No such file or directory: 'tmpwatch.fakebug.*'")
        else:
            self.fail("addPatch on bad filename should have failed")

    def testAddPatchSortOrder(self):
        class TestAddPatch(source.addPatch):
            sourceDir = '.'
            def __init__(x):
                pass
            def _findSource(x, httpArgs = {}, braceGlob = False):
                return ["3", "2", "1"]
            _checkSignature = lambda *args, **kwargs: None
            def doFile(x, path):
                self.orderedCalls.append(path)

        self.orderedCalls = []
        addPatch = TestAddPatch()
        addPatch.do()
        self.assertEquals(self.orderedCalls, ["1", "2", "3"])

    def _getHTTPServer(self, logFile):
        class FileHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
            count = 0
            def log_message(slf, *args, **kw):
                file(logFile, "a").write("%s\n" % slf.path)

            def do_GET(slf):
                if slf.path.endswith('/count'):
                    contentType = 'text/plain'
                    archpath = self.workDir + '/count'
                    FileHandler.count += 1
                    open(archpath, 'w').write('%s\n' % slf.count)
                elif (slf.path.startswith("/bad/") or
                    not slf.path.endswith('.tar.gz')):
                #if (slf.path.startswith("/bad/") or
                #        not (slf.path.endswith('.tar.gz') or
                #             slf.path.endswith('.tar.gz.asc'))):
                    slf.send_response(404)
                    slf.end_headers()
                    return
                elif slf.path.endswith('.tar.gz'):
                    archpath = os.path.join(resources.get_archive(), "foo-1.0.tar.gz")
                    contentType = "application/x-gzip"
                else:
                    archpath = os.path.join(self.workDir, "foo.asc")
                    file(archpath, "w+").write("Pretend this is a PGP sig")
                    contentType = 'application/pgp-signature'
                fileSize = os.stat(archpath).st_size
                slf.send_response(200)
                slf.send_header("Content-Type", contentType)
                slf.send_header("Content-Length", fileSize)
                slf.end_headers()
                util.copyStream(open(archpath), slf.wfile)

        hs = rephelp.HTTPServerController(FileHandler)
        return hs

    def testAddArchiveMultiURL(self):

        recipestr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive([
            "http://localhost:%(port)s/bad/",
            "http://localhost:%(port)s/good/",
        ], dir="/usr/share/foo/")
"""

        recipestrSrc = """
class FooSrc(PackageRecipe):
    name = 'foosrc'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addSource([
            "http://localhost:%(port)s/bad/foo-1.99.tar.gz",
            "http://localhost:%(port)s/good/",
        ], dir="/usr/share/foo/")
"""

        logFile = os.path.join(self.workDir, "webserver.log")
        hs = self._getHTTPServer(logFile)
        try:
            trv = self.build(recipestr % dict(port = hs.port), "Foo")
            self.assertEqual(sorted([ x[1] for x in trv.iterFileList() ]),
                                 sorted(['/usr/share/foo/blah/b',
                                         '/usr/share/foo/bam',
                                         '/usr/share/foo/foo-1.0/a']))

            # Specify an archive name
            recipestr2 = recipestr.replace("good/", "good/foo-1.1.tar.gz")
            trv = self.build(recipestr2 % dict(port = hs.port), "Foo")

            # Conflicting archive names
            recipestr3 = recipestr2.replace("bad/", "bad/blam-1.1.tar.gz")
            err = self.assertRaises(errors.CookError,
                        self.build, recipestr3 % dict(port = hs.port), "Foo")
            # White out the first part of the error
            strerr = str(err)
            strerr = strerr[strerr.find('SourceError: '):]
            self.assertEqual(strerr,
                "SourceError: Inconsistent archive names: 'blam-1.1.tar.gz' and 'foo-1.1.tar.gz'")

            recipeData = recipestrSrc % dict(port = hs.port)

            # Now test addSource
            trv = self.build(recipeData, "FooSrc")
            self.assertEqual([ x[1] for x in trv.iterFileList() ],
                                 ['/usr/share/foo/foo-1.99.tar.gz'])

            # RDST-2848
            # Try to commit the multi-URL recipe
            os.chdir(self.workDir)
            self.newpkg('foosrc')
            os.chdir('foosrc')
            open("foosrc.recipe", "w").write(recipeData)
            self.commit()
        finally:
            hs.close()

    def testAddArchivePassword(self):
        recipestr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addSource("http://foo:bar@localhost:%(port)s/good/count",
                     dest='/foo')
"""
        logFile = os.path.join(self.workDir, "webserver.log")
        hs = self._getHTTPServer(logFile)
        try:
            os.chdir(self.workDir)
            self.newpkg('foo')
            os.chdir('foo')
            self.writeFile('foo.recipe', recipestr % dict(port=hs.port))
            self.addfile('foo.recipe')
            self.commit()
            self.refresh()
            self.commit()
            xx = self.cookItem(self.openRepository(), self.cfg, 'foo')
            self.updatePkg('foo')
            self.assertEquals(open(self.cfg.root + '/foo').read(), '2\n')
        finally:
            hs.close()

    def testMultiURLMirror(self):

        recipestr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive([
            "mirror://alif/%(rest)s",
            "mirror://ba/%(rest)s",
        ], dir="/usr/share/foo/")
"""
        logFile = os.path.join(self.workDir, "webserver.log")
        hs = self._getHTTPServer(logFile)

        mirrorsDir = os.path.join(self.workDir, "mirrors")
        util.mkdirChain(mirrorsDir)
        file(os.path.join(mirrorsDir, "alif"), "w").write(
            "http://localhost:%s/bad/b1/\nhttp://localhost:%s/bad/b2/\n" %
                (hs.port, hs.port))
        file(os.path.join(mirrorsDir, "ba"), "w").write(
            "http://localhost:%s/bad/worst/\nhttp://localhost:%s/good/\n" %
                (hs.port, hs.port))
        self.cfg.mirrorDirs = [ mirrorsDir ]
        try:
            trv = self.build(recipestr % dict(rest = ''), "Foo")
            self.assertEqual(sorted([ x[1] for x in trv.iterFileList() ]),
                                 sorted(['/usr/share/foo/blah/b',
                                         '/usr/share/foo/bam',
                                         '/usr/share/foo/foo-1.0/a']))
            lines = [ x.strip() for x in open(logFile) ]
            self.assertEqual(lines[-1], '/good/foo-1.0.tar.gz')
            # Make sure we hit all URLs
            badUrls = set('/'.join(x.split('/')[:3]) for x in lines
                          if x.startswith('/bad/'))
            self.assertEqual(badUrls,
                                 set(['/bad/b1', '/bad/b2', '/bad/worst']))

            # CNY-2778
            # Reset log file
            file(logFile, "w+")
            # Reset cache
            util.rmtree(self.cacheDir, ignore_errors = True)
            # Now build again, with the archive as part of the multi-url
            trv = self.build(recipestr %
                dict(rest = '%(name)s-%(version)s.tar.gz'), "Foo")
            lines = [ x.strip() for x in open(logFile) ]
            self.assertEqual(lines[-1], '/good/foo-1.0.tar.gz')
            # Make sure we hit all URLs
            badUrls = set('/'.join(x.split('/')[:3]) for x in lines
                          if x.startswith('/bad/'))
            self.assertEqual(badUrls,
                                 set(['/bad/b1', '/bad/b2', '/bad/worst']))
        finally:
            hs.close()

    def testMirrorLookaside(self):
        # CNY-2696
        # There was a bug in the way the mirror name was computed

        recipestr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addArchive("mirror://alif/blah/", keyid = 'DEADBEEF',
                     dir="/usr/share/foo/")
"""
        logFile = os.path.join(self.workDir, "webserver.log")
        hs = self._getHTTPServer(logFile)

        mirrorsDir = os.path.join(self.workDir, "mirrors")
        util.mkdirChain(mirrorsDir)
        file(os.path.join(mirrorsDir, "alif"), "w").write(
            "http://localhost:%s/bad/b1/\nhttp://localhost:%s/good/b2/\n" %
                (hs.port, hs.port))
        self.cfg.mirrorDirs = [ mirrorsDir ]
        try:
            trv = self.build(recipestr, "Foo")
            self.assertEqual(sorted([ x[1] for x in trv.iterFileList() ]),
                                 sorted(['/usr/share/foo/blah/b',
                                         '/usr/share/foo/bam',
                                         '/usr/share/foo/foo-1.0/a']))
            lines = [ x.strip() for x in open(logFile) ]
            goodLines = [ x for x in lines if x.startswith('/good/')
                                              and x.endswith('.tar.gz') ]
            self.assertEqual(goodLines,
                [ '/good/b2/blah/foo-1.0.tar.gz' ])
            goodLines = [ x for x in lines if x.endswith('.tar.gz.sig') ]
            self.assertEqual(goodLines,
                [ '/bad/b1/blah/foo-1.0.tar.gz.sig',
                  '/good/b2/blah/foo-1.0.tar.gz.sig' ])
        finally:
            hs.close()

    @testhelp.context('CNY-2627')
    def testRpmLookaside(self):
        recipeStr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        # prove that the sourcedir level foo isn't even looked up if rpm is
        # specified
        r.addArchive('foo-1.0-1.src.rpm')
        r.addSource('foo', dir = '/bar', rpm = 'foo-1.0-1.src.rpm')
"""
        # create a source component that carries a file named foo
        # we're trying to test that the foo file is added from the
        # foo-1.0-1.src.rpm, which incidentally is blank (CNY-2627)
        self.addComponent('foo:source=1',
                [('foo.recipe', recipeStr),
                ('foo', 'wrong stuff'),
                ('foo-1.0-1.src.rpm', open(os.path.join(resources.get_archive(),
                                        'foo-1.0-1.src.rpm')).read())])
        client = self.getConaryClient()
        repos = client.getRepos()
        built, csf = self.cookItem(repos, self.cfg, 'foo')
        nvf = built[0]
        nvf = repos.findTrove(None, nvf)[0]
        fileDict = client.getFilesFromTrove(*nvf)
        self.assertNotEquals(fileDict['/bar/foo'].read(), 'wrong stuff')
        rpmLookaside = open(os.path.join(self.cfg.lookaside,
                '=RPM_CONTENTS=', 'foo', 'foo-1.0-1.src.rpm', 'foo')).read()
        self.assertEquals(rpmLookaside, '')
        fooLookasidePath = os.path.join(self.cfg.lookaside,'foo', 'foo')
        self.assertFalse(os.path.exists(fooLookasidePath),
                "foo file from sourcedir should not have been referenced.")

    @testhelp.context('CNY-2627')
    def testRpmLookaside2(self):
        recipeStr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        # now prove that both can be used at the same time
        r.addArchive('foo-1.0-1.src.rpm')
        # add the sorucedir foo first to "poison" the lookaside
        r.addSource('foo', dir = '/baz')
        r.addSource('foo', dir = '/bar', rpm = 'foo-1.0-1.src.rpm')
"""
        # create a source component that carries a file named foo
        # we're trying to test that the foo file is added from the
        # foo-1.0-1.src.rpm, which incidentally is blank (CNY-2627)
        self.addComponent('foo:source=1',
                [('foo.recipe', recipeStr),
                ('foo', 'wrong stuff'),
                ('foo-1.0-1.src.rpm', open(os.path.join(resources.get_archive(),
                                        'foo-1.0-1.src.rpm')).read())])
        client = self.getConaryClient()
        repos = client.getRepos()
        built, csf = self.cookItem(repos, self.cfg, 'foo')
        nvf = built[0]
        nvf = repos.findTrove(None, nvf)[0]
        fileDict = client.getFilesFromTrove(*nvf)
        self.assertNotEquals(fileDict['/bar/foo'].read(), 'wrong stuff')
        rpmLookaside = open(os.path.join(self.cfg.lookaside,
                '=RPM_CONTENTS=', 'foo', 'foo-1.0-1.src.rpm', 'foo')).read()
        self.assertEquals(rpmLookaside, '')
        fooLookasidePath = os.path.join(self.cfg.lookaside,'foo', 'foo')
        self.assertEquals(open(fooLookasidePath).read(), 'wrong stuff')
        self.assertEquals(fileDict['/baz/foo'].read(), 'wrong stuff')

    @testhelp.context('CNY-2627')
    def testRpmLookaside3(self):
        recipeStr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        # now prove that two rpm's can have distinct contents at once
        r.addArchive('foo-1.0-1.src.rpm')
        r.addArchive('foo-1.0-2.src.rpm')
        r.addSource('foo.spec', dir = '/1', rpm = 'foo-1.0-1.src.rpm')
        r.addSource('foo.spec', dir = '/2', rpm = 'foo-1.0-2.src.rpm')
"""
        # create a source component that carries a file named foo
        # we're trying to test that the foo file is added from the
        # foo-1.0-1.src.rpm, which incidentally is blank (CNY-2627)
        self.addComponent('foo:source=1',
                [('foo.recipe', recipeStr),
                ('foo.spec', 'wrong stuff'),
                ('foo-1.0-1.src.rpm', open(os.path.join(resources.get_archive(),
                                        'foo-1.0-1.src.rpm')).read()),
                ('foo-1.0-2.src.rpm', open(os.path.join(resources.get_archive(),
                                        'foo-1.0-2.src.rpm')).read())])
        client = self.getConaryClient()
        repos = client.getRepos()
        built, csf = self.cookItem(repos, self.cfg, 'foo')
        nvf = built[0]
        nvf = repos.findTrove(None, nvf)[0]
        fileDict = client.getFilesFromTrove(*nvf)
        # prove each specfile has distinct contents.
        self.assertTrue('Release: 1' in fileDict['/1/foo.spec'].read())
        self.assertTrue('Release: 2' in fileDict['/2/foo.spec'].read())
        self.assertEquals(set(fileDict.keys()),
                set(['/1/foo.spec', '/2/foo.spec']))

    @testhelp.context('CNY-2627')
    def testRpmLookaside4(self):
        recipeStr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        # ensure missing files within the rpm respond sensibly
        r.addArchive('foo-1.0-1.src.rpm')
        r.addSource('notfound', dir = '/bar', rpm = 'foo-1.0-1.src.rpm')
"""
        self.addComponent('foo:source=1',
                [('foo.recipe', recipeStr),
                ('foo', 'wrong stuff'),
                ('foo-1.0-1.src.rpm', open(os.path.join(resources.get_archive(),
                                        'foo-1.0-1.src.rpm')).read())])
        client = self.getConaryClient()
        repos = client.getRepos()
        err = self.assertRaises(IOError, self.cookItem, repos, self.cfg, 'foo')
        self.assertEquals(str(err),
                'failed to extract source notfound from RPM foo-1.0-1.src.rpm')

    def testRepositoryCookNoDownload(self):
        # CNY-3221, RMK-995
        # We want to make sure that repositoy cooks don't re-download the
        # content (nor do they try to download wrong guesses)
        recipestr = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        # ensure missing files within the rpm respond sensibly
        r.addArchive('http://localhost:%(port)s/good/', dir = "/usr")
"""
        repos = self.openRepository()

        logFile = os.path.join(self.workDir, "webserver.log")
        hs = self._getHTTPServer(logFile)
        try:
            origDir = os.getcwd()
            os.chdir(self.workDir)
            self.newpkg('foo')
            os.chdir('foo')
            self.writeFile("foo.recipe", recipestr % dict(port = hs.port))
            self.addfile("foo.recipe")
            self.discardOutput(self.commit)

            self.assertEqual([x.strip() for x in file(logFile)],
                ['/good/foo-1.0.tar.bz2', '/good/foo-1.0.tar.gz'])

            # Get rid of the lookaside cache
            util.rmtree(os.path.join(self.cacheDir, "NEGATIVE"))
            # Cook. Make sure we didn't hit the server again
            self.cookItem(repos, self.cfg, 'foo')

            self.assertEqual([x.strip() for x in file(logFile)],
                ['/good/foo-1.0.tar.bz2', '/good/foo-1.0.tar.gz'])
        finally:
            hs.close()
            os.chdir(origDir)

    def test_extractFilesFromXzRPM(self):
        if not os.path.exists('/usr/bin/xz'):
            raise testhelp.SkipTestException('The /usr/bin/xz binary is required to run this test')
        recipestr = """
class XzRpmPackage(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        # ensure missing files within the rpm respond sensibly
        r.addArchive('popt-1.13-6.fc12.i686.rpm', dir = '/')
"""
        (built, d) = self.buildRecipe(recipestr, "XzRpmPackage")
        trvs = [ x for x in built if x[0] == 'foo:lib' ]
        trvs = [ (x[0], VFS(x[1]), x[2]) for x in trvs ]
        repos = self.openRepository()
        trv = repos.getTroves(trvs)[0]
        self.assertEqual(
            sorted(os.path.basename(p[1]) for p in trv.iterFileList()),
            ['libpopt.so.0', 'libpopt.so.0.0.0'])
