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
import stat
import itertools
import types

#testsuite
import conary_test
from conary_test import rephelp
from conary_test import resources

#conary
from conary import trove
from conary import versions
from conary import rpmhelper
from conary import files as cfiles
from conary.build import source
from conary.build import errors as builderrors
from conary.build.policy import PolicyError
from conary.lib import sha1helper
from conary.repository import changeset
from testrunner import testhelp


class CookTestWithRPMCapsules(rephelp.RepositoryHelper):

    @conary_test.rpm
    def testCookWithRPMCapsule(self):
        # make sure that we can cook a binary RPM
        #
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = 'brltty'
    version = '3.7.2_1.fc6.1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('brltty-3.7.2-1.fc6.1.i386.rpm')
"""
        # brltty files has binary files under /etc
        pkgNames, _, _ = self._cookAndInstall(recipestr,
                                              'brltty-3.7.2-1.fc6.1.i386.rpm',
                                              'brltty')
        self.assertEquals(pkgNames, ['brltty', 'brltty:rpm'])

    @conary_test.rpm
    def testCookWithAddingCapsuleTwice(self):
        # make sure that we get an error if we add a capsule twice
        #
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = 'brltty'
    version = '3.7.2_1.fc6.1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('brltty-3.7.2-1.fc6.1.i386.rpm')
        r.addCapsule('brltty-3.7.2-1.fc6.1.i386.rpm')
"""
        self.assertRaises(source.SourceError, self._cookAndInstall,
                              recipestr, 'brltty-3.7.2-1.fc6.1.i386.rpm',
                              'brltty')

    @conary_test.rpm
    def testCookWithScripts(self):
        # test script handling
        recipestr = """
class TestCookWithScripts(CapsuleRecipe):
    name = 'scripts'
    version = '1.0_1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('scripts-1.0-1.x86_64.rpm')
        r.WarnScriptSharedLibrary(exceptions='scripts-1.0-1.x86_64.rpm/(pre(in|un)|postun)')
"""
        pkgName = 'scripts'
        rpmName = 'scripts-1.0-1.x86_64.rpm'

        oldVal = self.cfg.cleanAfterCook
        self.cfg.cleanAfterCook = False
        self._cookPkgs(recipestr, rpmName, pkgName)
        self.cfg.cleanAfterCook = oldVal

        for scriptName, scriptContents in (
            ('prein', '''#!/bin/sh
echo this script mentions /etc/ld.so.conf >/dev/null
'''),
            ('postin', '''#!/sbin/ldconfig
'''),
            ('preun', '''#!/bin/sh
echo this script mentions /etc/ld.so.conf from preun >/dev/null
'''),
            ('postun', '''#!/bin/sh
echo this script mentions /etc/ld.so.conf from postun >/dev/null
'''),
            ('trigger_triggerin_other1_0', '''#!/bin/sh
TYPE="triggerin"
ID="0"
NAME="other1"
VERSIONCMP=""

'''),
            ('trigger_triggerun_other2_1', '''#!/other2/interp
TYPE="triggerun"
ID="1"
NAME="other2"
VERSIONCMP=""

'''),
            ('trigger_triggerpostun_other3_2', '''#!/bin/sh
TYPE="triggerpostun"
ID="2"
NAME="other3"
VERSIONCMP="< 4"

'''),
            ('trigger_triggerprein_other4_3', '''#!/bin/sh
TYPE="triggerprein"
ID="3"
NAME="other4"
VERSIONCMP=""
echo triggerprein has non-sequential sense flag >/dev/null
'''),
        ):
            self.assertEquals(file(
                '%s/scripts/_CAPSULE_SCRIPTS_/scripts-1.0-1.x86_64.rpm/%s'
                %(self.buildDir, scriptName)).read(), scriptContents)


    @conary_test.rpm
    def testCookWithRPMCapsuleTorture(self):
        # this is an evil RPM that has every kind of crazy file that we could think of
        #
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = 'with-special-files'
    version = '5.23'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('with-special-files-0.1-1.x86_64.rpm')

"""
        pkgName = 'with-special-files'
        rpmName = 'with-special-files-0.1-1.x86_64.rpm'

        pkgNames, built, _ = \
            self._cookAndInstall(recipestr,
                                 rpmName, pkgName, output =
                                 'error: unpacking of archive failed on file /dev/sda;........: '
                                 'cpio: mknod failed - Operation not permitted\n')

        # verify that the package exists
        self.assertEquals(pkgNames, [pkgName, pkgName +':rpm'])


        # get file list from rpm header
        r = file(resources.get_archive() + '/' + rpmName, 'r')
        h = rpmhelper.readHeader(r)
        rpmFileList = dict(
            itertools.izip( h[rpmhelper.OLDFILENAMES],
                            itertools.izip( h[rpmhelper.FILEUSERNAME],
                                            h[rpmhelper.FILEGROUPNAME],
                                            h[rpmhelper.FILEMODES],
                                            h[rpmhelper.FILESIZES],
                                            h[rpmhelper.FILERDEVS],
                                            h[rpmhelper.FILEFLAGS],
                                            h[rpmhelper.FILEVERIFYFLAGS],
                                            h[rpmhelper.FILELINKTOS],
                                            )))

        foundFiles = dict.fromkeys(rpmFileList)
        repos = self.openRepository()
        nvf = repos.findTrove(None, built[0])
        trv = repos.getTrove(*nvf[0])
        fileList = list(trv.iterFileList())
        fileObjs = repos.getFileVersions([(x[0], x[2], x[3]) for x in fileList])

        for fileInfo, fileObj in zip(fileList, fileObjs):
            fpath = fileInfo[1]
            foundFiles[fpath] = True
            rUser, rGroup, rMode, rSize, rDev, rFlags, rVflags, rLinkto = rpmFileList[fpath]

            # First, tests based on the Conary changeset

            # file metadata verification
            self.assertEqual(rUser, fileObj.inode.owner())
            self.assertEqual(rGroup, fileObj.inode.group())
            if isinstance(fileObj, cfiles.SymbolicLink):
                self.assertEqual(0777, fileObj.inode.perms()) # CNY-3304
            else:
                self.assertEqual(stat.S_IMODE(rMode), fileObj.inode.perms())

            if isinstance(fileObj, cfiles.RegularFile):
                assert stat.S_ISREG(rMode)

                # RPM config flag mapping
                if rFlags & rpmhelper.RPMFILE_CONFIG:
                    if fileObj.linkGroup() or not fileObj.contents.size():
                        assert fileObj.flags.isInitialContents()
                    else:
                        assert fileObj.flags.isConfig() or \
                            fileObj.flags.isInitialContents()

            elif isinstance(fileObj, cfiles.Directory):
                assert stat.S_ISDIR( rMode )
                assert not fileObj.flags.isEncapsulatedContent()
            elif isinstance(fileObj, cfiles.CharacterDevice):
                assert stat.S_ISCHR( rMode )

                minor = rDev & 0xff | (rDev >> 12) & 0xffffff00
                major = (rDev >> 8) & 0xfff
                self.assertEqual(fileObj.devt.major(), major)
                self.assertEqual(fileObj.devt.minor(), minor)

                assert not fileObj.flags.isEncapsulatedContent()
            elif isinstance(fileObj, cfiles.BlockDevice):
                assert stat.S_ISBLK( rMode )

                minor = rDev & 0xff | (rDev >> 12) & 0xffffff00
                major = (rDev >> 8) & 0xfff
                self.assertEqual(fileObj.devt.major(), major)
                self.assertEqual(fileObj.devt.minor(), minor)

                assert not fileObj.flags.isEncapsulatedContent()
            elif isinstance(fileObj, cfiles.NamedPipe):
                assert( stat.S_ISFIFO( rMode ) )

                assert not fileObj.flags.isEncapsulatedContent()
            elif isinstance(fileObj, cfiles.SymbolicLink):
                assert( stat.S_ISLNK( rMode ) )
                self.assertEquals( fileObj.target(), rLinkto )

                assert not fileObj.flags.isEncapsulatedContent()
            else:
                # unhandled file type
                assert False, 'Found unhandled file type!'


            # Now, some tests based on the contents of the RPM header
            if (not stat.S_ISDIR(rMode)) and rFlags & rpmhelper.RPMFILE_GHOST:
                assert fileObj.flags.isInitialContents()

            if rFlags & rpmhelper.RPMFILE_MISSINGOK:
                assert fileObj.flags.isMissingOkay()
            if fileObj.flags.isMissingOkay():
                assert rFlags & rpmhelper.RPMFILE_MISSINGOK

            if not rVflags:
                # %doc -- CNY-3254
                assert not fileObj.flags.isInitialContents()


            # Finally, tests based on specific filenames for specific
            # semantics
            if fpath == '/usr/share/noverifydigest':
                # CNY-3254
                assert fileObj.flags.isInitialContents()
            elif fpath == '/etc/with-special-files.symlink.unverified.cfg':
                # CNY-3254
                assert fileObj.flags.isInitialContents()
            elif fpath == '/usr/share/documentation':
                # %doc -- CNY-3254
                assert isinstance(fileObj, cfiles.RegularFile)
                assert not fileObj.flags.isInitialContents()

        # Make sure we have explicitly checked every file in the RPM
        uncheckedFiles = [x[0] for x in foundFiles.iteritems() if not x[1]]
        assert not uncheckedFiles, uncheckedFiles

    @conary_test.rpm
    def testCookWithEmptyRPMCapsule(self):
        # make sure that we can cook a binary RPM that does not include
        # any files
        recipestr = """
class TestCookWithEmptyRPMCapsule(CapsuleRecipe):
    name = 'basesystem'
    version = '8.0_5.1.1.el5'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('basesystem-8.0-5.1.1.el5.centos.noarch.rpm')

"""
        # basesystem contains no files -- but still needs to build
        # as a conary package
        pkgNames, _, _ = self._cookAndInstall(recipestr,
            'basesystem-8.0-5.1.1.el5.centos.noarch.rpm', 'basesystem')
        self.assertEquals(pkgNames, ['basesystem', 'basesystem:rpm'])

    @conary_test.rpm
    def testCookWithRPMCapsulePackage(self):
        # make sure that we can cook a binary RPM and specify a arbitrary
        # component to create
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = 'brltty'
    version = '3.7.2_1.fc6.1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('brltty-3.7.2-1.fc6.1.i386.rpm', package='brltty:rpm')
"""
        pkgName = 'brltty'
        rpmName = 'brltty-3.7.2-1.fc6.1.i386.rpm'

        pkgNames, _, _ = self._cookAndInstall(recipestr,
            rpmName, pkgName)
        self.assertEquals(pkgNames, ['brltty', 'brltty:rpm'])

    @conary_test.rpm
    def testCookWithRPMCapsuleHybrid(self):
        '''
        Make sure that we can cook a binary RPM and some other files and put
        them in separate components
        '''
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = 'brltty'
    version = '3.7.2_1.fc6.1'

    clearBuildReqs()

    def setup(r):
        r.addSource('unstripped_binary.c', dir='/')
        r.addCapsule('brltty-3.7.2-1.fc6.1.i386.rpm')
"""
        pkgName = 'brltty'
        rpmName = 'brltty-3.7.2-1.fc6.1.i386.rpm'

        pkgNames, built, cs = self._cookAndInstall(recipestr,
            [rpmName, 'unstripped_binary.c'], pkgName)
        self.assertEquals(
            set(pkgNames), set(['brltty', 'brltty:rpm', 'brltty:runtime']))
        trvs = [x for x in cs.iterNewTroveList()]
        trvMap = dict(zip([x.getName() for x in trvs], trvs))
        fooFl = [ x[1] for x in trvMap['brltty:runtime'].getNewFileList() ]
        self.assertEqual(fooFl, [ '/unstripped_binary.c' ])

    @conary_test.rpm
    def testCookWithRPMCapsuleHybridFail(self):
        '''
        make sure that we can cook a binary RPM and some other files 
        and they they can't be in the same component
        '''
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = 'brltty'
    version = '3.7.2_1.fc6.1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('brltty-3.7.2-1.fc6.1.i386.rpm')
        r.addSource('unstripped_binary.c', package='brltty:rpm', dir='/')
"""
        pkgName = 'brltty'
        rpmName = 'brltty-3.7.2-1.fc6.1.i386.rpm'

        self.assertRaises(PolicyError, self._cookAndInstall, recipestr,
                          [rpmName, 'unstripped_binary.c'], pkgName)

    @conary_test.rpm
    def testCookWithRPMCapsuleRemoveFail(self):
        '''
        make sure that we get an error when we try to remove a file provided
        by a capsule
        '''
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = 'brltty'
    version = '3.7.2_1.fc6.1'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('brltty-3.7.2-1.fc6.1.i386.rpm')
        r.Remove('/etc/brltty.conf')
"""
        pkgName = 'brltty'
        rpmName = 'brltty-3.7.2-1.fc6.1.i386.rpm'

        self.assertRaises(builderrors.RecipeFileError, self._cookAndInstall,
                          recipestr, [rpmName], pkgName)

    @conary_test.rpm
    def testRPMCapsuleOddPaths(self):
        recipestr1 = r"""
class TestOddPaths(CapsuleRecipe):
    name = 'ghost'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('oddpaths-1.0-1.i386.rpm')
"""
        built, d = self.buildRecipe(recipestr1, "TestOddPaths")
        client = self.getConaryClient()
        repos = client.getRepos()
        nvf = repos.findTrove(None, built[0])
        trv = repos.getTrove(*nvf[0])
        fileList = list(trv.iterFileList())
        fileNames = sorted([x[1] for x in fileList])
        self.assertEquals(fileNames, ['/foo/%', '/foo/{'])

    @conary_test.rpm
    def testRPMCapsulePathOverlap(self):
        '''make sure that overlapping paths are represented in imports'''
        recipestr = """
class TestRPMCapsulePathOverlap(CapsuleRecipe):
    name = 'overlap-same'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('overlap-same-A-1.0-1.i386.rpm',
                     package='overlap-same-A:rpm')
        r.addCapsule('overlap-same-B-1.0-1.i386.rpm',
                     package='overlap-same-B:rpm')
"""
        pkgName = 'overlap-same'
        rpmNames = [
            'overlap-same-A-1.0-1.i386.rpm',
            'overlap-same-B-1.0-1.i386.rpm' ]
        builtPkgNames = ['overlap-same-A', 'overlap-same-B']

        pkgNames, built, cs = self._cookAndInstall(recipestr,
            rpmNames, pkgName, builtPkgNames)
        self.assertEquals(pkgNames,
            ['overlap-same-A', 'overlap-same-A:rpm',
             'overlap-same-B', 'overlap-same-B:rpm']
            )

        # Ensure that all the paths exist that should, including overlap
        for tcs in cs.iterNewTroveList():
            trv = trove.Trove(tcs)
            troveName = trv.getName()
            if troveName.endswith(':rpm'):
                paths = [x[1] for x in trv.iterFileList()]
                # make sure that each RPM ended up in the right package
                self.assertEquals(troveName.split(':')[0],
                                  trv.troveInfo.capsule.rpm.name())
                self.assertEquals(len(paths), 2)
                self.assertEquals('/file' in paths, True)

    @conary_test.rpm
    def testRPMCapsulePathOverlapConflicts(self):
        '''make sure that conflicting overlapping paths fail appropriately'''
        recipestr = """
class TestRPMCapsulePathOverlapConflicts(CapsuleRecipe):
    name = 'overlap-conflict'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('overlap-conflict-1.0-1.i386.rpm',
                     package='overlap-conflict:rpm')
        r.addCapsule('overlap-same-A-1.0-1.i386.rpm',
                     package='overlap-same-A:rpm')
"""
        pkgName = 'overlap-conflict'
        rpmNames = [
            'overlap-same-A-1.0-1.i386.rpm',
            'overlap-conflict-1.0-1.i386.rpm' ]

        self.assertRaises(builderrors.CookError,
            self._cookAndInstall, recipestr, rpmNames, pkgName)

    @conary_test.rpm
    def testRPMCapsulePathOverlapConflictsOK(self):
        '''make sure that conflicting overlapping paths can be overridden'''
        recipestr = """
class TestRPMCapsulePathOverlapConflictsOK(CapsuleRecipe):
    name = 'overlap-conflict'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('overlap-conflict-1.0-1.i386.rpm',
                     package='overlap-conflict:rpm')
        r.addCapsule('overlap-same-A-1.0-1.i386.rpm',
                     package='overlap-same-A:rpm',
                     ignoreConflictingPaths=set(('/file',)))
"""
        pkgName = 'overlap-conflict'
        rpmNames = [
            'overlap-same-A-1.0-1.i386.rpm',
            'overlap-conflict-1.0-1.i386.rpm' ]
        builtPkgNames = ['overlap-same-A', 'overlap-conflict']

        pkgNames, built, cs = self._cookAndInstall(
            recipestr, rpmNames, pkgName, builtPkgNames)
        self.assertEquals(pkgNames,
            ['overlap-conflict', 'overlap-conflict:rpm',
             'overlap-same-A', 'overlap-same-A:rpm']
            )

        # Ensure that all the paths exist that should, including overlap
        for tcs in cs.iterNewTroveList():
            trv = trove.Trove(tcs)
            troveName = trv.getName()
            if troveName.endswith(':rpm'):
                paths = [x[1] for x in trv.iterFileList()]
                self.assertEquals('/file' in paths, True)


    @conary_test.rpm
    def testRPMCapsuleMtimeOverlapConflictsOK(self):
        '''make sure that conflicting overlapping mtimes can be overridden'''
        recipestr = """
class TestRPMCapsulePathOverlapConflictsOK(CapsuleRecipe):
    name = 'simple'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('simple-1.0.1-1.i386.rpm')
        r.addCapsule('simple-1.0-1.i386.rpm')
"""
        pkgName = 'simple'
        rpmNames = [
            'simple-1.0.1-1.i386.rpm',
            'simple-1.0-1.i386.rpm' ]
        builtPkgNames = ['simple']

        pkgNames, built, cs = self._cookAndInstall(
            recipestr, rpmNames, pkgName, builtPkgNames)
        self.assertEquals(pkgNames, ['simple', 'simple:rpm'])

        # Ensure that all the paths exist that should, including overlap
        for tcs in cs.iterNewTroveList():
            trv = trove.Trove(tcs)
            troveName = trv.getName()
            if troveName.endswith(':rpm'):
                paths = [x[1] for x in trv.iterFileList()]
                self.assertEquals(sorted(paths), ['/config', '/dir', '/normal'])


    @conary_test.rpm
    def testRPMCapsuleOverlap(self):
        # this is a pair of evil RPMs that has many forms of conflicting
        # overlap
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = 'overlap-special-difference'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('overlap-special-difference-1.0-1.x86_64.rpm',
            package='overlap-special-difference:rpm')
        r.addCapsule('overlap-special-other-1.0-1.x86_64.rpm',
            ignoreConflictingPaths=set(PATHS),
            package='overlap-special-other:rpm')
"""
        pkgName = 'overlap-special-difference'

        def initialcontents(fileObj, fpath):
            self.assertEquals(fileObj.flags.isInitialContents() and True, True,
                msg='%s should be InitialContents' %fpath)
        def config(fileObj, fpath):
            self.assertEquals(fileObj.flags.isConfig() and True, True,
                msg='%s should be Config' %fpath)
        def directory(fileObj, fpath):
            self.assertEquals(isinstance(fileObj, cfiles.Directory), True,
                msg='%s should be Directory' %fpath)
        def blockdevice(fileObj, fpath):
            self.assertEquals(isinstance(fileObj, cfiles.BlockDevice), True,
                msg='%s should be BlockDevice' %fpath)
        def regular(fileObj, fpath):
            self.assertEquals(isinstance(fileObj, cfiles.RegularFile), True,
                msg='%s should be RegularFile' %fpath)
        def symlink(fileObj, fpath):
            self.assertEquals(isinstance(fileObj, cfiles.SymbolicLink), True,
                msg='%s should be SymbolicLink' %fpath)

        rpmVerifyData = [
            ('overlap-special-difference-1.0-1.x86_64.rpm', (
                    ('/etc/conf', 0600, 'root', 'root', initialcontents),
                    ('/etc/conf2', 0600, 'root', 'root', config),
                    ('/ghostly', 0700, 'root', 'root', directory),
                    ('/ghostly/file', 0600, 'root', 'root', initialcontents),
                    ('/etc/ghostconf', 0777, 'root', 'root', symlink),
                    ('/dev/sda', 0600, 'root', 'root', blockdevice),
                    ('/etc/noverify', 0600, 'root', 'root', initialcontents),
                    ('/etc/maybeverify', 0600, 'root', 'root', initialcontents),
                    ('/usr/normal', 0600, 'root', 'root', regular),
                    ('/usr/lib64/python2.4/config/Makefile',
                                    0600, 'root', 'root', regular),
            )),
            ('overlap-special-other-1.0-1.x86_64.rpm', (
                    ('/etc/conf', 0644, 'oot', 'oot', initialcontents),
                    ('/etc/conf2', 0644, 'oot', 'oot', config),
                    ('/ghostly', 0750, 'oot', 'oot', directory),
                    ('/ghostly/file', 0640, 'oot', 'oot', initialcontents),
                    ('/etc/ghostconf', 0777, 'root', 'root', symlink),
                    ('/dev/sda', 0660, 'oot', 'oot', blockdevice),
                    ('/etc/noverify', 0660, 'oot', 'oot', initialcontents),
                    ('/etc/maybeverify', 0600, 'oot', 'oot', regular),
                    ('/usr/normal', 0640, 'oot', 'oot', regular),
                    ('/usr/lib64/python2.4/config/Makefile',
                                    0600, 'root', 'root', regular),
            )),
        ]
        rpmPaths = sorted([x[0] for x in rpmVerifyData[0][1]])
        rpmNames = [x[0] for x in rpmVerifyData]
        builtPkgNames = [pkgName, 'overlap-special-other']

        pkgNames, built, cs = self._cookPkgs(
            recipestr.replace('PATHS', '%r'%rpmPaths),
            rpmNames, pkgName, builtPkgNames,
            macros={'lib': 'lib64'})

        # Ensure that all the paths exist that should, including overlap
        for tcs in cs.iterNewTroveList():
            trv = trove.Trove(tcs)
            troveName = trv.getName()
            if troveName.endswith(':rpm'):
                self.assertEquals(sorted([x[1] for x in trv.iterFileList()]),
                    rpmPaths)

        pathFlavors = []

        for rpmName, pathList in rpmVerifyData:
            pathMap = dict((x[0], x[1:5]) for x in pathList)
            # get file list from rpm header
            r = file(resources.get_archive() + '/' + rpmName, 'r')
            h = rpmhelper.readHeader(r)
            rpmFileList = dict(
                itertools.izip( h[rpmhelper.OLDFILENAMES],
                                itertools.izip( h[rpmhelper.FILEUSERNAME],
                                                h[rpmhelper.FILEGROUPNAME],
                                                h[rpmhelper.FILEMODES],
                                                h[rpmhelper.FILESIZES],
                                                h[rpmhelper.FILERDEVS],
                                                h[rpmhelper.FILEFLAGS],
                                                h[rpmhelper.FILEVERIFYFLAGS],
                                                h[rpmhelper.FILELINKTOS],
                                                )))

            foundFiles = dict.fromkeys(rpmFileList)
            repos = self.openRepository()
            desiredTrove = rpmName.replace('-1.0-1.x86_64.', ':')
            nvf = repos.findTrove(None, [x for x in built if x[0] == desiredTrove][0])
            trv = repos.getTrove(*nvf[0])
            fileList = list(trv.iterFileList())
            fileObjs = repos.getFileVersions([(x[0], x[2], x[3]) for x in fileList])

            for fileInfo, fileObj in zip(fileList, fileObjs):
                fpath = fileInfo[1]
                if fpath == '/usr/lib64/python2.4/config/Makefile':
                    pathFlavors.append(str(fileObj.flavor()))
                foundFiles[fpath] = True
                rUser, rGroup, rMode, rSize, rDev, rFlags, rVflags, rLinkto = rpmFileList[fpath]
                user = pathMap[fpath][1]
                group = pathMap[fpath][2]

                # run the correct validator
                if pathMap[fpath][3]:
                    pathMap[fpath][3](fileObj, fpath)

                # file metadata verification
                self.assertEqual(rUser, fileObj.inode.owner())
                self.assertEqual(user, fileObj.inode.owner())
                self.assertEqual(rGroup, fileObj.inode.group())
                self.assertEqual(group, fileObj.inode.group())
                self.assertEqual(stat.S_IMODE(rMode), fileObj.inode.perms())
                self.assertEqual(stat.S_IMODE(rMode), pathMap[fpath][0])

                if isinstance(fileObj, cfiles.RegularFile):
                    assert stat.S_ISREG( rMode )

                if isinstance(fileObj, cfiles.BlockDevice):
                    assert stat.S_ISBLK( rMode )

                    minor = rDev & 0xff | (rDev >> 12) & 0xffffff00
                    major = (rDev >> 8) & 0xfff
                    self.assertEqual(fileObj.devt.major(), major)
                    self.assertEqual(fileObj.devt.minor(), minor)

                    assert not fileObj.flags.isEncapsulatedContent()

                elif isinstance(fileObj, cfiles.NamedPipe):
                    assert( stat.S_ISFIFO( rMode ) )
                    assert not fileObj.flags.isEncapsulatedContent()

                elif isinstance(fileObj, cfiles.SymbolicLink):
                    assert( stat.S_ISLNK( rMode ) )
                    self.assertEquals( fileObj.target(), rLinkto )
                    assert not fileObj.flags.isEncapsulatedContent()

            # Make sure we have explicitly checked every file in the RPM
            uncheckedFiles = [x[0] for x in foundFiles.iteritems() if not x[1]]
            assert not uncheckedFiles, uncheckedFiles

        # ensure that the flavor is set (will be different on different archs
        [self.assertTrue(bool(x)) for x in pathFlavors]
        # ensure that the flavor is the same across both components
        self.assertEquals(pathFlavors[0], pathFlavors[1])

    @conary_test.rpm
    def testRPMRepresentMtimeInTroveinfo(self):
        '''make sure that real mtime from RPM is represented in troveinfo'''
        def checkMtimes(trvCs, rpmPath):
            archivePath = resources.get_archive()

            trv = trove.Trove(trvCs)
            f = open(archivePath + '/' + rpmPath, "r")
            h = rpmhelper.readHeader(f)

            rpmMtimes = dict( (path, mtime) for path, mtime in
                                itertools.izip(h[rpmhelper.OLDFILENAMES],
                                               h[rpmhelper.FILEMTIMES]) )
            conaryMtimes = dict( (path, mtime) for
                        ((pathId, path, fileId, version), mtime) in
                        itertools.izip(sorted(trv.iterFileList()),
                                       trv.troveInfo.mtimes) )
            self.assertEquals(rpmMtimes, conaryMtimes)

        recipestr = """
class TestRPMRepresentMtimeInTroveinfo(CapsuleRecipe):
    name = 'simple'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('simple-1.0-1.i386.rpm')
"""
        pkgName = 'simple'
        rpmName = 'simple-1.0-1.i386.rpm'

        r1 = self._cookPkgs(recipestr, rpmName, pkgName, 'simple')
        trvCs1 = [ x for x in r1[2].iterNewTroveList()
                      if x.getName() == 'simple:rpm' ][0]

        r2 = self._cookPkgs(recipestr.replace('1.0', '1.1'),
                            rpmName.replace('1.0', '1.1'),
                            pkgName.replace('1.0', '1.1'),
                            'simple',
                            updatePackage=True)
        trvCs2 = [ x for x in r2[2].iterNewTroveList()
                      if x.getName() == 'simple:rpm' ][0]

        checkMtimes(trvCs1, 'simple-1.0-1.i386.rpm')
        checkMtimes(trvCs2, 'simple-1.1-1.i386.rpm')

    @conary_test.rpm
    def testRPMObsoletes(self):
        '''make sure that obsoletes is represented in troveinfo'''

        recipestr = """
class TestRPMObsoletes(CapsuleRecipe):
    name = 'obsolete'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('obsolete-1.0-1.i386.rpm')
"""
        pkgName = 'obsolete'
        rpmName = 'obsolete-1.0-1.i386.rpm'

        r1 = self._cookPkgs(recipestr, rpmName, pkgName, 'obsolete')
        trvCs = [ x for x in r1[2].iterNewTroveList()
                      if x.getName() == 'obsolete:rpm' ][0]

        archivePath = resources.get_archive()

        trv = trove.Trove(trvCs)
        f = open(archivePath + '/' + rpmName, "r")
        h = rpmhelper.readHeader(f)

        obs = [x[1] for x in trv.troveInfo.capsule.rpm.obsoletes.iterAll()]
        obl = [(x.name(), x.flags(), x.version()) for x in obs]
        obl.sort()

        reference = [('bar', 2L, '1.0'), ('baz', 4L, '2.0'), ('foo', 0L, '')]
        self.assertEqual(obl, reference)

    @conary_test.rpm
    def testRPMSHA1SigTag(self):
        '''make sure that SHA1HEADER/SIG_SHA1 is represented in troveinfo'''

        recipestr = """
class TestRPMSHA1(CapsuleRecipe):
    name = 'simple'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('simple-1.0-1.i386.rpm')
"""
        pkgName = 'simple'
        rpmName = 'simple-1.0-1.i386.rpm'

        r = self._cookPkgs(recipestr, rpmName, pkgName, 'simple')
        trvCs = [ x for x in r[2].iterNewTroveList()
                      if x.getName() == 'simple:rpm' ][0]

        archivePath = resources.get_archive()

        trv = trove.Trove(trvCs)
        f = open(archivePath + '/' + rpmName, "r")
        h = rpmhelper.readHeader(f)

        sha1header = trv.troveInfo.capsule.rpm.sha1header()
        self.assertEqual(h.get(rpmhelper.SIG_SHA1),
            sha1helper.sha1ToString(sha1header))

    @conary_test.rpm
    @testhelp.context('fileoverlap')
    def testRpmDocSharing(self):
        'test for CNY-3420'
        if os.uname()[4] != 'x86_64':
            raise testhelp.SkipTestException(
                'this test only works on x86_64 platforms')
        recipestr = r'''
class docConflict(CapsuleRecipe):
    name = 'doc-conflict'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.addCapsule('doc-conflict-1.0-1.%s.rpm', use=Arch.%s)
'''
        groupRecipe = r'''
class GroupShare(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    autoResolve = False

    def setup(r):
        r.add('doc-conflict', flavor ='is:x86')
        r.add('doc-conflict', flavor ='is:x86_64')
'''

        c0, d = self.buildRecipe(recipestr % ('x86_64', 'x86_64'), 'docConflict')
        self.overrideBuildFlavor('is: x86')
        c1, d = self.buildRecipe(recipestr % ('i386', 'x86'), 'docConflict')
        g, d = self.buildRecipe(groupRecipe, 'GroupShare')
        self.assertEquals(g[0][0], 'group-dist')
        self._installPkgs(['group-dist'], output = '^$')
        self.resetRoot()
        self._installPkgs(['doc-conflict[is:x86]'], output = '^$')
        self._installPkgs(['doc-conflict[is:x86_64]'], output = '^$',
                          keepExisting=True)
        self.resetRoot()
        self._installPkgs(['doc-conflict[is:x86]',
                           'doc-conflict[is:x86_64]'], output = '^$',
                          keepExisting=True)
        self.resetRoot()
        self._installPkgs(['doc-conflict[is:x86]',
                           'doc-conflict[is:x86_64]'], output = '^$',
                          keepExisting=True,
                          justDatabase=True)
        
    @conary_test.rpm
    @testhelp.context('fileoverlap')
    def testRpmGhostSharing(self):
        recipestr = r'''
class ghostConflict(CapsuleRecipe):
    name = 'ghost-conflict'
    version = '%s'
    clearBuildReqs()
    def setup(r):
        r.addCapsule('ghost-conflict-%s-1.noarch.rpm')
'''
        groupRecipe = r'''
class GroupShare(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    autoResolve = False

    def setup(r):
        r.VersionConflicts(exceptions = 'group-dist')
        r.add('ghost-conflict', '1.0')
        r.add('ghost-conflict', '1.1')
'''

        c0, d = self.buildRecipe(recipestr % ('1.0', '1.0'), 'ghostConflict')
        c1, d = self.buildRecipe(recipestr % ('1.1', '1.1'), 'ghostConflict')
        g, d = self.buildRecipe(groupRecipe, 'GroupShare')
        self.assertEquals(g[0][0], 'group-dist')
        self._installPkgs(['group-dist'], output = '')
        self.resetRoot()
        self._installPkgs(['ghost-conflict=1.0'], output = '^$')
        file(self.rootDir+'/etc/fake', 'w')
        self._installPkgs(['ghost-conflict=1.1'], output = '^$',
                          keepExisting=True)
        self.resetRoot()
        self._installPkgs(['ghost-conflict=1.0',
                           'ghost-conflict=1.1'], output = '^$',
                          keepExisting=True)
        self.resetRoot()
        self._installPkgs(['ghost-conflict=1.0',
                           'ghost-conflict=1.1'], output = '^$',
                          keepExisting=True,
                          justDatabase=True)

    @conary_test.rpm
    def testRpmFileFlavors(self):
        if os.uname()[4] != 'x86_64':
            # this test only works on x86_64 platforms
            return

        recipestr1 = r"""
class Tmpwatch(CapsuleRecipe):
    name = 'tmpwatch'
    version = '%s'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('%s')
"""
        built, d = self.buildRecipe(
            recipestr1 % ('2.9.7_1.1.el5',
                          'tmpwatch-2.9.7-1.1.el5.2.i386.rpm'), "Tmpwatch")
        built, d = self.buildRecipe(
            recipestr1 % ('2.9.7_1.1.el5',
                          'tmpwatch-2.9.7-1.1.el5.2.x86_64.rpm'), "Tmpwatch")

        groupRecipe = r"""
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    autoResolve = False

    def setup(r):
        r.add('tmpwatch', flavor ='is:x86')
        r.add('tmpwatch', flavor ='is:x86_64')
"""
        built, d = self.buildRecipe(groupRecipe, "GroupConflicts")

        built, d = self.buildRecipe(
            recipestr1 % ('2.9.7',
                          'tmpwatch-2.9.1-1.i386.rpm'), "Tmpwatch")

        groupRecipe = r"""
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    autoResolve = False

    def setup(r):
        r.add('tmpwatch', versionStr = '2.9.7_1.1.el5', flavor ='is:x86')
        r.add('tmpwatch', versionStr ='2.9.7')
"""
        self.assertRaises(builderrors.GroupPathConflicts,
                self.buildRecipe, groupRecipe, "GroupConflicts")

    @conary_test.rpm
    def testRpmHardLinkCompabilitity(self):
        recipestr1 = r"""
class HLConflict(CapsuleRecipe):
    name = 'hardlinkconflict'
    version = '%s'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('%s')
"""
        built, d = self.buildRecipe(
            recipestr1 % ('1.0_1',
                          'hardlinkconflict-1.0-1.x86_64.rpm'), "HLConflict")
        built, d = self.buildRecipe(
            recipestr1 % ('1.0_2',
                          'hardlinkconflict-1.0-2.x86_64.rpm'), "HLConflict")

        groupRecipe = r"""
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    autoResolve = False

    def setup(r):
        r.VersionConflicts(exceptions = 'group-dist')
        r.add('hardlinkconflict', '1.0_1')
        r.add('hardlinkconflict', '1.0_2')
"""
        built, d = self.buildRecipe(groupRecipe, "GroupConflicts")

    @conary_test.rpm
    def testRPMCapsulePathIdShareingAcrossLabels(self):
        '''make sure that path ids are consistent when a file exists in two
        separate packages on two different labels'''
        recipestr = """
class TestCookWithRPMCapsule(CapsuleRecipe):
    name = '%s'
    version = '3.7.2_1.fc6.1'

    clearBuildReqs()

    pathIdSearchBranches = ['/localhost@rpl:linuxA',
                            '/localhost@rpl:linuxB',
                            '/localhost@rpl:linuxC',
                            '/localhost@rpl:linuxD'] # linuxD doesn't ever exist

    def setup(r):
        r.addCapsule('brltty-3.7.2-1.fc6.1.i386.rpm')

"""
        pkgname = 'brltty'
        rpmname = 'brltty-3.7.2-1.fc6.1.i386.rpm'
        # commit the source package
        repos = self.openRepository()
        recipename = pkgname + '.recipe'

        # build package A
        self.cfg.buildLabel = versions.Label('localhost@rpl:linuxA')
        self.cfg.installLabel = versions.Label('localhost@rpl:linuxA')
        self.cfg.installLabelPath.append(versions.Label('localhost@rpl:linuxA'))

        os.chdir(self.workDir)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(recipename, recipestr % pkgname)
        self.addfile(recipename)
        shutil.copyfile(
            resources.get_archive() + '/' + rpmname,
            rpmname)
        self.addfile(rpmname)
        self.commit()
        builtA, out = self.cookItem(repos, self.cfg, pkgname, macros={})
        assert not out

        # build package B
        self.cfg.buildLabel = versions.Label('localhost@rpl:linuxB')
        self.cfg.installLabel = versions.Label('localhost@rpl:linuxB')
        self.cfg.installLabelPath.append(versions.Label('localhost@rpl:linuxB'))

        os.chdir(self.workDir)
        shutil.rmtree(pkgname)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(recipename, recipestr % pkgname)
        self.addfile(recipename)
        shutil.copyfile(
            resources.get_archive() + '/' + rpmname,
            rpmname)
        self.addfile(rpmname)
        self.commit()
        builtB, out = self.cookItem(repos, self.cfg, pkgname, macros={})
        assert not out

        # build package C
        self.cfg.buildLabel = versions.Label('localhost@rpl:linuxC')
        self.cfg.installLabel = versions.Label('localhost@rpl:linuxC')
        self.cfg.installLabelPath.append(versions.Label('localhost@rpl:linuxC'))

        os.chdir(self.workDir)
        shutil.rmtree(pkgname)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(recipename, recipestr % pkgname)
        self.addfile(recipename)
        shutil.copyfile(
            resources.get_archive() + '/' + rpmname,
            rpmname)
        self.addfile(rpmname)
        self.commit()
        builtC, out = self.cookItem(repos, self.cfg, pkgname, macros={})
        assert not out


        # make sure all of the pathids are equal
        nvfA = repos.findTrove(None, builtA[0])
        trvA = repos.getTrove(*nvfA[0])
        nvfB = repos.findTrove(None, builtB[0])
        trvB = repos.getTrove(*nvfB[0])
        listB = list(trvB.iterFileList())
        mapB = dict(zip([x[1] for x in listB], listB))
        nvfC = repos.findTrove(None, builtC[0])
        trvC = repos.getTrove(*nvfC[0])
        listC = list(trvC.iterFileList())
        mapC = dict(zip([x[1] for x in listC], listC))
        for f in trvA.iterFileList():
            self.assertEquals(f[0:3],mapB[f[1]][0:3])
            self.assertEquals(f[0:3],mapC[f[1]][0:3])

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

    def _installPkgs(self, builtpkgnames, output = '', justDatabase=False,
                     keepExisting=False):
        rc, str = self.captureOutput(self.updatePkg, self.rootDir,
                                     builtpkgnames, depCheck=False,
                                     justDatabase=justDatabase,
                                     keepExisting=keepExisting)
        assert re.match(output, str), '%r != %r' %(output, str)


    def testCookRPMCapsuleSigVerifyMissingKey(self):
        recipestr = """
class Test(CapsuleRecipe):
    name = 'test'
    version = '1'

    clearBuildReqs()

    def setup(r):
        # Should fail, unable to fetch key id
        r.addCapsule('tmpwatch-2.9.7-1.1.el5.2.x86_64.rpm', keyid = 'aabbccdd')
"""
        self.mock(source.addCapsule, '_doDownloadPublicKey',
            lambda slf, x: None)
        e = self.assertRaises(source.SourceError,
            self.buildRecipe, recipestr, 'Test')
        self.assertEqual(str(e), "Failed to retrieve PGP key aabbccdd")

    def testCookRPMCapsuleSigVerifyWrongKey(self):
        recipestr = """
class Test(CapsuleRecipe):
    name = 'test'
    version = '1'

    clearBuildReqs()

    def setup(r):
        # Should fail, signature made with different key
        r.addCapsule('tmpwatch-2.9.7-1.1.el5.2.x86_64.rpm', keyid = 'AE07E378')
"""
        from conary.build import source

        self.mock(source.addCapsule, '_doDownloadPublicKey',
            lambda slf, x: pgpKey1)
        e = self.assertRaises(source.SourceError,
            self.buildRecipe, recipestr, 'Test')
        self.assertEqual(str(e), "Signature generated with key "
        "A8A447DCE8562897 does not match valid keys EFA3924DAE07E378")

    def testCookRPMCapsuleSigVerify(self):
        recipestr = """
class Test(CapsuleRecipe):
    name = 'test'
    version = '1'

    clearBuildReqs()

    def setup(r):
        # Should fail, signature made with different key
        r.addCapsule('tmpwatch-2.9.7-1.1.el5.2.x86_64.rpm', keyid = 'E8562897')
"""
        from conary.build import source

        self.mock(source.addCapsule, '_doDownloadPublicKey',
            lambda slf, x: pgpKeyCentos)
        self.buildRecipe(recipestr, 'Test')

pgpKey1 = """
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.1 (GNU/Linux)

mI0ERfN+dwEEAOT+tXGfM+sNV1ZUrHVpOxcw57cg6fdS2bSThA7AqBNEnbMkatgX
I4S71s1NTGpKpSIoT4Twk9tJrI3rsy/2QaSZ9J34+1WhQ8oD+tn8KDxg5HP02eAe
9u8sYuOEURQ4w8VVOiwMn1B789Lf76qGiU8HEvhQRAibbpigX5P2/S5tABEBAAG0
ME1paGFpIEliYW5lc2N1IChUZXN0IGtleSkgPG1pc2ErdGVzdDRAcnBhdGguY29t
Poi2BBMBAgAgBQJF8353AhsvBgsJCAcDAgQVAggDBBYCAwECHgECF4AACgkQ76OS
Ta4H43jRwwQAsdTqyCYpB8Btov9zrEF2qdSx5m5AwoBUyEQYuNN/XNGza1/y0tJr
BLbSYXe82Eal6+emiMkhdCGDA/BSPYd6UA284uJm+fhuJF+Sw5BvPBolMTx8IZoi
7U+kLJEXtyyzDaMjSM0WJctyvJGjQWyh0ukGn/jAv4ASX1yoKy7j4bm5AQ0ERfN+
eRAEAJwVsQ+3bvNj4e2GkakcnsfYnKMlAZTa7uZUG+g3NDNcNOsK1NMSE/r/oFhP
/yB3LbaWAT0xaCp7nd1DSCh0AmyHE7H9LNVf2EleRXmEtLt6E3o9TtfZNUWkJ6M9
hX6NXF83AFE9JRuLqe72LmNivwIrVctNV77uOiSLiT6PqqtXAAMFA/9BZ0YHcvnv
xfOX19rrDynvT01+vxlAJN07Rd6aldSGnPUTD6MUNknWavMdyzlfwFebwYxYlIYp
/2dMy2yXzOfNPzjktEmUTcAjiqJUXfQVzvVaK4ugzfdS7M552CbsTFPwdmu67Q4c
9kfrFLAGTupH6ryBK3aH63M/pFZQRwihhIifBBgBAgAJBQJF8355AhsMAAoJEO+j
kk2uB+N4zmUEAIHBgkhg2S5YI247umj/pwnfC5B1MW9wA4vy6puKMnY3rQMX7TUI
71JObz6iZDrXZKyYMNEyXBTtDgflzcTLBVZrLO6E897PBi9bBGajN6ZA6YlBeM5f
DXbmN/21Bb1iPCtWwfLfiTSgp9yuNFg2WdiA6LIFYkT4F/mMFyawKuGU
=R/ZN
-----END PGP PUBLIC KEY BLOCK-----
"""

pgpKeyCentos = """
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.2.6 (GNU/Linux)

mQGiBEWfB6MRBACrnYW6yKMT+MwJlCIhoyTxGf3mAxmnAiDEy6HcYN8rivssVTJk
CFtQBlBOpLV/OW2YtKrCO2xHn46eNfnMri8FGT8g+9JF3MUVi7kiV1He4iJynHXB
+F2ZqIvHf3IaUj1ys+p8TK64FDFxDQDrGQfIsD/+pkSGx53/877IrvdwjwCguQcr
Ioip5TH0Fj0OLUY4asYVZH8EAIqFHEqsY+9ziP+2R3/FyxSllKkjwcMLrBug+cYO
LYDD6eQXE9Mq8XKGFDj9ZB/0+JzK/XQeStheeFG75q3noq5oCPVFO4czuKErIRAB
qKbDBhaTj3JhOgM12XsUYn+rI6NeMV2ZogoQCC2tWmDETfRpYp2moo53NuFWHbAy
XjETA/sHEeQT9huHzdi/lebNBj0L8nBGfLN1nSRP1GtvagBvkR4RZ6DTQyl0UzOJ
RA3ywWlrL9IV9mrpb1Fmn60l2jTMMCc7J6LacmPK906N+FcN/Docj1M4s/4CNanQ
NhzcFhAFtQL56SNyLTCk1XzhssGZ/jwGnNbU/aaj4wOj0Uef5LRGQ2VudE9TLTUg
S2V5IChDZW50T1MgNSBPZmZpY2lhbCBTaWduaW5nIEtleSkgPGNlbnRvcy01LWtl
eUBjZW50b3Mub3JnPohkBBMRAgAkBQJFnwekAhsDBQkSzAMABgsJCAcDAgMVAgMD
FgIBAh4BAheAAAoJEKikR9zoViiXKlEAmwSoZDvZo+WChcg3s/SpNoWCKhMAAJwI
E2aXpZVrpsQnInUQWwkdrTiL5YhMBBMRAgAMBQJFnwiSBYMSzAIRAAoJEDjCFhY5
bKCk0hAAn134bIx3wSbq58E6P6U5RT7Z2Zx4AJ9VxnVkoGHkVIgSdsxHUgRjo27N
F7kBDQRFnwezEAQA/HnJ5yiozwgtf6jt+kii8iua+WnjqBKomPHOQ8moxbWdv5Ks
4e1DPhzRqxhshjmub4SuJ93sgMSAF2ayC9t51mSJV33KfzPF2gIahcMqfABe/2hJ
aMzcQZHrGJCEX6ek8l8SFKou7vICzyajRSIK8gxWKBuQknP/9LKsoczV+xsAAwUD
/idXPkk4vRRHsCwc6I23fdI0ur52bzEqHiAIswNfO521YgLk2W1xyCLc2aYjc8Ni
nrMX1tCnEx0/gK7ICyJoWH1Vc7//79sWFtX2EaTO+Q07xjFX4E66WxJlCo9lOjos
Vk5qc7R+xzLDoLGFtbzaTRQFzf6yr7QTu+BebWLoPwNTiE8EGBECAA8FAkWfB7MC
GwwFCRLMAwAACgkQqKRH3OhWKJfvvACfbsF1WK193zM7vSc4uq51XsceLwgAoI0/
9GxdNhGQEAweSlQfhPa3yYXH
=o/Mx
-----END PGP PUBLIC KEY BLOCK-----
"""
