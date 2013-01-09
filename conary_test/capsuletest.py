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


from testrunner import testhelp
import os

import conary_test
from conary import trove
from conary_test import rephelp

from conary import errors, files, rpmhelper, showchangeset, versions
from conary import conaryclient
from conary.deps import deps
from conary.cmds import query, queryrep, updatecmd
from conary.lib import util
from conary.lib.sha1helper import (md5FromString, sha1FromString, md5String,
        md5ToString)
from conary.repository import changeset, filecontents, filecontainer
from conary.conaryclient import update, cml, systemmodel

class CapsuleTest(rephelp.RepositoryHelper):

    id1 = md5FromString("00010001000100010001000100010001")
    id2 = md5FromString("00010001000100010001000100010002")
    id3 = md5FromString("00010001000100010001000100010003")
    id4 = md5FromString("00010001000100010001000100010004")
    id5 = md5FromString("00010001000100010001000100010005")

    fid1 = sha1FromString("1001000100010001000100010001000100010001")
    fid2 = sha1FromString("1001000100010001000100010001000100010002")
    fid3 = sha1FromString("1001000100010001000100010001000100010003")
    fid4 = sha1FromString("1001000100010001000100010001000100010004")
    fid5 = sha1FromString("1001000100010001000100010001000100010005")

    v1 = versions.ThawVersion('/localhost@foo:bar/1.0:1.0-1-1')

    @conary_test.rpm
    def test01_Trove(self):
        t = trove.Trove('foo:continer', self.v1, deps.Flavor())
        t.addFile(self.id1, '/1', self.v1, self.fid1)
        t.addFile(self.id2, '/2', self.v1, self.fid2)
        h = { rpmhelper.NAME    : 'foorpm',
              rpmhelper.VERSION : '1.0',
              rpmhelper.RELEASE : '1',
              rpmhelper.ARCH    : 'i386',
              rpmhelper.EPOCH   : [ 1 ] }
        t.addRpmCapsule('foo.rpm', self.v1, self.fid5, h)

        assert([ x[1] for x in t.iterFileList() ] ==
                    [ '/1', '/2'] )
        assert([ x[1] for x in t.iterFileList(members = True) ] ==
                    [ '/1', '/2'] )
        assert([ x[0:2] for x in t.iterFileList(capsules = True) ] ==
                    [ (trove.CAPSULE_PATHID, 'foo.rpm' ) ] )
        assert([ x[1] for x in t.iterFileList(members = True,
                                              capsules = True) ] ==
                    [ '/1', 'foo.rpm', '/2' ])
        assert([ x[1] for x in t.iterFileList(members = False,
                                              capsules = False) ] == [])

    @conary_test.rpm
    @testhelp.context('rollback')
    def test02_Repository(self):
        def csContents(repos, job):
            p = os.path.join(self.workDir, 'foo.ccs')
            repos.createChangeSetFile(job, p)
            fc = filecontainer.FileContainer(
                                    util.ExtendedFile(p, buffering = False))
            l = []
            info = fc.getNextFile()
            while info:
                l.append(info)
                info = fc.getNextFile()

            return l

        cmp = self.addRPMComponent("simple:rpm=1.0", 'simple-1.0-1.i386.rpm')
        repos = self.openRepository()
        reposCmp = repos.getTrove(*cmp.getNameVersionFlavor())
        assert(reposCmp == cmp)
        l = csContents(repos, [ ("simple:rpm", (None, None),
                                cmp.getNameVersionFlavor()[1:], True) ])
        assert([ x[0][0:16] for x in l ] ==
               ['CONARYCHANGESET',
                '\x8d\xeb\xb9\x15M!\xe0\xc5\x08D\x87\xf7\xe1\xf9fW',
                trove.CAPSULE_PATHID ])
        cmp = self.addRPMComponent("simple:rpm=1.1", 'simple-1.1-1.i386.rpm')

        self.updatePkg('simple:rpm=1.0')
        assert(os.path.isdir(self.rootDir + "/dir"))
        self.verifyFile(self.rootDir + "/config", "config\n")
        self.verifyFile(self.rootDir + "/normal", "normal\n")

        self.updatePkg('simple:rpm=1.1')
        self.verifyFile(self.rootDir + "/config", "changed-config\n")
        self.verifyFile(self.rootDir + "/normal", "changed-normal\n")

        self.rollback(self.rootDir, 1)
        self.verifyFile(self.rootDir + "/config", "config\n")
        self.verifyFile(self.rootDir + "/normal", "normal\n")

        self.writeFile(self.rootDir + '/config', 'config\nlocal config\n')
        rc, str = self.captureOutput(self.updatePkg, 'simple:rpm=1.1')
        self.assertEquals(str, 'warning: /config saved as /config.rpmsave\n')
        self.verifyFile(self.rootDir + '/config', 'changed-config\n')
        self.rollback(self.rootDir, 1)
        self.verifyFile(self.rootDir + '/config', 'config\nlocal config\n')

        rc, str = self.captureOutput(self.erasePkg, self.rootDir, 'simple:rpm')
        self.assertEquals(str, 'warning: /config saved as /config.rpmsave\n')
        assert(not os.path.exists(self.rootDir + '/config'))
        self.rollback(self.rootDir, 1)
        self.verifyFile(self.rootDir + '/config', 'config\nlocal config\n')

        rc, str = self.captureOutput(self.rollback, self.rootDir, 0)
        self.assertEquals(str, 'warning: /config saved as /config.rpmsave\n')
        assert(not os.path.exists(self.rootDir + "/dir"))
        assert(not os.path.exists(self.rootDir + "/config"))
        assert(not os.path.exists(self.rootDir + "/normal"))

    @conary_test.rpm
    @testhelp.context('rollback')
    def test03_Overlap(self):
        osA1 = self.addRPMComponent("overlap-same-A:rpm=1.0",
                                    'overlap-same-A-1.0-1.i386.rpm')
        osB1 = self.addRPMComponent("overlap-same-B:rpm=1.0",
                                    'overlap-same-B-1.0-1.i386.rpm')
        self.updatePkg('overlap-same-A:rpm=1.0')
        self.updatePkg('overlap-same-B:rpm=1.0')
        assert(os.path.exists(self.rootDir + '/A-1'))
        assert(os.path.exists(self.rootDir + '/B-1'))
        self.checkOwners('/file', [ osA1, osB1 ])

        self.erasePkg(self.rootDir, 'overlap-same-B:rpm')
        assert(os.path.exists(self.rootDir + '/A-1'))
        assert(os.path.exists(self.rootDir + '/file'))
        self.checkOwners('/file', [ osA1 ])

        self.rollback(2)
        self.checkOwners('/file', [ osA1, osB1 ])

        self.rollback(1)
        self.checkOwners('/file', [ osA1 ])

    @conary_test.rpm
    def test04_DirectoryPreexisting(self):
        self.addRPMComponent("simple:rpm=1.0", 'simple-1.0-1.i386.rpm')
        os.mkdir(self.rootDir + '/dir')
        self.updatePkg('simple:rpm')
        assert(os.path.exists(self.rootDir + '/normal'))

    @conary_test.rpm
    def test05_NoJobSplitting(self):
        self.addRPMComponent("simple:rpm=1.0", 'simple-1.0-1.i386.rpm')
        self.addRPMComponent("ownerships:rpm=1.0", 'ownerships-1.0-1.i386.rpm')

        try:
            old = self.cfg.updateThreshold
            self.cfg.updateThreshold = 1
            rc, txt = self.captureOutput(self.updatePkg,
                                         [ 'simple:rpm', 'ownerships:rpm' ],
                                         raiseError=True, info=True)

            # if job splitting happens you get "Job 1 of 2" here because of the
            # small updateThreashold
            self.assertEquals(txt, '    Install ownerships:rpm=1.0-1-1\n'
                                   '    Install simple:rpm=1.0-1-1\n')

            # make sure having a critical update doesn't make the splitting
            # break
            criticalUpdateInfo = updatecmd.CriticalUpdateInfo()
            criticalUpdateInfo.setCriticalTroveRegexps(['simple:rpm'])
            itemsList = [
                ('simple:rpm', (None, None), (None, None), True),
                ('ownerships:rpm', (None, None), (None, None), True),
            ]
            client = conaryclient.ConaryClient(self.cfg)
            updJob = client.newUpdateJob()
            client.prepareUpdateJob(updJob, itemsList, migrate=True,
                                    criticalUpdateInfo = criticalUpdateInfo)
        finally:
            self.cfg.updateThreshold = old

        # info components should get split out of course CNY-3387
        self.addComponent("info-disk:user=1.0")
        rc, txt = self.captureOutput(self.updatePkg,
                        [ 'simple:rpm', 'info-disk:user' ], info = True )
        self.assertEquals(txt,
            'Job 1 of 2:\n'
            '    Install info-disk:user=1.0-1-1\n'
            'Job 2 of 2:\n'
            '    Install simple:rpm=1.0-1-1\n')

    @conary_test.rpm
    def test06_GhostPreexisting(self):
        self.addRPMComponent("ghost:rpm=1.0", 'ghost-1.0-1.i386.rpm')
        os.mkdir(self.rootDir + '/foo')
        self.writeFile(self.rootDir + '/foo/ghost', 'orig contents')
        rc, str = self.captureOutput(self.updatePkg, 'ghost:rpm')
        assert(not str)
        self.verifyFile(self.rootDir + '/foo/ghost', 'orig contents')

    @conary_test.rpm
    def test07_BadRpmCapsule(self):
        cmp = self.addRPMComponent("ghost:rpm=1.0", 'epoch-1.0-1.i386.rpm')
        repos = self.openRepository()
        orig = self.workDir + '/ghost.ccs'
        modified = self.workDir + '/ghost-new.ccs'
        repos.createChangeSetFile([ (cmp.getName(), (None, None),
                                     cmp.getNameVersionFlavor()[1:],
                                     True) ], orig)
        fc = filecontainer.FileContainer(
                                util.ExtendedFile(orig, buffering = False))
        newFc = filecontainer.FileContainer(
                        util.ExtendedFile(modified, "w", buffering = False))
        # CONARYCHANGESET
        (name, tag, contents) = fc.getNextFile()
        newFc.addFile(name, filecontents.FromFile(contents), tag,
                      precompressed = True)
        # the RPM
        (name, tag, contents) = fc.getNextFile()
        contents = filecontents.FromString("busted!")
        newFc.addFile(name, contents, tag)
        cs = changeset.ChangeSetFromFile(modified)
        # this throws away the output about the install failing
        self.assertRaises(files.Sha1Exception, self.captureOutput,
                          self.updatePkg, self.rootDir, cs)

    @conary_test.rpm
    @testhelp.context('sysmodel', 'rollback')
    def test08_OverlapConflict(self):
        osA1 = self.addRPMComponent("overlap-same-A:rpm=1.0",
                                    'overlap-same-A-1.0-1.i386.rpm')
        self.addCollection('overlap-same-A', '1.0', [ ':rpm' ])
        osB1 = self.addRPMComponent("overlap-conflict:rpm",
                                    'overlap-conflict-1.0-1.i386.rpm')
        self.addCollection('overlap-conflict', '1.0', [ ':rpm' ])
        self.updatePkg('overlap-same-A:rpm=1.0')
        self.assertRaises(update.UpdateError,
            self.updatePkg, 'overlap-conflict:rpm=1.0', raiseError=True)

        self.updatePkg('overlap-conflict:rpm=1.0', replaceFiles = True)
        self.checkOwners('/file', [ osB1 ])
        self.rollback(1)
        self.checkOwners('/file', [ osA1 ])

        # now test that allowing overlaps via groups pathConflicts is OK
        # in system model
        self.addCollection('group-dist', '1.0', [
            ('overlap-same-A:rpm', '1.0'),
            ('overlap-conflict:rpm', '1.0')],
            pathConflicts=['/file'])

        root = self.cfg.root
        util.mkdirChain(root+'/etc/conary')
        file(root+'/etc/conary/system-model', 'w').write(
            'install group-dist=localhost@rpl:linux/1.0\n')
        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)
        updatecmd.doModelUpdate(self.cfg, model, modelFile, [],
            keepExisting=True)



        self.resetRoot()
        self.updatePkg(['overlap-same-A:rpm',
                        'overlap-conflict:rpm=1.0'], replaceFiles = True)
        self.rollback(0)

    @conary_test.rpm
    def test08_CapsuleTroveDisplay(self):
        cmp = self.addRPMComponent("simple:rpm=1.0", 'simple-1.0-1.i386.rpm')
        repos = self.openRepository()
        cs = repos.createChangeSet([ (cmp.getName(), (None, None),
                                      cmp.getNameVersionFlavor()[1:], True) ])

        db = self.openDatabase()
        self.updatePkg('simple:rpm')

        for method, args in [
                (showchangeset.displayChangeSet, (None, cs, None, self.cfg) ),
                (queryrep.displayTroves, (self.cfg, [ "simple:rpm" ]) ),
                (query.displayTroves, (db, self.cfg, [ "simple:rpm" ]) ) ]:
            rc, res = self.captureOutput(method, ls = True, *args)
            self.assertEquals(res, '/config\n/dir\n/normal\n')

            rc, res = self.captureOutput(method, capsules = True, ls = True,
                                         *args)
            self.assertEquals(res, 'simple-1.0-1.i386.rpm\n')

    @conary_test.rpm
    def test09_CapsuleInstallFlags(self):
        # test some random flags, like --test, --justdb, and
        # --replace-unmanaged-files
        cmp = self.addRPMComponent("simple:rpm=1.0", 'simple-1.0-1.i386.rpm')
        self.updatePkg('simple:rpm', test = True)
        assert(not os.path.exists(self.rootDir + '/normal'))
        assert(not self.owners('/normal'))
        str = os.popen('rpm --root %s -qa' % self.rootDir).readlines()
        assert(not str)

        self.updatePkg('simple:rpm', justDatabase = True)
        assert(len(self.owners('/normal')) == 1)
        assert(not os.path.exists(self.rootDir + '/normal'))
        str = os.popen('rpm --root %s -qa' % self.rootDir).readlines()
        self.assertEquals(str, [ 'simple-1.0-1.i386\n' ])
        rc, str = self.captureOutput(self.erasePkg, self.rootDir,
                                     'simple:rpm',
                                     justDatabase = True)
        self.assertEquals(str,
            'warning: cannot remove /normal: No such file or directory\n'
            'warning: cannot remove /config: No such file or directory\n'
            'warning: cannot remove /dir: No such file or directory\n')
        str = os.popen('rpm --root %s -qa' % self.rootDir).readlines()
        self.assertEquals(str, [])
        assert(not self.owners('/normal'))

        self.writeFile(self.rootDir + '/normal', 'placeholder')
        rc, str = self.captureOutput(self.updatePkg, 'simple:rpm')
        self.assertEquals(str,
                'error: changeset cannot be applied:\n'
                'applying update would cause errors:\n'
                '/normal is in the way of a newly created file in '
                        'simple:rpm=/localhost@rpl:linux/1.0-1-1[]\n')
        rc, str = self.captureOutput(self.updatePkg, 'simple:rpm',
                                     replaceUnmanagedFiles = True)
        self.assertEquals(str, '')
        self.verifyFile(self.rootDir + '/normal', 'normal\n')

        self.resetRoot()

        self.updatePkg('simple:rpm', justDatabase = True, skipCapsuleOps = True)
        assert(len(self.owners('/normal')) == 1)
        assert(not os.path.exists(self.rootDir + '/normal'))
        str = os.popen('rpm --root %s -qa' % self.rootDir).readlines()
        self.assertEquals(str, [ ])
        # syncCapsuleDatabase would helpfully erase the trove before we get a
        # chance to do the same thing, so turn it off for this test
        self.cfg.syncCapsuleDatabase = False
        try:
            rc, str = self.captureOutput(self.erasePkg, self.rootDir,
                                     'simple:rpm',
                                     justDatabase = True,
                                     skipCapsuleOps = True)
        finally:
            self.cfg.resetToDefault('syncCapsuleDatabase')
        self.assertEquals(str,
            'warning: cannot remove /normal: No such file or directory\n'
            'warning: cannot remove /config: No such file or directory\n'
            'warning: cannot remove /dir: No such file or directory\n')
        str = os.popen('rpm --root %s -qa' % self.rootDir).readlines()
        self.assertEquals(str, [])
        assert(not self.owners('/normal'))

    @conary_test.rpm
    def test10_UnchangedRPM(self):
        cmp = self.addRPMComponent("simple:rpm=1.0-1-1",
                                   'simple-1.0-1.i386.rpm')
        cmp = self.addRPMComponent("simple:rpm=1.0-1-2",
                                   'simple-1.0-1.i386.rpm')
        self.updatePkg('simple:rpm=1.0-1-1')
        self.updatePkg('simple:rpm=1.0-1-2')
        # this makes sure the right version is installed
        self.erasePkg(self.rootDir, 'simple:rpm=1.0-1-2')

    @conary_test.rpm
    def test11_UnchangedUpdate(self):
        # CNY-3335
        simple10  = self.addRPMComponent("simple:rpm=1.0-1-1",
                                         'simple-1.0-1.i386.rpm')
        simple101 = self.addRPMComponent("simple:rpm=1.0.1-1-1",
                                         'simple-1.0.1-1.i386.rpm',
                                         versus = simple10)
        self.updatePkg('simple:rpm=1.0-1-1')
        self.updatePkg('simple:rpm=1.0.1-1-1')

    @conary_test.rpm
    def test_unchangedFileid(self):
        # CNY-3719
        simple10  = self.addRPMComponent("simple:rpm=1.0-1-1",
                                         'simple-1.0-1.i386.rpm')
        # Commit a new version so all the file versions are new
        simple11 = self.addRPMComponent("simple:rpm=1.1-1-1",
                                         'simple-1.1-1.i386.rpm',
                                         versus = simple10)
        # Now go back to the old fileids, the filevers will be new again
        simple101  = self.addRPMComponent("simple:rpm=1.0.1-1-1",
                                         'simple-1.0.1-1.i386.rpm',
                                         versus = simple11)
        self.updatePkg('simple:rpm=1.0-1-1')
        self.updatePkg('simple:rpm=1.0.1-1-1')

    @conary_test.rpm
    def test12_TroveZeroEpoch(self):
        t = trove.Trove('foo:continer', self.v1, deps.Flavor())
        t.addFile(self.id1, '/1', self.v1, self.fid1)
        t.addFile(self.id2, '/2', self.v1, self.fid2)
        h = { rpmhelper.NAME    : 'foorpm',
              rpmhelper.VERSION : '1.0',
              rpmhelper.RELEASE : '1',
              rpmhelper.ARCH    : 'i386',
              rpmhelper.EPOCH   : [ 0 ] }
        t.addRpmCapsule('foo.rpm', self.v1, self.fid5, h)

        self.assertEqual(t.troveInfo.capsule.rpm.name(), 'foorpm')
        self.assertEqual(t.troveInfo.capsule.rpm.epoch(), 0)
        self.assertEqual(t.troveInfo.capsule.rpm.version(), '1.0')
        self.assertEqual(t.troveInfo.capsule.rpm.release(), '1')
        self.assertEqual(t.troveInfo.capsule.rpm.arch(), 'i386')

    @conary_test.rpm
    def testChangesetExploder(self):
        simple10  = self.addRPMComponent("simple:rpm=1.0-1-1",
                                         'simple-1.0-1.i386.rpm')
        repos = self.openRepository()
        cs = repos.createChangeSet([
            (simple10.getName(), (None, None),
             simple10.getNameVersionFlavor()[1:], True) ])
        changeset.ChangesetExploder(cs, self.rootDir)
        assert(os.path.isdir(self.rootDir + '/dir'))
        self.assertEquals(
            os.stat(self.rootDir + '/normal').st_size, 7)

    @conary_test.rpm
    @testhelp.context('rollback')
    def testRpmFileColoring(self):
        if os.uname()[4] != 'x86_64':
            # this test only works on x86_64 platforms
            return

        i386md5 = '0fc54eafb8daf886ff7d43c4448acc71'
        x64md5 = '4cb13908ca1d7989be493c581a6fa1d3'

        oldtrv  = self.addRPMComponent("tmpwatch:rpm=0.1-1-1",
                                       'tmpwatch-2.9.1-1.i386.rpm')
        i386trv  = self.addRPMComponent("tmpwatch:rpm=1.0-1-1",
                                        'tmpwatch-2.9.7-1.1.el5.2.i386.rpm')
        x64trv  = self.addRPMComponent("tmpwatch:rpm=2.0-1-1",
                                       'tmpwatch-2.9.7-1.1.el5.2.x86_64.rpm')

        self.updatePkg('tmpwatch:rpm=1.0-1-1')
        self.checkMd5('/usr/sbin/tmpwatch', i386md5)
        self.checkOwners('/usr/sbin/tmpwatch',  [ i386trv ])
        self.updatePkg('tmpwatch:rpm=2.0-1-1', keepExisting = True)
        self.checkMd5('/usr/sbin/tmpwatch', x64md5)
        self.checkOwners('/usr/sbin/tmpwatch',  [ x64trv ])
        self.erasePkg(self.rootDir, 'tmpwatch:rpm=2.0-1-1')
        # it's a shame we don't restore the file here
        assert(not os.path.exists(self.rootDir + '/usr/sbin/tmpwatch'))
        self.rollback(2)
        self.checkMd5('/usr/sbin/tmpwatch', x64md5)
        self.rollback(1)
        self.checkMd5('/usr/sbin/tmpwatch', i386md5)
        self.checkOwners('/usr/sbin/tmpwatch',  [ i386trv ])

        self.resetRoot()
        self.updatePkg('tmpwatch:rpm=2.0-1-1')
        self.checkMd5('/usr/sbin/tmpwatch', x64md5)
        self.checkOwners('/usr/sbin/tmpwatch',  [ x64trv ])
        self.updatePkg('tmpwatch:rpm=1.0-1-1', keepExisting = True)
        self.checkMd5('/usr/sbin/tmpwatch', x64md5)
        self.checkOwners('/usr/sbin/tmpwatch',  [ x64trv ])
        self.erasePkg(self.rootDir, 'tmpwatch:rpm=1.0-1-1')
        self.checkMd5('/usr/sbin/tmpwatch', x64md5)

        self.resetRoot()
        self.updatePkg([ 'tmpwatch:rpm=1.0-1-1', 'tmpwatch:rpm=2.0-1-1' ])
        self.checkMd5('/usr/sbin/tmpwatch', x64md5)
        self.checkOwners('/usr/sbin/tmpwatch',  [ x64trv ])

        # this still causes conflicts
        rc, str = self.captureOutput(self.updatePkg, 'tmpwatch:rpm=0.1')
        assert('conflicts' in str)

    @conary_test.rpm
    @testhelp.context('rollback')
    def test14_testRpmSharedHardLinks(self):
        # we build fro msource here rather than use addRPMComponent to get
        # the hard links right
        recipestr1 = r"""
class HardLinks(CapsuleRecipe):
    name = 'hardlinkconflict'
    version = '%s'
    clearBuildReqs()

    def setup(r):
        r.addCapsule('%s')
"""
        built, d = self.buildRecipe(
            recipestr1 % ('1.0_1',
                          'hardlinkconflict-1.0-1.x86_64.rpm'), "HardLinks")
        built, d = self.buildRecipe(
            recipestr1 % ('1.0_2',
                          'hardlinkconflict-1.0-2.x86_64.rpm'), "HardLinks")

        self.updatePkg('hardlinkconflict=1.0_1')
        self.updatePkg('hardlinkconflict=1.0_2', keepExisting = True)
        assert(len(self.owners('/foo')) == 2)
        self.erasePkg(self.rootDir, 'hardlinkconflict=1.0_2')
        assert(len(self.owners('/foo')) == 1)

        self.resetRoot()
        self.updatePkg([ 'hardlinkconflict=1.0_1', 'hardlinkconflict=1.0_2' ])
        assert(len(self.owners('/foo')) == 2)
        self.erasePkg(self.rootDir, 'hardlinkconflict=1.0_2')
        assert(len(self.owners('/foo')) == 1)

    @conary_test.rpm
    def test15_SetupHandling(self):
        oldTrv  = self.addRPMComponent("setup:rpm=0.1-1-1",
                                       'setup-2.5.58-7.el5.noarch.rpm')
        self.resetRoot()
        util.mkdirChain(self.rootDir + '/etc')
        self.writeFile(self.rootDir + '/etc/passwd',
                       "passwd junk\n"
                       "root:wrong rootuser\n")
        self.writeFile(self.rootDir + '/etc/group',
                       "group junk\n"
                       "root:wrong root group\n")
        self.updatePkg('setup:rpm', replaceFiles = True)

        p = open(self.rootDir + '/etc/passwd').readlines()
        self.assertEquals(p[0], 'root:*:0:0:root:/root:/bin/bash\n')
        self.assertEquals(p[-1], 'passwd junk\n')
        assert(not os.path.exists(self.rootDir + '/etc/passwd.rpmnew'))

        p = open(self.rootDir + '/etc/group').readlines()
        self.assertEquals(p[0], 'root::0:root\n')
        self.assertEquals(p[-1], 'group junk\n')
        assert(not os.path.exists(self.rootDir + '/etc/group.rpmnew'))

    @conary_test.rpm
    def testScriptFailures(self):
        'test what happens when RPM scripts fail'
        # CNY-3454
        f = self.addRPMComponent('aaa_first:rpm=1.0-1-1',
                                 'aaa_first-1.0-1.noarch.rpm')
        f = self.addRPMComponent('failpost:rpm=1.0-1-1',
                                 'failpost-1.0-1.noarch.rpm')
        f = self.addRPMComponent('failpostun:rpm=1.0-1-1',
                                 'failpostun-1.0-1.noarch.rpm')
        f = self.addRPMComponent('failpre:rpm=1.0-1-1',
                                 'failpre-1.0-1.noarch.rpm')
        f = self.addRPMComponent('failpreun:rpm=1.0-1-1',
                                 'failpreun-1.0-1.noarch.rpm')
        f = self.addRPMComponent('zzz_last:rpm=1.0-1-1',
                                 'zzz_last-1.0-1.noarch.rpm')
        f == f

        self.updatePkg('aaa_first:rpm=1.0-1-1')
        self.checkOrdering('aaa_first-1.0-1.noarch')
        self.verifyFile(self.rootDir + '/dummy/aaa_first')
        util.mkdirChain(self.rootDir+'/var/tmp')
        rc, str = self.captureOutput(self.updatePkg, 'failpost:rpm=1.0-1-1')
        self.assertEquals(str,
            'warning: %post(failpost-1.0-1.noarch) scriptlet failed, exit status 127\n')
        self.checkOrdering('failpost-1.0-1.noarch')
        self.assertEquals(rc, None)
        self.verifyFile(self.rootDir + '/dummy/file')
        self.updatePkg('zzz_last:rpm=1.0-1-1')
        self.checkOrdering('zzz_last-1.0-1.noarch')
        self.verifyFile(self.rootDir + '/dummy/zzz_last')
        self.resetRoot(); util.mkdirChain(self.rootDir+'/var/tmp')


        rc, str = self.captureOutput(self.updatePkg,
                       ['aaa_first:rpm=1.0-1-1',
                        'failpost:rpm=1.0-1-1',
                        'zzz_last:rpm=1.0-1-1'])
        self.assertEquals(str,
            'warning: %post(failpost-1.0-1.noarch) scriptlet failed, exit status 127\n')
        self.assertEquals(rc, None)
        self.checkOrdering('aaa_first-1.0-1.noarch'
                           ' failpost-1.0-1.noarch'
                           ' zzz_last-1.0-1.noarch')
        self.verifyFile(self.rootDir + '/dummy/aaa_first')
        self.verifyFile(self.rootDir + '/dummy/file')
        self.verifyFile(self.rootDir + '/dummy/zzz_last')
        self.resetRoot(); util.mkdirChain(self.rootDir+'/var/tmp')

        self.updatePkg(['aaa_first:rpm=1.0-1-1',
                        'failpostun:rpm=1.0-1-1',
                        'zzz_last:rpm=1.0-1-1'])
        self.verifyFile(self.rootDir + '/dummy/aaa_first')
        self.verifyFile(self.rootDir + '/dummy/file')
        self.checkOrdering('aaa_first-1.0-1.noarch'
                           ' failpostun-1.0-1.noarch'
                           ' zzz_last-1.0-1.noarch')
        rc, str = self.captureOutput(self.erasePkg, self.rootDir,
            'failpostun:rpm=1.0-1-1')
        self.assertEquals(str,
            'warning: %postun(failpostun-1.0-1.noarch) scriptlet failed, exit status 127\n')
        self.verifyNoFile(self.rootDir + '/dummy/file')
        self.verifyFile(self.rootDir + '/dummy/zzz_last')
        self.resetRoot(); util.mkdirChain(self.rootDir+'/var/tmp')

        rc, str = self.captureOutput(self.updatePkg,
                        ['aaa_first:rpm=1.0-1-1',
                        'failpre:rpm=1.0-1-1',
                        'zzz_last:rpm=1.0-1-1'])
        self.assertEquals(str, '\n'.join((
          'error: %pre(failpre-1.0-1.noarch) scriptlet failed, exit status 127',
          'error: install: %pre scriptlet failed (2), skipping failpre-1.0-1',
          'error: RPM failed to install requested packages: failpre-1.0-1.noarch',
          '')))
        self.assertEquals(rc, None)
        self.checkOrdering('aaa_first-1.0-1.noarch'
                           ' failpre-1.0-1.noarch'
                           ' zzz_last-1.0-1.noarch')
        self.verifyFile(self.rootDir + '/dummy/aaa_first')
        self.verifyNoFile(self.rootDir + '/dummy/file')
        self.verifyFile(self.rootDir + '/dummy/zzz_last')
        self.resetRoot(); util.mkdirChain(self.rootDir+'/var/tmp')

        rc, str = self.captureOutput(self.updatePkg,
                       ['aaa_first:rpm=1.0-1-1',
                        'failpreun:rpm=1.0-1-1',
                        'zzz_last:rpm=1.0-1-1'])
        self.assertEquals(rc, None)
        self.assertEquals(str, '')
        self.checkOrdering('aaa_first-1.0-1.noarch'
                           ' failpreun-1.0-1.noarch'
                           ' zzz_last-1.0-1.noarch')
        self.verifyFile(self.rootDir + '/dummy/aaa_first')
        self.verifyFile(self.rootDir + '/dummy/file')
        self.verifyFile(self.rootDir + '/dummy/zzz_last')
        rc, str = self.captureOutput(self.erasePkg,
            self.rootDir, 'failpreun:rpm=1.0-1-1')
        self.assertEquals(str,
            'error: %preun(failpreun-1.0-1.noarch) scriptlet failed, exit status 127\n')
        self.verifyNoFile(self.rootDir + '/dummy/file')

    @conary_test.rpm
    def testNonRootUnpackFailures(self):
        'test what happens when RPM unpack fails as non-root'
        # CNY-3462
        f = self.addRPMComponent('aaa_first:rpm=1.0-1-1',
                                 'aaa_first-1.0-1.noarch.rpm')
        f = self.addRPMComponent('dev:rpm=1.0-1-1',
                                 'dev-1.0-1.noarch.rpm')
        rc, str = self.captureOutput(self.updatePkg,
                       ['aaa_first:rpm=1.0-1-1',
                        'dev:rpm=1.0-1-1'])
        warning = str.strip().split('\n')[-1]
        self.assertEquals(warning,
                          'warning: RPM failed to unpack dev-1.0-1.noarch')
        self.verifyFile(self.rootDir + '/dummy/aaa_first')

    @conary_test.rpm
    def testDuplicateRpms(self):
        # CNY-3470
        v1 = self.addRPMComponent("simple:rpm=1.0-1", 'simple-1.0-1.i386.rpm')
        v2 = self.addRPMComponent("simple:rpm=1.0-2", 'simple-1.0-1.i386.rpm')

        self.updatePkg('simple:rpm=1.0-1')
        self.updatePkg('simple:rpm=1.0-2', keepExisting = True)
        self.erasePkg(self.rootDir, 'simple:rpm=1.0-1')
        self.erasePkg(self.rootDir, 'simple:rpm=1.0-2')

        self.updatePkg('simple:rpm=1.0-1')
        self.updatePkg('simple:rpm=1.0-2', keepExisting = True)
        self.erasePkg(self.rootDir, [ 'simple:rpm=1.0-1', 'simple:rpm=1.0-2' ])

        self.updatePkg('simple:rpm=1.0-1')
        self.updatePkg('simple:rpm=1.0-2')
        self.erasePkg(self.rootDir, 'simple:rpm=1.0-2')

    @conary_test.rpm
    def testNetSharedPath(self):
        # CNY-3503
        p = self.addRPMComponent('netshared:rpm=1.0-1',
                                 'netshared-1.0-1.noarch.rpm')

        util.mkdirChain(self.cfg.root + '/etc/rpm/')
        #self.updatePkg('netshared:rpm=1.0-1')
        #self.verifyFile(self.rootDir + '/local/shouldexist')
        ##self.verifyFile(self.rootDir + '/excluded/shouldnotexist')

        self.resetRoot()
        util.mkdirChain(self.cfg.root + '/etc/rpm/')
        util.mkdirChain(self.cfg.root + '/excluded')
        self.writeFile(self.cfg.root + '/excluded/shouldnotexist', 'unmanaged')

        # inside the method to let @conary_test.rpm keep the import conditional
        import rpm
        rpm.addMacro('_netsharedpath', '/excluded:/usr/local')
        try:
            self.updatePkg('netshared:rpm=1.0-1')
        finally:
            rpm.delMacro('_netsharedpath')

        self.verifyFile(self.rootDir + '/local/shouldexist')
        self.verifyFile(self.rootDir + '/excluded/shouldnotexist', 'unmanaged')

        self.erasePkg(self.rootDir, 'netshared:rpm')

    @conary_test.rpm
    def testFileOverrides(self):
        # CNY-3586; CNY-3590
        # Unfortunately we can't use addRPMComponent, we need the policy to
        # mark files as being overwritten
        recipe = r"""
class TestPackage(CapsuleRecipe):
    name = 'with-config-special'
    version = '0.1'

    clearBuildReqs()
    def setup(r):
        r.addCapsule('with-config-special-0.3-1.noarch.rpm')
        r.Create('/usr/share/with-config-special.txt',
            contents='some other content\n', mode=0755)
        r.Create('/usr/share/new-content.txt',
            contents='some new content\n', mode=0644)
"""
        pkgname = 'with-config-special'
        self.makeSourceTrove(pkgname, recipe)
        built = self.cookFromRepository(pkgname)
        nvf = built[0]

        self.updatePkg('%s=0.1-1-1' % pkgname)
        self.verifyFile(self.rootDir + '/usr/share/with-config-special.txt',
            'Some extra data\n', perms=0644)
        # File additions should be honored
        self.verifyFile(self.rootDir + '/usr/share/new-content.txt',
            'some new content\n', perms=0644)

    @conary_test.rpm
    def testFileModeChanges(self):
        # CNY-3590
        # Unfortunately we can't use addRPMComponent, we need the policy to
        # mark files as being overwritten
        recipe = r"""
class TestPackage(CapsuleRecipe):
    name = 'with-config-special'
    version = '0.1'

    clearBuildReqs()
    def setup(r):
        r.addCapsule('with-config-special-0.3-1.noarch.rpm')
        r.SetModes('/usr/share/with-config-special.txt', 0755)
"""
        pkgname = 'with-config-special'
        self.makeSourceTrove(pkgname, recipe)
        built = self.cookFromRepository(pkgname)
        nvf = built[0]

        self.updatePkg('%s=0.1-1-1' % pkgname)
        # Make sure we DO NOT change the mode here (see CNY-3590)
        self.verifyFile(self.rootDir + '/usr/share/with-config-special.txt',
            'Some extra data\n', perms=0644)

    @conary_test.rpm
    def testUnchangedRPM(self):
        cmp1 = self.addRPMComponent("simple:rpm=1.0-1-1",
                                    'simple-1.0-1.i386.rpm')
        self.updatePkg('simple:rpm')
        cmp1 = self.addRPMComponent("simple:rpm=1.0-1-2",
                                    'simple-1.0-1.i386.rpm')
        self.updatePkg('simple:rpm')

    @conary_test.rpm
    @testhelp.context('rollback')
    def testFileTypeChange(self):
        self.addRPMComponent("file-type-change:rpm=1-1-1",
                             'file-type-change-1-1.i386.rpm')
        self.addRPMComponent("file-type-change:rpm=2-1-1",
                             'file-type-change-2-1.i386.rpm')
        self.updatePkg('file-type-change:rpm=1')
        self.assertEquals(os.readlink(self.rootDir + '/test'), 'foo')
        self.updatePkg('file-type-change:rpm=2')
        self.verifyFile(self.rootDir + '/test', '')
        self.rollback(1)
        self.assertEquals(os.readlink(self.rootDir + '/test'), 'foo')

    @conary_test.rpm
    def testChangingSharedUpdateRPM(self):
        'CNY-3620'
        if 'x86_64' not in str(self.cfg.buildFlavor):
            raise testhelp.SkipTestException('Skip test on x86 arch')
        # avoid needing to flavor using names
        su1032 = self.addRPMComponent('shared-update-32:rpm=1.0',
                                      'shared-update-1.0-1.i386.rpm')
        su1064 = self.addRPMComponent('shared-update-64:rpm=1.0',
                                      'shared-update-1.0-1.x86_64.rpm')
        su1132 = self.addRPMComponent('shared-update-32:rpm=1.1',
                                      'shared-update-1.1-1.i386.rpm',
                                      versus=su1032)
        su1164 = self.addRPMComponent('shared-update-64:rpm=1.1',
                                      'shared-update-1.1-1.x86_64.rpm',
                                      versus=su1064)
        self.updatePkg(['shared-update-32:rpm=1.0', 'shared-update-64:rpm=1.0'])
        self.verifyFile(self.rootDir + '/usr/share/test', 'contents1.0\n')
        self.updatePkg(['shared-update-32:rpm=1.1', 'shared-update-64:rpm=1.1'])
        self.verifyFile(self.rootDir + '/usr/share/test', 'contents1.1\n')

    @conary_test.rpm
    def testRPMJobOrdering(self):
        """
        Ensure that introducing a native conary package in the dependency chain
        between two encapsuled RPM packages causes Conary to split the three
        packages into separate jobs.

        foo:rpm -> bar:runtime -> baz:rpm
        """

        self.addRPMComponent('baz:rpm=1.0', 'simple-1.0-1.i386.rpm')
        self.addComponent('bar:runtime=1.0',
            requires=deps.parseDep('trove: baz:rpm'))
        self.addRPMComponent('foo:rpm=1.0', 'ownerships-1.0-1.i386.rpm',
            requires=deps.parseDep('trove: bar:runtime'))

        rc, txt = self.captureOutput(self.updatePkg,
            [ 'foo:rpm', ], resolve=True, raiseError=True, info=True)

        self.assertEquals(txt, 'Job 1 of 3:\n'
                               '    Install baz:rpm=1.0-1-1\n'
                               'Job 2 of 3:\n'
                               '    Install bar:runtime=1.0-1-1\n'
                               'Job 3 of 3:\n'
                               '    Install foo:rpm=1.0-1-1\n')

    @conary_test.rpm
    @testhelp.context('rollback')
    def testCapsuleLocalRollbacks(self):
        try:
            self.cfg.localRollbacks = True
            c = self.addRPMComponent("simple:rpm=1.0", 'simple-1.0-1.i386.rpm')
            csPath = self.workDir + '/simple.ccs'
            repos = self.getRepositoryClient()
            repos.createChangeSetFile([
                (c.getName(), ( None, None ),
                              ( c.getVersion(), c.getFlavor() ), True ) ]
                                      , csPath)
            file("/tmp/foo", "w").write(c.freeze())
            self.addRPMComponent("simple:rpm=1.1", 'simple-1.1-1.i386.rpm')
            self.updatePkg('simple:rpm=1.0')

            testPath = os.path.join(self.rootDir, 'normal')
            self.verifyFile(testPath, 'normal\n')
            f = file(testPath, "w")
            f.write("rollback check")

            self.updatePkg('simple:rpm=1.1')
            self.verifyFile(testPath, 'changed-normal\n')

            e = self.assertRaises(errors.MissingRollbackCapsule,
                                  self.rollback, 1)
            assert('simple-1.0-1.i386.rpm' in str(e))
            self.rollback(1, capsuleChangesets = [ csPath ] )
            f.write("rollback check")

            self.updatePkg('simple:rpm=1.1')
            self.verifyFile(testPath, 'changed-normal\n')
            self.writeFile(self.workDir + '/other', 'some file')
            self.rollback(1, capsuleChangesets = [ self.workDir ] )
            f.write("rollback check")

            self.erasePkg(self.rootDir, 'simple:rpm')
            self.rollback(1, capsuleChangesets = [ self.workDir ] )
        finally:
            self.cfg.localRollbacks = False

    def checkMd5(self, path, md5):
        d = md5String(open(self.rootDir + path).read())
        self.assertEquals(md5ToString(d), md5)

    def owners(self, path):
        db = self.openDatabase()
        return set( x[0:3] for x in db.iterFindPathReferences(
                                            path, justPresent = True) )

    def checkOrdering(self, troves):
        lastOrder = [x[22:].strip() for x in
                     file(self.rootDir + '/var/log/conary').readlines()
                     if 'RPM install order' in x][-1]
        self.assertEquals(lastOrder.split(': ', 1)[1], troves)

    def checkOwners(self, path, troves):
        assert(self.owners(path) ==
                set( x.getNameVersionFlavor() for x in troves ))

    @conary_test.rpm
    def testEncapToNative(self):
        """
        Update from encapsulated package to native package with the same
        contents and modes, ensure that the file does not disappear.

        @tests: CNY-3762
        """
        self.addRPMComponent('foo:rpm=1.0', 'simple-1.0-1.i386.rpm')
        self.updatePkg('foo:rpm', raiseError=True)

        recipe = r"""
class TestPackage(CapsuleRecipe):
    name = 'foo'
    version = '2.0'

    clearBuildReqs()
    def setup(r):
        r.addArchive('simple-1.0-1.i386.rpm', dir='/', preserveOwnership=True)
"""
        src = self.makeSourceTrove('foo', recipe)
        self.cookFromRepository('foo')[0]
        self.updatePkg(['-foo:rpm', 'foo:runtime'], depCheck=False,
                raiseError=True)
        path = os.path.join(self.rootDir, 'normal')
        self.assertEqual(os.stat(path).st_size, 7)

    @conary_test.rpm
    def testEncapToNativeSloppy(self):
        """
        Update from encapsulated package to native package with the same
        contents but different modes, ensure that the file does not disappear.

        @tests: CNY-3762
        """
        self.addRPMComponent('foo:rpm=1.0', 'simple-1.0-1.i386.rpm')
        self.updatePkg('foo:rpm', raiseError=True)

        recipe = r"""
class TestPackage(CapsuleRecipe):
    name = 'foo'
    version = '2.0'

    clearBuildReqs()
    def setup(r):
        r.addArchive('simple-1.0-1.i386.rpm', dir='/')
        r.Ownership('nobody', 'nobody', '.*')
        r.SetModes('/dir', 0700)
        r.SetModes('/normal', 0600)
"""
        src = self.makeSourceTrove('foo', recipe)
        self.cookFromRepository('foo')[0]
        self.updatePkg(['-foo:rpm', 'foo:runtime'], depCheck=False,
                raiseError=True)
        path = os.path.join(self.rootDir, 'normal')
        self.assertEqual(os.stat(path).st_size, 7)
        self.assertEqual(oct(os.stat(path).st_mode), '0100600')
        path = os.path.join(self.rootDir, 'dir')
        self.assertEqual(oct(os.stat(path).st_mode), '040700')

    @conary_test.rpm
    def testEncapToNativeChanged(self):
        """
        Update from encapsulated to native package with some different contents

        @tests: CNY-3762
        """
        # Intentionally put a trailing slash on the root dir to tickle bugs
        # where a path not created with joinPaths() might cause mismatches. Of
        # course the most common real-world case, root = '/', has a "trailing"
        # slash, whereas usually in the testsuite a chroot is used and no
        # trailing slash is present.
        self.rootDir += '/'

        self.addRPMComponent('foo:rpm=1.0', 'simple-1.0-1.i386.rpm')
        self.updatePkg('foo:rpm', raiseError=True)

        recipe = r"""
class TestPackage(CapsuleRecipe):
    name = 'foo'
    version = '2.0'

    clearBuildReqs()
    def setup(r):
        r.addArchive('simple-1.0-1.i386.rpm', dir='/')
        r.Ownership('nobody', 'nobody', '.*')
        r.SetModes('/dir', 0700)
        r.Create('/normal', contents='different stuff\n', mode=0600)
"""
        src = self.makeSourceTrove('foo', recipe)
        self.cookFromRepository('foo')[0]
        self.updatePkg(['-foo:rpm', 'foo:runtime'], depCheck=False,
                raiseError=True)
        path = os.path.join(self.rootDir, 'normal')
        self.assertEqual(os.stat(path).st_size, 16)
        self.assertEqual(oct(os.stat(path).st_mode), '0100600')
        path = os.path.join(self.rootDir, 'dir')
        self.assertEqual(oct(os.stat(path).st_mode), '040700')
