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


from testrunner import testhelp

import os, tempfile

from conary_test import rephelp
from conary_test import resources
from conary_test.cvctest import sigtest

from conary import versions, conarycfg, trove
from conary.conaryclient import mirror
from conary.lib import openpgpfile, openpgpkey
from conary.build import signtrove
from conary.repository import errors, netclient

def skipproxy(fn):
    def noproxy(*args, **kwargs):
        if 'CONARY_PROXY' in os.environ:
            raise testhelp.SkipTestException('Cannot test mirror with proxy')

        return fn(*args, **kwargs)
    noproxy.func_name = fn.func_name
    return noproxy

def mockversion(fn, testName, ver):
    # allow testing mirror code against servers that run against a different
    # protocol ver
    def frobversion(*args, **kwargs):
        old = netclient.ServerProxy.getProtocolVersion
        netclient.ServerProxy.getProtocolVersion = lambda self: ver
        try:
            fn(*args, **kwargs)
        finally:
            netclient.ServerProxy.getProtocolVersion = old
            pass
    frobversion.func_name = testName
    return frobversion

class MirrorTest(rephelp.RepositoryHelper):
    def setUp(self):
        rephelp.RepositoryHelper.setUp(self)
        # is the target repos (2nd repository) running?
        self.targetRunning = 0

    def tearDown(self):
        for idx in range(0, self.targetRunning+1):
            self.servers.stopServer(idx)

    def createMirrorUser(self, repos, serverName = "localhost"):
        label = versions.Label("%s@foo:bar" % serverName)
        self.addUserAndRole(repos, label, "mirror", "mirror")
        repos.addAcl(label, "mirror", None, None, write=True, remove=True)
        repos.setRoleIsAdmin(label, "mirror", True)
        repos.setRoleCanMirror(label, "mirror", True)
        # add a user without mirroring or admin access
        self.addUserAndRole(repos, label, "nomirror", "nomirror")
        repos.addAcl(label, "nomirror", None, None, write=True)
 
    def createTroves(self, repos, start, count, version='1.0', flavor=''):
        md5s = rephelp.sha1helper.md5FromString
        for i in range(start, start + count):
            self.addComponent('test%d:runtime' % i, version, flavor,
                              fileContents = [
                ("/test/file%d" % x, rephelp.RegularFile(contents="hello file %d\n" % x,
                                                         pathId = md5s("%032d" % x)) )
                for  x in range(0, i) ], repos = repos)
            self.addCollection('test%d' % i, version,
                               [ ("test%d:runtime" % i, version, flavor) ],
                              repos = repos)

    def runMirror(self, cfgFile, verbose = False, fastSync = False,
                  absolute=False):
        args = ["--config-file", cfgFile]
        if verbose:
            args.append("-v")
        if fastSync:
            args.append("--fast-sync")
        if absolute:
            args.append("--absolute")
        # Clean up some globals
        mirror.recursedGroups.clear()
        self.sleep(1.2)
        mirror.Main(args)
        self.sleep(1.2)

    def _openRepository(self, idx, serverName="localhost"):
        serverNames = serverName
        if isinstance(serverName, str):
            serverNames = [serverName]
        map = dict(self.cfg.repositoryMap)
        repo = self.openRepository(idx, serverName=serverNames)
        self.resetRepository(idx)
        # always use the latest version for addAcl - helpful for when
        # we mock version 46 protocol below
        createMirrorUser = mockversion(self.createMirrorUser, 'createMirrorUser',
                                       netclient.CLIENT_VERSIONS[-1])
        createMirrorUser(repo, serverName=serverNames[0])
        if idx: # restore the source map to avoid being clobbered by other openrepos
            self.cfg.repositoryMap = conarycfg.RepoMap(map)
            self.targetRunning = max(self.targetRunning, idx)
        return repo

    def createRepositories(self):
        self._openRepository(0)
        # this could be left open from a previous testsuite running
        self.stopRepository(1)
        self._openRepository(1)
        # now create the user=mirror client instances
        src = self.getRepositoryClient("mirror", "mirror", serverIdx=0)
        dst = self.getRepositoryClient("mirror", "mirror", serverIdx=1)
        # save the source and target maps
        self.sourceMap = src.c.map["localhost"]
        self.targetMap = dst.c.map["localhost"]
        return (src, dst)

    def createConfigurationFile(self, matchTroves=None, matchTroveSpecs=None,
                                labels=None, recurseGroups=False,
                                srcuser="mirror mirror",
                                dstuser="mirror mirror"):
        (fd, mirrorFile) = tempfile.mkstemp()
        os.close(fd)
        mirrorCfg = open(mirrorFile, "w")

        print >> mirrorCfg, "host localhost"
        if matchTroves:
            print >> mirrorCfg, "matchTroves ", matchTroves
        if matchTroveSpecs:
            print >> mirrorCfg, "matchTroveSpecs ", matchTroveSpecs
        if labels:
            print >> mirrorCfg, "labels ", " ".join(labels)
        if recurseGroups:
            print >> mirrorCfg, "recurseGroups True"
        print >> mirrorCfg
        print >> mirrorCfg, "[source]"
        print >> mirrorCfg, "user localhost %s" % srcuser
        print >> mirrorCfg, "repositoryMap localhost %s" % self.sourceMap
        print >> mirrorCfg
        print >> mirrorCfg, "[target]"
        print >> mirrorCfg, "user localhost %s" % dstuser
        print >> mirrorCfg, "repositoryMap localhost %s" % self.targetMap
        mirrorCfg.close()
        return mirrorFile

    def _flatten(self, troveSpec):
        l = []
        for name, versionD in troveSpec.iteritems():
            for version, flavorList in versionD.iteritems():
                l += [ (name, version, flavor) for flavor in flavorList ]
        l.sort()
        return l

    def compareRepositories(self, repos1, repos2, serverName = "localhost"):
        troveD1 = repos1.getTroveVersionList(serverName, { None : None },
                                             troveTypes = netclient.TROVE_QUERY_ALL)
        troveD2 = repos2.getTroveVersionList(serverName, { None : None },
                                             troveTypes = netclient.TROVE_QUERY_ALL)
        troveL1 = self._flatten(troveD1)
        troveL2 = self._flatten(troveD2)
        self.assertEqual(troveL1, troveL2)

        troves1 = repos1.getTroves(troveL1)
        troves2 = repos2.getTroves(troveL2)
        self.assertEqual(troves1, troves2)

        pgpKeys1 = repos1.getNewPGPKeys("localhost", -1)
        pgpKeys2 = repos2.getNewPGPKeys("localhost", -1)
        self.assertEqual(set(pgpKeys1), set(pgpKeys2))

    @skipproxy
    def testMirrorOptions(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()
        # test simple trove mirroring using the --fast-sync flag
        self.createTroves(sourceRepos, 10, 1, "1.0")
        self.runMirror(mirrorFile, fastSync=True)
        self.compareRepositories(sourceRepos, targetRepos)
        self.sleep(1.2)
        # test mirroring with --absolute passed in
        self.createTroves(sourceRepos, 10, 1, "2.0")
        self.runMirror(mirrorFile, absolute=True)
        self.compareRepositories(sourceRepos, targetRepos)
        
    @skipproxy
    def testSimpleMirror(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()
        # test simple trove mirroring
        self.createTroves(sourceRepos, 10, 1, "1.0")
        self.sleep(1.2)
        self.createTroves(sourceRepos, 10, 1, "2.0")
        self.sleep(1.2)
        self.createTroves(sourceRepos, 10, 1, "3.0")

        def mockedCs(real, *args, **kwargs):
            assert(kwargs['mirrorMode'])
            return real(*args, **kwargs)

        # ensure that mirrors use mirrorMode
        f = netclient.NetworkRepositoryClient.createChangeSetFile
        self.mock(netclient.NetworkRepositoryClient,
                  'createChangeSetFile',
                  lambda *args, **kwargs: mockedCs(f, *args, **kwargs ) )
        self.runMirror(mirrorFile)

        self.compareRepositories(sourceRepos, targetRepos)
        # add a trove with missing bits
        self.addCollection("test", "1.0", [":missing1"], repos = sourceRepos)
        self.runMirror(mirrorFile)

        # "test" must be present
        ts = sourceRepos.getTroveVersionList("localhost", { "test" : None })
        ts = self._flatten(ts)
        assert(sourceRepos.getTroves(ts))
        assert(targetRepos.getTroves(ts))
        
        # test:missing should not be present
        (n,v,f) = ts[0]
        ts = [("test:missing1", v, f)]
        self.assertRaises(errors.TroveMissing, sourceRepos.getTroves, ts)
        self.assertRaises(errors.TroveMissing, targetRepos.getTroves, ts)

    @skipproxy
    # this test the testsuite's compare function for correct operation
    def testCompareRepos(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()
        self.createTroves(sourceRepos, 1,1)
        # repos should be different
        self.assertRaises(AssertionError, self.compareRepositories,
                          sourceRepos, targetRepos)
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        # sign our trove
        sourceRepos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', sigtest.unexpiredKey)
        self.cfg.signatureKey = '7CCD34B5C5D9CD1F637F6743D4F8F127C267B79D'
        signtrove.signTroves(self.cfg, [ "test1:runtime" ])
        self.assertRaises(AssertionError, self.compareRepositories,
                          sourceRepos, targetRepos)
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        # test metadata changes
        if sourceRepos.c['localhost'].getProtocolVersion() < 47:
            return
        ti = trove.TroveInfo()
        mi = trove.MetadataItem()
        mi.shortDesc.set('testsuite is fun to run')
        ti.metadata.addItem(mi)
        tl = sourceRepos.getTroveVersionList("localhost", {"test1:runtime":None})
        tl = self._flatten(tl)
        sourceRepos.setTroveInfo(zip(tl, [ti] * len(tl)))
        self.assertRaises(AssertionError, self.compareRepositories,
                          sourceRepos, targetRepos)
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)

    testCompareReposV46 = mockversion(testCompareRepos, 'testCompareReposV46',
                                      46)

    @skipproxy
    def testMirrrorInfoExcluded(self):
        sourceRepos, targetRepos = self.createRepositories()
        # only mirror test10 troves
        mirrorFile = self.createConfigurationFile(matchTroves="+test10.*")
        self.createTroves(sourceRepos, 10, 3, "1.0")
        self.runMirror(mirrorFile)
        src = sourceRepos.getTroveVersionList("localhost", { None : None })
        src = self._flatten(src)
        self.assertEqual(len(src), 6)
        dst = targetRepos.getTroveVersionList("localhost", { None : None })
        dst = self._flatten(dst)
        self.assertEqual(len(dst), 2)
        # test mirroring signatures
        sourceRepos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', sigtest.unexpiredKey)
        self.cfg.signatureKey = '7CCD34B5C5D9CD1F637F6743D4F8F127C267B79D'
        signtrove.signTroves(self.cfg, [ "test10:runtime", "test11:runtime" ])
        self.runMirror(mirrorFile)
        signtrove.signTroves(self.cfg, [ "test12:runtime" ])
        self.runMirror(mirrorFile)
        troves1 = sourceRepos.getTroves(dst)
        troves2 = targetRepos.getTroves(dst)
        self.assertEqual(troves1, troves2)
                                
    @skipproxy
    def testMirrorInfo(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()
        self.createTroves(sourceRepos, 10, 2, "1.0")
        self.createTroves(sourceRepos, 10, 2, "2.0")
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        # test mirroring signatures
        sourceRepos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', sigtest.unexpiredKey)
        self.createTroves(sourceRepos, 20, 2, "1.1")
        self.cfg.signatureKey = '7CCD34B5C5D9CD1F637F6743D4F8F127C267B79D'
        signtrove.signTroves(self.cfg, [ "test10:runtime", "test11:runtime" ])
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        signtrove.signTroves(self.cfg, [ "test20:runtime" ])
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        # test mirroring metadata changes
        ti = trove.TroveInfo()
        mi = trove.MetadataItem()
        mi.shortDesc.set('This is the short description')
        ti.metadata.addItem(mi)
        # get the versions of the troves we're testing
        tl = [ "test%d:runtime" % x for x in (10,20,21) ]
        tl = sourceRepos.getTroveVersionList("localhost", dict.fromkeys(tl))
        tl = self._flatten(tl)
        tl = dict([ (n, (v,f)) for n,v,f in tl ])
        self.sleep(1.2)
        for tidx in (21, 20, 10):
            t = "test%d:runtime" % tidx
            sourceRepos.setTroveInfo([((t, tl[t][0], tl[t][1]), ti)])
            self.assertRaises(AssertionError, self.compareRepositories,
                                  sourceRepos, targetRepos)
            if tidx == 10:
                self.createTroves(sourceRepos, 30, 10, "3.0")
            self.runMirror(mirrorFile)
            self.compareRepositories(sourceRepos, targetRepos)
        # test mirroring with the addMetadataItem call
        mi = trove.MetadataItem()
        mi.longDesc.set("this should be areally long description, but it isn't")
        mi.url.set("http://localhost/")
        mi.keyValue['key'] = 'value'
        for tidx in (21, 20, 10):
            t = "test%d:runtime" % tidx
            sourceRepos.addMetadataItems([((t,tl[t][0],tl[t][1]), mi)])
            self.assertRaises(AssertionError, self.compareRepositories,
                                  sourceRepos, targetRepos)
            if tidx == 10:
                self.createTroves(sourceRepos, 40, 10, "3.1")
            self.runMirror(mirrorFile)
            self.compareRepositories(sourceRepos, targetRepos)
        
    @skipproxy
    def testMirror(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()
        # this is really ugly, but needed to ensure test consistency
        db = self.servers.getServer(0).reposDB.connect()
        cu = db.cursor()

        # test simple trove mirroring
        self.createTroves(sourceRepos, 1, 2, "1.0")
        self.createTroves(sourceRepos, 1, 2, "2.0")
        self.sleep(1.2)
        self.createTroves(sourceRepos, 1, 2, "3.0")
        # the following is just to get the trigger to fire
        cu.execute("update Instances set changed=1")
        db.commit()
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)

        # simulate multiple trove versions with the same mark
        self.createTroves(sourceRepos, 10, 2, "1.0", "~foo")
        self.createTroves(sourceRepos, 10, 2, "1.0", "~!foo")
        self.createTroves(sourceRepos, 10, 2, "1.1")
        # the following is just to get the trigger to fire
        cu.execute("update Instances set changed=1")
        db.commit()
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)

        # test mirroring of ascii keys
        sourceRepos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', sigtest.unexpiredKey)
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)

        # test mirroring of new trove signatures
        self.createTroves(sourceRepos, 20, 2)
        self.cfg.signatureKey = '7CCD34B5C5D9CD1F637F6743D4F8F127C267B79D'
        signtrove.signTroves(self.cfg, [ "test10:runtime", "test20:runtime" ])
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)

        fprint='F7440D78FE813C882212C2BF8AC2828190B1E477'
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fprint, '111111')
        self.cfg.signatureKey = fprint

        keyRing = open(resources.get_archive() + '/pubring.gpg')
        keyData = openpgpfile.exportKey(fprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()
        sourceRepos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)
        signtrove.signTroves(self.cfg, [ "test11:runtime", "test21:runtime" ])
        self.createTroves(sourceRepos, 30, 2)
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)

    @skipproxy
    def testMirrorLabels(self):
        sourceRepos, targetRepos = self.createRepositories()
        labels = ["localhost@test:foo", "localhost@test:bar"]
        mirrorFile = self.createConfigurationFile(labels = labels)
        def _check(repo, count):
            self.runMirror(mirrorFile)
            tl = repo.getTroveVersionList("localhost", { None : None },
                                          troveTypes = netclient.TROVE_QUERY_ALL)
            tl = self._flatten(tl)
            self.assertEqual(len(tl), count)
        # add troves that should not be mirrored
        self.createTroves(sourceRepos, 1, 5)
        # nothing should have made it in the target
        _check(targetRepos, 0)
        # add 10 troves on one label we want mirrored
        self.createTroves(sourceRepos, 10, 5, "/localhost@test:foo/1.0-1")
        _check(targetRepos, 10)
        # add 10 on the other label we want mirrored
        self.createTroves(sourceRepos, 20, 5, "/localhost@test:bar/1.0-1")
        _check(targetRepos, 20)
        # add troves on multiple labels
        self.createTroves(sourceRepos, 30, 5, "/localhost@test:foo/2.0-1")
        self.createTroves(sourceRepos, 30, 5, "/localhost@test:bar/2.0-1")
        self.createTroves(sourceRepos, 30, 5, "/localhost@test:baz/2.0-1")
        self.createTroves(sourceRepos, 30, 5)
        _check(targetRepos, 40)
        
    @skipproxy
    def testMatchTrovesMirror(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile(matchTroves = "-test20.* +.*")

        # test simple trove mirroring
        self.createTroves(sourceRepos, 10, 2)
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        set1 = sourceRepos.getTroveVersionList("localhost", { None : None }).items()

        # put some troves in the source that match the exclusion pattern
        self.createTroves(sourceRepos, 20, 1)
        self.runMirror(mirrorFile)
        set2 = targetRepos.getTroveVersionList("localhost", { None : None }).items()
        self.assertEqual(len([ x for x in set2 if x not in set1 ]), 0)

    @skipproxy
    def testMatchTroveSpecsMirror(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile(
            matchTroveSpecs="-test25.* +test2.*")

        # test simple trove mirroring
        self.createTroves(sourceRepos, 10, 1)
        self.createTroves(sourceRepos, 25, 1)
        self.createTroves(sourceRepos, 20, 1)
        self.runMirror(mirrorFile)
        set1 = sourceRepos.getTroveVersionList(
            "localhost", {None: None}).items()
        set2 = targetRepos.getTroveVersionList(
            "localhost", {None: None}).items()
        self.assertEqual(len(set1), 6)
        self.assertEqual(len(set2), 2)

    @skipproxy
    def testMirrorRemoved(self):
        def _checkMissing(repos, *args):
            for t in args:
                trv = repos.getTrove(t.getName(), t.getVersion(),
                                     t.getFlavor())
                self.assertEqual(trv.type(), trove.TROVE_TYPE_REMOVED)

        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()
        # grab a second target and mirror into it as well
        self.servers.stopServer(2)
        self._openRepository(2)
        target2Repos = self.getRepositoryClient("mirror", "mirror", serverIdx=2)
        self.target2Map = target2Repos.c.map["localhost"]
        mirrorFile2 = self.createConfigurationFile()
        fd = open(mirrorFile2, "a")
        print >> fd
        print >> fd, "[target2]"
        print >> fd, "user localhost mirror mirror"
        print >> fd, "repositoryMap localhost %s" % self.target2Map
        fd.close()
        
        self.createTroves(sourceRepos, 10, 2)
        # test simple trove mirroring
        self.runMirror(mirrorFile2)
        self.compareRepositories(sourceRepos, targetRepos)
        self.compareRepositories(sourceRepos, target2Repos)

        # now add a trove, mirror it, remove, mirror it, compare all along
        t = self.addComponent("removed:runtime", "1.0-1-1", repos=sourceRepos)
        self.addCollection("removed", "1.0", [t.getName()], repos=sourceRepos)
        self.runMirror(mirrorFile2)
        self.compareRepositories(sourceRepos, targetRepos)
        self.compareRepositories(sourceRepos, target2Repos)

        self.sleep(1.2)
        self.markRemoved(t.getName(), repos=sourceRepos)
        _checkMissing(sourceRepos, t)
        ver = sourceRepos.getTroveLatestVersion(t.getName(), t.getVersion().branch(),
                                                troveTypes = netclient.TROVE_QUERY_ALL)
        self.assertEqual(str(ver), '/localhost@rpl:linux/1.0-1-1')
        self.assertRaises(errors.TroveMissing, sourceRepos.getTroveLatestVersion,
                          t.getName(), t.getVersion().branch())
        # mirror the removal into the first target only
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        _checkMissing(targetRepos, t)
        ver = targetRepos.getTroveLatestVersion(t.getName(), t.getVersion().branch(),
                                                troveTypes = netclient.TROVE_QUERY_ALL)
        self.assertEqual(str(ver), '/localhost@rpl:linux/1.0-1-1')
        self.assertRaises(errors.TroveMissing, targetRepos.getTroveLatestVersion,
                          t.getName(), t.getVersion().branch())

        # add another set to simulate a more complex mirror operation
        t1 = self.addComponent('removed:runtime', '2.0', filePrimer=10, repos=sourceRepos)
        t2 = self.addComponent('removed:data', '2.0', filePrimer=11, repos=sourceRepos)
        self.addCollection('removed', '2.0', [':runtime', ':data'], repos=sourceRepos)
        self.runMirror(mirrorFile2)
        self.compareRepositories(sourceRepos, targetRepos)
        self.compareRepositories(sourceRepos, target2Repos)

        self.addComponent('removed:runtime', '2.1', filePrimer=20, repos=sourceRepos)
        self.addComponent('removed:data', '2.1', filePrimer=21, repos=sourceRepos)
        t = self.addCollection('removed', '2.1', [':runtime', ':data'], repos=sourceRepos)
        self.sleep(1.2)
        # remove the 2.0 stuff, leave the 2.1 alone
        self.markRemoved('removed=2.0', repos=sourceRepos)
        _checkMissing(sourceRepos, t1, t2)
        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        _checkMissing(targetRepos, t1, t2)
        # versions 2.0 should show up as removed in the target
        self.assertRaises(errors.TroveMissing, targetRepos.getTroveLatestVersion,
                          t1.getName(), t1.getVersion())
        self.assertRaises(errors.TroveMissing, targetRepos.getTroveLatestVersion,
                          t2.getName(), t2.getVersion())
        # latest versions should be 2.1
        self.assertEqual(targetRepos.getTroveLatestVersion(
            t1.getName(), t1.getVersion().branch() ), t.getVersion() )
        self.assertEqual(targetRepos.getTroveLatestVersion(
            t2.getName(), t2.getVersion().branch() ), t.getVersion() )
        
        # END OF TEST
        self.servers.stopServer(2)
        self.servers.stopServer(1)

    @skipproxy
    def testPathIdConflict(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()
        self.addComponent("trove1:data", fileContents = [
            ('/usr/share/t1/file1', 'this is file 1'),
            ('/usr/share/t1/file2', 'this is file 2'),
            ('/usr/share/common/file', 'this is a common file')],
                          repos = sourceRepos)
        self.addCollection("trove1", "1.0", [":data"],
                           repos = sourceRepos)
        self.addComponent("trove2:data", fileContents = [
            ('/usr/share/t2/file1', 'this is file 1'),
            ('/usr/share/t2/file2', 'this is file 2'),
            ('/usr/share/common/file', 'this is a common file')],
                          repos = sourceRepos)
        self.addCollection("trove2", "1.0", [":data"],
                           repos = sourceRepos)
        f = rephelp.RegularFile(pathId = '1', contents = 'foo\n')
        foo = rephelp.RegularFile(pathId = '222', contents = 'foobar\n')
        bar = rephelp.RegularFile(pathId = '222', contents = 'bar\n')
        self.addComponent('foo:runtime', '2.0-1-1', "",
                          fileContents = [ ( '/bin/foo', f),
                                           ( '/etc/config', foo ),
                                           ( '/lib/file', f) ],
                          repos = sourceRepos)
        self.addCollection("foo", "2.0-1-1", [":runtime"], repos = sourceRepos)
        self.addComponent('bar:runtime', '2.0-1-1', "",
                          fileContents = [ ( '/bin/bar', f),
                                           ( '/etc/config', bar ),
                                           ( '/lib/file', f) ],
                          repos = sourceRepos)
        self.addCollection("bar", "2.0-1-1", [":runtime"], repos = sourceRepos)

        self.runMirror(mirrorFile)
        self.compareRepositories(sourceRepos, targetRepos)
        self.servers.stopServer(1)

    @skipproxy
    def testCallbackMissingFiles(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()

        self.addComponent("trove:data", '1', fileContents="some")
        self.addCollection("trove", '1', [":data"])
        self.addCollection("group-foo", '1', ["trove"])

        # Mock mirrorRepository
        oldMirrorRepository = mirror.mirrorRepository
        self.mirrorRepositoryRun = False
        def mockedMirrorRepository(*args, **kwargs):
            callback = kwargs['callback']
            self.assertTrue(hasattr(callback, 'missingFiles'))
            self.mirrorRepositoryRun = True
            return oldMirrorRepository(*args, **kwargs)

        try:
            mirror.mirrorRepository = mockedMirrorRepository
            self.runMirror(mirrorFile)
            self.assertTrue(self.mirrorRepositoryRun)
        finally:
            mirror.mirrorRepository = oldMirrorRepository
            del self.mirrorRepositoryRun

    @skipproxy
    def testMirrorRecurseGroups(self):
        src, dst = self.createRepositories()
        mirrorFile = self.createConfigurationFile(matchTroves="+group-.*", recurseGroups=True)
        # create test10 and test11 troves
        self.createTroves(src, 10, 2)
        self.addCollection("group-test", "10", [("test10", "1.0")], repos=src)
        self.runMirror(mirrorFile)
        # test11 should not have made it through
        dstDict = dst.getTroveVersionList("localhost", { None : None },
                                          troveTypes = netclient.TROVE_QUERY_ALL)
        self.assertTrue(set(dstDict.keys()) == set(["test10", "test10:runtime", "group-test"]))
        # add test11 and mirror again
        self.addCollection("group-test", "11", [("test10", "1.0"), ("test11", "1.0")], repos=src)
        self.runMirror(mirrorFile)
        self.compareRepositories(src, dst)

    @skipproxy
    # test directly the mirrorRepository call to simulate how rBuilder
    # folk use it. also test different access acls.
    def testMirrorAccess(self):
        src, dst = self.createRepositories()
        mirrorFile = self.createConfigurationFile()
        cfg = mirror.MirrorConfiguration()
        cfg.host = "localhost"
        # get a non-mirror user access
        src1 = self.getRepositoryClient("nomirror", "nomirror", serverIdx=0)
        dst1 = self.getRepositoryClient("nomirror", "nomirror", serverIdx=1)
        self.createTroves(src, 10, 2)
        self.createTroves(src1, 20, 2)
        self.assertRaises(AssertionError, self.compareRepositories, src, dst)
        # this needs to raise an assertionerror before insufficient permission
        self.assertRaises(AssertionError, self.compareRepositories, src1, dst1)

        # test acess permissions
        self.assertRaises(errors.InsufficientPermission, mirror.mirrorRepository,
                              src1, dst, cfg)
        self.assertRaises(errors.InsufficientPermission, mirror.mirrorRepository,
                              src, dst1, cfg)
        mirror.mirrorRepository(src, dst, cfg)
        self.compareRepositories(src, dst)

    @skipproxy
    def testMirrorPublish(self):
        # tests publish by group access
        label = versions.Label("localhost@foo:bar")
        src, dst = self.createRepositories()
        # create users with a limited view
        src.deleteUserByName(label, "anonymous")
        src.addRole(label, "rmirror")
        src.addUser(label, "rmirror", "rmirror")
        src.addRoleMember(label, "rmirror", "rmirror")
        src.setRoleCanMirror(label, "rmirror", True)
        rsrc = self.getRepositoryClient("rmirror", "rmirror", serverIdx = 0)
        mirrorFile = self.createConfigurationFile(srcuser = "rmirror rmirror")
        # 
        self.createTroves(src, 1, 2, "1")
        g1 = self.addCollection("group-test", "1.0", [("test1", "1")], repos = src)
        src.addTroveAccess("rmirror", [g1.getNameVersionFlavor()])
        self.runMirror(mirrorFile)
        self.compareRepositories(rsrc, dst)
        self.assertRaises(AssertionError, self.compareRepositories, src, dst)
        # now remove the access 
        self.createTroves(src, 1, 1, "2")
        self.createTroves(src, 1, 1, "3")
        g2 = self.addCollection("group-test", "2.0", [("test1", "2")],
                                repos = src)
        g3 = self.addCollection("group-test", "3.0", [("test1", "3")],
                                repos = src)
        src.addTroveAccess("rmirror", [g2.getNameVersionFlavor(), g3.getNameVersionFlavor()])
        src.deleteTroveAccess("rmirror", [g1.getNameVersionFlavor()])
        self.runMirror(mirrorFile)
        # add back the access so we can comoare the repos
        s = rsrc.getTroveVersionList(label.getHost(), { None : None })
        d =  dst.getTroveVersionList(label.getHost(), { None : None })
        src.addTroveAccess("rmirror", [g1.getNameVersionFlavor()])
        self.compareRepositories(rsrc, dst)

    # open up a target that answers to multiple repository names
    def _getMultiTarget(self, idx, nameList):
        # this is dirty but it does the trick. we need to remap all
        # servers from nameList into the same target repository
        self.servers.stopServer(idx)
        self._openRepository(idx, serverName=nameList)
        server = self.servers.getCachedServer(idx)
        map = server.getMap()
        for x in nameList[1:]:
            map[x] = map[nameList[0]]
        dst = self.getRepositoryClient("mirror", "mirror", serverIdx=idx)
        dst.c.map = conarycfg.RepoMap(map)
        for x in nameList[1:]:
            dst.c.userMap.addServerGlob(x, "mirror", "mirror")
        dst.reopen()
        return dst
    # run a mirror config
    def _runMirrorCfg(self, src, dst, cfg, verbose=False):
        if verbose:
            from conary.lib import log
            log.setVerbosity(log.DEBUG)
        while 1:
            ret = mirror.mirrorRepository(src, dst, cfg)
            if not ret:
                break
        return

    @skipproxy
    def testMirrorMode(self):
        # open up two sources
        self._openRepository(0, serverName="myhost")
        self._openRepository(1, serverName="myotherhost")
        # get the repo clients
        src = self.getRepositoryClient("mirror", "mirror")

        self.addQuickTestComponent('test:runtime', '/myhost@rpl:linux/1.0-1-1',
                                   fileContents = [
            ('/bin/foo', rephelp.RegularFile(version='/myhost@rpl:linux/1.0-1-1',
                                             contents = 'foo')),
            ("/usr/foo", rephelp.Directory(version='/myhost@rpl:linux/1.0-1-1', perms=0777)),
            ], repos=src)
        old = self.addQuickTestComponent('test:runtime', '/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1-1',
                                         fileContents = [
            ( '/bin/foo', rephelp.RegularFile(version = '/myhost@rpl:linux/1.0-1-1',
                                              contents = 'foo')),
            ("/usr/foo", rephelp.Directory(version='/myhost@rpl:linux/1.0-1-1', perms=0777)),
            ], repos=src)

        new = self.addQuickTestComponent('test:runtime', '/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1.1-1',
                                         fileContents = [
            ( '/bin/foo', rephelp.RegularFile(version='/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1.1-1',
                                              contents = 'foo')),
            ("/usr/foo", rephelp.Directory(version='/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1.1-1',
                                           perms=0701)),
            ], repos=src)
        veryNew = self.addQuickTestComponent('test:runtime', '/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1.2-1',
                                             fileContents = [
            ( '/bin/foo', rephelp.RegularFile(version='/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1.1-1',
                                              contents = 'foo')),
            ("/usr/foo", rephelp.Directory(version='/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1.1-1',
                                           perms=0701)),
            ], repos=src)
        dst = self._getMultiTarget(2, ["myhost", "myotherhost"])
        # create the mirror config
        cfg = mirror.MirrorConfiguration()
        cfg.host = "myotherhost"
        self._runMirrorCfg(src, dst, cfg)

    @skipproxy
    # test mirroring of group recursion when groups include foreign troves
    def testMirrorForeign(self):
        # open up two sources
        self._openRepository(0, serverName="myhost")
        self._openRepository(1, serverName="myotherhost")
        # get the repo clients
        src = self.getRepositoryClient("mirror", "mirror")
        self.createTroves(src, 10, 2, "myhost@src:1/1.0")
        self.createTroves(src, 10, 2, "myotherhost@src:2/2.0")
        s1 = src.getTroveVersionList("myhost", { None : None })
        s2 = src.getTroveVersionList("myotherhost", { None : None })
        self.assertEqual(s1.keys(), s2.keys())
        # add a group that includes troves from both repos
        trv = self.addCollection("group-test", "myhost@src:test/10", [
            ("test10", "myhost@src:1/1.0"),
            ("test10", "myotherhost@src:2/2.0")], repos=src)
        grp = src.createChangeSet( [(trv.getName(), (None, None), (trv.getVersion(), trv.getFlavor()), True)],
                                   withFiles=False, withFileContents = False, recurse = True)
        dst = self._getMultiTarget(2, ["myhost","myotherhost"])

        # create the mirror config
        cfg = mirror.MirrorConfiguration()
        cfg.host = "myhost"
        cfg.labels = [versions.Label("myhost@src:test")]
        cfg.recurseGroups = True
        self.assertEqual(dst.getTroveVersionList("myhost", {None:None}), {})
        self.assertEqual(dst.getTroveVersionList("myotherhost", {None:None}), {})
        def _getTroves(src, dst):
            # check if we mirrored everything in the target
            s1 = src.getTroveVersionList("myhost", {None:None})
            s2 = src.getTroveVersionList("myotherhost", {None:None})
            s = self._flatten(s1) + self._flatten(s2)
            # a single call will return everything in the target, not just
            # stuff on "myhost". blame the netclient api...
            d = dst.getTroveVersionList("myhost", {None:None})
            d = self._flatten(d)
            return (s,d)
        self._runMirrorCfg(src,dst,cfg)
        s,d = _getTroves(src, dst)
        self.assertEqual(len(d), 5)
        self.assertEqual(len(s), 9)
        # test11 troves should not have made it over
        self.assertEqual(len([x for x in d if x[0].startswith("test11")]), 0)
        # add a new set of troves and check that they're mirrored over
        self.createTroves(src, 10, 2, "myhost@src:1/1.1")
        self.createTroves(src, 10, 2, "myotherhost@src:2/2.1")
        trv = self.addCollection("group-test", "myhost@src:test/10.1", [
            ("test10", "myhost@src:1/1.1"),
            ("test10", "myotherhost@src:2/2.1")], repos=src)
        self._runMirrorCfg(src,dst,cfg)
        s,d = _getTroves(src, dst)
        self.assertEqual(len(s), 18)
        self.assertEqual(len(d), 10)
        # test11 troves should not have made it over
        self.assertEqual(len([x for x in d if x[0].startswith("test11")]), 0)
        for x in d:
            self.assertTrue(x in s)

    @skipproxy
    def testUnchangedConfigFileMirrorSingleRepos(self):
        sourceRepos, targetRepos = self.createRepositories()
        mirrorFile = self.createConfigurationFile()

        self.addComponent('test:runtime', '/localhost@rpl:linux/1.0-1-1',
                       fileContents = [
            ('/etc/foo',
                rephelp.RegularFile(version='/localhost@rpl:linux/1.0-1-1',
                                    contents = 'foo\n')) ],
                                    repos = sourceRepos )
        self.addComponent('test:runtime', '/localhost@rpl:linux/2.0-1-1',
                       fileContents = [
            ('/etc/foo',
                rephelp.RegularFile(version='/localhost@rpl:linux/2.0-1-1',
                                    contents = 'foo\n')) ],
                                    repos = sourceRepos )

        self.runMirror(mirrorFile)

    @skipproxy
    def testUnchangedConfigFileMirrorDistRepos(self):
        # open up two sources
        self._openRepository(0, serverName="myhost")
        self._openRepository(1, serverName="myotherhost")
        # get the repo clients
        src = self.getRepositoryClient("mirror", "mirror")

        self.addComponent('test:runtime',
                       '/myhost@rpl:linux/1.0-1-1',
                       fileContents = [
            ('/etc/foo',
                rephelp.RegularFile(
                  version='/myhost@rpl:linux/1.0-1-1',
                  contents = 'foo\n')) ],
                  repos = src )

        self.addComponent('test:runtime',
                       '/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1-1',
                       fileContents = [
            ('/etc/foo',
                rephelp.RegularFile(
                  version='/myhost@rpl:linux/1.0-1-1',
                  contents = 'foo\n')) ],
                  repos = src )


        self.addComponent('test:runtime',
                       '/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1.1-1',
                       fileContents = [
            ('/etc/foo',
                rephelp.RegularFile(
                  version='/myhost@rpl:linux//myotherhost@rpl:linux/1.0-1.1-1',
                  contents = 'foo\n')) ],
                  repos = src )


        dst = self._getMultiTarget(2, ["myhost", "myotherhost"])
        # create the mirror config
        cfg = mirror.MirrorConfiguration()
        cfg.host = "myotherhost"
        self._runMirrorCfg(src, dst, cfg)
