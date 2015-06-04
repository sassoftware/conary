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
from testutils import sock_utils
import copy
import itertools
import os
import shutil
import tempfile
import time
import SimpleHTTPServer

from conary_test import recipes
from conary_test import rephelp

#conary
from conary import callbacks
from conary import conarycfg, conaryclient
from conary import errors as gerrors
from conary import sqlite3
from conary import trove
from conary import versions
from conary.cmds import updatecmd
from conary.deps import arch, deps
from conary.lib import httputils
from conary.lib import util
from conary.repository import changeset, calllog, errors, filecontainer
from conary.repository import netclient
from conary.server import server as conaryserver


class NetclientTest(rephelp.RepositoryHelper):
    def testGetFileContents(self):
        # create a testcase:source component
        # add a file called "foo"
        # rename file to "bar"
        # verify that "bar"'s contents can be retrieved
        origDir = os.getcwd()
        self.resetRepository()
        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.writeFile('foo', 'hi\n')
        self.addfile('testcase.recipe')
        self.addfile('foo', binary = True)
        self.commit()
        self.rename('foo', 'bar')
        self.commit()
        repos = self.openRepository()
        # find the version of the testcase:source component
        troveVer = repos.getAllTroveLeaves('localhost',
                  { 'testcase:source' : None })['testcase:source'].keys()[0]
        # find the version of the file
        barId = None
        for f in repos.iterFilesInTrove('testcase:source', troveVer,
                                        deps.Flavor(), withFiles=True):
            pathId, path, fileId, version, fileObj = f
            if path == 'bar':
                barId = (fileId, version)
            elif path == 'testcase.recipe':
                recipeIds = (fileId, version)
        if barId is None:
            self.fail('version of renamed file not found')
        f = repos.getFileContents( [ barId ])[0]
        assert(f.get().read() == 'hi\n')

        # get multiple contents at the same time
        f, recipe = repos.getFileContents([ barId, recipeIds ])
        assert(f.get().read() == 'hi\n')
        assert(recipe.get().read() == recipes.testRecipe1)
        os.chdir(origDir)

    def testFileContentsErrors(self):
        # set up two repositores. create a shadow of test:runtime from
        # the localhost repository into the localhost1 repository.
        # Ask the localhost1 for the file contents from a file that
        # resides on localhost.  The file stream does not exist on
        # localhost1, so it should cause an exception.
        self.openRepository(1)
        t = self.addQuickTestComponent('test:runtime', '1.0-1-1')
        self.addCollection('test', '1.0-1-1', [':runtime'])
        # get the fileId and version of the file that we created.
        fileList = list(t.iterFileList())
        assert(len(fileList) == 1)
        fileId, version = fileList[0][2:4]
        # create the shadow in localhost1
        self.mkbranch("1.0-1-1", "localhost1@rpl:shadow", "test",
                      shadow = True, binaryOnly=True)
        # build up the version string that points at the shadow
        shadowVersion = versions.VersionFromString(
            '/localhost@rpl:linux//localhost1@rpl:shadow/1.0-1-1')
        # open the repository here when the proxy we use will be up to date
        repos = self.openRepository()
        try:
            repos.getFileContents([(fileId, shadowVersion)])
        except errors.FileStreamNotFound, e:
            assert((e.fileId, e.fileVer) == (fileId, shadowVersion))
        else:
            assert(0)

        # ask for a random fileId, should always fail
        bogusV = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        bogusFileId = '1' * 20
        bogusPathId = '2' * 16
        try:
            repos.getFileContents([(bogusFileId, bogusV)])
        except errors.FileStreamNotFound, e:
            assert((e.fileId, e.fileVer) == (bogusFileId, bogusV))
        else:
            assert(0)

        try:
            repos.getFileVersions([(bogusPathId, bogusFileId, bogusV)])
        except errors.FileStreamMissing, e:
            assert(e.fileId == bogusFileId)
        else:
            assert(0)

        # try getting the contents of a file which doesn't have contents
        t = self.addComponent('symtest:runtime', '1.0-1-1',
                              fileContents = [ ('/symlink',
                                                rephelp.Symlink('something')) ])
        fileList = list(t.iterFileList())
        assert(len(fileList) == 1)
        linkFileId, linkVersion = fileList[0][2:4]
        try:
            repos.getFileContents([ (linkFileId, linkVersion) ])
        except errors.FileHasNoContents, e:
            assert((e.fileId, e.fileVer) == (linkFileId, linkVersion))
        else:
            assert(0)

        # play a nasty trick on the server by removing the contents
        # to test missing contents error handling.  It's OK to do this
        # since the server is about to be reset anyway...
        server = self.servers.getServer(0)
        shutil.rmtree(server.contents.getPath())
        try:
            f = repos.getFileContents([ (fileId, version) ])
        except errors.FileContentsNotFound, e:
            assert((e.fileId, e.fileVer) == (fileId, version))
        else:
            assert(0)

    def testAnonymousAccess(self):
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        repos = self.openRepository()
        items = repos.getTroveLeavesByLabel(
                            { 'testcase' : { self.cfg.buildLabel : None } } )
        assert(len(items) == 1)

        port = self.cfg.repositoryMap['localhost'].split(':')[-1].split('/')[0]
        cfg = copy.copy(self.cfg)
        cfg.user = self.cfg.user.__class__()
        client = conaryclient.ConaryClient(cfg).getRepos()

        items = client.getTroveLeavesByLabel(
                            { 'testcase' : { self.cfg.buildLabel : None } } )
        assert(len(items) == 1)

    def _testBranchPathIds(self, shadow):
        repos = self.openRepository()
        self.makeSourceTrove("PathIdTest", recipes.pathIdTest1)
        self.logFilter.add()
        version = self.cookFromRepository("PathIdTest")[0][1]
        self.logFilter.remove()
        version = versions.VersionFromString(version)
        trunk = version.branch()

        if shadow:
            branch = version.createShadow(
                                    versions.Label("localhost@rpl:branch"))
        else:
            branch = version.createBranch(
                                    versions.Label("localhost@rpl:branch"))

        ids = repos.getPackageBranchPathIds("PathIdTest:source", trunk)
        assert(len(ids) == 3)

        self.mkbranch("1.0-1", "@rpl:branch", "PathIdTest:source",
                      shadow = shadow)

        self.updateSourceTrove("PathIdTest", recipes.pathIdTest2,
                               "localhost@rpl:branch")
        version = self.cookFromRepository("PathIdTest",
                    buildLabel = versions.Label("localhost@rpl:branch"))[0][1]
        branch = versions.VersionFromString(version).branch()
        branchIds = repos.getPackageBranchPathIds("PathIdTest:source", branch)

        ids = repos.getPackageBranchPathIds("PathIdTest:source", trunk)
        assert(len(ids) == 3)
        branchIds = repos.getPackageBranchPathIds("PathIdTest:source", branch)
        assert(len(branchIds) == 2)
        assert(ids["/lib/1"][2] == branchIds["/lib/1"][2])

        self.updateSourceTrove("PathIdTest", recipes.pathIdTest3)
        self.cookFromRepository("PathIdTest")

        ids = repos.getPackageBranchPathIds("PathIdTest:source", trunk)
        assert(len(ids) == 5)
        branchIds = repos.getPackageBranchPathIds("PathIdTest:source", branch)
        assert(len(branchIds) == 2)
        assert(ids["/lib/1"][2] == branchIds["/lib/1"][2])
        assert(ids["/lib/2"][2] == branchIds["/lib/2"][2])

        self.updateSourceTrove("PathIdTest", recipes.pathIdTest4,
                               "localhost@rpl:branch")
        self.cookFromRepository("PathIdTest",
                buildLabel = versions.Label("localhost@rpl:branch"))[0][1]
        ids = repos.getPackageBranchPathIds("PathIdTest:source", trunk)
        assert(len(ids) == 5)
        branchIds = repos.getPackageBranchPathIds("PathIdTest:source", branch)
        assert(len(branchIds) == 4)
        assert(ids["/lib/1"][2] == branchIds["/lib/1"][2])
        assert(ids["/lib/2"][2] == branchIds["/lib/2"][2])
        assert(ids["/lib/3"][2] == branchIds["/lib/3"][2])

    def testBranchPathIdsForShadow(self):
        self._testBranchPathIds(True)

    def testBranchPathIdsForBranch(self):
        self._testBranchPathIds(False)

    def testCreateChangeset(self):
        ver = '/localhost@rpl:linux/1.0-1-1'
        self.addQuickTestComponent('test:runtime', ver)
        repos = self.openRepository()
        flavor = deps.parseFlavor('')
        version = versions.VersionFromString(ver)
        csfile = os.path.join(self.workDir, 'foo.ccs')
        repos.createChangeSetFile(
            [('test:runtime', (None, flavor), (version, flavor), True)], csfile,
            primaryTroveList = [('test:runtime', version, flavor)])
        cs = changeset.ChangeSetFromFile(csfile)
        assert(cs.getPrimaryTroveList() == [('test:runtime', version, flavor)])

    def testCreateChangeset2(self):
        self.openRepository()
        repos = self.openRepository(1)
        t1 = self.addComponent('foo:run', '1', '', [('/etc/foo', 'foo\n'),
                                                    ('/etc/bar', 'bam\n')])
        t2 = self.addComponent('foo:run', '/localhost1@rpl:branch/1.0-1-1', '',
                                [('/etc/foo', 'foo2\n'),
                                 ('/etc/bar', 'bam2\n')])
        cs = repos.createChangeSet([('foo:run',
                                     (t1.getVersion(), t1.getFlavor()),
                                     (t2.getVersion(), t2.getFlavor()),
                                    False)])

    def testCreateChangesetViaHTTPProxy(self):
        # CNY-2058, CNY-2056
        self.openRepository()

        # first add test:runtime to the repository (not via HTTP proxy)
        ver = '/localhost@rpl:linux/1.0-1-1'
        self.addQuickTestComponent('test:runtime', ver)
        flavor = deps.parseFlavor('')
        version = versions.VersionFromString(ver)
        csfile = os.path.join(self.workDir, 'foo.ccs')

        # now switch to using a HTTP proxy
        h = rephelp.HTTPProxy(os.path.join(self.workDir, "http-cache"))
        proxyUri = h.start()
        if proxyUri is None:
            raise testhelp.SkipTestException('Squid is not installed')
        proxyUrl = "http://" + proxyUri
        proxies = dict(http = proxyUrl, https = proxyUrl)

        # switch client config to talk to the proxy
        self.cfg.configLine('proxy http://localhost:%s' % h.port)
        client = conaryclient.ConaryClient(self.cfg)
        repos = client.getRepos()

        # create the changeset and verify
        repos.createChangeSetFile(
            [('test:runtime', (None, flavor), (version, flavor), True)], csfile,
            primaryTroveList = [('test:runtime', version, flavor)])
        cs = changeset.ChangeSetFromFile(csfile)
        assert(cs.getPrimaryTroveList() == [('test:runtime', version, flavor)])

    def testTimestamps(self):
        """
        Ensure that the repository is resetting timestamps proprely.
        """
        # create an absolute changeset
        flavor = deps.parseFlavor('')
        cs = changeset.ChangeSet()
        # add a pkg diff
        v = versions.VersionFromString('/localhost@foo:bar/1.2-1-2',
                                       timeStamps=[1.000])
        pkg = trove.Trove('test', v, flavor, None)
        pkg.troveInfo.sourceName.set('test:source')
        pkg.addTrove('test:foo', v, flavor, byDefault=True)
        pkg.computeDigests()

        # add the 'test' package
        diff = pkg.diff(None)[0]
        cs.newTrove(diff)
        cs.addPrimaryTrove('test', v, flavor)

        # add the test:foo component
        foo = trove.Trove('test:foo', v, flavor, None)
        pkg.troveInfo.sourceName.set('test:source')
        foo.computeDigests()
        diff = foo.diff(None)[0]
        cs.newTrove(diff)

        repos = self.openRepository()
        now = time.time()

        from conary.cmds.updatecmd import UpdateCallback
        callback = UpdateCallback()
        repos.commitChangeSet(cs, callback=callback)

        trvs = repos.getTroves([('test', v, flavor), ('test:foo', v, flavor)])
        assert(trvs[0].getVersion().trailingRevision().getTimestamp() ==
               trvs[1].getVersion().trailingRevision().getTimestamp())
        assert(trvs[0].getVersion().trailingRevision().getTimestamp() >= now)

    def testGetTroveLeavesByBranch(self):
        self.addComponent('foo:r', '1.0', '!readline')
        self.addComponent('foo:r', '2.0', 'readline,~!ssl')
        self.addComponent('foo:r', '2.0', '!readline,~ssl')
        self.addComponent('foo:r', '2.0', '!readline,~!ssl')

    def testUnknownMethod(self):
        repos = self.openRepository()
        self.assertRaises(errors.MethodNotSupported,
                          repos.c['localhost'].callWrapper)

    def testConfigFilesRaisePathIdsConflict(self):
        t1 = self.addComponent('foo:runtime', '1', '',
                               [('/etc/foo', 'contents0\n')])
        t2 = self.addComponent('bar:runtime', '1', '',
                               [('/etc/foo', 'contents1\n')])
        t3 = self.addComponent('bang:runtime', '1', '',
                               [('/etc/foo', 'contents0\n')])
        repos = self.openRepository()

        # new format changesets don't conflict here as the fileIds are different
        repos.createChangeSet([
            (t1.getName(), (None, None), (t1.getVersion(), t1.getFlavor()),
             True),
            (t2.getName(), (None, None), (t2.getVersion(), t2.getFlavor()),
             True) ])

        # we don't conflict here either, since they aren't diffs
        repos.createChangeSet([
            (t1.getName(), (None, None), (t1.getVersion(), t1.getFlavor()),
             True),
            (t3.getName(), (None, None), (t3.getVersion(), t3.getFlavor()),
             True) ])

        repos.c['localhost'].setProtocolVersion(41)
        try:
            repos.createChangeSet([
                (t1.getName(), (None, None), (t1.getVersion(), t1.getFlavor()),
                 True),
                (t2.getName(), (None, None), (t2.getVersion(), t2.getFlavor()),
                 True) ])
        except changeset.PathIdsConflictError, e:
            assert str(e) == ('PathIdsConflictError:\n'
                              '  /etc/foo (bar:runtime 1-1-1)\n'
                              '     conflicts with\n'
                              '  /etc/foo (foo:runtime 1-1-1)')
        else:
            assert 0, 'expecting pathId conflict'

    def testFind64BitTroves(self):
        self.cfg.flavorPreferences = arch.getFlavorPreferences(
            [[ deps.Dependency('x86_64') ]])

        self.addComponent('foo:run', '1-1-1', 'is:x86_64')
        trv = self.addComponent('foo:run', '1-1-2', 'is:x86')

        def _getFlavors(d, name):
            return [ str(x) for x in itertools.chain(*d[name].itervalues()) ]

        repos = self.openRepository()
        n = 'foo:run'
        l = trv.getVersion().trailingLabel()
        f = deps.parseFlavor('is:x86_64 x86')
        d = repos.getTroveLeavesByLabel({n: {l : [f]}}, bestFlavor = True)
        self.assertEqual(_getFlavors(d, n), ['is: x86_64'])

        self.resetRepository()
        repos = self.openRepository()

        self.addComponent('foo:run', '1-1-1', 'is:x86_64 x86')
        trv = self.addComponent('foo:run', '1-1-2', 'is:x86')
        d = repos.getTroveLeavesByLabel({n: {l : [f]}}, bestFlavor = True)
        self.assertEqual(_getFlavors(d, n), ['is: x86 x86_64'])

    def testFindNegativeFlavor(self):
        self.addComponent('foo:runtime', '1', '!readline')
        repos = self.openRepository()
        results = repos.getTroveLeavesByLabel({'foo:runtime':
                            { self.cfg.buildLabel : None }})
        assert(results)
        results = repos.getTroveLeavesByLabel({'foo:runtime':
                            { self.cfg.buildLabel : [deps.parseFlavor('')] }})
        assert(results)

    def testCreateChangeSetWithMissingTroves(self):
        # CNY-948 - createChangeSet acts a little differently
        trv = self.addCollection('foo', '1', [':run'])
        repos = self.openRepository()

        # foo:run, which is referenced by this trove, does not exist
        cs = repos.createChangeSet([('foo',
                                     (None, None),
                                     (trv.getVersion(), trv.getFlavor()),
                                     True)],
                                   recurse=True, withFileContents=False)
        for tcs in cs.iterNewTroveList():
            if tcs.getName() == 'foo':
                self.assertTrue(tcs.troveType() == trove.TROVE_TYPE_NORMAL)
            elif tcs.getName() == 'foo:run':
                self.assertTrue(tcs.troveType() == trove.TROVE_TYPE_REMOVED)
                ti = trove.TroveInfo(tcs.troveInfoDiff.freeze())
                self.assertTrue(ti.flags.isMissing())
            else:
                self.fail('unexpected trove changeset')

        repos = self.openRepository(1)
        trv = self.addCollection('group-bar', '/localhost1@a:b/1.1-1-1',
                                 [ ('missing', '/localhost@a:b/1.1-1-1') ],
                                 weakRefList = [])

        self.assertRaises(errors.TroveMissing,
                          repos.createChangeSet,
                          [(trv.getName(),
                            (None, None),
                            (trv.getVersion(), trv.getFlavor()),
                            True)],
                          recurse=True, withFileContents=False)

    def testNonpresentTroves(self):
        # CNY-947
        trv = self.addCollection('foo', '1', [':run'])
        repos = self.openRepository()

        d = repos.getTroveVersionsByLabel({ 'foo:run' :
                              { trv.getVersion().trailingLabel() : None } } )
        assert(not d)

        d = repos.getTroveVersionsByBranch({ 'foo:run' :
                              { trv.getVersion().branch() : None } } )
        assert(not d)

        d = repos.getTroveLeavesByLabel({ 'foo:run' :
                              { trv.getVersion().trailingLabel() : None } } )
        assert(not d)

        d = repos.getTroveLeavesByBranch({ 'foo:run' :
                              { trv.getVersion().branch() : None } } )
        assert(not d)

        d = repos.getTroveVersionList('localhost', { 'foo:run' : None })
        assert(not d)

        d = repos.getAllTroveLeaves('localhost', { 'foo:run' : None })
        assert(not d)

        d = repos.getAllTroveFlavors( { 'foo:run' : [ trv.getVersion() ] } )
        assert(not d)

        self.assertRaises(errors.TroveMissing, repos.getTroveLatestVersion,
                          'foo:run', trv.getVersion().branch())

        assert(not repos.hasTrove('foo:run', trv.getVersion(), trv.getFlavor()))
        self.assertRaises(errors.TroveMissing, repos.getTrove,
                          'foo:run', trv.getVersion(), trv.getFlavor())

    def testGetAllTroveLeavesMultipleServers(self):
        # CNY-1771
        repos = self.openRepository(serverName = [ 'localhost', 'localhost1' ])
        self.addComponent('foo:runtime', '/localhost@rpl:linux/1-1-1')
        self.addComponent('foo:runtime', '/localhost1@rpl:linux/1-1-1')
        self.addComponent('bar:runtime', '/localhost1@rpl:linux/1-1-1')

        q = repos.getAllTroveLeaves('localhost', {})
        assert(q.keys() == [ 'foo:runtime' ] )
        assert(len(q['foo:runtime']) == 1)
        self.stopRepository(0)

    def testPrepareCallbackException(self):
        # CNY-1271

        class Callback(updatecmd.UpdateCallback):
            def preparingChangeSet(self):
                e = Exception("Except me")
                e.errorIsUncatchable = True
                raise e

        self.addComponent('testcase:runtime', '1')
        self.addCollection('testcase', '1', [':runtime'])

        client = conaryclient.ConaryClient(self.cfg)
        applyList = conaryclient.cmdline.parseChangeList('testcase')
        client.setUpdateCallback(Callback(self.cfg))
        try:
            (updJob, suggMap) = client.updateChangeSet(applyList)
        except Exception, e:
            self.assertEqual(str(e), "Except me")
        else:
            self.fail("Exception not raised")

    def testPrepareCallbackException2(self):
        # CNY-1271
        # Same as above, but the exception is not marked as catchable. The
        # code should fail with a RepositoryError

        self.addComponent('testcase:runtime', '1')
        self.addComponent('testcase:runtime', '2')


        class Callback(callbacks.UpdateCallback):
            def __init__(self, cfg):
                # Intentionally don't call our __init__, to make sure we don't
                # fail. Inherit from the lesser callbacks.UpdateCallback which
                # does not do locking
                self.abortEvent = None
                pass
            def preparingChangeSet(self):
                raise Exception("Except me")

        client = conaryclient.ConaryClient(self.cfg)
        applyList = conaryclient.cmdline.parseChangeList('testcase:runtime=1')
        client.setUpdateCallback(Callback(self.cfg))
        self.logFilter.add()
        (updJob, suggMap) = self.discardOutput(
            client.updateChangeSet, applyList)
        self.logFilter.regexpCompare('warning: Unhandled exception occurred when invoking callback:\n.*netclienttest\.py:[0-9]+\n Exception: Except me')

        # CNY-2304 - by default we don't stop the update. Make one that's
        # stoppable

        class Callback(updatecmd.UpdateCallback):
            def preparingChangeSet(self):
                raise gerrors.CancelOperationException("Except me")

        applyList = conaryclient.cmdline.parseChangeList('testcase:runtime=2')
        client.setUpdateCallback(Callback(self.cfg))
        self.logFilter.add()
        try:
            (updJob, suggMap) = self.discardOutput(
                client.updateChangeSet, applyList)
        except errors.RepositoryError:
            pass
        self.logFilter.regexpCompare('warning: Unhandled exception occurred when invoking callback:\n.*netclienttest\.py:[0-9]+\n CancelOperationException: Except me')

        self.assertRaises(errors.RepositoryError,
            client.applyUpdate, updJob)

    def testChangeSetConversionError(self):
        # Test returning non-200 or 403 errors
        def errorHttpPutFile(*args, **kwargs):
            return 404, "File not found"

        cs = changeset.ChangeSet()
        trv = trove.Trove('test:runtime',
                versions.ThawVersion('/localhost@foo:test/1.0:1.0-1-1'),
                deps.parseFlavor(''), None)
        trv.computeDigests()
        trvCs = trv.diff(None, absolute = True)[0]
        cs.newTrove(trvCs)

        repos = self.openRepository()


        old = netclient.httpPutFile
        try:
            netclient.httpPutFile = errorHttpPutFile

            self.assertRaises(errors.CommitError, repos.commitChangeSet, cs)
        finally:
            netclient.httpPutFile = old

    def testReferences(self):
        if sqlite3.sqlite_version_info() < (3,7,0):
            raise testhelp.SkipTestException("buggy sqlite; use embedded sqlite")
        repos = self.openRepository()
        # remove anonymous access
        repos.deleteUserByName("localhost@rpl:linux", "anonymous")
        self.addUserAndRole(repos, "localhost@rpl:linux", "alluser", "allpw")
        repos.addAcl("localhost@rpl:linux", "alluser", ".*", "ALL")
        allrepo = self.getRepositoryClient(user = "alluser", password = "allpw")
        # a limited user
        self.addUserAndRole(repos, "localhost@rpl:qa", "qauser", "qapw")
        repos.addAcl("localhost@rpl:qa", "qauser", "beta.*", "localhost@rpl:qa")
        qarepo = self.getRepositoryClient(user="qauser", password="qapw")

        # set up the test case
        verlist = [
            "/localhost@rpl:linux//devel/1.0-1-1",
            "/localhost@rpl:linux//qa/1.0-1-1",
            "/localhost@rpl:linux//prod/1.0-1-1",
            "/localhost@rpl:linux//devel/2.0-1-1",
            "/localhost@rpl:linux//qa/2.0-1-1",
            "/localhost@rpl:linux//prod/2.0-1-1",
            ]
        for i, v in enumerate(verlist):
            for n1 in ["alpha", "beta"]:
                for n2 in [":runtime", ":data"]:
                    self.addComponent(n1+n2, v)
                self.addCollection(n1, v, [":runtime", ":data"])
            self.addCollection("alpha%d" % i, v, [("alpha", v), ("alpha:runtime",v)])
            self.addCollection("beta%d" % i, v, [("beta", v), ("beta:runtime",v)], weakRefList = [("alpha:data",v)])
        # getTroveReferences
        noF = deps.Flavor()
        for i, vStr in enumerate(verlist):
            v = versions.VersionFromString(vStr)
            # test alluser
            ret = allrepo.getTroveReferences("localhost", [("alpha", v, noF)])
            self.assertEqual(ret, [[("alpha%d" %i, v, noF)]] )
            ret = allrepo.getTroveReferences("localhost", [("alpha:runtime", v, noF)])
            self.assertEqual(set(ret[0]), set([("alpha", v, noF), ("alpha%d" %i, v, noF)]))
            ret = allrepo.getTroveReferences("localhost", [("alpha:data", v, noF)])
            self.assertEqual(set(ret[0]),
                                 set([("alpha",v,noF),("alpha%d"%i,v,noF),("beta%d"%i,v,noF)]) )

            # test qauser
            ret = qarepo.getTroveReferences("localhost", [("beta:runtime",v,noF), ("beta:data",v,noF)])
            if v.branch().label().asString() == "localhost@rpl:qa":
                self.assertEqual(set(ret[0]),
                                     set([("beta",v,noF), ("beta%d"%i,v,noF)]))
                self.assertEqual(ret[1],[("beta", v, noF)])
            else:
                self.assertEqual(ret, [[],[]])
        # getTroveDescendants
        ret = allrepo.getTroveDescendants("localhost", [
            ("alpha", versions.VersionFromString("/localhost@rpl:linux//prod"), noF)])
        self.assertEqual(ret, [[]])
        ret = allrepo.getTroveDescendants("localhost", [
            ("alpha", versions.VersionFromString("/localhost@rpl:linux"), noF)])
        self.assertEqual(set(ret[0]),
                             set([(versions.VersionFromString(x), noF) for x in verlist]) )
        # limited user
        qaset = set([
            (versions.VersionFromString("/localhost@rpl:linux//qa/1.0-1-1"), noF),
            (versions.VersionFromString("/localhost@rpl:linux//qa/2.0-1-1"), noF),
            ])
        ret = qarepo.getTroveDescendants("localhost", [
            ("alpha", versions.VersionFromString("/localhost@rpl:linux"), noF),
            ("beta", versions.VersionFromString("/localhost@rpl:linux"), noF)])
        self.assertEqual(ret[0], [])
        self.assertEqual(set(ret[1]), qaset)
        # test deep branching
        branch = "/localhost@rpl:linux"
        retset = set()
        for b in ["devel", "qa", "prod"]:
            branch = branch + "//" + b
            verStr = branch+"/1.0-1-1"
            self.addComponent("gamma:runtime", verStr)
            self.addCollection("gamma", verStr, [":runtime"])
            retset.add((versions.VersionFromString(verStr), noF))
        ret = allrepo.getTroveDescendants("localhost", [
            ("gamma", versions.VersionFromString("/localhost@rpl:linux"), noF)])
        self.assertEqual(set(ret[0]), retset)

    def testFlavoredReferences(self):
        repos = self.openRepository()
        # set up a trove flavored two different ways
        v1 = "/localhost@rpl:linux//prod/1.0-1-1"
        v2 = "/localhost@rpl:baz/1.0-1-1"

        f1 = deps.parseFlavor("use: foo")
        f2 = deps.parseFlavor("use: !foo, bar")

        self.addComponent("trove:runtime", v1, f1)
        self.addComponent("trove:runtime", v1, f2)

        self.addCollection("trove", v1, [":runtime"], defaultFlavor = f1)
        self.addCollection("trove", v1, [":runtime"], defaultFlavor = f2)

        # set up a collection
        self.addCollection("group-dist", v2, [("trove", v1, f1)], defaultFlavor = f1)

        # query for references to both flavors; expected result is only one match
        ret = repos.getTroveReferences("localhost", [
            ("trove", versions.VersionFromString(v1), f1),
            ("trove", versions.VersionFromString(v1), f2)])
        # get a match for the first trove, no match for the second
        self.assertEqual(ret, [[('group-dist', versions.VersionFromString(v2), f1)], [] ] )

        # add a new collection for the second flavor
        self.addCollection("group-dist", v2, [("trove", v1, f2)], defaultFlavor = f2)
        # query again for references to both flavors
        ret = repos.getTroveReferences("localhost", [
            ("trove", versions.VersionFromString(v1), f1),
            ("trove", versions.VersionFromString(v1), f2)])
        # this time get results for both
        self.assertEqual(ret, [
            [('group-dist', versions.VersionFromString(v2), f1)],
            [('group-dist', versions.VersionFromString(v2), f2)],
            ])

    def testCommitGroupScriptsOldRepo(self):
        self.addComponent('foo:runtime', '1.0')
        repos = self.openRepository()
        repos.c['localhost'].setProtocolVersion(40)
        try:
            self.addCollection('group-foo', '1.0', [ 'foo:runtime' ],
                               postUpdateScript = 'foo', repos = repos)
            assert(not 'Commit should fail due to old protocol')
        except errors.CommitError, e:
            assert(str(e) == 'The changeset being committed needs a newer '
                             'repository server.')
        except:
            assert(not 'Commit should fail due to old protocol')

    def testRepositoryMapWildcards(self):
        self.servers.stopAllServers()
        repos = self.openRepository(serverName = [ 'localhost', 'localhost1' ])
        trv1 = self.addComponent('foo:runtime', 'localhost@rpl:linux/1')
        trv2 = self.addComponent('foo:runtime', 'localhost1@rpl:linux/1')

        # gross
        urls = list(set(x[1] for x in repos.c.map))
        assert(len(urls) == 1)
        newMap = conarycfg.RepoMap()
        newMap['*'] = urls[0]
        wildcardRepos = self.getRepositoryClient(repositoryMap = newMap)

        assert(wildcardRepos.hasTrove(*trv1.getNameVersionFlavor()))
        assert(wildcardRepos.hasTrove(*trv2.getNameVersionFlavor()))
        self.stopRepository(0)

    def testErrorMessagesMentioningProxy(self):
        # CNY-1313

        # Lame HTTP server to act as a bogus HTTP proxy/Conary proxy
        class RequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
            def log_message(self, *args, **kw):
                pass

            def do_GET(self):
                self.send_response(200)
                response = 'Hello, world!\n'
                self.send_header('Content-type', 'text/unknown')
                self.send_header('Content-Length', len(response))
                self.end_headers()
                self.wfile.write(response)

        httpServer = rephelp.HTTPServerController(RequestHandler)
        sock_utils.tryConnect("localhost", httpServer.port)

        self.cfg.repositoryMap['localhost'] = 'http://localhost/conary/'
        self.cfg.configLine("conaryProxy http://localhost:%d" % httpServer.port)
        repos = conaryclient.ConaryClient(self.cfg).getRepos()
        try:
            repos.c['localhost'].getProtocolVersion()
        except errors.OpenError, e:
            self.assertEqual(str(e),
                    "Error opening http://test:<PASSWD>@localhost/conary/ via "
                    "conary proxy localhost:%d: 501 Unsupported method ('POST')"
                    % httpServer.port)
        else:
            self.fail("Should have failed")
        self.cfg.resetToDefault('conaryProxy')

    def testClientLog(self):
        p = self.workDir + '/client.log'
        os.environ['CONARY_CLIENT_LOG'] = p
        repos = self.openRepository()
        del os.environ['CONARY_CLIENT_LOG']

        log = calllog.ClientCallLogger(p)
        methods = [ x.methodName for x in log ]
        self.assertEqual(methods, ['checkVersion', 'addNewAsciiPGPKey'])

    def testRetryChangesetDownload(self):
        self.cfg.downloadRetryTrim = 16
        repos = self.openRepository()
        trv = self.addComponent('foo:runtime', '1')
        job = [trv.getNameVersionFlavor().asJob()]
        host = trv.getVersion().getHost()

        clean = os.path.join(self.workDir, 'clean.ccs')
        repos.createChangeSetFile(job, clean)
        sp = repos.c[host]
        orig_cs = sp.getChangeSet
        did_truncate = [False]
        def getChangeSet(*args, **kwargs):
            rc = orig_cs(*args, **kwargs)
            fobj = rc[0]
            spool = tempfile.TemporaryFile(dir=self.workDir)
            size = util.copyfileobj(fobj, spool)
            spool.seek(size // 2)
            spool.write('garbage')
            spool.truncate()
            spool.seek(0)
            # Patch this function back out so the second try works
            sp.getChangeSet = orig_cs
            did_truncate[0] = True
            return [spool] + rc[1:]

        sp.getChangeSet = getChangeSet
        did_truncate = [False]
        self.cfg.downloadAttempts = 1
        self.assertRaises(errors.TruncatedResponseError,
                repos.createChangeSet, job)

        sp.getChangeSet = getChangeSet
        did_truncate = [False]
        self.cfg.downloadAttempts = 2
        retry = os.path.join(self.workDir, 'retry.ccs')
        repos.createChangeSetFile(job, retry)
        assert did_truncate[0]
        self.assertEqual(open(clean).read(), open(retry).read())


class ServerProxyTest(rephelp.RepositoryHelper):
    def testBadProtocol(self):
        # CNY-932
        port = self.cfg.repositoryMap['localhost'] = 'httsp://localhost/'
        repos = conaryclient.ConaryClient(self.cfg).getRepos()
        try:
            repos.troveNamesOnServer('localhost')
        except errors.OpenError, e:
            self.assertEqual(str(e),
                                 'Error occurred opening repository '
                                 'httsp://test:<PASSWD>@localhost/: '
                                 "ParameterError: Unknown URL scheme 'httsp'")
        else:
            self.fail('expected exception not raised')

    def testProtocolVersionOverride(self):
        repos = self.openRepository()
        versions = repos.c['localhost'].checkVersion()
        # Grab a version older than the one supported by the server
        override = versions[0] - 1
        repos.c['localhost'].setProtocolVersion(override)
        try:
            repos.c['localhost'].checkVersion()
        except errors.InvalidClientVersion, e:
            assert e.args[0].startswith('Invalid client version %d' % override)
        else:
            self.fail("InvalidClientVersion not raised")

    def testOverrideChangeSetVersion(self):
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        c = conaryclient.ConaryClient(self.cfg)
        dest = os.path.join(self.workDir, "test.ccs")
        ver = versions.VersionFromString('/%s/1-1-1' % str(self.defLabel))
        csVersion = changeset.getNativeChangesetVersion(42)
        c.repos.createChangeSetFile([('foo', (None, None),
                                     (ver, deps.parseFlavor('')), True)],
                                     dest, changesetVersion = csVersion)
        fcont = filecontainer.FileContainer(util.ExtendedFile(
            dest, buffering = False))
        self.assertEqual(fcont.version, csVersion)

    def testOldClient(self):
        t = self.addComponent('foo:run', '1')
        c = conaryclient.ConaryClient(self.cfg)
        c.repos.getTrove(t.getName(), t.getVersion(), t.getFlavor())
        c.repos.c['localhost'].setProtocolVersion(42)
        self.mock(filecontainer, 'READABLE_VERSIONS',
                  filecontainer.READABLE_VERSIONS[1:])
        self.mock(filecontainer, 'FILE_CONTAINER_VERSION_LATEST',
                  max(filecontainer.READABLE_VERSIONS))
        dest = os.path.join(self.workDir, "test.ccs")
        c.repos.createChangeSetFile([(t.getName(), (None, None),
                                     (t.getVersion(), t.getFlavor()), True)],
                                     dest)
        fcont = filecontainer.FileContainer(util.ExtendedFile(
            dest, buffering = False))
        csVersion = changeset.getNativeChangesetVersion(42)
        self.assertEqual(fcont.version, csVersion)

    def testMultitenantServers(self):
        self.stopRepository(0)
        repos = self.openRepository(0, serverName = [ 'localhost', 'localhost1' ])

        trv1 = self.addComponent('foo:runtime', 'localhost@rpl:linux/1')
        trv2 = self.addComponent('foo:runtime', 'localhost1@rpl:linux/1')

        repos.createChangeSet([ ('foo:runtime',
                    (trv1.getVersion(), trv1.getFlavor()),
                    (trv2.getVersion(), trv2.getFlavor()), False) ] )

        # this makes sure only one ServerProxy
        # NOTE - this is no longer true - in fact we send different
        # servername headers depending on who we are talking to
        # so there are different objects.
        assert(repos.c['localhost'] is not repos.c['localhost1'])
        self.servers.stopAllServers()

    def testJobSizes(self):
        def _check(job, size):
            fn = self.workDir + '/foo.ccs'
            cs1 = repos.createChangeSetFile(job, fn)
            assert(os.stat(fn).st_size == size)

        first = self.addComponent('first:run', '1')
        firstJob = ('first:run', (None, None),
                    (first.getVersion(), first.getFlavor() ), True)

        second = self.addComponent('second:run', '1',
                                   fileContents = [ '/some/file/name' ] )
        secondJob = ('second:run', (None, None),
                     (second.getVersion(), second.getFlavor() ), True)

        removeJob = ('second:run', (second.getVersion(), second.getFlavor() ),
                     (None, None), True)

        repos = self.openRepository()

        # CNY-3876: use old version to determine repo name
        assert(repos.getChangeSetSize([ removeJob ])[0] == 0)

        sizes = repos.getChangeSetSize([ firstJob, removeJob, secondJob ])
        _check([ firstJob ], sizes[0])
        assert(sizes[1] == 0)
        _check([ secondJob ], sizes[2])

        # try this for a the pre-infoOnly protocol
        repos.c['localhost'].setProtocolVersion(50)
        otherSizes = repos.getChangeSetSize([ firstJob, removeJob, secondJob ])
        assert(sizes == otherSizes)

    def testServerProxyMethods(self):
        sp = netclient.ServerProxy('http://localhost:7', 'localhost', None,
                None, None)
        self.assertFalse(hasattr(sp, '__asdf'))

    def testPathIdPermissions(self):
        self.addComponent('foo:source', '1.0-1')
        self.addComponent('foo:source', '2.0-1')

        foo1 = self.addComponent('foo:run', '1.0-1', sourceName = 'foo:source',
                  fileContents = [ ( '/1', rephelp.RegularFile(pathId = "1") )
                                 ] )
        foo2 = self.addComponent('foo:run', '2.0-1', sourceName = 'foo:source',
                  fileContents = [ ( '/2', rephelp.RegularFile(pathId = "2") )
                                 ] )

        branch = foo1.getVersion().branch()

        repos = self.openRepository()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')
        repos.addUser(self.cfg.buildLabel, 'other', 'pass')
        repos.addRole(self.cfg.buildLabel, 'role')
        repos.updateRoleMembers(self.cfg.buildLabel, 'role', [ 'other' ])
        otherClient = self.getRepositoryClient(user = 'other',
                                               password = 'pass')

        ids = repos.getPackageBranchPathIds('foo:source', branch)
        assert(ids['/1'][0] == '1000000000000000')
        assert(ids['/2'][0] == '2000000000000000')
        assert(len(ids) == 2)

        ids = otherClient.getPackageBranchPathIds('foo:source', branch)
        assert(not ids)

        repos.addTroveAccess('role', [ foo1.getNameVersionFlavor() ] )
        ids = otherClient.getPackageBranchPathIds('foo:source', branch)
        assert(ids['/1'][0] == '1000000000000000')
        assert(len(ids) == 1)

        repos.addTroveAccess('role', [ foo2.getNameVersionFlavor() ] )
        ids = otherClient.getPackageBranchPathIds('foo:source', branch)
        assert(ids['/1'][0] == '1000000000000000')
        assert(ids['/2'][0] == '2000000000000000')
        assert(len(ids) == 2)

    def testHttpProxyHttpsServer(self):
        # CNY-2067
        if not conaryserver.SSL:
            raise testhelp.SkipTestException("m2crypto not installed")
        self.stopRepository(1)
        self.openRepository(1, useSSL=True)
        try:
            proxy = self.getConaryProxy()
            proxy.start()
            try:
                proxy.addToConfig(self.cfg)
                client = conaryclient.ConaryClient(self.cfg)
                repos = client.getRepos()
                repos.c['localhost1'].checkVersion()
            finally:
                proxy.stop()
        finally:
            self.stopRepository(1)

    def testUnderUnderMethodName(self):
        # CNY-2289
        repos = self.openRepository()
        self.assertRaises(AttributeError,
                lambda: repos.c['localhost'].__foo.bar)
        self.assertRaises(AttributeError,
                lambda: repos.c['localhost'].foo.__bar)
        self.assertRaises(AttributeError,
                lambda: repos.c['localhost'].getTroveLeavesByLabel.__safe_str__)

    def testPermissionFallbackCode(self):
        # test to make sure that old calls are used for manipulating
        # permissions in old repositories
        m = (('addRole', 'addAccessGroup'),
             ('addEntitlementClass', 'addEntitlementGroup'),
             ('addEntitlementClassOwner', 'addEntitlementOwnerAcl'),
             ('addEntitlementKeys', 'addEntitlements'),
             ('deleteRole', 'deleteAccessGroup'),
             ('deleteEntitlementClass', 'deleteEntitlementGroup'),
             ('deleteEntitlementClassOwner', 'deleteEntitlementOwnerAcl'),
             ('deleteEntitlementKeys', 'deleteEntitlements'),
             ('getEntitlementClassesRoles', 'getEntitlementClassAccessGroup'),
             ('getRoles', 'getUserGroups'),
             ('listRoles', 'listAccessGroups'),
             ('listEntitlementClasses', 'listEntitlementGroups'),
             ('listEntitlementKeys', 'listEntitlements'),
             ('setEntitlementClassesRoles', 'setEntitlementClassAccessGroup'),
             ('setRoleCanMirror', 'setUserGroupCanMirror'),
             ('setRoleIsAdmin', 'setUserGroupIsAdmin'),
             ('updateRoleMembers', 'updateAccessGroupMembers'))

        class MockServerProxy:
            def __init__(self):
                self.methodCalled = None

            def getProtocolVersion(self):
                return 59

            def __getattr__(self, attr):
                self.methodCalled = attr
                def f(*args, **kw):
                    return []
                return f

        empty = conarycfg.ConaryConfiguration(False)
        repos = netclient.NetworkRepositoryClient(cfg=empty)
        server = MockServerProxy()
        repos.c = { 'localhost': server }
        for method, serverMethod in m:
            f = getattr(repos, method)
            # build up an argument list, starting with 'localhost'
            # (as these methods take a server name for the first argument)
            # then filling in the rest with empty lists.  Knock 2 off the
            # argument count for self and the server name.
            args = ('localhost',) + ([],) * (f.func_code.co_argcount - 2)
            f(*args)
            # make sure that the old method would have been called
            self.assertEqual(server.methodCalled, serverMethod)

    def testFakeProtocolVersion(self):
        '''
        Fake a protocolVersion in a server call
        '''
        # Previous behavior would use protocolVersion but pass it on,
        # causing errors on the repository.
        repos = self.openRepository()
        repos.c['localhost'].checkVersion(protocolVersion=50)

    def testUpdateUsesCache(self):
        ipcache = httputils.IPCache
        ipcache.clear()
        repos = self.openRepository()
        if repos.c.proxyMap:
            # proxies are all localhost so this test fails to
            # execute.
            return
        repos = self.openRepository(1)
        self.addComponent('foo:run')
        self.addComponent('bar:run=localhost1@rpl:1', filePrimer=1)
        oldCacheHostLookups = repos._cacheHostLookups
        def checkCacheHostLookups(self, hosts):
            ipcache.clear()
            self.assertEqual(set(hosts), set(['localhost', 'localhost1']))
            rv = oldCacheHostLookups(hosts)
            self.assertEqual(ipcache._cache._map.keys(), [] )
            return rv
        self.mock(netclient.NetworkRepositoryClient,
                  '_cacheHostLookups', checkCacheHostLookups)

    def testUpdateUsesCache2(self):
        ipcache = httputils.IPCache
        ipcache.clear()
        oldLocalHosts = httputils.LocalHosts
        try:
            httputils.LocalHosts = set()
            repos = self.openRepository()
            if repos.c.proxyMap:
                # proxies are all localhost so this test fails to
                # execute.
                return
            repos = self.openRepository(1)
            self.addComponent('foo:run')
            self.addComponent('bar:run=localhost1@rpl:1', filePrimer=1)
            self.assertEqual(ipcache._cache._map.keys(), [ 'localhost' ] )
            oldCacheHostLookups = repos._cacheHostLookups
            def checkCacheHostLookups(self, hosts):
                ipcache.clear()
                self.assertEqual(set(hosts), set(['localhost', 'localhost1']))
                rv = oldCacheHostLookups(hosts)
                self.assertEqual(ipcache._cache._map.keys(), [ 'localhost' ] )
                return rv
            self.mock(netclient.NetworkRepositoryClient,
                      '_cacheHostLookups', checkCacheHostLookups)
            self.updatePkg(['foo:run', 'bar:run=localhost1@rpl:1'])
        finally:
            httputils.LocalHosts = oldLocalHosts

    def testGetChangeSetFingerprints(self):
        self.openRepository(0)
        repos = self.openRepository(1)
        v1 = versions.VersionFromString('/localhost@rpl:1/2.0-1')
        v2 = versions.VersionFromString('/localhost@rpl:1/3.0-1')
        v3 = versions.VersionFromString('/localhost1@rpl:1/2.0-1')
        v4 = versions.VersionFromString('/localhost1@rpl:1/3.0-1')
        f = deps.parseFlavor('')
        self.addComponent('test:source', str(v1))
        self.addComponent('test:source', str(v2))
        self.addComponent('test:source', str(v3))
        self.addComponent('test:source', str(v4))
        jobList = [ ('test:source', (None, None), (v1, f), False),
                    ('test:source', (None, None), (v2, f), False),
                    ('test:source', (None, None), (v3, f), False),
                    ('test:source', (None, None), (v4, f), False) ]
        expectedList = [
            (dict(recurse=False,
                  withFiles=False,
                  withFileContents=False,
                  excludeAutoSource=False,
                  mirrorMode=False),
               ['d8d8dfbcd9be91b4a55538b04ee5127a9c05b572',
                'dd242d0b8b377591cae61dc84a86a8bf86490a74',
                '57b6223c9a6651cede572e2fef0a9aaa903cac8f',
                '02c5606e76f2cd8208f466cfe48552cc118bc35e']),
            (dict(recurse=True,
                  withFiles=False,
                  withFileContents=False,
                  excludeAutoSource=False,
                  mirrorMode=False),
               ['6371f655c66156e9cd13501fb0abb01033ff9a78',
                '8a953401c12803f07ca9c929d3935f51f2e231cf',
                'd8314baf822583611cbfed4518435fb703c0d64c',
                'c1be835956fc71f9e8c4c731591e0da3a6498854']),
            (dict(recurse=False,
                  withFiles=True,
                  withFileContents=False,
                  excludeAutoSource=False,
                  mirrorMode=False),
               ['ce4a199bba0ad370c406d2fc53b3afcd67a9fc44',
                '1e9c44a6edc7447bec2711b60a5df63f3315aa8e',
                '4a65cccf71e57f9ca2e6f5152e621d595439e6f3',
                '06f9f8088007f3f8738e0d6ae9ec573bc6682c12']),
            (dict(recurse=False,
                  withFiles=False,
                  withFileContents=True,
                  excludeAutoSource=False,
                  mirrorMode=False),
               ['d1e16cf48157bd0a2c17005a3fd16936980e01fe',
                '4e329de181ef0e15ff858f1ae79d89f815d37ef0',
                '29c7a7f0fb7d514dc562a2df2e00927cf6ab1bcd',
                '0845814fd5192a5f38818941c27841ff2d745a85']),
            (dict(recurse=False,
                  withFiles=False,
                  withFileContents=False,
                  excludeAutoSource=True,
                  mirrorMode=False),
               ['47796599dae75026414ea08767ddaf4aaa52bdb3',
                '36ef56955559b79e2abf9b7abddf9bcfd16070c0',
                'af7899f31fab9aea4a912772f9e7059165131d8e',
                'e81ba99f8be5da4481a007196603781f4171bde4']),
            (dict(recurse=False,
                  withFiles=False,
                  withFileContents=False,
                  excludeAutoSource=False,
                  mirrorMode=True),
               ['2bbbd9b59cec1f6324d1bc1448cb73d1f23e1778',
                '1ff3dc1e60aa10c4ec50314b1c90e475bc298055',
                '30ae8f54702367b937e886701011cdd1035d93d4',
                '030b675cb133ccb1eefbdc8245f2ce2c4394ce94']),
            ]
        failed = False
        for i, (kwargs, expected) in enumerate(expectedList):
            csf = repos.getChangeSetFingerprints(jobList,
                                                 **kwargs)
            if csf != expected:
                if not failed:
                    print
                print "Job", i
                print "  got:", csf
                print "  expected: ", expected
                failed = True
            #self.assertEqual(csf, expected)
        if failed:
            self.fail("Fingerprints do not match")

    def testGetFileContentsFromTrove(self):
        trv = self.addComponent('foo:run=1', [('/foo', 'contents!'),
                                              ('/bar', 'other')])
        repos = self.openRepository()
        n,v,f = trv.getNameVersionFlavor()
        contents = repos.getFileContentsFromTrove(n,v,f,
                                                  ['/foo', '/bar'])
        assert(contents[0].get().read() == 'contents!')
        assert(contents[1].get().read() == 'other')
        self.assertRaises(errors.PathsNotFound,
                          repos.getFileContentsFromTrove,
                          n,v,f,
                          ['/foo2'])
        repos.c['localhost'].getProtocolVersion = lambda: 66
        contents = repos.getFileContentsFromTrove(n,v,f,
                                                  ['/foo', '/bar'])
        assert(contents[0].get().read() == 'contents!')
        assert(contents[1].get().read() == 'other')



    def testGetLabelsForHost(self):
        try:
            self.openRepository(0, serverName=['localhost', 'localhost1'])
            self.addComponent('foo:run=1')
            self.addComponent('foo:run=@rpl:branch')
            self.addComponent('foo:run=localhost1@rpl:2')
            self.addComponent(
                    'foo:run=/localhost@rpl:1//localhost1@rpl:branch/1-1-1')
            labels = self.openRepository().getLabelsForHost('localhost')
            assert(sorted([str(x) for x in labels]) ==
                   ['localhost@rpl:branch', 'localhost@rpl:linux'])
            labels = self.openRepository().getLabelsForHost('localhost1')
            assert(sorted([str(x) for x in labels]) ==
                   ['localhost1@rpl:2', 'localhost1@rpl:branch'])
        finally:
            self.resetRepository(0)
            self.stopRepository(0)
