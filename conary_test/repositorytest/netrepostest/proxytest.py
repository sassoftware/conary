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


from testrunner import testcase, testhelp
from testutils import mock, sock_utils
import copy
import itertools
import os
import tempfile
import urllib

from conary_test import rephelp
import SimpleHTTPServer

from conary import conaryclient
from conary import rpmhelper
from conary import trove
from conary.files import ThawFile
from conary.lib import util
from conary.repository import errors
from conary.repository import netclient
from conary.repository import xmlshims
from conary.repository.netrepos import proxy as netreposproxy
from conary.repository.netrepos import netserver
from conary.server import server as cnyserver


class IndexerRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    logFile = None
    archivePath = None
    pathPrefix = '/toplevel'
    tmpDir = None
    def log_message(self, *args, **kwargs):
        file(self.logFile, "a").write("%s\n" % self.path)

    def do_GET(self):
        if not self.path.startswith(self.pathPrefix):
            # We only accept requests starting with the prefix
            return self.notFound()
        path = self.path[len(self.pathPrefix):]
        # Split by /
        arr = path.split('/')
        if len(arr) < 3:
            return self.notFound()
        # First one is package
        pkg = arr[1]
        sha1 = arr[2]
        fpath = os.path.join(self.archivePath, pkg)
        if not os.path.exists(fpath):
            return self.notFound()
        if len(arr) == 3:
            # Capsule download
            contentType = "application/x-rpm"
            fileSize = os.stat(fpath).st_size
            self.send_response(200)
            self.send_header('Content-Type', contentType)
            self.send_header('Content-Length', fileSize)
            self.end_headers()
            util.copyStream(file(fpath), self.wfile)
            return
        if len(arr) < 5:
            return self.notFound()
        # Need to extract file from the rpm
        # Because of differences in how rpm stores stuff in cpio archives, we
        # need to specify several ways to handle that
        payload = rpmhelper.UncompressedRpmPayload(file(fpath))
        fileName = urllib.unquote(urllib.unquote(arr[3]))
        util.mkdirChain(self.tmpDir)
        fileList = rpmhelper.extractFilesFromCpio(payload, [ fileName ],
            tmpDir = self.tmpDir)

        fileObj = fileList[0]
        if fileObj is None:
            header = rpmhelper.RpmHeader(file(fpath))
            # Fetch the file by path
            paths = list(header.getFilesByPath([fileName]))
            if not paths or not paths[0].isEmpty():
                return self.notFound()
            fileSize = 0
            fileObj = file(os.devnull)
        else:
            fileSize = os.fstat(fileObj.fileno()).st_size

        contentType = "application/octet-stream"
        self.send_response(200)
        self.send_header('Content-Type', contentType)
        self.send_header('Content-Length', fileSize)
        self.end_headers()
        util.copyStream(fileObj, self.wfile)
        return

    def notFound(self):
        self.send_response(404)
        self.end_headers()

def runproxy(**kwargs):

    def deco(fn):

        def dorunproxy(obj, *args, **kwargs):

            def runOnce(useApache):
                if 'withCapsuleContentServer' in dorunproxy._proxyKwArgs:
                    class RequestHandler(IndexerRequestHandler):
                        logFile = os.path.join(obj.workDir, "capsuleContentServer.log")
                        archivePath = rephelp.resources.get_archive()
                        tmpDir = os.path.join(obj.workDir, "tmpdir-cpio")
                    srv = rephelp.HTTPServerController(RequestHandler)
                    obj.capsuleContentServer = srv
                proxy = obj.getConaryProxy(useApache = useApache,
                                              **dorunproxy._proxyKwArgs)

                obj.stopRepository(1)
                obj.openRepository(1, useSSL = True, forceSSL = True)

                cfg = copy.deepcopy(obj.cfg)
                cfg.configLine('conaryproxy http http://localhost:%s' % proxy.port)
                cfg.configLine('conaryproxy https http://localhost:%s' % proxy.port)
                client = conaryclient.ConaryClient(cfg)
                repos = client.getRepos()

                proxy.start()

                try:
                    sock_utils.tryConnect("127.0.0.1", proxy.port)
                    fn(obj, repos, *args, **kwargs)
                finally:
                    proxy.stop()
                    server = obj.servers.getServer(1)
                    if server is not None:
                        server.reset()
                        obj.stopRepository(1)

            if 'CONARY_PROXY' in os.environ:
                raise testhelp.SkipTestException("testInjectedEntitlements doesn't run with a proxy already running")


            runOnce(True)
            runOnce(False)

        dorunproxy.func_name = fn.func_name
        dorunproxy._proxyKwArgs = kwargs

        return dorunproxy

    return deco

class ProxyUnitTest(testcase.TestCaseWithWorkDir):
    def testGetChangeSet(self):
        # Now mock ChangesetCache, to log things
        origChangesetCache = netreposproxy.ChangesetCache
        lockLogFile = os.path.join(self.workDir, "locks.log")
        class MockChangesetCache(origChangesetCache):
            llf = file(lockLogFile, "a")
            def get(slf, key, shouldLock = True):
                csPath = origChangesetCache.hashKey(slf, key)
                ret = origChangesetCache.get(slf, key, shouldLock=shouldLock)
                if shouldLock and ret is None:
                    slf.llf.write("Lock acquired for %s\n" % csPath)
                    self.assertTrue(os.path.exists(csPath + '.lck'))
                return ret

            def set(slf, key, value):
                csPath = origChangesetCache.hashKey(slf, key)
                if csPath in slf.locksMap:
                    slf.llf.write("Releasing lock for %s\n" % csPath)
                return origChangesetCache.set(slf, key, value)

            def resetLocks(slf):
                for csPath in sorted(slf.locksMap):
                    slf.llf.write("Resetting unused lock for %s\n" % csPath)
                return origChangesetCache.resetLocks(slf)

        self.mock(netreposproxy, 'ChangesetCache', MockChangesetCache)


        cfg = netserver.ServerConfig()
        cfg.changesetCacheDir = os.path.join(self.workDir, "changesetCache")
        cfg.proxyContentsDir = os.path.join(self.workDir, "proxyContents")
        prs = netreposproxy.ProxyRepositoryServer(cfg, "/someUrl")
        rawUrl = '/blah'
        headers = {'X-Conary-Proxy-Host' : 'repos.example.com'}
        prs.setBaseUrlOverride(rawUrl, headers, isSecure = True)
        # callWrapper normally sets this, but nothing here invokes it
        prs._serverName = 'repos.example.com'

        caller = mock.mockClass(netreposproxy.ProxyCaller)()
        caller._getBasicUrl._mock.setDefaultReturn('http://blah')
        caller.checkVersion._mock.setDefaultReturn([51, 52, 53])
        # Make sure we present the fingerprints in non-sorted order, we need
        # to verify we sort them
        fingerprints = ['aac3aac3', 'aaa1aaa1', 'aab2aab2']
        caller.getChangeSetFingerprints._mock.setDefaultReturn(fingerprints)
        csSizes = [ 12, 13, 14 ]
        allInfo = [
            (str(x), 'trovesNeeded%d' % i, 'filesNeeded%d' % i,
                'removedTroves%d' % i, str(x))
            for i, x in enumerate(csSizes) ]
        csFileObj = file(os.path.join(self.workDir, "changeset"), "w+")
        magic = netreposproxy.filecontainer.FILE_CONTAINER_MAGIC
        fver = netreposproxy.filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX
        fver = netreposproxy.filecontainer.struct.pack("!I", fver)
        for i, csSize in enumerate(csSizes):
            csFileObj.write(magic)
            csFileObj.write(fver)
            rest = csSize - len(magic) - len(fver)
            csFileObj.write((chr(ord('a') + i) * rest))
        csFileObj.seek(0)

        changeSetList = [ (x, (None, None), (None, None), False) for x in
                            ['a', 'b', 'c'] ]

        caller.getChangeSet._mock.appendReturn(
            ('http://repos.example.com/my-changeset-url', allInfo),
            53, changeSetList, False, True, False, True, 2007022001,
            False, False)

        urlOpener = mock.MockObject()
        uo = mock.MockObject()
        self.mock(netreposproxy.transport, 'ConaryURLOpener', urlOpener)
        urlOpener._mock.setDefaultReturn(uo)
        uo.open._mock.appendReturn(
            csFileObj,
            'http://repos.example.com/my-changeset-url',
            forceProxy=caller._lastProxy,
            headers=[('X-Conary-Servername', 'repos.example.com')])

        authToken = (None, None, [])
        clientVersion = 51

        prs.getChangeSet(caller, authToken, clientVersion, changeSetList,
            recurse = False, withFiles = True, withFileContents = False,
            excludeAutoSource = True)

        MockChangesetCache.llf.close()
        f = file(lockLogFile)
        contents = [ x.strip() for x in f ]
        sortedFP = sorted(fingerprints)

        logEntries1 = contents[:len(fingerprints)]
        self.assertEqual(logEntries1,
            [ 'Lock acquired for %s/%s/%s-2007022001' %
                (cfg.changesetCacheDir, fp[:2], fp[2:])
              for fp in sortedFP ])
        logEntries2 = contents[len(fingerprints):2 * len(fingerprints)]
        self.assertEqual(logEntries2,
            [ 'Releasing lock for %s/%s/%s-2007022001' %
                (cfg.changesetCacheDir, fp[:2], fp[2:])
              for fp in fingerprints ])
        # We're not releasing locks we didn't close
        self.assertEqual(len(contents), 2 * len(fingerprints))

class ProxyTest(rephelp.RepositoryHelper):
    def tearDown(self):
        rephelp.RepositoryHelper.tearDown(self)
        if hasattr(self, 'capsuleContentServer'):
            self.capsuleContentServer.stop()

    def _getRepos(self, proxyRepos):
        hostname = 'localhost1'
        label = self.cfg.buildLabel.asString().replace('localhost', hostname)
        repos = self.openRepository(1, useSSL = True, forceSSL = True)
        return repos, label, hostname


    @runproxy(entitlements = [ ('localhost1', 'ent1234') ])
    def testInjectedEntitlements(self, proxyRepos):
        repos, label, hostname = self._getRepos(proxyRepos)

        repos.addRole(label, 'entgroup')
        repos.addAcl(label, 'entgroup', None, None)
        repos.addEntitlementClass(hostname, 'entgroup', 'entgroup')
        repos.addEntitlementKeys(hostname, 'entgroup', [ 'ent1234' ])
        repos.deleteUserByName(label, 'anonymous')
        repos.deleteUserByName(label, 'test')

        # since both users have been erased from the repository, this can
        # only work if the entitlement got added by the proxy
        proxyRepos.c[hostname].checkVersion()

    def testInjectedEntitlementsNonSSL(self):
        # CNY-3176
        # We are trying to force the situation where an entitlement was
        # injected for a server running on the default http port (80, but the
        # URL not specifying it).
        self.cfg.entitlement.append(('example.com', 'sikrit'))
        authToken = ['test', 'foo', [], '127.0.0.1']
        caller = netreposproxy.ProxyCallFactory.createCaller('unused', 'unused',
            'http://example.com/conary', proxyMap = self.cfg.getProxyMap(),
            authToken = authToken, localAddr = '1.2.3.4',
            protocolString = "protocolString", headers = {}, cfg = self.cfg,
            targetServerName = 'example.com', remoteIp = '5.6.7.8',
            isSecure = False, baseUrl = "http://blah", systemId='foo')
        self.assertEquals(caller.url.scheme, 'https')
        self.assertEquals(caller.url.hostport.port, 443)
        # This whole thing points out a workaround for _not_ going through SSL
        # if you choose so: add a repositoryMap that explicitly adds :80 to
        # the server URL.

    @runproxy(users = [ ('localhost1', 'otheruser', 'pw') ])
    def testUserOverrides(self, proxyRepos):
        repos, label, hostname = self._getRepos(proxyRepos)

        self.addUserAndRole(repos, label, 'otheruser', 'pw')
        repos.addAcl(label, 'otheruser', None, None)

        repos.deleteUserByName(label, 'anonymous')
        repos.deleteUserByName(label, 'test')

        # since both users have been erased from the repository, this can
        # only work if the 'other' user is added in by the proxy
        proxyRepos.c[hostname].checkVersion()

    def testTruncatedChangesets(self):
        """
        Test that a proxy will not cache a changeset that has been truncated in
        transit.
        """
        # Get a proxy server and a repository server both with changeset caches.
        self.stopRepository(2)
        repos = self.openRepository(2, withCache=True)
        reposServer = self.servers.getCachedServer(2)

        proxyServer = self.getConaryProxy()
        try:
            proxyServer.start()
            sock_utils.tryConnect("127.0.0.1", proxyServer.port)

            cfg = copy.deepcopy(self.cfg)
            cfg.configLine('conaryProxy http http://localhost:%s' %
                    proxyServer.port)
            cfg.configLine('conaryProxy https http://localhost:%s' %
                    proxyServer.port)
            client = conaryclient.ConaryClient(cfg)
            proxyRepos = client.getRepos()

            trv = self.addComponent('foo:data', '/localhost2@rpl:linux/1-1-1',
                    repos=repos)
            jobList = [ (trv.getName(), (None, None),
                (trv.getVersion(), trv.getFlavor()), True) ]

            # First populate the repository (not proxy) cscache
            kwargs = dict(recurse=False, withFiles=True, withFileContents=True,
                    excludeAutoSource=False)
            cs = repos.createChangeSet(jobList, **kwargs)

            # Now corrupt the changeset and try to pull it through the proxy
            # cache.  Unfortunately the simplest way to do this is to truncate
            # the contents file which is transcluded into the changeset. We get
            # the path to that using the file contents sha1 from the changeset
            # we fetched earlier.
            assert len(cs.files) == 1
            sha1 = ThawFile(cs.files.values()[0], None).contents.sha1()
            sha1 = sha1.encode('hex')
            path = os.path.join(reposServer.reposDir, 'contents',
                    sha1[:2], sha1[2:4], sha1[4:])

            os.rename(path, path + '.old')
            open(path, 'w').write('hahaha')

            # At this point, fetching a changeset through the proxy should fail.
            err = self.assertRaises(errors.RepositoryError,
                    proxyRepos.createChangeSet, jobList, **kwargs)
            if 'Changeset was truncated in transit' not in str(err):
                self.fail("Unexpected error when fetching truncated "
                        "changeset: %s" % str(err))

            # If we put the file back, it should succeed.
            os.rename(path + '.old', path)
            proxyRepos.createChangeSet(jobList, **kwargs)

        finally:
            proxyServer.stop()
            self.stopRepository(2)

    @runproxy(withCapsuleContentServer = True)
    def testReassembleChangesets(self, proxyRepos):
        # Reassemble capsule based contents
        repos, label, hostname = self._getRepos(proxyRepos)

        ver0 = "/localhost1@rpl:linux/1-1-1"
        trv0 = self.addComponent("foo:data", ver0, filePrimer = 1)
        rpmFile0 = os.path.join(self.sourceSearchDir, 'with-config-0.1-1.noarch.rpm')
        trv0 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver0),
            rpmPath = rpmFile0)
        self.addCollection("foo", ver0, [":data", ":runtime"])

        # Relative changeset
        ver1 = "/localhost1@rpl:linux/1-1-2"
        trv1 = self.addComponent("foo:data", ver1, filePrimer = 1)
        trv1 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver1),
            rpmPath = rpmFile0)
        self.addCollection("foo", ver1, [":data", ":runtime"])

        # Relative changeset
        ver2 = "/localhost1@rpl:linux/2-1-1"
        trv2 = self.addComponent("foo:data", ver2, filePrimer = 2)
        rpmFile2 = os.path.join(self.sourceSearchDir, 'with-config-0.2-1.noarch.rpm')
        trv2 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver2),
            rpmPath = rpmFile2)
        self.addCollection("foo", ver2, [":data", ":runtime"])

        # Absolute changeset
        joblist = [ ('foo', (None, None),
                            (trv0.getVersion(), trv0.getFlavor()), True) ]

        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)
        csFiles, trvCsMap = self._processAbsoluteChangeset(cs)
        data = []
        for (pathId, path, fileId, fileVer, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))
        rpmContents = file(rpmFile0).read()
        self.assertEqual(data, [
            (('foo:runtime', (None, ''),
                (ver0, ''), True),
              'cft-file', os.path.basename(rpmFile0), rpmContents),
            (('foo:data', (None, ''),
                (ver0, ''), True),
              'cft-file', '/contents1', 'hello, world!\n'),
            (('foo:runtime', (None, ''),
                (ver0, ''), True),
                'cft-file', '/etc/with-config.cfg', 'config option\n'),
        ])


        absCs = cs

        # Relative changeset, the rpm capsule does not change
        joblist = [ ('foo', (trv0.getVersion(), trv0.getFlavor()),
            (trv1.getVersion(), trv1.getFlavor()), False) ]
        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)

        csFiles, trvCsMap1 = self._processRelativeChangeset(cs, absCs, trvCsMap)
        data = []
        for (pathId, path, fileId, fileVersion, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))
        rpmContents = file(rpmFile0).read()
        self.assertEqual(data, [
            (('foo:runtime', (ver0, ''), (ver1, ''), False),
              'cft-file', os.path.basename(rpmFile0), rpmContents),
            (('foo:runtime', (ver0, ''), (ver1, ''), False),
                'cft-file', '/etc/with-config.cfg', 'config option\n'),
        ])


        # Relative changeset with a change in the RPM
        joblist = [ ('foo', (trv1.getVersion(), trv1.getFlavor()),
            (trv2.getVersion(), trv2.getFlavor()), False) ]
        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)

        csFiles, trvCsMap2 = self._processRelativeChangeset(cs, absCs, trvCsMap1)
        data = []
        for (pathId, path, fileId, fileVersion, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))

        rpmContents = file(rpmFile2).read()
        self.assertEqual(data, [
            (('foo:runtime', (ver1, ''), (ver2, ''), False),
              'cft-file', os.path.basename(rpmFile2), rpmContents),
            (('foo:runtime', (ver1, ''), (ver2, ''), False),
                'cft-file', '/etc/with-config.cfg', 'config option\nand another config option\n'),
        ])

    @runproxy(withCapsuleContentServer = True)
    def testReassembleChangesetsWithSpecialFiles(self, proxyRepos):
        # Reassemble capsule based contents
        repos, label, hostname = self._getRepos(proxyRepos)

        ver0 = "/localhost1@rpl:linux/1-1-1"
        trv0 = self.addComponent("foo:data", ver0, filePrimer = 1)
        rpmFile0 = os.path.join(self.sourceSearchDir, 'with-config-special-0.1-1.noarch.rpm')
        trv0 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver0),
            rpmPath = rpmFile0)
        self.addCollection("foo", ver0, [":data", ":runtime"])

        # Relative changeset
        ver2 = "/localhost1@rpl:linux/2-1-1"
        trv2 = self.addComponent("foo:data", ver2, filePrimer = 2)
        rpmFile2 = os.path.join(self.sourceSearchDir, 'with-config-special-0.2-1.noarch.rpm')
        trv2 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver2),
            rpmPath = rpmFile2)
        self.addCollection("foo", ver2, [":data", ":runtime"])

        # Absolute changeset
        joblist = [ ('foo', (None, None),
                            (trv0.getVersion(), trv0.getFlavor()), True) ]

        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)
        csFiles, trvCsMap = self._processAbsoluteChangeset(cs)
        data = []
        for (pathId, path, fileId, fileVer, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))
        data.sort()
        rpmContents = file(rpmFile0).read()
        self.assertEqual(data, [
            (('foo:data', (None, ''), (ver0, ''), True),
              'cft-file', '/contents1', 'hello, world!\n'),
            (('foo:runtime', (None, ''), (ver0, ''), True),
              'cft-file', '/etc/with-config-special.cfg', 'config option\n'),
            (('foo:runtime', (None, ''), (ver0, ''), True),
              'cft-file', os.path.basename(rpmFile0), rpmContents),
        ])

        absCs = cs

        # Relative changeset
        joblist = [ ('foo', (trv0.getVersion(), trv0.getFlavor()),
            (trv2.getVersion(), trv2.getFlavor()), False) ]
        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)

        csFiles, trvCsMap1 = self._processRelativeChangeset(cs, absCs, trvCsMap)
        data = []
        for (pathId, path, fileId, fileVersion, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))
        data.sort()
        rpmContents = file(rpmFile2).read()
        self.assertEqual(data, [
            (('foo:runtime', (ver0, ''), (ver2, ''), False),
              'cft-file', '/etc/with-config-special.2.cfg', 'second config file\n'),
            (('foo:runtime', (ver0, ''), (ver2, ''), False),
              'cft-file', '/etc/with-config-special.cfg', 'config option\nand another config line\n'),
            (('foo:runtime', (ver0, ''), (ver2, ''), False),
              'cft-file', os.path.basename(rpmFile2), rpmContents),
        ])

    @runproxy(withCapsuleContentServer = True)
    def testReassembleChangesetsDerivedCapsule(self, proxyRepos):
        # Reassemble capsule based contents, derived capsule case
        repos, label, hostname = self._getRepos(proxyRepos)

        pkgname = 'with-config'
        # create a capsule package
        recipe = """
class TestPackage(CapsuleRecipe):
    name = 'with-config'
    version = '0.1'

    clearBuildReqs()
    def setup(r):
        r.addCapsule('with-config-0.1-1.noarch.rpm')
"""
        derivedRecipe = r"""
class TestPackage(DerivedCapsuleRecipe):
    name = 'with-config'
    version = '0.1'

    clearBuildReqs()
    def setup(r):
        # Add a file
        r.Create("/usr/share/newfile.txt",
            contents="New file contents\n")
        # Change a config file
        r.Create("/etc/with-config.cfg",
            contents="Configuration file contents\n")
        # Change a regular file
        r.Create("/usr/share/with-config.txt",
            contents="Regular file contents\n")
"""
        from conary import versions as CV
        buildLabel = CV.Label("localhost1@rpl:linux")
        self.makeSourceTrove(pkgname, recipe, buildLabel=buildLabel)
        built = self.cookFromRepository(pkgname, buildLabel=buildLabel)
        nvf = built[0]

        # create a derived package
        from conary.build import derive
        shadowLabel = CV.Label('localhost1@rpl:shadowLabel')
        checkoutDir = os.path.join(self.workDir, pkgname + '-derived')
        derive.derive(repos, self.cfg, shadowLabel,
            (pkgname, nvf[1], nvf[2]),
            checkoutDir=checkoutDir)
        file(os.path.join(checkoutDir, "with-config.recipe"), "w").write(
            derivedRecipe)
        oldDir = os.getcwd()
        os.chdir(checkoutDir)
        self.commit()
        os.chdir(oldDir)

        built = self.cookFromRepository(pkgname, buildLabel=shadowLabel)
        nvfd = built[0]

        joblist = [ (pkgname, (None, None),
            (CV.VersionFromString(nvfd[1]), nvfd[2]), False) ]

        expFiles = [
            (('with-config:rpm', (None, ''),
              ('/localhost1@rpl:linux//shadowLabel/0.1-1.1-1', ''), False),
             'cft-file', 'with-config.rpm'),
            (('with-config:rpm', (None, ''),
              ('/localhost1@rpl:linux//shadowLabel/0.1-1.1-1', ''), False),
             'cft-file', '/usr/share/with-config.txt'),
            (('with-config:rpm', (None, ''),
              ('/localhost1@rpl:linux//shadowLabel/0.1-1.1-1', ''), False),
             'cft-file', '/etc/with-config.cfg'),
            (('with-config:data', (None, ''),
              ('/localhost1@rpl:linux//shadowLabel/0.1-1.1-1', ''), False),
             'cft-file', '/usr/share/newfile.txt'),
        ]
        expFileContents = ['Regular file contents\n',
            'Configuration file contents\n',
            'New file contents\n']

        # fetch the changeset without encapsulation - verify CNY-3577
        cs = repos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)
        csFiles, trvCsMap = self._processAbsoluteChangeset(cs)
        data = []
        for (pathId, path, fileId, fileVer, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))

        self.assertEqual([x[:3] for x in data], expFiles)
        self.assertEqual([ x[3] for x in data[1:] ], expFileContents)

        # Now fetch the changeset through an injecting proxy
        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)
        csFiles, trvCsMap = self._processAbsoluteChangeset(cs)
        data = []
        for (pathId, path, fileId, fileVer, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))
        self.assertEqual([x[:3] for x in data], expFiles)
        self.assertEqual([ x[3] for x in data[1:] ], expFileContents)

    def _processAbsoluteChangeset(self, cs):
        csFiles = []
        trvCsMap = {}
        for trvCs in sorted(cs.iterNewTroveList()):
            trvCsMap[trvCs.getNewNameVersionFlavor()] = trvCs
            # For capsule changesets, we only store the capsule and config
            # files
            capsuleType = trvCs.getTroveInfo().capsule.type()
            for pathId, path, fileId, fileVer in trvCs.getNewFileList():
                if capsuleType and pathId != rephelp.trove.CAPSULE_PATHID:
                    fileStream = cs.getFileChange(None, fileId)
                    fileObj = rephelp.files.ThawFile(fileStream, pathId)
                    if not (
                            (isinstance(fileObj, rephelp.files.RegularFile)
                                and not fileObj.flags.isEncapsulatedContent()
                                and fileObj.contents.size() == 0)
                            or fileObj.flags.isConfig()
                            or fileObj.flags.isCapsuleAddition()
                            or fileObj.flags.isCapsuleOverride()
                        ) or isinstance(fileObj, (rephelp.files.SymbolicLink,
                                     rephelp.files.Directory)):
                        continue
                csFiles.append((pathId, path, fileId, fileVer, trvCs.getJob()))
        csFiles.sort(key = lambda x: (x[0], x[2]))
        return csFiles, trvCsMap

    def _processRelativeChangeset(self, cs, absCs, trvCsMap):
        newMap = {}
        csFiles = []
        for trvCs in sorted(cs.iterNewTroveList()):
            oldTrvCs = trvCsMap[trvCs.getOldNameVersionFlavor()]
            oldTrv = rephelp.trove.Trove(oldTrvCs)
            newTrv = oldTrv.copy()
            newTrv.applyChangeSet(trvCs)
            newMap[trvCs.getNewNameVersionFlavor()] = newTrv.diff(None)[0]
            capsuleType = trvCs.getTroveInfo().capsule.type()
            for pathId, path, fileId, fileVer in itertools.chain(
                                        trvCs.getChangedFileList(),
                                        trvCs.getNewFileList()):
                path = newTrv.getFile(pathId)[0]
                oldFileId = (oldTrv.hasFile(pathId) and
                    oldTrv.getFile(pathId)[1]) or None
                fileDiff = cs.getFileChange(oldFileId, fileId)
                if capsuleType and pathId != rephelp.trove.CAPSULE_PATHID:
                    if rephelp.changeset.files.fileStreamIsDiff(fileDiff):
                        assert oldFileId is not None
                        oldFileStream = absCs.getFileChange(None, oldFileId)
                        fileObj = rephelp.changeset.files.ThawFile(oldFileStream, pathId)
                    else:
                        fileObj = rephelp.changeset.files.ThawFile(fileDiff, pathId)
                    if not fileObj.flags.isConfig() or isinstance(fileObj,
                         (rephelp.files.SymbolicLink, rephelp.files.Directory)):
                        continue
                elif not capsuleType and not rephelp.changeset.files.contentsChanged(fileDiff):
                    continue
                csFiles.append((pathId, path, fileId, fileVer, trvCs.getJob()))
        csFiles.sort()
        return csFiles, newMap

    @classmethod
    def _strVerFlv(cls, (version, flavor)):
        if version is not None:
            version = str(version)
        if flavor is not None:
            flavor = str(flavor)
        return (version, flavor)

    @classmethod
    def _strJob(cls, job):
        return (job[0], cls._strVerFlv(job[1]), cls._strVerFlv(job[2]), job[3])

    @runproxy(withCapsuleContentServer = True)
    def testGetFileContents(self, proxyRepos):
        # Reassemble capsule based contents
        repos, label, hostname = self._getRepos(proxyRepos)

        ver0 = "/localhost1@rpl:linux/1-1-1"
        trv0 = self.addComponent("foo:data", ver0, filePrimer = 1)
        rpmFile0 = os.path.join(self.sourceSearchDir, 'with-config-special-0.1-1.noarch.rpm')
        trv0 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver0),
            rpmPath = rpmFile0)
        self.addCollection("foo", ver0, [":data", ":runtime"])

        joblist = [ ('foo', (None, None),
                            (trv0.getVersion(), trv0.getFlavor()), True) ]

        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)

        # Find a config file
        csFiles, trvCsMap = self._processAbsoluteChangeset(cs)

        fileList = [ (x[2], x[3]) for x in csFiles ]
        # Add a fake file
        fileList.append(('\011' * 20, fileList[0][1]))

        data = repos.getFileContentsCapsuleInfo(fileList)
        expected = [
            ('rpm', ('with-config-special', None, '0.1', '1', 'noarch'),
                'd1f2985d7f390b5c4d7e1cad062b3fedcdff53d1',
                '',
                'd1f2985d7f390b5c4d7e1cad062b3fedcdff53d1'),
            None,
            ('rpm', ('with-config-special', None, '0.1', '1', 'noarch'),
                'd1f2985d7f390b5c4d7e1cad062b3fedcdff53d1',
                '/etc/with-config-special.cfg',
                'cb7bc5acfd68ebdff031c82ae8f989e2c633fea3'),
            None,
        ]
        from conary.lib import sha1helper
        actual = [ (x[0], x[1], sha1helper.sha1ToString(x[2]), x[3],
                sha1helper.sha1ToString(x[4]))
            for x in [data[0], data[2]] ]
        self.assertEqual([ actual[0], None, actual[1], None],
            expected)

        # Drop the extra file id
        fileList = fileList[:-1]
        # Make sure we did hit the capsule indexer
        logFile = os.path.join(self.workDir, "capsuleContentServer.log")
        logCount = len([x for x in file(logFile)])
        ret = proxyRepos.getFileContents(fileList)
        self.assertEqual(len([x for x in file(logFile)]) - logCount, 2)

    @runproxy(withCapsuleContentServer = True)
    def testReassembleChangesetsMixedServers(self, proxyRepos):
        # Reassemble capsule based contents
        self.stopRepository(2)
        repos2 = self.openRepository(2)
        repos, label, hostname = self._getRepos(proxyRepos)
        proxyRepos.c.map.extend([x for x in repos2.c.map
            if x[0] == 'localhost2'])

        ver0 = "/localhost1@rpl:linux/1-1-1"
        trv0 = self.addComponent("foo:data", ver0, filePrimer = 1)
        rpmFile0 = os.path.join(self.sourceSearchDir, 'with-config-special-0.1-1.noarch.rpm')
        trv0 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver0),
            rpmPath = rpmFile0)
        self.addCollection("foo", ver0, [":data", ":runtime"])

        ver1 = "/localhost2@rpl:linux/2-1-1"
        trv1 = self.addComponent("bar:data", ver1, filePrimer = 2)
        rpmFile1 = os.path.join(self.sourceSearchDir, 'with-config-special-0.2-1.noarch.rpm')
        trv1 = self.addRPMComponent("%s=%s" % ("bar:runtime", ver1),
            rpmPath = rpmFile1)
        self.addCollection("bar", ver1, [":data", ":runtime"])

        # Absolute changeset (although for testing mixed repo types it should
        # not matter
        joblist = [ ('foo', (None, None),
                            (trv0.getVersion(), trv0.getFlavor()), True),
                    ('bar', (None, None),
                            (trv1.getVersion(), trv1.getFlavor()), True),
        ]

        # Make sure we did hit the capsule indexer
        logFile = os.path.join(self.workDir, "capsuleContentServer.log")
        if os.path.exists(logFile):
            os.unlink(logFile)
        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)
        self.assertEqual(len([x for x in file(logFile)]), 2)
        # Also, make sure we only requested stuff for foo
        expected = '/toplevel/%s' % os.path.basename(rpmFile0)
        for row in file(logFile):
            self.assertTrue(row.startswith(expected), row.rstrip())

        csFiles, trvCsMap = self._processAbsoluteChangeset(cs)
        data = []
        for (pathId, path, fileId, fileVer, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))
        data.sort()
        rpmContents0 = file(rpmFile0).read()
        rpmContents1 = file(rpmFile1).read()
        self.assertEqual(data, [
            (('bar:data', (None, ''), (ver1, ''), True),
              'cft-file', '/contents2', 'hello, world!\n'),
            (('bar:runtime', (None, ''), (ver1, ''), True),
              'cft-file', '/etc/with-config-special.2.cfg', 'second config file\n'),
            (('bar:runtime', (None, ''), (ver1, ''), True),
                'cft-file', '/etc/with-config-special.cfg', 'config option\nand another config line\n'),
            (('bar:runtime', (None, ''), (ver1, ''), True),
              'cft-file', os.path.basename(rpmFile1), rpmContents1),
            (('foo:data', (None, ''), (ver0, ''), True),
              'cft-file', '/contents1', 'hello, world!\n'),
            (('foo:runtime', (None, ''), (ver0, ''), True),
              'cft-file', '/etc/with-config-special.cfg', 'config option\n'),
            (('foo:runtime', (None, ''), (ver0, ''), True),
              'cft-file', os.path.basename(rpmFile0), rpmContents0),
        ])

    @runproxy(withCapsuleContentServer = True)
    def testReassembleChangesetsWithGhostFiles(self, proxyRepos):
        # Reassemble capsule based contents
        repos, label, hostname = self._getRepos(proxyRepos)

        ver0 = "/localhost1@rpl:linux/1-1-1"
        trv0 = self.addComponent("foo:data", ver0, filePrimer = 1)
        rpmFile0 = os.path.join(self.sourceSearchDir, 'with-config-special-0.3-1.noarch.rpm')
        trv0 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver0),
            rpmPath = rpmFile0)
        self.addCollection("foo", ver0, [":data", ":runtime"])

        # Absolute changeset
        joblist = [ ('foo', (None, None),
                            (trv0.getVersion(), trv0.getFlavor()), True) ]

        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)
        csFiles, trvCsMap = self._processAbsoluteChangeset(cs)
        data = []
        for (pathId, path, fileId, fileVer, trvspec) in csFiles:
            contType, cont = cs.getFileContents(pathId, fileId)
            data.append((self._strJob(trvspec), contType, path, cont.get().read()))
        data.sort()
        rpmContents = file(rpmFile0).read()
        self.assertEqual(data, [
            (('foo:data', (None, ''), (ver0, ''), True),
              'cft-file', '/contents1', 'hello, world!\n'),
            (('foo:runtime', (None, ''), (ver0, ''), True),
              'cft-file', '/etc/with-config-special.2.cfg', 'second config file\n'),
            (('foo:runtime', (None, ''), (ver0, ''), True),
                'cft-file', '/etc/with-config-special.cfg', 'config option\nand another config line\n'),
            (('foo:runtime', (None, ''), (ver0, ''), True),
                'cft-file', '/usr/share/subdir/ghost-file-empty.txt', ''),
            (('foo:runtime', (None, ''), (ver0, ''), True),
                'cft-file', '/usr/share/subdir/ghost-file.txt', ''),
            (('foo:runtime', (None, ''), (ver0, ''), True),
              'cft-file', os.path.basename(rpmFile0), rpmContents),
        ])

    @runproxy(withCapsuleContentServer = True)
    def testGetFileContentsGhostFiles(self, proxyRepos):
        # Make sure we can fetch contents for ghost files
        # Reassemble capsule based contents
        repos, label, hostname = self._getRepos(proxyRepos)

        ver0 = "/localhost1@rpl:linux/1-1-1"
        trv0 = self.addComponent("foo:data", ver0, filePrimer = 1)
        rpmFile0 = os.path.join(self.sourceSearchDir, 'with-config-special-0.3-1.noarch.rpm')
        trv0 = self.addRPMComponent("%s=%s" % ("foo:runtime", ver0),
            rpmPath = rpmFile0)
        self.addCollection("foo", ver0, [":data", ":runtime"])

        # Absolute changeset
        joblist = [ ('foo', (None, None),
                            (trv0.getVersion(), trv0.getFlavor()), True) ]

        cs = proxyRepos.createChangeSet(joblist, withFiles = True,
            withFileContents = True)
        csFiles, trvCsMap = self._processAbsoluteChangeset(cs)
        interestingFiles = set([
            '/usr/share/subdir/ghost-file-empty.txt'])
        fileList = [ (x[2], x[3]) for x in csFiles
            if x[1] in interestingFiles ]
        ret = proxyRepos.getFileContents(fileList)
        self.assertEqual(ret[0].f.read(), '')


    def testGetFileContentsCheckAuth(self):
        """Test CachingRepositoryServer.getFileContents with and without
        authCheckOnly.

        @tests: CNY-3574
        """
        # Since there's currently no way to instantiate CachingRepositoryServer
        # using config files alone (mint is the only consumer), we have to do
        # it without a separate standalone repository. Start a regular
        # repository to get a server config file, then instantiate the caching
        # repository on top of that.
        repos = self.openRepository()
        trv = self.addComponent("foo:runtime")

        server = self.servers.getCachedServer(0)
        serverrc = server.getServerConfigPath()
        servercfg = cnyserver.ServerConfig()
        servercfg.read(serverrc)

        cacheDir = tempfile.mkdtemp()
        try:
            servercfg.changesetCacheDir = cacheDir
            servercfg.proxyContentsDir = cacheDir
            basicUrl = 'http://localhost:%s/conary/' % (server.port,)
            netServer = netserver.NetworkRepositoryServer(servercfg, basicUrl)
            netFilter = netreposproxy.CachingRepositoryServer(servercfg,
                    basicUrl, netServer)
            authToken = ['test', 'foo', [], '127.0.0.1']

            # Lame shim client implementation that lets us pass things that
            # netclient doesn't (authCheckOnly)
            def call(method, *args, **kwargs):
                version = netserver.SERVER_VERSIONS[-1]
                req = xmlshims.RequestArgs(version, args, kwargs)
                headers = {'X-Conary-Servername': 'localhost1'}
                netFilter.setBaseUrlOverride(basicUrl, headers, isSecure=True)
                response, respHeaders = netFilter.callWrapper('https',
                        server.port, method, authToken, req, headers=headers,
                        isSecure=True)
                if response.isException:
                    raise netclient.unmarshalException(response.excName,
                            response.excArgs, response.excKwargs)
                return response.result

            # Do a regular getFileContents first
            _, _, fileId, fileVer = list(trv.iterFileList())[0]
            fileList = [(repos.fromFileId(fileId), repos.fromVersion(fileVer))]
            url, sizes = call('getFileContents', fileList)
            self.assertEquals(sizes, ['34'])

            # Now do authCheckOnly
            assert call('getFileContents', fileList, authCheckOnly=True)

        finally:
            util.rmtree(cacheDir)

    # uncomment this to test against a real memcached
    #@runproxy(cacheTimeout = 0, cacheLocation = "127.0.0.1:11211")
    @runproxy(cacheTimeout = 0)
    def testProxyCaching(self, proxyRepos):
        # Make sure we can fetch contents for ghost files
        # Reassemble capsule based contents
        ver0 = "/localhost1@rpl:linux/1-1-1"
        trv0 = self.addComponent("foo:data", ver0, filePrimer = 1)
        trv = proxyRepos.getTrove(*trv0.getNameVersionFlavor())
        deps = proxyRepos.getDepsForTroveList([ trv.getNameVersionFlavor() ],
                                              provides = True, requires = True)
        ti = proxyRepos.getTroveInfo(trove._TROVEINFO_TAG_SOURCENAME,
                                     [ trv.getNameVersionFlavor() ])
        self.stopRepository(1)
        trv1 = proxyRepos.getTrove(*trv0.getNameVersionFlavor())
        deps1 = proxyRepos.getDepsForTroveList([ trv.getNameVersionFlavor() ],
                                              provides = True, requires = True)
        ti1 = proxyRepos.getTroveInfo(trove._TROVEINFO_TAG_SOURCENAME,
                                      [ trv.getNameVersionFlavor() ])
        self.assertEquals(trv, trv1)
        self.assertEquals(deps, deps1)
        self.assertEquals(ti, ti1)

        # we reopen it for proper cleanup in the runproxy() decorator
        self.openRepository(1)
