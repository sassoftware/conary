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
import os
import signal
import sys
import urllib2

from conary_test import rephelp
from conary_test import resources

from conary import conarycfg
from conary import conaryclient
from conary import dbstore
from conary.lib import util
from conary.repository.netrepos import netserver, proxy
from conary.repository.netrepos import netauth
from conary.server import server
from conary.server import schema


class ServerTest(rephelp.RepositoryHelper):
    def testDownloadMissingCf(self):
        repos = self.openRepository()
        base = repos.c.map['localhost']
        for path in ('changeset?foobar.cf', 'changeset?foobar.cf-out'):
            url = base + path
            try:
                f = urllib2.urlopen(url)
            except urllib2.HTTPError, e:
                assert(e.code == 404)
            else:
                raise RuntimeError('404 not returned')

    def testSlowActionWithStandalone(self):
        return self._testSlowActionWithStandalone()

    def testSlowActionWithStandaloneSSL(self):
        raise testhelp.SkipTestException("Fails periodically in automated tests")
        if not server.SSL:
            raise testhelp.SkipTestException("m2crypto not installed")
        return self._testSlowActionWithStandalone(useSSL = True)

    def _testSlowActionWithStandalone(self, useSSL = False):
        # Test to make sure that slow commits still work even with the
        # proxy keepalive code added (CNY-1341)
        cfg = server.ServerConfig()
        cfg.port = testhelp.findPorts(1)[0]
        cfg.contentsDir = self.workDir + '/contents'
        cfg.repositoryDB = ('sqlite', self.workDir + '/serverdb')
        cfg.logFile = self.workDir + '/serverlog'
        cfg.tmpDir = self.workDir + '/tmp'
        cfg.serverName = 'localhost'
        util.mkdirChain(cfg.tmpDir)
        util.mkdirChain(cfg.contentsDir)
        if useSSL:
            cfg.useSSL = True
            cfg.sslCert = os.path.join(resources.get_archive(), 'ssl-cert.crt')
            cfg.sslKey = os.path.join(resources.get_archive(), 'ssl-cert.key')

        (driver, database) = cfg.repositoryDB
        db = dbstore.connect(database, driver)
        schema.loadSchema(db)
        schema.setupTempTables(db)
        auth = netauth.NetworkAuthorization(db, 'localhost')
        auth.addRole('foo')
        auth.addUser('foo', 'foo')
        auth.addRoleMember('foo', 'foo')
        auth.addAcl('foo', None, None, write = True)


        if useSSL:
            proto = "https"
        else:
            proto = "http"
        baseUrl = '%s://localhost:%s/' % (proto, cfg.port)

        pid = os.fork()
        if not pid:
            try:
                netServer = netserver.NetworkRepositoryServer(cfg, baseUrl)

                oldGetChangeSet = netServer.getChangeSet
                @netserver.accessReadOnly
                def getChangeSet(*args, **kw):
                    rv = oldGetChangeSet(*args, **kw)
                    # make sure the client sends its message
                    self.sleep(7)
                    return rv
                getChangeSet.im_func = getChangeSet
                netServer.getChangeSet = getChangeSet

                class HttpRequestsSubclass(server.HttpRequests):
                    tmpDir = cfg.tmpDir
                    netRepos = proxy.SimpleRepositoryFilter(cfg, baseUrl, netServer)
                    restHandler = None

                HttpRequestsSubclass.cfg = cfg

                if useSSL:
                    ctx = server.createSSLContext(cfg)
                    httpServer = server.SecureHTTPServer(("", cfg.port),
                                                         HttpRequestsSubclass, ctx)
                else:
                    httpServer = server.HTTPServer(("", cfg.port),
                                                   HttpRequestsSubclass)
                self.captureOutput(server.serve, httpServer)
            finally:
                os._exit(0)
        try:
            sock_utils.tryConnect("127.0.0.1", cfg.port)
            cfg = conarycfg.ConaryConfiguration(False)
            cfg.repositoryMap  = {'localhost' : baseUrl }
            cfg.user.addServerGlob('localhost', 'foo', 'foo')
            client = conaryclient.ConaryClient(cfg)
            repos = client.getRepos()

            trv, cs = self.Component('foo:run', '1')
            repos.commitChangeSet(cs)
            # getTrove will fail because it takes more than 5 seconds
            assert(repos.getTrove(*trv.getNameVersionFlavor()))
        finally:
            os.kill(pid, signal.SIGTERM)
            os.waitpid(pid, 0)

    def testSecureHTTPServer(self):
        # Checks that the secure SSL server works
        if not server.SSL:
            raise testhelp.SkipTestException("m2crypto not installed")

        cfg = server.ServerConfig()
        cfg.port = testhelp.findPorts(1)[0]
        cfg.tmpdir = os.path.join(self.workDir, 'proxyTmpDir')
        cfg.changesetCacheDir = os.path.join(self.workDir, 'changesetCacheDir')
        cfg.proxyContentsDir = os.path.join(self.workDir, 'proxyContentsDir')
        cfg.traceLog = (10, os.path.join(self.workDir, "proxy.debug"))

        cfg.useSSL = True
        cfg.sslCert = os.path.join(resources.get_archive(), 'ssl-cert.crt')
        cfg.sslKey = os.path.join(resources.get_archive(), 'ssl-cert.key')

        cfgfile = os.path.join(self.workDir, "proxy.conf")
        serverrc = open(cfgfile, "w+")
        cfg.store(serverrc, includeDocs=False)
        serverrc.close()
        util.mkdirChain(cfg.tmpdir)
        util.mkdirChain(cfg.changesetCacheDir)
        util.mkdirChain(cfg.proxyContentsDir)

        pid = os.fork()
        if pid == 0:
            # In the child
            try:
                errfd = os.open(os.path.join(self.workDir, "proxy.log"),
                    os.O_RDWR | os.O_CREAT)
                os.dup2(errfd, sys.stderr.fileno())
                os.close(errfd)
                srvpy = os.path.abspath(server.__file__).replace('.pyc', '.py')
                os.execv(srvpy, [ srvpy, '--config-file', cfgfile ])
                os._exit(0)
            except:
                os._exit(70)

        self.openRepository(2, useSSL=True)
        # In the parent
        try:
            # Set conary proxy
            self.cfg.configLine("conaryProxy https://localhost:%s" % cfg.port)

            client = conaryclient.ConaryClient(self.cfg)

            sock_utils.tryConnect("127.0.0.1", cfg.port)

            srvVers = client.repos.c['localhost2'].checkVersion()
            self.assertTrue(srvVers)
        finally:
            self.servers.stopServer(2)
            os.kill(pid, signal.SIGTERM)
            os.waitpid(pid, 0)
