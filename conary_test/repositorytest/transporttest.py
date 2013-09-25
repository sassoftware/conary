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
import SimpleHTTPServer
import socket
import time
import httplib
from SimpleHTTPServer import SimpleHTTPRequestHandler
from SimpleXMLRPCServer import SimpleXMLRPCServer

from conary_test import rephelp
from conary import conaryclient
from conary.lib.http import opener as opmod
from conary.lib.http import proxy_map
from conary.lib.http import request
from conary.repository import transport, errors
from conary.repository.netrepos import netserver
from conary.lib import httputils
from conary.lib import networking
from conary.lib import util
from M2Crypto import SSL
from testrunner import testhelp
from testutils import sock_utils
from conary_test import resources


class TransportTest(rephelp.RepositoryHelper):
    def testRetrying(self):
        """We test that conary retries in a bunch of cases"""
        class successException(Exception):
            '''This exception will be raised in place of actually calling
            the actual function'''
            pass

        self.mockMax = 0
        self.theException = None
        self.theReturn = socket.socket()

        def mockCountdown(*args, **kwargs):
            if self.mockMax <= 1:
                raise successException
            else:
                self.mockMax -= 1
            raise self.theException

        try:
            # setup
            self.openRepository()
            client = conaryclient.ConaryClient(self.cfg)
            self.mock(transport.URLOpener, 'createConnection',
                      mockCountdown)

            # test ssl errors
            self.theException = socket.sslerror(socket.SSL_ERROR_EOF,
                                                'A particular ssl error')
            self.mockMax = 2
            try:
                client.repos.c['localhost'].checkVersion()
            except errors.OpenError, e:
                if 'successException' not in e.args[0]:
                    raise

            self.mockMax = 6
            try:
                client.repos.c['localhost'].checkVersion()
            except errors.OpenError, e:
                if 'A particular ssl error' not in e.args[0]:
                    raise

            # test gai errors
            self.theException = socket.gaierror(socket.EAI_AGAIN,
                                                'A particular gai error')
            self.mockMax = 2
            try:
                client.repos.c['localhost'].checkVersion()
            except errors.OpenError, e:
                if 'successException' not in e.args[0]:
                    raise

            self.mockMax = 6
            try:
                client.repos.c['localhost'].checkVersion()
            except errors.OpenError, e:
                if 'A particular gai error' not in e.args[0]:
                    raise

            # this should fail right away
            self.theException = socket.gaierror('bogus error',
                                                'bogus gai error')
            self.mockMax = 100
            try:
                client.repos.c['localhost'].checkVersion()
            except errors.OpenError, e:
                self.assertEquals(self.mockMax, 99)

            # test bad status errors
            self.theException = httplib.BadStatusLine(
                'A particular bad status line error')
            self.mockMax = 2
            try:
                client.repos.c['localhost'].checkVersion()
            except errors.OpenError, e:
                if 'successException' not in e.args[0]:
                    raise

            self.mockMax = 6
            try:
                client.repos.c['localhost'].checkVersion()
            except errors.OpenError, e:
                if 'A particular bad status line error' not in e.args[0]:
                    raise

        finally:
            self.unmock()
            self.stopRepository(serverIdx=2)

    def testAbort(self):
        class SlowServer:
            def snooze(self):
                time.sleep(100000)

            def ping(self):
                return [True]

        class SlowXMLServer:
            def __init__(self):
                self.port = testhelp.findPorts(num = 1)[0]
                self.childPid = os.fork()
                if self.childPid > 0:
                    sock_utils.tryConnect('127.0.0.1', self.port)
                    return

                server = SimpleXMLRPCServer(("127.0.0.1", self.port),
                                            logRequests=False)
                server.register_instance(SlowServer())
                server.serve_forever()

            def kill(self):
                os.kill(self.childPid, 15)
                os.waitpid(self.childPid, 0)

            def url(self):
                # make sure the child is ready
                sock_utils.tryConnect("127.0.0.1", self.port)
                return "http://127.0.0.1:%d/" % self.port

        class AbortChecker:
            def __init__(self):
                self.checked = 0

            def check(self):
                self.checked += 1
                # abort on the 3rd check
                if self.checked == 3:
                    return True
                return False

        transporter = transport.Transport()
        checker = AbortChecker()
        transporter.setAbortCheck(checker.check)

        server = SlowXMLServer()
        try:
            proxy = util.ServerProxy(url=server.url(), transport=transporter)
            self.assertEqual(proxy.ping(), ([True],))
            # now try something that takes a long time..
            self.assertRaises(transport.AbortError, proxy.snooze)
        finally:
            server.kill()

    def testHostnameDoesNotResolve(self):
        # make sure that a hostname that doesn't resolve does not prevent
        # the transport code from working (CNY-1531)
        def gethostname():
            return 'garblygook'

        class Server:
            def snooze(self):
                time.sleep(100000)

            def ping(self):
                return [True]

        class XMLServer:
            def __init__(self):
                self.port = testhelp.findPorts(num = 1)[0]
                self.childPid = os.fork()
                if self.childPid > 0:
                    sock_utils.tryConnect('127.0.0.1', self.port)
                    return

                server = SimpleXMLRPCServer(("127.0.0.1", self.port),
                                            logRequests=False)
                server.register_instance(Server())
                server.serve_forever()

            def kill(self):
                os.kill(self.childPid, 15)
                os.waitpid(self.childPid, 0)

            def url(self):
                return "http://127.0.0.1:%d/" % self.port

        oldgethostname = socket.gethostname
        server = XMLServer()
        socket.gethostname = gethostname
        try:
            transporter = transport.Transport()
            proxy = util.ServerProxy(url=server.url(), transport=transporter)
            proxy.ping()
        finally:
            socket.gethostname = oldgethostname
            server.kill()

    def _assertProxy(self, via, cProxy):
        via = networking.HostPort(via.strip().split(' ')[1])
        # Not all app servers can report their local port, and not all app
        # servers will even have one (UNIX socket, etc.)
        self.assertIn(via.port, [cProxy.appServer.port, 0])

    def testConaryProxy(self):
        self.openRepository()
        cProxy = self.getConaryProxy(proxies = self.cfg.conaryProxy)
        if self.cfg.conaryProxy:
            proxyCount = 2
        else:
            proxyCount = 1

        try:
            cProxy.start()
            cProxy.addToConfig(self.cfg)
            repos = conaryclient.ConaryClient(self.cfg).getRepos()

            pv = repos.c['localhost'].getProtocolVersion()
            self.assertEqual(pv, netserver.SERVER_VERSIONS[-1])
            transport = repos.c['localhost']._transport
            self.assertTrue('via' in transport.responseHeaders)
            via = transport.responseHeaders['via']
            via = via.split(',')
            if os.environ.get('CONARY_HTTP_PROXY', None) and \
                              os.environ.get('CONARY_PROXY', None):
                proxyCount += 1
            self.assertEqual(len(via), proxyCount)
            self._assertProxy(via[-1], cProxy)
        finally:
            cProxy.stop()

    def testChainedConaryProxies(self):
        cProxy1 = cProxy2 = None
        self.openRepository()

        try:
            if self.cfg.conaryProxy:
                proxyCount = 1
            else:
                proxyCount = 0

            cProxy1 = self.getConaryProxy(proxies = self.cfg.conaryProxy)
            cProxy1.start()

            proxy1Addr = str(request.URL(cProxy1.getUrl()).hostport)
            proxyHash = dict(http  = ('conary://' + proxy1Addr),
                             https = ('conarys://' + proxy1Addr))

            cProxy2 = self.getConaryProxy(1, proxies=proxyHash)
            cProxy2.start()

            proxy2Addr = str(request.URL(cProxy2.getUrl()).hostport)

            self.cfg.configLine("conaryProxy http://" + proxy2Addr)
            proxyCount += 2

            repos = conaryclient.ConaryClient(self.cfg).getRepos()
            pv = repos.c['localhost'].getProtocolVersion()
            self.assertEqual(pv, netserver.SERVER_VERSIONS[-1])
            transport = repos.c['localhost']._transport
            self.assertTrue('via' in transport.responseHeaders)
            via = transport.responseHeaders['via']
            via = via.split(',')

            if os.environ.get('CONARY_HTTP_PROXY', None) and \
                              os.environ.get('CONARY_PROXY', None):
                proxyCount += 1
            self.assertEqual(len(via), proxyCount)
            self._assertProxy(via[-2], cProxy1)
            self._assertProxy(via[-1], cProxy2)
        finally:
            if cProxy1: cProxy1.stop()
            if cProxy2: cProxy2.stop()

    def testConaryProxyThroughHTTPProxy(self):
        repos = self.openRepository()

        # Tests that a conary proxy can use an HTTP proxy
        if not os.path.exists(rephelp.HTTPProxy.proxyBinPath):
            raise testhelp.SkipTestException('testConaryProxyThroughHTTPProxy depends on squid being installed')


        class MockConfig:
            pass
        cfg = MockConfig()

        h = self.getHTTPProxy()
        h.updateConfig(cfg)

        cp = self.getConaryProxy(proxies = cfg.proxy)

        # Client config
        cp.addToConfig(self.cfg)
        client = conaryclient.ConaryClient(self.cfg)

        try:
            cp.start()

            logsz0 = h.getAccessLogSize()

            srvVers = client.repos.c['localhost'].checkVersion()
            self.assertEqual(srvVers[-1], netserver.SERVER_VERSIONS[-1])

            logEntry = h.getAccessLogEntry(logsz0)
            self.assertTrue(logEntry)

        finally:
            cp.stop()
            h.stop()


    def testHTTPProxy(self):
        if not os.path.exists(rephelp.HTTPProxy.proxyBinPath):
            raise testhelp.SkipTestException('testHTTPProxy depends on squid being installed')

        h = rephelp.HTTPProxy(os.path.join(self.workDir, "http-cache"))
        proxyUri = h.start()

        def runTests(opener, port, sslport):
            url = 'http://localhost:%d/' %port
            sslurl = 'https://localhost:%d' %sslport

            logsz0 = h.getAccessLogSize()
            fd = opener.open(url)
            # Drain the file descriptor
            fd.read()
            logEntry = h.getAccessLogEntry(logsz0)
            opener.close()

            # logEntry looks like:
            # ['1177451531.683', '115', '127.0.0.1', 'TCP_MISS/200', '18671', 'GET',
            # 'http://www.rb.rpath.com', '-', 'DIRECT/172.16.58.31', 'text/html']
            self.assertEqual(logEntry[5:7], ['GET', url])

            logsz0 = h.getAccessLogSize()
            fd = opener.open(sslurl)
            # Drain the file descriptor
            fd.read()
            opener.close()

            logEntry = h.getAccessLogEntry(logsz0)
            self.assertEqual(logEntry[5:7],
                                 ['CONNECT', 'localhost:%d' %sslport])

        proxies = {
            'http'  : "http://%s" % proxyUri,
            'https' : "https://%s" % proxyUri,
        }
        opener = transport.XMLOpener(proxies = proxies)

        class Always200Handler(SimpleHTTPRequestHandler):
            def log_message(self, *args, **kw):
                pass

            def do_GET(self):
                response = 'Hello, world!'
                self.send_response(200)
                self.send_header("Content-type", "text/unknown")
                self.send_header("Content-Length", len(response))
                self.end_headers()
                self.wfile.write(response)

        server = rephelp.HTTPServerController(Always200Handler)
        secureServer = rephelp.HTTPServerController(Always200Handler, ssl=True)
        try:
            port = server.port
            sslport = secureServer.port
            runTests(opener, port, sslport)

            # Test that setting the proxy environment variable(s) works
            environ = os.environ.copy()
            for k, v in proxies.items():
                k = k + '_proxy'
                environ[k.upper()] = v

            self.mock(os, "environ", environ)

            opener = transport.XMLOpener()
            runTests(opener, port, sslport)

            # Point environment variables to some dummy location
            environ["HTTP_PROXY"] = "http://foo:1"
            environ["HTTPS_PROXY"] = "http://foo:1"

            # make sure explicit config overrides env var
            opener = transport.XMLOpener(proxies = proxies)
            runTests(opener, port, sslport)

            # Shut down the proxy
            h.stop()
            # Adding some timeout, otherwise we see a 503 Service Unavailable
            self.sleep(.1)

            e = self.assertRaises(socket.error, runTests, opener, port,
                    sslport)
            self.assertEqual(e.args, (111,
                "Connection refused (via HTTP proxy http://%s)" % proxyUri))
        finally:
            server.kill()
            secureServer.kill()
            h.stop()

    def testAuthenticatedHTTPProxy(self):
        # CNY-1687
        self.openRepository()
        if not os.path.exists(rephelp.HTTPProxy.proxyBinPath):
            raise testhelp.SkipTestException('testHTTPProxy depends on squid being installed')

        h = rephelp.HTTPProxy(os.path.join(self.workDir, "http-cache"))
        proxyUri = h.start()
        try:
            sock_utils.tryConnect("localhost", h.port)

            # We cannot have an HTTP proxy and a Conary proxy chained together.
            # (we can have a client talking to a Conary proxy that talks to a repo
            # via an HTTP proxy)
            self.cfg.conaryProxy = {}

            # Unauthenticated, should fail
            self.cfg.configLine("proxy http://localhost:%s" %
                                h.authPort)
            client = conaryclient.ConaryClient(self.cfg)
            repos = client.getRepos()
            try:
                versions = repos.c['localhost'].checkVersion()
            except errors.OpenError, e:
                self.assertTrue('407 Proxy Authentication Required' in str(e),
                                str(e))
            else:
                self.fail("Should have failed")

            # Authenticated
            self.cfg.configLine("proxy http://rpath:rpath@localhost:%s" %
                                h.authPort)
            client = conaryclient.ConaryClient(self.cfg)
            repos = client.getRepos()
            versions = repos.c['localhost'].checkVersion()

            # Using a different server for SSL, it's better to not stop the one on
            # slot 0 for caching reasons
            self.stopRepository(serverIdx=2)

            try:
                self.openRepository(serverIdx=2, useSSL=True)
                self.cfg.conaryProxy = {}

                # Unauthenticated, should fail
                self.cfg.configLine("proxy https https://localhost:%s" %
                                    h.authPort)
                client = conaryclient.ConaryClient(self.cfg)
                repos = client.getRepos()
                try:
                    versions = repos.c['localhost2'].checkVersion()
                except errors.OpenError, e:
                    self.assertTrue('407 Proxy Authentication Required' in
                            str(e), str(e))
                else:
                    self.fail("Should have failed")

                # Authenticated
                self.cfg.configLine("proxy https https://rpath:rpath@localhost:%s" %
                                    h.authPort)
                client = conaryclient.ConaryClient(self.cfg)
                repos = client.getRepos()
                versions = repos.c['localhost2'].checkVersion()
            finally:
                self.stopRepository(serverIdx=2)
        finally:
            h.stop()

    def testSSLxmlrpc(self):
        self.openRepository()
        if self.cfg.proxy:
            raise testhelp.SkipTestException('Cannot test squid proxy when another proxy is being used')
        if not os.path.exists(rephelp.HTTPProxy.proxyBinPath):
            raise testhelp.SkipTestException('testSSLxmlrpc depends on squid being installed')

        h = rephelp.HTTPProxy(os.path.join(self.workDir, "http-cache"))
        proxyUri = h.start()

        try:
            h.updateConfig(self.cfg)
            self.cfg.proxy['http'] = self.cfg.proxy['http'].replace('localhost', '127.0.0.1')
            self.cfg.proxy['https'] = self.cfg.proxy['https'].replace('localhost', '127.0.0.1')
            httputils.LocalHosts.remove('127.0.0.1')
            httputils.LocalHosts.remove('localhost')

            self.stopRepository(1)
            self.openRepository(1, useSSL = True)
            try:

                # We cannot have an HTTP proxy and a Conary proxy chained together.
                # (we can have a client talking to a Conary proxy that talks to a repo
                # via an HTTP proxy)
                self.cfg.conaryProxy = {}
                repos = conaryclient.ConaryClient(self.cfg).getRepos()

                logsz0 = h.getAccessLogSize()
                versions = repos.c['localhost1'].checkVersion()
                logEntry = h.getAccessLogEntry(logsz0)
                self.assertTrue(logEntry)
                self.assertTrue('CONNECT' in logEntry, "CONNECT not in %s" % logEntry)

                logsz1 = h.getAccessLogSize()
                versions = repos.c['localhost1'].checkVersion()
                # Wait for a second, make sure the proxy's log file did increase
                self.sleep(1)
                logsz2 = h.getAccessLogSize()
                self.assertFalse(logsz1 == logsz2)
            finally:
                httputils.LocalHosts.add('127.0.0.1')
                httputils.LocalHosts.add('localhost')
                self.stopRepository(1)

        finally:
            h.stop()

    def testProxyBypass(self):
        if not os.path.exists(rephelp.HTTPProxy.proxyBinPath):
            raise testhelp.SkipTestException('testProxyBypass depends on squid being installed')

        class Controller(ServerController):
            class XMLRPCHandler:
                def ping(self):
                    return [True]

            def handlerFactory(self):
                return self.XMLRPCHandler()

        sc = Controller()
        h = None
        oldLocalHosts = httputils.LocalHosts
        try:
            h = rephelp.HTTPProxy(os.path.join(self.workDir, "http-cache"))
            proxyUri = h.start()

            proxyMap = proxy_map.ProxyMap()
            proxyMap.addStrategy('*', ['http://' + proxyUri])

            tsp = transport.Transport(proxyMap=proxyMap)
            sp = util.ServerProxy(url=sc.url(), transport=tsp)

            # Make sure we're proxying
            logsz0 = h.getAccessLogSize()
            self.assertEqual(sp.ping(), ([True],))
            logEntry = h.getAccessLogEntry(logsz0)
            self.assertTrue(logEntry)

            # Now mangle localhosts. Proxy is 127.0.0.1, server is on
            # localhost.
            # Make the proxy appear to be remote

            localhosts = set(httputils.LocalHosts)
            localhosts.remove('127.0.0.1')
            httputils.LocalHosts = localhosts

            logsz0 = h.getAccessLogSize()
            sp.ping()
            self.sleep(1)
            logsz1 = h.getAccessLogSize()

            self.assertEqual(logsz1, logsz0)
        finally:
            httputils.LocalHosts = oldLocalHosts
            sc.kill()
            if h:
                h.stop()

    def testProxyBypassConary(self):
        """Test that no_proxy does not affect conary proxies (CNY-3349)"""
        if not os.path.exists(rephelp.HTTPProxy.proxyBinPath):
            raise testhelp.SkipTestException('testProxyBypass depends on squid being installed')

        class Controller(ServerController):
            class XMLRPCHandler:
                def ping(self):
                    return [True]

            def handlerFactory(self):
                return self.XMLRPCHandler()

        environ = os.environ.copy()
        self.mock(os, 'environ', environ)

        sc = Controller()
        h = None
        oldLocalHosts = httputils.LocalHosts
        try:
            h = rephelp.HTTPProxy(os.path.join(self.workDir, "http-cache"))
            proxyUri = h.start()

            proxyMap = proxy_map.ProxyMap()
            proxyMap.addStrategy('*', ['conary://' + proxyUri])

            tsp = transport.Transport(proxyMap=proxyMap)
            sp = util.ServerProxy(url=sc.url(), transport=tsp)

            # Now mangle localhosts. Proxy is 127.0.0.1, server is on
            # localhost.
            # Make the proxy appear to be remote
            localhosts = set(httputils.LocalHosts)
            localhosts.remove('127.0.0.1')
            httputils.LocalHosts = localhosts

            # Remote proxy, local host. Go direct
            logsz0 = h.getAccessLogSize()
            sp.ping()
            self.sleep(1)
            logsz1 = h.getAccessLogSize()
            self.assertEqual(logsz1, logsz0)

            # Local proxy, remote host. Proxy
            localhosts.add('127.0.0.1')
            localhosts.remove('localhost')

            logsz0 = h.getAccessLogSize()
            sp.ping()
            self.sleep(1)
            logsz1 = h.getAccessLogSize()
            self.assertTrue(logsz1 > logsz0, "Proxy should have been used")

            environ['no_proxy'] = '*'
            # Proxy should still be used, the environemnt variable should
            # not affect the result since it's a Conary proxy
            logsz0 = h.getAccessLogSize()
            sp.ping()
            self.sleep(1)
            logsz1 = h.getAccessLogSize()
            self.assertTrue(logsz1 > logsz0, "Proxy should have been used")
        finally:
            httputils.LocalHosts = oldLocalHosts
            sc.kill()
            if h:
                h.stop()

    def testProxyBypassNoProxy(self):
        environ = os.environ.copy()
        proxyPlain = request.URL("http://user:pass@host.example.com:1234")
        proxyConary = request.URL("conary://user:pass@host.example.com:1234")
        url = request.URL("http://rpath.com")

        # We should not hit the proxy at all
        self.mock(os, 'environ', environ)

        # All flavors of localhost should be direct
        proxyMap = proxy_map.ProxyMap()
        opener = opmod.URLOpener(proxyMap)

        # Normal hosts will proxy through
        self.assertEqual(opener._shouldBypass(url, proxyPlain), False)
        self.assertEqual(opener._shouldBypass(url, proxyConary), False)
        for h in httputils.LocalHosts:
            self.assertEqual(opener._shouldBypass(
                request.URL("http://%s:33" % h), proxyPlain), True)
            self.assertEqual(opener._shouldBypass(
                request.URL("http://%s:33" % h), proxyConary), True)

        # Force direct for everything on HTTP proxies
        environ['no_proxy'] = "*"
        self.assertEqual(opener._shouldBypass(url, proxyPlain), True)
        # environment variable should not affect the selection of conary proxy
        self.assertEqual(opener._shouldBypass(url, proxyConary), False)

        # Test that NO_PROXY is also used, not just no_proxy
        del environ['no_proxy']
        self.assertEqual(opener._shouldBypass(url, proxyPlain), False)
        environ['NO_PROXY'] = "*"
        self.assertEqual(opener._shouldBypass(url, proxyPlain), True)
        self.assertEqual(opener._shouldBypass(url, proxyConary), False)
        # no_proxy takes precedence over NO_PROXY
        environ['no_proxy'] = "host.com"
        self.assertEqual(opener._shouldBypass(url, proxyPlain), False)
        self.assertEqual(opener._shouldBypass(
            request.URL("http://host.com"), proxyPlain), True)
        # http://lynx.isc.org/lynx2.8.5/lynx2-8-5/lynx_help/keystrokes/environments.html - comma-separated list of domains (with a trailing, to force empty domain)
        environ['no_proxy'] = "host.domain.dom, domain1.dom, domain2, "
        tests = [
            ('rpath.com:80', False),
            ('domain.com:80', False),
            ('host.domain.dom:80', True),
            ('hosthost.domain.dom:80', True),
            ('sub.host.domain.dom:80', True),
            ('www.domain1.dom:80', True),
            # Hmm. We will assume the domain is qualified up to ., the domain2
            # example probably assumes it matches any domain2 in the domain
            # search path, but we don't handle that.
            ('domain2:80', True),
            ('www.domain2:80', True),
        ]
        for h, expected in tests:
            self.assertEqual(opener._shouldBypass(
                request.URL('http://' + h), proxyPlain), expected)

    def testGetIPAddress(self):
        def gethostbyname(x):
            raise socket.gaierror()
        httputils.IPCache.clear()
        httputils.IPCache._cache.set('somehost', '1.1.1.1')
        self.mock(socket, 'gethostbyname', gethostbyname)
        self.assertEqual(httputils.IPCache._cache.get('somehost'),
            '1.1.1.1')
        self.assertRaises(socket.gaierror,
            httputils.IPCache.get, 'someotherhost')

    def _testSSLCertCheck(self, keyPair=None):
        httpServer = rephelp.HTTPServerController(RequestHandler200, ssl=keyPair)
        try:
            caPath = os.path.join(resources.get_archive(), 'ssl-cert-authority.pem')
            opener = transport.URLOpener(caCerts=[caPath])
            opener.open("https://localhost:%s/someurl" % httpServer.port)

        finally:
            httpServer.kill()

    def testSSLCertCheck(self):
        # Good (signed by a trusted CA)
        self._testSSLCertCheck(('ssl-cert.crt', 'ssl-cert.key'))

        # Bad (self-signed)
        self.assertRaises(SSL.SSLError, self._testSSLCertCheck,
                ('ssl-self-signed.pem', 'ssl-self-signed.pem'))


class SimpleXMLRPCServer6(SimpleXMLRPCServer):
    address_family = socket.AF_INET6


class ServerController:
    def __init__(self):
        self.port = testhelp.findPorts(num = 1)[0]
        self.childPid = os.fork()
        if self.childPid > 0:
            sock_utils.tryConnect('127.0.0.1', self.port)
            return

        server = SimpleXMLRPCServer6(("::", self.port), logRequests=False)
        server.register_instance(self.handlerFactory())
        server.serve_forever()

    def handlerFactory(self):
        raise NotImplementedError

    def kill(self):
        if not self.childPid:
            return
        os.kill(self.childPid, 15)
        os.waitpid(self.childPid, 0)
        self.childPid = 0

    def url(self):
        return "http://localhost:%d/" % self.port

    def __del__(self):
        self.kill()


class RequestHandler404(SimpleHTTPServer.SimpleHTTPRequestHandler):
    code = 404

    def log_message(self, *args, **kw):
        pass

    def do_GET(self):
        self.send_response(self.code)
        self.end_headers()


class RequestHandler200(RequestHandler404):
    code = 200
