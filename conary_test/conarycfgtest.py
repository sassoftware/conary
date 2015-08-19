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
import BaseHTTPServer
import copy
import os
import Queue
import shutil
import socket
import sys
from StringIO import StringIO
import tempfile
import threading
import pwd
import warnings


#testsuite
import conary_test
from conary_test import rephelp

#conary
from conary import conarycfg
from conary import conaryclient
from conary import checkin
from conary import errors
from conary import flavorcfg
from conary import versions
from conary.deps import deps
from conary.lib import httputils
from conary.lib import networking
from conary.lib import util
from conary.lib.http import connection
from conary.lib.http import request
URL = request.URL


class ConaryCfgTest(rephelp.RepositoryHelper):

    #topDir = "/tmp/test"
    #cleanupDir = 0

    def testConaryCfgOptions(self):
        configfile = """
repositoryMap host.somewhere.com http://someotherhost.com:port/loc
proxyMap host.somewhere.com http://someotherhost.com:123/loc
installLabelPath conary.rpath.com@rpl:1 contrib.rpath.org@rpl:1
macros a  a
macros b  b b
macros c
flavor use:ssl,krb,readline,\
       bootstrap is: x86(i486,i586,i686,mmx) 
"""
        dir = tempfile.mkdtemp()
        cwd = os.getcwd()
        os.chdir(dir)
        f = open('foo', 'w')
        f.write(configfile)
        f.close()
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.read(dir + '/foo')
        assert(cfg.buildPath) # make sure defaults are working
        assert(cfg['macros']['a'] == 'a')
        assert(cfg['macros']['b'] == 'b b')
        assert(cfg['macros']['c'] == '')
        assert(cfg['repositoryMap']['host.somewhere.com']
                == 'http://someotherhost.com:port/loc')

        filter, targets = cfg.proxyMap.filterList[0]
        self.assertEqual(filter.address,
                networking.HostPort('host.somewhere.com'))
        self.assertEqual(targets,
                [request.URL('http://someotherhost.com:123/loc')])

        ilp = cfg['installLabelPath']
        l0 = versions.Label('conary.rpath.com@rpl:1')
        l1 = versions.Label('contrib.rpath.org@rpl:1')
        lOther = versions.Label('contrib.rpath.org@rpl:devel')
        assert(ilp == [ l0, l1 ] )

        assert(ilp.priority(l0, l1) == -1)
        assert(ilp.priority(l0, l0) == 0)
        assert(ilp.priority(l1, l0) == 1)
        assert(ilp.priority(lOther, l0) is None)
        assert(ilp.priority(lOther, lOther) is None)
        assert(ilp.priority(l0, lOther) is None)

        v0 = versions.VersionFromString('/conary.rpath.com@rpl:1/1.0-1-1')
        v1 = versions.VersionFromString('/contrib.rpath.org@rpl:1/1.0-1-1')
        assert(ilp.versionPriority(v0, v1) == -1)
        assert(ilp.versionPriority(v0, v0) == 0)
        assert(ilp.versionPriority(v1, v0) == 1)

        assert(str(cfg.flavor[0]) == 'bootstrap,krb,readline,ssl is: x86(i486,i586,i686,mmx)')
        out = StringIO()
        cfg.displayKey('flavor', out)
        assert(out.getvalue().split(None, 1)[1] == 'bootstrap, krb, readline, ssl is: x86(i486, i586, i686, mmx)\n')
        out.close()
        out = StringIO()
        cfg.setDisplayOptions(prettyPrint=True)
        cfg.displayKey('flavor', out)
        assert(out.getvalue().split(None, 1)[1] == 'bootstrap, krb, readline, ssl is: x86(i486,\n                          i586, i686, mmx)\n')
        out.close()
        out = StringIO()
        cfg.displayKey('repositoryMap', out)
        assert(out.getvalue() == 'repositoryMap             host.somewhere.com        http://someotherhost.com:port/loc\n')
        out.close()
        out = StringIO()
        cfg.displayKey('proxyMap', out)
        self.assertEqual(out.getvalue(),
            "proxyMap                  host.somewhere.com "
            "http://someotherhost.com:123/loc\n")
        out.close()
        cfg.setDisplayOptions(hidePasswords=True)
        out = StringIO()
        cfg.displayKey('repositoryMap', out)
        assert(out.getvalue() == 'repositoryMap             host.somewhere.com        http://someotherhost.com:port/loc\n')
        out.close()

        keys = cfg.macros.keys()
        keys.sort()
        assert(keys == ['a', 'b', 'c'])
        os.remove('foo')
        foo = open('foo', 'w')
        cfg.display(foo)
        foo.close()
        lines = [x.split() for x in open('foo', 'r')]
        assert(['macros', 'a', 'a'] in lines)
        assert(['macros', 'c'] in lines)
        os.remove('foo')
        os.chdir(cwd)
        os.rmdir(dir)

    def testPathAlwaysAbsolute(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine('root tmp')
        assert(cfg.root == os.getcwd() + '/tmp')
        rc, txt = self.captureOutput(cfg.displayKey, 'root')
        assert(txt == 'root                      tmp\n')

    def testReload(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.repositoryMap['a'] = 'foo'
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        assert(dict(cfg.repositoryMap) == {})

    def testUseDirectory(self):
        def testOne(dir, flagList, emptyPreferred = True):
            files = os.listdir(dir)
            for fileName in files:
                os.unlink(os.path.join(dir, fileName))

            for (flag, sense) in flagList:
                f = open(os.path.join(dir, flag), "w")
                if sense == deps.FLAG_SENSE_PREFERRED:
                    if not emptyPreferred:
                        f.write('sense preferred\n')
                elif sense == deps.FLAG_SENSE_PREFERNOT:
                    f.write('sense prefernot\n')
                elif sense == deps.FLAG_SENSE_DISALLOWED:
                    f.write('sense disallowed\n')
                elif sense == deps.FLAG_SENSE_REQUIRED:
                    f.write('sense required\n')
                else:
                    assert(0)

                f.close()

            use = flavorcfg.FlavorConfig(dir, '')
            cmp = deps.Flavor()
            cmp.addDep(deps.UseDependency, deps.Dependency('use', flagList))
            assert(use.toDependency() == cmp)

        dir = tempfile.mkdtemp()
        flags = [ ( 'on', deps.FLAG_SENSE_PREFERRED ),
                  ( 'off', deps.FLAG_SENSE_PREFERNOT ),
                  ( 'req', deps.FLAG_SENSE_REQUIRED ),
                  ( 'no', deps.FLAG_SENSE_DISALLOWED ) ]
        try:
            testOne(dir, flags, emptyPreferred = True)
            testOne(dir, flags, emptyPreferred = False)
        finally:
            shutil.rmtree(dir)

    def testSignatureKeyOptions(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine("signatureKey")
        if (cfg.signatureKey is not None):
            self.fail("signatureKey line with nothing following didn't return None")
        cfg.configLine("signatureKey A_FINGERPRINT #item")
        if (cfg.signatureKey is None):
            self.fail("signatureKey line illegally returned None")
        assert(cfg.signatureKey == 'A_FINGERPRINT')

        cfg.configLine("signatureKey None")
        if (cfg.signatureKey is not None):
            self.fail("signatureKey line didn't return None when explicitly specified")

        cfg.configLine("signatureKeyMap label_1")
        cfg.configLine("signatureKeyMap label_2 None")
        for label,fingerprint in cfg.signatureKeyMap:
            if (fingerprint is not None):
                self.fail("signatureKeyMap for label %s didn't return None" %label)
        out = StringIO()
        cfg.displayKey('signatureKeyMap', out)
        assert(out.getvalue() == 'signatureKeyMap           label_1 None\nsignatureKeyMap           label_2 None\n')

        cfg.configLine("signatureKeyMap label_2 A_FINGERPRINT")
        cfg.configLine("signatureKey")
        if(cfg.signatureKeyMap):
            self.fail("assigning a signatureKey did not reset signatureKeyMap")

    def testIncludeConfigFileRecursiveLoop(self):
        # CNY-914: inclinding a config file that includes the original one
        # results in a loop
        fd1, configfile1 = tempfile.mkstemp()
        fd2, configfile2 = tempfile.mkstemp()

        f1 = os.fdopen(fd1, "w+")
        f2 = os.fdopen(fd2, "w+")

        f1.write("includeConfigFile %s\n" % configfile2)
        f2.write("includeConfigFile %s\n" % configfile1)

        f1.close()
        f2.close()

        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        try:
            cfg.read(configfile1)
        except conarycfg.ParseError:
            self.fail("Recursive load of configuration file")

        os.unlink(configfile1)
        os.unlink(configfile2)

    def testIncludeConfigFileStableOrder(self):
        # CNY-2483
        cfgdir = os.path.join(self.workDir, "cfgdir")
        os.mkdir(cfgdir)
        cfg = os.path.join(self.workDir, "cfg")
        cfg1 = os.path.join(cfgdir, "cfg1")
        cfg2 = os.path.join(cfgdir, "cfg2")

        # Make sure we create cfg1 after cfg2 - we have higher chances of glob
        # sorting them by creation time
        self.writeFile(cfg2, "installLabelPath A@B:C\n")
        self.writeFile(cfg1, "installLabelPath a@b:c\n")

        self.writeFile(cfg, "includeConfigFile %s/*\n" % cfgdir)
        c = conarycfg.ConaryConfiguration(readConfigFiles=False)

        obc = util.braceGlob
        def mockBraceGlob(val):
            return reversed(obc(val))
        self.mock(util, 'braceGlob', mockBraceGlob)

        c.read(cfg)
        self.assertEqual(len(c.installLabelPath), 1)
        self.assertEqual(str(c.installLabelPath[0]), "A@B:C")

    def testIncludeConfigFileNoLoop(self):
        # CNY-914: including the same file in two contexts
        fd, configfile = tempfile.mkstemp()
        fd1, configfile1 = tempfile.mkstemp()
        fd2, configfile2 = tempfile.mkstemp()
        fd3, configfile3 = tempfile.mkstemp()

        f = os.fdopen(fd, "w+")
        f1 = os.fdopen(fd1, "w+")
        f2 = os.fdopen(fd2, "w+")
        f3 = os.fdopen(fd3, "w+")

        f.write("includeConfigFile %s\n" % configfile1)
        f.write("includeConfigFile %s\n" % configfile2)

        f1.write("[sect1]\n")
        f1.write("buildLabel c.example.com@foo:1\n")
        f1.write("includeConfigFile %s\n" % configfile3)

        f2.write("[sect2]\n")
        f2.write("buildLabel c.example.com@foo:2\n")
        f2.write("includeConfigFile %s\n" % configfile3)

        f3.write("lookaside /tmp/foobar\n")

        for fobj in (f, f1, f2, f3):
            fobj.close()

        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.read(configfile)
        
        expected = {
            'sect1' : {
                'buildLabel'        : versions.Label('c.example.com@foo:1'),
                'lookaside'         : '/tmp/foobar',
            },
            'sect2' : {
                'buildLabel'        : versions.Label('c.example.com@foo:2'),
                'lookaside'         : '/tmp/foobar',
            }
        }
        for sectName in cfg.iterSectionNames():
            self.assertTrue(sectName in expected)
            sect = cfg.getSection(sectName)
            expSect = expected[sectName]
            for k in ['buildLabel', 'lookaside']:
                self.assertEqual(sect[k], expSect[k])
            del expected[sectName]

        # More sections?
        self.assertFalse(expected)

        for cf in [configfile, configfile1, configfile2, configfile3]:
            os.unlink(cf)

    def testIncludeUnreachableNetworkConfigFile(self):
        configUrl = "http://10.1.1.1/conaryrc"
        fobj = tempfile.NamedTemporaryFile()
        print >> fobj, "includeConfigFile", configUrl
        fobj.flush()
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)

        def _getOpener():
            opener = type(cfg)._getOpener(cfg)
            opener.connectAttempts = 1
            return opener
        cfg._getOpener = _getOpener
        def connectSocket(self):
            raise socket.timeout('timed out')
        oldfn = connection.Connection.connectSocket
        oldStderr = sys.stderr
        connection.Connection.connectSocket = connectSocket
        try:
            sys.stderr = StringIO()
            try:
                cfg.read(fobj.name)
            except conarycfg.ParseError, e:
                assert 'timed out' in str(e) or 'timeout' in str(e)
            else:
                self.fail('expected ParseError')
        finally:
            connection.Connection.connectSocket = oldfn
            sys.stderr = oldStderr
        # Make sure we cleaned up the timeout
        self.assertEqual(socket.getdefaulttimeout(), None)

    def testBraceGlobSorted(self):
        confDir = util.joinPaths(self.workDir, "config-dir")
        util.mkdirChain(confDir)
        
        topCfg = util.joinPaths(self.workDir, "some-config")
        open(topCfg, "w").write("includeConfigFile %s/*\n" % confDir)

        cfgfn1 = util.joinPaths(confDir, "config1")
        cfgfn2 = util.joinPaths(confDir, "config2")
        open(cfgfn1, "w").write("name Name1\n")
        open(cfgfn2, "w").write("name Name2\n")

        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        # Mock util.braceExpand to return items in reverse order

        def mockBraceExpand(path):
            return [cfgfn2, cfgfn1]

        self.mock(util, 'braceExpand', mockBraceExpand)
        cfg.read(topCfg)
        self.unmock()
        self.assertEqual(cfg.name, "Name2")

    def testUnreadableEntitlementsDirectory(self):
        configFile = util.joinPaths(self.workDir, "config-unreadable-ent")
        entDir = util.joinPaths(self.workDir, "ent-directory")
        os.mkdir(entDir)
        unreadableFile = util.joinPaths(entDir, "unreadable")
        open(unreadableFile, "w")
        try:
            os.chmod(unreadableFile, 0)
            cfgfile = open(configFile, "w")
            cfgfile.write("""
entitlementDirectory %s
    """ % entDir)
            cfgfile.close()

            cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)

            cfg.read(configFile)
            cfg.readEntitlementDirectory()
        finally:
            os.chmod(unreadableFile, 0644)

    def testProxyUsedForGettingIncludeConfigFile(self):
        # CNY-2363
        if not os.path.exists(rephelp.HTTPProxy.proxyBinPath):
            raise testhelp.SkipTestException('testHTTPProxy depends on squid being installed')

        h = rephelp.HTTPProxy(os.path.join(self.workDir, "http-cache"))
        proxyUri = h.start()

        #NOTE: this web server only serves one time - if we try to access
        # it twice, it won't work, so that's another test.
        srvThread, port, queue = startServer()
        val = queue.get(block=True)
        self.assertEqual(val, "started")
        serverUrl = "http://127.0.0.1:%s/" % port

        configFile = self.workDir + '/foo'
        self.writeFile(configFile, '''
includeConfigFile %s
proxy http http://%s
''' % (serverUrl, proxyUri))


        calls = [0]
        def readFiles(self):
            calls[0] += 1
            if calls[0] == 1:
                assert(not self.proxy)
            elif calls[0] == 2:
                # the proxy is there when we read the files the second time
                assert(self.proxy == {'http' : 'http://%s' % proxyUri})
            self.read(configFile)

        # replace readfiles so that we only read our file.
        oldReadFiles = conarycfg.ConaryConfiguration.readFiles
        try:
            logsz0 = h.getAccessLogSize()
            conarycfg.ConaryConfiguration.readFiles = readFiles
            cfg = conarycfg.ConaryConfiguration(readConfigFiles=True)
            logEntry = h.getAccessLogEntry(logsz0)
            self.assertEqual(logEntry[5:7], ['GET', serverUrl])
        finally:
            conarycfg.ConaryConfiguration.readFiles = oldReadFiles

    def testPubringForRoot(self):
        rootKeyrings =  ['/root/.gnupg/pubring.gpg', '/etc/conary/pubring.gpg']
        homeDir = pwd.getpwuid(os.getuid())[5]
        nonRootKeyrings =  [ os.path.join(homeDir, '.gnupg', 'pubring.gpg'),
            '/etc/conary/pubring.gpg' ]
        noHomeDirKeyrings = [ '/etc/conary/pubring.gpg']

        pks = conarycfg._getDefaultPublicKeyrings()
        self.assertEqual(pks, nonRootKeyrings)

        # CNY-2630, CNY-2722
        mockGetuid = lambda: 0
        self.mock(os, "getuid", mockGetuid)
        pks = conarycfg._getDefaultPublicKeyrings()
        self.assertEqual(pks, rootKeyrings)
        self.unmock()

        nenv = os.environ.copy()
        del nenv['HOME']
        self.mock(os, "environ", nenv)

        # Now try with a non-root user with no HOME set

        mockGetpwuid = lambda x: (None, None, None, None, None, '/h2')
        self.mock(pwd, "getpwuid", mockGetpwuid)
        pks = conarycfg._getDefaultPublicKeyrings()
        self.assertEqual(pks, noHomeDirKeyrings)

        self.unmock()

        def mockGetpwuid(x):
            raise KeyError(x)
        self.mock(pwd, "getpwuid", mockGetpwuid)
        pks = conarycfg._getDefaultPublicKeyrings()
        self.assertEqual(pks, noHomeDirKeyrings)

class ConaryTypesTest(rephelp.RepositoryHelper):
    def testUserInfo(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.user.addServerGlob('name*', 'user', 'passwd')
        cfg.configLine('user foobar a')
        self.assertEqual(cfg.user.find('namefoo'), ('user', 'passwd'))
        self.assertEqual(cfg.user.find('foobar'), ('a', None))

        out = StringIO()
        cfg.setDisplayOptions(hidePasswords=True)
        cfg.displayKey('user', out)
        assert(out.getvalue() == """\
user                      foobar a
user                      name* user <password>
""")
        out = StringIO()
        cfg.setDisplayOptions(hidePasswords=False)
        cfg.displayKey('user', out)
        assert(out.getvalue() == """\
user                      foobar a
user                      name* user passwd
""")


        assert(cfg.user.find('nameone') == ('user', 'passwd'))
        cfg.user.addServerGlob('nameone', 'user1', 'passwd1')
        assert(cfg.user.find('nameone') == ('user1', 'passwd1'))

        # CNY-1267
        try:
            cfg.configLine('user conary.rpath.com "Michael Jordon" a')
        except conarycfg.ParseError, e:
            self.assertEqual(str(e), "override:<No line>: expected <hostglob> <user> [<password>] for configuration item 'user'")

        # test iterator
        l = [ x for x in cfg.user ]
        self.assertEqual(l, [
            ('foobar', 'a', None),
            ('nameone', 'user1', 'passwd1'),
            ('name*', 'user', 'passwd'),
            ])

    def testProxy(self):
        def _test(*args):
            cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
            for l in args:
                cfg.configLine(l)
            return cfg

        cfg = _test('proxy http://foo.com/')
        assert(cfg.proxy['http'] == 'http://foo.com/')
        assert(cfg.proxy['https'] == 'https://foo.com/')

        # CNY-1586
        cfg = _test('proxy https http://foo.com/', 'proxy http://bar.com/')
        self.assertEqual(cfg.proxy,
            {'http' : 'http://bar.com/', 'https' : 'https://bar.com/'})

        cfg = _test('proxy http http://foo.com/',
                    'proxy https https://foo.com/')
        assert(cfg.proxy['http'] == 'http://foo.com/')
        assert(cfg.proxy['https'] == 'https://foo.com/')

        # CNY-1378 - None should override the proxy settings
        cfg = _test('proxy http://a/', 'proxy None')
        self.assertEqual(cfg.proxy, {})

        cfg = _test('proxy http://a/', 'proxy http None')
        self.assertEqual(cfg.proxy, {'https' : 'https://a/'})

    def testRepositoryMap(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine('repositorymap a.b.c http://a.b.c:8000/conary/')
        self.assertRaises(conarycfg.ParseError,
           cfg.configLine, 'repositorymap a.b.c http://u:p@a.b.c:8000/conary/')
        self.assertRaises(conarycfg.ParseError,
           cfg.configLine, 'repositorymap a.b.c http://u:p@a.b.c/conary/')
        cfg.configLine('repositorymap * http://other/conary/')

        assert(cfg.repositoryMap['a.b.c'] == 'http://a.b.c:8000/conary/')
        assert(cfg.repositoryMap['foo.org'] == 'http://other/conary/')

    def assertUrlSetsEqual(self, actual, expected):
        errs = []
        for url in expected - actual:
            errs.append("Expected URL to match but it didn't: " + str(url))
        for url in actual - expected:
            errs.append("Expected URL NOT to match but it did: " + str(url))
        if errs:
            raise AssertionError("\n".join(errs))

    def testProxyMap(self):
        # Mock IPCache so no actual name resolution is done.
        ipcache = httputils.IPCache._cache.__class__()
        def _resolve(host):
            # Don't let DNS requests leak out
            self.fail("Host not mocked: %s" % host)
        self.mock(httputils.IPCache, '_resolve', _resolve)
        self.mock(httputils.IPCache, '_cache', ipcache)

        ipcache.set('example.one', ['10.1.1.1'])
        ipcache.set('example.two', ['10.1.1.2'])

        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)

        # Test clearing the mapping
        cfg.configLine(
            'proxyMap repo1.example.com rus1.example.com rus2.example.com')
        cfg.configLine(
            'proxyMap repo3.example.com rus1.example.com rus4.example.com')
        cfg.configLine(
            'proxyMap repo5.example.com rus3.example.com rus6.example.com')
        self.assertEqual(len(cfg.proxyMap.filterList), 3)

        cfg.configLine('proxyMap []')
        self.assertEqual(cfg.proxyMap.filterList, [])

        # Matching a hostname
        cfg.configLine('proxyMap []')
        cfg.configLine('proxyMap example.one good1.com good2.net')
        cfg.configLine('proxyMap example.two bad.com bad.net')
        cfg.configLine('proxyMap ex*.one:123 good3.com')
        cfg.configLine('proxyMap ex*.two:123 bad.com')
        cfg.configLine('proxyMap 10.0.0.0/8 good4.com conary://wrong.protocol')
        cfg.configLine('proxyMap 11.0.0.0/8 bad.com')
        cfg.configLine('proxyMap 10.1.1.1:123 good5.com')
        cfg.configLine('proxyMap 10.1.1.1/32 good6.com')
        cfg.configLine('proxyMap 10.1.1.2:123 bad.com')
        cfg.configLine('proxyMap 10.1.1.2/32 bad.com')
        actual = set(cfg.proxyMap.getProxyIter(
            'http://example.one:123/bar/baz.zip'))
        expected = set([
                    URL('good1.com'), URL('good2.net'),
                    URL('good3.com'),
                    URL('good4.com'),
                    URL('good5.com'),
                    URL('good6.com'),
                    ])
        self.assertUrlSetsEqual(actual, expected)

        # deepcopy just to make sure proxyMaps are copiable
        cfg = copy.deepcopy(cfg)

        # Blacklist one of the targets and try again.
        cfg.proxyMap.blacklistUrl(URL('good2.net'))
        actual = set(cfg.proxyMap.getProxyIter(
            'http://example.one:123/bar/baz.zip'))
        expected = set([
                    URL('good1.com'),
                    URL('good3.com'),
                    URL('good4.com'),
                    URL('good5.com'),
                    URL('good6.com'),
                    ])

        # Matching an IP
        actual = set(cfg.proxyMap.getProxyIter(
            'http://10.1.1.1:123/bar/baz.zip'))
        expected = set([
                    URL('good1.com'),
                    URL('good4.com'),
                    URL('good5.com'),
                    URL('good6.com'),
                    ])
        self.assertUrlSetsEqual(actual, expected)

    def testGetProxyMap(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine("conaryproxy http http://localhost:123")
        cfg.configLine("conaryproxy https http://localhost:123")
        pm = cfg.getProxyMap()
        # getProxyMap() does not return proxyMap in this case - it's a new
        # proxyMap object
        self.assertFalse(pm is cfg.proxyMap)
        got = dict((x[0].protocol, (x[0].address, x[1])) for x in pm.filterList)
        self.assertEquals(got, {
            'http': (networking.HostPort('*'),
                [request.URL('conary://localhost:123')]),
            'https': (networking.HostPort('*'),
                [request.URL('conary://localhost:123')]),
            })

    def testProxyMapEquality(self):
        proxySpecs = [
            ('1.1.1.1', 'proxy1.example.com proxy2.example.com'),
            ('1.2.3.4', 'direct'),
        ]
        cfg1 = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg2 = conarycfg.ConaryConfiguration(readConfigFiles=False)
        for cfg in [ cfg1, cfg2 ]:
            cfg.configLine("proxyMap []")
            for host, proxy in proxySpecs:
                cfg.configLine("proxyMap %s %s" % (host, proxy))
        pm1 = cfg1.getProxyMap()
        pm2 = cfg2.getProxyMap()
        self.assertEqual(pm1, pm2)

        del pm1.filterList[0]
        self.assertFalse(pm1 == pm2)

    def testProxyMapDirect(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine("proxyMap []")
        cfg.configLine("proxyMap 1.1.1.1 direct")
        pm = cfg.getProxyMap()
        from conary.lib.http import proxy_map
        self.assertEquals([ x for x in pm.getProxyIter('1.1.1.1') ],
            [ proxy_map.DirectConnection ])

    def testDependencyClassList(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine('ignoreDependencies perl php')
        self.assertEquals(set(cfg.ignoreDependencies),
                          set((deps.PerlDependencies, deps.PhpDependencies)) )
        try:
            cfg.configLine('ignoreDependencies foo')
            assert(0)
        except conarycfg.ParseError, e:
            self.assertEquals(str(e), "override:<No line>: unknown dependency "
                    "class: foo for configuration item 'ignoreDependencies'")

    def testFingerprintMap(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine('signatureKeyMap local@rpl:devel 00000')
        out = StringIO()
        cfg.displayKey('signatureKeyMap', out)
        assert(out.getvalue() == 'signatureKeyMap           local@rpl:devel 00000\n')

    def testServerGlob(self):
        # CNY-2083
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        l = []
        for i in range(10000):
            l.append(("server-%05d" % i, ('name-%05d' % i, 'pass')))
        cfg.user.addServerGlobs(l)

        cfg.user.clear()
        l = [
            ('*-commits', ('c', 'pass')),
            ('c*', ('c', 'pass')),
            ('conary-commits', ('conary-commits', 'pass')),
            ('conary-commits', ('conary-commits2', 'pass')),
            ('conary-*', ('conary-', 'pass')),
            ('conary-commits', ('conary-commits3', 'pass')),
        ]
        cfg.user.addServerGlobs(l)
        target = [
                ('conary-commits', 'conary-commits', 'pass'),
                ('conary-*', 'conary-', 'pass'),
                ('c*', 'c', 'pass'),
                ('*-commits', 'c', 'pass'),
            ]

        self.assertEqual(list(cfg.user), target)

        # Now verify that addServerGlob does the same thing
        cfg.user.clear()

        # extend reverses the list, so to achieve the same effect we have to
        # process l in reverse order too
        for item in reversed(l):
            cfg.user.addServerGlob(*item)
        self.assertEqual(list(cfg.user), target)


class EntitlementTest(testhelp.TestCase):

    @testhelp.context('entitlements')
    def testFileLoads(self):
        def _doTest(cfg, server, content, value):
            fullPath = os.path.join(cfg.entitlementDir, server)
            open(fullPath, "w").write(content)
            os.chmod(fullPath, 0644)
            rc = conarycfg.loadEntitlement(cfg.entitlementDir, server)
            assert(rc == value)

            os.unlink(fullPath)

            f = open(fullPath, "w")
            f.write("#!/bin/bash\n")
            f.write("cat <<EOFEOF\n")
            f.write(content)
            f.write("EOFEOF\n")
            f.close()
            os.chmod(fullPath, 0755)

            rc = conarycfg.loadEntitlement(cfg.entitlementDir, server)
            assert(rc == value)

        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.entitlementDir = tempfile.mkdtemp()

        try:
            # test full-blown generated version
            generatedContent = conarycfg.emitEntitlement('localhost', 'customer', 'ABCD01234')
            _doTest(cfg, 'localhost', generatedContent,
                    ('localhost', 'customer', 'ABCD01234'))

            # test hand-written file with entitlement tags
            _doTest(cfg, 'localhost',
                        "<entitlement>"
                        "<server>localhost</server><class>customer</class>"
                        "<key>ABCD01234</key>\n"
                        "</entitlement>\n",
                        ('localhost', 'customer', 'ABCD01234'))

            # test hand-written file without entitlement tags
            _doTest(cfg, 'somehost',
                        "<server>somehost</server><class>customer</class>"
                                            "<key>ABCD01234</key>\n",
                        ('somehost', 'customer', 'ABCD01234'))

            # make sure a globbed entitlements gets loaded properly
            xml = conarycfg.emitEntitlement('*', key = 'ABCD01234')
            _doTest(cfg, 'localhost', xml, ('*', None, 'ABCD01234'))
        finally:
            shutil.rmtree(cfg.entitlementDir)

    @testhelp.context('entitlements')
    def testParsing(self):
        withoutClass = ('<entitlement><server>localhost</server>'
                        '<key>ABCD01234</key></entitlement>\n')
        assert(conarycfg.loadEntitlementFromString(withoutClass)
                == ('localhost', None, 'ABCD01234'))

        withoutClass = conarycfg.emitEntitlement('localhost', key = 'ABCD01234')
        assert(conarycfg.loadEntitlementFromString(withoutClass)
                == ('localhost', None, 'ABCD01234'))

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            conarycfg.loadEntitlementFromString(withoutClass, 'localhost', '<foo>')
            self.assertIn('The serverName argument to loadEntitlementFromString has been deprecated', w[-1].message)

    @testhelp.context('entitlements')
    def testFailedEntitlements(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)

        def _test(content, execable=False):
            fullPath = os.path.join(cfg.entitlementDir, 'localhost')
            open(fullPath, "w").write(content)
            if execable:
                os.chmod(fullPath, 0755)
            else:
                os.chmod(fullPath, 0644)

            try:
                conarycfg.loadEntitlement(cfg.entitlementDir, 'localhost')
            except errors.ConaryError, err:
                return str(err)
            self.fail('load should have failed!') # none of these should succeed

        d = tempfile.mkdtemp()
        cfg.entitlementDir = d
        try:
            unparsable = '<server><'
            self.assertEquals(_test(unparsable), 'Malformed entitlement at %s/localhost: not well-formed (invalid token): line 1, column 22' % d)
            self.assertEquals(_test(unparsable, True).split('\n')[0], 'Entitlement generator at "%s/localhost" died with exit status 1 - stderr output follows:' % d)

            scriptTemplate = """#!/bin/bash
cat <<EOFEOF
%s
EOFEOF
    """
            scriptUnparsable = scriptTemplate % unparsable
            self.assertEquals(_test(scriptUnparsable, True), 'Malformed entitlement at %s/localhost: not well-formed (invalid token): line 1, column 22' % d)

            exitCode = ((scriptTemplate % unparsable)
                        + "echo 'foo' > /dev/stderr\nexit 22")
            self.assertEquals(_test(exitCode, True), 'Entitlement generator at "%s/localhost" died with exit status 22 - stderr output follows:\nfoo\n' % d)
            exitSignal = """#!/usr/bin/python
import os, sys
print  >>sys.stderr, "foo\\n"
os.kill(os.getpid(), 9)
"""
            self.assertEquals(_test(exitSignal, True), 'Entitlement generator at "%s/localhost" died with signal 9 - stderr output follows:\nfoo\n\n' % d)
        finally:
            shutil.rmtree(cfg.entitlementDir)

    def testPrintEntitlement(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine("entitlement a b")
        sio = StringIO()
        cfg.storeKey('entitlement', sio)
        # Drop extra white spaces
        line = ' '.join(x for x in sio.getvalue().strip().split() if x)
        self.assertEqual(line, 'entitlement a b')

    def testSetEntitlementWithClass(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine("entitlement a b c")
        sio = StringIO()
        cfg.storeKey('entitlement', sio)
        line = ' '.join(x for x in sio.getvalue().strip().split() if x)
        self.assertEqual(line, 'entitlement a b c')

    def testEntitlemntCfgObject(self):
        cfg = conarycfg.EntitlementList
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine("entitlement some.host.com MAGIC")
        cfg.configLine("entitlement *.com MAGIC2")
        assert(cfg.entitlement.find('some.host.com') ==
                    [ (None, 'MAGIC'), (None, 'MAGIC2') ] )

        assert(cfg.entitlement.find('another.host.com') ==
                    [ (None, 'MAGIC2') ] )

        cfg.configLine("entitlement some.host.com ANOTHER")
        assert(cfg.entitlement.find('some.host.com') ==
                    [ (None, 'MAGIC'), (None, 'ANOTHER'), (None, 'MAGIC2') ] )

    def testTimeoutParsing(self):
        xml = conarycfg.emitEntitlement('server', key = 'ABCD01234',
                                        retryOnTimeout = True, timeout = 60)
        ent = conarycfg.loadEntitlementFromString(xml, returnTimeout = True)
        assert(ent[3] == 60 and ent[4] is True)

        xml = conarycfg.emitEntitlement('server', key = 'ABCD01234')
        ent = conarycfg.loadEntitlementFromString(xml, returnTimeout = True)
        assert(ent[3] is None and ent[4] is True)

        xml = conarycfg.emitEntitlement('server', key = 'ABCD01234',
                                        timeout = 30)
        ent = conarycfg.loadEntitlementFromString(xml, returnTimeout = True)
        assert(ent[3] == 30 and ent[4] is True)

class ContextTest(testhelp.TestCase):

    @conary_test.installed_conarydb
    def testNonExistantContext(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        client = conaryclient.ConaryClient(cfg)
        repos = client.getRepos()
        self.logCheck(checkin.setContext, [cfg, 'nonesuch'],
                      'error: context nonesuch does not exist')


    def testContextOptions(self):
        configfile = """
showLabels True
repositoryMap conary.rpath.com http://localhost/conary/
user conary.rpath.com dbc 
user * foo
[foo]
showLabels False
repositoryMap foo.rpath.org http://localhost:1/conary/
signatureKey None
user conary.rpath.com bar
"""
        tdir = tempfile.mkdtemp()
        cwd = os.getcwd()
        os.chdir(tdir)
        try:
            f = open('foo', 'w')
            f.write(configfile)
            f.close()
            cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
            cfg.read(tdir + '/foo')
            assert(cfg.showLabels)
            cfg.setContext('foo')
            assert(not cfg.showLabels)
            assert(cfg.signatureKey is None)
            rc, txt = self.captureOutput(cfg.displayKey, 'user')
            assert(txt == '''\
user                      conary.rpath.com bar
user                      * foo
''')
            rc, txt = self.captureOutput(cfg.displayKey, 'repositoryMap')
            assert(txt == '''\
repositoryMap             conary.rpath.com          http://localhost/conary/
repositoryMap             foo.rpath.org             http://localhost:1/conary/
''')
            rc, txt = self.captureOutput(cfg.displayContext)
            self.assertEqual(txt, '''\
[foo]
repositoryMap             foo.rpath.org             http://localhost:1/conary/
showLabels                False
signatureKey              None
user                      conary.rpath.com bar
''')

        finally:
            os.chdir(cwd)
            shutil.rmtree(tdir)

    def testIncludeConfigFileInContext(self):
        configfile1 = """
[foo]
buildLabel foo.rpath.org@rpl:devel
includeConfigFile bar
threaded False
"""
        configfile2 = """
installLabelPath foo.rpath.org@rpl:ilp

[bam]
buildLabel bam.rpath.org@rpl:devel
"""
        tdir = tempfile.mkdtemp()
        cwd = os.getcwd()
        os.chdir(tdir)
        try:
            f = open('foo', 'w').write(configfile1)
            f = open('bar', 'w').write(configfile2)
            cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
            cfg.read(tdir + '/foo')
            cfg.setContext('foo')
            assert(str(cfg.buildLabel) == 'foo.rpath.org@rpl:devel')
            assert(str(cfg.installLabelPath[0]) == 'foo.rpath.org@rpl:ilp')
            assert(len(cfg.installLabelPath) == 1)
            rc, txt = self.captureOutput(cfg.displayContext)
            self.assertEqual(txt, '''\
[foo]
buildLabel                foo.rpath.org@rpl:devel
installLabelPath          foo.rpath.org@rpl:ilp
threaded                  False
''')
        finally:
            os.chdir(cwd)
            shutil.rmtree(tdir)

    def testCopyContext(self):
        """
        Test that applying a context to a deepcopy'd config behaves sanely; in
        particular the defaultness of the values in each section must be
        preserved.
        """
        cfg = conarycfg.ConaryConfiguration(False)
        cfg.configLine("installLabelPath desired@install:label")
        cfg.configLine("[context]")
        cfg.configLine("flavor is: x86_64")
        desired = [versions.Label("desired@install:label")]
        self.assertEqual(cfg.installLabelPath, desired)
        # NB: This will COW the context's value before copying in order to
        # expose the original issue.
        self.assertEqual(cfg.getSection('context').installLabelPath, [])

        cfg2 = copy.deepcopy(cfg)
        cfg2.setContext('context')
        self.assertEqual(cfg2.installLabelPath, desired)


class ProxyConfigtest(testhelp.TestCase):
    def testProxyOverrides(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.configLine('proxy http http://foo:123')
        proxy = conarycfg.getProxyFromConfig(cfg)
        self.assertEqual(proxy, {'http' : 'http://foo:123'})

        cfg.configLine('conaryProxy https https://bar')
        proxy = conarycfg.getProxyFromConfig(cfg)
        self.assertEqual(proxy, {'https' : 'conarys://bar'})

        cfg.configLine('proxy https https://foo:123')
        proxy = conarycfg.getProxyFromConfig(cfg)
        self.assertEqual(proxy, {'https' : 'conarys://bar'})


class ExtraHeadersTest(testhelp.TestCase):
    def testExtraHeaders(self):
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)

        srvThread, port, queue = startServer()
        val = queue.get(block=True)
        self.assertEqual(val, "started")

        cfg.readUrl("http://127.0.0.1:%s/" % port)
        srvThread.join()
        headers = {}
        while not queue.empty():
            ent = queue.get(block=True)
            headers[ent[0].lower()] = ent[1]
        self.assertTrue('x-conary-version' in headers)
        self.assertTrue('x-conary-config-version' in headers)

def startServer():
    def startFunc(port, queue):
        srv = HTTPServer(queue, ("127.0.0.1", port), QueuedRequestHandler)
        queue.put("started")
        # Handle only one request
        srv.handle_request()

    port = testhelp.findPorts(num = 1)[0]
    queue = Queue.Queue(maxsize = 20)
    t = threading.Thread(target = startFunc, args = (port, queue))
    t.start()
    return t, port, queue

class QueuedRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        response = "proxy http://foo:123\n"
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)

        # Put the headers in the queue
        for k, v in self.headers.items():
            self.server.queue.put((k, v))
        # We're done

    def log_message(self, *args):
        # Silence the spewage to stderr
        pass


class HTTPServer(BaseHTTPServer.HTTPServer):
    def __init__(self, queue, *args):
        self.queue = queue
        BaseHTTPServer.HTTPServer.__init__(self, *args)
