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


from conary_test import rephelp

import copy
import gzip
import os

from conary.repository import calllog, errors, shimclient
from conary.repository.netrepos import netserver
from conary import conarycfg

class ShimNetClientTest(rephelp.RepositoryHelper):

    def _setupShim(self, log = None, authToken = None):
        if authToken is None:
            authToken = ( 'anonymous', 'anonymous', None, None )

        self.openRepository()
        testserver = self.servers.getServer()

        cfg = netserver.ServerConfig()
        cfg.serverName = testserver.getName()
        cfg.tmpDir = self.tmpDir
        cfg.contentsDir = ('legacy', [testserver.contents.getPath()])
        cfg.configLine('repositoryDB %s' %testserver.reposDB.getDriver())

        server = shimclient.NetworkRepositoryServer(cfg,
                self.cfg.repositoryMap['localhost'])

        if log:
            os.environ['CONARY_CLIENT_LOG'] = log

        # remove localhost from the map; we don't need it since this is a shim
        cliCfg = copy.copy(self.cfg)
        rm = conarycfg.RepoMap()
        for host, url in self.cfg.repositoryMap:
            if host != 'localhost':
                rm.append((host, url))
        cliCfg.repositoryMap = rm
        shim = shimclient.ShimNetClient(server, 'http', 80, authToken, cliCfg)

        if log:
            del os.environ['CONARY_CLIENT_LOG']

        return shim, cfg

    def testShimClient(self):
        t = self.addQuickTestComponent('test:runtime', '1.0-1-1')

        shim, cfg = self._setupShim()

        label = t.getVersion().branch().label()
        self.assertEqual(shim.troveNames(label), ['test:runtime'])

        n, v, f = t.getName(), t.getVersion(), t.getFlavor()

        trove = shim.getTroves([(n, v, f)])[0]
        self.assertEqual(trove.getName(), n)

        # test 3-member auth token
        server = shimclient.NetworkRepositoryServer(cfg,
                self.cfg.repositoryMap['localhost'])
        empty = conarycfg.ConaryConfiguration(False)
        shim = shimclient.ShimNetClient(server, 'http', 80,
            ('anonymous', 'anonymous', []), cfg=empty)
        trove = shim.getTroves([(n, v, f)])[0]
        self.assertEqual(trove.getName(), n)

        # test exceptions
        self.assertRaises(errors.InsufficientPermission,
            shim.updateRoleMembers, 'localhost', 'unknown_group', [])

    def testShimClientFileContents(self):
        self.openRepository(1)
        shim = self._setupShim()[0]

        t = self.addQuickTestComponent('test:runtime', '1.0-1-1',
                                       fileContents = [ ( '/path', 'hello' ) ] )

        label = t.getVersion().branch().label()
        self.assertEqual(shim.troveNames(label), ['test:runtime'])

        n, v, f = t.getName(), t.getVersion(), t.getFlavor()

        trove = shim.getTroves([(n, v, f)])[0]
        self.assertEqual(trove.getName(), n)

        fileObj = shim.getFileContents([(fileId, version)
                for pathId, path, fileId, version in trove.iterFileList()])[0]
        assert(not fileObj.isCompressed())
        assert(fileObj.get().readlines() == [ 'hello' ])

        fileObj = shim.getFileContents([(fileId, version)
                for pathId, path, fileId, version in trove.iterFileList()],
                compressed = True)[0]
        assert(fileObj.isCompressed())
        f = gzip.GzipFile(fileobj = fileObj.get(), mode = "r")
        assert(f.readlines() == [ 'hello' ])

        t = self.addComponent('test:source', '/localhost1@foo:bar/1.0-1')
        shim.getFileContents([ (x[2], x[3]) for x in t.iterFileList() ])

    def testShimLog(self):
        p = self.workDir + '/client.log'
        shim = self._setupShim(p)[0]
        trv = self.addComponent('foo:runtime', '1.0')

        # protocol, port, and authToken are only used for certain operations;
        # we can pass generally-reasonable values here.

        log = calllog.ClientCallLogger(p)
        shim.troveNames(trv.getVersion().trailingLabel())
        logEntries = list(log)
        assert(len(logEntries) == 1)
        assert(logEntries[0].methodName == 'troveNames')
        assert(logEntries[0].url == 'shim-localhost')

    def testShimCommit(self):
        self.openRepository(1)
        p = self.workDir + '/client.log'
        shim = self._setupShim(p, authToken = ('test', 'foo', None, None))[0]

        trv, cs = self.Component('foo:runtime', '1.0')
        shim.commitChangeSet(cs)
        repos = self.openRepository()
        trv2 = repos.getTrove(*trv.getNameVersionFlavor())
        assert(trv == trv2)
        del trv, trv2

        trv, cs = self.Component('foo:runtime', '2.0')
        cs.writeToFile(self.workDir + '/foo.ccs')
        shim.commitChangeSetFile(self.workDir + '/foo.ccs')

        # test another repository
        trv, cs = self.Component('foo:runtime',
                                 '/localhost1@foo:bar/2.0')
        shim.commitChangeSet(cs)
