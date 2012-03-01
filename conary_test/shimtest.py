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
        cfg.contentsDir = testserver.reposDir + '/contents'
        cfg.configLine('repositoryDB %s' %testserver.reposDB.getDriver())

        server = shimclient.NetworkRepositoryServer(cfg,
                self.cfg.repositoryMap['localhost'])

        if log:
            os.environ['CONARY_CLIENT_LOG'] = log

        rm = copy.copy(self.cfg.repositoryMap)
        # remove localhost from the map; we don't need it since this is a shim
        del rm[[ x[0] for x in enumerate(rm) if x[1][0] == 'localhost'][0]]

        shim = shimclient.ShimNetClient(server, 'http', 80,
             authToken, rm, self.cfg.user)

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
        shim = shimclient.ShimNetClient(server, 'http', 80,
            ('anonymous', 'anonymous', []), {},
             conarycfg.UserInformation())
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
