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


from conary.lib import util
from conary.repository import calllog, changeset, filecontents, netclient
from conary.repository.netrepos import netserver

import gzip
import os
import tempfile
import time

# this returns the same server for any server name or label
# requested; because a shim can only refer to one server.
class FakeServerCache(netclient.ServerCache):
    def __init__(self, server, cfg):
        self._server = server
        netclient.ServerCache.__init__(self, cfg=cfg)

    def __getitem__(self, item):
        serverName = self._getServerName(item)
        # return the proxy object for anything that matches the
        # serverNames on this repository
        if serverName in self._server._server.serverNameList:
            return self._server
        # otherwise get a real repository client
        return netclient.ServerCache.__getitem__(self, item)

class NetworkRepositoryServer(netserver.NetworkRepositoryServer):
    @netserver.accessReadOnly
    def getFileContents(self, *args, **kwargs):
        location = netserver.NetworkRepositoryServer.getFileContents(self,
                                                        *args, **kwargs)[0]
        path = os.path.join(self.tmpPath,location.split('?')[1] + '-out')
        paths = open(path).readlines()
        os.unlink(path)
        return [ x.split(" ")[0] for x in paths ]

    @netserver.accessReadOnly
    def getFileContentsFromTrove(self, *args, **kwargs):
        location, sizes = netserver.NetworkRepositoryServer.getFileContentsFromTrove(
                                            self, *args, **kwargs)
        path = os.path.join(self.tmpPath,location.split('?')[1] + '-out')
        paths = open(path).readlines()
        os.unlink(path)
        return [ x.split(" ")[0] for x in paths ]


    def getChangeSet(self, authToken, clientVersion, chgSetList, recurse,
                     withFiles, withFileContents, excludeAutoSource):
        csList = []
        def _cvtTroveList(l):
            new = []
            for (name, (oldV, oldF), (newV, newF), absolute) in l:
                if oldV:
                    oldV = self.fromVersion(oldV)
                    oldF = self.fromFlavor(oldF)
                else:
                    oldV = 0
                    oldF = 0

                if newV:
                    newV = self.fromVersion(newV)
                    newF = self.fromFlavor(newF)
                else:
                    # this happens when a distributed group has a trove
                    # on a remote repository disappear
                    newV = 0
                    newF = 0

                new.append((name, (oldV, oldF), (newV, newF), absolute))

            return new

        for (name, (old, oldFlavor), (new, newFlavor), absolute) in chgSetList:
            if old == 0:
                l = (name, (None, None),
                           (self.toVersion(new), self.toFlavor(newFlavor)),
                           absolute)
            else:
                l = (name, (self.toVersion(old), self.toFlavor(oldFlavor)),
                           (self.toVersion(new), self.toFlavor(newFlavor)),
                           absolute)
            csList.append(l)

        ret = self.repos.createChangeSet(csList,
                                recurse = recurse,
                                withFiles = withFiles,
                                withFileContents = withFileContents,
                                excludeAutoSource = excludeAutoSource)

        (cs, trovesNeeded, filesNeeded, removedTroveList) = ret
        assert(not filesNeeded)
        assert(not removedTroveList)

        # FIXME: we need a way to remove these temporary
        # files when we're done with them.
        fd, tmpFile = tempfile.mkstemp(suffix = '.ccs')
        os.close(fd)
        cs.writeToFile(tmpFile)
        size = os.stat(tmpFile).st_size
        return (tmpFile, [size], _cvtTroveList(trovesNeeded), [], [])


class ShimNetClient(netclient.NetworkRepositoryClient):
    """
    A subclass of NetworkRepositoryClient which can take a
    shimclient.NetworkRepositoryServer instance (plus a few other
    pieces of information) and expose the netclient interface without
    the overhead of XMLRPC.

    If 'server' is a regular netserver.NetworkRepositoryServer
    instance, the shim won't be able to return changesets. If 'server'
    is a shimclient.NetworkRepositoryServer, it will.

    NOTE: Conary proxies are only used for "real" netclients
    outside this repository's serverNameList.
    """

    def getFileContentsObjects(self, server, fileList, callback, outF,
                               compressed):
        if not isinstance(self.c[server], ShimServerProxy):
            return netclient.NetworkRepositoryClient.getFileContentsObjects(
                self, server, fileList, callback, outF, compressed)
        filePaths = self.c[server].getFileContents(fileList)
        fileObjList = []
        for path in filePaths:
            if compressed:
                fileObjList.append(
                    filecontents.FromFilesystem(path, compressed = True))
            else:
                f = gzip.GzipFile(path, "r")
                fileObjList.append(filecontents.FromFile(f))

        return fileObjList

    def getFileContentsFromTrove(self, n, v, f, pathList,
                                 callback = None, compressed = False):
        server = v.trailingLabel().getHost()
        if not isinstance(self.c[server], ShimServerProxy):
            return netclient.NetworkRepositoryClient.getFileContentsFromTrove(
                self, n, v, f, pathList, callback = callback,
                compressed = compressed)
        pathList = [self.fromPath(x) for x in pathList]
        v = self.fromVersion(v)
        f = self.fromFlavor(f)
        filePaths = self.c[server].getFileContentsFromTrove(n,v,f,
                                                            pathList)
        fileObjList = []
        for path in filePaths:
            if compressed:
                fileObjList.append(
                    filecontents.FromFilesystem(path, compressed = True))
            else:
                f = gzip.GzipFile(path, "r")
                fileObjList.append(filecontents.FromFile(f))
        return fileObjList

    def commitChangeSet(self, chgSet, callback = None, mirror = False,
                        hidden = False):
        trvCs = chgSet.iterNewTroveList().next()
        newLabel = trvCs.getNewVersion().trailingLabel()

        if not isinstance(self.c[newLabel], ShimServerProxy):
            return netclient.NetworkRepositoryClient.commitChangeSet(self,
                chgSet, callback = callback, mirror = False, hidden = False)

        (fd, path) = tempfile.mkstemp(dir = self.c[newLabel]._server.tmpPath,
                                      suffix = '.ccs-in')
        os.close(fd)
        chgSet.writeToFile(path)
        base = os.path.basename(path)[:-3]
        url = util.normurl(self.c[newLabel]._server.basicUrl) + "?" + base

        self.c[newLabel].commitChangeSet(url, mirror = mirror,
                                         hidden = hidden)

    def commitChangeSetFile(self, fName, mirror = False, callback = None,
                            hidden = False):
        # this could be more efficient. it rewrites the trove every time,
        # but it doesn't seem to be heavily used
        cs = changeset.ChangeSetFromFile(fName)
        self.commitChangeSet(cs, callback = callback, mirror = mirror,
                             hidden = hidden)

    def __init__(self, server, protocol, port, authToken, cfg):
        if type(authToken[2]) is not list:
            # old-style [single entitlement] authToken
            authToken = (authToken[0], authToken[1],
                         [ ( authToken[2], authToken[3]) ], None )
        elif len(authToken) == 3:
            authToken = authToken + (None,)

        netclient.NetworkRepositoryClient.__init__(self, cfg=cfg)
        proxy = ShimServerProxy(server, protocol, port, authToken,
                systemId=self.c.systemId)
        self.c = FakeServerCache(proxy, cfg=cfg)


class ShimServerProxy(netclient.ServerProxy):

    def __init__(self, server, protocol, port, authToken, systemId=None):
        self._authToken = authToken
        self._server = server
        self._protocol = protocol
        self._port = port
        self._systemId = systemId
        self._protocolVersion = netclient.CLIENT_VERSIONS[-1]

        if 'CONARY_CLIENT_LOG' in os.environ:
            self._callLog = calllog.ClientCallLogger(
                                os.environ['CONARY_CLIENT_LOG'])
        else:
            self._callLog = None

    def __repr__(self):
        return '<ShimServerProxy for %r>' % (self._server,)

    def setAbortCheck(self, *args):
        pass

    def getChangeSetObj(self, *args):
        return self._server._getChangeSetObj(self._authToken, *args)

    def usedProxy(self, *args):
        return False

    def _request(self, method, args, kwargs):
        args = [self._protocolVersion] + list(args)
        start = time.time()
        result = self._server.callWrapper(self._protocol, self._port, method,
                self._authToken, args, kwargs, systemId=self._systemId)

        if self._callLog:
            self._callLog.log("shim-" + self._server.repos.serverNameList[0],
                               [], method, result, args,
                               latency = time.time() - start)

        return result
