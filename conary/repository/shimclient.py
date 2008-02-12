#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
from conary.repository import netclient
from conary.repository.netrepos import netserver

import os
import tempfile

# this returns the same server for any server name or label
# requested; because a shim can only refer to one server.
class FakeServerCache(netclient.ServerCache):
    def __init__(self, server, repMap, userMap, conaryProxies):
        self._server = server
        netclient.ServerCache.__init__(self, repMap, userMap,
                proxies=conaryProxies)

    def __getitem__(self, item):
        serverName = self._getServerName(item)
        # return the proxy object for anything that matches the
        # serverNames on this repository
        if serverName in self._server._server.serverNameList:
            return self._server
        # otherwise get a real repository client
        return netclient.ServerCache.__getitem__(self, item)

class NetworkRepositoryServer(netserver.NetworkRepositoryServer):
    def getChangeSet(self, authToken, clientVersion, chgSetList, recurse,
                     withFiles, withFileContents, excludeAutoSource):
        paths = []
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
            newVer = self.toVersion(new)
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
    def __init__(self, server, protocol, port, authToken, repMap, userMap,
            conaryProxies=None):
        if type(authToken[2]) is not list:
            # old-style [single entitlement] authToken
            authToken = (authToken[0], authToken[1],
                         [ ( authToken[2], authToken[3]) ], None )
        elif len(authToken) == 3:
            authToken = authToken + (None,)

        netclient.NetworkRepositoryClient.__init__(self, repMap, userMap,
                proxy=conaryProxies)
        proxy = ShimServerProxy(server, protocol, port, authToken)
        self.c = FakeServerCache(proxy, repMap, userMap, conaryProxies)


class _ShimMethod(netclient._Method):
    def __init__(self, server, protocol, port, authToken, name):
        self._server = server
        self._authToken = authToken
        self._name = name
        self._protocol = protocol
        self._port = port

    def __repr__(self):
        return "<server._ShimMethod(%r)>" % (self._name)

    def __call__(self, *args, **kwargs):
        args = [netclient.CLIENT_VERSIONS[-1]] + list(args)
        return self._server.callWrapper(self._protocol, self._port,
                                          self._name, self._authToken, args,
                                          kwargs)

class ShimServerProxy(netclient.ServerProxy):
    def __init__(self, server, protocol, port, authToken):
        self._authToken = authToken
        self._server = server
        self._protocol = protocol
        self._port = port

    def setAbortCheck(self, *args):
        pass

    def getChangeSetObj(self, *args):
        return self._server._getChangeSetObj(self._authToken, *args)

    def usedProxy(self, *args):
        return False

    def __getattr__(self, name):
        return _ShimMethod(self._server,
            self._protocol, self._port,
            self._authToken, name)
