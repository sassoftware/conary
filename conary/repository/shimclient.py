#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
from conary.repository import netclient
from conary.repository.netrepos import netserver

import os
import tempfile

# this returns the same server for any server name or label
# requested; because a shim can only refer to one server.
class FakeServerCache:
    def __init__(self, server):
        self._server = server

    def __getitem__(self, item):
        return self._server

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

        (cs, trovesNeeded, filesNeeded) = ret
        assert(not filesNeeded)

        # FIXME: we need a way to remove these temporary
        # files when we're done with them.
        tmpFile = tempfile.mktemp(suffix = '.ccs')
        cs.writeToFile(tmpFile)
        size = os.stat(tmpFile).st_size
        return (tmpFile, [size], _cvtTroveList(trovesNeeded), [])


class ShimNetClient(netclient.NetworkRepositoryClient):
    """
    A subclass of NetworkRepositoryClient which can take a shimclient.NetworkRepositoryServer
    instance (plus a few other pieces of information) and expose the netclient
    interface without the overhead of XMLRPC.

    If 'server' is a regular netserver.NetworkRepositoryServer instance, the shim won't be
    able to return changesets. If 'server' is a shimclient.NetworkRepositoryServer, it will.
    """
    def __init__(self, server, protocol, port, authToken, repMap, userMap):
        netclient.NetworkRepositoryClient.__init__(self, repMap, userMap)
        proxy = ShimServerProxy(server, protocol, port, authToken)
        self.c = FakeServerCache(proxy)


class _ShimMethod(netclient._Method):
    def __init__(self, server, protocol, port, authToken, name):
        self._server = server
        self._authToken = authToken
        self._name = name
        self._protocol = protocol
        self._port = port

    def __repr__(self):
        return "<server._ShimMethod(%r)>" % (self._name)

    def __call__(self, *args):
        args = [netclient.CLIENT_VERSIONS[-1]] + list(args)
        asAnonymous, isException, result = self._server.callWrapper(
            self._protocol, self._port,
            self._name, self._authToken, args)

        if not isException:
            return result
        else:
            self.handleError(result)


class ShimServerProxy(netclient.ServerProxy):
    def __init__(self, server, protocol, port, authToken):
        self._authToken = authToken
        self._server = server
        self._protocol = protocol
        self._port = port

    def __getattr__(self, name):
        return _ShimMethod(self._server,
            self._protocol, self._port,
            self._authToken, name)
