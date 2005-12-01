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

# this returns the same server for any server name or label
# requested; because a shim can only refer to one server.
class FakeServerCache:
    def __init__(self, server):
        self._server = server

    def __getitem__(self, item):
        return self._server

class ShimNetClient(netclient.NetworkRepositoryClient):
    """
    A subclass of NetworkRepositoryClient which can take a NetworkRepositoryServer
    instance (plus a few other pieces of information) and expose the netclient
    interface without the overhead of XMLRPC.
    """
    def __init__(self, server, protocol, port, authToken, repMap):
        netclient.NetworkRepositoryClient.__init__(self, repMap)
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
        isException, result = self._server.callWrapper(
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
