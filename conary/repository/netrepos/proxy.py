#
# Copyright (c) 2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import xmlrpclib

# a list of the protocol versions we understand. Make sure the first
# one in the list is the lowest protocol version we support and the
# last one is the current server protocol version
SERVER_VERSIONS = [ 36, 37, 38, 39, 40 ]

from conary import conarycfg
from conary.lib import tracelog
from conary.repository import netclient, transport, xmlshims
from conary.repository.netrepos import netserver

class ProxyClient(xmlrpclib.ServerProxy):

    pass

class ProxyRepositoryServer(xmlshims.NetworkConvertors):

    publicCalls = netserver.NetworkRepositoryServer.publicCalls

    def __init__(self, cfg, basicUrl):
        self.cfg = cfg
        self.basicUrl = basicUrl
        self.logFile = cfg.logFile

        self.log = tracelog.getLog(None)
        if cfg.traceLog:
            (l, f) = cfg.traceLog
            self.log = tracelog.getLog(filename=f, level=l, trace=l>2)

        if self.logFile:
            self.callLog = calllog.CallLogger(self.logFile, [])
        else:
            self.callLog = None

        self.log(1, "proxy url=%s" % basicUrl)
        self.reposSet = netclient.NetworkRepositoryClient(
                                cfg.repositoryMap, conarycfg.UserInformation())

    def callWrapper(self, protocol, port, methodname, authToken, args,
                    remoteIp = None, targetServerName = None):
        if methodname not in self.publicCalls:
            return (False, True, ("MethodNotSupported", methodname, ""))

        if hasattr(self, methodname):
            # handled internally
            method = self.__getattribute__(methodname)
            r = method(authToken, *args)

            if self.callLog:
                self.callLog.log(remoteIp, authToken, methodname, args)

            return (False, False, r)
        else:
            # simple proxy. FIXME: caching these might help; building all
            # of this framework for every request seems dumb. it seems like
            # we could get away with one total since we're just changing
            # hostname/username/entitlement
            
            # FIXME: entitlements! https! users! ack!
            url = self.cfg.repositoryMap.get(targetServerName, None)
            if url is None:
                if authToken[0] != 'anonymous' or authToken[2]:
                    # with a username or entitlement, use https. otherwise
                    # use the same protocol which was used to connect to us
                    proxyProtocol = 'http'
                else:
                    proxyProtocol = protocol
                url = '%s://%s/conary/' % (proxyProtocol, targetServerName)

            # paste in the user/password info
            s = url.split('/')
            s[2] = ('%s:%s@' % (netclient.quote(authToken[0]),
                                netclient.quote(authToken[1]))) + s[2]
            url = '/'.join(s)

            if authToken[2] is not None:
                entitlement = authToken[2:3]
            else:
                entitlement = None

            transporter = transport.Transport(https = (protocol == 'https'),
                                              entitlement = entitlement)
            transporter.setCompress(True)
            proxy = ProxyClient(url, transporter)

            try:
                rc = proxy.__getattr__(methodname)(*args)
            except IOError, e:
                return [ 'OpenError', [], [] ]

            return rc

    def checkVersion(self, authToken, clientVersion):
        self.log(2, authToken[0], "clientVersion=%s" % clientVersion)
        # cut off older clients entirely, no negotiation
        if clientVersion < SERVER_VERSIONS[0]:
            raise errors.InvalidClientVersion(
               'Invalid client version %s.  Server accepts client versions %s '
               '- read http://wiki.rpath.com/wiki/Conary:Conversion' %
               (clientVersion, ', '.join(str(x) for x in SERVER_VERSIONS)))
        return SERVER_VERSIONS
