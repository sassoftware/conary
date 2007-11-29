#
# Copyright (c) 2004-2007 rPath, Inc.
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

import base64
import gzip
import httplib
import itertools
import os
import socket
import sys, time
import urllib
import xml
import xmlrpclib

#conary
from conary import callbacks
from conary import conarycfg
from conary import files
from conary import metadata
from conary import trove
from conary import versions
from conary.lib import util
from conary.repository import calllog
from conary.repository import changeset
from conary.repository import errors
from conary.repository import filecontainer
from conary.repository import filecontents
from conary.repository import findtrove
from conary.repository import repository
from conary.repository import transport
from conary.repository import trovesource
from conary.repository import xmlshims

# FIXME: remove these compatibility exception classes later
AlreadySignedError = errors.AlreadySignedError
FileStreamNotFound = errors.FileStreamNotFound
UserNotFound = errors.UserNotFound
GroupAlreadyExists = errors.GroupAlreadyExists
PermissionAlreadyExists = errors.PermissionAlreadyExists

shims = xmlshims.NetworkConvertors()

# end of range or last protocol version + 1
CLIENT_VERSIONS = range(36, 60 + 1)

from conary.repository.trovesource import TROVE_QUERY_ALL, TROVE_QUERY_PRESENT, TROVE_QUERY_NORMAL

# this is a quote function that quotes all RFC 2396 reserved characters,
# including / (which is normally considered "safe" by urllib.quote)
quote = lambda s: urllib.quote(s, safe='')

# mask out the username and password for error messages
def _cleanseUrl(protocol, url):
    if url.find('@') != -1:
        return protocol + '://<user>:<pwd>@' + url.rsplit('@', 1)[1]
    return url

class _Method(xmlrpclib._Method, xmlshims.NetworkConvertors):

    def __init__(self, send, name, host, pwCallback, anonymousCallback,
                 altHostCallback, protocolVersion, transport, serverName,
                 entitlementDir, callLog):
        xmlrpclib._Method.__init__(self, send, name)
        self.__name = name
        self.__host = host
        self.__pwCallback = pwCallback
        self.__anonymousCallback = anonymousCallback
        self.__altHostCallback = altHostCallback
        self.__protocolVersion = protocolVersion
        self.__serverName = serverName
        self.__entitlementDir = entitlementDir
        self._transport = transport
        self.__callLog = callLog

    def __repr__(self):
        return "<netclient._Method(%s, %r)>" % (self._Method__send, self._Method__name) 

    def __str__(self):
        return self.__repr__()

    def __call__(self, *args, **kwargs):
        # Keyword arguments are ignored, we just use them to override the
        # protocol version
        protocolVersion = (kwargs.get('protocolVersion', None) or
            self.__protocolVersion)

        # always use protocol version 50 for checkVersion.  If we're about
        # to talk to a pre-protocol-version 51 server, we will make it
        # trace back with too many arguments if we try to pass kwargs
        if self.__name == 'checkVersion':
            protocolVersion = min(protocolVersion, 50)

        if protocolVersion < 51:
            assert(not kwargs)
            return self.doCall(protocolVersion, *args)

        return self.doCall(protocolVersion, args, kwargs)

    def __doCall(self, clientVersion, argList,
                 retryOnEntitlementTimeout = True):
        newArgs = ( clientVersion, ) + argList

        start = time.time()

        try:
            rc = self.__send(self.__name, newArgs)
        except xmlrpclib.ProtocolError, e:
            if e.errcode == 403:
                raise errors.InsufficientPermission(e.url.split("/")[2])
            raise

        if clientVersion < 60:
            usedAnonymous, isException, result = rc
        else:
            usedAnonymous = False
            isException, result = rc

        if self.__callLog:
            self.__callLog.log(self.__host, self._transport.getEntitlements(),
                               self.__name, rc, newArgs,
                               latency = time.time() - start)

        if usedAnonymous:
            self.__anonymousCallback()

        if not isException:
            return result

        try:
            self.handleError(clientVersion, result)
        except errors.EntitlementTimeout:
            if not retryOnEntitlementTimeout:
                raise

            entList = self._transport.getEntitlements()
            exception = errors.EntitlementTimeout(result[1])

            singleEnt = conarycfg.loadEntitlement(self.__entitlementDir,
                                                  self.__serverName)
            # remove entitlement(s) which timed out
            newEntList = [ x for x in entList if x[1] not in
                                exception.getEntitlements() ]
            newEntList.insert(0, singleEnt[1:])

            # try again with the new entitlement
            self._transport.setEntitlements(newEntList)
            return self.__doCall(clientVersion, argList,
                                 retryOnEntitlementTimeout = False)
        else:
            # this can't happen as handleError should always result in
            # an exception
            assert(0)

    def doCall(self, clientVersion, *args):
        try:
            return self.__doCall(clientVersion, args)
        except errors.InsufficientPermission:
            # no password was specified -- prompt for it
            if not self.__pwCallback():
                # It's possible we switched to anonymous
                # for an earlier query, and now need to 
                # switch back to our specified user/passwd
                if self.__altHostCallback and self.__altHostCallback():
                    self.__altHostCallback = None
                    # recursively call doCall to get all the 
                    # password handling goodness
                    return self.doCall(clientVersion, *args)
                raise
        except xmlrpclib.ProtocolError, err:
            if err.errcode == 500:
                raise errors.InternalServerError(err)
            self._postprocessProtocolError(err)
            raise
        except:
            raise

        return self.__doCall(clientVersion, args)

    def _postprocessProtocolError(self, err):
        proxyHost = getattr(self._transport, 'proxyHost', 'None')
        if proxyHost is None:
            return
        proxyProtocol = self._transport.proxyProtocol
        if proxyProtocol.startswith('http'):
            pt = 'HTTP'
        else:
            pt = 'Conary'
        err.url = "%s (via %s proxy %s)" % (err.url, pt, proxyHost)

    def handleError(self, clientVersion, result):
        if clientVersion < 60:
            exceptionName = result[0]
            exceptionArgs = result[1:]
            exceptionKwArgs = {}
        else:
            exceptionName = result[0]
            exceptionArgs = result[1]
            exceptionKwArgs = result[2]

        if exceptionName == "TroveIntegrityError" and len(exceptionArgs) > 1:
            # old repositories give TIE w/ no trove information or with a
            # string error message. exceptionArgs[0] is that message if
            # exceptionArgs[1] is not set or is empty.
            raise errors.TroveIntegrityError(error=exceptionArgs[0], 
                                        *self.toTroveTup(exceptionArgs[1]))
        elif not hasattr(errors, exceptionName):
            raise errors.UnknownException(exceptionName, exceptionArgs)
        else:
            exceptionClass = getattr(errors, exceptionName)

            if hasattr(exceptionClass, 'demarshall'):
                args, kwArgs = exceptionClass.demarshall(self, exceptionArgs,
                                                         exceptionKwArgs)
                raise exceptionClass(*args, **kwArgs)

            for klass, marshall in errors.simpleExceptions:
                if exceptionName == marshall:
                    raise klass(exceptionArgs[0])
	    raise errors.UnknownException(exceptionName, exceptionArgs)

    def __getattr__(self, name):
        # Don't invoke methods that start with __
        if name.startswith('__'):
            raise AttributeError(name)
        return xmlrpclib._Method.__getattr__(self, name)

class ServerProxy(util.ServerProxy):

    def __passwordCallback(self):
        if self.__pwCallback is None:
            return False

        l = self.__host.split('@', 1)
        if len(l) == 1: 
            fullHost = l[0]
            user, password = self.__pwCallback(self.__serverName)
            if not user or not password:
                return False
            if not self.__usedMap:
                # the user didn't specify what protocol to use, therefore
                # we assume that when we need a user/password we need
                # to use https
                self.__transport.https = True
        else:
            user, fullHost = l
            if user[-1] != ':':
                return False

            user = user[:-1]

            # if there is a port number, strip it off
            l = fullHost.split(':', 1)
            if len(l) == 2:
                host = l[0]
            else:
                host = fullHost

            user, password = self.__pwCallback(self.__serverName, user)
            if not user or not password:
                return False

        password = util.ProtectedString(password)
        self.__host = util.ProtectedTemplate('${user}:${passwd}@${host}',
                            user = user, passwd = password, host = fullHost)

        return True

    def __usedAnonymousCallback(self):
        self.__altHost = self.__host
        self.__host = self.__host.split('@')[-1]

    def __altHostCallback(self):
        if self.__altHost:
            self.__host = self.__altHost
            self.__altHost = None
            return True
        else:
            return False

    def _createMethod(self, name):
        return _Method(self._request, name, self.__host,
                       self.__passwordCallback, self.__usedAnonymousCallback,
                       self.__altHostCallback, self.getProtocolVersion(),
                       self.__transport, self.__serverName,
                       self.__entitlementDir, self.__callLog)

    def usedProxy(self):
        return self.__transport.usedProxy

    def setAbortCheck(self, check):
        self.__transport.setAbortCheck(check)

    def setProtocolVersion(self, val):
        self.__protocolVersion = val

    def getProtocolVersion(self):
        return self.__protocolVersion

    def __init__(self, url, serverName, transporter, pwCallback, usedMap,
                 entitlementDir, callLog):
        try:
            util.ServerProxy.__init__(self, url, transporter)
        except IOError, e:
            proto, url = urllib.splittype(url)
            raise errors.OpenError('Error occurred opening repository '
                                   '%s: %s' % (_cleanseUrl(proto, url), e))
        self.__pwCallback = pwCallback
        self.__altHost = None
        self.__serverName = serverName
        self.__usedMap = usedMap
        self.__protocolVersion = CLIENT_VERSIONS[-1]
        self.__entitlementDir = entitlementDir
        self.__callLog = callLog

class ServerCache:
    def __init__(self, repMap, userMap, pwPrompt=None, entitlements = None,
                 callback=None, proxies=None, entitlementDir = None):
	self.cache = {}
        self.shareCache = {}
	self.map = repMap
	self.userMap = userMap
	self.pwPrompt = pwPrompt
        self.entitlements = entitlements
        self.proxies = proxies
        self.entitlementDir = entitlementDir
        self.callLog = None

        if 'CONARY_CLIENT_LOG' in os.environ:
            self.callLog = calllog.ClientCallLogger(
                                os.environ['CONARY_CLIENT_LOG'])

    def __getPassword(self, host, user=None):
        if not self.pwPrompt:
            return None, None
        user, pw = self.pwPrompt(host, user)
        if user is None or pw is None:
            return None, None
        self.userMap.addServerGlob(host, user, pw)
        return user, pw

    def _getServerName(self, item):
	if isinstance(item, (versions.Label, versions.VersionSequence)):
	    serverName = item.getHost()
	elif isinstance(item, str):
             # Detect a label passed in as a string instead of a label object.
             # This is only useful for misbehaving consumers of the ShimNetClient.
             # That code should be fixed by passing in a Label object or a
             # server name as a string, not a label as a string.
             if '@' in item:
                 serverName = item.split('@')[0]
             else:
                 serverName = item
        else:
            serverName = str(item)

        if serverName == 'local':
            raise errors.OpenError(
             '\nError: Tried to access repository on reserved host name'
             ' "local" -- this host is reserved for troves compiled/created'
             ' locally, and cannot be queried.')
        return serverName

    def __delitem__(self, item):
        serverName = self._getServerName(item)
        del self.cache[serverName]

    def keys(self):
        return self.cache.keys()

    def singleServer(self, *items):
        foundServer = None
        for item in items:
            if item.branch().getHost() == 'local':
                return False

            try:
                server = self[item]
            except errors.OpenError:
                # can't get to a server; fall back to hostname checking
                return (len(set( self._getServerName(x) for x in items )) == 1)

            if foundServer is None:
                foundServer = server
            elif foundServer is not server:
                return False

        return True

    def __getitem__(self, item):
        serverName = self._getServerName(item)

	server = self.cache.get(serverName, None)
        if server is not None:
            return server

        url = self.map.get(serverName, None)
        if isinstance(url, repository.AbstractTroveDatabase):
            return url

        userInfo = self.userMap.find(serverName)

        if userInfo and userInfo[1] is None:
            userInfo = (userInfo[0], "")

        # load any entitlement for this server which is on-disk
        if self.entitlementDir is not None:
            singleEnt = conarycfg.loadEntitlement(self.entitlementDir,
                                                  serverName)
        else:
            singleEnt = None

        # look for any entitlements for this server
        if self.entitlements:
            entList = self.entitlements.find(serverName)
        else:
            entList = []

        if singleEnt and singleEnt[1:] not in entList:
            entList.append(singleEnt[1:])

        usedMap = url is not None
        if url is None:
            if entList or userInfo:
                # if we have authentication information, use https
                protocol = 'https'
            else:
                # if we are using anonymous, use http
                protocol = 'http'

            if userInfo is None:
                url = "%s://%s/conary/" % (protocol, serverName)
            else:
                url = "%s://%s:%s@%s/conary/"
                url = util.ProtectedString(url   % (protocol,
                                                 quote(userInfo[0]),
                                                 quote(userInfo[1]),
                                                 serverName))
        elif userInfo:
            s = url.split('/')
            if s[1]:
                # catch "http/server/"
                raise errors.OpenError(
                    'Invalid URL "%s" when trying access the %s repository. '
                    'Check your repositoryMap entries' % (url, serverName))
            s[2] = ('%s:%s@' % (quote(userInfo[0]), quote(userInfo[1]))) + s[2]
            url = '/'.join(s)
            usedMap = True

        shareTuple = (url, userInfo, tuple(entList))
        server = self.shareCache.get(shareTuple, None)
        if server is not None:
            self.cache[serverName] = server
            return server

        protocol, uri = urllib.splittype(url)
        transporter = transport.Transport(https = (protocol == 'https'),
                                          proxies = self.proxies,
                                          serverName = serverName)
        transporter.setCompress(True)
        transporter.setEntitlements(entList)
        server = ServerProxy(url, serverName, transporter, self.__getPassword,
                             usedMap = usedMap,
                             entitlementDir = self.entitlementDir,
                             callLog = self.callLog)

        # Avoid poking at __transport
        server._transport = transporter

        try:
            serverVersions = server.checkVersion()
        except errors.InsufficientPermission:
            raise
        except Exception, e:
            if isinstance(e, socket.error):
                errmsg = e[1]
            # includes OS and IO errors
            elif isinstance(e, EnvironmentError):
                errmsg = e.strerror
                # sometimes there is a socket error hiding 
                # inside an IOError!
                if isinstance(errmsg, socket.error):
                    errmsg = errmsg[1]
            else:
                errmsg = str(e)
            url = _cleanseUrl(protocol, url)
            if not errmsg:
                errmsg = '%r' % e
            tb = sys.exc_traceback
            raise errors.OpenError('Error occurred opening repository '
                        '%s: %s' % (url, errmsg)), None, tb

        intersection = set(serverVersions) & set(CLIENT_VERSIONS)
        if not intersection:
            url = _cleanseUrl(protocol, url)
            raise errors.InvalidServerVersion(
                "While talking to repository " + url + ":\n"
                "Invalid server version.  Server accepts client "
                "versions %s, but this client only supports versions %s"
                " - download a valid client from wiki.rpath.com" %
                (",".join([str(x) for x in serverVersions]),
                 ",".join([str(x) for x in CLIENT_VERSIONS])))

        # this is the protocol version we should use when talking
        # to this repository - the maximum we both understand
        server.setProtocolVersion(max(intersection))

        self.cache[serverName] = server
        self.shareCache[shareTuple] = server

	return server

    def getPwPrompt(self):
        return self.pwPrompt

    def getUserMap(self):
        return self.userMap

class NetworkRepositoryClient(xmlshims.NetworkConvertors,
			      repository.AbstractRepository, 
                              trovesource.SearchableTroveSource):
    # Constants for changeset versions
    FILE_CONTAINER_VERSION_FILEID_IDX = \
                            filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX
    FILE_CONTAINER_VERSION_WITH_REMOVES = \
                            filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES
    FILE_CONTAINER_VERSION_NO_REMOVES = \
                            filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES

    # fixme: take a cfg object instead of all these parameters
    def __init__(self, repMap, userMap,
                 localRepository = None, pwPrompt = None,
                 entitlementDir = None, downloadRateLimit = 0,
                 uploadRateLimit = 0, entitlements = None,
                 proxy = None):
        # the local repository is used as a quick place to check for
        # troves _getChangeSet needs when it's building changesets which
        # span repositories. it has no effect on any other operation.
        if pwPrompt is None:
            pwPrompt = lambda x, y: (None, None)

        self.downloadRateLimit = downloadRateLimit
        self.uploadRateLimit = uploadRateLimit

        if proxy:
            self.proxies = proxy
        else:
            self.proxies = None

        if entitlements is None:
            entitlements = conarycfg.EntitlementList()
        elif type(entitlements) == dict:
            newEnts = conarycfg.EntitlementList()
            for (server, (entClass, ent)) in entitlements.iteritems():
                newEnts.addEntitlement(server, ent, entClass = entClass)
            entitlements = newEnts

	self.c = ServerCache(repMap, userMap, pwPrompt, entitlements,
                             proxies = self.proxies,
                             entitlementDir = entitlementDir)
        self.localRep = localRepository

        trovesource.SearchableTroveSource.__init__(self, searchableByType=True)
        self.searchAsRepository()

        self.TROVE_QUERY_ALL = TROVE_QUERY_ALL
        self.TROVE_QUERY_PRESENT = TROVE_QUERY_PRESENT
        self.TROVE_QUERY_NORMAL = TROVE_QUERY_NORMAL

    def __del__(self):
        self.c = None

    def close(self, *args):
        pass

    def open(self, *args):
        pass

    def reopen(self, hostname = None):
        if hostname is None:
            for hostname in self.c.keys():
                del self.c[hostname]
        else:
            del self.c[hostname]

    def getUserMap(self):
        """
        The user/password map can be updated at runtime since we're prompting
        the user for passwords. We may need to get those passwords back out
        again to avoid having to reprompt for passwords.
        """
        return self.c.getUserMap()

    def getPwPrompt(self):
        return self.c.pwPrompt

    def updateMetadata(self, troveName, branch, shortDesc, longDesc = "",
                       urls = [], licenses=[], categories = [],
                       source="local", language = "C"):
        self.c[branch].updateMetadata(troveName, self.fromBranch(branch), shortDesc, longDesc,
                                      urls, licenses, categories, source, language)

    def updateMetadataFromXML(self, troveName, branch, xmlStr):
        doc = xml.dom.minidom.parseString(xmlStr)

        # the only required tag
        shortDesc = str(doc.getElementsByTagName("shortDesc")[0].childNodes[0].data)
       
        # optional tags
        longDesc = ""
        language = "C"
        source = "local"

        node = doc.getElementsByTagName("longDesc")
        if node and node[0].childNodes:
            longDesc = node[0].childNodes[0].data
        node = doc.getElementsByTagName("source")
        if node and node[0].childNodes:
            source = node[0].childNodes[0].data
        node = doc.getElementsByTagName("language")
        if node and node[0].childNodes:
            language = node[0].childNodes[0].data
        
        urls = []
        licenses = []
        categories = []

        for l, tagName in (urls, "url"),\
                          (licenses, "license"),\
                          (categories, "category"):
            node = doc.getElementsByTagName(tagName)
            for child in node:
                l.append(str(child.childNodes[0].data))
        
        self.c[branch].updateMetadata(troveName, self.fromBranch(branch),
                                      shortDesc, longDesc,
                                      urls, licenses, categories,
                                      source, language)

    def getMetadata(self, troveList, label, language="C"):
        if type(troveList[0]) is str:
            troveList = [troveList]

        frozenList = []
        for trove in troveList:
            branch = self.fromBranch(trove[1])
            if len(trove) == 2:
                version = ""
            else:
                version = self.fromBranch(trove[2])
            item = (trove[0], branch, version)
            frozenList.append(item)
         
        mdDict = {}
        md = self.c[label].getMetadata(frozenList, language)
        for troveName, md in md.items():
            mdDict[troveName] = metadata.Metadata(md)
        return mdDict

    def addUser(self, label, user, newPassword):
        # the label just identifies the repository to create the user in
        self.c[label].addUser(user, newPassword)

    def addUserByMD5(self, label, user, salt, password):
        #Base64 encode salt
        self.c[label].addUserByMD5(user, base64.encodestring(salt), password)

    def addAccessGroup(self, label, groupName):
        return self.c[label].addAccessGroup(groupName)

    def addDigitalSignature(self, name, version, flavor, digsig):
        if self.c[version].getProtocolVersion() < 45:
            raise InvalidServerVersion, "Cannot sign troves on Conary " \
                    "repositories older than 1.1.20"

        encSig = base64.b64encode(digsig.freeze())
        self.c[version].addDigitalSignature(name, self.fromVersion(version),
                                            self.fromFlavor(flavor),
                                            encSig)

    def addMetadataItems(self, itemList):
        byServer = {}
        for (name, version, flavor), item in itemList:
            l = byServer.setdefault(version.getHost(), [])
            l.append(
                ((name, self.fromVersion(version), self.fromFlavor(flavor)),
                 base64.b64encode(item.freeze())))
        for server in byServer.keys():
            s = self.c[version]
            if s.getProtocolVersion() < 47:
                raise InvalidServerVersion, "Cannot add metadata to troves on " \
                      "repositories older than 1.1.24"
        for server in byServer.keys():
            s = self.c[server]
            s.addMetadataItems(byServer[server])

    def addNewAsciiPGPKey(self, label, user, keyData):
        self.c[label].addNewAsciiPGPKey(user, keyData)

    def addNewPGPKey(self, label, user, keyData):
        encKeyData = base64.b64encode(keyData)
        self.c[label].addNewPGPKey(user, encKeyData)

    def getAsciiOpenPGPKey(self, label, keyId):
        return self.c[label].getAsciiOpenPGPKey(keyId)

    def listUsersMainKeys(self, label, userId):
        return self.c[label].listUsersMainKeys(userId)

    def listSubkeys(self, label, fingerprint):
        return self.c[label].listSubkeys(fingerprint)

    def getOpenPGPKeyUserIds(self, label, keyId):
        return self.c[label].getOpenPGPKeyUserIds(keyId)

    def changePGPKeyOwner(self, label, user, key):
        self.c[label].changePGPKeyOwner(user, key)

    def deleteUserByName(self, label, user):
        self.c[label].deleteUserByName(user)

    def deleteUserById(self, label, userId):
        self.c[label].deleteUserById(userId)

    def deleteAccessGroup(self, label, groupName):
        self.c[label].deleteAccessGroup(groupName)

    def updateAccessGroupMembers(self, label, groupName, members):
        self.c[label].updateAccessGroupMembers(groupName, members)

    def setUserGroupCanMirror(self, reposLabel, userGroup, canMirror):
        self.c[reposLabel].setUserGroupCanMirror(userGroup, canMirror)

    def setUserGroupIsAdmin(self, reposLabel, userGroup, admin):
        self.c[reposLabel].setUserGroupIsAdmin(userGroup, admin)

    def addTroveAccess(self, role, troveList):
        byServer = {}
        for tup in troveList:
            l = byServer.setdefault(tup[1].trailingLabel().getHost(), [])
            l.append( (tup[0], self.fromVersion(tup[1]),
                       self.fromFlavor(tup[2])) )

        for serverName, troveList in byServer.iteritems():
            self.c[serverName].addTroveAccess(role, troveList)

    def deleteTroveAccess(self, role, troveList):
        byServer = {}
        for tup in troveList:
            l = byServer.setdefault(tup[1].trailingLabel().getHost(), [])
            l.append( (tup[0], self.fromVersion(tup[1]),
                       self.fromFlavor(tup[2])) )

        for serverName, troveList in byServer.iteritems():
            self.c[serverName].deleteTroveAccess(role, troveList)

    def listTroveAccess(self, serverName, role):
        return [ ( x[0], self.toVersion(x[1]), self.toFlavor(x[2]) ) for x in
                            self.c[serverName].listTroveAccess(role) ]

    def listAcls(self, reposLabel, userGroup):
        return self.c[reposLabel].listAcls(userGroup)

    def addAcl(self, reposLabel, userGroup, trovePattern, label, write = False,
               remove = False):
        if self.c[reposLabel].getProtocolVersion() < 60:
            raise errors.InvalidServerVersion(
                    "addAcl only works on Conary 2.0 and later")

        if not label:
            label = "ALL"
        elif type(label) == str:
            pass
        else:
            label = self.fromLabel(label)

        if not trovePattern:
            trovePattern = "ALL"

        self.c[reposLabel].addAcl(userGroup, trovePattern, label,
                                  write = write, remove = remove)

        return True

    def editAcl(self, reposLabel, userGroup, oldTrovePattern, oldLabel,
                trovePattern, label, write = False, canRemove = False):
        if self.c[reposLabel].getProtocolVersion() < 60:
            raise errors.InvalidServerVersion(
                    "editAcl only works on Conary 2.0 and later")

        if not label:
            label = "ALL"
        elif type(label) == str:
            pass
        else:
            label = self.fromLabel(label)

        if not oldLabel:
            oldLabel = "ALL"
        elif type(oldLabel) == str:
            pass
        else:
            oldLabel = self.fromLabel(oldLabel)

        if not trovePattern:
            trovePattern = "ALL"

        if not oldTrovePattern:
            oldTrovePattern = "ALL"

        self.c[reposLabel].editAcl(userGroup, oldTrovePattern, oldLabel,
                                   trovePattern, label, write = write,
                                   canRemove = canRemove)

        return True

    def deleteAcl(self, reposLabel, userGroup, trovePattern, label):
        if not label:
            label = "ALL"
        elif type(label) == str:
            pass
        else:
            label = self.fromLabel(label)

        if not trovePattern:
            trovePattern = "ALL"

        self.c[reposLabel].deleteAcl(userGroup, trovePattern, label)
        return True

    def changePassword(self, label, user, newPassword):
        self.c[label].changePassword(user, newPassword)

    def getUserGroups(self, label):
        return self.c[label].getUserGroups()

    def addEntitlements(self, serverName, entGroup, entitlements):
        entitlements = [ self.fromEntitlement(x) for x in entitlements ]
        return self.c[serverName].addEntitlements(entGroup, entitlements)

    def deleteEntitlements(self, serverName, entGroup, entitlements):
        entitlements = [ self.fromEntitlement(x) for x in entitlements ]
        return self.c[serverName].deleteEntitlements(entGroup, entitlements)

    def addEntitlementGroup(self, serverName, entGroup, userGroup):
        return self.c[serverName].addEntitlementGroup(entGroup, userGroup)

    def deleteEntitlementGroup(self, serverName, entGroup):
        return self.c[serverName].deleteEntitlementGroup(entGroup)

    def addEntitlementOwnerAcl(self, serverName, userGroup, entGroup):
        return self.c[serverName].addEntitlementOwnerAcl(userGroup, entGroup)

    def deleteEntitlementOwnerAcl(self, serverName, userGroup, entGroup):
        return self.c[serverName].deleteEntitlementOwnerAcl(userGroup, entGroup)

    def listEntitlements(self, serverName, entGroup):
        l = self.c[serverName].listEntitlements(entGroup)
        return [ self.toEntitlement(x) for x in l ]

    def listEntitlementGroups(self, serverName):
        return self.c[serverName].listEntitlementGroups()

    def getEntitlementClassAccessGroup(self, serverName, classList):
        return self.c[serverName].getEntitlementClassAccessGroup(classList)

    def setEntitlementClassAccessGroup(self, serverName, classInfo):
        return self.c[serverName].setEntitlementClassAccessGroup(classInfo)

    def listAccessGroups(self, serverName):
        return self.c[serverName].listAccessGroups()

    def troveNames(self, label, troveTypes = TROVE_QUERY_PRESENT):
        if self.c[label].getProtocolVersion() < 60:
            return self.c[label].troveNames(self.fromLabel(label))

        return self.c[label].troveNames(self.fromLabel(label),
                                        troveTypes = troveTypes)

    def troveNamesOnServer(self, server, troveTypes = TROVE_QUERY_PRESENT):
        if self.c[server].getProtocolVersion() < 60:
            return self.c[server].troveNames("")

        return self.c[server].troveNames("", troveTypes = troveTypes)

    def getTroveLeavesByPath(self, pathList, label):
        l = self.c[label].getTrovesByPaths(pathList, self.fromLabel(label), 
                                           False)
        return dict([ (x[0],
                        [(y[0], self.thawVersion(y[1]), self.toFlavor(y[2])) 
                         for y in x[1]]) for x in itertools.izip(pathList, l) ])

    def getTroveVersionsByPath(self, pathList, label):
        l = self.c[label].getTrovesByPaths(pathList, self.fromLabel(label), True)
        return dict([ (x[0], 
                       [(y[0], self.thawVersion(y[1]), self.toFlavor(y[2])) 
                         for y in x[1]]) for x in itertools.izip(pathList, l) ])
 
    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
        # XXX this code should most likely go away, and anything that
        # uses it should be written to use other functions
        l = [(troveName, (None, None), (version, flavor), True)]
        cs = self._getChangeSet(l, recurse = False, withFiles = True,
                                withFileContents = False)
        try:
            trvCs = cs.getNewTroveVersion(troveName, version, flavor)
        except KeyError:
            raise StopIteration

        t = trove.Trove(trvCs, skipIntegrityChecks = not withFiles)
        # if we're sorting, we'll need to pull out all the paths ahead
        # of time.  We'll use a generator that returns the items
        # in the same order as iterFileList() to reuse code.
        if sortByPath:
            pathDict = {}
            for pathId, path, fileId, version in t.iterFileList():
                pathDict[path] = (pathId, fileId, version)
            paths = pathDict.keys()
            paths.sort()
            def rearrange(paths, pathDict):
                for path in paths:
                    (pathId, fileId, version) = pathDict[path]
                    yield (pathId, path, fileId, version)
            generator = rearrange(paths, pathDict)
        else:
            generator = t.iterFileList()
        for pathId, path, fileId, version in generator:
            if withFiles:
                fileStream = files.ThawFile(cs.getFileChange(None, fileId),
                                            pathId)
                yield (pathId, path, fileId, version, fileStream)
            else:
                yield (pathId, path, fileId, version)

    def _mergeTroveQuery(self, resultD, response):
        for troveName, troveVersions in response.iteritems():
            if not resultD.has_key(troveName):
                resultD[troveName] = {}
            for versionStr, flavors in troveVersions.iteritems():
                version = self.thawVersion(versionStr)
                resultD[troveName][version] = \
                            [ self.toFlavor(x) for x in flavors ]

        return resultD

    def _setTroveTypeArgs(self, serverIdent, *args, **kwargs):
        if self.c[serverIdent].getProtocolVersion() >= 38:
            return args + ( kwargs.get('troveTypes', TROVE_QUERY_PRESENT), )
        else:
            return args

    def getAllTroveLeaves(self, serverName, troveNameList,
                          troveTypes = TROVE_QUERY_PRESENT):
        req = {}
        for name, flavors in troveNameList.iteritems():
            if name is None:
                name = ''

            if flavors is None:
                req[name] = True
            else:
                req[name] = [ self.fromFlavor(x) for x in flavors ]

        d = self.c[serverName].getAllTroveLeaves(
                        *self._setTroveTypeArgs(serverName, req,
                                                troveTypes = troveTypes))

        result = self._mergeTroveQuery({}, d)

        # filter the result by server name; repositories hosting multiple
        # server names will return results for all server names the user
        # is allowed to see
        for versionDict in result.itervalues():
            for version in versionDict.keys():
                if version.trailingLabel().getHost() != serverName:
                    del versionDict[version]

        for name, versionDict in result.items():
            if not versionDict:
                del result[name]

        return result

    def getTroveVersionList(self, serverName, troveNameList,
                            troveTypes = TROVE_QUERY_PRESENT):
        req = {}
        for name, flavors in troveNameList.iteritems():
            if name is None:
                name = ''

            if flavors is None:
                req[name] = True
            else:
                req[name] = [ self.fromFlavor(x) for x in flavors ]

        d = self.c[serverName].getTroveVersionList(
                        *self._setTroveTypeArgs(serverName, req,
                                                troveTypes = troveTypes))
        return self._mergeTroveQuery({}, d)

    def getTroveLeavesByLabel(self, troveSpecs, bestFlavor = False,
                              troveTypes = TROVE_QUERY_PRESENT):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor, 
                                           'getTroveLeavesByLabel', 
                                           labels = True,
                                           troveTypes = troveTypes,
                                           getLeaves = True,
                                           splitByBranch = True)

    def getTroveLatestByLabel(self, troveSpecs, bestFlavor = False,
                              troveTypes = TROVE_QUERY_PRESENT):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor,
                                           'getTroveLeavesByLabel',
                                           labels = True,
                                           troveTypes = troveTypes,
                                           getLeaves = True)



    def getTroveVersionsByLabel(self, troveSpecs, bestFlavor = False,
                                troveTypes = TROVE_QUERY_PRESENT):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor, 
                                           'getTroveVersionsByLabel', 
                                           labels = True,
                                           troveTypes = troveTypes)

    def getTroveVersionFlavors(self, troveSpecs, bestFlavor = False,
                               troveTypes = TROVE_QUERY_PRESENT):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor,
                                           'getTroveVersionFlavors',
                                           versions = True,
                                           troveTypes = troveTypes)

    def getAllTroveFlavors(self, troveDict):
        d = {}
        for name, versionList in troveDict.iteritems():
            d[name] = {}.fromkeys(versionList, [ None ])

	return self.getTroveVersionFlavors(d)

    def _getTroveInfoByVerInfo(self, troveSpecs, bestFlavor, method, 
                               branches = False, labels = False, 
                               versions = False, 
                               troveTypes = TROVE_QUERY_PRESENT,
                               getLeaves = False, splitByBranch = False):
        assert(branches + labels + versions == 1)

        d = {}
        for name, verSet in troveSpecs.iteritems():
            if not name:
                name = ""

            for ver, flavors in verSet.iteritems():
                host = ver.getHost()
                if branches:
                    verStr = self.fromBranch(ver)
                elif versions:
                    verStr = self.fromVersion(ver)
                else:
                    verStr = self.fromLabel(ver)

                versionDict = d.setdefault(host, {})
                flavorDict = versionDict.setdefault(name, {})

                flavorDict[verStr] = ''

        result = {}
	if not d:
	    return result

        for host, requestD in d.iteritems():
            respD = self.c[host].__getattr__(method)(
                            *self._setTroveTypeArgs(host, requestD,
                                                    bestFlavor,
                                                    troveTypes = troveTypes))
            self._mergeTroveQuery(result, respD)
        if not result:
            return result
        scoreCache = {}
        filteredResult = {}
        for name, versionFlavorDict in result.iteritems():
            if branches:
                keyFn = lambda version: version.branch()
            elif labels:
                keyFn = lambda version: version.trailingLabel()
            elif versions:
                keyFn = lambda version: version
            resultsByKey = {}
            for version, flavorList in versionFlavorDict.iteritems():
                key = keyFn(version)
                if key not in resultsByKey:
                    resultsByKey[key] = {}
                resultsByKey[key][version] = flavorList
            if getLeaves:
                latestFilter = trovesource._GET_TROVE_VERY_LATEST
            else:
                latestFilter = trovesource._GET_TROVE_ALL_VERSIONS

            if bestFlavor:
                flavorFilter = trovesource._GET_TROVE_BEST_FLAVOR
            else:
                flavorFilter = trovesource._GET_TROVE_ALL_FLAVORS
            flavorCheck = trovesource._CHECK_TROVE_REG_FLAVOR

            if name in troveSpecs:
                queryDict = troveSpecs[name]
            elif '' in troveSpecs:
                queryDict =  troveSpecs['']
            elif None in troveSpecs:
                queryDict = troveSpecs[None]

            for versionQuery, flavorQueryList in queryDict.iteritems():
                versionFlavorDict = resultsByKey.get(versionQuery, None)
                if not versionFlavorDict:
                    continue
                self._filterResultsByFlavor(name, filteredResult,
                                            versionFlavorDict,
                                            flavorQueryList, flavorFilter,
                                            flavorCheck, latestFilter,
                                            scoreCache, 
                                            splitByBranch=splitByBranch)
        return filteredResult

    def getTroveLeavesByBranch(self, troveSpecs, bestFlavor = False,
                               troveTypes = TROVE_QUERY_PRESENT):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor, 
                                           'getTroveLeavesByBranch', 
                                           branches = True,
                                           troveTypes = troveTypes,
                                           getLeaves=True)

    def getTroveVersionsByBranch(self, troveSpecs, bestFlavor = False,
                                 troveTypes = TROVE_QUERY_PRESENT):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor, 
                                           'getTroveVersionsByBranch', 
                                           branches = True,
                                           troveTypes = troveTypes)

    def getTroveLatestVersion(self, troveName, branch,
                              troveTypes = TROVE_QUERY_PRESENT):
	b = self.fromBranch(branch)
        v = self.c[branch].getTroveLatestVersion(
                            *self._setTroveTypeArgs(branch, troveName, b,
                                                    troveTypes = troveTypes))
        if v == 0:
            raise errors.TroveMissing(troveName, branch)
	return self.thawVersion(v)

    # added at protocol version 43
    def getTroveReferences(self, serverName, troveInfoList):
        if not troveInfoList:
            return []
        # if the server can't talk to us, don't traceback
        if self.c[serverName].getProtocolVersion() < 43:
            return []
        ret = self.c[serverName].getTroveReferences(
            [(n,self.fromVersion(v),self.fromFlavor(f)) for (n,v,f) in troveInfoList])
        return [ [(n,self.toVersion(v),self.toFlavor(f)) for (n,v,f) in retl] for retl in ret ]

    # added at protocol version 43
    def getTroveDescendants(self, serverName, troveList):
        if not troveList:
            return []
        # if the server can't talk to us, don't traceback
        if self.c[serverName].getProtocolVersion() < 43:
            return []
        ret = self.c[serverName].getTroveDescendants(
            [(n,self.fromBranch(b),self.fromFlavor(f)) for (n,b,f) in troveList])
        return [ [(self.toVersion(v), self.toFlavor(f)) for (v,f) in retl] for retl in ret ]

    def hasTrove(self, name, version, flavor):
        return self.hasTroves([(name, version, flavor)])[name, version, flavor]

    def hasTroves(self, troveInfoList, hidden = False):
        if not troveInfoList:
            return {}
        byServer = {}
        for name, version, flavor in troveInfoList:
            l = byServer.setdefault(version.getHost(), [])
            l.append(((name, version, flavor),
                      (name, self.fromVersion(version), 
                             self.fromFlavor(flavor))))

        d = {}
        for server, l in byServer.iteritems():
            if server == 'local':
                exists = [False] * len(l)
            else:
                if hidden and self.c[server].getProtocolVersion() >= 46:
                    args = [ hidden ]
                else:
                    # older servers didn't support hidden troves
                    args = []

                exists = self.c[server].hasTroves([x[1] for x in l], *args)
            d.update(dict(itertools.izip((x[0] for x in l), exists)))

        return d

    def getTrove(self, troveName, troveVersion, troveFlavor, withFiles = True,
                 callback = None):
	rc = self.getTroves([(troveName, troveVersion, troveFlavor)],
                            withFiles = withFiles, callback = callback)
	if rc[0] is None:
	    raise errors.TroveMissing(troveName, version = troveVersion)

	return rc[0]

    def getTroves(self, troves, withFiles = True, callback = None):
        """
        @param troves: List of troves to be retrieved
        @type troves: list
        @param withFiles: If set (default), retrieve files.
        @type withFiles: bool
        @raise RepositoryError: if a repository error occurred.
        """
        if not troves:
            return []
	chgSetList = []
	for (name, version, flavor) in troves:
	    chgSetList.append((name, (None, None), (version, flavor), True))

	cs = self._getChangeSet(chgSetList, recurse = False, 
                                withFiles = withFiles,
                                withFileContents = False, 
                                callback = callback)

	l = []
        # walk the list so we can return the troves in the same order
        for (name, version, flavor) in troves:
            try:
                troveCs = cs.getNewTroveVersion(name, version, flavor)
            except KeyError:
                l.append(None)
                continue

            # trove integrity checks don't work when file information is
            # excluded
            t = trove.Trove(troveCs, skipIntegrityChecks = not withFiles) 
            l.append(t)

	return l

    def getChangeSetSize(self, jobList):
        # make sure all of the jobs are on the same server
        verSet = set()
        wireJobs = []
        for name, (oldVersion, oldFlavor), (newVersion, newFlavor), abs \
                                                            in jobList:
            if newVersion is None:
                continue

            if oldVersion:
                verSet.add(oldVersion)
                oldVersion = oldVersion.asString()
                oldFlavor = oldFlavor.freeze()
            else:
                oldVersion = 0
                oldFlavor = 0

            verSet.add(newVersion)
            newVersion = newVersion.asString()
            newFlavor = newFlavor.freeze()

            wireJobs.append( (name, (oldVersion, oldFlavor),
                                    (newVersion, newFlavor), abs) )

        if not self.c.singleServer(*verSet):
            raise errors.CannotCalculateDownloadSize('job on multiple servers')

        server = self.c[jobList[0][2][0]]

        if server.getProtocolVersion() >= 51:
            infoList = server.getChangeSet(wireJobs, False, True, True,
                           False, filecontainer.FILE_CONTAINER_VERSION_LATEST,
                           False, True)
        elif server.getProtocolVersion() < 50:
            raise errors.CannotCalculateDownloadSize('repository too old')
        else:
            infoList = server.getChangeSet(wireJobs, False, True, True,
                           False, filecontainer.FILE_CONTAINER_VERSION_LATEST,
                           False)

        sizeList = [ x[0] for x in infoList[1] ]
        jobSizes = []
        for singleJob in jobList:
            totalSize = 0
            if singleJob[2][0] is not None:
                totalSize += int(sizeList.pop(0))

            jobSizes.append(totalSize)

        return jobSizes

    def createChangeSet(self, jobList, withFiles = True,
                        withFileContents = True,
                        excludeAutoSource = False, recurse = True,
                        primaryTroveList = None, callback = None):
        """
        @raise RepositoryError: if a repository error occurred.
        """
        allJobs = [ (jobList, False) ]
        mergeTarget = None

        while allJobs:
            fullJob, forceLocal = allJobs.pop(0)

            try:
                cs = self._getChangeSet(fullJob, withFiles = withFiles, 
                                        withFileContents = withFileContents,
                                        excludeAutoSource = excludeAutoSource,
                                        recurse = recurse,
                                        primaryTroveList = primaryTroveList,
                                        callback = callback,
                                        forceLocalGeneration = forceLocal)

                if mergeTarget is None:
                    return cs

                mergeTarget.merge(cs)
            except errors.TroveMissing, e:
                if forceLocal:
                    # trying again won't help
                    raise

                # Split the job into two pieces. This will force local
                # generation more agressively than is absolutely necessary
                # (since TroveMissing doesn't convey flavor information)
                brokenJob = []
                workingJob = []
                for job in fullJob:
                    if job[0] == e.troveName and                    \
                          (job[1][0] == e.version or job[2][0] == e.version):
                        brokenJob.append(job)
                    else:
                        workingJob.append(job)

                if not brokenJob:
                    # we can't figure out what exactly is broken -
                    # it's included implicitly due to recurse.
                    raise

                allJobs.append( (brokenJob, True) )
                allJobs.append( (workingJob, False) )

                if mergeTarget is None:
                    mergeTarget = changeset.ReadOnlyChangeSet()

        return mergeTarget

    def createChangeSetFile(self, jobList, fName, recurse = True,
                            primaryTroveList = None, callback = None,
                            changesetVersion = None,
                            mirrorMode = False):
        """
        @param changesetVersion: (optional) request a specific changeset
            version from the server. The value is one of the FILE_CONTAINER_*
            constants defined in the NetworkRepositoryClient class.
        @raise FilesystemError: if the destination file is not writable
        @raise RepositoryError: if a repository error occurred.
        """

        # mirrorMode forces contents to be included whenever the fileId
        # changes; normally they change only if the sha1 changes
        return self._getChangeSet(jobList, target = fName,
                                  recurse = recurse,
                                  primaryTroveList = primaryTroveList,
                                  callback = callback,
                                  changesetVersion = changesetVersion,
                                  mirrorMode = mirrorMode)

    def _getChangeSet(self, chgSetList, recurse = True, withFiles = True,
		      withFileContents = True, target = None,
                      excludeAutoSource = False, primaryTroveList = None,
                      callback = None, forceLocalGeneration = False,
                      changesetVersion = None, mirrorMode = False):
        # This is a bit complicated due to servers not wanting to talk
        # to other servers. To make this work, we do this:
        #
        #   1. Split the list of change set requests into ones for
        #   remote servers (by server) and ones we need to generate
        #   locally
        #
        #   2. Get the changesets from the remote servers. This also
        #   gives us lists of other changesets we need (which need
        #   to be locally generated, or the repository server would
        #   have created them for us). 
        #
        #   3. Create the local changesets. Doing this could well
        #   result in our needing changesets which we're better off
        #   generating on a server.
        #
        #   4. If more changesets are needed (from step 3) go to
        #   step 2.
        #
        #   5. Download any extra files (and create any extra diffs)
        #   which step 2 couldn't do for us.

        def _separateJobList(jobList, removedList, forceLocalGeneration,
                             mirrorMode):
            if forceLocalGeneration:
                return {}, jobList

            serverJobs = {}
            ourJobList = []
            for (troveName, (old, oldFlavor), (new, newFlavor), absolute) in \
                    jobList:
                if not new:
                    # XXX does doing this on the client get recursion right?
                    ourJobList.append((troveName, (old, oldFlavor),
                                       (new, newFlavor), absolute))
                    continue

                serverName = new.getHost()
                if old and mirrorMode and \
                            self.c[serverName].getProtocolVersion() < 49:
                    # old clients don't support mirrorMode argument; force
                    # local changeset generation (but only for relative
                    # change sets)
                    ourJobList.append((troveName, (old, oldFlavor),
                                       (new, newFlavor), absolute))
                elif old:
                    if self.c.singleServer(old, new):
                        l = serverJobs.setdefault(serverName, [])
                        l.append((troveName, 
                                  (self.fromVersion(old), 
                                   self.fromFlavor(oldFlavor)), 
                                  (self.fromVersion(new), 
                                   self.fromFlavor(newFlavor)),
                                  absolute))
                    else:
                        ourJobList.append((troveName, (old, oldFlavor),
                                           (new, newFlavor), absolute))
                else:
                    l = serverJobs.setdefault(serverName, [])
                    l.append((troveName, 
                              (0, 0),
                              (self.fromVersion(new), 
                               self.fromFlavor(newFlavor)),
                              absolute))

            ourJobList += removedList

            return (serverJobs, ourJobList)

        def _cvtTroveList(l):
            new = []
            for (name, (oldV, oldF), (newV, newF), absolute) in l:
                if oldV == 0:
                    oldV = None
                    oldF = None
                else:
                    oldV = self.toVersion(oldV)
                    oldF = self.toFlavor(oldF)

                if newV == 0:
                    newV = None
                    newF = None
                else:
                    newV = self.toVersion(newV)
                    newF = self.toFlavor(newF)

                new.append((name, (oldV, oldF), (newV, newF), absolute))

            return new

        def _cvtFileList(l):
            new = []
            for (pathId, troveName, (oldTroveV, oldTroveF, oldFileId, oldFileV),
                                    (newTroveV, newTroveF, newFileId, newFileV)) in l:
                if oldTroveV == 0:
                    oldTroveV = None
                    oldFileV = None
                    oldFileId = None
                    oldTroveF = None
                else:
                    oldTroveV = self.toVersion(oldTroveV)
                    oldFileV = self.toVersion(oldFileV)
                    oldFileId = self.toFileId(oldFileId)
                    oldTroveF = self.toFlavor(oldTroveF)

                newTroveV = self.toVersion(newTroveV)
                newFileV = self.toVersion(newFileV)
                newFileId = self.toFileId(newFileId)
                newTroveF = self.toFlavor(newTroveF)

                pathId = self.toPathId(pathId)

                new.append((pathId, troveName, 
                               (oldTroveV, oldTroveF, oldFileId, oldFileV),
                               (newTroveV, newTroveF, newFileId, newFileV)))

            return new

        def _getLocalTroves(troveList):
            if not self.localRep or not troveList:
                return [ None ] * len(troveList)

            return self.localRep.getTroves(troveList, pristine=True)

        def _getCsFromRepos(target, cs, server, job, recurse,
                            withFiles, withFileContents,
                            excludeAutoSource, filesNeeded,
                            chgSetList, removedList, changesetVersion,
                            mirrorMode):
            abortCheck = None
            if callback:
                callback.requestingChangeSet()
            server.setAbortCheck(abortCheck)
            args = (job, recurse, withFiles, withFileContents,
                    excludeAutoSource)
            serverVersion = server.getProtocolVersion()

            if mirrorMode and serverVersion >= 49:
                if not changesetVersion:
                    changesetVersion = \
                        filecontainer.FILE_CONTAINER_VERSION_LATEST

                args += (changesetVersion, mirrorMode, )
            elif changesetVersion and serverVersion > 47:
                args += (changesetVersion, )

            l = server.getChangeSet(*args)
            if serverVersion >= 50:
                url = l[0]
                sizes = [ x[0] for x in l[1] ]
                extraTroveList = [ x for x in itertools.chain(
                                    *[ x[1] for x in l[1] ] ) ]
                extraFileList = [ x for x in itertools.chain(
                                    *[ x[2] for x in l[1] ] ) ]
                removedTroveList = [ x for x in itertools.chain(
                                    *[ x[3] for x in l[1] ] ) ]
            elif serverVersion < 38:
                (url, sizes, extraTroveList, extraFileList) = l
                removedTroveList = []
            else:
                (url, sizes, extraTroveList,
                 extraFileList, removedTroveList) = l
            # ensure that sizes are integers.  protocol version 44 and
            # later sends them as strings instead of ints due to the 2
            # GiB limitation
            sizes = [ int(x) for x in sizes ]
            server.setAbortCheck(None)

            chgSetList += _cvtTroveList(extraTroveList)
            filesNeeded.update(_cvtFileList(extraFileList))
            removedList += _cvtTroveList(removedTroveList)

            # FIXME: This check is for broken conary proxies that
            # return a URL with "localhost" in it.  The proxy will know
            # how to handle that.  So, we force the url to be reinterpreted
            # by the proxy no matter what.
            forceProxy = server.usedProxy()
            try:
                inF = transport.ConaryURLOpener(proxies = self.proxies,
                                                forceProxy=forceProxy).open(url)
            except transport.TransportError, e:
                raise errors.RepositoryError(*e.args)

            if callback:
                wrapper = callbacks.CallbackRateWrapper(
                    callback, callback.downloadingChangeSet,
                    sum(sizes))
                copyCallback = wrapper.callback
                abortCheck = callback.checkAbort
            else:
                copyCallback = None
                abortCheck = None

            # seek to the end of the file
            outFile.seek(0, 2)
            start = outFile.tell()
            totalSize = util.copyfileobj(inF, outFile,
                                         callback = copyCallback,
                                         abortCheck = abortCheck,
                                         rateLimit = self.downloadRateLimit)

            # attempt to remove temporary local files
            # possibly created by a shim client
            if os.path.exists(url) and os.access(url, os.W_OK):
                os.unlink(url)

            if totalSize == None:
                raise errors.RepositoryError("Unknown error downloading changeset")
            assert(totalSize == sum(sizes))
            inF.close()

            for size in sizes:
                f = util.SeekableNestedFile(outFile, size, start)
                newCs = changeset.ChangeSetFromFile(f)

                if not cs:
                    cs = newCs
                else:
                    cs.merge(newCs)

                totalSize -= size
                start += size

            assert(totalSize == 0)
            return (cs, _cvtTroveList(extraTroveList),
                    _cvtFileList(extraFileList))

        def _getCsFromShim(target, cs, server, job, recurse, withFiles,
                           withFileContents, excludeAutoSource,
                           filesNeeded, chgSetList, removedList):
            (newCs, extraTroveList, extraFileList, removedList) = \
                  server.getChangeSetObj(job, recurse,
                                         withFiles, withFileContents,
                                         excludeAutoSource)
            if not cs:
                cs = newCs
            else:
                cs.merge(newCs)
            return cs, extraTroveList, extraFileList

        if not chgSetList:
            # no need to work hard to find this out
            return changeset.ReadOnlyChangeSet()

        # make sure the absolute flag isn't set for any differential change
        # sets
        assert(not [ x for x in chgSetList if (x[1][0] and x[-1]) ])

        cs = None
        scheduledSet = {}
        internalCs = None
        filesNeeded = set()
        removedList = []

        if target:
            try:
                outFile = util.ExtendedFile(target, "w+", buffering = False)
            except IOError, e:
                strerr = "Error writing to file %s: %s" % (e.filename,
                    e.strerror)
                raise errors.FilesystemError(e.errno, e.filename, e.strerror,
                    strerr)
        else:
            (outFd, tmpName) = util.mkstemp(suffix = '.ccs')
            outFile = util.ExtendedFile(tmpName, "w+", buffering = False)
            os.close(outFd)
            os.unlink(tmpName)

        if primaryTroveList is None:
            # (name, version, release) list. removed troves aren't primary
            primaryTroveList = [ (x[0], x[2][0], x[2][1]) for x in chgSetList 
                                        if x[2][0] is not None ]

        while chgSetList or removedList:
            (serverJobs, ourJobList) = _separateJobList(chgSetList,
                                                        removedList,
                                                        forceLocalGeneration,
                                                        mirrorMode)

            chgSetList = []
            removedList = []

            for serverName, job in serverJobs.iteritems():
                server = self.c[serverName]
                args = (target, cs, server, job, recurse, withFiles,
                        withFileContents, excludeAutoSource,
                        filesNeeded, chgSetList, removedList)

                try:
                    if server.__class__ == ServerProxy:
                        # this is a XML-RPC proxy for a remote repository
                        rc = _getCsFromRepos(*(args + (changesetVersion,
                                                       mirrorMode)))
                    else:
                        # assume we are a shim repository
                        rc = _getCsFromShim(*args)
                    cs, extraTroveList, extraFileList = rc
                except Exception:
                    if target and os.path.exists(target):
                        os.unlink(target)
                    elif os.path.exists(tmpName):
                        os.unlink(tmpName)
                    raise

                chgSetList += extraTroveList
                filesNeeded.update(extraFileList)

            if (ourJobList or filesNeeded) and not internalCs:
                internalCs = changeset.ChangeSet()

            # Handle everything in ourJobList which is just a deletion. We
            # need timestamped versions for this; only go the repository
            # to get those if the ones we have are not versioned.
            delList = []
            timesNeeded = [ ]
            for i, (troveName, (oldVersion, oldFlavor),
                  (newVersion, newFlavor), absolute) in enumerate(ourJobList):
                if not newVersion:
                    delList.append(((troveName, oldVersion, oldFlavor), i))
                    if not sum(oldVersion.timeStamps()):
                        timesNeeded.append(delList[-1])

            # XXX this is an expensive way to get a version w/ timestamps, but
            # it's easier than other approaches :-(
            trvs = self.getTroves([ x[0] for x in timesNeeded ], 
                                  withFiles = False)
            timeDict = dict(zip([ x[0] for x in timesNeeded ], 
                                [ x.getVersion() for x in trvs ]))

            # this lets us remove from ourJobList from back to front, keeping
            # our indices valid
            delList.reverse()

            for trvInfo, i in delList:
                ver = timeDict.get(trvInfo, trvInfo[1])
                internalCs.oldTrove(trvInfo[0], ver, trvInfo[2])
                del ourJobList[i]
            del delList

            # generate this change set, and put any recursive generation
            # which is needed onto the chgSetList for the next pass
            allTrovesNeeded = []
            for (troveName, (oldVersion, oldFlavor),
                            (newVersion, newFlavor), absolute) in ourJobList:
                if oldVersion is not None:
                    allTrovesNeeded.append((troveName, oldVersion, oldFlavor))
                allTrovesNeeded.append((troveName, newVersion, newFlavor))

            troves = _getLocalTroves(allTrovesNeeded)
            remoteTrovesNeeded = []
            indices = []
            for i, (trove, req) in enumerate(zip(troves, allTrovesNeeded)):
                # don't ask for local troves from a remote server
                if trove is None and not req[1].isOnLocalHost():
                    remoteTrovesNeeded.append(req)
                    indices.append(i)

            remoteTroves = self.getTroves(remoteTrovesNeeded)
            for i, trove in zip(indices, remoteTroves):
                troves[i] = trove

            del allTrovesNeeded, remoteTrovesNeeded, indices, remoteTroves

            i = 0
            for (troveName, (oldVersion, oldFlavor),
                            (newVersion, newFlavor), absolute) in ourJobList:
                if oldVersion is not None:
                    old = troves[i]
                    i += 1
                else:
                    old = None

                new = troves[i]
                i += 1

                # if the old version is marked removed, pretend as though
                # it doesn't exist.
                if old and old.isRemoved():
                    old = None
                (troveChgSet, newFilesNeeded, pkgsNeeded) = \
                                new.diff(old, absolute = absolute)
                # newFilesNeeded = [ (pathId, oldFileVersion, newFileVersion) ]
                filesNeeded.update( ( (x[0], troveName, 
                        (oldVersion, oldFlavor, x[1], x[2]),
                        (newVersion, newFlavor, x[3], x[4]))
                            for x in newFilesNeeded ) )

                if recurse:
                    for (otherTroveName, (otherOldVersion, otherOldFlavor),
                                         (otherNewVersion, otherNewFlavor),
                         otherIsAbsolute) in pkgsNeeded:
                        chgSetList.append((otherTroveName, 
                                           (otherOldVersion, otherOldFlavor),
                                           (otherNewVersion, otherNewFlavor),
                                           absolute))

                internalCs.newTrove(troveChgSet)

        # Files that are missing from upstream
        missingFiles = []

        if withFiles and filesNeeded:
            need = []
            for (pathId, troveName, 
                (oldTroveVersion, oldTroveFlavor, oldFileId, oldFileVersion),
                (newTroveVersion, newTroveFlavor, newFileId, newFileVersion)) \
                                in filesNeeded:
                if oldFileVersion:
                    need.append((pathId, oldFileId, oldFileVersion))
                need.append((pathId, newFileId, newFileVersion))

            # If a callback was passed in, then allow for missing files
            fileObjs = self.getFileVersions(need, lookInLocal = True,
                                            allowMissingFiles = bool(callback))
            fileDict = {}
            for ((pathId, fileId, fileVersion), fileObj) in zip(need, fileObjs):
                fileDict[(pathId, fileId)] = fileObj
            del fileObj, fileObjs, need, fileId

            contentsNeeded = []
            fileJob = []

            for (pathId, troveName, 
                    (oldTroveVersion, oldTroveF, oldFileId, oldFileVersion),
                    (newTroveVersion, newTroveF, newFileId, newFileVersion)) \
                                in filesNeeded:
                if oldFileVersion:
                    oldFileObj = fileDict[(pathId, oldFileId)]
                else:
                    oldFileObj = None

                newFileObj = fileDict[(pathId, newFileId)]
                if newFileObj is None:
                    # File missing from server
                    missingFiles.append((troveName, newTroveVersion, newTroveF, 
                                         pathId, newFileId, newFileVersion))
                    continue

                if mirrorMode:
                    (filecs, hash) = changeset.fileChangeSet(pathId,
                                                             None, 
                                                             newFileObj)
                else:
                    (filecs, hash) = changeset.fileChangeSet(pathId,
                                                             oldFileObj, 
                                                             newFileObj)

		internalCs.addFile(oldFileId, newFileId, filecs)

                if excludeAutoSource and newFileObj.flags.isAutoSource():
                    continue

                if (withFileContents and 
                        ((mirrorMode and newFileObj.hasContents) or hash)):
                    # pull contents from the trove it was originally
                    # built in
                    fetchItems = []
                    needItems = []

                    if (not mirrorMode and 
                                    changeset.fileContentsUseDiff(oldFileObj,
                                                                  newFileObj)):
                        fetchItems.append( (oldFileId, oldFileVersion, 
                                            oldFileObj) ) 
                        needItems.append( (pathId, None, oldFileObj) ) 

                    fetchItems.append( (newFileId, newFileVersion, newFileObj) )
                    needItems.append( (pathId, newFileId, newFileObj) )
                    contentsNeeded += fetchItems

                    fileJob.extend([ needItems ])

            contentList = self.getFileContents(contentsNeeded,
                                               tmpFile = outFile,
                                               lookInLocal = True,
                                               callback = callback,
                                               compressed = True)

            i = 0
            for item in fileJob:
                pathId, fileId, fileObj = item[0]
                contents = contentList[i]
                i += 1

                if len(item) == 1:
                    internalCs.addFileContents(pathId, fileId,
                                   changeset.ChangedFileTypes.file,
                                   contents,
                                   fileObj.flags.isConfig(),
                                   compressed = True)
                else:
                    # Don't bother with diffs. Clients can reconstruct them for
                    # installs and they're just a pain to assemble here anyway.
                    fileId = item[1][1]
                    newFileObj = item[1][2]
                    newContents = contentList[i]
                    i += 1

                    (contType, cont) = changeset.fileContentsDiff(None,
                                            None, newFileObj, newContents,
                                            mirrorMode = mirrorMode)
                    internalCs.addFileContents(pathId, fileId, contType,
                                               cont, True,
                                               compressed = True)

        if not cs and internalCs:
            cs = internalCs
            internalCs = None
        elif cs and internalCs:
            cs.merge(internalCs)

        # convert the versions in here to ones w/ timestamps
        cs.setPrimaryTroveList([])
        oldTroveSet = dict([ (x,x) for x in cs.getOldTroveList() ] )
        for (name, version, flavor) in primaryTroveList:
            if cs.hasNewTrove(name, version, flavor):
                trove = cs.getNewTroveVersion(name, version, flavor)
                cs.addPrimaryTrove(name, trove.getNewVersion(), flavor)
            else:
                cs.addPrimaryTrove(*oldTroveSet[(name, version,flavor)])

        if missingFiles:
            mfs = []
            for mf in missingFiles:
                trvName, trvVersion, trvFlavor = mf[:3]
                trv = cs.getNewTroveVersion(trvName, trvVersion, trvFlavor)
                # Find the file path associated with this missing file
                for pathId, path, fileId, version in trv.getNewFileList():
                    if (pathId, fileId, version) == mf[3:]:
                        break
                else: # for
                    # Unable to find this file
                    raise Exception("Cannot find file in changeset")
                mfs.append((trvName, trvVersion, trvFlavor, 
                            pathId, path, fileId, version))

            # The test for the presence of the callback is redundant, if we
            # have missing files we should have a callback, otherwise
            # getFileVersions would have raised an exception because of the
            # allowMissingFiles flag.
            ret = False
            if callback:
                assert(hasattr(callback, 'missingFiles'))
                ret = callback.missingFiles(mfs)

            # If the callback returns False, or no callback is present, 
            # keep the old behavior of raising the exception
            # Note that the callback can choose to raise an exception itself,
            # in which case this code will not get executed.
            if not ret:
                # Grab just the first file
                mf = mfs[0]
                raise errors.FileStreamMissing(mf[5])

        if target and cs:
            if cs.oldTroves or cs.newTroves:
                os.unlink(target)
                cs.writeToFile(target, versionOverride = changesetVersion)

            cs = None
        elif target:
            os.unlink(target)



	return cs

    def resolveDependencies(self, label, depList, leavesOnly=False):
        l = [ self.fromDepSet(x) for x in depList ]
        if self.c[label].getProtocolVersion() < 43:
            args = ()
        else:
            args = (leavesOnly,)

        d = self.c[label].getDepSuggestions(self.fromLabel(label), l, *args)
        r = {}
        for (key, val) in d.iteritems():
            l = []
            for items in val:
                l.append([ (x[0], self.thawVersion(x[1]), self.toFlavor(x[2]))
                                    for x in items ])

            r[self.toDepSet(key)] = l

        return r

    def resolveDependenciesByGroups(self, groupTroves, depList):
        if not (groupTroves and depList):
            return {}

        seen = []
        notMatching = [ x.getNameVersionFlavor() for x in groupTroves ]

        # here's what we pass to servers: all groups + any troves
        # that are not mentioned by a group on the same host.
        # that should be the minimal set of troves.
        while groupTroves:
            groupsToGet = []
            for group in groupTroves:
                groupHost = group.getVersion().getHost()

                for info in group.iterTroveList(strongRefs=True,
                                                   weakRefs=True):
                    h = info[1].getHost()
                    if groupHost == h:
                        seen.append(info)
                    else:
                        notMatching.append(info)
                        if info[0].startswith('group-'):
                            groupsToGet.append(info)

            if not groupsToGet:
                break
            groupTroves = self.getTroves(groupsToGet)

        # everything in seen was seen by a parent group on the same host
        # and can be skipped
        notMatching = set(notMatching)
        notMatching.difference_update(seen)
        del seen

        # remove all components if their packages are in the set.
        notMatching = set(x for x in notMatching 
              if not ':' in x[0] 
                 or not (x[0].split(':', 1)[0], x[1], x[2]) in notMatching)

        trovesByHost = {}
        for info in notMatching:
            trovesByHost.setdefault(info[1].getHost(), []).append(info)

        frozenDeps = [ self.fromDepSet(x) for x in depList ]

        r = {}
        for host, troveList in trovesByHost.iteritems():
            t = [ self.fromTroveTup(x) for x in set(troveList) ]
            d = self.c[host].getDepSuggestionsByTroves(frozenDeps, t)

            # combine the results for the same dep on different host - 
            # there's no preference of troves on different hosts in a group
            # therefore there can be no preference here.
            for (key, val) in d.iteritems():
                dep = self.toDepSet(key)
                if dep not in r:
                    lst = [ [] for x in val ]
                    r[dep] = lst
                else:
                    lst = r[dep]
                for i, items in enumerate(val):
                    # NOTE: this depends on servers returning the
                    # dependencies in the same order.
                    # That should be true, but there are no assertions
                    # we can make anywhere to ensure it, except perhaps
                    # to ensure that each dependency is resolved when it
                    # is supposed to be.
                    lst[i].extend(self.toTroveTup(x, withTime=True)
                                  for x in items)
        return r


    def getFileVersions(self, fullList, lookInLocal = False,
                        allowMissingFiles = False):
        # if allowMissingFiles is False, a FileStreamMissing error is passed
        # straight down to the client. Otherwise, missing files will have None
        # as their file objects, and callbacks can react to that.
        def getFromServer(server, items, result):
            sentFiles = {}
            for ent in items:
                fileId = ent[1][1]
                if fileId in sentFiles:
                    fl = sentFiles[fileId]
                else:
                    fl = sentFiles[fileId] = []
                fl.append(ent)

            # Special care is required here; the whole set will fail for one
            # missing file, we have to extract it from the list and keep
            # trying.
            while sentFiles:
                # Concatenate all the values in the send list
                templ = []
                for l in sentFiles.values():
                    templ.extend(l)
                templ.sort(lambda a, b: cmp(a[0], b[0]))
                sendL = [ x[1] for x in templ ]
                idxL = [ x[0] for x in templ ]
                try:
                    fileStreams = self.c[server].getFileVersions(sendL)
                except errors.FileStreamMissing, e:
                    if not allowMissingFiles:
                        # Re-raise the exception
                        raise
                    missingFileId = self.fromFileId(e.fileId)
                    if missingFileId not in sentFiles:
                        # This shouldn't happen - the server sent us a file id
                        # that we don't know about
                        raise Exception("Invalid file ID", missingFileId)

                    # Remove this file from the big dictionary and try again
                    del sentFiles[missingFileId]
                    continue
                except errors.OpenError, e:
                    if not allowMissingFiles:
                        # Re-raise the exception
                        raise
                    # No sense in trying the rest of the files, the server is
                    # dead. It's a stiff. Bereft of life.
                    return result

                # Call succeded
                for (fileStream, idx) in zip(fileStreams, idxL):
                    result[idx] = self.toFile(fileStream)
                return result

            # All the files failed
            return result

        if self.localRep and lookInLocal:
            result = [ x for x in self.localRep.getFileVersions(fullList) ]
        else:
            result = [ None ] * len(fullList)

        byServer = {}
        for i, (pathId, fileId, version) in enumerate(fullList):
            if result[i] is not None:
                continue

            server = version.getHost()
            if not byServer.has_key(server):
                byServer[server] = []
            byServer[server].append((i, (self.fromPathId(pathId), 
                                     self.fromFileId(fileId))))
        
        for (server, l) in byServer.iteritems():
            getFromServer(server, l, result)

        return result

    def getFileVersion(self, pathId, fileId, version):
        return self.toFile(self.c[version].getFileVersion(
				   self.fromPathId(pathId), 
				   self.fromFileId(fileId)))

    def getFileContents(self, fileList, tmpFile = None, lookInLocal = False,
                        callback = None, compressed = False):
        contents = [ None ] * len(fileList)

        if self.localRep and lookInLocal:
            for i, item in enumerate(fileList):
                if len(item) < 3: continue

                sha1 = item[2].contents.sha1()
                if self.localRep._hasFileContents(sha1):
                    # retrieve the contents from the database now so that
                    # the changeset can be shared between threads
                    c = self.localRep.getFileContents([item])[0].get().read()
                    if compressed:
                        f = util.BoundedStringIO()
                        compressor = gzip.GzipFile(None, "w", fileobj = f)
                        compressor.write(c)
                        compressor.close()
                        f.seek(0)
                        c = f.read()

                    contents[i] = filecontents.FromString(c)

        byServer = {}

        for i, item in enumerate(fileList):
            if contents[i] is not None:
                continue

            # we try to get the file from the trove which originally contained
            # it since we know that server has the contents; other servers may
            # not
            (fileId, fileVersion) = item[0:2]
            server = fileVersion.getHost()
            l = byServer.setdefault(server, [])
            l.append((i, (fileId, fileVersion)))

        for server, itemList in byServer.iteritems():
            fileList = [ (self.fromFileId(x[1][0]), 
                          self.fromVersion(x[1][1])) for x in itemList ]
            if callback:
                if hasattr(callback, 'requestingFileContentsWithCount'):
                    callback.requestingFileContentsWithCount(len(fileList))
                else:
                    callback.requestingFileContents()
            (url, sizes) = self.c[server].getFileContents(fileList)
            # protocol version 44 and later return sizes as strings rather
            # than ints to avoid 2 GiB limits
            sizes = [ int(x) for x in sizes ]
            assert(len(sizes) == len(fileList))

            # FIXME: This check is for broken conary proxies that
            # return a URL with "localhost" in it.  The proxy will know
            # how to handle that.  So, we force the url to be reinterpreted
            # by the proxy no matter what.
            forceProxy = self.c[server].usedProxy()
            inF = transport.ConaryURLOpener(proxies = self.proxies,
                                            forceProxy=forceProxy).open(url)

            if callback:
                wrapper = callbacks.CallbackRateWrapper(
                    callback, callback.downloadingFileContents, sum(sizes))
                copyCallback = wrapper.callback
            else:
                copyCallback = None

            if tmpFile:
		# make sure we append to the end (creating the gzip file
		# object does a certain amount of seeking through the
		# nested file object which we need to undo
		tmpFile.seek(0, 2)
                start = tmpFile.tell()
                outF = tmpFile
            else:
                (fd, path) = util.mkstemp(suffix = 'filecontents')
                outF = util.ExtendedFile(path, "r+", buffering = False)
                os.close(fd)
                os.unlink(path)
                start = 0

            totalSize = util.copyfileobj(inF, outF,
                                         rateLimit = self.downloadRateLimit,
                                         callback = copyCallback)
            del inF

            for (i, item), size in itertools.izip(itemList, sizes):
                nestedF = util.SeekableNestedFile(outF, size, start)

                totalSize -= size
                start += size

                if compressed:
                    contents[i] = filecontents.FromFile(nestedF)
                else:
                    gzfile = gzip.GzipFile(fileobj = nestedF)
                    contents[i] = filecontents.FromGzFile(gzfile)

            assert(totalSize == 0)

        return contents

    def getPackageBranchPathIds(self, sourceName, branch, filePrefixes=None,
                                fileIds=None):
        """
        Searches all of the troves generated from sourceName on the
        given branch, and returns the latest pathId for each path
        as a dictionary indexed by path.

        @param sourceName: name of the source trove
        @type sourceName: str
        @param branch: branch to restrict the source to
        @type branch: versions.Branch
        """
        if filePrefixes is None or self.c[branch].getProtocolVersion() < 39:
            args = [sourceName, self.fromVersion(branch)]
        else:
            args = [sourceName, self.fromVersion(branch), filePrefixes]
        if fileIds is not None and self.c[branch].getProtocolVersion() >= 42:
            # Make sure we send a (possibly empty) filePrefixes
            assert(filePrefixes is not None)
            args.append(base64.b64encode("".join(fileIds)))
        ids = self.c[branch].getPackageBranchPathIds(*args)
        return dict((self.toPath(x[0]), (self.toPathId(x[1][0]),
                                         self.toVersion(x[1][1]),
                                         self.toFileId(x[1][2])))
                    for x in ids.iteritems())

    def getCollectionMembers(self, troveName, branch):
        """
        Returns all members of any collection named troveName on branch.
        Matches are for all versions and flavors of troveName (though
        each member trove name is returned only once.
        """
        return self.c[branch].getCollectionMembers(troveName, 
                                                   self.fromBranch(branch))

    def getTrovesBySource(self, sourceName, sourceVersion):
        """
        Returns (troveName, version, flavor) lists of all of the troves on the
        server built from sourceVersion.
        """
        l = self.c[sourceVersion].getTrovesBySource(sourceName,
                                            self.fromVersion(sourceVersion))
        return [ (x[0], self.toVersion(x[1]), self.toFlavor(x[2]))
                            for x in l ]
                    
    def commitChangeSetFile(self, fName, mirror = False, callback = None,
                            hidden = False):
        cs = changeset.ChangeSetFromFile(fName)
        return self._commit(cs, fName, mirror = mirror, callback = callback,
                            hidden = hidden)

    def presentHiddenTroves(self, serverName):
        if self.c[serverName].getProtocolVersion() >= 46:
            # otherwise no support for hidden troves
            self.c[serverName].presentHiddenTroves()

    def commitChangeSet(self, chgSet, callback = None, mirror = False,
                        hidden = False):
	(outFd, path) = util.mkstemp()
	os.close(outFd)
	chgSet.writeToFile(path)

	try:
            result = self._commit(chgSet, path, callback = callback,
                                  mirror = mirror, hidden = hidden)
        finally:
            os.unlink(path)

        return result

    def getTroveSigs(self, troveList):
        byServer = {}
        results = [ None ] * len(troveList)
        for i, info in enumerate(troveList):
            l = byServer.setdefault(info[1].getHost(), [])
            l.append((i, info))
        for host, l in byServer.iteritems():
            sigs = self.c[host].getTroveSigs([ 
                       (x[1][0], self.fromVersion(x[1][1]),
                        self.fromFlavor(x[1][2])) for x in l ])
            for (i, info), sig in itertools.izip(l, sigs):
                results[i] = base64.decodestring(sig)
        return results

    def setTroveSigs(self, itemList):
        # infoList is a set of ((name, version, flavor), sigBlock) tuples
        byServer = {}
        for item in itemList:
            l = byServer.setdefault(item[0][1].getHost(), [])
            l.append(item)

        total = 0
        for host, itemList in byServer.iteritems():
            total += self.c[host].setTroveSigs(
                    [ ((x[0][0], self.fromVersion(x[0][1]),
                                 self.fromFlavor(x[0][2])),
                       base64.encodestring(x[1])) for x in itemList ])

        return total

    def getMirrorMark(self, host):
        return self.c[host].getMirrorMark(host)

    def setMirrorMark(self, host, mark):
        return self.c[host].setMirrorMark(host, mark)

    def getNewSigList(self, host, mark):
        return [ (x[0], (x[1][0], self.toVersion(x[1][1]), 
                                  self.toFlavor(x[1][2]))) for
                    x in self.c[host].getNewSigList(mark) ]

    def getNewTroveInfo(self, host, mark, infoTypes=[], labels=[], thaw=True):
        server = self.c[host]
        if server.getProtocolVersion() < 47:
            raise errors.InvalidServerVersion('getNewTroveInfo requires '
                                              'Conary 1.1.24 or newer')
        if thaw:
            labels = [ self.fromLabel(x) for x in labels ]
        info = server.getNewTroveInfo(mark, infoTypes, labels)
        # we always thaw the trove tuples
        info = [ (m, (n,self.toVersion(v),self.toFlavor(f)), ti)
                 for (m,(n,v,f),ti) in info ]
        if not thaw:
            return info
        # need to thaw the troveinfo as well
        return [ (m,t,trove.TroveInfo(base64.b64decode(ti)))
                 for (m,t,ti) in info ]

    def setTroveInfo(self, info, freeze=True):
        # info is a set of ((name, version, flavor), troveInfo) tuples
        byServer = {}
        for item in info:
            (n,v,f), ti = item
            l = byServer.setdefault(v.getHost(), [])
            l.append(item)
        total = 0
        # all servers we talk to have to support the protocol we need
        for host in byServer.iterkeys():
            server = self.c[host]
            if server.getProtocolVersion() < 47:
                raise errors.InvalidServerVersion(
                    'setTroveInfo requires Conary repository running 1.1.24 or newer')
        # now we can do work
        total = 0
        for host, infoList in byServer.iteritems():
            server = self.c[host]
            #(n,v,f) are always assumed to be instances
            infoList = [ ((n,self.fromVersion(v),self.fromFlavor(f)), ti)
                     for (n,v,f),ti in infoList ]
            if freeze: # need to freeze the troveinfo as well
                infoList = [ (t, base64.b64encode(ti.freeze()))
                             for t, ti in infoList ]
            total += server.setTroveInfo(infoList)
        return total
    
    def getNewTroveList(self, host, mark):
        server = self.c[host]
        # from server protocol 40 onward we get returned the real troveTypes
        if server.getProtocolVersion() < 40:
            return [ ( x[0],
                       (x[1][0], self.thawVersion(x[1][1]), self.toFlavor(x[1][2])),
                       trove.TROVE_TYPE_NORMAL
                     ) for x in server.getNewTroveList(mark) ]
        return [ ( x[0],
                   (x[1][0], self.thawVersion(x[1][1]), self.toFlavor(x[1][2])),
                   x[2]
                 ) for x in server.getNewTroveList(mark) ]

    def addPGPKeyList(self, host, keyList):
        self.c[host].addPGPKeyList([ base64.encodestring(x) for x in keyList ])

    def getNewPGPKeys(self, host, mark):
        return [ base64.decodestring(x) for x in
                    self.c[host].getNewPGPKeys(mark) ]

    def getTroveInfo(self, infoType, troveList):
        # first, we need to know about this infoType
        if infoType not in trove.TroveInfo.streamDict.keys():
            raise Exception("Invalid infoType requested")
        byServer = {}
        results = [ None ] * len(troveList)
        for i, info in enumerate(troveList):
            l = byServer.setdefault(info[1].getHost(), [])
            l.append((i, info))
        for host, l in byServer.iteritems():
            tl = [ x[1] for x in l ]
            if self.c[host].getProtocolVersion() < 41:
                # this server does not support the getTroveInfo call,
                # so we need to synthetize it from a getTroves call
                troveInfoList = self.getTroves(tl, withFiles = False)
                for (i, tup), trv in itertools.izip(l, troveInfoList):
                    if trv is not None:
                        attrname = trove.TroveInfo.streamDict[infoType][2]
                        results[i] = getattr(trv.troveInfo, attrname, None)
                continue
            tl = [ (x[0], self.fromVersion(x[1]), self.fromFlavor(x[2]))
                   for x in tl ]
            infoList = self.c[host].getTroveInfo(infoType, tl)
            for (i, tup), (present, dataStr) in itertools.izip(l, infoList):
                if present == -1:
                    raise errors.TroveMissing(tup[0], tup[1])
                if present  == 0:
                    continue
                data = base64.decodestring(dataStr)
                results[i] = trove.TroveInfo.streamDict[infoType][1](data)
        return results

    def findTroves(self, labelPath, troves, defaultFlavor = None, 
                  acrossLabels = False, acrossFlavors = False,
                  affinityDatabase = None, allowMissing=False, 
                  getLeaves = True, bestFlavor = True,
                  troveTypes=TROVE_QUERY_PRESENT, exactFlavors=False):
        """ 
        Searches for the given troveSpec requests in the context of a labelPath,
        affinityDatabase, and defaultFlavor.

        versionStr formats accepted are:

            *^ empty/None
            *  full version (branch + revision)
            *  branch
            *  label  (host@namespace:tag)
            *  @branchname (@namespace:tag)
            *  :tag        
            *^ revision (troveVersion-sourceCount-buildCount)
            *^ troveVersion 

        VersionStr types with a ^ by them will be limited to the branches of 
        affinity troves if they exist.
        
        @param labelPath: label path to search for troves that don't specify a
        label/branch/version to search on
        @type labelPath: label or list of labels
        @param troves: trove specs that list the troves to search for
        @type troves: set of (name, versionStr, flavor) tuples, where 
        versionStr or flavor can be None
        @param defaultFlavor: flavor to use for those troves specifying None
        as their flavor.  Overridden by relevant flavors found in affinityDb
        @type flavor or None
        @param acrossLabels: if True, for each trove, return the best 
        result for each label listed in the labelPath used.  If False, 
        for each trove, return the best result for the first label that 
        matches.
        @type boolean
        @param acrossFlavors: if True, for each trove, return the best 
        result for each flavor listed in the flavorPath used.  If False, 
        for each trove, return the best result for the first flavor that 
        matches.
        @type boolean
        @type affinityDatabase: database to search for affinity troves.  
        Affinity troves for a trove spec match the trove name exactly, and
        match the branch/label requested if explicitly requested in the 
        trove spec.  The affinity trove's flavor will be used if no flavor 
        was specified in the trove spec, and the affinity trove's branch will
        be used as if it were explicitly requested if no branch or label is 
        listed in the trove spec.
        @param allowMissing: if true, do not raise an error if a trove spec
        could not be matched in the repository.
        @type boolean
        @return a dict whose keys the (name, versionStr, flavor) troves passed
        to this function.  The value for each key is a list of 
        (name, version, flavor) tuples that match that key's trove spec.
        If allowMissing is True, trove specs passed in that do not match any 
        trove in the repository will not be listed in the return value.
        """
        troveFinder = findtrove.TroveFinder(self, labelPath, 
                                            defaultFlavor, acrossLabels,
                                            acrossFlavors, affinityDatabase,
                                            getLeaves, bestFlavor,
                                            troveTypes=troveTypes,
                                            exactFlavors=exactFlavors)
        return troveFinder.findTroves(troves, allowMissing)

    def findTrove(self, labelPath, (name, versionStr, flavor), 
                  defaultFlavor=None, acrossLabels = False, 
                  acrossFlavors = False, affinityDatabase = None,
                  getLeaves = True, bestFlavor = True, 
                  troveTypes = TROVE_QUERY_PRESENT, exactFlavors = False):
        res = self.findTroves(labelPath, ((name, versionStr, flavor),),
                              defaultFlavor, acrossLabels, acrossFlavors,
                              affinityDatabase, False, getLeaves, bestFlavor,
                              troveTypes=troveTypes,
                              exactFlavors=exactFlavors)
        return res[(name, versionStr, flavor)]

    def getConaryUrl(self, version, flavor):
        # make sure the server supports us.
        # XXX: when reworking the server cache one can save the extra
        # checkVersion call below (we already called it in __getitem__)
        serverVersions = self.c[version].checkVersion()
        # as a result of the server cache __getitem__ work we know
        # that this intersection is not empty
        commonVersions = set(serverVersions) & set(CLIENT_VERSIONS)
        # getConaryUrl call was introduced at proto version 37
        if max(commonVersions) < 37:
            hostInfo = version.branch().label().asString()
            raise errors.InvalidServerVersion, \
                  ("While talking to " + hostInfo + " ...\n"
                   "Server protocol version does not have the "
                   "necessary support for the updateconary call")
        ver = version.trailingRevision()
        return self.c[version].getConaryUrl(self.fromVersion(ver),
                                            self.fromFlavor(flavor))

    def _commit(self, chgSet, fName, callback = None, mirror = False,
                hidden = False):
	serverName = None
        if chgSet.isEmpty():
            raise errors.CommitError('Attempted to commit an empty changeset')

        # new-style TroveInfo means support for versioned signatures and
        # storing unknown TroveInfo
        minProtocolRequired = CLIENT_VERSIONS[0]
        if hidden:
            minProtocolRequired = 46

        newOnlySkipSet = {}
        for tagId in trove.TroveInfo.streamDict:
            if tagId <= trove._TROVEINFO_TAG_DIR_HASHES:
                newOnlySkipSet[trove.TroveInfo.streamDict[tagId][2]] = \
                                                        True
        jobs = []
	for trvCs in chgSet.iterNewTroveList():
            # See if there is anything which needs new trove info handling
            # to commit
            troveInfo = trove.TroveInfo(trvCs.getFrozenTroveInfo())
            if troveInfo.freeze(skipSet = newOnlySkipSet):
                minProtocolRequired = max(minProtocolRequired, 45)

            # Removals of groups requires new servers. It's true of redirects
            # too, but I can't tell if this is a redirect or not so the
            # server failure will have to do :-(
            if (trvCs.getName().startswith('group-') and
                        not trvCs.getNewVersion()):
                minProtocolRequired = max(minProtocolRequired, 45)

	    v = trvCs.getOldVersion()
	    if v:
		if serverName is None:
		    serverName = v.getHost()
		assert(serverName == v.getHost())
                oldVer = self.fromVersion(v)
                oldFlavor = self.fromFlavor(trvCs.getOldFlavor())
            else:
                oldVer = ''
                oldFlavor = ''

	    v = trvCs.getNewVersion()
	    if serverName is None:
		serverName = v.getHost()
	    assert(serverName == v.getHost())

            jobs.append((trvCs.getName(), (oldVer, oldFlavor),
                         (self.fromVersion(trvCs.getNewVersion()),
                          self.fromFlavor(trvCs.getNewFlavor())),
                         trvCs.isAbsolute()))

        # XXX We don't check the version of the changeset we're committing,
        # so we might do an unnecessary conversion. It won't hurt anything
        # though.
        server = self.c[serverName]

        if server.getProtocolVersion() < minProtocolRequired:
            raise errors.CommitError('The changeset being committed needs '
                                     'a newer repository server.')

        if server.getProtocolVersion() >= 38:
            url = server.prepareChangeSet(jobs, mirror)
        else:
            url = server.prepareChangeSet()

        if server.getProtocolVersion() <= 42:
            (outFd, tmpName) = util.mkstemp()
            os.close(outFd)
            changeset._convertChangeSetV2V1(fName, tmpName)
            autoUnlink = True
            fName = tmpName
        else:
            autoUnlink = False

        try:
            inFile = open(fName)
            size = os.fstat(inFile.fileno()).st_size

            # use chunked transfer encoding to work around servers that do not
            # handle Content-length of > 2 GiB
            chunked = False
            if size >= 0x80000000:
                # protocol version 44 introduces the ability to decode chunked
                # PUTs
                if server.getProtocolVersion() < 44:
                    raise errors.CommitError('The changeset being uploaded is '
                                             'too large for the server to '
                                             'handle.')
                chunked = True

            status, reason = httpPutFile(url, inFile, size, callback = callback,
                                         rateLimit = self.uploadRateLimit,
                                         proxies = self.proxies,
                                         chunked = chunked)

            # give a slightly more helpful message for 403
            if status == 403:
                raise errors.CommitError('Permission denied. Check username, '
                                         'password, and https settings.')
            # and a generic message for a non-OK status
            if status != 200:
                raise errors.CommitError('Error uploading to repository: '
                                         '%s (%s)' %(status, reason))
        finally:
            if autoUnlink:
                os.unlink(fName)

        # avoid sending the mirror and hidden argumentsunless we have to.
        # this helps preserve backwards compatibility with old
        # servers.
        if hidden:
            server.commitChangeSet(url, mirror, hidden)
        elif mirror:
            server.commitChangeSet(url, mirror)
        else:
            server.commitChangeSet(url)

def httpPutFile(url, inFile, size, callback = None, rateLimit = None,
                proxies = None, chunked=False):
    """
    send a file to a url.  Takes a wrapper, which is an object
    that has a callback() method which takes amount, total, rate
    """

    protocol, uri = urllib.splittype(url)
    assert(protocol in ('http', 'https'))

    opener = transport.XMLOpener(proxies=proxies)
    c, urlstr, selector, headers = opener.createConnection(uri,
        ssl = (protocol == 'https'), withProxy=True)

    BUFSIZE = 8192

    callbackFn = None
    if callback:
        wrapper = callbacks.CallbackRateWrapper(callback,
                                                callback.sendingChangeset,
                                                size)
        callbackFn = wrapper.callback

    c.connect()
    c.putrequest("PUT", selector)
    for k, v in headers:
        c.putheader(k, v)

    if chunked:
        c.putheader('Transfer-Encoding', 'chunked')
        try:
            c.endheaders()
        except socket.error, e:
            opener._processSocketError(e)
            raise

        # keep track of the total amount of data sent so that the
        # callback passed in to copyfileobj can report progress correctly
        total = 0
        while size:
            # send in 256k chunks
            chunk = 262144
            if chunk > size:
                chunk = size
            # first send the hex-encoded size
            c.send('%x\r\n' %chunk)
            # then the chunk of data
            util.copyfileobj(inFile, c, bufSize=chunk, callback=callbackFn,
                             rateLimit = rateLimit, sizeLimit = chunk,
                             total=total)
            # send \r\n after the chunked data
            c.send("\r\n")
            total =+ chunk
            size -= chunk
        # terminate the chunked encoding
        c.send('0\r\n\r\n')
    else:
        c.putheader('Content-length', str(size))
        try:
            c.endheaders()
        except socket.error, e:
            opener._processSocketError(e)
            raise

        util.copyfileobj(inFile, c, bufSize=BUFSIZE, callback=callbackFn,
                         rateLimit = rateLimit, sizeLimit = size)

    resp = c.getresponse()
    if resp.status != 200:
        opener.handleProxyErrors(resp.status)
    return resp.status, resp.reason
