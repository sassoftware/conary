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


import base64
import errno
import gzip
import itertools
import os
import time
import urllib
import xml

#conary
from conary import callbacks
from conary import conarycfg
from conary import files
from conary.cmds import metadata
from conary import trove as trv_mod
from conary import trovetup
from conary import versions
from conary.lib import util, api
from conary.lib import httputils
from conary.lib import log
from conary.lib.http import proxy_map, request as req_mod
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
CLIENT_VERSIONS = range(36, 73 + 1)

from conary.repository.trovesource import TROVE_QUERY_ALL, TROVE_QUERY_PRESENT, TROVE_QUERY_NORMAL

# this is a quote function that quotes all RFC 2396 reserved characters,
# including / (which is normally considered "safe" by urllib.quote)
quote = lambda s: urllib.quote(s, safe='')

class PartialResultsError(Exception):

    # this is expected to be handled by the caller!
    def __init__(self, partialResults):
        self.partialResults = partialResults


def unmarshalException(exceptionName, exceptionArgs, exceptionKwArgs):
    conv = xmlshims.NetworkConvertors()
    if exceptionName == "TroveIntegrityError" and len(exceptionArgs) > 1:
        # old repositories give TIE w/ no trove information or with a
        # string error message. exceptionArgs[0] is that message if
        # exceptionArgs[1] is not set or is empty.
        return errors.TroveIntegrityError(error=exceptionArgs[0],
                                    *conv.toTroveTup(exceptionArgs[1]))
    elif not hasattr(errors, exceptionName):
        return errors.UnknownException(exceptionName, exceptionArgs)
    else:
        exceptionClass = getattr(errors, exceptionName)

        if hasattr(exceptionClass, 'demarshall'):
            args, kwArgs = exceptionClass.demarshall(conv, exceptionArgs,
                                                     exceptionKwArgs)
            raise exceptionClass(*args, **kwArgs)

        for klass, marshall in errors.simpleExceptions:
            if exceptionName == marshall:
                return klass(exceptionArgs[0])
        return errors.UnknownException(exceptionName, exceptionArgs)


class ServerProxyMethod(util.ServerProxyMethod):

    def __call__(self, *args, **kwargs):
        return self._send(self._name, args, kwargs)


class ServerProxy(util.ServerProxy):

    _requestFilter = xmlshims.RequestArgs
    _responseFilter = xmlshims.ResponseArgs

    def _createMethod(self, name):
        return ServerProxyMethod(self._request, name)

    def usedProxy(self):
        return self._transport.usedProxy

    def setAbortCheck(self, check):
        self._transport.setAbortCheck(check)

    def setProtocolVersion(self, val):
        self._protocolVersion = val

    def getProtocolVersion(self):
        return self._protocolVersion

    def _request(self, method, args, kwargs):
        protocolVersion = (kwargs.pop('protocolVersion', None) or
            self.getProtocolVersion())

        # always use protocol version 50 for checkVersion.  If we're about
        # to talk to a pre-protocol-version 51 server, we will make it
        # trace back with too many arguments if we try to pass kwargs
        if method == 'checkVersion':
            protocolVersion = min(protocolVersion, 50)
        request = self._requestFilter(version=protocolVersion, args=args,
                kwargs=kwargs)
        try:
            return self._marshalCall(method, request)
        except errors.EntitlementTimeout, err:
            entList = self._transport.getEntitlements()

            singleEnt = conarycfg.loadEntitlement(self._entitlementDir,
                    self._serverName)
            # remove entitlement(s) which timed out
            newEntList = [ x for x in entList if x[1] not in
                    err.getEntitlements() ]
            newEntList.insert(0, singleEnt[1:])

            # try again with the new entitlement
            self._transport.setEntitlements(newEntList)
            return self._marshalCall(method, request)

    def _marshalCall(self, method, request):
        start = time.time()
        rawRequest = request.toWire()
        rawResponse = util.ServerProxy._request(self, method, rawRequest)
        # XMLRPC responses are a 1-tuple
        rawResponse, = rawResponse
        response = self._responseFilter.fromWire(request.version, rawResponse,
                self._transport.responseHeaders)

        if self._callLog:
            host = str(self._url.hostport)
            elapsed = time.time() - start
            self._callLog.log(host, self._transport.getEntitlements(),
                    method, rawRequest, rawResponse, latency=elapsed)

        if response.isException:
            raise unmarshalException(response.excName, response.excArgs,
                    response.excKwargs)
        else:
            return response.result

    def __init__(self, url, serverName, transporter,
                 entitlementDir, callLog):
        try:
            util.ServerProxy.__init__(self, url=url, transport=transporter)
        except IOError, e:
            raise errors.OpenError('Error occurred opening repository '
                    '%s: %s' % (url, e))
        self._serverName = serverName
        self._protocolVersion = CLIENT_VERSIONS[-1]
        self._entitlementDir = entitlementDir
        self._callLog = callLog

class ServerCache(object):
    TransportFactory = transport.Transport

    def __init__(self, cfg, pwPrompt=None):
        self.cache = {}
        self.shareCache = {}
        self.map = cfg.repositoryMap
        self.userMap = cfg.user
        self.pwPrompt = pwPrompt
        self.entitlements = cfg.entitlement
        self.proxyMap = cfg.getProxyMap()
        self.entitlementDir = cfg.entitlementDirectory
        self.caCerts = cfg.trustedCerts
        self.connectAttempts = cfg.connectAttempts
        self.callLog = None
        self.systemId = util.SystemIdFactory(cfg.systemIdScript).getId()

        if 'CONARY_CLIENT_LOG' in os.environ:
            self.callLog = calllog.ClientCallLogger(
                                os.environ['CONARY_CLIENT_LOG'])

    def __getPassword(self, host, user=None):
        if not self.pwPrompt:
            return None, None
        user, pw = self.pwPrompt(host, user)
        if not user or not pw:
            return None, None
        pw = util.ProtectedString(pw)
        if self._setAndCheckPassword(host, (user, pw)):
            return user, pw
        for x in range(3):
            user, pw = self.pwPrompt(host, user, useCached=False)
            if not user or not pw:
                return None, None
            pw = util.ProtectedString(pw)
            if self._setAndCheckPassword(host, (user, pw)):
                return user, pw
        return None, None

    def _setAndCheckPassword(self, host, userInfo):
        try:
            self._connect(host, cache=False, userInfo=userInfo)
        except errors.InsufficientPermission:
            return False
        else:
            self.userMap.addServerGlob(host, *userInfo)
            return True

    @staticmethod
    def _getServerName(item):
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
        return self._connect(item)

    def _connect(self, serverName, cache=True, userInfo=None):
        serverName = self._getServerName(serverName)

        server = self.cache.get(serverName, None)
        if cache and server is not None:
            return server

        url = self.map.get(serverName, None)
        if isinstance(url, repository.AbstractTroveDatabase):
            return url

        if userInfo is None:
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

        if url is None:
            if entList or userInfo:
                # if we have authentication information, use https
                protocol = 'https'
            else:
                # if we are using anonymous, use http
                protocol = 'http'
            url = "%s://%s/conary/" % (protocol, serverName)

        url = req_mod.URL.parse(url)
        if userInfo:
            if not userInfo[1]:
                # Prompt user for a password
                userInfo = self.__getPassword(serverName, userInfo[0])
            if userInfo[1]:
                # Protect the password string if it isn't already protected.
                userInfo = userInfo[0], util.ProtectedString(userInfo[1])
            url = url._replace(userpass=userInfo)

        shareTuple = (url, userInfo, tuple(entList), serverName)
        server = self.shareCache.get(shareTuple, None)
        if cache and server is not None:
            self.cache[serverName] = server
            return server

        transporter = self.TransportFactory(
                proxyMap=self.proxyMap, serverName=serverName,
                caCerts=self.caCerts, connectAttempts=self.connectAttempts)
        transporter.setCompress(True)
        transporter.setEntitlements(entList)
        transporter.addExtraHeaders({'X-Conary-SystemId': self.systemId})
        server = ServerProxy(url=url, serverName=serverName,
                transporter=transporter, entitlementDir=self.entitlementDir,
                callLog=self.callLog)

        # Avoid poking at __transport
        server._transport = transporter

        serverVersions = server.checkVersion()
        intersection = set(serverVersions) & set(CLIENT_VERSIONS)
        if not intersection:
            raise errors.InvalidServerVersion(
                "While talking to repository %s:\n"
                "Invalid server version.  Server accepts client "
                "versions %s, but this client only supports versions %s"
                " - download a valid client from wiki.rpath.com" %
                (url, ",".join([str(x) for x in serverVersions]),
                 ",".join([str(x) for x in CLIENT_VERSIONS])))

        # this is the protocol version we should use when talking
        # to this repository - the maximum we both understand
        server.setProtocolVersion(max(intersection))

        if cache:
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

    def __init__(self, cfg, localRepository=None, pwPrompt=None):
        # the local repository is used as a quick place to check for
        # troves _getChangeSet needs when it's building changesets which
        # span repositories. it has no effect on any other operation.
        if pwPrompt is None:
            pwPrompt = lambda x, y: (None, None)

        self.cfg = cfg
        self.downloadRateLimit = cfg.downloadRateLimit
        self.uploadRateLimit = cfg.uploadRateLimit
        self.c = ServerCache(cfg, pwPrompt)
        self.localRep = localRepository

        trovesource.SearchableTroveSource.__init__(self, searchableByType=True)
        self.searchAsRepository()

        self.TROVE_QUERY_ALL = TROVE_QUERY_ALL
        self.TROVE_QUERY_PRESENT = TROVE_QUERY_PRESENT
        self.TROVE_QUERY_NORMAL = TROVE_QUERY_NORMAL

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
        # Free the DOM - CNY-2674
        doc.unlink()

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

    def addRole(self, label, role):
        if self.c[label].getProtocolVersion() < 61:
            return self.c[label].addAccessGroup(role)
        return self.c[label].addRole(role)

    def addDigitalSignature(self, name, version, flavor, digsig):
        if self.c[version].getProtocolVersion() < 45:
            raise errors.InvalidServerVersion("Cannot sign troves on Conary "
                                       "repositories older than 1.1.20")

        encSig = base64.b64encode(digsig.freeze())
        self.c[version].addDigitalSignature(name, self.fromVersion(version),
                                            self.fromFlavor(flavor),
                                            encSig)

    def getChangeSetFingerprints(self, csList, recurse, withFiles,
                                 withFileContents, excludeAutoSource,
                                 mirrorMode):
        byServer = {}
        for cs in csList:
            name, (oldV, oldF), (newV, newF), abs = cs
            host = newV.getHost()
            if oldV and oldV.getHost() != host:
                raise RuntimeError('requested fingerprint for a changeset '
                                   'between two different repositories')
            l = byServer.setdefault(host, [])
            l.append(cs)
        fingerprints = {}
        for host, subCsList in byServer.iteritems():
            req = []
            for name, oldVF, newVF, absolute in subCsList:
                if oldVF[0]:
                    oldVF = (self.fromVersion(oldVF[0]),
                            self.fromFlavor(oldVF[1]))
                else:
                    oldVF = (0, 0)
                newVF = self.fromVersion(newVF[0]), self.fromFlavor(newVF[1])
                req.append((name, oldVF, newVF, absolute))

            l = self.c[host].getChangeSetFingerprints(
                req,
                recurse=(recurse and 1 or 0),
                withFiles=(withFiles and 1 or 0),
                withFileContents=(withFileContents and 1 or 0),
                excludeAutoSource=(excludeAutoSource and 1 or 0),
                mirrorMode=(mirrorMode and 1 or 0))
            for cs, fp in itertools.izip(subCsList, l):
                fingerprints[cs] = fp

        l = []
        for cs in csList:
            l.append(fingerprints[cs])

        return l

    def addMetadataItems(self, itemList):
        byServer = {}
        for (name, version, flavor), item in itemList:
            # this does nothing if it's already been digested
            item.computeDigests()
            l = byServer.setdefault(version.getHost(), [])
            l.append(
                ((name, self.fromVersion(version), self.fromFlavor(flavor)),
                 base64.b64encode(item.freeze())))
        for server in byServer.keys():
            s = self.c[version]
            if s.getProtocolVersion() < 47:
                raise errors.InvalidServerVersion("Cannot add metadata to "
                        "troves on repositories older than 1.1.24")
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

    def deleteRole(self, label, role):
        if self.c[label].getProtocolVersion() < 61:
            self.c[label].deleteAccessGroup(role)
            return
        self.c[label].deleteRole(role)

    def addRoleMember(self, label, role, username):
        if self.c[label].getProtocolVersion() < 63:
            raise errors.InvalidServerVersion('addRoleMember requires '
                'a server running Conary 2.0.18 or later')
        self.c[label].addRoleMember(role, username)

    def getRoleMembers(self, label, role):
        if self.c[label].getProtocolVersion() < 63:
            raise errors.InvalidServerVersion('getRoleMembers requires '
                'a server running Conary 2.0.18 or later')
        return self.c[label].getRoleMembers(role)

    def updateRoleMembers(self, label, role, members):
        if self.c[label].getProtocolVersion() < 61:
            self.c[label].updateAccessGroupMembers(role, members)
            return
        self.c[label].updateRoleMembers(role, members)

    def setRoleCanMirror(self, reposLabel, role, canMirror):
        if self.c[reposLabel].getProtocolVersion() < 61:
            self.c[reposLabel].setUserGroupCanMirror(role, canMirror)
            return
        self.c[reposLabel].setRoleCanMirror(role, canMirror)

    def setRoleIsAdmin(self, reposLabel, role, admin):
        if self.c[reposLabel].getProtocolVersion() < 61:
            self.c[reposLabel].setUserGroupIsAdmin(role, admin)
            return
        self.c[reposLabel].setRoleIsAdmin(role, admin)

    def getRoleFilters(self, label, roles):
        if self.c[label].getProtocolVersion() < 72:
            raise errors.InvalidServerVersion("getRoleFilters requires a "
                    "server running Conary 2.5.0 or later")
        result = self.c[label].getRoleFilters(roles)
        ret = {}
        for role, (acceptFlags, filterFlags) in result.iteritems():
            ret[role] = ( self.toFlavor(acceptFlags),
                    self.toFlavor(filterFlags) )
        return ret

    def setRoleFilters(self, label, roleFiltersMap):
        if self.c[label].getProtocolVersion() < 72:
            raise errors.InvalidServerVersion("setRoleFilters requires a "
                    "server running Conary 2.5.0 or later")
        out = {}
        for role, (acceptFlags, filterFlags) in roleFiltersMap.iteritems():
            out[role] = ( self.fromFlavor(acceptFlags),
                    self.fromFlavor(filterFlags) )
        self.c[label].setRoleFilters(out)

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

    def listAcls(self, reposLabel, role):
        return self.c[reposLabel].listAcls(role)

    def addAcl(self, reposLabel, role, trovePattern, label, write = False,
               remove = False):
        if self.c[reposLabel].getProtocolVersion() < 61:
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

        self.c[reposLabel].addAcl(role, trovePattern, label,
                                  write = write, remove = remove)

        return True

    def editAcl(self, reposLabel, role, oldTrovePattern, oldLabel,
                trovePattern, label, write = False, canRemove = False):
        if self.c[reposLabel].getProtocolVersion() < 61:
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

        self.c[reposLabel].editAcl(role, oldTrovePattern, oldLabel,
                                   trovePattern, label, write = write,
                                   canRemove = canRemove)

        return True

    def deleteAcl(self, reposLabel, role, trovePattern, label):
        if not label:
            label = "ALL"
        elif type(label) == str:
            pass
        else:
            label = self.fromLabel(label)

        if not trovePattern:
            trovePattern = "ALL"

        self.c[reposLabel].deleteAcl(role, trovePattern, label)
        return True

    def changePassword(self, label, user, newPassword):
        self.c[label].changePassword(user, newPassword)

    def getRoles(self, label):
        if self.c[label].getProtocolVersion() < 61:
            return self.c[label].getUserGroups()
        return self.c[label].getRoles()

    def addEntitlementKeys(self, serverName, entClass, entKeys):
        entKeys = [ self.fromEntitlement(x) for x in entKeys ]
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].addEntitlements(entClass, entKeys)
        return self.c[serverName].addEntitlementKeys(entClass, entKeys)

    def deleteEntitlementKeys(self, serverName, entClass, entKeys):
        entKeys = [ self.fromEntitlement(x) for x in entKeys ]
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].deleteEntitlements(entClass, entKeys)
        return self.c[serverName].deleteEntitlementKeys(entClass, entKeys)

    def addEntitlementClass(self, serverName, entClass, role):
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].addEntitlementGroup(entClass, role)
        return self.c[serverName].addEntitlementClass(entClass, role)

    def deleteEntitlementClass(self, serverName, entClass):
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].deleteEntitlementGroup(entClass)
        else:
            return self.c[serverName].deleteEntitlementClass(entClass)

    def addEntitlementClassOwner(self, serverName, role, entClass):
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].addEntitlementOwnerAcl(role, entClass)
        return self.c[serverName].addEntitlementClassOwner(role, entClass)

    def deleteEntitlementClassOwner(self, serverName, role, entClass):
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].deleteEntitlementOwnerAcl(role, entClass)
        return self.c[serverName].deleteEntitlementClassOwner(role, entClass)

    def listEntitlementKeys(self, serverName, entClass):
        if self.c[serverName].getProtocolVersion() < 61:
            l = self.c[serverName].listEntitlements(entClass)
        else:
            l = self.c[serverName].listEntitlementKeys(entClass)
        return [ self.toEntitlement(x) for x in l ]

    def listEntitlementClasses(self, serverName):
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].listEntitlementGroups()
        return self.c[serverName].listEntitlementClasses()

    def getEntitlementClassesRoles(self, serverName, classList):
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].getEntitlementClassAccessGroup(classList)
        return self.c[serverName].getEntitlementClassesRoles(classList)

    def setEntitlementClassesRoles(self, serverName, classInfo):
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].setEntitlementClassAccessGroup(classInfo)
        return self.c[serverName].setEntitlementClassesRoles(classInfo)

    def listRoles(self, serverName):
        if self.c[serverName].getProtocolVersion() < 61:
            return self.c[serverName].listAccessGroups()
        return self.c[serverName].listRoles()

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
                         sortByPath = False, withFiles = False,
                         capsules = False):
        # XXX this code should most likely go away, and anything that
        # uses it should be written to use other functions
        l = [(troveName, (None, None), (version, flavor), True)]
        cs = self._getChangeSet(l, recurse = False, withFiles = True,
                                withFileContents = False)
        try:
            trvCs = cs.getNewTroveVersion(troveName, version, flavor)
        except KeyError:
            raise StopIteration

        t = trv_mod.Trove(trvCs, skipIntegrityChecks = not withFiles)
        # if we're sorting, we'll need to pull out all the paths ahead
        # of time.  We'll use a generator that returns the items
        # in the same order as iterFileList() to reuse code.
        if sortByPath:
            pathDict = {}
            for pathId, path, fileId, version in t.iterFileList(
                                capsules = capsules, members = not capsules):
                pathDict[path] = (pathId, fileId, version)
            paths = pathDict.keys()
            paths.sort()
            def rearrange(paths, pathDict):
                for path in paths:
                    (pathId, fileId, version) = pathDict[path]
                    yield (pathId, path, fileId, version)
            generator = rearrange(paths, pathDict)
        else:
            generator = t.iterFileList(capsules = capsules,
                                       members = not capsules)
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

    @api.publicApi
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

    @api.publicApi
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

    def _getTroveInfoByVerInfoTuples(self, troveSpecs, bestFlavor, method,
                               branches = False, labels = False,
                               versions = False,
                               troveTypes = TROVE_QUERY_PRESENT,
                               getLeaves = False, splitByBranch = False):
        assert(branches + labels + versions == 1)

        d = {}
        specsByName = {}
        if not troveSpecs:
            return [], []

        finalResults = [ [] for x in troveSpecs ]
        finalAltFlavors = [ [] for x in troveSpecs ]

        if branches:
            freezeFn = self.fromBranch
            keyFn = lambda version: version.branch()
        elif labels:
            freezeFn = self.fromLabel
            keyFn = lambda version: version.trailingLabel()
        elif versions:
            freezeFn = self.fromVersion
            keyFn = lambda version: version


        for idx, (name, ver, flavor) in enumerate(troveSpecs):
            if not name:
                name = ""
            host = ver.getHost()
            verStr = freezeFn(ver)
            specsByName.setdefault(name, []).append((idx, ver, flavor))
            versionDict = d.setdefault(host, {})
            flavorDict = versionDict.setdefault(name, {})
            # don't pass in a flavor, we'll do all flavor work on this
            # side.
            flavorDict[verStr] = ''

        result = {}
        for host, requestD in d.iteritems():
            respD = self.c[host].__getattr__(method)(
                            *self._setTroveTypeArgs(host, requestD,
                                                    bestFlavor,
                                                    troveTypes = troveTypes))
            self._mergeTroveQuery(result, respD)


        if not result:
            return finalResults, []

        filterOptions = self._getFilterOptions(getLeaves, bestFlavor,
                                               troveTypes,
                                               splitByBranch=splitByBranch)

        scoreCache = {}
        for name, versionFlavorDict in result.iteritems():
            resultsByKey = {}
            # create a results dictionary that is based off of the key
            # passed in.
            for version, flavorList in versionFlavorDict.iteritems():
                key = keyFn(version)
                if key not in resultsByKey:
                    resultsByKey[key] = {}
                if version not in resultsByKey[key]:
                    vDict = resultsByKey[key][version] = {}
                else:
                    vDict = resultsByKey[key][version]
                for flavor in flavorList:
                    if flavor not in vDict:
                        vDict[flavor] = []
                    vDict[flavor].append(name)

            if name in specsByName:
                queryList = specsByName[name]
            elif '' in specsByName:
                queryList =  specsByName['']
            elif None in specsByName:
                queryList = specsByName[None]

            # for each relevant query, results are available by versionSepc
            # (the "key")
            for idx, versionQuery, flavorQuery in queryList:
                versionFlavorDict = resultsByKey.get(versionQuery, None)
                if not versionFlavorDict:
                    continue
                results, altFlavors = self._filterResultsByFlavor(
                                            versionFlavorDict,
                                            flavorQuery, filterOptions,
                                            scoreCache)
                if altFlavors:
                    finalAltFlavors[idx].extend(altFlavors)
                for version, flavorList in results.iteritems():
                    for flavor in flavorList:
                        for name in versionFlavorDict[version][flavor]:
                            finalResults[idx].append(trovetup.TroveTuple(name,
                                version, flavor))
        for idx, results in enumerate(finalResults):
            if results:
                finalAltFlavors[idx] = [] # any results means no alternates
                                          # are needed
            else:
                finalAltFlavors[idx] = list(set(finalAltFlavors[idx]))
        return finalResults, finalAltFlavors

    def _getTroveInfoByVerInfo(self, troveSpecs, bestFlavor, method,
                               branches = False, labels = False,
                               versions = False,
                               troveTypes = TROVE_QUERY_PRESENT,
                               getLeaves = False, splitByBranch = False):
        # if necessary, convert troveSpecs to tuples before
        # processing.  In tuple form the results need the least
        # massaging so we do all work in tuple form.
        troveSpecList = []
        if isinstance(troveSpecs, dict):
            for name, versionDict in troveSpecs.iteritems():
                for version, flavorList in versionDict.iteritems():
                    if flavorList is None or flavorList is '':
                        troveSpecList.append((name, version, flavorList))
                    else:
                        troveSpecList.extend((name, version, x) for x in
                                             flavorList)
        else:
            troveSpecList = troveSpecs
        results, altFlavors = self._getTroveInfoByVerInfoTuples(
                                                 troveSpecList, bestFlavor,
                                                 method,
                                                 branches=branches,
                                                 labels=labels,
                                                 versions=versions,
                                                 troveTypes=troveTypes,
                                                 getLeaves=getLeaves,
                                                 splitByBranch=splitByBranch)
        if not isinstance(troveSpecs, dict):
            return results, altFlavors
        resultDict = {}
        for troveList in results:
            for (name, version, flavor) in troveList:
                if name not in resultDict:
                    vDict = resultDict[name] = {}
                else:
                    vDict = resultDict[name]
                if version not in vDict:
                    fList = vDict[version] = []
                else:
                    fList = vDict[version]
                fList.append(flavor)
        for name, versionDict in resultDict.iteritems():
            for version in versionDict:
                versionDict[version] = list(set(versionDict[version]))
        return resultDict

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

    @api.publicApi
    def getTrove(self, troveName, troveVersion, troveFlavor, withFiles = True,
                 callback = None):
        rc = self.getTroves([(troveName, troveVersion, troveFlavor)],
                            withFiles = withFiles, callback = callback)
        if rc[0] is None:
            raise errors.TroveMissing(troveName, version = troveVersion)

        return rc[0]

    @api.publicApi
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
            t = trv_mod.Trove(troveCs, skipIntegrityChecks = not withFiles)
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

        if jobList[0][2][0] is None:
            # this trove is being removed, so use old version
            server = self.c[jobList[0][1][0]]
        else:
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

    def _clearHostCache(self):
        httputils.IPCache.clear()

    def _cacheHostLookups(self, hosts):
        if not self.c.proxyMap.isEmpty:
            return
        hosts = set(hosts)
        for host in hosts:
            url = self.c.map[host]
            if url:
                mappedHost = urllib.splithost(urllib.splittype(url)[1])[0]
            else:
                mappedHost = host
            transport.httputils.IPCache.get(mappedHost)

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
        Create a changeset file based on a job list.

        @param changesetVersion: (optional) request a specific changeset
            version from the server. The value is one of the C{FILE_CONTAINER_*}
            constants defined in the L{NetworkRepositoryClient} class. To map
            a protocol version into a changeset version, use
            L{repository.changeset.getNativeChangesetVersion}.
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

        def _getLocalTroves(troveList):
            if not self.localRep or not troveList:
                return [ None ] * len(troveList)

            return self.localRep.getTroves(troveList, pristine=True)

        def _getCsFromRepos(target, cs, server, job, recurse,
                            withFiles, withFileContents,
                            excludeAutoSource, filesNeeded,
                            chgSetList, removedList, changesetVersion,
                            mirrorMode):
            if callback:
                callback.requestingChangeSet()
            server.setAbortCheck(None)
            args = (job, recurse, withFiles, withFileContents,
                    excludeAutoSource)
            kwargs = {}
            serverVersion = server.getProtocolVersion()

            if mirrorMode and serverVersion >= 49:
                if not changesetVersion:
                    changesetVersion = \
                        filecontainer.FILE_CONTAINER_VERSION_LATEST

                args += (changesetVersion, mirrorMode, )
            elif changesetVersion and serverVersion > 47:
                args += (changesetVersion, )

            # seek to the end of the file
            outFile.seek(0, 2)
            start = resume = outFile.tell()
            attempts = max(1, self.cfg.downloadAttempts)
            while attempts > 0:
                if resume - start:
                    assert serverVersion >= 73
                    outFile.seek(resume)
                    kwargs['resumeOffset'] = resume - start
                    if callback:
                        callback.warning("Changeset download was interrupted. "
                                "Attempting to resume where it left off.")
                try:
                    (sizes, extraTroveList, extraFileList, removedTroveList,
                            extra,) = _getCsOnce(serverVersion, args, kwargs)
                    break
                except errors.TruncatedResponseError:
                    attempts -= 1
                    if not attempts or serverVersion < 73:
                        raise
                    # Figure out how many bytes were downloaded, then trim off
                    # a bit to ensure any garbage (e.g. a proxy error page) is
                    # discarded.
                    keep = max(resume, outFile.tell() -
                            self.cfg.downloadRetryTrim)
                    if self.cfg.downloadRetryTrim and (
                            keep - resume > self.cfg.downloadRetryThreshold):
                        attempts = max(1, self.cfg.downloadAttempts)
                    resume = keep

            chgSetList += self.toJobList(extraTroveList)
            filesNeeded.update(self.toFilesNeeded(extraFileList))
            removedList += self.toJobList(removedTroveList)

            for size in sizes:
                f = util.SeekableNestedFile(outFile, size, start)
                try:
                    newCs = changeset.ChangeSetFromFile(f)
                except IOError, err:
                    assert False, 'IOError in changeset (%s); args = %r' % (
                            str(err), args,)
                if not cs:
                    cs = newCs
                else:
                    cs.merge(newCs)
                start += size

            return (cs, self.toJobList(extraTroveList),
                    self.toFilesNeeded(extraFileList))

        def _getCsOnce(serverVersion, args, kwargs):
            l = server.getChangeSet(*args, **kwargs)
            extra = {}
            if serverVersion >= 50:
                url = l[0]
                sizes = [ x[0] for x in l[1] ]
                extraTroveList = [ x for x in itertools.chain(
                                    *[ x[1] for x in l[1] ] ) ]
                extraFileList = [ x for x in itertools.chain(
                                    *[ x[2] for x in l[1] ] ) ]
                removedTroveList = [ x for x in itertools.chain(
                                    *[ x[3] for x in l[1] ] ) ]
                if serverVersion >= 73:
                    extra = l[2]
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

            if hasattr(url, 'read'):
                # Nested changeset file in a multi-part response
                inF = url
            elif os.path.exists(url):
                # attempt to remove temporary local files
                # possibly created by a shim client
                inF = open(url, 'rb')
                try:
                    os.unlink(url)
                except OSError, err:
                    if err.args[0] != errno.EPERM:
                        raise
            else:
                # "forceProxy" here makes sure that multi-part requests go back
                # through the same proxy on subsequent requests.
                forceProxy = server.usedProxy()
                headers = [('X-Conary-Servername', server._serverName)]
                try:
                    inF = transport.ConaryURLOpener(proxyMap=self.c.proxyMap
                            ).open(url, forceProxy=forceProxy, headers=headers)
                except transport.TransportError, e:
                    raise errors.RepositoryError(str(e))

            if callback:
                wrapper = callbacks.CallbackRateWrapper(
                    callback, callback.downloadingChangeSet,
                    sum(sizes))
                copyCallback = wrapper.callback
                abortCheck = callback.checkAbort
            else:
                copyCallback = None
                abortCheck = None

            resumeOffset = kwargs.get('resumeOffset') or 0
            # Start the total at resumeOffset so that progress callbacks
            # continue where they left off.
            copied = util.copyfileobj(inF, outFile, callback=copyCallback,
                    abortCheck=abortCheck, rateLimit=self.downloadRateLimit,
                    total=resumeOffset)
            if copied is None:
                raise errors.RepositoryError("Unknown error downloading changeset")
            totalSize = copied + resumeOffset
            if hasattr(inF, 'headers') and 'content-length' in inF.headers:
                expectSize = resumeOffset + long(inF.headers['content-length'])
                if totalSize != expectSize:
                    raise errors.TruncatedResponseError(expectSize, totalSize)
                assert sum(sizes) == expectSize
            elif totalSize != sum(sizes):
                raise errors.TruncatedResponseError(sum(sizes), totalSize)
            inF.close()

            return (sizes, extraTroveList, extraFileList, removedTroveList,
                    extra)

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
            for ((pathId, fileId, fileVersion), fileObj, error) in zip(
                    need, fileObjs, fileObjs.errors):
                fileDict[(pathId, fileId)] = fileObj, error
            del fileObj, fileObjs, need, fileId

            contentsNeeded = []
            fileJob = []

            for (pathId, troveName,
                    (oldTroveVersion, oldTroveF, oldFileId, oldFileVersion),
                    (newTroveVersion, newTroveF, newFileId, newFileVersion)) \
                                in filesNeeded:
                if oldFileVersion:
                    oldFileObj, _ = fileDict[(pathId, oldFileId)]
                else:
                    oldFileObj = None

                newFileObj, newFileError = fileDict[(pathId, newFileId)]
                if newFileObj is None:
                    # File missing from server
                    missingFiles.append((troveName, newTroveVersion, newTroveF,
                        newFileError, pathId, newFileId, newFileVersion))
                    continue

                forceAbsolute = mirrorMode and oldFileObj and (
                        oldFileId != newFileId
                        or oldFileVersion.getHost() != newFileVersion.getHost()
                        )
                if forceAbsolute:
                    (filecs, hash) = changeset.fileChangeSet(pathId,
                                                             None,
                                                             newFileObj)
                else:
                    (filecs, hash) = changeset.fileChangeSet(pathId,
                                                             oldFileObj,
                                                             newFileObj)

                internalCs.addFile(oldFileId, newFileId, filecs)

                if not withFileContents:
                    continue
                if excludeAutoSource and newFileObj.flags.isAutoSource():
                    continue

                if hash or (forceAbsolute and newFileObj.hasContents):
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

                    if not newFileObj.flags.isEncapsulatedContent():
                        fetchItems.append( (newFileId, newFileVersion, newFileObj) )
                        contentsNeeded += fetchItems

                        needItems.append( (pathId, newFileId, newFileObj) )
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
                trvName, trvVersion, trvFlavor, error = mf[:4]
                trv = cs.getNewTroveVersion(trvName, trvVersion, trvFlavor)
                # Find the file path associated with this missing file
                for pathId, path, fileId, version in trv.getNewFileList():
                    if (pathId, fileId, version) == mf[4:]:
                        break
                else: # for
                    # Unable to find this file
                    raise Exception("Cannot find file in changeset")
                mfs.append((trvName, trvVersion, trvFlavor,
                            pathId, path, fileId, version, error))

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
                fileId, error = mf[5], mf[7]
                if error:
                    error.throw()
                else:
                    raise errors.FileStreamMissing(fileId)

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
                        if trv_mod.troveIsGroup(info[0]):
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
                    error = util.SavedException()
                    for idx in idxL:
                        result.errors[idx] = error
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
        result = FileResultSet(result)

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


    def getFileContentsObjects(self, server, fileList, callback, outF,
                               compressed):
        url, sizes = self.c[server].getFileContents(fileList)
        return self._getFileContentsObjects(server, url, sizes,
                                            fileList, callback, outF,
                                            compressed)

    def _getFileContentsObjects(self, server, url, sizes,
                                fileList, callback, outF, compressed):
        # protocol version 44 and later return sizes as strings rather
        # than ints to avoid 2 GiB limits
        sizes = [ int(x) for x in sizes ]
        assert(len(sizes) == len(fileList))

        # "forceProxy" here makes sure that multi-part requests go back through
        # the same proxy on subsequent requests.
        forceProxy = self.c[server].usedProxy()
        headers = [('X-Conary-Servername', server)]
        inF = transport.ConaryURLOpener(proxyMap = self.c.proxyMap).open(url,
                forceProxy=forceProxy, headers=headers)

        if callback:
            wrapper = callbacks.CallbackRateWrapper(
                callback, callback.downloadingFileContents, sum(sizes))
            copyCallback = wrapper.callback
        else:
            copyCallback = None

        # make sure we append to the end (creating the gzip file
        # object does a certain amount of seeking through the
        # nested file object which we need to undo
        outF.seek(0, 2)
        start = outF.tell()

        totalSize = util.copyfileobj(inF, outF,
                                     rateLimit = self.downloadRateLimit,
                                     callback = copyCallback)
        if totalSize == None:
            raise errors.RepositoryError("Unknown error downloading changeset")
        elif hasattr(inF, 'headers') and 'content-length' in inF.headers:
            expectSize = long(inF.headers['content-length'])
            if totalSize != expectSize:
                raise errors.TruncatedResponseError(expectSize, totalSize)
        elif totalSize != sum(sizes):
            raise errors.TruncatedResponseError(sum(sizes), totalSize)

        fileObjList= []
        for size in sizes:
            nestedF = util.SeekableNestedFile(outF, size, start)

            totalSize -= size
            start += size

            if compressed:
                fc = filecontents.FromFile(nestedF,
                                                    compressed = True)
            else:
                gzfile = gzip.GzipFile(fileobj = nestedF)
                fc = filecontents.FromFile(gzfile)

            fileObjList.append(fc)

        assert(totalSize == 0)

        return fileObjList

    # added at protocol version 67
    def getFileContentsFromTrove(self, name, version, flavor, pathList,
                                 callback = None, compressed = False):
        server = version.trailingLabel().getHost()
        if self.c[server].getProtocolVersion() < 67:
            return self._getFilesFromTrove(name, version, flavor, pathList)
        pathList = [self.fromPath(x) for x in pathList]
        version = self.fromVersion(version)
        flavor = self.fromFlavor(flavor)
        if callback:
            if hasattr(callback, 'requestingFileContentsWithCount'):
                callback.requestingFileContentsWithCount(len(pathList))
            else:
                callback.requestingFileContents()

        url, sizes = self.c[server].getFileContentsFromTrove(name, version,
                                                             flavor, pathList)

        (fd, path) = util.mkstemp(suffix = 'filecontents')
        outF = util.ExtendedFile(path, "r+", buffering = False)
        os.close(fd)
        os.unlink(path)
        return self._getFileContentsObjects(server, url, sizes,
                                            pathList, callback, outF,
                                            compressed)

    def _getFilesFromTrove(self, name, version, flavor, pathList):
        # Backwards compatibility interface for getFileContentsFromTrove.
        # Should not be called directory
        trv = self.getTrove(name, version, flavor, withFiles=True)
        results = {}

        filesToGet = []
        paths = []
        for pathId, path, fileId, fileVer in trv.iterFileList():
            if path in pathList:
                filesToGet.append((fileId, fileVer))
                paths.append((path))
        fileContents = self.getFileContents(filesToGet)
        for (contents, path) in zip(fileContents, paths):
            results[path] = contents
        return [results[x] for x in pathList]

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

        if tmpFile:
            outF = tmpFile
        else:
            (fd, path) = util.mkstemp(suffix = 'filecontents')
            outF = util.ExtendedFile(path, "r+", buffering = False)
            os.close(fd)
            os.unlink(path)

        for server, itemList in byServer.iteritems():
            fileList = [ (self.fromFileId(x[1][0]),
                          self.fromVersion(x[1][1])) for x in itemList ]
            if callback:
                if hasattr(callback, 'requestingFileContentsWithCount'):
                    callback.requestingFileContentsWithCount(len(fileList))
                else:
                    callback.requestingFileContents()

            fileObjList = self.getFileContentsObjects(server, fileList,
                                                      callback, outF,
                                                      compressed)

            for (i, item), fObj in itertools.izip(itemList, fileObjList):
                contents[i] = fObj

        return contents

    def getPackageBranchPathIds(self, sourceName, branch, dirnames = [], fileIds=None):
        """
        Searches all of the troves generated from sourceName on the
        given branch, and returns the latest pathId for each path
        as a dictionary indexed by path.

        @param sourceName: name of the source trove
        @type sourceName: str
        @param branch: branch to restrict the source to
        @type branch: versions.Branch
        @param dirnames: list of directory names of the oathids being looked up
        @type dirnames: list of strings
        @param fileIds: list of preferred fileIds in case more than one matches in the repo query
        @type fileIds: list of fileId values
        """
        def _commonPrefixes(dirlist):
            # Eliminate prefixes of prefixes
            ret = []
            oldp = None
            for p in sorted(dirlist):
                if oldp and p.startswith(oldp):
                    continue
                ret.append(p)
                oldp = p
            return ret
        serverProtocol = self.c[branch].getProtocolVersion()
        if dirnames is None or serverProtocol < 39:
            args = [sourceName, self.fromVersion(branch)]
        elif serverProtocol < 62:
            # up until protocol version 62 we attemoted to limit the
            # data passed on by only listing common prefixes
            args = [sourceName, self.fromVersion(branch), _commonPrefixes(dirnames)]
        else:
            # since protocol 62, we pass on the full list of dirnames since the repository
            # has paths indexed by dirnames now, and prefix lookups are more expensive
            args = [sourceName, self.fromVersion(branch), list(set(dirnames))]
        if fileIds is not None and serverProtocol >= 42:
            # Make sure we send a (possibly empty) dirnames
            assert(dirnames is not None)
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

    def getPackageCreatorTroves(self, reposName):
        """
        Returns ((name, version, flavor), pkgData) tuples for all troves
        in the repository which have packageCreatorData troveinfo
        available.
        """
        l = self.c[reposName].getPackageCreatorTroves(reposName)
        return [ ((x[0], self.toVersion(x[1]), self.toFlavor(x[2])), x[3])
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

    def commitCheck(self, troveList):
        byServer = {}
        for tup in troveList:
            l = byServer.setdefault(tup[1].getHost(), [])
            l.append((tup[0], tup[1]))
        for host, l in byServer.iteritems():
            server = self.c[host]
            if server.getProtocolVersion() < 62:
                raise errors.InvalidServerVersion(
                        "Server %s does not have support "
                                           "for a commitCheck() call" % (host,))
            ret = self.c[host].commitCheck([(n, self.fromVersion(v)) for n,v in l])
            for (n,v), r in itertools.izip(l, ret):
                if not r:
                    raise errors.TroveAccessError(n,v)
        return True

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
        return [ (m,t,trv_mod.TroveInfo(base64.b64decode(ti)))
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
            infoList = [(self.fromTroveTup(tup), ti_)
                    for (tup, ti_) in infoList]
            if freeze: # need to freeze the troveinfo as well
                if server.getProtocolVersion() < 65:
                    skipSet = ti._newMetadataItems
                else:
                    skipSet = None
                infoList = [ (t, base64.b64encode(ti_.freeze(skipSet=skipSet)))
                             for t, ti_ in infoList ]
            total += server.setTroveInfo(infoList)
        return total

    def getNewTroveList(self, host, mark):
        server = self.c[host]
        # from server protocol 40 onward we get returned the real troveTypes
        if server.getProtocolVersion() < 40:
            return [ ( x[0],
                       (x[1][0], self.thawVersion(x[1][1]), self.toFlavor(x[1][2])),
                       trv_mod.TROVE_TYPE_NORMAL
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

    def getTimestamps(self, troveList):
        # partialTroveList is a list of (name, version) or
        # (name, version, flavor) tuples. result is a parallel list of
        # versions with timestamp information included. if any timestamps
        # cannot be found on a repository, None is returned for those
        # elements.
        byServer = {}
        for (i, troveTup) in enumerate(troveList):
            l = byServer.setdefault(troveTup[1].getHost(), [])
            l.append((i, troveTup[0:2]))

        partialOnly = False
        results = [ None ] * len(troveList)
        for host, l in byServer.iteritems():
            if self.c[host].getProtocolVersion() < 70:
                partialOnly = True
                continue

            tl = [ (x[1][0], self.fromVersion(x[1][1]) ) for x in l ]
            hostResult = self.c[host].getTimestamps(tl)

            for ((idx, troveTup), timeStamps) in itertools.izip(l, hostResult):
                if timeStamps == 0:
                    results[idx] = None
                else:
                    results[idx] = troveList[idx][1].copy()
                    results[idx].setTimeStamps(
                                 [ float(x) for x in timeStamps.split(':') ])

        if partialOnly:
            raise PartialResultsError(results)

        return results

    def getDepsForTroveList(self, troveList, provides = True, requires = True):
        # for old servers, UnsupportedCallError is raised, and the caller
        # is expected to handle it. we do it this way because the outer
        # layers often want to cache a complete trove in this case instead
        # of getting a trove and throwing it away immediately
        byServer = {}
        for i, info in enumerate(troveList):
            l = byServer.setdefault(info[1].getHost(), [])
            l.append((i, info))

        results = [ None ] * len(troveList)
        partialOnly = False
        for host, l in byServer.iteritems():
            if self.c[host].getProtocolVersion() < 70:
                partialOnly = True
                continue

            tl = [ (x[1][0], self.fromVersion(x[1][1]),
                             self.fromFlavor(x[1][2]))
                   for x in l ]
            result = self.c[host].getDepsForTroveList(tl, provides = provides,
                                                      requires = requires)

            provSet = None
            reqSet = None
            for ((idx, troveTup), (prov, req)) in itertools.izip(l, result):
                if provides:
                    provSet = self.toDepSet(prov)
                if requires:
                    reqSet = self.toDepSet(req)

                results[idx] = (provSet, reqSet)

        if partialOnly:
            raise PartialResultsError(results)

        return results

    def getTroveInfo(self, infoType, troveList):
        # first, we need to know about this infoType
        if infoType not in trv_mod.TroveInfo.streamDict.keys():
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
                        attrname = trv_mod.TroveInfo.streamDict[infoType][2]
                        results[i] = getattr(trv.troveInfo, attrname, None)
                continue
            elif (infoType >= trv_mod._TROVEINFO_TAG_CLONEDFROMLIST and
                  self.c[host].getProtocolVersion() < 64):
                # server doesn't support this troveInfo type
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
                results[i] = trv_mod.TroveInfo.streamDict[infoType][1](data)
        return results

    @api.publicApi
    def getLabelsForHost(self, hostname):
        """
        Returns the list of labels that have troves on them for the given
        host.

        @param hostname: hostname to return labels for
        @return: list of labels that exist for that host.
        """
        return [self.toLabel(x) for x
                 in self.c[hostname].getLabelsForHost(hostname) ]

    @api.publicApi
    def findTroves(self, labelPath, troves, defaultFlavor = None,
                  acrossLabels = False, acrossFlavors = False,
                  affinityDatabase = None, allowMissing=False,
                  getLeaves = True, bestFlavor = True,
                  troveTypes=TROVE_QUERY_PRESENT, exactFlavors=False,
                  requireLatest = False):
        """
        Searches for the given troveSpec requests in the context of a labelPath,
        affinityDatabase, and defaultFlavor.

        I{Version} formats accepted are:

            - ^ empty/None
            -  full version (branch + revision)
            -  branch
            -  label  (C{host@namespace:tag})
            -  branch name (C{@namespace:tag})
            -  C{:tag}
            - ^ revision (C{troveVersion-sourceCount-buildCount})
            - ^ C{troveVersion}

        I{Version} formats with a ^ by them will be limited to the branches of
        affinity troves if they exist.

        @param labelPath: label path to search for troves that don't specify a
        label/branch/version to search on
        @type labelPath: label or list of labels
        @param troves: trove specs that list the troves to search for
        @type troves: set of C{(name, versionStr, flavor)} tuples, where
        C{versionStr} or C{flavor} can be C{None}
        @param defaultFlavor: flavor to use for those troves specifying
        C{None} as their flavor.  Overridden by relevant flavors found in
        C{affinityDb}
        @type defaultFlavor: flavor or None
        @param acrossLabels: if True, for each trove, return the best
        result for each label listed in the labelPath used.  If False,
        for each trove, return the best result for the first label that
        matches.
        @type acrossLabels: bool
        @param acrossFlavors: if True, for each trove, return the best
        result for each flavor listed in the flavorPath used.  If False,
        for each trove, return the best result for the first flavor that
        matches.
        @type acrossFlavors: bool
        @param affinityDatabase: database to search for affinity troves.
        Affinity troves for a trove spec match the trove name exactly, and
        match the branch/label requested if explicitly requested in the
        trove spec.  The affinity trove's flavor will be used if no flavor
        was specified in the trove spec, and the affinity trove's branch will
        be used as if it were explicitly requested if no branch or label is
        listed in the trove spec.
        @param allowMissing: if true, do not raise an error if a trove spec
        could not be matched in the repository.
        @type allowMissing: bool
        @rtype: dict
        @return: a dict whose keys are the C{(name, versionStr, flavor)} troves
        passed to this function.  The value for each key is a list of
        C{(name, version, flavor)} tuples that match that key's trove spec.
        If C{allowMissing} is C{True}, trove specs passed in that do not match
        any trove in the repository will not be listed in the return value.
        @raises repository.errors.TroveMissing: raised if a troveSpec could
        not be matched in the repository and allowMissing is False
        """
        troveFinder = findtrove.TroveFinder(self, labelPath,
                                            defaultFlavor, acrossLabels,
                                            acrossFlavors, affinityDatabase,
                                            getLeaves, bestFlavor,
                                            troveTypes=troveTypes,
                                            exactFlavors=exactFlavors,
                                            requireLatest=requireLatest)
        return troveFinder.findTroves(troves, allowMissing)

    @api.publicApi
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

    def _commit(self, chgSet, fName, callback = None, mirror = False,
                hidden = False):
        serverName = None
        if chgSet.isEmpty():
            raise errors.CommitError('Attempted to commit an empty changeset')
        minProtocolRequired = 69

        jobs = []
        for trvCs in chgSet.iterNewTroveList():
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

        server = self.c[serverName]
        if server.getProtocolVersion() < minProtocolRequired:
            raise errors.CommitError('The changeset being committed needs '
                                     'a newer repository server.')

        url, hasStatus = server.prepareChangeSet(jobs, mirror)
        inFile = open(fName)
        size = os.fstat(inFile.fileno()).st_size

        # use chunked transfer encoding to work around servers that do not
        # handle Content-length of > 2 GiB
        chunked = False
        if size >= 0x80000000:
            chunked = True

        headers = [('X-Conary-Servername', serverName)]
        status, reason = httpPutFile(url, inFile, size, callback = callback,
                                     rateLimit = self.uploadRateLimit,
                                     proxyMap = self.c.proxyMap,
                                     chunked=chunked,
                                     headers=headers,
                                     )

        # give a slightly more helpful message for 403
        if status == 403:
            raise errors.CommitError('Permission denied. Check username, '
                                     'password, and https settings.')
        # and a generic message for a non-OK status
        if status != 200:
            raise errors.CommitError('Error uploading to repository: '
                                     '%s (%s)' %(status, reason))

        if hasStatus and callback:
            # build up a function with access to local server, url, and
            # callback variables that will get the progress updates
            def abortCheck():
                try:
                    try:
                        # first we have to unset the abort check
                        # or we'll end up in an infinite loop...
                        server.setAbortCheck(None)
                        rc = server.getCommitProgress(url)
                        # getCommitProgress returns a tuple of
                        # callback function name, arg1, arg2, ...
                        # or False if there is no info available
                        if rc:
                            if hasattr(callback, rc[0]):
                                getattr(callback, rc[0])(*rc[1:])
                            else:
                                callback.csMsg('unhandled progress update from server: %s' % (' '.join(str(x) for x in rc)))
                    except:
                        # avoid crashing out the commit process just
                        # from progress reporting
                        pass
                finally:
                    server.setAbortCheck(abortCheck)
                return False
            server.setAbortCheck(abortCheck)

        try:
            server.commitChangeSet(url, mirror, hidden)
        finally:
            server.setAbortCheck(None)


class FileResultSet(list):
    """
    Container for the result of a getFileVersions call. Behaves like a list of
    results, but also stores additional information about the result.
    """
    def __init__(self, results, errors=None):
        if errors is None:
            errors = [None] * len(results)
        assert len(results) == len(errors)
        list.__init__(self, results)
        self.errors = errors

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, list.__repr__(self))


def httpPutFile(url, inFile, size, callback = None, rateLimit = None,
        proxies=None, proxyMap=None, chunked=False, headers=(),
        withResponse=False):
    """
    send a file to a url.  Takes a wrapper, which is an object
    that has a callback() method which takes amount, total, rate
    """

    callbackFn = None
    if callback:
        wrapper = callbacks.CallbackRateWrapper(callback,
                                                callback.sendingChangeset,
                                                size)
        callbackFn = wrapper.callback

    if proxies and not proxyMap:
        proxyMap = proxy_map.ProxyMap.fromDict(proxies)
    opener = transport.XMLOpener(proxyMap=proxyMap)
    req = opener.newRequest(url, method='PUT', headers=headers)
    req.setData(inFile, size, callback=callbackFn, chunked=chunked,
            rateLimit=rateLimit)
    response = opener.open(req)
    if withResponse:
        return response
    else:
        return response.status, response.reason
