#
# Copyright (c) 2004-2006 rPath, Inc.
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

import base64
import gzip
import httplib
import itertools
import os
import socket
import sys
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
from conary.repository import changeset
from conary.repository import errors
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

CLIENT_VERSIONS = [ 36, 37 ]

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
                 altHostCallback):
        xmlrpclib._Method.__init__(self, send, name)
        self.__host = host
        self.__pwCallback = pwCallback
        self.__anonymousCallback = anonymousCallback
        self.__altHostCallback = altHostCallback

    def __repr__(self):
        return "<netclient._Method(%s, %r)>" % (self._Method__send, self._Method__name) 

    def __str__(self):
        return self.__repr__()

    def __call__(self, *args):
        return self.doCall(CLIENT_VERSIONS[-1], *args)

    def __doCall(self, clientVersion, argList):
        newArgs = ( clientVersion, ) + argList

        try:
            usedAnonymous, isException, result = self.__send(self.__name,
                                                             newArgs)
        except xmlrpclib.ProtocolError, e:
            if e.errcode == 403:
                raise errors.InsufficientPermission(e.url.split("/")[2])
            raise
        if usedAnonymous:
            self.__anonymousCallback()

	if not isException:
	    return result
        else:
            self.handleError(result)

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
            raise
        except:
            raise

        return self.__doCall(clientVersion, args)

    def handleError(self, result):
	exceptionName = result[0]
	exceptionArgs = result[1:]

	if exceptionName == "TroveMissing":
	    (name, version) = exceptionArgs
	    if not name: name = None
	    if not version:
		version = None
	    else:
		version = shims.toVersion(version)
	    raise errors.TroveMissing(name, version)
        elif exceptionName == "MethodNotSupported":
	    raise errors.MethodNotSupported(exceptionArgs[0])
        elif exceptionName == "IntegrityError":
	    raise errors.IntegrityError(exceptionArgs[0])
        elif exceptionName == "TroveIntegrityError":
            if len(exceptionArgs) > 1:
                # old repositories give TIE w/ no
                # trove information or with a string error message.
                # exceptionArgs[0] is that message if exceptionArgs[1]
                # is not set or is empty.
                raise errors.TroveIntegrityError(error=exceptionArgs[0], 
                                            *self.toTroveTup(exceptionArgs[1]))
            else:
                raise errors.TroveIntegrityError(error=exceptionArgs[0])
        elif exceptionName == "TroveSchemaError":
            # value 0 is the full message, for older clients that don't
            # know about this exception
            n, v, f = self.toTroveTup(exceptionArgs[1])
            raise errors.TroveSchemaError(n, v, f,
                                          exceptionArgs[2], exceptionArgs[3])
        elif exceptionName == errors.TroveChecksumMissing.__name__:
            raise errors.TroveChecksumMissing(*self.toTroveTup(exceptionArgs[1]))
        elif exceptionName == errors.RepositoryMismatch.__name__:
            raise errors.RepositoryMismatch(*exceptionArgs)
        elif exceptionName == 'FileContentsNotFound':
            raise errors.FileContentsNotFound((self.toFileId(exceptionArgs[0]),
                                               self.toVersion(exceptionArgs[1])))
        elif exceptionName == 'FileStreamNotFound':
            raise errors.FileStreamNotFound((self.toFileId(exceptionArgs[0]),
                                             self.toVersion(exceptionArgs[1])))
        elif exceptionName == 'FileHasNoContents':
            raise errors.FileHasNoContents((self.toFileId(exceptionArgs[0]),
                                            self.toVersion(exceptionArgs[1])))
        elif exceptionName == 'FileStreamMissing':
            raise errors.FileStreamMissing((self.toFileId(exceptionArgs[0])))
        elif exceptionName == 'RepositoryLocked':
            raise errors.RepositoryLocked
	else:
            for klass, marshall in errors.simpleExceptions:
                if exceptionName == marshall:
                    raise klass(exceptionArgs[0])
	    raise errors.UnknownException(exceptionName, exceptionArgs)

class ServerProxy(xmlrpclib.ServerProxy):

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

        self.__host = '%s:%s@%s' % (user, password, fullHost)

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

    def __getattr__(self, name):
        #from conary.lib import log
        #log.debug('Calling %s:%s' % (self.__host.split('@')[-1], name))
        return _Method(self.__request, name, self.__host, 
                       self.__passwordCallback, self.__usedAnonymousCallback,
                       self.__altHostCallback)

    def setAbortCheck(self, check):
        self.__transport.setAbortCheck(check)

    def __init__(self, url, serverName, transporter, pwCallback, usedMap):
        try:
            xmlrpclib.ServerProxy.__init__(self, url, transporter)
        except IOError, e:
            proto, url = urllib.splittype(url)
            raise errors.OpenError('Error occurred opening repository '
                                   '%s: %s' % (_cleanseUrl(proto, url), e))
        self.__pwCallback = pwCallback
        self.__altHost = None
        self.__serverName = serverName
        self.__usedMap = usedMap

class ServerCache:
    def __init__(self, repMap, userMap, pwPrompt=None,
                 entitlementDir=None, entitlements={}, callback=None):
	self.cache = {}
	self.map = repMap
	self.userMap = userMap
	self.pwPrompt = pwPrompt
        self.entitlementDir = entitlementDir
        self.entitlements = entitlements

    def __getPassword(self, host, user=None):
        user, pw = self.pwPrompt(host, user)
        if user is None or pw is None:
            return None, None
        self.userMap.append((host, user, pw))
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

        # check for an entitlement for this server
        ent = self.entitlements.get(serverName, None)
        if ent is None:
            ent = conarycfg.loadEntitlement(self.entitlementDir, serverName)

        usedMap = url is not None
        if url is None:
            if ent or userInfo:
                # if we have a username/password, use https
                protocol = 'https'
            else:
                # if we are using anonymous, use http
                protocol = 'http'

            if userInfo is None:
                url = "%s://%s/conary/" % (protocol, serverName)
            else:
                url = "%s://%s:%s@%s/conary/" % (protocol,
                                                 quote(userInfo[0]),
                                                 quote(userInfo[1]),
                                                 serverName)
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

        protocol, uri = urllib.splittype(url)
        transporter = transport.Transport(https = (protocol == 'https'),
                                          entitlement = ent)
        transporter.setCompress(True)
        server = ServerProxy(url, serverName, transporter, self.__getPassword,
                             usedMap = usedMap)
        try:
            serverVersions = server.checkVersion()
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
            raise errors.OpenError('Error occurred opening repository '
                        '%s: %s' % (url, errmsg))

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
        server._protocolVersion = max(intersection)
        self.cache[serverName] = server

	return server

    def getPwPrompt(self):
        return self.pwPrompt

    def getUserMap(self):
        return self.userMap

class NetworkRepositoryClient(xmlshims.NetworkConvertors,
			      repository.AbstractRepository, 
                              trovesource.SearchableTroveSource):
    # fixme: take a cfg object instead of all these parameters
    def __init__(self, repMap, userMap,
                 localRepository = None, pwPrompt = None,
                 entitlementDir = None, downloadRateLimit = 0,
                 uploadRateLimit = 0, entitlements = {}):
        # the local repository is used as a quick place to check for
        # troves _getChangeSet needs when it's building changesets which
        # span repositories. it has no effect on any other operation.
        if pwPrompt is None:
            pwPrompt = lambda x, y: (None, None)

        self.downloadRateLimit = downloadRateLimit
        self.uploadRateLimit = uploadRateLimit

	self.c = ServerCache(repMap, userMap, pwPrompt, entitlementDir,
                             entitlements)
        self.localRep = localRepository

        trovesource.SearchableTroveSource.__init__(self)
        self.searchAsRepository()

    def __del__(self):
        self.c = None

    def close(self, *args):
        pass

    def open(self, *args):
        pass

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
        signature = trove.DigitalSignature()
        signature.set(digsig)
        encSig = base64.b64encode(signature.freeze())
        self.c[version].addDigitalSignature(name, self.fromVersion(version),
                                            self.fromFlavor(flavor),
                                            encSig)

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

    def addAcl(self, reposLabel, userGroup, trovePattern, label, write,
               capped, admin):
        if not label:
            label = "ALL"
        else:
            label = self.fromLabel(label)

        if not trovePattern:
            trovePattern = "ALL"

        self.c[reposLabel].addAcl(userGroup, trovePattern, label, write,
                                  capped, admin)

    def editAcl(self, reposLabel, userGroup, oldTrovePattern, oldLabel,
                trovePattern, label, write, capped, admin):
        if not label:
            label = "ALL"
        else:
            label = self.fromLabel(label)

        if not oldLabel:
            oldLabel = "ALL"
        else:
            oldLabel = self.fromLabel(oldLabel)

        if not trovePattern:
            trovePattern = "ALL"

        if not oldTrovePattern:
            oldTrovePattern = "ALL"

        self.c[reposLabel].editAcl(userGroup, oldTrovePattern, oldLabel,
                                   trovePattern, label, write, capped, admin)

        return True

    def changePassword(self, label, user, newPassword):
        self.c[label].changePassword(user, newPassword)

    def getUserGroups(self, label):
        return self.c[label].getUserGroups()

    def addEntitlement(self, serverName, entGroup, entitlement):
        entitlement = self.fromEntitlement(entitlement)
        return self.c[serverName].addEntitlement(entGroup, entitlement)

    def deleteEntitlement(self, serverName, entGroup, entitlement):
        entitlement = self.fromEntitlement(entitlement)
        return self.c[serverName].deleteEntitlement(entGroup, entitlement)

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

    def listAccessGroups(self, serverName):
        return self.c[serverName].listAccessGroups()

    def troveNames(self, label):
	return self.c[label].troveNames(self.fromLabel(label))

    def troveNamesOnServer(self, server):
        return self.c[server].troveNames("")
    
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

    def getAllTroveLeaves(self, serverName, troveNameList):
        req = {}
        for name, flavors in troveNameList.iteritems():
            if name is None:
                name = ''

            if flavors is None:
                req[name] = True
            else:
                req[name] = [ self.fromFlavor(x) for x in flavors ]

	d = self.c[serverName].getAllTroveLeaves(req)

        return self._mergeTroveQuery({}, d)

    def getTroveVersionList(self, serverName, troveNameList):
        req = {}
        for name, flavors in troveNameList.iteritems():
            if name is None:
                name = ''

            if flavors is None:
                req[name] = True
            else:
                req[name] = [ self.fromFlavor(x) for x in flavors ]

	d = self.c[serverName].getTroveVersionList(req)
        return self._mergeTroveQuery({}, d)

    def getTroveLeavesByLabel(self, troveSpecs, bestFlavor = False):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor, 
                                           'getTroveLeavesByLabel', 
                                           labels = True)

    def getTroveVersionsByLabel(self, troveSpecs, bestFlavor = False):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor, 
                                           'getTroveVersionsByLabel', 
                                           labels = True)

    def getTroveVersionFlavors(self, troveSpecs, bestFlavor = False):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor,
                                           'getTroveVersionFlavors',
                                           versions = True)

    def getAllTroveFlavors(self, troveDict):
        d = {}
        for name, versionList in troveDict.iteritems():
            d[name] = {}.fromkeys(versionList, [ None ])

	return self.getTroveVersionFlavors(d)

    def _getTroveInfoByVerInfo(self, troveSpecs, bestFlavor, method, 
                               branches = False, labels = False, 
                               versions = False):
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

                if flavors is None:
                    flavorDict[verStr] = ''
                else:
                    flavorDict[verStr] = [ self.fromFlavor(x) for x in flavors ]

        result = {}
	if not d:
	    return result

        for host, requestD in d.iteritems():
            respD = self.c[host].__getattr__(method)(requestD, bestFlavor)
            self._mergeTroveQuery(result, respD)

        return result

    def getTroveLeavesByBranch(self, troveSpecs, bestFlavor = False):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor, 
                                           'getTroveLeavesByBranch', 
                                           branches = True)

    def getTroveVersionsByBranch(self, troveSpecs, bestFlavor = False):
        return self._getTroveInfoByVerInfo(troveSpecs, bestFlavor, 
                                           'getTroveVersionsByBranch', 
                                           branches = True)

    def getTroveLatestVersion(self, troveName, branch):
	b = self.fromBranch(branch)
	v = self.c[branch].getTroveLatestVersion(troveName, b)
        if v == 0:
            raise errors.TroveMissing(troveName, branch)
	return self.thawVersion(v)

    def hasTrove(self, name, version, flavor):
        return self.hasTroves([(name, version, flavor)])[name, version, flavor]

    def hasTroves(self, troveInfoList):
        if not troveInfoList:
            return {}
        byServer = {}
        for name, version, flavor in troveInfoList:
            l = byServer.setdefault(version.branch().label().getHost(), [])
            l.append(((name, version, flavor),
                      (name, self.fromVersion(version), 
                             self.fromFlavor(flavor))))

        d = {}
        for server, l in byServer.iteritems():
            if server == 'local':
                exists = [False] * len(l)
            else:
                exists = self.c[server].hasTroves([x[1] for x in l])
            d.update(dict(itertools.izip((x[0] for x in l), exists)))

        return d

    def getTrove(self, troveName, troveVersion, troveFlavor, withFiles = True):
	rc = self.getTroves([(troveName, troveVersion, troveFlavor)],
                            withFiles = withFiles)
	if rc[0] is None:
	    raise errors.TroveMissing(troveName, version = troveVersion)

	return rc[0]

    def getTroves(self, troves, withFiles = True, callback = None):
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

    def createChangeSet(self, jobList, withFiles = True,
                        withFileContents = True,
                        excludeAutoSource = False, recurse = True,
                        primaryTroveList = None, callback = None):
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
                            primaryTroveList = None, callback = None):
        """
        @raise FilesystemError: if the destination file is not writable
        """
        return self._getChangeSet(jobList, target = fName,
                                  recurse = recurse,
                                  primaryTroveList = primaryTroveList,
                                  callback = callback)

    def _getChangeSet(self, chgSetList, recurse = True, withFiles = True,
		      withFileContents = True, target = None,
                      excludeAutoSource = False, primaryTroveList = None,
                      callback = None, forceLocalGeneration = False):
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

        def _separateJobList(jobList, forceLocalGeneration):
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

                if old:
                    if old.getHost() == serverName:
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
                            chgSetList):
            abortCheck = None
            if callback:
                callback.requestingChangeSet()
                abortCheck = callback.checkAbort
            server.setAbortCheck(abortCheck)
            (url, sizes, extraTroveList, extraFileList) = \
                  server.getChangeSet(job, recurse,
                                      withFiles, withFileContents,
                                      excludeAutoSource)
            server.setAbortCheck(None)

            inF = urllib.urlopen(url)

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
                sys.exit(0)
            #assert(totalSize == sum(sizes))
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
                           filesNeeded, chgSetList):
            (newCs, extraTroveList, extraFileList) = \
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
        filesNeeded = []

        if target:
            try:
                outFile = open(target, "w+")
            except IOError, e:
                strerr = "Error writing to file %s: %s" % (e.filename,
                    e.strerror)
                raise errors.FilesystemError(e.errno, e.filename, e.strerror,
                    strerr)
        else:
            (outFd, tmpName) = util.mkstemp()
            outFile = os.fdopen(outFd, "w+")
            os.unlink(tmpName)

        if primaryTroveList is None:
            # (name, version, release) list. removed troves aren't primary
            primaryTroveList = [ (x[0], x[2][0], x[2][1]) for x in chgSetList 
                                        if x[2][0] is not None ]

        while chgSetList:
            (serverJobs, ourJobList) = _separateJobList(chgSetList,
                                                        forceLocalGeneration)

            chgSetList = []

            for serverName, job in serverJobs.iteritems():
                server = self.c[serverName]

                args = (target, cs, server, job, recurse, withFiles,
                        withFileContents, excludeAutoSource,
                        filesNeeded, chgSetList)

                try:
                    if server.__class__ == ServerProxy:
                        # this is a XML-RPC proxy for a remote repository
                        rc = _getCsFromRepos(*args)
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
                filesNeeded += extraFileList

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

                (troveChgSet, newFilesNeeded, pkgsNeeded) = \
                                new.diff(old, absolute = absolute)
                # newFilesNeeded = [ (pathId, oldFileVersion, newFileVersion) ]
                filesNeeded += [ (x[0], troveName, 
                        (oldVersion, oldFlavor, x[1], x[2]),
                        (newVersion, newFlavor, x[3], x[4])) for x in newFilesNeeded ]

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

            fileObjs = self.getFileVersions(need, lookInLocal = True)
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

		(filecs, hash) = changeset.fileChangeSet(pathId, oldFileObj, 
                                                         newFileObj)

		internalCs.addFile(oldFileId, newFileId, filecs)

                if excludeAutoSource and newFileObj.flags.isAutoSource():
                    continue

                if withFileContents and hash:
                    # pull contents from the trove it was originally
                    # built in
                    fetchItems = []
                    needItems = []

                    if changeset.fileContentsUseDiff(oldFileObj, newFileObj):
                        fetchItems.append( (oldFileId, oldFileVersion, 
                                            oldFileObj) ) 
                        needItems.append( (pathId, oldFileObj) ) 

                    fetchItems.append( (newFileId, newFileVersion, newFileObj) )
                    needItems.append( (pathId, newFileObj) )
                    contentsNeeded += fetchItems


                    fileJob += (needItems,)

            contentList = self.getFileContents(contentsNeeded, 
                                               tmpFile = outFile,
                                               lookInLocal = True,
                                               callback = callback)

            i = 0
            for item in fileJob:
                pathId = item[0][0]
                fileObj = item[0][1]
                contents = contentList[i]
                i += 1

                if len(item) == 1:
                    internalCs.addFileContents(pathId, 
                                   changeset.ChangedFileTypes.file, 
                                   contents, 
                                   fileObj.flags.isConfig())
                else:
                    newFileObj = item[1][1]
                    newContents = contentList[i]
                    i += 1

                    (contType, cont) = changeset.fileContentsDiff(fileObj, 
                                            contents, newFileObj, newContents)
                    internalCs.addFileContents(pathId, contType,
                                               cont, True)

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

        if target and cs:
            if cs.oldTroves or cs.newTroves:
                os.unlink(target)
                cs.writeToFile(target)

            cs = None
        elif target:
            os.unlink(target)

        if callback and missingFiles:
            assert(hasattr(callback, 'missingFiles'))
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

            if not callback.missingFiles(mfs):
                # Grab just the first file
                mf = mfs[0]
                raise errors.FileStreamMissing(mf[5])

	return cs

    def resolveDependencies(self, label, depList):
        l = [ self.fromDepSet(x) for x in depList ]
        d = self.c[label].getDepSuggestions(self.fromLabel(label), l)
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


    def getFileVersions(self, fullList, lookInLocal = False):
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
                    missingFileId = self.fromFileId(e.fileId)
                    if missingFileId not in sentFiles:
                        # This shouldn't happen - the server sent us a file id
                        # that we don't know about
                        raise Exception("Invalid file ID", missingFileId)

                    # Remove this file from the big dictionary and try again
                    del sentFiles[missingFileId]
                    continue

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
                        callback = None):
        contents = [ None ] * len(fileList)

        if self.localRep and lookInLocal:
            for i, item in enumerate(fileList):
                if len(item) < 3: continue

                sha1 = item[2].contents.sha1()
                if self.localRep._hasFileContents(sha1):
                    # retrieve the contents from the database now so that
                    # the changeset can be shared between threads
                    c = self.localRep.getFileContents([item])[0].get().read()
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
                callback.requestingFileContents()
            (url, sizes) = self.c[server].getFileContents(fileList)
            assert(len(sizes) == len(fileList))

            inF = urllib.urlopen(url)

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
                (fd, path) = util.mkstemp()
                os.unlink(path)
                outF = os.fdopen(fd, "r+")
                start = 0

            totalSize = util.copyfileobj(inF, outF,
                                         rateLimit = self.downloadRateLimit,
                                         callback = copyCallback)
            del inF

            for (i, item), size in itertools.izip(itemList, sizes):
                nestedF = util.SeekableNestedFile(outF, size, start)

                totalSize -= size
                start += size

                gzfile = gzip.GzipFile(fileobj = nestedF)

                contents[i] = filecontents.FromGzFile(gzfile)

            assert(totalSize == 0)

        return contents

    def getPackageBranchPathIds(self, sourceName, branch):
        """
        Searches all of the troves generated from sourceName on the
        given branch, and returns the latest pathId for each path
        as a dictionary indexed by path.

        @param sourceName: name of the source trove
        @type sourceName: str
        @param branch: branch to restrict the source to
        @type branch: versions.Branch
        """
        ids = self.c[branch].getPackageBranchPathIds(sourceName,
                                                     self.fromVersion(branch))
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
                    
    def commitChangeSetFile(self, fName, mirror = False, callback = None):
        cs = changeset.ChangeSetFromFile(fName)
        return self._commit(cs, fName, mirror = mirror, callback = callback)

    def commitChangeSet(self, chgSet, callback = None, mirror = False):
	(outFd, path) = util.mkstemp()
	os.close(outFd)
	chgSet.writeToFile(path)

	try:
            result = self._commit(chgSet, path, callback = callback,
                                  mirror = mirror)
        finally:
            os.unlink(path)

        return result

    def getTroveSigs(self, troveList):
        byServer = {}
        results = [ None ] * len(troveList)
        for i, info in enumerate(troveList):
            l = byServer.setdefault(info[1].branch().label().getHost(), [])
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
            l = byServer.setdefault(item[0][1].branch().label().getHost(), [])
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

    def getNewTroveList(self, host, mark):
        return [ (x[0], (x[1][0], self.thawVersion(x[1][1]), 
                                  self.toFlavor(x[1][2]))) for
                    x in self.c[host].getNewTroveList(mark) ]

    def addPGPKeyList(self, host, keyList):
        self.c[host].addPGPKeyList([ base64.encodestring(x) for x in keyList ])
        
    def getNewPGPKeys(self, host, mark):
        return [ base64.decodestring(x) for x in 
                    self.c[host].getNewPGPKeys(mark) ]

    def findTroves(self, labelPath, troves, defaultFlavor = None, 
                  acrossLabels = False, acrossFlavors = False,
                  affinityDatabase = None, allowMissing=False, 
                  getLeaves = True, bestFlavor = True):
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
                                            getLeaves, bestFlavor)
        return troveFinder.findTroves(troves, allowMissing)

    def findTrove(self, labelPath, (name, versionStr, flavor), 
                  defaultFlavor=None, acrossLabels = False, 
                  acrossFlavors = False, affinityDatabase = None,
                  getLeaves = True, bestFlavor = True):
        res = self.findTroves(labelPath, ((name, versionStr, flavor),),
                              defaultFlavor, acrossLabels, acrossFlavors,
                              affinityDatabase, False, getLeaves, bestFlavor)
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

    def _commit(self, chgSet, fName, callback = None, mirror = False):
	serverName = None
        if chgSet.isEmpty():
            raise errors.CommitError('Attempted to commit an empty changeset')
            
	for trove in chgSet.iterNewTroveList():
	    v = trove.getOldVersion()
	    if v:
		if serverName is None:
		    serverName = v.getHost()
		assert(serverName == v.getHost())

	    v = trove.getNewVersion()
	    if serverName is None:
		serverName = v.getHost()
	    assert(serverName == v.getHost())

        server = self.c[serverName]
        url = server.prepareChangeSet()

        self._putFile(url, fName, callback = callback)

        if mirror:
            # avoid sending the mirror keyword unless we have to.
            # this helps preserve backwards compatibility with old
            # servers.
            server.commitChangeSet(url, mirror)
        else:
            server.commitChangeSet(url)

    def _putFile(self, url, path, callback = None):
        """
        send a file to a url.  Takes a wrapper, which is an object
        that has a callback() method which takes amount, total, rate
        """
        protocol, uri = urllib.splittype(url)
        assert(protocol in ('http', 'https'))
	(host, putPath) = url.split("/", 3)[2:4]
        if protocol == 'http':
            c = httplib.HTTPConnection(host)
        else:
            c = httplib.HTTPSConnection(host)

	f = open(path)
        size = os.fstat(f.fileno()).st_size
        BUFSIZE = 8192

        callbackFn = None
        if callback:
            wrapper = callbacks.CallbackRateWrapper(callback,
                                                    callback.sendingChangeset,
                                                    size)
            callbackFn = wrapper.callback

	c.connect()
        c.putrequest("PUT", url)
        c.putheader('Content-length', str(size))
        c.endheaders()

        c.url = url

        util.copyfileobj(f, c, bufSize=BUFSIZE, callback=callbackFn,
                         rateLimit = self.uploadRateLimit)

	r = c.getresponse()
        # give a slightly more helpful message for 403
        if r.status == 403:
            raise errors.CommitError('Permission denied. Check username, '
                                     'password, and https settings.')
        # and a generic message for a non-OK status
        if r.status != 200:
            raise errors.CommitError('Error uploading to repository: '
                                     '%s (%s)' %(r.status, r.reason))

