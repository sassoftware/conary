#
# Copyright (c) 2004-2005 Specifix, Inc.
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

import base64
import exceptions
import filecontents
import files
import gzip
import httplib
import itertools
import xml
from lib import log
import os
import repository
import changeset
import metadata
import socket
import tempfile
import transport
import trove
import urllib
from lib import util
import versions
import xmlrpclib
import xmlshims
from deps import deps

shims = xmlshims.NetworkConvertors()

CLIENT_VERSIONS = [ 30, 31 ]

class _Method(xmlrpclib._Method):

    def __repr__(self):
        return "<netclient._Method(%s, %r)>" % (self._Method__send, self._Method__name) 

    def __str__(self):
        return self.__repr__()

    def __call__(self, *args):
        return self.doCall(CLIENT_VERSIONS[-1], *args)

    def doCall(self, clientVersion, *args):
        newArgs = ( clientVersion, ) + args
        isException, result = self.__send(self.__name, newArgs)
	if not isException:
	    return result

	exceptionName = result[0]
	exceptionArgs = result[1:]

	if exceptionName == "TroveMissing":
	    (name, version) = exceptionArgs
	    if not name: name = None
	    if not version:
		version = None
	    else:
		version = shims.toVersion(version)
	    raise repository.TroveMissing(name, version)
	elif exceptionName == "CommitError":
	    raise repository.CommitError(exceptionArgs[0])
	elif exceptionName == "DuplicateBranch":
	    raise repository.DuplicateBranch(exceptionArgs[0])
        elif exceptionName == "MethodNotSupported":
	    raise repository.MethodNotSupported(exceptionArgs[0])
        elif exceptionName in "InvalidClientVersion":
            from netrepos import netserver
	    raise netserver.InvalidClientVersion, exceptionArgs[0]
        elif exceptionName == "UserAlreadyExists":
            import netrepos
	    raise UserAlreadyExists(exceptionArgs[0])
	else:
	    raise UnknownException(exceptionName, exceptionArgs)

class ServerProxy(xmlrpclib.ServerProxy):

    def __getattr__(self, name):
        return _Method(self.__request, name)

class ServerCache:

    def __getitem__(self, item):
	if isinstance(item, versions.Label):
	    serverName = item.getHost()
	elif isinstance(item, str):
	    serverName = item
	else:
            if isinstance(item, versions.Branch):
		serverName = item.label().getHost()
	    else:
		serverName = item.branch().label().getHost()

	server = self.cache.get(serverName, None)
	if server is None:
	    url = self.map.get(serverName, None)
	    if isinstance(url, repository.AbstractTroveDatabase):
		return url

	    if url is None:
		url = "http://%s/conary/" % serverName
            protocol, uri = urllib.splittype(url)
            if protocol == 'http':
                transporter = transport.Transport(https=False)
            elif protocol == 'https':
                transporter = transport.Transport(https=True)
            server = ServerProxy(url, transporter)
	    self.cache[serverName] = server

	    try:
                serverVersions = server.checkVersion()
	    except Exception, e:
                if isinstance(e, socket.error):
                    errmsg = e[1]
                # includes OS and IO errors
                elif isinstance(e, exceptions.EnvironmentError):
                    errmsg = e.strerror
                    # sometimes there is a socket error hiding 
                    # inside an IOError!
                    if isinstance(errmsg, socket.error):
                        errmsg = errmsg[1]
                else:
                    errmsg = str(e)
                if url.find('@') != -1:
                    url = protocol + '://<user>:<pwd>@' + url.split('@')[1]
		raise repository.OpenError('Error occured opening repository '
			    '%s: %s' % (url, errmsg))

            intersection = set(serverVersions) & set(CLIENT_VERSIONS)
            if not intersection:
                raise InvalidServerVersion, \
                   ("Invalid server version.  Server accepts client "
                    "versions %s, but this client only supports versions %s"
                    " - download a valid client from www.specifix.com") % \
                    (",".join([str(x) for x in serverVersions]),
                     ",".join([str(x) for x in CLIENT_VERSIONS]))

            server.serverVersion = max(intersection)

	return server
		
    def __init__(self, repMap):
	self.cache = {}
	self.map = repMap

class NetworkRepositoryClient(xmlshims.NetworkConvertors,
			      repository.AbstractRepository):

    def close(self, *args):
        pass

    def open(self, *args):
        pass

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

    def addAcl(self, reposLabel, userGroup, trovePattern, label, write,
               capped, admin):
        if not label:
            label = ""
        else:
            label = self.fromLabel(label)

        if not trovePattern:
            trovePattern = ""

        self.c[reposLabel].addAcl(userGroup, trovePattern, label, write,
                                  capped, admin)

    def troveNames(self, label):
	return self.c[label].troveNames(self.fromLabel(label))

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
        # XXX this code should most likely go away, and anything that
        # uses it should be written to use other functions
        l = [(troveName, (None, None), (version, flavor), True)]
        cs = self._getChangeSet(l, recurse = False, withFiles = True,
                                withFileContents = False)
        try:
            trvCs = cs.getNewPackageVersion(troveName, version, flavor)
        except KeyError:
            raise StopIteration
        
        t = trove.Trove(trvCs.getName(), trvCs.getOldVersion(),
                        trvCs.getNewFlavor(), trvCs.getChangeLog())
        t.applyChangeSet(trvCs)
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
                if branches:
                    host = ver.label().getHost()
                    verStr = self.fromBranch(ver)
                elif versions:
                    host = ver.branch().label().getHost()
                    verStr = self.fromVersion(ver)
                else:
                    host = ver.getHost()
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
            raise repository.TroveMissing(troveName, branch)
	return self.thawVersion(v)

    def getTrove(self, troveName, troveVersion, troveFlavor, withFiles = True):
	rc = self.getTroves([(troveName, troveVersion, troveFlavor)],
                            withFiles = withFiles)
	if rc[0] is None:
	    raise repository.TroveMissing(troveName, version = troveVersion)

	return rc[0]

    def getTroves(self, troves, withFiles = True):
	chgSetList = []
	for (name, version, flavor) in troves:
	    chgSetList.append((name, (None, None), (version, flavor), True))
	
	cs = self._getChangeSet(chgSetList, recurse = False, 
                                withFiles = withFiles,
                                withFileContents = False)

	l = []
        # walk the list so we can return the troves in the same order
        for (name, version, flavor) in troves:
            try:
                pkgCs = cs.getNewPackageVersion(name, version, flavor)
            except KeyError:
                l.append(None)
                continue
            
            t = trove.Trove(pkgCs.getName(), pkgCs.getOldVersion(),
                            pkgCs.getNewFlavor(), pkgCs.getChangeLog())
            t.applyChangeSet(pkgCs)
            l.append(t)

	return l

    def createChangeSet(self, list, withFiles = True, withFileContents = True,
                        excludeAutoSource = False, recurse = True,
                        primaryTroveList = None):
	return self._getChangeSet(list, withFiles = withFiles, 
                                  withFileContents = withFileContents,
                                  excludeAutoSource = excludeAutoSource,
                                  recurse = recurse, 
                                  primaryTroveList = primaryTroveList)

    def createChangeSetFile(self, list, fName, recurse = True,
                            primaryTroveList = None):
	self._getChangeSet(list, target = fName, recurse = recurse,
                           primaryTroveList = primaryTroveList)

    def _getChangeSet(self, chgSetList, recurse = True, withFiles = True,
		      withFileContents = True, target = None,
                      excludeAutoSource = False, primaryTroveList = None):
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

        def _separateJobList(jobList):
            serverJobs = {}
            ourJobList = []
            for (troveName, (old, oldFlavor), (new, newFlavor), absolute) in \
                    jobList:
                if not new:
                    ourJobList.append((troveName, (old, oldFlavor),
                                       (new, newFlavor), absolute))
                    continue

                serverName = new.branch().label().getHost()
                if not serverJobs.has_key(serverName):
                    serverJobs[serverName] = []

                if old:
                    if old.branch().label().getHost() == serverName:
                        serverJobs[serverName].append((troveName, 
                                  (self.fromVersion(old), 
                                   self.fromFlavor(oldFlavor)), 
                                  (self.fromVersion(new), 
                                   self.fromFlavor(newFlavor)),
                                  absolute))
                    else:
                        ourJobList.append((troveName, (old, oldFlavor),
                                           (new, newFlavor), absolute))
                else:
                    serverJobs[serverName].append((troveName, 
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

        if not chgSetList:
            # no need to work hard to find this out
            return changeset.ReadOnlyChangeSet()

        cs = None
        scheduledSet = {}
        internalCs = None
        urlsFetched = 0
        filesNeeded = []

        if target:
            outFile = open(target, "w+")
        else:
            (outFd, tmpName) = tempfile.mkstemp()
            outFile = os.fdopen(outFd, "w+")
            os.unlink(tmpName)

        if primaryTroveList is None:
            # (name, version, release) list. removed troves aren't primary
            primaryTroveList = [ (x[0], x[2][0], x[2][1]) for x in chgSetList 
                                        if x[2][0] is not None ]

        while chgSetList:
            (serverJobs, ourJobList) = _separateJobList(chgSetList)

            chgSetList = []

            for serverName, job in serverJobs.iteritems():
                (url, sizes, extraTroveList, extraFileList) = \
                    self.c[serverName].getChangeSet(job, recurse, 
                                                withFiles, withFileContents,
                                                excludeAutoSource)

                chgSetList += _cvtTroveList(extraTroveList)
                filesNeeded += _cvtFileList(extraFileList)

                urlsFetched += len(sizes)

                inF = urllib.urlopen(url)

                try:
                    # seek to the end of the file
                    outFile.seek(0, 2)
                    start = outFile.tell()
                    totalSize = util.copyfileobj(inF, outFile)
                    #assert(totalSize == sum(sizes))
                    inF.close()
                except:
                    if target and os.path.exists(target):
                        os.unlink(target)
                    elif os.path.exists(tmpName):
                        os.unlink(tmpName)
                    raise

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

            if (ourJobList or filesNeeded) and not internalCs:
                internalCs = changeset.ChangeSet()

            # handle everything in ourJobList which is just a deletion
            delList = []
            for i, (troveName, (oldVersion, oldFlavor),
                  (newVersion, newFlavor), absolute) in enumerate(ourJobList):
                if not newVersion:
                    internalCs.oldPackage(troveName, oldVersion, oldFlavor)
                    delList.append(i)

            for i in reversed(delList):
                del ourJobList[i]
            del delList

            # generate this change set, and put any recursive generation
            # which is needed onto the chgSetList for the next pass
            allTrovesNeeded = []
            for (troveName, (oldVersion, oldFlavor),
                            (newVersion, newFlavor), absolute) in ourJobList:
                # old version and new version are both set, otherwise
                # we wouldn't need to generate the change set ourself
                allTrovesNeeded.append((troveName, oldVersion, oldFlavor))
                allTrovesNeeded.append((troveName, newVersion, newFlavor))

            troves = _getLocalTroves(allTrovesNeeded)
            remoteTrovesNeeded = []
            indices = []
            for i, (trove, req) in enumerate(zip(troves, allTrovesNeeded)):
                if trove is None:
                    remoteTrovesNeeded.append(req)
                    indices.append(i)

            remoteTroves = self.getTroves(remoteTrovesNeeded)
            for i, trove in zip(indices, remoteTroves):
                troves[i] = trove

            del allTrovesNeeded, remoteTrovesNeeded, indices, remoteTroves

            i = 0
            for (troveName, (oldVersion, oldFlavor),
                            (newVersion, newFlavor), absolute) in ourJobList:
                old = troves[i]
                new = troves[i + 1]
                i += 2

                (pkgChgSet, newFilesNeeded, pkgsNeeded) = \
                                new.diff(old, absolute = absolute) 
                # newFilesNeeded = [ (pathId, oldFileVersion, newFileVersion) ]
                filesNeeded += [ (x[0], troveName, 
                        (oldVersion, oldFlavor, x[1], x[2]),
                        (newVersion, newFlavor, x[3], x[4])) for x in newFilesNeeded ]

                if recurse:
                    for (otherTroveName, otherOldVersion, otherNewVersion, 
                         otherOldFlavor, otherNewFlavor) in pkgsNeeded:
                        chgSetList.append((otherTroveName, 
                                           (otherOldVersion, otherOldFlavor),
                                           (otherNewVersion, otherNewFlavor),
                                           absolute))

                internalCs.newPackage(pkgChgSet)

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
            del fileObj, fileObjs, need

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

		(filecs, hash) = changeset.fileChangeSet(pathId, oldFileObj, 
                                                         newFileObj)

		internalCs.addFile(oldFileId, newFileId, filecs)

                if withFileContents and hash:
                    # pull contents from the trove it was originally
                    # built in
                    fetchItems = []
                    needItems = []

                    if changeset.fileContentsUseDiff(oldFileObj, newFileObj):
                        fetchItems.append( (fileId, oldFileVersion, 
                                            oldFileObj) ) 
                        needItems.append( (pathId, oldFileObj) ) 

                    fetchItems.append( (newFileId, newFileVersion, newFileObj) )
                    needItems.append( (pathId, newFileObj) ) 

                    contentsNeeded += fetchItems
                    fileJob += (needItems,)

            contentList = self.getFileContents(contentsNeeded, 
                                               tmpFile = outFile,
                                               lookInLocal = True)

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
        for (name, version, flavor) in primaryTroveList:
            trove = cs.getNewPackageVersion(name, version, flavor)
            cs.addPrimaryTrove(name, trove.getNewVersion(), flavor)

        if target and cs:
            if urlsFetched > 1 or internalCs:
                os.unlink(target)
                cs.writeToFile(target)

            cs = None

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

    def getFileVersions(self, fullList, lookInLocal = False):
        if self.localRep and lookInLocal:
            result = [ x for x in self.localRep.getFileVersions(fullList) ]
        else:
            result = [ None ] * len(fullList)

        byServer = {}
        for i, (pathId, fileId, version) in enumerate(fullList):
            if result[i] is not None:
                continue

            server = version.branch().label().getHost()
            if not byServer.has_key(server):
                byServer[server] = []
            byServer[server].append((i, (self.fromPathId(pathId), 
                                     self.fromFileId(fileId))))
        
        for (server, l) in byServer.iteritems():
            sendL = [ x[1] for x in l ]
            idxL = [ x[0] for x in l ]
            fileStreams = self.c[server].getFileVersions(sendL)
            for (fileStream, idx) in zip(fileStreams, idxL):
                result[idx] = self.toFile(fileStream)

        return result

    def getFileVersion(self, pathId, fileId, version):
        return self.toFile(self.c[version].getFileVersion(
				   self.fromPathId(pathId), 
				   self.fromFileId(fileId)))

    def getFileContents(self, fileList, tmpFile = None, lookInLocal = False):
        contents = [ None ] * len(fileList)

        if self.localRep and lookInLocal:
            for i, item in enumerate(fileList):
                if len(item) < 3: continue

                sha1 = item[2].contents.sha1()
                if self.localRep._hasFileContents(sha1):
                    contents[i] = self.localRep.getFileContents([item])[0]

        byServer = {}

        for i, item in enumerate(fileList):
            if contents[i] is not None:
                continue

            # we try to get the file from the trove which originally contained
            # it since we know that server has the contents; other servers may
            # not
            (fileId, fileVersion) = item[0:2]
            server = fileVersion.branch().label().getHost()
            l = byServer.setdefault(server, [])
            l.append((i, (fileId, fileVersion)))

        for server, itemList in byServer.iteritems():
            fileList = [ (self.fromFileId(x[1][0]), 
                          self.fromVersion(x[1][1])) for x in itemList ]
            (url, sizes) = self.c[server].getFileContents(fileList)
            assert(len(sizes) == len(fileList))

            inF = urllib.urlopen(url)

            if tmpFile:
		# make sure we append to the end (creating the gzip file
		# object does a certain amount of seeking through the
		# nested file object which we need to undo
		tmpFile.seek(0, 2)
                start = tmpFile.tell()
                outF = tmpFile
            else:
                (fd, path) = tempfile.mkstemp()
                os.unlink(path)
                outF = os.fdopen(fd, "r+")
                start = 0

            totalSize = util.copyfileobj(inF, outF)
            del inF

            for (i, item), size in itertools.izip(itemList, sizes):
                nestedF = util.SeekableNestedFile(outF, size, start)

                totalSize -= size
                start += size

                gzfile = gzip.GzipFile(fileobj = nestedF)
                gzfile.fullSize = util.gzipFileSize(nestedF)

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
        return dict((self.toPath(x[0]), self.toPathId(x[1]))
                    for x in ids.iteritems())

    def commitChangeSetFile(self, fName):
        cs = changeset.ChangeSetFromFile(fName)
        return self._commit(cs, fName)

    def commitChangeSet(self, chgSet):
	(outFd, path) = tempfile.mkstemp()
	os.close(outFd)
	chgSet.writeToFile(path)

	try:
            result = self._commit(chgSet, path)
        finally:
            os.unlink(path)

        return result

    def findTroves(self, labelPath, troves, defaultFlavor = None, 
                  acrossRepositories = False, 
                  affinityDatabase = None, allowMissing=False):
        results = {}
        remaining = []
        affinityTroveMap = {}
        byBranch = {}
        byBranchNoFlavor = {}
        byBranchMap = {}
        byVersion = {}
        byVersionNoFlavor = {}
        byVersionMap = {}
        byLabel = {}
        byLabelMap = {}
        verRelLabel = {}
        verRelLabelMap = {}
        verRelBranch = {}
        verRelBranchMap = {}

        if labelPath and not type(labelPath) == list:
            labelPath = [ labelPath ]

        # two step process:
        # 1. iterate through all trove specs, sorting them by request type
        # 2. for each request type, find the resulting troves, and 
        #    store an entry in finalMap from the trove spec to the result.
        # Note: because we want to be able to keep pointers from the 
        # requested trove spec to its resulting troves, we cannot handle 
        # requests of the same type for the same trove name gracefully.
        # Instead, we collect all such duplicate troves in the remaining
        # list, and call findTroves again with such values.  Thus, findTroves
        # offers absolutely 0 savings for cases where you request the same
        # trove twice.

        # potential time savings:
        # * replace affinityDatabase.findTrove
        # * others? 

        # duplicate code:
        # there's still duplicate code hanging around here, with minute 
        # differences dividing the handling of affinity troves in different
        # places.  It's possible that some of this duplicate code could be
        # removed.

        # affinityTroves/flavor:
        # it's not clear that affinityTroves are used the same way everywhere
        # when you are grabbing by label, affinityTrove flavors are only 
        # considered relevant if they match name and label.  But if you 
        # grab by branch, only name is important.

        ######### Step 1 - sorting by request type
        for tup in troves:

            name, versionStr, flavor = tup 
            if not labelPath:
                # if we don't have a label path, we need a fully qualified
                # version string; make sure have it
                if versionStr[0] != "/" and (versionStr.find("/") != -1 or
                                             versionStr.find("@") == -1):
                    raise repository.TroveNotFound, \
                        "fully qualified version or label " + \
                        "expected instead of %s" % versionStr

            if affinityDatabase:
                try:
                    affinityTroves = affinityDatabase.findTrove(None, name)
                except repository.TroveNotFound:
                    affinityTroves = []
            else:
                affinityTroves = []
            # set up flavor for all cases except when 
            # 1. there is no flavor and 2. there are affinity troves
            if flavor is not None:
                f = flavor
            elif not affinityTroves:
                f = defaultFlavor
            else:
                f = None

            if not versionStr:
                # 1.1 - no version specified
                # if we've got affinity troves, search on the branches
                # of the troves.
                # otherwise, search on  the installLabels
                if affinityTroves:
                    if name in byBranch:
                        remaining.append(tup)
                        continue
                    byBranch[name] = {}
                    byBranchMap[name] = tup
                    for dummy, afVersion, afFlavor in affinityTroves:
                        if not flavor:
                            f = deps.overrideFlavor(defaultFlavor, afFlavor, 
                                       mergeType = deps.DEP_MERGE_TYPE_PREFS)
                        branch = afVersion.branch()
                        byBranch[name].setdefault(afVersion.branch(),
                                                                  []).append(f)
                else:
                    if name in byLabel:
                        remaining.append(tup)
                        continue
                    byLabelMap[name] = tup
                    if acrossRepositories:
                        byLabel[name] = [dict.fromkeys(labelPath, [f])]
                    else:
                        byLabel[name] = []
                        for label in labelPath:
                            byLabel[name].append({label :  [f]})
            elif versionStr[0] == '/':
                # 1.2 - version or branch specified
                # in both cases, we need to split the troves depending on 
                # whether there is any flavor associated with the trove.
                # If the flavor is None, the bestFlavor option to 
                # getTroveVersionFlavors or getTroveLeavesByBranch must 
                # not be passed, on pain of raising an assertion on the server
                # so we split into four cases.
                # flavor might be modified by affinity troves.
                try:
                    version = versions.VersionFromString(versionStr)
                except versions.ParseError, e:
                    raise repository.TroveNotFound, str(e)
                if isinstance(version, versions.Branch):
                    d = byBranch
                    dNoFlavor = byBranchNoFlavor
                    mapD = byBranchMap
                else:
                    d = byVersion
                    dNoFlavor = byVersionNoFlavor
                    mapD = byVersionMap

                if flavor is None:
                    if affinityTroves:
                        flavors = [x[2] for x in affinityTroves]
                        f = flavors[0]
                        for otherFlavor in flavors:
                            if otherFlavor != f:
                                f = defaultFlavor
                                break
                        f = deps.overrideFlavor(defaultFlavor, f, 
                                    mergeType = deps.DEP_MERGE_TYPE_PREFS)
                    if f is None:
                        dNoFlavor[name] = { version : [ None ] }
                        mapD[name] = tup
                        continue
                if name in d:
                    remaining.append(tup)
                    continue
                d[name] = { version : [f]}
                mapD[name] = tup
            elif versionStr.find('/') != -1:
                # if we've got a version string, and it doesn't start with a
                # /, no / is allowed
                raise repository.TroveNotFound, \
                        "incomplete version string %s not allowed"
            elif versionStr[0] in ('@', ':') or versionStr.count('@'):
                # 1.3, 1.4, 1.5 - label, tag, or branchName given.
                # all three of these cases are very similar -- they 
                # modify the labelPath in some way, and then search 
                # for the trove along that modified path.  
                # if acrossRepositories is given, all labels will be searched,
                # if not, the results on the first label to return a result 
                # will be used.
                if name in byLabel:
                    remaining.append(tup)
                    continue
                byLabelMap[name] = tup
                # First, modify the label path => new label path
                if versionStr[0] == ":":
                    repositories = [(x.getHost(), x.getNamespace()) \
                                                            for x in labelPath ]
                    newLabelPath = []
                    for serverName, namespace in repositories:
                        newLabelPath.append(versions.Label("%s@%s%s" %
                                           (serverName, namespace, versionStr)))
                elif versionStr[0] == '@':
                    # just a branch name was specified
                    repositories = [ x.getHost() for x in labelPath ]
                    newLabelPath = []
                    for serverName in repositories:
                        newLabelPath.append(versions.Label("%s%s" %
                                                     (serverName, versionStr)))
                else:
                    try:
                        label = versions.Label(versionStr)
                        newLabelPath = [ label ]
                    except versions.ParseError:
                        raise repository.TroveNotFound, \
                                            "invalid version %s" % versionStr

                # set up the query for getTroveLeavesByLabel.
                # this query is a little special.  
                # if acrossRepositories is not given, we do not want to search
                # all of the labels at once, we want to search them one by
                # one until one returns (although it might be the case that 
                # timing shows that it's better to search all at once, even
                # if the searches on some of the labels are wasted)
                # so, in that case, we create a list of queries for 
                # getTroveLeavesByLabel.  If acrossRepositories is given,
                # the list is one element containing all of the queries
                # for all labels.
                if not flavor and affinityTroves:
                    if acrossRepositories:
                        # d is the label : flavor dict for this trove
                        d = {}
                        byLabel[name] = [d]
                    else:
                        # lst is a list of {label : flavor} dicts for this trove
                        lst = []
                        byLabel[name] = lst    

                    for label in newLabelPath:  
                        flavors = []
                        for (afName, afVersion, afFlavor) in affinityTroves:
                            if afVersion.branch().label() == label:
                                flavors.append(afFlavor)
                        if not flavors:
                            f = defaultFlavor
                        else:
                            f = flavors[0]
                            for otherFlavor in flavors:
                                if otherFlavor != f:
                                    f = defaultFlavor
                                    break
                            f = deps.overrideFlavor(defaultFlavor, f, 
                                       mergeType = deps.DEP_MERGE_TYPE_PREFS)
                        if acrossRepositories:
                            # acrossRepositories - 
                            # mesh this query into d
                            d[label] = [f] 
                        else:
                            # not acrossRepositories - 
                            # append this query onto lst
                            lst.append({label :  [f]})
                elif acrossRepositories:
                    # acrossRepositories but no affinityTroves - we know the
                    # value of f beforehand so we can just create one dict
                    byLabel[name] = [dict.fromkeys(newLabelPath, [f])]
                else:
                    lst = []
                    byLabel[name] = lst
                    for label in newLabelPath:
                        lst.append({label :  [f]})

            else:
                # 1.6 - trove version string or trailing revision given
                # this case is most similar to the no version case, 
                # except that instead of desiring the leaf on each label/branch,
                # we wish to filter by the trailingRevision instead.
                # if there are affinity troves, we use their branches as 
                # the desired branches to search, otherwise, search all 
                # the labelPath as in 1.3-1.5
                for char in ' ,':
                    if char in versionStr:
                        raise RuntimeError, \
                            ('%s reqests illegal version/revision %s' 
                                                    % (name, versionStr))
                if affinityTroves:
                    if name in verRelBranch:
                        remaining.append(tup)
                        continue
                    verRelBranch[name] = {}
                    for dummy, afVersion, afFlavor in affinityTroves:
                        # XXX matches no version given code
                        if not flavor:
                            f = deps.overrideFlavor(defaultFlavor, afFlavor, 
                                       mergeType = deps.DEP_MERGE_TYPE_PREFS)
                        branch = afVersion.branch()
                        verRelBranch[name].setdefault(afVersion.branch(), 
                                                                  []).append(f)
                    verRelBranchMap[name] = tup
                elif name in verRelLabel:
                    remaining.append(tup)
                    continue
                elif acrossRepositories:
                    verRelLabel[name] = [dict.fromkeys(labelPath, [f])]
                    verRelLabelMap[name] = tup
                else:
                    verRelLabelMap[name] = tup
                    verRelLabel[name] = []
                    for label in labelPath:
                        verRelLabel[name].append({label :  [f]})

        # Step 2.  Actually send requests.
        missing = []
        finalMap = {}

        if byBranch:
            res = self.getTroveLeavesByBranch(byBranch, bestFlavor=True)
            for name in byBranch:
                if name not in res or not res[name]:
                    missing.append(byBranchMap[name])
                    continue
                pkgList = []
                for version, flavorList in res[name].iteritems():
                    pkgList.extend((name, version, f) for f in flavorList)
                finalMap[byBranchMap[name]] = pkgList
            del byBranch

        if byBranchNoFlavor:
            res = self.getTroveLeavesByBranch(byBranchNoFlavor, 
                                              bestFlavor=False)
            for name in byBranchNoFlavor:
                if name not in res or not res[name]:
                    missing.append(byBranchMap[name])
                    continue
                pkgList = []
                for version, flavorList in res[name].iteritems():
                    pkgList.extend((name, version, f) for f in flavorList)
                finalMap[byBranchMap[name]] = pkgList
            del byBranchNoFlavor
        del byBranchMap
        if byVersion:
            res = self.getTroveVersionFlavors(byVersion, bestFlavor=True)
            for name in byVersion:
                if name not in res or not res[name]:
                    missing.append(byVersionMap[name])
                    continue
                pkgList = []
                for version, flavorList in res[name].iteritems():
                    pkgList.extend((name, version, f) for f in flavorList)
                finalMap[byVersionMap[name]] = pkgList
            del byVersion

        if byVersionNoFlavor:
            res = self.getTroveVersionFlavors(byVersionNoFlavor, 
                                              bestFlavor=False)
            for name in byVersionNoFlavor:
                if name not in res or not res[name]:
                    missing.append(byVersionMap[name])
                    continue
                pkgList = []
                for version, flavorList in res[name].iteritems():
                    pkgList.extend((name, version, f) for f in flavorList)
                finalMap[byVersionMap[name]] = pkgList
            del byVersionNoFlavor
        del byVersionMap

        if verRelBranch:
            res = self.getTroveVersionsByBranch(verRelBranch, bestFlavor=True)
            for name in verRelBranch:
                versionStr = verRelBranchMap[name][1]
                try:
                    verRel = versions.Revision(versionStr)
                except versions.ParseError, e:
                    verRel = None
                found = False
                for version in reversed(sorted(res[name].iterkeys())):
                    if verRel:
                        if version.trailingRevision() != verRel:
                            continue
                    else:
                        if version.trailingRevision().version != versionStr:
                            continue
                    found = True
                    pkgList = [(name, version, x) \
                                    for x in res[name][version]]
                    finalMap[verRelBranchMap[name]] = pkgList
                    break
                if not found:
                    missing.append(verRelBranchMap[name])
            del verRelBranch
        if byLabel:
            index = 0
            while byLabel:
                query = {}
                for name in byLabel.keys():
                    try:
                        req = byLabel[name][index]
                    except IndexError:
                        missing.append(byLabelMap[name])
                        del byLabel[name]
                        continue
                    else:
                        query[name] = req

                # map [ None ] flavor to None
                for verSet in query.itervalues():
                    for version, flavorList in verSet.items():
                        if flavorList == [ None ]:
                            verSet[version] = None
                        else:
                            assert(None not in flavorList)

                res = self.getTroveLeavesByLabel(query, bestFlavor=True)

                for name in res:
                    if not res[name]:
                        continue
                    del byLabel[name]
                    pkgList = []
                    for version, flavorList in res[name].iteritems():
                        pkgList.extend((name, version, f) for f in flavorList)
                    finalMap[byLabelMap[name]] = pkgList
                index +=1
            del byLabel
            del byLabelMap
            del query
        if verRelLabel:
            index = 0
            while verRelLabel:
                query = {}
                for name in verRelLabel.keys():
                    try:
                        req = verRelLabel[name][index]
                    except IndexError:
                        missing.append(verRelLabelMap[name])
                        del verRelLabel[name]
                        continue
                    query[name] = req

                # map [ None ] flavor to None
                for verSet in query.itervalues():
                    for version, flavorList in verSet.items():
                        if flavorList == [ None ]:
                            verSet[version] = None
                        else:
                            assert(None not in flavorList)

                res = self.getTroveVersionsByLabel(query, bestFlavor=True)

                for name in res:
                    if not res[name]:
                        continue
                    versionStr = verRelLabelMap[name][1]
                    try:
                        verRel = versions.Revision(versionStr)
                    except versions.ParseError, e:
                        verRel = None
                    if acrossRepositories:
                        vByLabel = {}
                        for version in res[name]:
                            vByLabel.setdefault(version.branch().label(),
                                                            []).append(version)
                        pkgList  = []
                        for label in vByLabel:
                            for version in reversed(sorted(vByLabel[label])):
                                if verRel:
                                    if version.trailingRevision() != verRel:
                                        continue
                                else:
                                    if version.trailingRevision().version \
                                                                != versionStr:
                                        continue
                                pkgList.extend((name, version, x) \
                                                for x in res[name][version])
                                break
                        if not pkgList:
                            missing.append(verRelLabelMap[name])
                        else:
                            finalMap[verRelLabelMap[name]] = pkgList
                            del verRelLabel[name]
                    else:
                        for version in reversed(sorted(res[name].iterkeys())):
                            if verRel:
                                if version.trailingRevision() != verRel:
                                    continue
                            else:
                                if version.trailingRevision().version \
                                                                != versionStr:
                                    continue
                            del verRelLabel[name]
                            pkgList = [(name, version, x) \
                                            for x in res[name][version]]
                            finalMap[verRelLabelMap[name]] = pkgList
                            break
                index += 1

            del verRelLabel
            del verRelLabelMap
            del query
        if missing and not allowMissing:
            import updatecmd
            raise repository.TroveNotFound, 'Troves %s not found' % \
                                   [updatecmd.toTroveSpec(*x) for x in missing]

        if remaining: 
            findTroveMap = self.findTroves(labelPath, remaining, defaultFlavor, 
                                           acrossRepositories,
                                           affinityDatabase, 
                                           allowMissing)
            finalMap.update(findTroveMap)
        return finalMap


    def findTrove(self, labelPath, (name, versionStr, flavor), 
                  defaultFlavor=None, acrossRepositories = False, 
                  affinityDatabase = None):
        if labelPath and not isinstance(labelPath, list):
            labelPath = [labelPath]
        try:
            res = self.findTroves(labelPath, ((name, versionStr, flavor),),
                                  defaultFlavor, acrossRepositories, 
                                  affinityDatabase)
        except repository.TroveNotFound, msg:
            if not versionStr:
                raise repository.TroveNotFound,\
                    ("%s was not on found on path %s" \
                     % (name, ', '.join(x.asString() for x in labelPath)))
            elif labelPath:
                raise repository.TroveNotFound,\
                    ("version %s of %s was not on found on path %s" \
                     % (versionStr, name, 
                        ', '.join(x.asString() for x in labelPath)))
            else:
                raise repository.TroveNotFound,\
                    ("version %s of %s was not on found" % (versionStr, name))
                        

                


        return res[(name, versionStr, flavor)]
	    
    def _commit(self, chgSet, fName):
	serverName = None
	for pkg in chgSet.iterNewPackageList():
	    v = pkg.getOldVersion()
	    if v:
		if serverName is None:
		    serverName = v.branch().label().getHost()
		assert(serverName == v.branch().label().getHost())

	    v = pkg.getNewVersion()
	    if serverName is None:
		serverName = v.branch().label().getHost()
	    assert(serverName == v.branch().label().getHost())
	    
	url = self.c[serverName].prepareChangeSet()

        self._putFile(url, fName)

	self.c[serverName].commitChangeSet(url)

    def _putFile(self, url, path):
        protocol, uri = urllib.splittype(url)
        assert(protocol in ('http', 'https'))
	(host, putPath) = url.split("/", 3)[2:4]
        if protocol == 'http':
            c = httplib.HTTPConnection(host)
        else:
            c = httplib.HTTPSConnection(host)
	f = open(path)
	c.connect()
	c.request("PUT", url, f.read())
	r = c.getresponse()
	assert(r.status == 200)

    def __init__(self, repMap, localRepository = None):
        # the local repository is used as a quick place to check for
        # troves _getChangeSet needs when it's building changesets which
        # span repositories. it has no effect on any other operation.
	self.c = ServerCache(repMap)
        self.localRep = localRepository

class UnknownException(repository.RepositoryError):

    def __str__(self):
	return "UnknownException: %s %s" % (self.eName, self.eArgs)

    def __init__(self, eName, eArgs):
	self.eName = eName
	self.eArgs = eArgs

class UserAlreadyExists(Exception):
    pass

class InvalidServerVersion(Exception):
    pass
