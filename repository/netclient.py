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

CLIENT_VERSION=24

class _Method(xmlrpclib._Method):

    def __repr__(self):
        return "<netclient._Method(%s, %r)>" % (self._Method__send, self._Method__name) 

    def __str__(self):
        return self.__repr__()

    def __call__(self, *args):
        newArgs = ( CLIENT_VERSION, ) + args
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
	    server = ServerProxy(url, transport.Transport())
	    self.cache[serverName] = server

	    try:
                server.checkVersion()
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
                    url = 'http://<user>:<pwd>@' + url.split('@')[1]
		raise repository.OpenError('Error occured opening repository '
			    '%s: %s' % (url, errmsg))
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

    def getTroveLeavesByLabel(self, troveNameList, label, flavorFilter = None):
	d = self.c[label].getTroveLeavesByLabel(troveNameList, 
						label.asString(),
                                                self.fromFlavor(flavorFilter))
        return self._mergeTroveQuery({}, d)
	
    def getTroveVersionsByLabel(self, troveNameList, label, 
                                flavorFilter = None):
	d = self.c[label].getTroveVersionsByLabel(troveNameList, 
						  label.asString(),
                                                  self.fromFlavor(flavorFilter))
        return self._mergeTroveQuery({}, d)
	
    def getTroveVersionFlavors(self, troveDict, bestFlavor = False):

        def _cvtFlavor(flavor):
            if flavor is None:
                return 0
            else:
                return self.fromFlavor(flavor)

	requestD = {}

	for (troveName, subVersionDict) in troveDict.iteritems():
	    for version, flavorList in subVersionDict.iteritems():
		serverName = version.branch().label().getHost()

                if not requestD.has_key(serverName):
                    requestD[serverName] = {}
                if not requestD[serverName].has_key(troveName):
                    requestD[serverName][troveName] = {}

		versionStr = self.fromVersion(version)

                requestD[serverName][troveName][versionStr] = \
                    [ _cvtFlavor(x) for x in flavorList ]

        newD = {}
	if not requestD:
	    return newD

        for serverName, passD in requestD.iteritems():
            result = self.c[serverName].getTroveVersionFlavors(passD, 
                                                               bestFlavor)
            self._mergeTroveQuery(newD, result)

	return newD

    def getAllTroveFlavors(self, troveDict):
        d = {}
        for name, versionList in troveDict.iteritems():
            d[name] = {}.fromkeys(versionList, [ None ])

	return self.getTroveVersionFlavors(d)

    def _getTroveInfoByBranch(self, troveSpecs, bestFlavor, method):
        d = {}
        for name, branches in troveSpecs.iteritems():
            for branch, flavors in branches.iteritems():
                host = branch.label().getHost()
                if not d.has_key(host):
                    d[host] = {}

                subD = d[host].get(name, None)
                if subD is None:
                    subD = {}
                    d[host][name] = subD

                if flavors is None:
                    subD[branch.asString()] = ''
                else:
                    subD[branch.asString()] = \
                                    [ self.fromFlavor(x) for x in flavors ]

        result = {}

        for host, requestD in d.iteritems():
            respD = self.c[host].__getattr__(method)(requestD, bestFlavor)
            self._mergeTroveQuery(result, respD)

        return result

    def getTroveLeavesByBranch(self, troveSpecs, bestFlavor = False):
        return self._getTroveInfoByBranch(troveSpecs, bestFlavor, 
                                          'getTroveLeavesByBranch')

    def getTroveVersionsByBranch(self, troveSpecs, bestFlavor = False):
        return self._getTroveInfoByBranch(troveSpecs, bestFlavor, 
                                          'getTroveVersionsByBranch')

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

    def createChangeSet(self, list, withFiles = True, withFileContents = True):
	return self._getChangeSet(list, withFiles = withFiles, 
                                  withFileContents = withFileContents)

    def createChangeSetFile(self, list, fName):
	self._getChangeSet(list, target = fName)

    def _getChangeSet(self, chgSetList, recurse = True, withFiles = True,
		      withFileContents = True, target = None):
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
                return troveList

            return self.localRep.getTroves(troveList)

        if not chgSetList:
            # no need to work hard to find this out
            return changeset.ReadOnlyChangeSet()

        cs = None
        scheduledSet = {}
        internalCs = None
        passCount = 0
        filesNeeded = []

        if target:
            outFile = open(target, "w+")
        else:
            (outFd, tmpName) = tempfile.mkstemp()
            outFile = os.fdopen(outFd, "w+")
            os.unlink(tmpName)
            

        # it might a good idea to dedup the job list as we go? the only
        # thing that makes that tricky is the first job, which could be
        # written to a file and not yet read in (and it may never need
        # to be read)

        while chgSetList:
            (serverJobs, ourJobList) = _separateJobList(chgSetList)

            chgSetList = []

            for serverName, job in serverJobs.iteritems():
                (urlList, extraTroveList, extraFileList) = \
                    self.c[serverName].getChangeSet(job, recurse, 
                                                withFiles, withFileContents)

                chgSetList += _cvtTroveList(extraTroveList)
                filesNeeded += _cvtFileList(extraFileList)

                for url in urlList:
                    inF = urllib.urlopen(url)

                    try:
                        # seek to the end of the file
                        outFile.seek(0, 2)
                        start = outFile.tell()
                        size = util.copyfileobj(inF, outFile)
                        f = util.SeekableNestedFile(outFile, size, start)

                        inF.close()

                        newCs = changeset.ChangeSetFromFile(f)

                        if not cs:
                            cs = newCs
                        else:
                            cs.merge(newCs)
                    except:
                        if target and os.path.exists(target):
                            os.unlink(target)
                        elif os.path.exists(tmpName):
                            os.unlink(tmpName)
                        raise

            if (ourJobList or filesNeeded) and not internalCs:
                internalCs = changeset.ChangeSet()

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

                if passCount == 0:
                    internalCs.addPrimaryPackage(troveName, newVersion, 
                                                 newFlavor)
            passCount += 1

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

        if target and cs:
            if passCount > 1 or internalCs:
                os.unlink(target)
                cs.writeToFile(target)

            cs = None

	return cs

    def resolveDependencies(self, label, depList):
        l = [ self.fromDepSet(x) for x in depList ]
        d = self.c[label].getDepSuggestions(self.fromLabel(label), l)
        r = {}
        for (key, val) in d.iteritems():
            r[self.toDepSet(key)] = val

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

        for i, item in enumerate(fileList):
            if contents[i] is not None:
                continue

            (fileId, fileVersion) = item[0:2]

            # we try to get the file from the trove which originally contained
            # it since we know that server has the contents; other servers may
            # not
            url = self.c[fileVersion].getFileContents(self.fromFileId(fileId),
                                              self.fromVersion(fileVersion))

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

            size = util.copyfileobj(inF, outF)
            del inF

            if tmpFile:
                outF = util.SeekableNestedFile(tmpFile, size, start)
	    else:
		outF.seek(0)

            gzfile = gzip.GzipFile(fileobj = outF)
            gzfile.fullSize = util.gzipFileSize(outF)

            contents[i] = filecontents.FromGzFile(gzfile)

        return contents

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

    def nextVersion(self, troveName, versionStr, troveFlavors, currentBranch,
                    binary = True, sourceVersion = None, alwaysBumpCount=False):
        """
        Calculates the version to use for a newly built trove which is about
        to be added to the repository.

        @param troveName: name of the trove being built
        @type troveName: str
        @param versionStr: version string from the recipe
        @type versionStr: string
        @param troveFlavor: flavor of the trove being built
        @type troveFlavor: deps.deps.DependencySet
        @param currentBranch: branch the new version should be on
        @type currentBranch: versions.Version
        @param binary: true if this version should use the binary build field
        @type binary: boolean
        @param sourceName: the name of the :source component related to this
                           trove.  The default is troveName + ':source'
        @type sourceName: string
        @param alwaysBumpCount: if True, then do not return a version that 
        matches an existing trove, even if their flavors would differentiate 
        them, instead, increase the appropriate count.  
        @type alwaysBumpCount: bool
        """

        currentVersions = []

        d = self.getTroveVersionsByBranch(
                        { troveName : { currentBranch : None } })
        if d.has_key(troveName):
            # this mimics the result of an old call which was sorted
            # earliest to latest, and provides currentVersion as
            # a set of (version, flavor) tuples
            for ver in sorted(d[troveName].keys(), versions.Version.compare):
                currentVersions += [ (ver, x) for x in d[troveName][ver] ]

        del d

        assert(troveFlavors is not None)
        if not isinstance(troveFlavors, (list, tuple)):
            troveFlavors = (troveFlavors,)

        # find the latest version of this trove and the latest version of
        # this flavor of this trove
        latestForFlavor = None
        latest = None
        # this works because currentVersions is sorted earliest to latest
        for (version, flavor) in currentVersions:
            if flavor in troveFlavors:
                if not latestForFlavor or version.isAfter(latestForFlavor):
                    latestForFlavor = version
            latest = version

        # if we have a sourceVersion, and its release is newer than the latest
        # binary on the branch, use it instead.
        if sourceVersion is not None:
            sourceTrailing = sourceVersion.trailingVersion()
            # if the upstream version part of the source component is the same
            # as what we're currently using, we can use the source version
            if versionStr == sourceTrailing.getVersion():
                # if there isn't a latest, we can just use the source version
                # number after incrementing the build count
                if latest is None:
                    latest = sourceVersion.getBinaryBranch()
                    latest.incrementBuildCount()
                    return latest

                # check to see if the latest binary trove is for a different
                # version, or if the versions are the same but the 
                # source component release is newer.
                # If so, use the source component.  Otherwise, latest will
                # be used below and the build count will be incremented.
                latestTrailing = latest.trailingVersion()
                if (latestTrailing.getVersion() != versionStr or 
                    latestTrailing.getRelease() < sourceTrailing.getRelease()):
                    latest = sourceVersion.getBinaryBranch()
                    latest.incrementBuildCount()
                    return latest

        if latest is None or latest.trailingVersion().getVersion() != versionStr:
            # this is a good guess, but it could be wrong since the same version
            # can appear at discountinuous points in the tree. it would be
            # better if this search was done on the server (it could be much
            # more efficient), but this works for now
            allVersions = self.getTroveVersionsByBranch(
                        { troveName : { currentBranch : None } })

            lastOnBranch = None
            for version in allVersions.get(troveName, []):
                if (version.trailingVersion().getVersion() == 
                            versionStr) and \
                    (not lastOnBranch or version.isAfter(lastOnBranch)):
                    lastOnBranch = version

            if lastOnBranch:
                newVersion = lastOnBranch.copy()
            else:
                # new package or package uses new upstream version. we
                # use -0 here and let the increment method figure out where
                # the 1 goes (this gets things right for shadows)
                if binary:
                    newVersion = currentBranch.createVersion(
                                versions.VersionRelease("%s-1-0" % versionStr))
                else:
                    newVersion = currentBranch.createVersion(
                                versions.VersionRelease("%s-0" % versionStr))

            if binary:
                newVersion.incrementBuildCount()
            else:
                newVersion.incrementRelease()

        elif (latestForFlavor != latest) and not alwaysBumpCount:
            # this is a flavor that does not exist at the latest
            # version on the branch.  Reuse the latest version to sync up.
            newVersion = latest
        else:
            # This is new build of an existing version with the same flavor,
            # increment the build count or release accordingly
            newVersion = latest.copy()
            if binary:
                newVersion.incrementBuildCount()
            else:
                newVersion.incrementRelease()
            
        return newVersion

    def findTrove(self, labelPath, name, defaultFlavor, versionStr = None,
                  acrossRepositories = False, withFiles = True,
                  affinityDatabase = None, flavor = None):
	assert(not defaultFlavor or 
	       isinstance(defaultFlavor, deps.DependencySet))

        if not type(labelPath) == list:
            labelPath = [ labelPath ]

	if not labelPath:
	    # if we don't have a label path, we need a fully qualified
	    # version string; make sure have it
	    if versionStr[0] != "/" and (versionStr.find("/") != -1 or
					 versionStr.find("@") == -1):
		raise repository.TroveNotFound, \
		    "fully qualified version or label " + \
		    "expected instead of %s" % versionStr

        if affinityDatabase and affinityDatabase.hasPackage(name):
            affinityTroves = affinityDatabase.findTrove(name)
        else:
            affinityTroves = []

        if not versionStr:
            query = {}
            if affinityTroves:
                query[name] = {}
                for trove in affinityTroves:
                    # XXX what if multiple troves are on this branch,
                    # but with different flavors?

                    if flavor is not None:
                        f = flavor
                    else:
                        f = defaultFlavor.copy()
                        f.union(trove.getFlavor(), 
                                     mergeType = deps.DEP_MERGE_TYPE_PREFS)

                    branch = trove.getVersion().branch()

                    if not query[name].has_key(branch):
                        query[name][branch] = [ f ]
                    else:
                        query[name][branch].append(f)

            if query:
                flavorDict = self.getTroveLeavesByBranch(query, 
                                                         bestFlavor = True)
            else:
                flavorDict = { name : {} }
                if flavor is None:
                    flavor = defaultFlavor

                for label in labelPath:
                    d = self.getTroveLeavesByLabel([name], label, 
                                             flavorFilter = flavor)

                    if not d.has_key(name):
                        continue
                    elif not acrossRepositories:
                        flavorDict = d
                        break
                    else:
                        self.queryMerge(flavorDict, d)
        elif (versionStr[0] != "/" and (versionStr.find("/") == -1)  \
             and versionStr.count("@")):
            # a version is a label if
            #   1. it doesn't being with / (it isn't fully qualified)
            #   2. it only has one element (no /)
            #   3. it contains an @ sign
	    # either the supplied version is a label, or we need to get the
            # branch from the affinity db, or or we're going to use
	    # the default labelPath
            if versionStr[0] != "@":
		try:
		    label = versions.Label(versionStr)
                    labelPath = [ label ]
		except versions.ParseError:
		    raise TroveMissing, "invalid version %s" % versionStr
            else:
                # just a branch name was specified
                repositories = [ x.getHost() for x in labelPath ]
                labelPath = []
                for serverName in repositories:
                    labelPath.append(versions.Label("%s%s" % 
                                                    (serverName, versionStr)))

            flavorDict = { name : {} }
            for label in labelPath:
                if flavor is not None:
                    finalFlavor = flavor
                else:
                    flavors = []
                    for trove in affinityTroves:
                        if trove.getVersion().branch().label() == label:
                            flavors.append(trove.getFlavor())

                    if not flavors:
                        finalFlavor = defaultFlavor
                    else:
                        # make sure the flavors are the same; otherwise
                        # fall back to the default flavor
                        f = flavors[0]
                        for otherFlavor in flavors:
                            if otherFlavor != f:
                                f = defaultFlavor
                                break

                        finalFlavor = defaultFlavor.copy()
                        finalFlavor.union(f, 
                                      mergeType = deps.DEP_MERGE_TYPE_PREFS)
            
                d = self.getTroveLeavesByLabel([name], label, 
                                               flavorFilter = finalFlavor)
                if not d.get(name, None):
                    continue
                elif not acrossRepositories:
                    flavorDict = d
                    break
                else:
                    self.queryMerge(flavorDict, d)
	elif versionStr[0] != "/" and versionStr.find("/") == -1:
	    # version/release was given. look in the affinityDatabase
            # for the branches to look on
	    try:
		verRel = versions.VersionRelease(versionStr)
	    except versions.ParseError, e:
		raise repository.TroveNotFound, str(e)

            query = {}
            if affinityTroves:
                query[name] = {}
                for trove in affinityTroves:
                    # XXX what if multiple troves are on this label,
                    # but with different flavors?

                    if flavor is not None:
                        f = flavor
                    else:
                        f = defaultFlavor.copy()
                        f.union(trove.getFlavor(), 
                                mergeType = deps.DEP_MERGE_TYPE_PREFS)

                    query[name][trove.getVersion().branch()] = [ f ]

            if query:
                flavorDict = self.getTroveVersionsByBranch(query, 
                                                           bestFlavor = True)

                for version in flavorDict[name].keys():
                    if version.trailingVersion() != verRel:
                        del flavorDict[name][version]
            else:
                flavorDict = { name : {} }
                if flavor is not None:
                    flavor = defaultFlavor

                for label in labelPath:
                    d = self.getTroveVersionsByLabel([name], label, 
                                             flavorFilter = flavor)
                    for version in d.get(name, {}).keys():
                        if version.trailingVersion() != verRel:
                            del d[name][version]

                    if not d.has_key(name):
                        continue
                    elif not acrossRepositories:
                        flavorDict = d
                        break
                    else:
                        self.queryMerge(flavorDict, d)
	elif versionStr[0] != "/":
	    # partial version string, we don't support this
	    raise repository.TroveNotFound, \
		"incomplete version string %s not allowed" % versionStr
	else:
	    try:
		version = versions.VersionFromString(versionStr)
	    except versions.ParseError, e:
		raise repository.TroveNotFound, str(e)

            if isinstance(version, versions.Branch):
                fn = self.getTroveLeavesByBranch
            else:
                fn = self.getTroveVersionFlavors

            if flavor is not None:
                finalFlavor = flavor
            elif affinityTroves:
                flavors = [ x.getFlavor() for x in affinityTroves ]
                f = flavors[0]
                for otherFlavor in flavors:
                    if otherFlavor != f:
                        f = defaultFlavor
                        break

                finalFlavor = defaultFlavor.copy()
                finalFlavor.union(f, mergeType = deps.DEP_MERGE_TYPE_PREFS)
            else:
                finalFlavor = defaultFlavor

            # we're not allowed to ask for the bestFlavor if the 
            # defaultFlavor is None
            bestFlavor = finalFlavor is not None

            flavorDict = fn({ name : { version : [ finalFlavor ] } },
                            bestFlavor = bestFlavor)

            if not flavorDict.has_key(name):
                flavorDict[name] = {}

        if not flavorDict.has_key(name) or not flavorDict[name]:
            if not labelPath or not labelPath[0]:
                raise repository.TroveNotFound, "trove %s not found" % name
            elif versionStr:
                raise repository.TroveNotFound, \
                    "version %s of %s was not on found on path %s" % \
                    (versionStr, name, " ".join([x.asString() for x in 
                        labelPath]))
            else:
                raise repository.TroveNotFound, \
                    "%s was not on found on path %s" % \
                    (name, " ".join([x.asString() for x in labelPath]))

	pkgList = []
	for version, flavorList in flavorDict[name].iteritems():
            pkgList += [ (name, version, f) for f in flavorList ]

	if not pkgList:
	    raise repository.TroveNotFound, "trove %s does not exist" % name

	pkgList = self.getTroves(pkgList, withFiles = withFiles)

	return pkgList

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
	assert(url.startswith("http://"))
	(host, putPath) = url.split("/", 3)[2:4]
	c = httplib.HTTPConnection(host)
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
