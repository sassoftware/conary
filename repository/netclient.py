#
# Copyright (c) 2004 Specifix, Inc.
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

import filecontainer
import filecontents
import gzip
import httplib
import log
import os
import package
import repository
import socket
import tempfile
import transport
import urllib
import util
import versions
import xmlrpclib
import xmlshims
from deps import deps

shims = xmlshims.NetworkConvertors()

class _Method(xmlrpclib._Method):

    def __call__(self, *args):
        isException, result = self.__send(self.__name, args)
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
	else:
	    raise UnknownException(exceptionName, exceptionArgs)

class ServerProxy(xmlrpclib.ServerProxy):

    def __getattr__(self, name):
        return _Method(self.__request, name)

class ServerCache:

    def __getitem__(self, item):
	if isinstance(item, versions.BranchName):
	    serverName = item.getHost()
	elif isinstance(item, str):
	    serverName = item
	else:
	    if item.isBranch():
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
		if server.checkVersion(1) < 1:
		    raise repository.OpenError('Server version too old')
	    except OSError, e:
		raise repository.OpenError('Error occured opening repository '
			    '%s: %s' % (url, e.strerror))
	    except socket.error, e:
		raise repository.OpenError('Error occured opening repository '
			    '%s: %s' % (url, e[1]))
	    except Exception, e:
		raise repository.OpenError('Error occured opening repository '
			    '%s: %s' % (url, str(e)))

	return server
		
    def __init__(self, repMap):
	self.cache = {}
	self.map = repMap

class NetworkRepositoryClient(xmlshims.NetworkConvertors,
			      repository.AbstractRepository):

    def close(self, *args):
        pass

    def createBranch(self, newBranch, where, troveList):
	if isinstance(where, versions.Version):
	    kind = 'v'
	    frz = self.fromVersion(where)
	    if where.isBranch():
		label = where.label()
	    else:
		label = where.branch().label()
	else:
	    kind = 'l'
	    frz = self.fromLabel(where)
	    label = where

	newBranchStr = self.fromLabel(newBranch)

	self.c[newBranch].createBranch(newBranchStr, kind, frz, troveList)

    def open(self, *args):
        pass

    def hasPackage(self, serverName, pkg):
        return self.c[serverName].hasPackage(pkg)

    def iterAllTroveNames(self, serverName):
	for name in self.c[serverName].allTroveNames():
	    yield name

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
        gen = self.c[version].getFilesInTrove(troveName,
                                     self.fromVersion(version),
                                     self.fromFlavor(flavor),
                                     sortByPath,
                                     withFiles)
        if withFiles:
            for (fileId, path, version, f) in gen:
                yield (fileId, path, self.toVersion(version), self.toFile(f))
        else:
            for (fileId, path, version) in gen:
                yield (fileId, path, self.toVersion(version))

    def getAllTroveLeafs(self, serverName, troveNames):
	d = self.c[serverName].getAllTroveLeafs(troveNames)
	for troveName, troveVersions in d.iteritems():
	    d[troveName] = [ self.toVersion(x) for x in troveVersions ]

	return d

    def getTroveFlavorsLatestVersion(self, troveName, branch):
	return [ (versions.VersionFromString(x[0], 
			timeStamps = [ float(z) for z in x[1].split(":")]),
		  self.toFlavor(x[2])) for x in 
                 self.c[branch].getTroveFlavorsLatestVersion(troveName, 
                                                     branch.asString()) ]

    def getTroveVersionList(self, serverName, troveNameList):
	d = self.c[serverName].getTroveVersionList(troveNameList)
	for troveName, troveVersions in d.iteritems():
	    d[troveName] = [ self.thawVersion(x) for x in troveVersions ]

	return d

    def getTroveLeavesByLabel(self, troveNameList, label):
	d = self.c[label].getTroveLeavesByLabel(troveNameList, 
						label.asString())
	for troveName, troveVersions in d.iteritems():
	    d[troveName] = [ self.thawVersion(x) for x in troveVersions ]

	return d
	
    def getTroveVersionsByLabel(self, troveNameList, label):
	d = self.c[label].getTroveVersionsByLabel(troveNameList, 
						  label.asString())
	for troveName, troveVersions in d.iteritems():
	    d[troveName] = [ self.thawVersion(x) for x in troveVersions ]

	return d
	
    def getTroveVersionFlavors(self, troveDict):
	passD = {}
	versionDict = {}

	serverName = None

	for (troveName, versionList) in troveDict.iteritems():
	    passD[troveName] = []
	    for version in versionList:
		s = version.branch().label().getHost()
		if serverName is None:
		    serverName = s

		# XXX 
		assert(serverName == s)

		versionStr = self.fromVersion(version)
		versionDict[versionStr] = version
		passD[troveName].append(versionStr)

	if not serverName:
	    newD = {}
	    for troveName in passD:
		newD[troveName] = {}

	    return newD

	result = self.c[serverName].getTroveVersionFlavors(passD)

	newD = {}
	for troveName, troveVersions in result.iteritems():
	    newD[troveName] = {}
	    for versionStr, flavors in troveVersions.iteritems():
		version = versionDict[versionStr]
		newD[troveName][version] = [ self.toFlavor(x) for x in flavors ]

	return newD

    def getTroveLatestVersion(self, troveName, branch):
	b = self.fromBranch(branch)
	v = self.c[branch].getTroveLatestVersion(troveName, b)
        if v == 0:
            raise repository.TroveMissing(troveName, branch)
	return self.thawVersion(v)

    def getTrove(self, troveName, troveVersion, troveFlavor):
	rc = self.getTroves([(troveName, troveVersion, troveFlavor)])
	if rc[0] is None:
	    raise repository.TroveMissing(troveName, version = troveVersion)

	return rc[0]

    def getTroves(self, troves):
	chgSetList = []
	for (name, version, flavor) in troves:
	    chgSetList.append((name, flavor, None, version, True))
	
	cs = self._getChangeSet(chgSetList, recurse = False, withFiles = False)

	l = []
        # walk the list so we can return the troves in the same order
        for (name, version, flavor) in troves:
            try:
                pkgCs = cs.getNewPackageVersion(name, version, flavor)
            except KeyError:
                l.append(None)
                continue
            
            t = package.Trove(pkgCs.getName(), pkgCs.getOldVersion(),
                              pkgCs.getFlavor(), pkgCs.getChangeLog())
            t.applyChangeSet(pkgCs)
            l.append(t)

	return l

    def createChangeSet(self, list):
	return self._getChangeSet(list)

    def _getChangeSet(self, chgSetList, recurse = True, withFiles = True):
	l = []
	serverName = None
	for (name, flavor, old, new, absolute) in chgSetList:
	    if old:
		l.append((name, self.fromFlavor(flavor),
			  self.fromVersion(old), self.fromVersion(new), 
			  absolute))
		if serverName is None:
		    serverName = old.branch().label().getHost()
		assert(serverName == old.branch().label().getHost())
	    else:
		l.append((name, self.fromFlavor(flavor),
			  0, self.fromVersion(new),
			  absolute))

	    if serverName is None:
		serverName = new.branch().label().getHost()
	    assert(serverName == new.branch().label().getHost())

	url = self.c[serverName].getChangeSet(l, recurse, withFiles)

	# XXX we shouldn't need to copy this locally most of the time
	inF = urllib.urlopen(url)
	(outFd, name) = tempfile.mkstemp()
	outF = os.fdopen(outFd, "w")
	try:
	    util.copyfileobj(inF, outF)
            outF.close()
	    cs = repository.changeset.ChangeSetFromFile(name)
	finally:
	    inF.close()
	    os.unlink(name)

	return cs

    def getFileVersion(self, fileId, version):
        return self.toFile(self.c[version].getFileVersion(fileId,
                                                 self.fromVersion(version)))

    def getFileContents(self, troveName, troveVersion, troveFlavor, path,
		        fileVersion, fileObj = None):
	# we try to get the file from the trove which originally contained
	# it since we know that server has the contents; other servers may
	# not
	url = self.c[fileVersion].getFileContents(troveName, 
		    self.fromVersion(fileVersion), 
		    self.fromFlavor(troveFlavor),
		    path, self.fromVersion(fileVersion))

	inF = urllib.urlopen(url)
	(fd, path) = tempfile.mkstemp()
	os.unlink(path)
	outF = os.fdopen(fd, "r+")
	util.copyfileobj(inF, outF)
	del inF

	outF.seek(0)

	gzfile = gzip.GzipFile(fileobj = outF)
	gzfile.fullSize = util.gzipFileSize(outF)

	return filecontents.FromGzFile(gzfile)

    def commitChangeSet(self, chgSet):
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
	    
	(outFd, path) = tempfile.mkstemp()
	os.close(outFd)
	chgSet.writeToFile(path)

	url = self.c[serverName].prepareChangeSet()

	try:
	    self._putFile(url, path)
	finally:
	    os.unlink(path)

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

    def __init__(self, repMap):
	self.c = ServerCache(repMap)

class UnknownException(repository.RepositoryError):

    def __str__(self):
	return "UnknownException: %s %s" % (self.eName, self.eArgs)

    def __init__(self, eName, eArgs):
	self.eName = eName
	self.eArgs = eArgs
