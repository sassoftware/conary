#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import filecontainer
import gzip
import httplib
import os
import package
import repository
import socket
import tempfile
import urllib
import util
import versions
import xmlrpclib
import xmlshims
from deps import deps

class NetworkRepositoryClient(xmlshims.NetworkConvertors,
			      repository.AbstractRepository):

    def close(self, *args):
        pass

    def createBranch(self, newBranch, where, troveList):
	if isinstance(where, versions.Version):
	    kind = 'v'
	    frz = self.fromVersion(where)
	else:
	    kind = 'l'
	    frz = self.fromLabel(where)

	self.s.createBranch(self.fromLabel(newBranch), kind, frz, troveList)

    def open(self, *args):
        pass

    def hasPackage(self, pkg):
        return self.s.hasPackage(pkg)

    def hasTrove(self, pkgName, version, flavor):
	return self.s.hasTrove(pkgName, version, flavor)

    def iterAllTroveNames(self):
	for name in self.s.allTroveNames():
	    yield name

    def iterAllTroveNames(self):
	for name in self.s.allTroveNames():
	    yield name

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
        gen = self.s.getFilesInTrove(troveName,
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

    def getAllTroveLeafs(self, troveNames):
	d = self.s.getAllTroveLeafs(troveNames)
	for troveName, troveVersions in d.iteritems():
	    d[troveName] = [ self.toVersion(x) for x in troveVersions ]

	return d

    def getTroveFlavorsLatestVersion(self, troveName, branch):
	return [ (versions.VersionFromString(x[1]),
		  deps.ThawDependencySet(x[0])) for x in 
                 self.s.getTroveFlavorsLatestVersion(troveName, 
                                                     branch.asString()) ]

    def getTroveVersionList(self, troveNameList):
	d = self.s.getTroveVersionList(troveNameList)
	for troveName, troveVersions in d.iteritems():
	    d[troveName] = [ self.toVersion(x) for x in troveVersions ]

	return d

    def getTroveLeavesByLabel(self, troveNameList, label):
	d = self.s.getTroveLeavesByLabel(troveNameList, label.asString())
	for troveName, troveVersions in d.iteritems():
	    d[troveName] = [ self.toVersion(x) for x in troveVersions ]

	return d
	
    def getTroveVersionFlavors(self, troveDict):
	passD = {}
	for (troveName, versionList) in troveDict.iteritems():
	    passD[troveName] = [ self.fromVersion(x) for x in versionList ]

	result = self.s.getTroveVersionFlavors(passD)

	newD = {}
	for troveName, troveVersions in result.iteritems():
	    newD[troveName] = {}
	    for versionStr, flavors in troveVersions.iteritems():
		version = self.toVersion(versionStr)
		newD[troveName][version] = [ self.toFlavor(x) for x in flavors ]

	return newD

    def getTroveLatestVersion(self, troveName, branch):
	b = self.fromBranch(branch)
	v = self.s.getTroveLatestVersion(troveName, b)
        if v == 0:
            raise repository.PackageMissing(troveName, branch)
	return self.toVersion(v)

    def getTrove(self, troveName, troveVersion, troveFlavor):
	rc = self.getTroves([(troveName, troveVersion, troveFlavor)])
	if rc[0] is None:
	    raise repository.PackageMissing(troveName, version = troveVersion)

	return rc[0]

    def getTroves(self, troveNames):
	chgSetList = []
	for (name, version, flavor) in troveNames:
	    chgSetList.append((name, flavor, None, version, True))
	
	cs = self._getChangeSet(chgSetList, recurse = False, withFiles = False)

	l = []
	for pkgCs in cs.iterNewPackageList():
	    p = package.Trove(pkgCs.getName(), pkgCs.getOldVersion(),
			      pkgCs.getFlavor())
	    p.applyChangeSet(pkgCs)
	    l.append(p)

	return l

    def createChangeSet(self, list):
	return self._getChangeSet(list)

    def _getChangeSet(self, chgSetList, recurse = True, withFiles = True):
	l = []
	for (name, flavor, old, new, absolute) in chgSetList:
	    if old:
		l.append((name, self.fromFlavor(flavor),
			  self.fromVersion(old), self.fromVersion(new), 
			  absolute))
	    else:
		l.append((name, self.fromFlavor(flavor),
			  0, self.fromVersion(new),
			  absolute))

	url = self.s.getChangeSet(l, recurse, withFiles)

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

    def getFileVersion(self, fileId, version, withContents = 0):
        # XXX handle withContents
        assert(withContents == 0)
        return self.toFile(self.s.getFileVersion(fileId,
                                                 self.fromVersion(version)))

    def getFileContents(self, sha1List):
	l = sha1List[:]
	l.sort()

	sha1s = []
	for item in sha1List:
	    if type(item) == tuple:
		sha1s.append(item[0])
	    else:
		sha1s.append(item)

	url = self.s.getFileContents(sha1s)

	# XXX we shouldn't need to copy this locally; doing so is silly
	inF = urllib.urlopen(url)
	(fd, path) = tempfile.mkstemp()
	os.unlink(path)
	outF = os.fdopen(fd, "r+")
	util.copyfileobj(inF, outF)
	inF.close()

	outF.seek(0)
	fc = filecontainer.FileContainer(outF)
	del outF

	d = {}
	for item in sha1List:
	    if type(item) == tuple:
		(sha1, path) = item
		f = open(path, "w")
		returnFile = False
	    else:
		sha1 = item
		(fd, path) = tempfile.mkstemp()
		f = os.fdopen(fd, "w+")
		returnFile = True

	    inF = fileobj = fc.getFile(sha1)
	    util.copyfileobj(inF, f)
	    del inF

	    if returnFile:
		f.seek(0)
		d[sha1] = f
	    else:
		del f
		d[sha1] = path

	return d

    def commitChangeSet(self, chgSet):
	(outFd, path) = tempfile.mkstemp()
	os.close(outFd)
	chgSet.writeToFile(path)

	url = self.s.prepareChangeSet()

	try:
	    self._putFile(url, path)
	finally:
	    os.unlink(path)

	self.s.commitChangeSet(url)

    def _putFile(self, url, path):
	assert(url.startswith("http://"))
	(host, putPath) = url.split("/", 3)[2:4]
	c = httplib.HTTPConnection(host)
	f = open(path)
	c.connect()
	c.request("PUT", url, f.read())
	r = c.getresponse()
	assert(r.status == 200)

    def __init__(self, server):
	self.s = xmlrpclib.Server(server)
        try:
            if self.s.checkVersion(0) < 0:
                raise repository.OpenError('Server version too old')
        except OSError, e:
            raise repository.OpenError('Error occured opening the repository: %s' %e.strerror)
        except socket.error, e:
            raise repository.OpenError('Error occured opening the repository: %s' %e[1])
        except Exception, e:
            raise repository.OpenError('Error occured opening the repository: %s' %str(e))
