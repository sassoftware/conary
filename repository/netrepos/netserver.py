from repository import changeset
from repository import fsrepos
import filecontainer
import os
import tempfile
import xmlshims

class NetworkRepositoryServer(xmlshims.NetworkConvertors):

    def allTroveNames(self):
	return [ x for x in self.iterAllTroveNames() ]

    def createBranch(self, newBranch, kind, frozenLocation, troveList):
	newBranch = self.toLabel(newBranch)
	if kind == 'v':
	    location = self.toVersion(frozenLocation)
	elif kind == 'l':
	    location = self.toLabel(frozenLocation)
	else:
	    return 0

	self.repos.createBranch(newBranch, location, troveList)
	return 1

    def hasPackage(self, pkgName):
	return self.repos.troveStore.hasTrove(pkgName)

    def hasTrove(self, pkgName, version, flavor):
	return self.repos.troveStore.hasTrove(pkgName, troveVersion = version,
					troveFlavor = flavor)

    def getTroveVersionList(self, troveNameList):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ x for x in
			    self.repos.troveStore.iterTroveVersions(troveName) ]

	return d

    def getFilesInTrove(self, troveName, version, flavor,
                        sortByPath = False, withFiles = False):
        gen = self.repos.troveStore.iterFilesInTrove(troveName,
                                               self.toVersion(version),
                                               self.toFlavor(flavor),
                                               sortByPath, 
                                               withFiles) 
        if withFiles:
            return [ (x[0], x[1], self.fromVersion(x[2]), self.fromFile(x[3]))
                     for x in gen ]
        else:
            return [ (x[0], x[1], self.fromVersion(x[2])) for x in gen ]

    def getFileContents(self, sha1list):
	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.cfc-out')
	f = os.fdopen(fd, "w")

	fc = filecontainer.FileContainer(f)
	del f
	d = self.repos.getFileContents(sha1list)

	for sha1 in sha1list:
	    fc.addFile(sha1, d[sha1], "", d[sha1].fullSize)
	fc.close()

	fileName = os.path.basename(path)
	return "%s/%s" % (self.urlBase, fileName[:-4])

    def getAllTroveLeafs(self, troveNames):
	d = {}
	for troveName in troveNames:
	    d[troveName] = [ x for x in
			    self.repos.troveStore.iterAllTroveLeafs(troveName) ]
	return d

    def getTroveLeavesByLabel(self, troveNameList, labelStr):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ x for x in
			self.repos.troveStore.iterTroveLeafsByLabel(troveName,
								   labelStr) ]

	return d

    def getTroveVersionFlavors(self, troveDict):
	newD = {}
	for (troveName, versionList) in troveDict.iteritems():
	    innerD = {}
	    for versionStr in versionList:
		innerD[versionStr] = [ self.fromFlavor(x) for x in 
		    self.repos.troveStore.iterTroveFlavors(troveName, 
						 self.toVersion(versionStr)) ]
	    newD[troveName] = innerD

	return newD

    def getTroveLatestVersion(self, pkgName, branchStr):
        try:
            return self.fromVersion(self.repos.troveStore.troveLatestVersion(pkgName, 
                                                                             self.toBranch(branchStr)))
        except KeyError:
            return 0

    def getTroveFlavorsLatestVersion(self, troveName, branch):
	return [ x for x in self.repos.troveStore.iterTrovePerFlavorLeafs(troveName, branch) ]

    def getChangeSet(self, chgSetList, recurse, withFiles):
	l = []
	for (name, flavor, old, new, absolute) in chgSetList:
	    if old == 0:
		l.append((name, self.toFlavor(flavor), None,
			 self.toVersion(new), absolute))
	    else:
		l.append((name, self.toFlavor(flavor), self.toVersion(old),
			 self.toVersion(new), absolute))

	cs = self.repos.createChangeSet(l, recurse = recurse, 
					withFiles = withFiles)
	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.ccs-out')
	os.close(fd)
	cs.writeToFile(path)
	fileName = os.path.basename(path)
	return "%s/%s" % (self.urlBase, fileName[:-4])

    def iterAllTroveNames(self):
	return self.repos.iterAllTroveNames()

    def prepareChangeSet(self):
	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.ccs-in')
	os.close(fd)
	fileName = os.path.basename(path)
	return "%s/%s" % (self.urlBase, fileName[:-3])

    def commitChangeSet(self, url):
	assert(url.startswith(self.urlBase))
	fileName = url[len(self.urlBase):] + "-in"
	path = "%s/%s" % (self.tmpPath, fileName)

	try:
	    cs = changeset.ChangeSetFromFile(path)
	finally:
	    pass
	    os.unlink(path)

	self.repos.commitChangeSet(cs)

	return True

    def getFileVersion(self, fileId, version, withContents = 0):
	f = self.repos.troveStore.getFile(fileId, self.toVersion(version))
	return self.fromFile(f)

    def checkVersion(self, clientVersion):
        if clientVersion < 0:
            raise RuntimeError, "client is too old"
        return 0

    def __init__(self, path, tmpPath, urlBase):
	self.repos = fsrepos.FilesystemRepository(path)
	self.tmpPath = tmpPath
	self.urlBase = urlBase

