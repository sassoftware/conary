#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# implements a db-based repository

import log
import repository
import repository.netclient
import util
import versions

from deps import deps
import trovestore
from repository.repository import AbstractRepository
from repository.repository import DataStoreRepository
from repository.repository import ChangeSetJob
from repository.repository import PackageMissing
from repository import changeset
from repository import filecontents

class FilesystemRepository(DataStoreRepository, AbstractRepository):

    ### Package access functions

    def thawFlavor(self, flavor):
	if flavor and flavor != "none":
	    return deps.ThawDependencySet(flavor)

	return None

    def iterAllTroveNames(self):
	a = self.troveStore.iterTroveNames()
	return a

    def getAllTroveLeafs(self, troveNameList):
	d = {}
	for (troveName, troveLeafList) in \
		self.troveStore.iterAllTroveLeafs(troveNameList):
	    d[troveName] = [ versions.VersionFromString(x) for x in
				troveLeafList ]
	return d

    def getTroveVersionList(self, troveNameList):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ versions.VersionFromString(x) for x in
				self.troveStore.iterTroveVersions(troveName) ]

	return d

    def getTroveLeavesByLabel(self, troveNameList, label):
	d = {}
	labelStr = label.asString()
	for troveName in troveNameList:
	    d[troveName] = [ x for x in 
		self.troveStore.iterTroveLeafsByLabel(troveName, labelStr) ]

	return d

    def getTroveVersionsByLabel(self, troveNameList, label):
	d = {}
	labelStr = label.asString()
	for troveName in troveNameList:
	    d[troveName] = [ x for x in 
		self.troveStore.iterTroveVersionsByLabel(troveName, labelStr) ]

	return d

    def getTroveFlavorsLatestVersion(self, troveName, branch):
	return [ (versions.VersionFromString(x[0], 
			timeStamps = [ float(z) for z in x[1].split(":")]),
		  self.thawFlavor(x[2])) for x in 
		    self.troveStore.iterTrovePerFlavorLeafs(troveName, 
							    branch.asString()) ]
	
    def getTroveVersionFlavors(self, troveDict):
	newD = self.troveStore.getTroveFlavors(troveDict)

	for troveName in newD.iterkeys():
	    for version in newD[troveName].iterkeys():
		newD[troveName][version] = \
		    [ self.thawFlavor(x) for x in newD[troveName][version] ]

	return newD

    def hasPackage(self, pkgName):
	return self.troveStore.hasTrove(pkgName)

    def hasTrove(self, pkgName, version, flavor):
	return self.troveStore.hasTrove(pkgName, troveVersion = version,
					troveFlavor = flavor)

    def getTroveLatestVersion(self, pkgName, branch):
        try:
            return self.troveStore.troveLatestVersion(pkgName, branch)
        except KeyError:
            raise PackageMissing(pkgName, branch)

    def getTrove(self, pkgName, version, flavor, pristine = True):
	try:
	    return self.troveStore.getTrove(pkgName, version, flavor)
	except KeyError:
	    raise PackageMissing(pkgName, version)

    def eraseTrove(self, pkgName, version, flavor):
	self.troveStore.eraseTrove(pkgName, version, flavor)

    def addTrove(self, pkg):
	self.troveStore.addTrove(pkg)

    def addPackage(self, pkg):
	self.troveStore.addTrove(pkg)

    def commit(self):
	self.troveStore.commit()

    def rollback(self):
	self.troveStore.rollback()

    def branchesOfTroveLabel(self, troveName, label):
	return self.troveStore.branchesOfTroveLabel(troveName, label)

    def createTroveBranch(self, pkgName, branch):
	log.debug("creating branch %s for %s", branch.asString(), pkgName)
	if not self.hasPackage(pkgName):
	    raise PackageMissing, pkgName
        return self.troveStore.createTroveBranch(pkgName, branch)

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
	return self.troveStore.iterFilesInTrove(troveName, version, flavor,
                                                sortByPath, withFiles)

    ### File functions

    def getFileVersion(self, fileId, version, withContents = 0):
	file = self.troveStore.getFile(fileId, version)
	if withContents:
	    if file.hasContents:
		cont = filecontents.FromDataStore(self.contentsStore, 
						    file.contents.sha1(), 
						    file.contents.size())
	    else:
		cont = None

	    return (file, cont)

	return file

    def addFileVersion(self, fileId, version, file):
	# don't add duplicated to this repository
	if not self.troveStore.hasFile(fileId, version):
	    self.troveStore.addFile(file, version)

    def eraseFileVersion(self, fileId, version):
	self.troveStore.eraseFile(fileId, version)

    ###

    def __del__(self):
	self.close()

    def createBranch(self, newBranch, where, troveList = []):
	if not troveList:
	    troveList = self.iterAllTroveNames()

	troveList = [ (x, where) for x in troveList ]

	branchedTroves = {}
	branchedFiles = {}

	while troveList:
	    troveName = troveList[0][0]
	    location = troveList[0][1]
	    del troveList[0]

	    if branchedTroves.has_key(troveName): continue
	    branchedTroves[troveName] = 1
	    if not self.hasPackage(troveName):
		log.warning("package %s does not exist" % troveName)
		continue

	    if isinstance(location, versions.Version):
		verDict = { troveName : [ location ] }
	    else:
		verDict = self.getTroveLeavesByLabel([troveName], location)

	    # XXX this probably doesn't get flavors right

	    d = self.getTroveVersionFlavors(verDict)
	    fullList = []
	    for (version, flavors) in d[troveName].iteritems():
		for flavor in flavors:
		    fullList.append((troveName, version, flavor))

	    troves = self.getTroves(fullList)

	    for trove in troves:
		branchedVersion = trove.getVersion().fork(newBranch, 
							  sameVerRel = 1)
		self.createTroveBranch(troveName, branchedVersion.branch())
		trove.changeVersion(branchedVersion)

		# make a copy of this list since we're going to update it
		l = [ x for x in trove.iterTroveList() ]
		for (name, version, flavor) in l:
		    troveList.append((name, version))

		    branchedVersion = version.fork(newBranch, sameVerRel = 1)
		    trove.delTrove(name, version, flavor, False)
		    trove.addTrove(name, branchedVersion, flavor)

		self.addTrove(trove)

        # commit branch to the repository
        self.commit()
		    
    def open(self):
	if self.troveStore is not None:
	    self.close()

	self.troveStore = trovestore.TroveStore(self.sqlDB)

    def commitChangeSet(self, cs):
        self.troveStore.begin()
        try:
            # a little odd that creating a class instance has the side
            # effect of modifying the repository...
            ChangeSetJob(self, cs)
        except:
            self.rollback()
            raise
        else:
            self.commit()

    def createChangeSet(self, troveList, recurse = True, withFiles = True):
	"""
	troveList is a list of (troveName, flavor, oldVersion, newVersion, 
        absolute) tuples. 

	if oldVersion == None and absolute == 0, then the trove is assumed
	to be new for the purposes of the change set

	if newVersion == None then the trove is being removed
	"""
	cs = changeset.ChangeSetFromRepository(self)
	for (name, flavor, v1, v2, absolute) in troveList:
	    cs.addPrimaryPackage(name, v2, flavor)

	dupFilter = {}

	# make a copy to remove things from
	troveList = troveList[:]

	# don't use a for in here since we grow troveList inside of
	# this loop
	while troveList:
	    (troveName, flavor, oldVersion, newVersion, absolute) = \
		troveList[0]
	    del troveList[0]

	    # make sure we haven't already generated this changeset; since
	    # troves can be included from other troves we could try
	    # to generate quite a few duplicates
	    if dupFilter.has_key((troveName, flavor)):
		match = False
		for (otherOld, otherNew) in dupFilter[(troveName, flavor)]:
		    if not otherOld and not oldVersion:
			same = True
		    elif not otherOld and oldVersion:
			same = False
		    elif otherOld and not oldVersion:
			same = False
		    else:
			same = otherOld == newVersion

		    if same and otherNew == newVersion:
			match = True
			break
		
		if match: continue

		dupFilter[(troveName, flavor)].append((oldVersion, newVersion))
	    else:
		dupFilter[(troveName, flavor)] = [(oldVersion, newVersion)]

	    if not newVersion:
		# remove this trove and any trove contained in it
		old = self.getTrove(troveName, oldVersion, flavor)
		cs.oldPackage(troveName, oldVersion, flavor)
		for (name, version, flavor) in old.iterTroveList():
                    # it's possible that a component of a trove
                    # was erased, make sure that it is installed
                    if self.hasTrove(name, version, flavor):
                        troveList.append((name, flavor, version, None, 
					    absolute))
		    
		continue
		    
	    new = self.getTrove(troveName, newVersion, flavor)
	 
	    if oldVersion:
		old = self.getTrove(troveName, oldVersion, flavor)
	    else:
		old = None

	    (pkgChgSet, filesNeeded, pkgsNeeded) = \
				new.diff(old, absolute = absolute)

	    if recurse:
		for (pkgName, old, new, flavor) in pkgsNeeded:
		    troveList.append((pkgName, flavor, old, new, absolute))

	    cs.newPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion, newPath) in filesNeeded:
		if oldVersion:
		    (oldFile, oldCont) = self.getFileVersion(fileId, 
				oldVersion, withContents = 1)
		else:
		    oldFile = None
		    oldCont = None

		(newFile, newCont) = self.getFileVersion(fileId, newVersion,
					    withContents = 1)

		(filecs, hash) = changeset.fileChangeSet(fileId, oldFile, 
							 newFile)

		cs.addFile(fileId, oldVersion, newVersion, filecs)

		if hash and withFiles:
		    (contType, cont) = changeset.fileContentsDiff(oldFile, 
						oldCont, newFile, newCont)
		    cs.addFileContents(fileId, contType, cont, 
				       newFile.flags.isConfig())

	return cs

    def close(self):
	if self.troveStore is not None:
	    self.troveStore.db.close()
	    self.troveStore = None

    def __init__(self, name, path):
	self.top = path
	self.troveStore = None
	self.reposSet = repository.netclient.NetworkRepositoryClient({ name : self })
	
	self.sqlDB = self.top + "/sqldb"

	try:
	    util.mkdirChain(self.top)
	except OSError, e:
	    raise repository.OpenError(str(e))
	    
        self.open()

	DataStoreRepository.__init__(self, path)
	AbstractRepository.__init__(self)

