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

# implements a db-based repository

from deps import deps
import os
from lib import util, stackutil, log
from netauth import NetworkAuthorization
import repository
import repository.netclient
from repository.repository import AbstractRepository
from repository.repository import ChangeSetJob
from datastore import DataStoreRepository, DataStore
from repository.repository import DuplicateBranch
from repository.repository import RepositoryError
from repository.repository import TroveMissing
from repository import changeset
from repository import filecontents
import sqlite3
import sys
import trovestore
import versions

class FilesystemRepository(DataStoreRepository, AbstractRepository):

    ### Package access functions

    def thawFlavor(self, flavor):
	if flavor and flavor != "none":
	    return deps.ThawDependencySet(flavor)

	return deps.DependencySet()

    def hasTrove(self, pkgName, version, flavor):
	return self.troveStore.hasTrove(pkgName, troveVersion = version,
					troveFlavor = flavor)

    def getTrove(self, pkgName, version, flavor, pristine = True,
                 withFiles = True):
	try:
	    return self.troveStore.getTrove(pkgName, version, flavor,
                                            withFiles = withFiles)
	except KeyError:
	    raise TroveMissing(pkgName, version)

    def addTrove(self, pkg):
	return self.troveStore.addTrove(pkg)

    def addTroveDone(self, pkg):
	self.troveStore.addTroveDone(pkg)

    ### File functions

    def getFileVersion(self, pathId, fileId, fileVersion, withContents = 0):
	# the get trove netclient provides doesn't work with a 
	# FilesystemRepository (it needs to create a change set which gets 
	# passed)
	if fileVersion.branch().label().getHost() != self.name:
	    assert(not withContents)
	    return self.reposSet.getFileVersion(pathId, fileId, fileVersion)

	file = self.troveStore.getFile(pathId, fileId)
	if withContents:
	    if file.hasContents:
		cont = filecontents.FromDataStore(self.contentsStore, 
						    file.contents.sha1())
	    else:
		cont = None

	    return (file, cont)

	return file

    def addFileVersion(self, troveInfo, pathId, fileObj, path, fileId, fileVersion):
	# don't add duplicates to this repository
	#if not self.troveStore.hasFile(fileObj.pathId(), fileVersion):
	self.troveStore.addFile(troveInfo, pathId, fileObj, path, fileId, fileVersion)

    ###

    def commitChangeSet(self, cs, serverName):
	# let's make sure commiting this change set is a sane thing to attempt
	for pkg in cs.iterNewPackageList():
	    v = pkg.getNewVersion()
	    label = v.branch().label()
	    if isinstance(label, versions.EmergeLabel):
		raise repository.repository.CommitError, \
		    "can not commit items on localhost@local:EMERGE"
	    
	    if isinstance(label, versions.CookLabel):
		raise repository.repository.CommitError, \
		    "can not commit items on localhost@local:COOK"

        self.troveStore.begin()
        try:
            # a little odd that creating a class instance has the side
            # effect of modifying the repository...
            ChangeSetJob(self, cs, [ serverName ], resetTimestamps = True)
        except:
            print >> sys.stderr, "exception occurred while committing change set"
            stackutil.printTraceBack()
            print >> sys.stderr, "attempting rollback"
            self.troveStore.rollback()
            raise
        else:
            self.troveStore.commit()

    def getFileContents(self, itemList):
        contents = []
        
        for item in itemList:
            (fileId, fileVersion) = item[0:2]
    
            # the get trove netclient provides doesn't work with a 
            # FilesystemRepository (it needs to create a change set which gets 
            # passed)
            if fileVersion.branch().label().getHost() == self.name:
                fileObj = item[2]
                cont = filecontents.FromDataStore(self.contentsStore, 
                                                  fileObj.contents.sha1())
            else:
                # a bit of sleight of hand here... we look for this file in
                # the trove it was first built in
                #
                # this could cause us to run out of file descriptors on large
                # troves. it might be better to close the file and return
                # a filecontents object?
                cont = self.reposSet.getFileContents([ item ])[0]

            contents.append(cont)

        return contents

    def createChangeSet(self, troveList, recurse = True, withFiles = True,
                        withFileContents = True, excludeAutoSource = False):
	"""
	troveList is a list of (troveName, flavor, oldVersion, newVersion, 
        absolute) tuples. 

	if oldVersion == None and absolute == 0, then the trove is assumed
	to be new for the purposes of the change set

	if newVersion == None then the trove is being removed
	"""
	cs = changeset.ChangeSetFromRepository(self)

        externalTroveList = []
        externalFileList = []

	dupFilter = {}

	# make a copy to remove things from
	troveList = troveList[:]

	# don't use a for in here since we grow troveList inside of
	# this loop
	while troveList:
	    (troveName, (oldVersion, oldFlavor), 
		        (newVersion, newFlavor), absolute) = \
		troveList[0]
	    del troveList[0]

	    # make sure we haven't already generated this changeset; since
	    # troves can be included from other troves we could try
	    # to generate quite a few duplicates
	    if dupFilter.has_key((troveName, oldFlavor, newFlavor)):
		match = False
		for (otherOld, otherNew) in \
				dupFilter[(troveName, oldFlavor, newFlavor)]:
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

		dupFilter[(troveName, oldFlavor, newFlavor)].append(
				    (oldVersion, newVersion))
	    else:
		dupFilter[(troveName, oldFlavor, newFlavor)] = \
				    [(oldVersion, newVersion)]

	    if not newVersion:
                if oldVersion.branch().label().getHost() != self.name:
                    externalTroveList.append((troveName, 
                                         (oldVersion, oldFlavor),
                                         (None, None), absolute))
                else:
                    # remove this trove and any trove contained in it
                    old = self.getTrove(troveName, oldVersion, oldFlavor)
                    cs.oldPackage(troveName, oldVersion, oldFlavor)
                    for (name, version, flavor) in old.iterTroveList():
                        troveList.append((name, (version, flavor),
                                                (None, None), absolute))
		continue

            if newVersion.branch().label().getHost() != self.name or \
               (oldVersion and 
                oldVersion.branch().label().getHost() != self.name):
                # don't try to make changesets between repositories; the
                # client can do that itself
                externalTroveList.append((troveName, (oldVersion, oldFlavor),
                                     (newVersion, newFlavor), absolute))
                continue

            new = self.getTrove(troveName, newVersion, newFlavor, 
                                withFiles = withFiles)
	 
	    if oldVersion:
                old = self.getTrove(troveName, oldVersion, oldFlavor,
                                    withFiles = withFiles)
	    else:
		old = None

	    (pkgChgSet, filesNeeded, pkgsNeeded) = \
				new.diff(old, absolute = absolute)

	    if recurse:
		for (pkgName, old, new, oldFlavor, newFlavor) in pkgsNeeded:
		    troveList.append((pkgName, (old, oldFlavor),
					       (new, newFlavor), absolute))

	    cs.newPackage(pkgChgSet)

	    # sort the set of files we need into bins based on the server
	    # name
	    serverIdx = {}
            getList = []
            newFilesNeeded = []

	    for (pathId, oldFileId, oldFileVersion, newFileId, newFileVersion) in filesNeeded:
                # if either the old or new file version is on a different
                # repository, creating this diff is someone else's problem
                if newFileVersion.branch().label().getHost() != self.name or \
                   (oldFileVersion and
                    oldFileVersion.branch().label().getHost() != self.name):
                    externalFileList.append((pathId, troveName,
                         (oldVersion, oldFlavor, oldFileId, oldFileVersion),
                         (newVersion, newFlavor, newFileId, newFileVersion)))
                else:
                    newFilesNeeded.append((pathId, oldFileId, oldFileVersion,
                                             newFileId, newFileVersion))
                    if oldFileVersion:
                        getList.append((pathId, oldFileId, oldFileVersion))
                    getList.append((pathId, newFileId, newFileVersion))

            filesNeeded = newFilesNeeded
            del newFilesNeeded
            idIdx = self.troveStore.getFiles(getList)

            # Walk this in reverse order. This may seem odd, but the
            # order in the final changeset is set by sorting that happens
            # in the change set object itself. The only reason we sort
            # here at all is to make sure PTR file types come before the
            # file they refer to. Reverse shorting makes this a bit easier.
            filesNeeded.sort()
            filesNeeded.reverse()

            ptrTable = {}
	    for (pathId, oldFileId, oldFileVersion, newFileId, newFileVersion) in filesNeeded:
		oldFile = None
		if oldFileVersion:
		    oldFile = idIdx[(pathId, oldFileId)]

		oldCont = None
		newCont = None

		newFile = idIdx[(pathId, newFileId)]

		(filecs, contentsHash) = changeset.fileChangeSet(pathId,
                                                                 oldFile, 
                                                                 newFile)

		cs.addFile(oldFileId, newFileId, filecs)

                if not withFileContents or (excludeAutoSource and
                   newFile.flags.isAutoSource()):
                    continue

		# this test catches files which have changed from not
		# config files to config files; these need to be included
		# unconditionally so we always have the pristine contents
		# to include in the local database
		if (contentsHash or (oldFile and newFile.flags.isConfig() 
                                      and not oldFile.flags.isConfig())):
		    if oldFileVersion and oldFile.hasContents:
			oldCont = self.getFileContents(
                            [ (oldFileId, oldFileVersion, oldFile) ])[0]

		    newCont = self.getFileContents(
                            [ (newFileId, newFileVersion, newFile) ])[0]

		    (contType, cont) = changeset.fileContentsDiff(oldFile, 
						oldCont, newFile, newCont)

                    # we don't let config files be ptr types; if they were
                    # they could be ptrs to things which aren't config files,
                    # which would completely hose the sort order we use. this
                    # could be relaxed someday to let them be ptr's to other
                    # config files
                    if not newFile.flags.isConfig() and \
                                contType == changeset.ChangedFileTypes.file:
                        contentsHash = newFile.contents.sha1()
                        ptr = ptrTable.get(contentsHash, None)
                        if ptr is not None:
                            contType = changeset.ChangedFileTypes.ptr
                            cont = filecontents.FromString(ptr)
                        else:
                            ptrTable[contentsHash] = pathId

                    if not newFile.flags.isConfig() and \
                                contType == changeset.ChangedFileTypes.file:
                        cont = filecontents.CompressedFromDataStore(
                                              self.contentsStore, 
                                              newFile.contents.sha1())
                        compressed = True
                    else:
                        compressed = False

                    # ptr entries are not compressed, whether or not they
                    # are config files. override the compressed rule from
                    # above
                    if contType == changeset.ChangedFileTypes.ptr:
                        compressed = False

		    cs.addFileContents(pathId, contType, cont, 
				       newFile.flags.isConfig(),
                                       compressed = compressed)

	return (cs, externalTroveList, externalFileList)

    def close(self):
	if self.troveStore is not None:
	    self.troveStore.db.close()
	    self.troveStore = None

    def __del__(self):
	self.close()

    def __init__(self, name, troveStore, path, repositoryMap, logFile = None):
	self.top = path
	self.name = name
	map = dict(repositoryMap)
	map[name] = self
	self.reposSet = repository.netclient.NetworkRepositoryClient(map)
	
	self.troveStore = troveStore

	self.sqlDbPath = self.top + "/sqldb"

	fullPath = path + "/contents"
	util.mkdirChain(fullPath)
        store = DataStore(fullPath, logFile = logFile)

	DataStoreRepository.__init__(self, path, logFile = logFile,
                                     dataStore = store)
	AbstractRepository.__init__(self)
