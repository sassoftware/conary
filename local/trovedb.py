#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import dbhash
import log
import os
import package
import struct
import versioned
import versions

class TroveDatabase:

    """
    Maintains an indexed package database. All of these access functions
    could be much more efficient; they instantiate a complete package object
    when quite often we just need the version or name.
    """

    def _updateIndicies(self, trvId, trv, method):
	method(self.nameIdx, trvId, trv.getName())

	for (fileId, (path, version)) in trv.iterFileList():
	    method(self.pathIdx, trvId, path)

	for (name, versionList) in trv.iterPackageList():
	    method(self.partofIdx, trvId, name)

    def addTrove(self, trv):
	"""
	Add a trove to the database, along with the appropriate index
	entries.
	"""
	trvId = self.trvs['COUNTER']
	numericId = struct.unpack('!i', trvId)[0]
	self.trvs['COUNTER'] = struct.pack('!i', numericId + 1)
	str = "\0".join([trv.getName(), trv.getVersion().freeze(), 
			 trv.formatString()])
	self.trvs[trvId] = str
	self._updateIndicies(trvId, trv, Index.addEntry)

    def updateTrove(self, trv):
	"""
	Updates a trove in the database, along with the appropriate index
	entries.
	"""
	# FIXME: this could be more efficient
	self.delTrove(trv.getName(), trv.getVersion(), forUpdate = True)
	self.addTrove(trv)

    def delTrove(self, name, version, forUpdate = False):
	for trvId in self.nameIdx.iterGetEntries(name):
	    trv = self._getPackage(trvId)

	    if not trv.getVersion() == version:
		continue

	    del self.trvs[trvId]
	    self._updateIndicies(trvId, trv, Index.delEntry)

	if forUpdate:
	    return

	for trvId in self.partofIdx.iterGetEntries(name):
	    trv = self._getPackage(trvId)
	    updateTrove = False
	    for (inclName, versionList) in trv.iterPackageList():
		if inclName == name: 
		    for inclVersion in versionList:
			if inclVersion == version:
			    updateTrove = True
			    trv.delPackageVersion(name, version, 
						  missingOkay = False)
			    break
		    break

	    if updateTrove:
		self.updateTrove(trv)
		foundOne = True

    def getAllTroveNames(self):
	return self.nameIdx.keys()

    def iterFindByName(self, name):
	"""
	Returns all of the troves with a particular name.

	@param name: name of the trove
	@type name: str
	@rtype: list of package.Trove
	"""
	for trvId in self.nameIdx.iterGetEntries(name):
	    trv = self._getPackage(trvId)
	    yield trv

    def iterFindByPath(self, path):
	"""
	Returns all of the troves containing a particular path.

	@param path: path to find in the troves
	@type path: str
	@rtype: list of package.Trove
	"""
	for trvId in self.pathIdx.iterGetEntries(path):
	    trv = self._getPackage(trvId)
	    yield trv

    def hasByName(self, name):
	return self.nameIdx.has_key(name)

    def _getPackage(self, trvId):
	(name, version, str) = self.trvs[trvId].split("\0", 2)
	version = versions.ThawVersion(version)
	return package.TroveFromFile(name, versioned.FalseFile(str), version)

    def __init__(self, top, mode):
	"""
	Initialize a new trove database.

	@param top: directory the data files are stored in
	@type top: str
	@param mode: mode of the database
	@type mode: "c" or "r"
	"""
	self.top = top
	p = top + "/troves.db"
	if not os.path.exists(p) and mode == "c":
	    self.trvs = dbhash.open(p, mode)
	    self.trvs['COUNTER'] = struct.pack("!i", 0)
	else:
	    self.trvs = dbhash.open(p, mode)

	self.nameIdx = Index("name", top + "/name.idx", mode)
	self.pathIdx = Index("path", top + "/path.idx", mode)
	self.partofIdx = Index("partof", top + "/partof.idx", mode)

class Index:

    def iterGetEntries(self, item):
	if not self.db.has_key(item):
	    return

	idList = self.db[item]
	l = len(idList)
	i = 0
	while (i < len(idList)):
	    yield idList[i:i+4]
	    i += 4

    def addEntry(self, trvId, item):
	if self.db.has_key(item):
	    self.db[item] = self.db[item] + trvId
	else:
	    self.db[item] = trvId

    def delEntry(self, trvId, item):
	if not self.db.has_key(item):
	    log.warning("%s index missing entry for %s", self.name, item)
	    return

	idList = self.db[item]
	next = idList.find(trvId)
	last = -1
	foundOne = False
	while next != -1:
	    if next % 4 == 0:
		if foundOne:
		    log.warning("%s index has duplicate entry for %s", 
				self.name, item)
		idList = idList[:next] + idList[next + 4:]
		foundOne = True
	    else:
		last = next

	    next = idList.find(trvId, last + 1)
		
	if not foundOne:
	    log.warning("%s index missing entry for %s", self.name, item)

	if (not idList):
	    del self.db[item]
	else:
	    self.db[item] = idList

    def keys(self):
	return self.db.keys()

    def has_key(self, name):
	return self.db.has_key(name)

    def __init__(self, name, path, mode):
	self.name = name
	self.db = dbhash.open(path, mode)
	
