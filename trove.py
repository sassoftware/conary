#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import string
import versions

# this is the repository's idea of a package
class Package:
    def getName(self):
        return self.name
    
    def getVersion(self):
        return self.version
    
    def changeVersion(self, version):
        self.version = version
    
    def addFile(self, fileId, path, version):
	self.files[path] = (fileId, path, version)
	self.idMap[fileId] = (path, version)

    # fileId is the only thing that must be here; the other fields could
    # be "-"
    def updateFile(self, fileId, path, version):
	(origPath, origVersion) = self.idMap[fileId]

	if not path:
	    path = origPath
	else:
	    del self.files[path]

	if not version:
	    version = origVersion
	    
	self.files[path] = (fileId, path, version)
	self.idMap[fileId] = (path, version)

    def removeFile(self, fileId):   
	path = self.idMap[fileId][0]
	del self.files[path]
	del self.idMap[fileId]

    def fileList(self):
	l = []
        paths = self.files.keys()
        paths.sort()
        for path in paths:
	    l.append(self.files[path])

	return l

    def getFile(self, fileId):
	return self.idMap[fileId]

    def formatString(self):
	str = ""
	for (fileId, path, version) in self.files.values():
	    str = str + ("%s %s %s\n" % (fileId, path, version.asString()))
	return str

    # returns a dictionary mapping a fileId to a (path, version, pkgName) tuple
    def applyChangeSet(self, pkgCS):
	fileMap = {}

	for (fileId, path, fileVersion) in pkgCS.getNewFileList():
	    self.addFile(fileId, path, fileVersion)
	    fileMap[fileId] = self.idMap[fileId] + (self.name, )

	for (fileId, path, fileVersion) in pkgCS.getChangedFileList():
	    self.updateFile(fileId, path, fileVersion)
	    # look up the path/version in self.idMap as the ones here
	    # could be None
	    fileMap[fileId] = self.idMap[fileId] + (self.name, )

	for fileId in pkgCS.getOldFileList():
	    self.removeFile(fileId)

	return fileMap

    def diff(self, them, themVersion, ourVersion):
	# find all of the file ids which have been added, removed, and
	# stayed the same
	if them:
	    themMap = them.idMap
	    chgSet = PackageChangeSet(self.name, themVersion, ourVersion)
	else:
	    themMap = {}
	    chgSet = PackageChangeSet(self.name, None, ourVersion)

	removedIds = []
	addedIds = []
	sameIds = {}
	filesNeeded = []

	allIds = self.idMap.keys() + themMap.keys()
	for id in allIds:
	    inSelf = self.idMap.has_key(id)
	    inThem = themMap.has_key(id)
	    if inSelf and inThem:
		sameIds[id] = None
	    elif inSelf:
		addedIds.append(id)
	    else:
		removedIds.append(id)

	for id in removedIds:
	    chgSet.oldFile(id)

	for id in addedIds:
	    (selfPath, selfVersion) = self.idMap[id]
	    filesNeeded.append((id, None, selfVersion))
	    chgSet.newFile(id, selfPath, selfVersion)

	for id in sameIds.keys():
	    (selfPath, selfVersion) = self.idMap[id]
	    (themPath, themVersion) = themMap[id]

	    newPath = None
	    newVersion = None

	    if selfPath != themPath:
		newPath = selfPath

	    if not selfVersion.equal(themVersion):
		newVersion = selfVersion
		filesNeeded.append((id, themVersion, selfVersion))

	    if newPath or newVersion:
		chgSet.changedFile(id, newPath, newVersion)

	return (chgSet, filesNeeded)

    def __init__(self, name, version):
	self.files = {}
	self.idMap = {}
	self.name = name
	self.version = version

class PackageChangeSet:

    def newFile(self, fileId, path, version):
	self.newFiles.append((fileId, path, version))

    def getNewFileList(self):
	return self.newFiles

    def oldFile(self, fileId):
	self.oldFiles.append(fileId)

    def getOldFileList(self):
	return self.oldFiles

    def getName(self):
	return self.name

    def getOldVersion(self):
	return self.oldVersion

    def getNewVersion(self):
	return self.newVersion

    # path and/or version can be None
    def changedFile(self, fileId, path, version):
	self.changedFiles.append((fileId, path, version))

    def getChangedFileList(self):
	return self.changedFiles

    def parse(self, line):
	action = line[0]

	if action == "+" or action == "~":
	    (fileId, path, version) = string.split(line[1:])

	    if version == "-":
		version = None
	    else:
		version = versions.VersionFromString(version)

	    if path == "-":
		path = None

	    if action == "+":
		self.newFile(fileId, path, version)
	    else:
		self.changedFile(fileId, path, version)
	elif action == "-":
	    self.oldFile(line[1:])

    def formatToFile(self, changeSet, cfg, f):
	f.write("%s " % self.name)
	if self.oldVersion:
	    f.write("from %s to " % self.oldVersion.asString(cfg.defaultbranch))
	else:
	    f.write("abstract ")
	f.write("%s\n" % self.newVersion.asString(cfg.defaultbranch))

	for (fileId, path, version) in self.newFiles:
	    f.write("\tadded %s (%s(.*)%s)\n" % (path, fileId[:6], fileId[-6:]))
	for (fileId, path, version) in self.changedFiles:
	    f.write("\tchanged %s\n" % path)
	    change = changeSet.getFileChange(fileId)
	    print "\t\t%s" % change
	for fileId in self.oldFiles:
	    f.write("\tremoved %s(.*)%s\n" % (fileId[:6], fileId[-6:]))

    def asString(self):
	rc = ""

	for id in self.getOldFileList():
	    rc += "-%s\n" % id

	for (id, path, version) in self.getNewFileList():
	    rc += "+%s %s %s\n" % (id, path, version.asString())

	for (id, path, version) in self.getChangedFileList():
	    rc += "~%s " % id
	    if path:
		rc += path
	    else:
		rc += "-"

	    if version:
		rc += " " + version.asString() + "\n"
	    else:
		rc += " -\n"

	if self.oldVersion:
	    oldVerStr = self.oldVersion.asString()
	else:
	    oldVerStr = "(none)"

	hdr = "SRS PKG CHANGESET %s %s %s %d\n" % \
		  (self.name, oldVerStr, self.newVersion.asString(), 
		   rc.count("\n"))
	return hdr + rc	
    
    def __init__(self, name, oldVersion, newVersion):
	self.name = name
	self.oldVersion = oldVersion
	self.newVersion = newVersion
	self.newFiles = []
	self.oldFiles = []
	self.changedFiles = []

class PackageFromFile(Package):

    def read(self, dataFile):
	for line in dataFile.readLines():
	    (fileId, path, version) = string.split(line)
	    version = versions.VersionFromString(version)
	    self.addFile(fileId, path, version)

    def __init__(self, name, dataFile, version):
	Package.__init__(self, name, version)
	self.read(dataFile)

def stripNamespace(namespace, pkgName):
    if pkgName.startswith(namespace + ":"):
	return pkgName[len(namespace) + 1:]
    return pkgName
