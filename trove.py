#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import copy
import files
import versions

"""
Packages are groups of files and other packages, which are included by
reference. By convention, "package" often refers to a package with
files but no other packages, while a "group" means a package with other
packages but no files. While this object allows any mix of file and
package inclusion, in practice srs doesn't allow it.
"""

# this is the repository's idea of a package
class Package:

    def copy(self):
	return copy.deepcopy(self)

    def getName(self):
        return self.name
    
    def getVersion(self):
        return self.version
    
    def changeVersion(self, version):
        self.version = version

    def addFile(self, fileId, path, version):
	self.idMap[fileId] = (path, version)

    # fileId is the only thing that must be here; the other fields could
    # be "-"
    def updateFile(self, fileId, path, version):
	(origPath, origVersion) = self.idMap[fileId]

	if not path:
	    path = origPath

	if not version:
	    version = origVersion
	    
	self.idMap[fileId] = (path, version)

    def removeFile(self, fileId):   
	del self.idMap[fileId]

	return self.idMap.iteritems()

    def iterFileList(self):
	# don't use idMap.iteritems() here; we don't want to exposure
	# our internal format
	for (theId, (path, version)) in self.idMap.iteritems():
	    yield (theId, path, version)

    def getFile(self, fileId):
	return self.idMap[fileId]

    def hasFile(self, fileId):
	return self.idMap.has_key(fileId)

    def addPackageVersion(self, name, version, presentOkay = False):
	"""
	Adds a single version of a package.

	@param name: name of the package
	@type name: str
	@param version: version of the package
	@type version: versions.Version
	"""
	self.packages[(name, version)] = True

    def delPackageVersion(self, name, version, missingOkay):
	"""
	Removes a single version of a package.

	@param name: name of the package
	@type name: str
	@param version: version of the package
	@type version: versions.Version
	@param missingOkay: should we raise an error if the version isn't
	part of this trove?
	@type missingOkay: boolean
	"""
	if self.packages.has_key((name, version)):
	    del self.packages[(name, version)]
	elif missingOkay:
	    pass
	else:
	    # FIXME, this isn't the right thing to do
	    raise KeyError

    def iterPackageList(self):
	"""
	Returns a generator for (packageName, version) ordered pairs, listing
	all of the package in the group, along with their versions. 

	@rtype: list
	"""
	return self.packages.iterkeys()

    def hasPackageVersion(self, name, version):
	return self.packages.has_key((name, version))

    def read(self, dataFile):
	lines = dataFile.readlines()

	fields = lines[0].split()
	fileCount = int(fields[0])
	pkgCount = int(fields[1])

	start = 1
	fileEnd = start + fileCount
	pkgEnd = fileEnd + pkgCount

	for line in lines[start:fileEnd]:
	    (fileId, path, version) = line.split()
	    version = versions.ThawVersion(version)
	    self.addFile(fileId, path, version)

	for line in lines[fileEnd:pkgEnd]:
	    (name, version) = line.split()
	    self.addPackageVersion(name, versions.VersionFromString(version))

    def freeze(self):
	"""
	Returns a string representing everything about this package, which can
	later be read by the read() method. The format of the string is:

	<file count> <group count>
	FILEID1 PATH1 VERSION1
	FILEID2 PATH2 VERSION2
	.
	.
	.
	FILEIDn PATHn VERSIONn
	PACKAGE1 VERSION1
	PACKAGE2 VERSION2
	.
	.
	.
	PACKAGEn VERSIONn
	GROUP FILE

	Group file may be empty, in which case nothing follows the newline
	for the final file package entry.

	"""
        rc = []
	rc.append("%d %d\n" % (len(self.idMap), len(self.packages)))
        rc += [ "%s %s %s\n" % (x[0], x[1][0], x[1][1].freeze())
                for x in self.idMap.iteritems() ]

	rc += [ "%s %s\n" % (x[0], x[1].asString()) for x in 
				    self.packages.iterkeys() ]
	return "".join(rc)

    # returns a dictionary mapping a fileId to a (path, version, pkgName) tuple
    def applyChangeSet(self, pkgCS):
	"""
	Updates the package from the changes specified in a change set.
	Returns a dictionary, indexed by fileId, which gives the
	(path, version, packageName) for that file.

	@param pkgCS: change set
	@type pkgCS: PackageChangeSet
	@rtype: dict
	"""

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

	self.mergePackageListChanges(pkgCS.iterChangedPackages())

	return fileMap

    def mergePackageListChanges(self, changeList, redundantOkay = False):
	"""
	Merges a set of changes to the included package list into this
	package.

	@param changeList: A list or generator specifying a set of
	package changes; this is the same as returned by
	PackageChangeSet.iterChangedPackages()
	@type changeList: (name, list) tuple
	@param redundantOkay: Redundant changes are normally considered errors
	@type redundantOkay: boolean
	"""

	for (name, list) in changeList:
	    for (oper, version) in list:
		if oper == '+':
		    self.addPackageVersion(name, version, 
					   presentOkay = redundantOkay)

		elif oper == "-":
		    self.delPackageVersion(name, version, 
					   missingOkay = redundantOkay)
    
    def __eq__(self, them):
	"""
	Compare two troves for equality. This is an expensive operation,
	and shouldn't really be done. It's handy for testing the database
	though.
	"""
	if self.getName() != them.getName():
	    return False
	if self.getVersion() != them.getVersion():
	    return False

	(csg, pcl, fcl) = self.diff(them)
	return (not pcl) and (not fcl)

    def diff(self, them, absolute = 0):
	"""
	Generates a change set between them (considered the old version) and
	this instance. We return the change set, a list of other package diffs
	which should be included for this change set to be complete, and a list
	of file change sets which need to be included.  The list of package
	changes is of the form (pkgName, oldVersion, newVersion).  If absolute
	is True, oldVersion is always None and absolute diffs can be used.
	Otherwise, absolute versions are not necessary, and oldVersion of None
	means the package is new. The list of file changes is a list of
	(fileId, oldVersion, newVersion, newPath) tuples, where newPath is the
	path to the file in this package.

	@param them: object to generate a change set from (may be None)
	@type them: Group
	@param absolute: tells if this is a new group or an absolute change
	when them is None
	@type absolute: boolean
	@rtype: (ChangeSetGroup, packageChangeList, fileChangeList)
	"""

	# find all of the file ids which have been added, removed, and
	# stayed the same
	if them:
	    themMap = them.idMap
	    chgSet = PackageChangeSet(self.name, them.getVersion(),	
				      self.getVersion())
	else:
	    themMap = {}
	    chgSet = PackageChangeSet(self.name, None, self.getVersion(),
				      absolute = absolute)

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
	    filesNeeded.append((id, None, selfVersion, selfPath))
	    chgSet.newFile(id, selfPath, selfVersion)

	for id in sameIds.keys():
	    (selfPath, selfVersion) = self.idMap[id]
	    (themPath, themVersion) = themMap[id]

	    newPath = None
	    newVersion = None

	    if selfPath != themPath:
		newPath = selfPath

	    if not selfVersion == themVersion:
		newVersion = selfVersion
		filesNeeded.append((id, themVersion, selfVersion, selfPath))

	    if newPath or newVersion:
		chgSet.changedFile(id, newPath, newVersion)

	# now handle the packages we include
	added = {}
	removed = {}

	for key in self.packages.iterkeys():
	    if them and them.packages.has_key(key): continue

	    (name, version) = key
	    chgSet.newPackageVersion(name, version)
	    if added.has_key(name):
		added[name].append(version)
	    else:
		added[name] = [ version ]

	if them:
	    for key in them.packages.iterkeys():
		if self.packages.has_key(key): continue

		(name, version) = key
		chgSet.oldPackageVersion(name, version)
		if removed.has_key(name):
		    removed[name].append(version)
		else:
		    removed[name] = [ version ]

	pkgList = []

	if absolute:
	    for name in added.keys():
		for version in added[name]:
		    pkgList.append((name, None, version))
	    return (chgSet, filesNeeded, pkgList)

	# use added and removed to assemble a list of package diffs which need
	# to go along with this change set
	for name in added.keys():
	    if not removed.has_key(name):
		for version in added[name]:
		    pkgList.append((name, None, version))
		continue

	    # name was changed between this version. for each new version
	    # of a package, try and generate the diff between that package
	    # and the version of the package which was removed which was
	    # on the same branch. if that's not possible, see if the parent
	    # of the package was removed, and use that as the diff. if
	    # we can't do that and only one version of this package is
	    # being obsoleted, use that for the diff. if we can't do that
	    # either, throw up our hands in a fit of pique
	    
	    for version in added[name]:
		branch = version.branch()
		if version.hasParent():
		    parent = version.parent()
		else:
		    parent = None

		if len(removed[name]) == 1:
		    pkgList.append((name, removed[name][0], version))
		else:
		    sameBranch = None
		    parentNode = None

		    for other in removed[name]:
			if other.branch() == branch:
			    sameBranch = other
			if parent and other == parent:
			    parentNode = other

		    if sameBranch:
			pkgList.append((name, sameBranch, version))
		    elif parentNode:
			pkgList.append((name, parentNode, version))
		    else:
			# Here's the fit of pique. This shouldn't happen
			# except for the most ill-formed of groups.
			raise IOError, "ick. yuck. blech. ptooey."

	return (chgSet, filesNeeded, pkgList)

    def __init__(self, name, version):
	self.idMap = {}
	self.name = name
	self.version = version
	self.packages = {}

class Trove(Package):

    pass

class PackageChangeSet:

    """
    Represents the changes between two packages and forms part of a
    ChangeSet. 
    """

    def isAbsolute(self):
	return self.absolute

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

    def changeOldVersion(self, version):
	self.oldVersion = version

    def changeNewVersion(self, version):
	self.newVersion = version

    def getOldVersion(self):
	return self.oldVersion

    def getNewVersion(self):
	return self.newVersion

    # path and/or version can be None
    def changedFile(self, fileId, path, version):
	self.changedFiles.append((fileId, path, version))

    def getChangedFileList(self):
	return self.changedFiles

    def iterChangedPackages(self):
	return self.packages.iteritems()

    def newPackageVersion(self, name, version):
	"""
	Adds a version of a package which appeared in newVersion.

	@param name: name of the package
	@type name: str
	@param version: new version
	@type version: versions.Version
	"""

	if not self.packages.has_key(name):
	    self.packages[name] = []
	self.packages[name].append(('+', version))

    def updateChangedPackage(self, name, old, new):
	"""
	Removes package name, version old from the changed list and
	adds package name, version new to the list (with the same 
	change type).

	@param name: name of the package
	@type name: str
	@param old: version to remove from the changed list
	@type old: versions.VersionString
	@param new: version to add to the changed list
	@type new: versions.VersionString
	"""
	for (theName, list) in self.packages.iteritems():
	    if theName != name: continue
	    for (i, (change, ver)) in enumerate(list):
		if ver == old:
		    list[i] = (change, new)
		    return

    def oldPackageVersion(self, name, version):
	"""
	Adds a version of a package which appeared in oldVersion.

	@param name: name of the package
	@type name: str
	@param version: old version
	@type version: versions.Version
	"""
	if not self.packages.has_key(name):
	    self.packages[name] = []
	self.packages[name].append(('-', version))

    def formatToFile(self, changeSet, cfg, f):
	f.write("%s " % self.name)

	if self.isAbsolute():
	    f.write("absolute ")
	elif self.oldVersion:
	    f.write("from %s to " % self.oldVersion.asString(cfg.defaultbranch))
	else:
	    f.write("new ")

	f.write("%s\n" % self.newVersion.asString(cfg.defaultbranch))

	for (fileId, path, version) in self.newFiles:
	    f.write("\tadded %s (%s(.*)%s)\n" % (path, fileId[:6], fileId[-6:]))

	for (fileId, path, version) in self.changedFiles:
	    if path:
		f.write("\tchanged %s (%s(.*)%s)\n" % 
			(path, fileId[:6], fileId[-6:]))
	    else:
		f.write("\tchanged %s\n" % fileId)
	    change = changeSet.getFileChange(fileId)
	    print "\t\t%s" % " ".join(files.fieldsChanged(change))

	for fileId in self.oldFiles:
	    f.write("\tremoved %s(.*)%s\n" % (fileId[:6], fileId[-6:]))

	for name in self.packages.keys():
	    list = [ x[0] + x[1].asString(cfg.defaultbranch) for x in self.packages[name] ]
	    f.write("\t" + name + " " + " ".join(list) + "\n")

    def remapSinglePath(self, path, map, dict):
	# the first item in map remaps source packages, which are present
	# without a leading /
	newPath = path

	if path[0] != "/":
	    prefix = map[0][1] % dict
	    newPath = prefix + path
	else:
	    for (prefix, newPrefix) in map[1:]:
		if path.startswith(prefix):
		    newPath = newPrefix + path[len(prefix):]

	return newPath

    def remapPaths(self, map, dict):
	for filelist in (self.changedFiles, self.newFiles):
            for i, path in enumerate(filelist):
                if path[1]:
		    newPath = self.remapSinglePath(path[1], map, dict)
		    if newPath != path[1]:
			filelist[i] = (path[0], newPath, path[2])

    def freeze(self):
	"""
	Returns a string representation of this change set which can
	later be parsed by parse(). The representation begins with a
	header::

         ABS <name> <newversion>
         CS <name> <oldversion> <newversion>
         NEW <name> <newversion>

	It is followed by <linecount> lines, each of which specifies a
	new file, old file, removed file, or a change to the set of
	included packages. Each of these lines begins with a "+", "-",
	"~", or "p" respectively. Following that are <diffcount> (possibly
	0) lines which make up the diff for the group file.

	@rtype: string
	"""

	rc = []
	lines = 0

	for id in self.getOldFileList():
	    rc.append("-%s\n" % id)

	for (id, path, version) in self.getNewFileList():
	    rc.append("+%s %s %s\n" % (id, path, version.freeze()))

	for (id, path, version) in self.getChangedFileList():
	    rc.append("~%s " % id)
	    if path:
		rc.append(path)
	    else:
		rc.append("-")

	    if version:
		rc.append(" " + version.freeze() + "\n")
	    else:
		rc.append(" -\n")

	lines = []
	for name in self.packages.keys():
	    list = [ x[0] + x[1].freeze() for x in self.packages[name] ]
	    lines.append("p " + name + " " + " ".join(list))

	if lines:
	    rc.append("\n".join(lines) + "\n")
        rc = "".join(rc)

	if self.absolute:
	    hdr = "ABS %s %s\n" % \
		      (self.name, self.newVersion.freeze())
	elif not self.oldVersion:
	    hdr = "NEW %s %s\n" % \
		      (self.name, self.newVersion.freeze())
	else:
	    hdr = "CS %s %s %s\n" % \
		      (self.name, self.oldVersion.freeze(), 
		       self.newVersion.freeze())

	return hdr + rc

    def __init__(self, name, oldVersion, newVersion, absolute = 0):
	self.name = name
	self.oldVersion = oldVersion
	self.newVersion = newVersion
	self.newFiles = []
	self.oldFiles = []
	self.changedFiles = []
	self.absolute = absolute
	self.packages = {}

class ThawPackageChangeSet(PackageChangeSet):

    def parse(self, line):
	action = line[0]

	if action == "+" or action == "~":
	    (fileId, path, version) = line[1:].split()

	    if version == "-":
		version = None
	    else:
		version = versions.ThawVersion(version)

	    if path == "-":
		path = None

	    if action == "+":
		self.newFile(fileId, path, version)
	    else:
		self.changedFile(fileId, path, version)
	elif action == "-":
	    self.oldFile(line[1:])
	elif action == "p":
	    fields = line[2:].split()
	    name = fields[0]
	    for item in fields[1:]:
		op = item[0]
		v = versions.ThawVersion(item[1:])

		assert(op == "+" or op == "-")

		if op == "+":
		    self.newPackageVersion(name, v)
		else: # op == "-"
		    self.oldPackageVersion(name, v)
	
	# this makes our order match the order in the changeset
	self.changedFiles.sort()

    def __init__(self, lines):
	header = lines[0]

	l = header.split()

	pkgType = l[0]
	pkgName = l[1]

	if pkgType == "CS":
	    oldVersion = versions.ThawVersion(l[2])
	    rest = 3
	elif pkgType == "NEW" or pkgType == "ABS":
	    oldVersion = None
	    rest = 2
	else:
	    raise IOError, "invalid line in change set %s" % file

	newVersion = versions.ThawVersion(l[rest])

	PackageChangeSet.__init__(self, pkgName, oldVersion, newVersion,
			          absolute = (pkgType == "ABS"))

	for line in lines:
	    self.parse(line)

class PackageFromFile(Package):

    def __init__(self, name, dataFile, version):
	"""
	Initializes a PackageFromFile() object.

	@param name: Fully qualified name of the package 
	@type name: str
	@param dataFile: File representation of a package
	@type dataFile: file-type object
	@param version: Fully qualified version of the package
	@type version: versions.Version()
	"""

	Package.__init__(self, name, version)
	self.read(dataFile)

class TroveFromFile(PackageFromFile):

    pass

def walkPackageSet(repos, pkg):
    """
    Generator returns all of the packages included by pkg, including
    pkg itself.
    """
    yield pkg
    seen = { pkg.getName() : [ pkg.getVersion().asString() ] }

    list = []
    for (name, version) in pkg.iterPackageList():
	list.append((name, version))

    while list:
	(name, version) = list[0]
	del list[0]

	if seen.has_key(name):
	    match = False
	    for ver in seen[name]:
		if version == ver:
		    match = True
		    break
	    if match: continue

	    seen[name].append(version)
	else:
	    seen[name] = [ version ]

	pkg = repos.getPackageVersion(name, version)

	yield pkg

	for (pkg, version) in pkg.iterPackageList():
	    list.append((pkg, version))

class PackageError(Exception):

    """
    Ancestor for all exceptions raised by the package module.
    """

    pass

class ParseError(PackageError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

class PatchError(PackageError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

