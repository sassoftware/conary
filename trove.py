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
"""
Implements troves (packages, components, etc.) for the repository
"""

import changelog
import copy
import files
import versions
from deps import deps

class Trove:
    """
    Packages are groups of files and other packages, which are included by
    reference. By convention, "package" often refers to a package with
    files but no other packages, while a "group" means a package with other
    packages but no files. While this object allows any mix of file and
    package inclusion, in practice conary doesn't allow it.
    """
    def copy(self):
	return copy.deepcopy(self)

    def getName(self):
        return self.name
    
    def getVersion(self):
        return self.version
    
    def changeVersion(self, version):
        self.version = version

    def changeFlavor(self, flavor):
        self.flavor = flavor

    def addFile(self, fileId, path, version):
	self.idMap[fileId] = (path, version)

    # fileId is the only thing that must be here; the other fields could
    # be None
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

    def addTrove(self, name, version, flavor, presentOkay = False):
	"""
	Adds a single version of a package.

	@param name: name of the package
	@type name: str
	@param version: version of the package
	@type version: versions.Version
	@param flavor: flavor of the package to include
	@type flavor: deps.deps.DependencySet
	@param presentOkay: replace if this is a duplicate, don't complain
	@type presentOkay: boolean
	"""
	if not presentOkay and self.packages.has_key((name, version, flavor)):
	    raise TroveError, "duplicate trove included in %s" % self.name
	self.packages[(name, version, flavor)] = True

    def delTrove(self, name, version, flavor, missingOkay):
	"""
	Removes a single version of a package.

	@param name: name of the package
	@type name: str
	@param version: version of the package
	@type version: versions.Version
	@param flavor: flavor of the package to include
	@type flavor: deps.deps.DependencySet
	@param missingOkay: should we raise an error if the version isn't
	part of this trove?
	@type missingOkay: boolean
	"""
	if self.packages.has_key((name, version, flavor)):
	    del self.packages[(name, version, flavor)]
	elif missingOkay:
	    pass
	else:
	    # FIXME, we should have better text here
	    raise TroveError

    def iterTroveList(self):
	"""
	Returns a generator for (packageName, version, flavor) ordered pairs, 
	listing all of the package in the group, along with their versions. 

	@rtype: list
	"""
	return self.packages.iterkeys()

    def hasTrove(self, name, version, flavor):
	return self.packages.has_key((name, version, flavor))

    def readFileList(self, dataFile):
        line = dataFile.next()
	fileCount = int(line)

        for line in dataFile:
	    fields = line.split()
	    fileId = fields.pop(0)
	    version = fields.pop(-1)
	    path = " ".join(fields)

	    version = versions.VersionFromString(version)
	    self.addFile(fileId, path, version)

    def freezeFileList(self):
	"""
	Returns a string representing file information for this trove
	trove, which can later be read by the read() method. This is
	only used to create the Conary control file when dealing with
	:source component checkins, so things like trove dependency
	information is not needed.  The format of the string is:

	<file count>
	FILEID1 PATH1 VERSION1
	FILEID2 PATH2 VERSION2
	.
	.
	.
	FILEIDn PATHn VERSIONn
	"""
        assert(len(self.packages) == 0)
        rc = []
	rc.append("%d\n" % (len(self.idMap)))
        rc += [ "%s %s %s\n" % (x[0], x[1][0], x[1][1].asString())
                for x in self.idMap.iteritems() ]
	return "".join(rc)

    # returns a dictionary mapping a fileId to a (path, version, pkgName) tuple
    def applyChangeSet(self, pkgCS):
	"""
	Updates the package from the changes specified in a change set.
	Returns a dictionary, indexed by fileId, which gives the
	(path, version, packageName) for that file.

	@param pkgCS: change set
	@type pkgCS: TroveChangeSet
	@rtype: dict
	"""

	fileMap = {}

	for (fileId, path, fileVersion) in pkgCS.getNewFileList():
	    self.addFile(fileId, path, fileVersion)
	    fileMap[fileId] = self.idMap[fileId] + (self.name, None, None)

	for (fileId, path, fileVersion) in pkgCS.getChangedFileList():
	    (oldPath, oldVersion) = self.idMap[fileId]
	    self.updateFile(fileId, path, fileVersion)
	    # look up the path/version in self.idMap as the ones here
	    # could be None
	    fileMap[fileId] = self.idMap[fileId] + (self.name, oldPath, oldVersion)

	for fileId in pkgCS.getOldFileList():
	    self.removeFile(fileId)

	self.mergeTroveListChanges(pkgCS.iterChangedTroves())
	self.flavor = pkgCS.getNewFlavor()
	self.changeLog = pkgCS.getChangeLog()
	self.setProvides(pkgCS.getProvides())
	self.setRequires(pkgCS.getRequires())
	self.changeVersion(pkgCS.getNewVersion())
	self.changeFlavor(pkgCS.getNewFlavor())

	return fileMap

    def mergeTroveListChanges(self, changeList, redundantOkay = False):
	"""
	Merges a set of changes to the included package list into this
	package.

	@param changeList: A list or generator specifying a set of
	package changes; this is the same as returned by
	TroveChangeSet.iterChangedTroves()
	@type changeList: (name, list) tuple
	@param redundantOkay: Redundant changes are normally considered errors
	@type redundantOkay: boolean
	"""

	for (name, list) in changeList:
	    for (oper, version, flavor) in list:
		if oper == '+':
		    self.addTrove(name, version, flavor,
					   presentOkay = redundantOkay)

		elif oper == "-":
		    self.delTrove(name, version, flavor,
					   missingOkay = redundantOkay)
    
    def __eq__(self, them):
	"""
	Compare two troves for equality. This is an expensive operation,
	and shouldn't really be done. It's handy for testing the database
	though.
	"""
        if them is None:
            return False
	if self.getName() != them.getName():
	    return False
	if self.getVersion() != them.getVersion():
	    return False
	if self.getFlavor() != them.getFlavor():
	    return False

	(csg, pcl, fcl) = self.diff(them)
	return (not pcl) and (not fcl) and (not csg.getOldFileList())

    def __ne__(self, them):
	return not self == them

    def diff(self, them, absolute = 0):
	"""
	Generates a change set between them (considered the old version) and
	this instance. We return the change set, a list of other package diffs
	which should be included for this change set to be complete, and a list
	of file change sets which need to be included.  The list of package
	changes is of the form (pkgName, oldVersion, newVersion, oldFlavor,
	newFlavor).  If absolute is True, oldVersion is always None and
	absolute diffs can be used.  Otherwise, absolute versions are not
	necessary, and oldVersion of None means the package is new. The list of
	file changes is a list of (fileId, oldVersion, newVersion, oldPath,
	newPath) tuples, where newPath is 
	the path to the file in this package.

	@param them: object to generate a change set from (may be None)
	@type them: Group
	@param absolute: tells if this is a new group or an absolute change
	when them is None
	@type absolute: boolean
	@rtype: (TroveChangeSet, fileChangeList, troveChangeList)
	"""

	assert(not them or self.name == them.name)

	# find all of the file ids which have been added, removed, and
	# stayed the same
	if them:
	    themMap = them.idMap
	    chgSet = TroveChangeSet(self.name, self.changeLog,
				      them.getVersion(),	
				      self.getVersion(),
				      them.getFlavor(), self.getFlavor(),
				      absolute = False)
	else:
	    themMap = {}
	    chgSet = TroveChangeSet(self.name, self.changeLog,
				      None, self.getVersion(),
				      None, self.getFlavor(),
				      absolute = absolute)

	# dependency and flavor information is always included in total;
	# this lets us do dependency checking w/o having to load packages
	# on the client
	chgSet.setRequires(self.requires)
	chgSet.setProvides(self.provides)

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
	    filesNeeded.append((id, None, selfVersion, None, selfPath))
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
		filesNeeded.append((id, themVersion, selfVersion, themPath, 
				    selfPath))

	    if newPath or newVersion:
		chgSet.changedFile(id, newPath, newVersion)

	# now handle the packages we include
	added = {}
	removed = {}

	for key in self.packages.iterkeys():
	    if them and them.packages.has_key(key): continue

	    (name, version, flavor) = key
	    chgSet.newTroveVersion(name, version, flavor)

	    if not added.has_key(name):
		added[name] = {}

	    if added[name].has_key(flavor):
		added[name][flavor].append(version)
	    else:
		added[name][flavor] = [ version ]

	if them:
	    for key in them.packages.iterkeys():
		if self.packages.has_key(key): continue

		(name, version, flavor) = key
		chgSet.oldTroveVersion(name, version, flavor)
		if not removed.has_key(name):
		    removed[name] = {}

		if removed[name].has_key(flavor):
		    removed[name][flavor].append(version)
		else:
		    removed[name][flavor] = [ version ]

	pkgList = []

	if absolute:
	    for name in added.keys():
		for flavor in added[name]:
		    for version in added[name][flavor]:
			pkgList.append((name, None, version, None, flavor))
	    return (chgSet, filesNeeded, pkgList)

	# use added and removed to assemble a list of package diffs which need
	# to go along with this change set

	for name in added.keys(): 
	    if not removed.has_key(name):
		# there isn't anything which disappeared that has the same
		# name; this must be a new addition
		for newFlavor in added[name]:
		    for version in added[name][newFlavor]:
			pkgList.append((name, None, version, None, newFlavor))

		del added[name]

	changePair = []
	# we know everything added now has a matching name in removed; let's
	# try and match up the flavors. first of all we'll look for exact
	# matches
	for name in added.keys():
	    for newFlavor in added[name].keys():
		if removed[name].has_key(newFlavor):
		    # we have a name/flavor match
		    changePair.append((name, added[name][newFlavor], newFlavor,
				       removed[name][newFlavor], newFlavor))
		    del added[name][newFlavor]
		    del removed[name][newFlavor]

	    if not added[name]:
		del added[name]
	    if not removed[name]:
		del removed[name]

	# for things that are left, see if we can match flavors based on
	# the architecture
	for name in added.keys():
	    for newFlavor in added[name].keys():
		if not newFlavor:
		    # this isn't going to match anything well
		    continue

		match = None

		# first check for matches which are a superset of the old
		# flavor, then for ones which are a subset of the old flavor
		for oldFlavor in removed[name].keys():
		    if not oldFlavor:
			continue

		    if newFlavor.satisfies(oldFlavor):
			match = removed[name][oldFlavor]
			del removed[name][oldFlavor]
			break

		if match:
		    changePair.append((name, added[name][newFlavor], newFlavor, 
				       match, oldFlavor))
		    del added[name][newFlavor]
		    continue

		for oldFlavor in removed[name].keys():
		    if not oldFlavor:
			continue

		    if oldFlavor.satisfies(newFlavor):
			match = removed[name][oldFlavor]
			del removed[name][oldFlavor]
			break

		if match:
		    changePair.append((name, added[name][newFlavor], newFlavor, 
				       removed[name][oldFlavor], oldFlavor))
		    del added[name][newFlavor]

	    if not added[name]:
		del added[name]
	    if not removed[name]:
		del removed[name]
	    
	for name in added.keys():
	    if len(added[name]) == 1 and len(removed[name]) == 1:
		# one of each? they *must* be a good match...
		newFlavor = added[name].keys()[0]
		oldFlavor = removed[name].keys()[0]
		changePair.append((name, added[name][newFlavor], newFlavor, 
				   removed[name][oldFlavor], oldFlavor))
		del added[name]
		del removed[name]
		continue

	for name in added.keys():
	    for newFlavor in added[name].keys():
		# no good match. that's too bad
		changePair.append((name, added[name][newFlavor], newFlavor, 
				   None, None))

	for (name, newVersionList, newFlavor, oldVersionList, oldFlavor) \
		    in changePair:
	    assert(newVersionList)

	    if not oldVersionList:
		for newVersion in newVersionList:
		    pkgList.append((name, None, newVersion, 
					  None, newFlavor))

	    # for each new version of a package, try and generate the diff
	    # between that package and the version of the package which was
	    # removed which was on the same branch. if that's not possible,
	    # see if the parent of the package was removed, and use that as
	    # the diff. if we can't do that and only one version of this
	    # package is being obsoleted, use that for the diff. if we
	    # can't do that either, throw up our hands in a fit of pique
	    
	    for version in newVersionList:
		branch = version.branch()
		if version.hasParent():
		    parent = version.parent()
		else:
		    parent = None

		if len(oldVersionList) == 1:
		    pkgList.append((name, oldVersionList[0], version, 
				    oldFlavor, newFlavor))
		else:
		    sameBranch = None
		    parentNode = None

		    for other in oldVersionList:
			if other.branch() == branch:
			    sameBranch = other
			if parent and other == parent:
			    parentNode = other

		    if sameBranch:
			pkgList.append((name, sameBranch, version, 
					oldFlavor, newFlavor))
		    elif parentNode:
			pkgList.append((name, parentNode, version, 
					oldFlavor, newFlavor))
		    else:
			# Here's the fit of pique. This shouldn't happen
			# except for the most ill-formed of groups.
			raise IOError, "ick. yuck. blech. ptooey."

	return (chgSet, filesNeeded, pkgList)

    def setProvides(self, provides):
        self.provides = provides

    def setRequires(self, requires):
        self.requires = requires

    def getProvides(self):
        return self.provides

    def getRequires(self):
        return self.requires

    def getFlavor(self):
        return self.flavor

    def getChangeLog(self):
        return self.changeLog

    def __init__(self, name, version, flavor, changeLog):
	self.idMap = {}
	self.name = name
	self.version = version
	self.flavor = flavor
	self.packages = {}
        self.provides = None
        self.requires = None
	self.changeLog = changeLog

class TroveChangeSet:

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

    def getChangeLog(self):
	return self.changeLog

    def changeOldVersion(self, version):
	self.oldVersion = version

    def changeNewVersion(self, version):
	self.newVersion = version

    def changeChangeLog(self, cl):
	self.changeLog = cl

    def getOldVersion(self):
	return self.oldVersion

    def getNewVersion(self):
	return self.newVersion

    # path and/or version can be None
    def changedFile(self, fileId, path, version):
	self.changedFiles.append((fileId, path, version))

    def getChangedFileList(self):
	return self.changedFiles

    def iterChangedTroves(self):
	return self.packages.iteritems()

    def newTroveVersion(self, name, version, flavor):
	"""
	Adds a version of a package which appeared in newVersion.

	@param name: name of the package
	@type name: str
	@param version: new version
	@type version: versions.Version
	@param flavor: new flavor
	@type flavor: deps.deps.DependencySet
	"""

	if not self.packages.has_key(name):
	    self.packages[name] = []
	self.packages[name].append(('+', version, flavor))

    def updateChangedPackage(self, name, flavor, old, new):
	"""
	Removes package (name, flavor, old version) from the changed list and
	adds package (name, flavor, version) new to the list (with the same 
	change type).

	@param name: name of the package
	@type name: str
	@param flavor: flavor of the package
	@type flavor: deps.deps.DependencySet
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

    def oldTroveVersion(self, name, version, flavor):
	"""
	Adds a version of a package which appeared in oldVersion.

	@param name: name of the package
	@type name: str
	@param version: old version
	@type version: versions.Version
	@param flavor: old flavor
	@type flavor: deps.deps.DependencySet
	"""
	if not self.packages.has_key(name):
	    self.packages[name] = []
	self.packages[name].append(('-', version, flavor))

    def formatToFile(self, changeSet, f):
	f.write("%s " % self.name)

	if self.isAbsolute():
	    f.write("absolute ")
	elif self.oldVersion:
	    f.write("from %s to " % self.oldVersion.asString())
	else:
	    f.write("new ")

	f.write("%s\n" % self.newVersion.asString())

        def depformat(name, dep, f):
            f.write('\t%s: %s\n' %(name,
                                   str(dep).replace('\n', '\n\t%s'
                                                    %(' '* (len(name)+2)))))
        if self.requires:
            depformat('Requires', self.requires, f)
        if self.provides:
            depformat('Provides', self.provides, f)
        if self.oldFlavor:
            depformat('Old Flavor', self.oldFlavor, f)
        if self.newFlavor:
            depformat('New Flavor', self.newFlavor, f)

	for (fileId, path, version) in self.newFiles:
	    #f.write("\tadded (%s(.*)%s)\n" % (fileId[:6], fileId[-6:]))
            change = changeSet.getFileChange(fileId)
            fileobj = files.ThawFile(change, fileId)
            
	    if isinstance(fileobj, files.SymbolicLink):
		name = "%s -> %s" % (path, fileobj.target.value())
	    else:
		name = path
	    
	    print "\t%s    1 %-8s %-8s %s %s %s" % \
                  (fileobj.modeString(), fileobj.inode.owner(),
                   fileobj.inode.group(), fileobj.sizeString(),
                   fileobj.timeString(), name)

	for (fileId, path, version) in self.changedFiles:
	    if path:
		f.write("\tchanged %s (%s(.*)%s)\n" % 
			(path, fileId[:6], fileId[-6:]))
	    else:
		f.write("\tchanged %s\n" % fileId)
	    change = changeSet.getFileChange(fileId)
	    f.write("\t\t%s\n" % " ".join(files.fieldsChanged(change)))

	for fileId in self.oldFiles:
	    f.write("\tremoved %s(.*)%s\n" % (fileId[:6], fileId[-6:]))

	for name in self.packages.keys():
	    list = [ x[0] + x[1].asString() for x in self.packages[name] ]
	    f.write("\t" + name + " " + " ".join(list) + "\n")

    def freeze(self):
	"""
	Returns a string representation of this change set which can
	later be parsed by parse(). The representation begins with a
	header::

         ABS <name> <newversion> ||
           CS <name> <oldversion> <newversion> ||
           NEW <name> <newversion>
         [REQUIRES <dep set>]
         [PROVIDES <dep set>]
         [FLAVOR <dep set>]
	 [CL <name> <email> <linecount>
	   <line count lines of change log>]
         
	The remainder of the lines, each specifies a new file, old file,
	removed file, or a change to the set of included packages. Each of
	these lines begins with a "+", "-", "~", or "p" respectively. 

	@rtype: string
	"""

	rc = []
	lines = 0

	if self.absolute:
            rc.append("ABS %s %s\n" % (self.name, self.newVersion.freeze()))
	elif not self.oldVersion:
	    rc.append("NEW %s %s\n" % (self.name, self.newVersion.freeze()))
	else:
	    rc.append("CS %s %s %s\n" % (self.name, self.oldVersion.freeze(),
                                         self.newVersion.freeze()))
        if self.requires:
            rc.append("REQUIRES %s\n" % (self.requires.freeze()))
        if self.provides:
            rc.append("PROVIDES %s\n" % (self.provides.freeze()))

        if self.oldFlavor and self.newFlavor:
            rc.append("FLAVOR %s %s\n" % (self.oldFlavor.freeze(),
					  self.newFlavor.freeze()))
	elif self.oldFlavor and not self.newFlavor:
            rc.append("FLAVOR %s -\n" % (self.oldFlavor.freeze()))
	elif not self.oldFlavor and self.newFlavor:
            rc.append("FLAVOR - %s\n" % (self.newFlavor.freeze()))

	if self.changeLog:
	    frz = self.changeLog.freeze()
	    rc.append("CL %d\n" % frz.count("\n"))
	    rc.append(frz)

	for id in self.getOldFileList():
	    rc.append("-%s\n" % id)

	for (id, path, version) in self.getNewFileList():
	    rc.append("+%s %s %s\n" % (id, path, version.asString()))

	for (id, path, version) in self.getChangedFileList():
	    rc.append("~%s " % id)
	    if path:
		rc.append(path)
	    else:
		rc.append("-")

	    if version:
		rc.append(" " + version.asString() + "\n")
	    else:
		rc.append(" -\n")

	lines = []
	for name in self.packages.keys():
	    list = []
	    for (kind, version, flavor) in self.packages[name]:
		if flavor:
		    list.append(kind + version.freeze() + 
				    "|" + flavor.freeze())
		else:
		    list.append(kind + version.freeze() + 
				    "|")

	    lines.append("p " + name + " " + " ".join(list))

	if lines:
	    rc.append("\n".join(lines) + "\n")

	return "".join(rc)

    def setProvides(self, provides):
        self.provides = provides

    def getProvides(self):
        return self.provides

    def setRequires(self, requires):
        self.requires = requires

    def getRequires(self):
        return self.requires

    def getOldFlavor(self):
        return self.oldFlavor

    def getNewFlavor(self):
        return self.newFlavor

    def __init__(self, name, changeLog, oldVersion, newVersion, 
		 oldFlavor, newFlavor, absolute = 0):
	assert(isinstance(newVersion, versions.AbstractVersion))
	assert(not newFlavor or isinstance(newFlavor, deps.DependencySet))
	assert(not oldFlavor or isinstance(oldFlavor, deps.DependencySet))
	self.name = name
	self.oldVersion = oldVersion
	self.newVersion = newVersion
	self.changeLog = changeLog
	self.newFiles = []
	self.oldFiles = []
	self.changedFiles = []
	self.absolute = absolute
	self.packages = {}
        self.provides = None
        self.requires = None
	self.oldFlavor = oldFlavor
	self.newFlavor = newFlavor

class ThawTroveChangeSet(TroveChangeSet):

    def parse(self, line):
        if line.startswith('REQUIRES '):
            dep = line.split(' ', 1)[1]
            self.setRequires(deps.ThawDependencySet(dep))
            return
        if line.startswith('PROVIDES '):
            dep = line.split(' ', 1)[1]
            self.setProvides(deps.ThawDependencySet(dep))
            return
        
	action = line[0]

	if action == "+" or action == "~":
	    fields = line[1:].split()
	    fileId = fields.pop(0)
	    version = fields.pop(-1)
	    path = " ".join(fields)

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
	elif action == "p":
	    fields = line[2:].split()
	    name = fields[0]
	    for item in fields[1:]:
		op = item[0]
		v,f = item[1:].split("|", 1)
		v = versions.ThawVersion(v)
		if f:
		    f = deps.ThawDependencySet(f)
		else:
		    f = None

		assert(op == "+" or op == "-")

		if op == "+":
		    self.newTroveVersion(name, v, f)
		else: # op == "-"
		    self.oldTroveVersion(name, v, f)
	
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

	# find the flavor
	oldFlavor = None
	newFlavor = None
	for i, l in enumerate(lines[1:4]):
	    if l.startswith("FLAVOR "):
		oldFlavorStr, newFlavorStr = l.split(' ', 2)[1:3]

		if oldFlavorStr != "-":
		    oldFlavor = deps.ThawDependencySet(oldFlavorStr)

		if newFlavorStr != "-":
		    newFlavor = deps.ThawDependencySet(newFlavorStr)

		del lines[i + 1]
		break

	# find the change log
	changeLog = None
	for i, l in enumerate(lines[1:5]):
	    if l.startswith("CL "):
		cnt = int(l.split(' ', 1)[1])
		first = i + 2
		changeLog = changelog.ThawChangeLog(lines[first:first + cnt])
		del lines[i: first + cnt]
                break

	TroveChangeSet.__init__(self, pkgName, changeLog, oldVersion, 
				newVersion, oldFlavor, newFlavor, 
				absolute = (pkgType == "ABS"))
        
	for line in lines:
	    self.parse(line)

class TroveError(Exception):

    """
    Ancestor for all exceptions raised by the package module.
    """

    pass

class ParseError(TroveError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

class PatchError(TroveError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

