#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
A group is specified by a file which contains package names, white space,
and a version. If the package name isn't fully qualified, it's assumed to
be part of the same repository the group file is from. The version can
be either fully qualified or a branch nickname, in which case if refers
to the head of the branch at the time the group file is added to a 
repository. 

Group files are parsed into group objects, which resolve the package and
name as specified above; the original group file is preserved for future
modification. This also allows a group file to be checked out and back
in again, and have it updated with new head versions of it's components.
Some groups (notably ones derived from recipies) exist only in their
parsed forms; these groups cannot be checked in or out.

Group files can contain comment lines (which begin with #), and the first
two lines should read::

    name NAME
    version VERSION

Where the name is a simple package name (not fully qualified) and the
version is just a version/release.
"""

import difflib
import log
import patch
import versions

class Group:

    """
    Representation of a group of packages.
    """

    def getName(self):
	"""
	Returns the fully qualified name of the group.

	@rtype: str
	"""
	return self.name

    def getVersion(self):
	"""
	Returns the fully qualified version of the group.

	@rtype: versions.Version
	"""
	return self.version

    def getPackageList(self):
	"""
	Returns a list of (packageName, versionList) ordered pairs, listing
	all of the package in the group, along with their versions. 

	@rtype: list
	"""
	return self.packages.items()

    def addPackage(self, name, versionList):
	"""
	Adds a set of versions for a package.

	@param name: name of the package
	@type name: str
	@name: versionList
	@type: list of versions.Version
	"""
	self.packages[name] = versionList

    def addPackageVersion(self, name, version):
	"""
	Adds a single version of a package.

	@param name: name of the package
	@type name: str
	@name: version
	@type: versions.Version
	"""
	if self.packages.has_key(name):
	    self.packages[name].append(version)
	else:
	    self.packages[name] = [ version ]

    def setName(self, name):
	"""
	Sets the name of a group. Group names are of the form :repos:name

	@param name: The new name
	@type name: str
	"""
	self.name = name

    def applyChangeSet(self, cs):
	"""
	Updates the group from the changes specified in a change set.

	@param cs: change set
	@type cs: GroupChangeSet
	"""

	if cs.getPatch():
	    if not self.spec:
		old = []
	    else:
		old = self.spec.split('\n')

	    (new, failed) = patch.patch(old, cs.getPatch().split('\n'))
	    if failed:
		# FIXME. something smarter needs to happen here, but it's
		# not clear what as this really shouldn't happen
		raise IOError
	
	    self.spec = "\n".join(new)

	for (name, list) in cs.packages.items():
	    for (oper, version) in list:
		if oper == '+':
		    self.addPackageVersion(name, version)
		elif oper == "-":
		    for i, ver in enumerate(self.packages[name]):
			if ver.equal(version): break
		    if i == len(self.packages[name]):
			# FIXME, this isn't the right thing to do
			raise IOError
		    del(self.packages[i])

    def formatString(self):
	"""
	Returns a string representing everything about this group, which
	can later be read by the GroupFromFile object. The format of
	the string is:

	package count
	PACKAGE1 VERSION1
	PACKAGE2 VERSION2
	.
	.
	.
	PACKAGEN VERSIONN
	GROUP FILE

	Group file may be empty, in which case nothing follows the newline
	for the final file package entry.

	"""
	str = "%d\n" % len(self.packages.keys())
	for pkg in self.packages.keys():
	    str += pkg + " " +  \
		   " ".join([v.asString() for v in self.packages[pkg]]) + \
		   "\n"

	if self.spec:
	    str += "".join(self.spec)

	return str

    def diff(self, them, abstract = False):
	"""
	Generates a change set between them (considered the old version)
	and this instance.

	@param them: object to generate a change set from (may be None)
	@type them: Group
	@param abstract: tells if this is a new group or an abstract change
	when them is None
	@type abstract: boolean
	@rtype; ChangeSetGroup
	"""
	if them:
	    assert(self.__class__ == them.__class)
	    assert(self.name == them.name)
	    cs = GroupChangeSet(self.name, them.getVersion(), self.getVersion())
	else:
	    cs = GroupChangeSet(self.name, None, self.getVersion(), 
				abstract = abstract)

	if them:
	    diff = difflib.unified_diff(self.spec, them.spec, "old", "new")
	else:
	    diff = difflib.unified_diff(self.spec, "", "old", "new")

	cs.setPatch(diff)

	names = {}
	list = self.packages.keys()
	if them:
	    list += them.packages.keys()

	for name in list:
	    names[name] = 1

	for name in names.keys():
	    if self.packages.has_key(name):
		ourVersions = self.packages[name]
	    else:
		ourVersions = None

	    if them and them.packages.has_key(name):
		theirVersions = them.packages[name]
	    else:
		theirVersions = []

	    for (i, version) in enumerate(ourVersions):
		match = 0 
		for (j, v) in enumerate(theirVersions):
		    if v.equal(version):
			match = 1
			break

		if match:
		    # same version exists in both groups
		    del theirVersions[j]
		else:
		    # this is a new package
		    cs.newPackageVersion(name, version)

	    for version in theirVersions:
		cs.oldPackageVersion(name, version)

	return cs

    def setVersion(self, ver):
	"""
	Sets the version of the group.

	@param ver: The version for this group
	@type ver: versions.Version
	"""
	self.version = ver

    def __init__(self):
	self.packages = {}
	self.spec = None

class GroupFromTextFile(Group):

    def getSimpleVersion(self):
	"""
	Returns the version string defined in a group file.

	@rtype: str
	"""
	return self.simpleVersion

    def parseFile(self, f, packageNamespace, repos):
	"""
	Parses a group file into a group object. Any existing contents
	of the group object are erased. Any errors which occur are
	logged, and if the file has not been parsed properly False is
	return; if parsing is successful the simple version number from
	the group file is returned as a string.
	
	@param f: The file to be parsed
	@type f: file-like object
	@param packageNamespace: The name of the repository to prepend
	to package names which are not fully qualified; this should begin
	with a colon.
	@type packageNamespace: str
	@param repos: Branch nicknames are turned into fully qualified
	version numbers by looking them up in repos.
	@type repos: Repository
	@rtype: str
	"""
	self.spec = f.readlines()

	lines = []
	errors = 0
	for i, line in enumerate(self.spec):
	    line = line.strip()
	    if line and line[0] != '#': 
		fields = line.split()
		if len(fields) != 2:
		    log.error("line %d of group file has too many fields" % i)
		    errors = 1
		lines.append((i, fields))

	if lines[0][1][0] != "name":
	    log.error("group files must contain the group name on the first line")
	    errors = 1
	    name = "localhost:unknown"
	else:
	    name = lines[0][1][1]
	    if name[0] != ":":
		name = packageNamespace + ":" + name
	    else:
		name = name

	    if name.count(":") != 2:
		log.error("group names may not include colons")
		errors = 1
		name = "localhost:unknown"

	self.setName(name)

	if lines[1][1][0] != "version":
	    log.error("group files must contain the version on the first line")
	    errors = 1
	else:
	    self.simpleVersion = lines[1][1][1]

	for lineNum, (name, versionStr) in lines[2:]:
	    if name[0] != ":":
		name = packageNamespace + ":" + name

	    if versionStr[0] != "/":
		try:
		    nick = versions.BranchName(versionStr)
		except versions.ParseError:
		    log.error("invalid version on line %d: %s" % (lineNum, 
			      versionStr))
		    errors = 1
		    continue

		branchList = repos.getPackageNickList(name, nick)
		if not branchList:
		    log.error("branch %s does not exist for package %s"
				% (str(nick), name))
		    errors = 1
		else:
		    versionList = []
		    for branch in branchList:
			ver = repos.pkgLatestVersion(name, branch)
			versionList.append(ver)

		    self.addPackage(name, versionList)
	    else:
		try:
		    version = versions.VersionFromString(versionStr)
		except versions.ParseError:
		    log.error("invalid version on line %d: %s" % (lineNum, 
			      versionStr))
		    errors = 1
		    continue

		if not version.isVersion():
		    log.error("fully qualified branches may not be used " +
			      "as version on line %d" % lineNum)

		self.addPackage(name, [ version ])

	if errors:
	    raise ParseError

    def __init__(self, f, packageNamespace, repos):
	"""
	Initializes the object; parameters are the same as those
	to parseFile().
	"""

	Group.__init__(self)
	self.parseFile(f, packageNamespace, repos)

class GroupFromFile(Group):

    """
    Creates a group from a file which contains the format described
    in the comments for Group.formatString()
    """

    def parseGroup(self, f):
	"""
	Initializes the object from the data in f

	@param f: File representation of a group
	@type f: file-type object
	"""
	lines = f.readlines()
	pkgCount = int(lines[0][:-1])
	for i in range(1, pkgCount + 1):
	    line = lines[i]
	    items = line.split()
	    name = items[0]
	    self.packages[name] = []
	    for versionStr in items[1:]:
		version = versions.VersionFromString(versionStr)
		self.packages[name].append(version)
	
	if (i + 1) < len(lines):
	    self.spec = lines[i + 1:]
	else:
	    self.spec = None

    def __init__(self, name, f, version):
	"""
	Initializes a GroupFromFile() object.

	@param name: Fully qualified name of the group 
	@type name: str
	@param f: File representation of a group
	@type f: file-type object
	@param version: Fully qualified version of the group
	@type version: versions.Version()
	"""

	Group.__init__(self)
	self.version = version
	self.setName(name)
	self.parseGroup(f)

class GroupChangeSet:
    """
    Represents the changes between two groups and forms part of a
    ChangeSet. Changes for the parsed information have a structured
    representation while the changes for the input file are generated
    via patch.
    """

    def getName(self):
	"""
	@rtype: string
	"""
	return self.name

    def isAbstract(self):
	"""
	@rtype: boolean
	"""
	return self.isAbstract

    def getOldVersion(self):
	"""
	@rtype: versions.Version
	"""
	return self.oldVersion

    def getNewVersion(self):
	"""
	@rtype: versions.Version
	"""
	return self.newVersion

    def setPatch(self, patch):
	"""
	Adds the patch beween the input files for groups

	@param patch: the patch
	@type patch: str
	"""
	self.specPatch = patch

    def getPatch(self):
	"""
	Returns the patch between the input files.

	@rtype: str
	"""

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

    def formatToFile(self, cfg, f):
	"""
	Writes a human-readable representation of this change set.

	@rtype: str
	"""
	f.write("group %s " % self.name)
	if self.isAbstract():
	    f.write("abstract ")
	elif self.oldVersion:
	    f.write("from %s to " % self.oldVersion.asString(cfg.defaultbranch))
	else:
	    f.write("new ")
	f.write("%s\n" % self.newVersion.asString(cfg.defaultbranch))

	for name in self.packages.keys():
	    list = [ x[0] + x[1].asString(cfg.defaultbranch) for x in self.packages[name] ]
	    f.write("\t" + name + ": " + " ".join(list) + "\n")

    def freeze(self):
	"""
	Returns a string representation of this change set which can
	later be parsed by parse(). The representation is formatted
	as follows:

	HEADER
	<pkgname> <[+-]version>+
	<specdiff>

	Header is one of the following:
	    SRS GRP ABSTRACT <name> <newversion> <pkgcount>
	    SRS GRP CHANGESET <name> <oldversion> <newversion> <pkgcount>
	    SRS GRP NEW <name> <newversion> <pkgcount>

	@rtype: string
	"""

	if self.abstract:
	    hdr = "SRS GRP ABSTRACT %s %s %d\n" % \
		      (self.name, self.newVersion.freeze(), 
		       len(self.packages.keys()))
	elif not self.oldVersion:
	    hdr = "SRS GRP NEW %s %s %d\n" % \
		      (self.name, self.newVersion.freeze(),
                       len(self.packages.keys()))
	else:
	    hdr = "SRS GRP CHANGESET %s %s %s %d\n" % \
		      (self.name, self.oldVersion.freeze(), 
		       self.newVersion.freeze(), 
		       len(self.packages.keys()))

	lines = []
	for name in self.packages.keys():
	    list = [ x[0] + x[1].freeze() for x in self.packages[name] ]
	    lines.append(name + " " + " ".join(list))

	return hdr + "\n".join(lines) + "\n"

    def parse(self, line):
	"""
	Parses a single line from the frozen version of a group change
	set and updates this change set appropriately.
	
	@param line: line to parse
	@type line: str
	"""
	print "---", line
	fields = line.split()
	name = fields[0]
	verList = []
	for item in fields[1:]:
	    op = item[0]
	    v = versions.ThawVersion(item[1:])
	    verList.append((op, v))

	self.packages[name] = verList

    def __init__(self, name, oldVersion, newVersion, abstract = False):
	"""
	Initializes the object.

	@param name: Name of the group
	@type param: string or packagename.PackageName
	@param oldVersion: Version which this diff is from, or None
	@type oldVersion: versions.Version
	@param newVersion: Version which this diff is to
	@type newVersion: versions.Version
	@param abstract: If oldVersion is None, specified whether this
	represents an abstract group change or a new group.
	@type abstract: boolean
	"""
	self.name = name
	self.oldVersion = oldVersion
	self.newVersion = newVersion
	self.abstract = abstract
	self.packages = {}

class GroupError(Exception):

    """
    Ancestor for all exceptions raised by the group module.
    """

    pass

class ParseError(GroupError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

