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

Group files can contain comment lines (which begin with #), and the first
two lines should read:

    name NAME
    version VERSION

Where the name is a simple package name (not fully qualified) and the
version is just a version/release.
"""

import log
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
	"""
	str = "%d\n" % len(self.packages.keys())
	for pkg in self.packages.keys():
	    str += pkg + " " +  \
		   " ".join([v.asString() for v in self.packages[pkg]]) + \
		   "\n"

	str += "".join(self.spec)

	return str

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
	    self.name = "unknown"
	else:
	    self.name = lines[0][1][1]
	    if not self.name.startswith("group-"):
		log.error('group names must begin with "group-"')
		errors = 1
	    self.name = self.name[6:]
	    if self.name[0] != ":":
		self.name = packageNamespace + ":" + self.name

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

		    self.packages[name] = versionList

		print "a:", [ x.asString() for x in versionList ]
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

		self.packages[name] = [ version ]

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

	self.spec = lines[i + 1:]

    def __init__(self, name, f, version):
	"""
	Initializes a GroupFromFile() object.

	@param name: Fully qualified name of the group (w/o the group- bit)
	@type name: str
	@param f: File representation of a group
	@type f: file-type object
	@param version: Fully qualified version of the group
	@type version: versions.Version()
	"""

	Group.__init__(self)
	self.version = version
	self.name = name
	self.parseGroup(f)

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

