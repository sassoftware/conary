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

    def getName(self):
	return self.name

    def getVersion(self):
	return self.version

    def formatString(self):
	"""
	Returns a string representing everything about this group, which
	can later be read by the GroupFromFile object. The format of
	the string is:

	NAME
	VERSION
	package count
	PACKAGE1 VERSION1
	PACKAGE2 VERSION2
	.
	.
	.
	PACKAGEN VERSIONN
	group file count
	GROUP FILE
	"""
	str = "%s\n%s\n%d\n" % (self.name, self.version.asString(),
				len(self.packages.keys()))
	for pkg in self.packages.keys():
	    str += pkg + " " +  \
		   " ".join([v.asString() for v in self.packages[pkg]]) + \
		   "\n"

	str += "%d\n%s" % (len(self.spec), "".join(self.spec))

	return str

    def setVersion(self, ver):
	"""
	Sets the version of the group.

	@param ver: The version for this group
	@type ver: versions.Version
	"""
	self.version = ver

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

	if lines[1][1][0] != "version":
	    log.error("group files must contain the version on the first line")
	    errors = 1
	else:
	    simpleVersion = lines[1][1][1]

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

		versionList = repos.getPackageNickList(name, nick)
		if not versionList:
		    log.error("branch %s does not exist for package %s"
				% (str(nick), name))
		    errors = 1
		else:
		    self.packages[name] = versionList
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
	    return None

	return simpleVersion

    def __init__(self):
	self.packages = {}
	self.spec = None

