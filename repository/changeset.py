import filecontainer
import files
import package
import string
import versions
import os

class ChangeSet:

    def getPackageList(self):
	return self.packages

    def getFileList(self):
	return self.files.items()

    def formatToFile(self, cfg, f):
	for pkg in self.packages:
	    pkg.formatToFile(self, cfg, f)
	    print

    def getFileChange(self, fileId):
	return self.files[fileId][2]

    def __init__(self):
	self.packages = []
	self.files = {}
	pass

class ChangeSetFromFile(ChangeSet):

    def getFileContents(self, hash):
	loc = self.csf.getTag(hash)
	if loc == "seefile":
	    fn = self.csf.getFile(hash).read()
	    return open(fn, "r")

	return self.csf.getFile(hash)

    def read(self, file):
	f = open(file, "r")
	self.csf = filecontainer.FileContainer(f)
	f.close()

	control = self.csf.getFile("SRSCHANGESET")

	lines = control.readLines()
	i = 0
	while i < len(lines):
	    header = lines[i][:-1]
	    i = i + 1

	    if header[0:18] == "SRS PKG CHANGESET ":
		(pkgName, oldVerStr, newVerStr, lineCount) = \
			string.split(header)[3:7]

		if oldVerStr == "(none)":
		    # abstract change set
		    oldVersion = None
		else:
		    oldVersion = versions.VersionFromString(oldVerStr)

		newVersion = versions.VersionFromString(newVerStr)
		lineCount = int(lineCount)

		pkg = package.PackageChangeSet(pkgName, oldVersion, newVersion)

		end = i + lineCount
		while i < end:
		    pkg.parse(lines[i][:-1])
		    i = i + 1

		self.packages.append(pkg)
	    elif header[0:19] == "SRS FILE CHANGESET ":
		(fileId, oldVerStr, newVerStr) = string.split(header)[3:6]
		if oldVerStr == "(none)":
		    oldVersion = None
		else:
		    oldVersion = versions.VersionFromString(oldVerStr)
		newVersion = versions.VersionFromString(newVerStr)
		self.files[fileId] = (oldVersion, newVersion, lines[i][:-1])
		i = i + 1
	    else:
		raise IOError, "invalid line in change set %s" % file

	    header = control.read()

    def __init__(self, file):
	ChangeSet.__init__(self)
	self.read(file)

# old may be None
def fileChangeSet(fileId, old, oldVersion, new, newVersion):
    hash = None

    if old and old.__class__ == new.__class__:
	diff = new.diff(old)
	if isinstance(new, files.RegularFile) and      \
		  isinstance(old, files.RegularFile) \
		  and new.sha1() != old.sha1():
	    hash = new.sha1()
    else:
	# different classes; these are always written as abstract changes
	old = None
	diff = new.infoLine() + "\n"
	if isinstance(new, files.RegularFile):
	    hash = new.sha1()

    if old:
	oldStr = oldVersion.asString()
    else:
	oldStr = "(none)"

    rc = "SRS FILE CHANGESET %s %s %s\n" % \
	    (fileId, oldStr, newVersion.asString()) + diff

    return (rc, hash)

# this creates the changeset against None
#
# expects a list of (packageName, pkg, fileMap) tuples, where fileHash
# maps each fileid to a (file, realPath, filePath) tuple
def CreateFromFilesystem(pkgList, version, outFileName):
    cs = ""
    hashMap = {}

    for (packageName, pkg, fileMap) in pkgList:
	(chgSet, filesNeeded) = pkg.diff(None, None, version)
	cs = cs + chgSet.asString()

	for (fileId, oldVersion, newVersion) in filesNeeded:
	    (file, realPath, filePath) = fileMap[fileId]
	    (filecs, hash) = fileChangeSet(fileId, None, None, file, 
					   newVersion)

	    if hash:
		hashMap[hash] = realPath

	    cs = cs + filecs

    outFile = open(outFileName, "w+")
    csf = filecontainer.FileContainer(outFile)
    outFile.close()

    csf.addFile("SRSCHANGESET", cs, "")

    # this changeset points at the local filesystem for the contents
    # of a file
    for hash in hashMap.keys():
	csf.addFile(hash, hashMap[hash], "seefile")

    csf.close()

# packageList is a list of (pkgName, oldVersion, newVersion) tuples
def CreateFromRepository(repos, packageList, outFileName):

    cs = ""
    hashList = []

    for (packageName, oldVersion, newVersion) in packageList:
	pkgSet = repos.getPackageSet(packageName)

	new = pkgSet.getVersion(newVersion)
     
	if oldVersion:
	    old = pkgSet.getVersion(oldVersion)
	else:
	    old = None

	(chgSet, filesNeeded) = new.diff(old, oldVersion, newVersion)
	cs = cs + chgSet.asString()

	for (fileId, oldVersion, newVersion) in filesNeeded:
	    filedb = repos.getFileDB(fileId)

	    oldFile = None
	    if oldVersion:
		oldFile = filedb.getVersion(oldVersion)
	    newFile = filedb.getVersion(newVersion)

	    (filecs, hash) = fileChangeSet(fileId, oldFile, oldVersion,
					   newFile, newVersion)
	    cs = cs + filecs
	    if hash: hashList.append(hash)

    try:
	outFile = open(outFileName, "w+")
	csf = filecontainer.FileContainer(outFile)
	outFile.close()

	csf.addFile("SRSCHANGESET", cs, "")

	for hash in hashList:
	    f = repos.pullFileContentsObject(hash)
	    csf.addFile(hash, f, "")
	    f.close()

	csf.close()
    except:
	os.unlink(outFileName)
	raise

def ChangeSetCommand(repos, cfg, packageName, outFileName, oldVersionStr, \
	      newVersionStr):
    if packageName[0] != "/":
	packageName = cfg.packagenamespace + "/" + packageName

    newVersion = versions.VersionFromString(newVersionStr, cfg.defaultbranch)

    if (oldVersionStr):
	oldVersion = versions.VersionFromString(oldVersionStr, 
					        cfg.defaultbranch)
    else:
	oldVersion = None

    list = []
    for name in repos.getPackageList(packageName):
	list.append((name, oldVersion, newVersion))

    CreateFromRepository(repos, list, outFileName)
