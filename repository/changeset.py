import filecontainer
import files
import package
import string
import versions

class ChangeSet:

    def getFileContents(self, hash):
	return self.csf.getFile(hash)

    def useFile(self, file):
	f = open(file, "r")
	self.csf = filecontainer.FileContainer(f)
	f.close()

	control = self.csf.getFile("SRSCHANGESET")

	lines = control.readLines()
	i = 0
	while i < len(lines):
	    header = lines[i]
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
		    pkg.parse(lines[i])
		    i = i + 1

		self.packages.append(pkg)
	    elif header[0:19] == "SRS FILE CHANGESET ":
		fileId = string.split(header)[3]
		self.files[fileId] = lines[i]
		i = i + 1
	    else:
		raise IOError, "invalid line in change set %s" % file

	    header = control.read()

    def getPackageList(self):
	return self.packages

    def formatToFile(self, f):
	for pkg in self.packages:
	    pkg.formatToFile(f)

    def getFileChange(self, fileId):
	return self.files[fileId]

    def __init__(self):
	self.packages = []
	self.files = {}
	pass

# old, oldStr may be None
def packageChangeSet(packageName, old, oldStr, new, newStr):
    (rc, filesNeeded) = new.diff(old)

    if not old:
	oldStr = "(none)"

    rc = "SRS PKG CHANGESET %s %s %s %d\n" % (packageName, oldStr, newStr, 
					      rc.count("\n")) + rc
    
    return (rc, filesNeeded)
	
# old may be None
def fileChangeSet(fileId, old, new):
    hash = None

    if old and old.__class__ == new.__class__:
	diff = new.diff(old)
	if isinstance(new, files.RegularFile) and      \
		  isinstance(old, files.RegularFile) \
		  and new.sha1() != old.sha1():
	    hash = new.sha1()
    else:
	# different classes
	diff = new.infoLine() + "\n"
	if isinstance(new, files.RegularFile):
	    hash = new.sha1()

    rc = "SRS FILE CHANGESET %s\n" % (fileId) + diff

    return (rc, hash)

# this creates the changeset against None
#
# expects a list of (packageName, pkg, fileMap) tuples, where fileHash
# maps each fileid to a (file, realPath, filePath) tuple
def CreateFromFilesystem(pkgList, version, outFileName):
    cs = ""
    hashMap = {}

    for (packageName, pkg, fileMap) in pkgList:
	(newcs, filesNeeded) = packageChangeSet(packageName, None, None, pkg,
						version.asString())
	cs = cs + newcs

	for (fileId, oldVersion, newVersion) in filesNeeded:
	    (file, realPath, filePath) = fileMap[fileId]
	    (filecs, hash) = fileChangeSet(fileId, None, file)

	    if hash:
		hashMap[hash] = realPath

	    cs = cs + filecs

    outFile = open(outFileName, "w+")
    csf = filecontainer.FileContainer(outFile)
    outFile.close()

    csf.addFile("SRSCHANGESET", cs, "")

    for hash in hashMap.keys():
	f = open(hashMap[hash], "r")
	csf.addFile(hash, f, "")
	f.close()

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

	(newCs, filesNeeded) = packageChangeSet(packageName, old, 
						oldVersion.asString(), new, 
						newVersion.asString())
	cs = cs + newCs

	for (fileId, oldVersion, newVersion) in filesNeeded:
	    filedb = repos.getFileDB(fileId)

	    oldFile = None
	    if oldVersion:
		oldFile = filedb.getVersion(oldVersion)
	    newFile = filedb.getVersion(newVersion)

	    (filecs, hash) = fileChangeSet(fileId, oldFile, newFile)
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
