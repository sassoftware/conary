import filecontainer
import files
import package
import string
import versions

class ChangeSet:

    def readFile(self, file):
	f = open(file, "r")
	csf = filecontainer.FileContainer(f)
	f.close()

	control = csf.getFile("SRSCHANGESET")

	lines = control.readLines()
	i = 0
	while i < len(lines):
	    header = lines[i]
	    i = i + 1

	    if header[0:18] == "SRS PKG CHANGESET ":
		(pkgName, oldVerStr, newVerStr, lineCount) = \
			string.split(header)[3:7]

		if oldVerStr == "(none)":
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

    def formatToFile(self, f):
	for pkg in self.packages:
	    pkg.formatToFile(f)

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

def ChangeSetCommand(repos, cfg, packageName, outFileName, oldVersionStr, \
	      newVersionStr):
    if packageName[0] != "/":
	packageName = cfg.packagenamespace + "/" + packageName

    pkgSet = repos.getPackageSet(packageName)

    newVersion = versions.VersionFromString(newVersionStr, cfg.defaultbranch)
    new = pkgSet.getVersion(newVersion)
 
    if (oldVersionStr):
	oldVersion = versions.VersionFromString(oldVersionStr, 
					        cfg.defaultbranch)
	old = pkgSet.getVersion(oldVersion)
    else:
	old = None

    (cs, filesNeeded) = packageChangeSet(packageName, old, oldVersionStr, 
					 new, newVersionStr)
					 
    hashList = []
    for (fileId, oldVersion, newVersion) in filesNeeded:
	filedb = repos.getFileDB(fileId)

	oldFile = None
	if oldVersion:
	    oldFile = filedb.getVersion(oldVersion)
	newFile = filedb.getVersion(newVersion)

	(filecs, hash) = fileChangeSet(fileId, oldFile, newFile)
	cs = cs + filecs
	if hash: hashList.append(hash)

    outFile = open(outFileName, "w+")
    csf = filecontainer.FileContainer(outFile)
    outFile.close()

    csf.addFile("SRSCHANGESET", cs, "")

    for hash in hashList:
	f = repos.pullFileContentsObject(hash)
	csf.addFile(hash, f, "")
	f.close()

    csf.close()
