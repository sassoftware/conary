import filecontainer
import files
import versions

def packageChangeSet(repos, packageName, oldVersion, newVersion):
    pkgSet = repos.getPackageSet(packageName)

    if oldVersion:
	old = pkgSet.getVersion(oldVersion)
    else:
	old = None

    new = pkgSet.getVersion(newVersion)
    newStr = newVersion.asString()

    (rc, filesNeeded) = new.diff(old)

    if oldVersion:
	oldStr = oldVersion.asString()
    else:
	oldStr = "(none)"

    rc = "SRS PKG CHANGESET %s %s %s %d\n" % (packageName, oldStr, newStr, 
					      rc.count("\n")) + rc
    
    return (rc, filesNeeded)
	
# old may be None
def fileChangeSet(fileId, old, new):
    if old and old.__class__ == new.__class__:
	diff = new.diff(old)
	if isinstance(new, files.RegularFile) and      \
		  isinstance(old, files.RegularFile) \
		  and new.sha1() != old.sha1():
	    hash = new.sha1()
	else:
	    hash = None
    else:
	# different classes
	diff = new.infoLine() + "\n"
	hash = new.sha1()

    rc = "SRS FILE CHANGESET %s\n" % (fileId) + diff

    return (rc, hash)

def ChangeSet(repos, cfg, packageName, outFileName, oldVersionStr, \
	      newVersionStr):
    if packageName[0] != "/":
	packageName = cfg.packagenamespace + "/" + packageName

    newVersion = versions.VersionFromString(newVersionStr, cfg.defaultbranch)
 
    if (oldVersionStr):
	oldVersion = versions.VersionFromString(oldVersionStr, 
					        cfg.defaultbranch)
    else:
	oldVersion = None

    (cs, filesNeeded) = packageChangeSet(repos, packageName, oldVersion, 
					 newVersion)

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
