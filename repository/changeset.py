import filecontainer
import versions

def ChangeSet(repos, cfg, packageName, outFileName, oldVersionStr, \
	      newVersionStr):
    if packageName[0] != "/":
	packageName = cfg.packagenamespace + "/" + packageName

    pkgSet = repos.getPackageSet(packageName)

    newVersion = versions.VersionFromString(newVersionStr, cfg.defaultbranch)
 
    if (oldVersionStr):
	oldVersion = versions.VersionFromString(oldVersionStr, 
					        cfg.defaultbranch)
	(cs, filesNeeded) = pkgSet.changeSet(oldVersion, newVersion)
    else:
	(cs, filesNeeded) = pkgSet.changeSet(None, newVersion)

    hashList = []
    for (id, oldVersion, newVersion) in filesNeeded:
	filedb = repos.getFileDB(id)
	(filecs, hash) = filedb.changeSet(oldVersion, newVersion)
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
