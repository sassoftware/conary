import versions

def ChangeSet(repos, cfg, packageName, f, oldVersionStr, newVersionStr):
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

    for (id, oldVersion, newVersion) in filesNeeded:
	filedb = repos.getFileDB(id)
	cs = cs + filedb.changeSet(oldVersion, newVersion)

    import sys
    sys.stdout.write(cs)
	
