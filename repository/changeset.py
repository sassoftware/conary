import versions

def ChangeSet(repos, cfg, packageName, f, oldVersionStr, newVersionStr):
    if packageName[0] != "/":
	packageName = cfg.packagenamespace + "/" + packageName

    pkgSet = repos.getPackageSet(packageName)

    newVersion = versions.VersionFromString(newVersionStr, cfg.defaultbranch)
 
    if (oldVersionStr):
	oldVersion = versions.VersionFromString(oldVersionStr, 
					        cfg.defaultbranch)
	cs = pkgSet.changeSet(oldVersion, newVersion)
    else:
	cs = pkgSet.changeSet(None, newVersion)

    print cs
	
