#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import package
import files
import shutil
import string
import copy

# version is a short package version, not an SRS version string
def finalCommit(repos, cfg, pkgName, simpleVersion, fileList):
    pkgSet = repos.getPackageSet(pkgName, "w")

    version = pkgSet.getLatestVersion(cfg.defaultbranch)
    if version and simpleVersion == version.trailingVersion():
	# yes, increment the revision
	version = copy.deepcopy(version)
	version.incrementVersionRelease()
    else:
	# no, make a new version
	version = copy.deepcopy(cfg.defaultbranch)
	version.appendVersionRelease(simpleVersion, 1)

    p = package.Package(version)

    for (file, pathToFile, pathInPkg) in fileList:
	infoFile = repos.getFileDB(file.id())

	duplicateVersion = infoFile.checkBranchForDuplicate(cfg.defaultbranch,
					file)
	if not duplicateVersion:
	    infoFile.addVersion(version, file)

	    p.addFile(file.id(), pathInPkg, version)

	    infoFile.close()
	else:
	    p.addFile(file.id(), pathInPkg, duplicateVersion)

	file.archive(repos, pathToFile)

    pkgSet.addVersion(version, p)
    pkgSet.close()
