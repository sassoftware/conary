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
def finalCommit(cfg, pkgName, simpleVersion, root, fileList):
    pkgSet = package.PackageSet(cfg.reppath, pkgName)

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

    fileDB = cfg.reppath + "/files"

    for file in fileList:
	infoFile = files.FileDB(cfg.reppath, file.pathInRep(cfg.reppath))

	duplicateVersion = infoFile.checkBranchForDuplicate(cfg.defaultbranch,
					file)
	if not duplicateVersion:
	    file.version(version)
	    infoFile.addVersion(version, file)

	    if file.__class__ == files.SourceFile:
		p.addSource("/" + file.fileName(), 
			    file.version())
	    else:
		p.addFile(file.path(), file.version())

	    infoFile.close()
	else:
	    if file.__class__ == files.SourceFile:
		p.addSource(file.path(), duplicateVersion)
	    else:
		p.addFile(file.path(),  duplicateVersion)

	file.archive(cfg.reppath, root)

    pkgSet.addVersion(version, p)
    pkgSet.close()
