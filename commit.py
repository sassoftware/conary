#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import package
import files
import shutil
import string

# version is a short package version, not an SRS version string
def finalCommit(cfg, pkgName, version, root, fileList):
    pkgSet = package.PackageSet(cfg.reppath, pkgName)

    if pkgSet.hasVersion(version):
	raise KeyError, ("package %s version %s is already installed" %
		    (pkgName, version))
    p = pkgSet.createVersion(version)

    fileDB = cfg.reppath + "/files"

    for file in fileList:
	infoFile = files.FileDB(cfg.reppath, file.pathInRep(cfg.reppath))

	existing = infoFile.findVersion(file)
	if not existing:
	    file.version(version)
	    infoFile.addVersion(version, file)

	    if file.__class__ == files.SourceFile:
		p.addSource("/" + file.fileName(), 
			    file.version())
	    else:
		p.addFile(file.path(), file.version())

	    infoFile.write()
	else:
	    if file.__class__ == files.SourceFile:
		p.addSource(file.path(), existing[0])
	    else:
		p.addFile(file.path(), existing[0])

	file.archive(cfg.reppath, root)

    pkgSet.write()
