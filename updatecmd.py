#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import package
import files
import shutil
import pwd
import grp
import files

def doUpdate(cfg, root, pkgName, binaries = 1, sources = 0):
    if root == "/":
	print "using srs to update to your actual system is dumb."
	import sys
	sys.exit(0)

    if pkgName[0] != "/":
	pkgName = cfg.packagenamespace + "/" + pkgName

    pkgSet = package.PackageSet(cfg.reppath, pkgName)

    if (not len(pkgSet.versionList())):
	raise KeyError, "no versions exist of %s" % pkgName

    pkg = pkgSet.getLatestPackage(cfg.defaultbranch)

    fileList = []
    packageFiles = []

    if binaries:
	packageFiles = packageFiles + pkg.fileList()
    if sources:
	packageFiles = packageFiles + pkg.sourceList()

    for (fileName, version) in packageFiles:
	infoFile = files.FileDB(cfg.reppath, cfg.reppath + fileName)
	fileList.append(infoFile)

    for infoFile in fileList:
	f = infoFile.getVersion(version)
	f.restore(cfg.reppath, cfg.sourcepath, root)
