import package
import files
import os.path
import util
import shutil
import pwd
import grp
import files

def doUpdate(reppath, root, srcPath, pkgName, binaries = 1, sources = 0):
    if root == "/":
	print "using srs to update to your actual system is dumb."
	import sys
	sys.exit(0)

    pkgSet = package.PackageSet(reppath, pkgName)

    if (not len(pkgSet.versionList())):
	raise KeyError, "no versions exist of %s" % pkgName

    (version, pkg) = pkgSet.getLatest()

    fileList = []

    if binaries:
	for (fileName, version) in pkg.fileList():
	    infoFile = files.FileDB(reppath, 0, fileName)
	    fileList.append(infoFile)

    if sources:
	for (fileName, version) in pkg.sourceList():
	    infoFile = files.FileDB(reppath, 1, fileName)
	    fileList.append(infoFile)

    for infoFile in fileList:
	f = infoFile.getVersion(version)
	f.restore(reppath, srcPath, root)
