import package
import files
import os.path
import util
import shutil
import pwd
import grp
import files

def doUpdate(reppath, root, pkgName):
    if root == "/":
	print "using srs to update to your actual system is dumb."
	import sys
	sys.exit(0)

    pkgSet = package.PackageSet(reppath, pkgName)

    if (not len(pkgSet.versionList())):
	raise KeyError, "no versions exist of %s" % pkgName

    (version, pkg) = pkgSet.getLatest()

    for (fileName, version) in pkg.fileList():
	infoFile = files.FileDB(reppath, fileName)
	f = infoFile.getVersion(version)

	source = "%s/files/%s.contents/%s" % (reppath, fileName, f.uniqueName())

	target = "%s/%s" % (root, fileName)
	dir = os.path.split(target)[0]
	util.mkdirChain(dir)

	f.restore(reppath, None, root)
