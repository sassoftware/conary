import package
import files
import os.path
import util
import shutil

def doUpdate(DBPATH, root, pkgName):
    pkgSet = package.PackageSet(DBPATH, pkgName)

    if (not len(pkgSet.versionList())):
	raise KeyError, "no versions exist of %s" % pkgName

    (version, pkg) = pkgSet.getLatest()

    for (fileName, version) in pkg.fileList():
	infoFile = files.FileDB(DBPATH, fileName)
	f = infoFile.getVersion(version)

	target = "%s/%s" % (root, fileName)
	dir = os.path.split(target)[0]
	util.mkdirChain(dir)

	source = "%s/files/%s.contents/%s" % (DBPATH, fileName, f.md5())

	shutil.copyfile(source, target)
	os.chmod(target, f.perms())
