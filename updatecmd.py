import package
import files
import os.path
import util
import shutil
import pwd
import grp
import files

def doUpdate(DBPATH, root, pkgName):
    pkgSet = package.PackageSet(DBPATH, pkgName)

    if (not len(pkgSet.versionList())):
	raise KeyError, "no versions exist of %s" % pkgName

    (version, pkg) = pkgSet.getLatest()

    for (fileName, version) in pkg.fileList():
	infoFile = files.FileDB(DBPATH, fileName)
	f = infoFile.getVersion(version)

	source = "%s/files/%s.contents/%s" % (DBPATH, fileName, f.uniqueName())

	target = "%s/%s" % (root, fileName)
	dir = os.path.split(target)[0]
	util.mkdirChain(dir)

	f.copy(source, target)

	if (f.__class__ != files.SymbolicLink):
	    os.chmod(target, f.perms())

	    if not os.getuid():
		# root should set the file ownerships properly
		uid = pwd.getpwnam(f.owner())[2]
		gid = grp.getgrnam(f.group())[2]

		# FIXME: this needs to use lchown, which is in 2.3, and
		# this should happen unconditionally
		os.chown(target, uid, gid)
