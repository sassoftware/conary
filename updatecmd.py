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

	f.copy(source, target)

	if (f.__class__ != files.SymbolicLink and
		f.__class__ != files.Socket):
	    os.chmod(target, f.perms())

	    if not os.getuid():
		# root should set the file ownerships properly
		uid = pwd.getpwnam(f.owner())[2]
		gid = grp.getgrnam(f.group())[2]

		# FIXME: this needs to use lchown, which is in 2.3, and
		# this should happen unconditionally
		os.chown(target, uid, gid)
