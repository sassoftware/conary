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
import string
import sys
import versions

def doUpdate(repos, cfg, pkg, mainPackageName):
    if cfg.root == "/":
	print "using srs to update to your actual system is dumb."
	import sys
	sys.exit(0)

    for (fileId, path, version) in pkg.fileList():
	infoFile = repos.getFileDB(fileId)
	f = infoFile.getVersion(version)

	if f.__class__ == files.SourceFile:
	    d = {}
	    d['pkgname'] = mainPackageName

	    path = (cfg.sourcepath) % d + "/" + path

	f.restore(repos, cfg.root + path)

def update(repos, cfg, pkg, versionStr = None):
    if pkg and pkg[0] != "/":
	pkg = cfg.packagenamespace + "/" + pkg

    if versionStr and versionStr[0] != "/":
	versionStr = cfg.defaultbranch.asString() + "/" + versionStr

    if versionStr:
	version = versions.VersionFromString(versionStr)
    else:
	version = None

    list = []
    # XXX ewt: bail doesn't do anything
    bail = 0
    mainPackageName = None
    for pkgName in repos.getPackageList(pkg):
	pkgSet = repos.getPackageSet(pkgName)

	if not version:
	    version = pkgSet.getLatestVersion(cfg.defaultbranch)
	if not pkgSet.hasVersion(version):
	    sys.stderr.write("package %s does not contain version %s\n" %
				 (pkgName, version.asString()))
            # XXX ewt: bail doesn't do anything
	    bail = 1
	else:
	    pkg = pkgSet.getVersion(version)
	    list.append(pkg)

	# sources are only in source packages, which are always
	# named <pkgname>/<source>
	#
	# this means we can parse a simple name of the package
	# out of the full package identifier
	if pkgName.endswith('/sources'):
	    mainPackageName = pkgName.rstrip('/sources')

    for pkg in list:
	doUpdate(repos, cfg, pkg, mainPackageName)

