#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
Currently-disabled module for importing RPMs
"""
import commit
from build import cook
import os
import stat
import util

from build import buildpackage
from build import lookaside
from repository import changeset

def doImport(repos, cfg, rpmFile):
    # this is just to avoid a warning for all conary invocations when an
    # old rpm module is being used
    import rpm

    print "I can't handle flavors"
    assert(0)

    ts = rpm.TransactionSet()
    ts.setVSFlags(~(rpm._RPMVSF_NOSIGNATURES))

    pkgFile = rpmFile;
    if pkgFile[0] != "/":
	pkgFile = os.getcwd() + '/' + pkgFile

    fd = os.open(pkgFile, os.O_RDONLY)
    h = ts.hdrFromFdno(fd)
    os.close(fd)

    pkgName = h['name']
    pkgVersion = h['version']
    pkgRelease = h['release']

    if (not pkgRelease):
	print pkgFile + " does not appear to be a valid RPM"

    list = h['filenames']
    modes = h['filemodes']
    owners = h['fileusername']
    groups = h['filegroupname']
    mtimes = h['filemtimes']
    rdevs = h['filerdevs']
    linktos = h['filelinktos']
    flags = h['fileflags']

    buildBranch = cfg.defaultbranch
    del h
    del ts

    lcache = lookaside.RepositoryCache(repos)
    ident = cook._IdGen()
    currentVersion = None
    if repos.hasPackage(pkgName):
	currentVersion = repos.getTroveLatestVersion(pkgName, buildBranch)
	pkg = repos.getPackageVersion(pkgName, currentVersion)
	ident.populate(repos, lcache, pkg)

    newVersion = helper.nextVersion(pkgVersion, currentVersion, buildBranch,
				    binary = True)

    fileList = []

    buildPkg = buildpackage.BuildPackage(pkgName, newVersion)

    mustExtract = 0
    for i in xrange(0, len(list)):
	if (stat.S_ISREG(modes[i])):
	    if not (flags[i] & rpm.RPMFILE_GHOST):
		mustExtract = 1
		break

    scratch = "/tmp/importrpm"
    util.mkdirChain(scratch)

    if mustExtract:
	os.system("cd %s; rpm2cpio %s | cpio -iumd --quiet" % 
		    (scratch, pkgFile))
    
    for i in xrange(0, len(list)):
	if (stat.S_ISBLK(modes[i])):
	    buildPkg.addDevice(list[i], "b", (rdevs[i] & 0xff00) >> 8,
			       rdevs[i] & 0xff, owners[i], groups[i],
			       modes[i] & 07777)
	elif  (stat.S_ISCHR(modes[i])):
	    buildPkg.addDevice(list[i], "c", (rdevs[i] & 0xff00) >> 8,
			       rdevs[i] & 0xff, owners[i], groups[i],
			       modes[i] & 07777)
	else:
	    buildPkg.addFile(list[i], scratch + list[i])
	    f = buildPkg.getFile(list[i])
	    f.inode.setPerms(modes[i] & 07777)
	    f.inode.setOwner(owners[i])
	    f.inode.setGroup(groups[i])

    (p, fileMap) = cook._createPackage(repos, buildBranch, buildPkg, ident)
    packageList = [ (p, fileMap) ]
    changeSet = changeset.CreateFromFilesystem(packageList)
    changeSet.addPrimaryPackage(buildPkg.getName(), newVersion)

    repos.commitChangeSet(changeSet)

    if mustExtract:
	os.system("rm -rf %s" % scratch)

