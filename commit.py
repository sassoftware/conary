#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import copy
import files
import package
import string
import sys

def commitChangeSet(repos, cfg, cs):
    pkgList = []
    fileMap = {}

    # build todo set
    for pkg in cs.getPackageList():
	newVersion = pkg.getNewVersion()
	old = pkg.getOldVersion()
    
	if repos.hasPackage(pkg.getName()):
	    pkgSet = repos.getPackageSet(pkg.getName(), "r")

	    if pkgSet.hasVersion(newVersion):
		raise KeyError, "version %s for %s exists" % \
			(newVersion.asString(), pkg.getName())
	else:
	    pkgSet = None

	if old:
	    newPkg = copy.deepcopy(pkgSet.getVersion(old))
	else:
	    newPkg = package.Package(pkg.name)

	newFileMap = newPkg.applyChangeSet(repos, pkg)
	pkgList.append((pkg.getName(), newPkg, newVersion))
	fileMap.update(newFileMap)

    # create the file objects we'll need for the commit
    fileList = []
    for (fileId, (oldVer, newVer, infoLine)) in cs.getFileList():
	if oldVer:
	    fileDB = repos.getFileDB(fileId)
	    file = copy.deepcopy(fileDB.getVersion(oldVer))
	    file.applyChange(infoLine)
	    del fileDB
	else:
	    file = files.FileFromInfoLine(infoLine, fileId)

	assert(newVer.equal(fileMap[fileId][1]))
	fileList.append((fileId, newVer, file))

    # commit changes
    pkgsDone = []
    filesDone = []
    filesToArchive = {}
    try:
	for (pkgName, newPkg, newVersion) in pkgList:
	    pkgSet = repos.getPackageSet(pkgName, "w")
	    pkgSet.addVersion(newVersion, newPkg)
	    pkgsDone.append((pkgSet, newVersion))

	for (fileId, fileVersion, file) in fileList:
	    infoFile = repos.getFileDB(fileId)
	    pathInPkg = fileMap[fileId][0]
	    pkgName = fileMap[fileId][2]

	    # this version may already exist, abstract change sets
	    # include redundant files quite often
	    if not infoFile.hasVersion(fileVersion):
		infoFile.addVersion(fileVersion, file)
		infoFile.close()
		filesDone.append(fileId)
		filesToArchive[pathInPkg] = ((file, pathInPkg, pkgName))

        # sort paths and store in order (to make sure that directories
        # are stored before the files that reside in them in the case of
        # restore to a local file system
        pathsToArchive = filesToArchive.keys()
        pathsToArchive.sort()
	for pathInPkg in pathsToArchive:
            (file, path, pkgName) = filesToArchive[pathInPkg]
	    if isinstance(file, files.SourceFile):
		basePkgName = string.split(pkgName, '/')[-2]
		d = { 'pkgname' : basePkgName }
		path = (cfg.sourcepath) % d + "/" + path

	    repos.storeFileFromChangeset(cs, file, path)
    except:
	# something went wrong; try to unwind our commits
	for fileId in filesDone:
	    infoFile = repos.getFileDB(fileId)
	    (path, fileVersion) = fileMap[fileId][0:2]
	    infoFile.eraseVersion(fileVersion)

	for (pkgSet, newVersion) in pkgsDone:
	    pkgSet.eraseVersion(newVersion)

	raise 


def doCommit(repos, cfg, changeSetFile):
    cs = changeset.ChangeSetFromFile(changeSetFile)
    commitChangeSet(repos, cfg, cs)
