#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import copy
import files
import package
import sys

def commitChangeSet(repos, cfg, changeSetFile):
    cs = changeset.ChangeSet()
    cs.useFile(changeSetFile)

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
	    newPkg = package.Package(newVersion)

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
    try:
	for (pkgName, newPkg, newVersion) in pkgList:
	    pkgSet = repos.getPackageSet(pkgName, "w")
	    pkgSet.addVersion(newVersion, newPkg)
	    pkgsDone.append((pkgSet, newVersion))

	for (fileId, fileVersion, file) in fileList:
	    infoFile = repos.getFileDB(fileId)
	    infoFile.addVersion(fileVersion, file)
	    infoFile.close()
	    filesDone.append(fileId)

	for (fileId, fileVersion, file) in fileList:
	    if isinstance(file, files.RegularFile):
		f = cs.getFileContents(file.sha1())
		file.archive(repos, f)
		f.close()
    except:
	# something went wrong; try to unwind our commits
	for fileId in filesDone:
	    infoFile = repos.getFileDB(fileId)
	    (fileVersion, path) = fileMap[fileId]
	    infoFile.eraseVersion(fileVersion)

	for (pkgSet, newVersion) in pkgsDone:
	    pkgSet.eraseVersion(newVersion)

	raise 
