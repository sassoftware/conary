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
	if pkg.getOldVersion():
	    raise IOError, "only abstract change sets are currently supported "

	newVersion = pkg.getNewVersion()
    
	if repos.hasPackage(pkg.getName()):
	    pkgSet = repos.getPackageSet(pkg.getName(), "r")

	    if pkgSet.hasVersion(newVersion):
		raise KeyError, "version %s for %s exists" % \
			(newVersion.asString(), pkg.getName())
	else:
	    pkgSet = None

	newPkg = package.Package(newVersion)

	for (fileId, path, fileVersion) in pkg.getNewFileList():
	    info = cs.getFileChange(fileId)
	    file = files.FileFromInfoLine(info, fileId)

	    newPkg.addFile(fileId, path, fileVersion)

	    infoFile = repos.getFileDB(fileId)
	    if not infoFile.hasVersion(fileVersion):
		# abstract packages often contain file
		fileMap[fileId] = (fileVersion, file, path)
	
	pkgList.append((pkg.getName(), newPkg, newVersion))

    # commit changes
    pkgsDone = []
    filesDone = []
    try:
	for (pkgName, newPkg, newVersion) in pkgList:
	    pkgSet = repos.getPackageSet(pkgName, "w")
	    pkgSet.addVersion(newVersion, newPkg)
	    pkgsDone.append((pkgSet, newVersion))

	for (fileId, (fileVersion, file, path)) in fileMap.items():
	    infoFile = repos.getFileDB(fileId)
	    infoFile.addVersion(fileVersion, file)
	    infoFile.close()
	    filesDone.append(fileId)

	for (fileId, (fileVersion, file, path)) in fileMap.items():
	    if isinstance(file, files.RegularFile):
		f = cs.getFileContents(file.sha1())

		file.archive(repos, f)

		f.close()
    except:
	# something went wrong; try to unwind our commits
	for fileId in filesDone:
	    infoFile = repos.getFileDB(fileId)
	    (fileVersion, file, path) = fileMap[fileId]
	    infoFile.eraseVersion(fileVersion)

	for (pkgSet, newVersion) in pkgsDone:
	    pkgSet.eraseVersion(newVersion)

	raise 
