#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import package
import files
import shutil
import string

def finalCommit(reppath, pkgName, version, root, fileList):
    pkgSet = package.PackageSet(reppath, pkgName)
    if pkgSet.hasVersion(version):
	raise KeyError, ("package %s version %s is already installed" %
		    (pkgName, version))
    p = pkgSet.createVersion(version)

    fileDB = reppath + "/files"

    for file in fileList:
	infoFile = files.FileDB(reppath, file.__class__ == files.SourceFile, \
				file.path())

	existing = infoFile.findVersion(file)
	if not existing:
	    file.version(version)
	    infoFile.addVersion(version, file)

	    if file.__class__ == files.SourceFile:
		p.addSource(file.path(), file.version())
	    else:
		p.addFile(file.path(), file.version())

	    infoFile.write()
	else:
	    if file.__class__ == files.SourceFile:
		p.addSource(file.path(), existing[0])
	    else:
		p.addFile(file.path(), existing[0])

	file.archive(reppath, root)

    pkgSet.write()

def doCommit(reppath, pkgName, version, root, fileNameFile):
    f = open(fileNameFile, "r")
    list = []
    for n in f.readlines():
	list.append(n[:-1])	# chop
    f.close()

    fileList = []
    for i in range(0, len(list)):
	frags = string.split(list[i])
	path = frags[0]
	flags = frags[1:]

	type = "auto"
	for flag in flags:
	    if flag == "src":
		type = "src"

	f = files.FileFromFilesystem(root, path, type)
	fileList.append(f)

    finalCommit(reppath, pkgName, version, root, fileList)
