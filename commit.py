import package
import util
import os
import files
import shutil

def finalCommit(DBPATH, pkgName, version, root, fileList):
    pkgSet = package.PackageSet(DBPATH, pkgName)
    if pkgSet.hasVersion(version):
	raise KeyError, ("package %s version %s is already installed" %
		    (pkgName, version))
    p = pkgSet.createVersion(version)

    fileDB = DBPATH + "/files"

    for file in fileList:
	infoFile = files.FileDB(DBPATH, file.path())

	existing = infoFile.findVersion(file)
	if not existing:
	    file.version(version)
	    infoFile.addVersion(version, file)
	    p.addFile(file.path(), file.version())
	    infoFile.write()
	else:
	    p.addFile(file.path(), existing[0])

	if file.__class__ == files.RegularFile:
	    dest = fileDB + "/" + file.dir() + "/" + file.name() + ".contents" 

	    util.mkdirChain(dest)
	    print file.path()
	    dest = dest + "/" + file.uniqueName()
	    shutil.copyfile(root + "/" + file.path(), dest)

    pkgSet.write()

def doCommit(DBPATH, pkgName, version, root, fileNameFile):
    f = open(fileNameFile, "r")
    list = []
    for n in f.readlines():
	list.append(n[:-1])	# chop
    f.close()

    fileDB = DBPATH + "/files"

    fileList = []
    for i in range(0, len(list)):
	f = files.FileFromFilesystem(root, list[i])
	fileList.append(f)

    finalCommit(DBPATH, pkgName, version, root, fileList)
