#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import recipe
import time
import files
import commit
import os
import util

def cook(reppath, srcdir, builddir, recipeFile):
    classList = recipe.RecipeLoader(recipeFile)

    for (name, theClass) in classList.items():
	print "Building", name

	recp = theClass()

	ourBuildDir = builddir + "/" + recp.name

	recp.setup()
	recp.unpackSources(srcdir, ourBuildDir)
	recp.doBuild(ourBuildDir)

	rootDir = "/var/tmp/srs/%s-%d" % (recp.name, int(time.time()))
        util.mkdirChain(rootDir, 0700)
	recp.doInstall(ourBuildDir, rootDir)

	pkgSet = recp.packages(rootDir)

	for (name, buildPkg) in pkgSet.packageSet():
	    fileList = []

	    for filePath in buildPkg.keys():
		f = files.FileFromFilesystem(rootDir, filePath)
		fileList.append(f)

	    commit.finalCommit(reppath, recp.name + "/" + name, recp.version, 
			       rootDir, fileList)

	fileList = []
	for file in recp.allSources():
	    f = files.FileFromFilesystem(srcdir, "/" + file)
	    fileList.append(f)

	commit.finalCommit(reppath, recp.name + "/sources", recp.version,
			    srcdir, fileList)
