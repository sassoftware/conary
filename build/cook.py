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

def cook(cfg, srcdirs, recipeFile):
    classList = recipe.RecipeLoader(recipeFile)

    if recipeFile[0] != "/":
	raise IOError, "recipe file names must be absolute paths"

    for (name, theClass) in classList.items():
	print "Building", name

	recp = theClass()

	ourBuildDir = cfg.buildpath + "/" + recp.name

	recp.setup()
	recp.unpackSources(srcdirs, ourBuildDir)
	recp.doBuild(ourBuildDir)

	rootDir = "/var/tmp/srs/%s-%d" % (recp.name, int(time.time()))
        util.mkdirChain(rootDir)
	recp.doInstall(ourBuildDir, rootDir)

        recp.packages(rootDir)
        pkgSet = recp.getPackageSet()

        pkgname = cfg.packagenamespace + "/" + recp.name
        
	for (name, buildPkg) in pkgSet.packageSet():
	    fileList = []

	    for filePath in buildPkg.keys():
		f = files.FileFromFilesystem(recp.name, rootDir, filePath)
		fileList.append(f)

	    commit.finalCommit(cfg, pkgname + "/" + name, recp.version, 
                               rootDir, fileList)

	f = files.FileFromFilesystem(recp.name, "/", recipeFile, "src")
	fileList = [ f ]
	for file in recp.allSources():
            src = util.findFile(file, srcdirs)
	    f = files.FileFromFilesystem(recp.name, "/", src, "src")
	    fileList.append(f)

	commit.finalCommit(cfg, pkgname + "/sources", recp.version,
                           "/", fileList)

	recp.cleanup(ourBuildDir, rootDir)
