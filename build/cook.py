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
import sha1helper

def cook(repos, cfg, recipeFile):
    classList = recipe.RecipeLoader(recipeFile)

    if recipeFile[0] != "/":
	raise IOError, "recipe file names must be absolute paths"

    for (name, theClass) in classList.items():
	print "Building", name

	# find the files and ids which were owned by the last version of
	# this package on the branch
	fileIdMap = {}
	fullName = cfg.packagenamespace + "/" + name
	if repos.hasPackage(fullName):
	    for pkgName in repos.getPackageList(fullName):
		pkgSet = repos.getPackageSet(pkgName)
		pkg = pkgSet.getLatestPackage(cfg.defaultbranch)
		for (id, path, version) in pkg.fileList():
		    fileIdMap[path] = id

	id = idgen(fileIdMap)

	d = {}
	d['pkgname'] = name

	srcdirs = [ os.path.dirname(recipeFile), cfg.sourcepath % d ]

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

	for (name, buildPkg) in pkgSet.packageSet():
	    fileList = []

	    for filePath in buildPkg.keys():
		realPath = rootDir + filePath
		f = files.FileFromFilesystem(realPath, id(filePath))
		fileList.append((f, realPath, filePath))

	    commit.finalCommit(repos, cfg, 
			   cfg.packagenamespace + "/" + recp.name + "/" + name, 
			   recp.version, fileList)

	recipeName = os.path.basename(recipeFile)
	f = files.FileFromFilesystem(recipeFile, id(recipeName), type = "src")
	fileList = [ (f, recipeFile, recipeName) ]

	for file in recp.allSources():
            src = util.findFile(file, srcdirs)
	    srcName = os.path.basename(src)
	    f = files.FileFromFilesystem(src, id(srcName), type = "src")
	    fileList.append((f, src, srcName))

	commit.finalCommit(repos, cfg, 
			   cfg.packagenamespace + "/" + recp.name + "/sources",
			   recp.version, fileList)

	recp.cleanup(ourBuildDir, rootDir)

class idgen:

    def __call__(self, path):
	if self.map.has_key(path):
	    return self.map[path]

	return sha1helper.hashString("%s %f %s" % (path, time.time(), 
						    self.noise))

    def __init__(self, map):
	# file ids need to be unique. we include the time and path when
	# we generate them; any data put here is also used
	uname = os.uname()
	self.noise = "%s %s" % (uname[1], uname[2])
	self.map = map
