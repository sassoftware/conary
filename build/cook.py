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
import shutil

def cook(repos, cfg, recipeFile):
    classList = recipe.RecipeLoader(recipeFile)
    built = []

    if recipeFile[0] != "/":
	raise IOError, "recipe file names must be absolute paths"

    for (name, recipeClass) in classList.items():
	print "Building", name

	# find the files and ids which were owned by the last version of
	# this package on the branch
	fileIdMap = {}
	fullName = cfg.packagenamespace + "/" + name
	if repos.hasPackage(fullName):
	    for pkgName in repos.getPackageList(fullName):
		pkgSet = repos.getPackageSet(pkgName)
		pkg = pkgSet.getLatestPackage(cfg.defaultbranch)
		for (fileId, path, version) in pkg.fileList():
		    fileIdMap[path] = fileId

	ident = IdGen(fileIdMap)

        srcdirs = [ os.path.dirname(recipeFile), cfg.sourcepath % {'pkgname': name} ]
	recipeObj = recipeClass(cfg, srcdirs)

	ourBuildDir = cfg.buildpath + "/" + recipeObj.name

	recipeObj.setup()
	recipeObj.unpackSources(ourBuildDir)
	recipeObj.doBuild(ourBuildDir)

	rootDir = "/var/tmp/srs/%s-%d" % (recipeObj.name, int(time.time()))
        if os.path.exists(rootDir):
            shutil.rmtree(rootDir)
        util.mkdirChain(rootDir)
	recipeObj.doInstall(ourBuildDir, rootDir)

        recipeObj.packages(rootDir)
        pkgSet = recipeObj.getPackageSet()

        pkgname = cfg.packagenamespace + "/" + recipeObj.name

	for (name, buildPkg) in pkgSet.packageSet():
            built.append(pkgname + "/" + name)
	    fileList = []

	    for filePath in buildPkg.keys():
		realPath = rootDir + filePath
		f = files.FileFromFilesystem(realPath, ident(filePath))
		fileList.append((f, realPath, filePath))

	    commit.finalCommit(repos, cfg, pkgname + "/" + name,
                               recipeObj.version, fileList)

        # XXX include recipe files loaded by a recipe to derive
	recipeName = os.path.basename(recipeFile)
	f = files.FileFromFilesystem(recipeFile, ident(recipeName),
                                     type = "src")
	fileList = [ (f, recipeFile, recipeName) ]

	for file in recipeObj.allSources():
            src = util.findFile(file, srcdirs)
	    srcName = os.path.basename(src)
	    f = files.FileFromFilesystem(src, ident(srcName), type = "src")
	    fileList.append((f, src, srcName))

	commit.finalCommit(repos, cfg, pkgname + "/sources",
			   recipeObj.version, fileList)

	recipeObj.cleanup(ourBuildDir, rootDir)
    return built

class IdGen:

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
