#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import buildpackage
import changeset
import commit
import copy
import files
import lookaside
import os
import package
import recipe
import sha1helper
import shutil
import tempfile
import time
import types
import util

# type could be "src"
#
# returns a (pkg, fileMap) tuple
def createPackage(repos, cfg, destdir, bldPkg, ident, pkgtype = "auto"):
    fileMap = {}
    p = package.Package(bldPkg.getName(), bldPkg.getVersion())

    for filePath in bldPkg.keys():
	if pkgtype == "auto":
	    realPath = destdir + filePath
	    targetPath = filePath
	else:
	    realPath = filePath
	    targetPath = os.path.basename(filePath)

	file = files.FileFromFilesystem(realPath, ident(targetPath), 
					type = pkgtype)

	fileDB = repos.getFileDB(file.id())

        duplicateVersion = fileDB.checkBranchForDuplicate(cfg.defaultbranch,
                                                          file)
        if not duplicateVersion:
	    p.addFile(file.id(), targetPath, bldPkg.getVersion())
	else:
	    p.addFile(file.id(), targetPath, duplicateVersion)

        fileMap[file.id()] = (file, realPath, targetPath)

    return (p, fileMap)

def cook(repos, cfg, recipeFile, prep=0, macros=()):
    repos.open("r")

    if type(recipeFile) is types.ClassType:
        classList = {recipeFile.__name__: recipeFile}
    else:
        classList = recipe.RecipeLoader(recipeFile)
    built = []

    for (className, recipeClass) in classList.items():
	print "Building", className
	fullName = cfg.packagenamespace + ":" + recipeClass.name

	lcache = lookaside.RepositoryCache(repos)

	ident = buildpackage.IdGen()
        ident.populate(cfg, repos, lcache, recipeClass.name)

        srcdirs = [ os.path.dirname(recipeClass.filename), cfg.sourcepath % {'pkgname': recipeClass.name} ]
	recipeObj = recipeClass(cfg, lcache, srcdirs, macros)

	nameList = repos.getPackageList(fullName)
	version = None
	if nameList:
	    # if this package/version exists already, increment the
	    # existing revision
	    pkgSet = repos.getPackageSet(nameList[0])
	    version = pkgSet.getLatestVersion(cfg.defaultbranch)
	    if version and recipeObj.version == version.trailingVersion():
		version = copy.deepcopy(version)
		version.incrementVersionRelease()
	    else:
		version = None

	# this package/version doesn't exist yet
	if not version:
	    version = copy.deepcopy(cfg.defaultbranch)
	    version.appendVersionRelease(recipeObj.version, 1)

	builddir = cfg.buildpath + "/" + recipeObj.name

	recipeObj.setup()
	recipeObj.unpackSources(builddir)

        # if we're only extracting, continue to the next recipe class.
        if prep:
            continue
        
        cwd = os.getcwd()
        os.chdir(builddir + '/' + recipeObj.mainDir())
	repos.close()

	destdir = "/var/tmp/srs/%s-%d" % (recipeObj.name, int(time.time()))
        if os.path.exists(destdir):
            shutil.rmtree(destdir)
        util.mkdirChain(destdir)
	recipeObj.doBuild(builddir, destdir)
	recipeObj.doInstall(builddir, destdir)

	repos.open("w")
        
        os.chdir(cwd)
        
	packageList = []
        recipeObj.packages(cfg.packagenamespace, version, destdir)

	for (name, buildPkg) in recipeObj.getPackageSet().packageSet():
	    (p, fileMap) = createPackage(repos, cfg, destdir, buildPkg,
					 ident, "auto")
            built.append(fullName)
	    packageList.append((p, fileMap))

        recipes = [ recipeClass.filename ]
        # add any recipe that this recipeClass decends from to the sources
        baseRecipeClasses = list(recipeClass.__bases__)
        while baseRecipeClasses:
            parent = baseRecipeClasses.pop()
            baseRecipeClasses.extend(list(parent.__bases__))
            if not parent.__dict__.has_key('filename'):
                continue
            if not parent.filename in recipes:
                recipes.append(parent.filename)

	srcList = []
	srcName = cfg.packagenamespace + ":" + recipeObj.name + ":sources"
	srcBldPkg = buildpackage.BuildPackage(srcName, version)
	for file in recipeObj.allSources() + recipes:
            src = lookaside.findAll(cfg, lcache, file, recipeObj.name, srcdirs)
	    srcBldPkg.addFile(src)
	
	(p, fileMap) = createPackage(repos, cfg, destdir, srcBldPkg, 
				     ident, "src")
	packageList.append((p, fileMap))

	changeSet = changeset.CreateFromFilesystem(packageList)
	repos.commitChangeSet(cfg.sourcepath, changeSet)

	repos.open("r")

	recipeObj.cleanup(builddir, destdir)

    return built

