#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
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
def createPackage(repos, cfg, destdir, fileList, name, version, ident, 
		  pkgtype = "auto"):
    fileMap = {}
    p = package.Package(name)

    for filePath in fileList:
	if pkgtype == "auto":
	    realPath = destdir + filePath
	    targetPath = filePath
	else:
	    realPath = filePath
	    targetPath = os.path.basename(filePath)

	file = files.FileFromFilesystem(realPath, ident(targetPath), 
					type = pkgtype)

	infoFile = repos.getFileDB(file.id())

	duplicateVersion = \
	    infoFile.checkBranchForDuplicate(cfg.defaultbranch, file)
	if not duplicateVersion:
	    p.addFile(file.id(), targetPath, version)
	else:
	    p.addFile(file.id(), targetPath, duplicateVersion)

	if (pkgtype == "src"):
	    fileMap[file.id()] = (file, realPath, targetPath)
	else:
	    fileMap[file.id()] = (file, realPath, targetPath)

    return (p, fileMap)

def cook(repos, cfg, recipeFile, prep=0, macros=()):
    if type(recipeFile) is types.ClassType:
        classList = {recipeFile.__name__: recipeFile}
    else:
        classList = recipe.RecipeLoader(recipeFile)
    built = []

    for (className, recipeClass) in classList.items():
	print "Building", className

	# Find the files and ids which were owned by the last version of
	# this package on the branch. We also construct an object which
	# lets us look for source files this build needs inside of the
	# repository
	fileIdMap = {}
	fullName = cfg.packagenamespace + "/" + recipeClass.name
	lcache = lookaside.RepositoryCache(repos)
	pkg = None
	for pkgName in repos.getPackageList(fullName):
	    pkgSet = repos.getPackageSet(pkgName)
	    pkg = pkgSet.getLatestPackage(cfg.defaultbranch)
	    for (fileId, path, version) in pkg.fileList():
		fileIdMap[path] = fileId
		if path[0] != "/":
		    # we might need to retrieve this source file
		    # to enable a build, so we need to find the
		    # sha1 hash of it since that's how it's indexed
		    # in the file store
		    filedb = repos.getFileDB(fileId)
		    file = filedb.getVersion(version)
		    lcache.addFileHash(path, file.sha1())

	ident = IdGen(fileIdMap)

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
	recipeObj.doBuild(builddir)

	destdir = "/var/tmp/srs/%s-%d" % (recipeObj.name, int(time.time()))
        if os.path.exists(destdir):
            shutil.rmtree(destdir)
        util.mkdirChain(destdir)
	recipeObj.doInstall(builddir, destdir)
        
        os.chdir(cwd)
        
        pkgname = cfg.packagenamespace + "/" + recipeObj.name

	packageList = []
        recipeObj.packages(destdir)

	for (name, buildPkg) in recipeObj.getPackageSet().packageSet():
	    fullName = pkgname + "/" + name
	    (p, fileMap) = createPackage(repos, cfg, destdir, buildPkg.keys(), 
				         fullName, version, ident, "auto")

            built.append(fullName)
	    packageList.append((fullName, p, fileMap))

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
	for file in recipeObj.allSources() + recipes:
            src = lookaside.findAll(cfg, lcache, file, recipeObj.name, srcdirs)
	    srcList.append(src)
	
	(p, fileMap) = createPackage(repos, cfg, destdir, srcList, 
				     pkgname + "/sources", version, ident, 
				     "src")
	packageList.append((pkgname + "/sources", p, fileMap))

	changeSet = changeset.CreateFromFilesystem(packageList, version)
	commit.commitChangeSet(repos, cfg, changeSet)

	recipeObj.cleanup(builddir, destdir)

    return built

class IdGen:

    def __call__(self, path):
	if self.map.has_key(path):
	    return self.map[path]

	hash = sha1helper.hashString("%s %f %s" % (path, time.time(), 
							self.noise))
	self.map[path] = hash
	return hash

    def __init__(self, map):
	# file ids need to be unique. we include the time and path when
	# we generate them; any data put here is also used
	uname = os.uname()
	self.noise = "%s %s" % (uname[1], uname[2])
	self.map = map
