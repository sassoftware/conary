#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import buildpackage
import changeset
import files
import lookaside
import os
import package
import recipe
import sha1helper
import shutil
import time
import types
import util

# see if the head of the specified branch is a duplicate
# of the file object passed; it so return the version object
# for that duplicate
def checkBranchForDuplicate(repos, fileId, branch, file):
    version = repos.fileLatestVersion(fileId, branch)
    if not version:
	return None

    lastFile = repos.getFileVersion(fileId, version)

    if file.same(lastFile):
	return version

    return None

# type could be "src"
#
# returns a (pkg, fileMap) tuple
def createPackage(repos, cfg, bldPkg, ident):
    fileMap = {}
    p = package.Package(bldPkg.getName(), bldPkg.getVersion())

    for (path, file) in bldPkg.items():
        realPath = file.getRealPath()
        if realPath:
            file = files.FileFromFilesystem(realPath, ident(path), 
                                            type = file.getType())
        else:
            raise RuntimeError, "unable to find file on filesystem when building package"

	duplicateVersion = checkBranchForDuplicate(repos, file.id(),
						   cfg.defaultbranch, file)
        if not duplicateVersion:
	    p.addFile(file.id(), path, bldPkg.getVersion())
	else:
	    p.addFile(file.id(), path, duplicateVersion)

        fileMap[file.id()] = (file, realPath, path)

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

	ident = IdGen()
        ident.populate(cfg, repos, lcache, recipeClass.name)

        srcdirs = [ os.path.dirname(recipeClass.filename), cfg.sourcepath % {'pkgname': recipeClass.name} ]
	recipeObj = recipeClass(cfg, lcache, srcdirs, macros)

	nameList = repos.getPackageList(fullName)
	version = None
	if nameList:
	    # if this package/version exists already, increment the
	    # existing revision
	    version = repos.pkgLatestVersion(nameList[0], cfg.defaultbranch)
	    if version and recipeObj.version == version.trailingVersion():
		version = version.copy()
		version.incrementVersionRelease()
	    else:
		version = None

	# this package/version doesn't exist yet
	if not version:
	    version = cfg.defaultbranch.copy()
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
	    (p, fileMap) = createPackage(repos, cfg, buildPkg, ident)
            built.append(p.getName())
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

	srcName = cfg.packagenamespace + ":" + recipeObj.name + ":sources"
	srcBldPkg = buildpackage.BuildPackage(srcName, version)
	for file in recipeObj.allSources() + recipes:
            src = lookaside.findAll(cfg, lcache, file, recipeObj.name, srcdirs)
	    srcBldPkg.addFile(os.path.basename(src), src, type="src")
	
	(p, fileMap) = createPackage(repos, cfg, srcBldPkg, ident)
	packageList.append((p, fileMap))

	changeSet = changeset.CreateFromFilesystem(packageList)
	repos.commitChangeSet(cfg.sourcepath, changeSet)

	repos.open("r")

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

    def __init__(self, map=None):
	# file ids need to be unique. we include the time and path when
	# we generate them; any data put here is also used
	uname = os.uname()
	self.noise = "%s %s" % (uname[1], uname[2])
        if map is None:
            self.map = {}
        else:
            self.map = map

    def populate(self, cfg, repos, lcache, name):
	# Find the files and ids which were owned by the last version of
	# this package on the branch. We also construct an object which
	# lets us look for source files this build needs inside of the
	# repository
	fileIdMap = {}
	fullName = cfg.packagenamespace + ":" + name
	pkg = None
	for pkgName in repos.getPackageList(fullName):
	    pkg = repos.getLatestPackage(pkgName, cfg.defaultbranch)
	    for (fileId, path, version) in pkg.fileList():
		fileIdMap[path] = fileId
		if path[0] != "/":
		    # we might need to retrieve this source file
		    # to enable a build, so we need to find the
		    # sha1 hash of it since that's how it's indexed
		    # in the file store
		    file = repos.getFileVersion(fileId, version)
		    lcache.addFileHash(path, file.sha1())

        self.map.update(fileIdMap)
