#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Contains the doCook() function which builds a recipe and commits the
resulting packages to the repository.
"""

import buildpackage
import changeset
import files
import log
import lookaside
import os
import package
import recipe
import repository
import sha1helper
import time
import types
import util
import tempfile

# -------------------- private below this line -------------------------

# see if the head of the specified branch is a duplicate
# of the file object passed; it so return the version object
# for that duplicate
def _checkBranchForDuplicate(repos, fileId, branch, file):
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
def _createPackage(repos, branch, bldPkg, ident):
    fileMap = {}
    p = package.Package(bldPkg.getName(), bldPkg.getVersion())

    for (path, buildFile) in bldPkg.items():
        realPath = buildFile.getRealPath()
        if isinstance(buildFile, buildpackage.BuildDeviceFile):
            f = files.FileFromInfoLine(buildFile.infoLine(), ident(path))
        elif realPath:
            f = files.FileFromFilesystem(realPath, ident(path), 
                                         type = buildFile.getType(),
                                         requireSymbolicOwnership=True)
	    # setuid or setgid must be set explicitly in buildFile
	    f.thePerms &= 01777
        else:
            raise CookError("unable to create file object for package")

        # set ownership, flags, etc
        f.merge(buildFile)
        
	duplicateVersion = _checkBranchForDuplicate(repos, f.id(), branch, f)
        if not duplicateVersion:
	    p.addFile(f.id(), path, bldPkg.getVersion())
	else:
	    p.addFile(f.id(), path, duplicateVersion)

        fileMap[f.id()] = (f, realPath, path)

    return (p, fileMap)

class _IdGen:
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

    def populate(self, repos, lcache, pkg):
	# Find the files and ids which were owned by the last version of
	# this package on the branch. We also construct an object which
	# lets us look for source files this build needs inside of the
	# repository
	for (fileId, path, version) in pkg.fileList():
	    self.map[path] = fileId
	    if path[0] != "/":
		# we might need to retrieve this source file
		# to enable a build, so we need to find the
		# sha1 hash of it since that's how it's indexed
		# in the file store
		f = repos.getFileVersion(fileId, version)
		lcache.addFileHash(path, f.sha1())

def _cook(repos, cfg, recipeFile, prep=0, macros=()):
    repos.open("r")

    buildBranch = cfg.defaultbranch

    if type(recipeFile) is types.ClassType:
        classList = {recipeFile.__name__: recipeFile}
    else:
        try:
            classList = recipe.RecipeLoader(recipeFile)
        except recipe.RecipeFileError, msg:
            raise CookError(str(msg))
    built = []

    for (className, recipeClass) in classList.items():
	log.info("Building %s", className)
	fullName = cfg.packagenamespace + ":" + recipeClass.name
	srcName = fullName + ":sources"

	lcache = lookaside.RepositoryCache(repos)

        srcdirs = [ os.path.dirname(recipeClass.filename),
                    cfg.sourcepath % {'pkgname': recipeClass.name} ]
	recipeObj = recipeClass(cfg, lcache, srcdirs, macros)

	newVersion = None
	if repos.hasPackage(fullName):
	    # if this package/version exists already, increment the
	    # existing revision
	    newVersion = repos.pkgLatestVersion(fullName, buildBranch)
	    if newVersion and recipeObj.version == newVersion.trailingVersion():
		newVersion = newVersion.copy()
		newVersion.incrementVersionRelease()
	    else:
		newVersion = None

	# this package/version doesn't exist yet
	if not newVersion:
	    newVersion = buildBranch.copy()
	    newVersion.appendVersionRelease(recipeObj.version, 1)

	builddir = cfg.buildpath + "/" + recipeObj.name

	recipeObj.setup()
	recipeObj.unpackSources(builddir)

        # if we're only extracting, continue to the next recipe class.
        if prep:
            continue
        
        cwd = os.getcwd()
        util.mkdirChain(builddir + '/' + recipeObj.mainDir())
        os.chdir(builddir + '/' + recipeObj.mainDir())
	repos.close()

        util.mkdirChain(cfg.tmpdir)
	destdir = tempfile.mkdtemp("", "srs-%s-" % recipeObj.name, cfg.tmpdir)
	recipeObj.doBuild(builddir, destdir)
	log.info('Processing %s', className)
        recipeObj.doDestdirProcess() # includes policy

	repos.open("w")
        
        os.chdir(cwd)
        
	packageList = []

	# build up the name->fileid mapping so we reuse fileids wherever
	# possible; we do this by looking in the database for a pacakge
	# with the same name as the recipe and recursing through it's
	# subpackages; this mechanism continues to work as subpackages
	# come and go

	ident = _IdGen()
	if repos.hasPackage(fullName):
	    pkgList = [ (fullName, 
			[repos.pkgLatestVersion(fullName, buildBranch)]) ]
	    while pkgList:
		(name, versionList) = pkgList[0]
		del pkgList[0]
		for version in versionList:
		    pkg = repos.getPackageVersion(name, version)
		    pkgList += pkg.getPackageList()
		    ident.populate(repos, lcache, pkg)

	for buildPkg in recipeObj.getPackages(cfg.packagenamespace, newVersion):
	    (p, fileMap) = _createPackage(repos, buildBranch, buildPkg, ident)
            built.append((p.getName(), p.getVersion().asString()))
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

	srcBldPkg = buildpackage.BuildPackage(srcName, newVersion)
	for file in recipeObj.allSources() + recipes:
            src = lookaside.findAll(cfg, lcache, file, recipeObj.name, srcdirs)
	    srcBldPkg.addFile(os.path.basename(src), src, type="src")

        for recipeFile in recipes:
            srcBldPkg[os.path.basename(recipeFile)].isConfig(True)

	(p, fileMap) = _createPackage(repos, buildBranch, srcBldPkg, ident)
	packageList.append((p, fileMap))

	grpName = cfg.packagenamespace + ":" + recipeClass.name
	grp = package.Package(grpName, newVersion)
	for (pkg, map) in packageList:
	    grp.addPackage(pkg.getName(), [ pkg.getVersion() ])

	changeSet = changeset.CreateFromFilesystem(packageList)
	grpDiff = grp.diff(None, abstract = 1)[0]

	changeSet.newPackage(grpDiff)

	repos.commitChangeSet(changeSet)

	repos.open("r")

	recipeObj.cleanup(builddir, destdir)

    return built

# -------------------- public below this line -------------------------

def doCook(repos, cfg, recipeFile, prep=0, macros=()):
    try:
	return _cook(repos, cfg, recipeFile, prep = prep, macros = macros)
    except repository.RepositoryError, e:
	raise CookError(str(e))

class CookError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
