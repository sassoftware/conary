#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from fnmatch import fnmatchcase
import os

from conary.build.recipe import Recipe, RECIPE_TYPE_FILESET
from conary import errors
from conary.build import errors as builderrors
from conary.build import macros
from conary.lib import util

class FilesetRecipe(Recipe):
    _recipeType = RECIPE_TYPE_FILESET
    ignore = 1

    # XXX need to work on adding files from different flavors of troves
    def addFileFromPackage(self, pattern, pkg, recurse, remapList):
	pathMap = {}
	for (pathId, pkgPath, fileId, version) in pkg.iterFileList():
	    pathMap[pkgPath] = (pathId, fileId, version)

	patternList = util.braceExpand(pattern)
	matches = {}
	for pattern in patternList:
	    if not recurse:
		matchList = [ n for n in pathMap.keys() if 
				    fnmatchcase(n, pattern)]
	    else:
		matchList = []	
		dirCount = pattern.count("/")
		for n in pathMap.iterkeys():
		    i = n.count("/")
		    if i > dirCount:
			dirName = os.sep.join(n.split(os.sep)[:dirCount + 1])
			match = fnmatchcase(dirName, pattern)
		    elif i == dirCount:
			match = fnmatchcase(n, pattern)
		    else:
			match = False

		    if match: matchList.append(n)
			
	    for path in matchList:
		matches[path] = pathMap[path]

	if not matches:
	    return False

	for path in matches.keys():
	    (pathId, fileId, version) = matches[path]

	    for (old, new) in remapList:
		if path == old:
		    path = new
		    break
		elif len(path) > len(old) and path.startswith(old) and \
					      path[len(old)] == "/":
		    path = new + path[len(old):]
		    break

	    if self.paths.has_key(path):
		raise builderrors.RecipeFileError(
                        "%s has been included multiple times" % path)

	    self.files[pathId] = (path, fileId, version)
	    self.paths[path] = 1

	return True

    def addFile(self, pattern, component, versionStr = None, recurse = True,
		remap = []):
        self.requestedFiles.setdefault(
            (component, versionStr), []).append((pattern, recurse, remap))

    def _addFile(self, pkg, itemList):
	"""
	Adds files which match pattern from version versionStr of component.
	Pattern is glob-style, with brace expansion. If recurse is set,
	anything below a directory which matches pattern is also included,
	and the directory itself does not have to be part of the trove.
	Remap is a list of (oldPath, newPath) tuples. The first oldPath
	which matches the start of a matched pattern is rewritten as
	newPath.
	"""

        for (pattern, recurse, remap) in itemList:
            if type(remap) == tuple:
                remap = [ remap ]

            foundIt = False
            for sub in self.repos.walkTroveSet(pkg):
                foundIt = foundIt or self.addFileFromPackage(
                                            pattern, sub, recurse, remap)

            if not foundIt:
                raise builderrors.RecipeFileError(
                        "%s does not exist in version %s of %s" % \
                        (pattern, pkg.getVersion().asString(), pkg.getName()))

    def findAllFiles(self):
        findList = [ (x[0], x[1], None) for x in self.requestedFiles ]
        try:
            troveSet = self.repos.findTroves(self.label, findList,
                                             defaultFlavor = self.flavor)
        except errors.TroveNotFound, e:
            raise builderrors.RecipeFileError, str(e)

        for (component, versionStr), itemList in \
                                        self.requestedFiles.iteritems():
            pkgList = troveSet[(component, versionStr, None)]

            if len(pkgList) == 0:
                raise builderrors.RecipeFileError(
                                            "no packages match %s" % component)
            elif len(pkgList) > 1:
                raise builderrors.RecipeFileError("too many packages match %s" % component)

            pkg = self.repos.getTrove(*pkgList[0])
            self._addFile(pkg, itemList)
	    
    def iterFileList(self):
	for (pathId, (path, fileId, version)) in self.files.iteritems():
	    yield (pathId, path, fileId, version)

    def __init__(self, repos, cfg, label, flavor, extraMacros={}):
	self.repos = repos
	self.cfg = cfg
	self.files = {}
	self.paths = {}
	self.label = label
	self.flavor = flavor
        self.macros = macros.Macros()
        self.macros.update(extraMacros)
        self.requestedFiles = {}


