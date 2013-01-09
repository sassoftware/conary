#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from fnmatch import fnmatchcase
import os

from conary.build import defaultrecipes
from conary.build.recipe import Recipe, RECIPE_TYPE_FILESET
from conary.build.packagerecipe import BaseRequiresRecipe
from conary import errors
from conary.build import errors as builderrors
from conary.build import macros
from conary.lib import util

class _FilesetRecipe(Recipe):
    _recipeType = RECIPE_TYPE_FILESET
    internalAbstractBaseClass = 1

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
        pattern = pattern % self.macros
        component = component % self.macros
        if versionStr:
            versionStr = versionStr % self.macros
        if remap:
            if isinstance(remap, tuple):
                remap = [ remap ]
            remap = [ (old % self.macros, new % self.macros)
                      for (old,new) in remap ]
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
            foundIt = False
            for sub in self.repos.walkTroveSet(pkg, asTuple = False):
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

    def __init__(self, repos, cfg, label, flavor, extraMacros={},
                 laReposCache = None, srcdirs = None):
        Recipe.__init__(self, cfg, laReposCache=laReposCache, srcdirs=srcdirs)
        self.repos = repos
        self.files = {}
        self.paths = {}
        self.label = label
        self.flavor = flavor
        self.macros.update(extraMacros)
        self.requestedFiles = {}


exec defaultrecipes.FilesetRecipe
