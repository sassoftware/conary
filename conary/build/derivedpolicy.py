#
# Copyright (c) 2007 rPath, Inc.
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

import re, os

from conary import files, trove
from conary.build import buildpackage, filter, policy

class PackageSpec(policy.Policy):

    bucket = policy.PACKAGE_CREATION

    def preProcess(self):
        # map paths into the correct components
        filters = []
        self.pathObjs = {}
        for trvCs in self.recipe.cs.iterNewTroveList():
            trv = trove.Trove(trvCs)

            if not trv.isCollection():
                regexs = [ re.escape(x[1]) for x in trv.iterFileList() ]
                f = filter.Filter(regexs, self.recipe.macros,
                                  name = trv.getName().split(':')[1])
                filters.append(f)

            for (pathId, path, fileId, version) in trv.iterFileList():
                fileCs = self.recipe.cs.getFileChange(None, fileId)
                self.pathObjs[path] = files.ThawFile(fileCs, pathId)

        pkgFilter = filter.Filter('.*', self.recipe.macros,
                                  name = self.recipe.name)

        self.recipe.autopkg = \
                buildpackage.AutoBuildPackage([ pkgFilter ], filters,
                                              self.recipe)

    def doFile(self, path):
        destdir = self.recipe.macros.destdir

        if path not in self.pathObjs:
            if os.path.isdir(destdir + path):
                return

            # directories get created even if they aren't in any component
            raise builderrors.RecipeFileError(
                    'Cannot add files to derived recipe (%s)' % path)

        self.recipe.autopkg.addFile(path, destdir + path)
        pkgFile = self.recipe.autopkg.pathMap[path]
        fileObj = self.pathObjs[path]
        pkgFile.inode.owner.set(fileObj.inode.owner())
        pkgFile.inode.group.set(fileObj.inode.group())
        pkgFile.tags.thaw(fileObj.tags.freeze())
