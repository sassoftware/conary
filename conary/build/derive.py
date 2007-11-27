#
# Copyright (c) 2004-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Contains the functions which derive a package and commit the
resulting packages to the repository.
"""

import os
import stat

from conary import branch
from conary import checkin
from conary import conaryclient
from conary import state
from conary import updatecmd
from conary import versions
from conary.build import loadrecipe
from conary.lib import util

def derive(repos, cfg, target, troveSpecs, checkoutDir = None, extractDir = None, info = False, callback = None):
    branch.branch(repos, cfg, target, troveSpecs, makeShadow = True,
                  sourceOnly = True, binaryOnly = False,
                  info = info, targetFile = None)
    if info:
        return

    targetTroveSpecs = []
    for trvSpec in troveSpecs:
        nvf = conaryclient.cmdline.parseTroveSpec(trvSpec)
        targetTroveSpecs.append("%s=%s" % (nvf[0], target))

    coArgs = [repos, cfg, checkoutDir, targetTroveSpecs, callback]
    checkin.checkout(*coArgs)

    targetLabel = versions.Label(target)
    for trvSpec in targetTroveSpecs:
        nvf = conaryclient.cmdline.parseTroveSpec(trvSpec)
        recipe = loadrecipe.recipeLoaderFromSourceComponent( \
                nvf[0], cfg, repos, labelPath = targetLabel)[0]
        className, recipe = recipe.allRecipes().items()[0]

        dirName = checkoutDir or nvf[0]
        cnyState = state.ConaryStateFromFile( \
                os.path.join(dirName, "CONARY"), repos)
        st = cnyState.getSourceState()
        for (pathId, path, fileId, version) in \
                [x for x in st.iterFileList() \
                if not x[1].endswith('.recipe')]:
            st.removeFile(pathId)
            filename = os.path.join(dirName, path)
            if util.exists(filename):
                sb = os.lstat(filename)
                try:
                    if sb.st_mode & stat.S_IFDIR:
                        os.rmdir(filename)
                    else:
                        os.unlink(filename)
                except OSError, e:
                    log.error("cannot remove %s: %s" % (filename, e.strerror))
                    return 1
        cnyState.write(os.path.join(dirName, "CONARY"))

        recipePath = [x for x in [y[1] for y in st.iterFileList()] \
                if x.endswith('.recipe')][0]
        recipeFile = open(os.path.join(dirName, recipePath), 'w')

        recipeFile.write('class %s(DerivedPackageRecipe):\n' % className)
        recipeFile.write("    name = '%s'\n" % recipe.name)
        recipeFile.write("    version = '%s'\n" % recipe.version)
        recipeFile.write('\n')
        recipeFile.write('    def setup(r):\n')
        recipeFile.write('        pass\n')
        recipeFile.write('\n')

    if extractDir:
        kwargs = {}
        kwargs['callback'] = callback
        kwargs['depCheck'] = False
        cfg.root = os.path.abspath(extractDir)
        updatecmd.doUpdate(cfg, troveSpecs, **kwargs)
