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
import shutil

from conary import branch
from conary import checkin
from conary import conaryclient
from conary import state
from conary import updatecmd
from conary import versions
from conary.build import loadrecipe
from conary.lib import log, util

class DeriveCallback(checkin.CheckinCallback):
    def setUpdateJob(self, *args, **kw):
        # stifle update announcement for extract
        pass

def derive(repos, cfg, targetLabel, troveSpec, checkoutDir = None, 
           extract = False, info = False, callback = None):
    """
        Performs all the commands necessary to create a derived recipe.
        First it shadows the package, then it creates a checkout of the shadow
        and converts the checkout to a derived recipe package.

        Finally if extract = True, it installs an version of the binary 
        package into a root.

        @param repos: trovesource to search for and derive packages from
        @param cfg: configuration to use when deriving the package
        @type cfg: ConaryConfiguration object
        @param targetLabel: label to derive from
        @type targetLabel: versions.Label
        @param checkoutDir: directory to create the checkout in.  If None,
                             defaults to currentDir + packageName.
        @param extract: If True, creates a subdirectory of the checkout named
                         _ROOT_ with the contents of the binary of the derived
                         package.
        @param info: If true, only display the information about the shadow
                      that would be performed if the derive command were
                      completed.
        @param callback:
    """
    if callback is None:
        callback = DeriveCallback()
    troveName, versionSpec, flavor = conaryclient.cmdline.parseTroveSpec(
                                                                    troveSpec)
    result = repos.findTrove(cfg.buildLabel, (troveName, versionSpec, flavor),
                             cfg.flavor)
    # findTrove shouldn't return multiple items for one package anymore
    # when a flavor is specified.
    assert(len(result) == 1)
    troveToDerive, = result
    # displaying output along the screen allows there to be a record
    # of what operations were performed.  Since this command is
    # an aggregate of several commands I think that is appropriate,
    # rather than simply using a progress callback.
    log.info('Shadowing %s=%s[%s] onto %s' % (troveToDerive[0],
                                             troveToDerive[1],
                                             troveToDerive[2],
                                             targetLabel))
    if info:
        cfg.interactive = False

    error = branch.branch(repos, cfg, str(targetLabel),
                  ['%s=%s[%s]' % troveToDerive],
                  makeShadow = True, sourceOnly = True, binaryOnly = False,
                  info = info)
    if info or error:
        return
    shadowedVersion = troveToDerive[1].createShadow(targetLabel)
    shadowedVersion = shadowedVersion.getSourceVersion(False)
    troveName = troveName.split(':')[0]

    checkoutDir = checkoutDir or troveName
    checkin.checkout(repos, cfg, checkoutDir,
                     ["%s=%s" % (troveName, shadowedVersion)], 
                     callback=callback)
    os.chdir(checkoutDir)
    recipeName = troveName + '.recipe'
    recipePath = os.getcwd() + '/' + troveName + '.recipe'
    shadowBranch = shadowedVersion.branch()

    log.info('Rewriting recipe file')
    loader = loadrecipe.RecipeLoader(recipePath, cfg=cfg,
                                     repos=repos,
                                     branch=shadowBranch,
                                     buildFlavor=cfg.buildFlavor)
    recipeClass = loader.getRecipe()
    derivedRecipe = """
class %(className)s(DerivedPackageRecipe):
    name = '%(name)s'
    version = '%(version)s'

    def setup(r):
        pass

""" % dict(className=recipeClass.__name__,
           name=recipeClass.name,
           version=recipeClass.version)
    open(recipeName, 'w').write(derivedRecipe)

    log.info('Removing extra files from checkout')
    conaryState = state.ConaryStateFromFile('CONARY', repos)
    sourceState = conaryState.getSourceState()

    for (pathId, path, fileId, version) in list(sourceState.iterFileList()):
        if path == recipeName:
            continue
        sourceState.removeFile(pathId)
        if util.exists(path):
            statInfo = os.lstat(path)
            try:
                if statInfo.st_mode & stat.S_IFDIR:
                    os.rmdir(path)
                else:
                    os.unlink(path)
            except OSError, e:
                log.warning("cannot remove %s: %s" % (path, e.strerror))
    conaryState.write('CONARY')

    if extract:
        extractDir = os.path.join(os.getcwd(), '_ROOT_')
        log.info('extracting files from %s=%s[%s]' % (troveToDerive))
        cfg.root = os.path.abspath(extractDir)
        cfg.interactive = False
        updatecmd.doUpdate(cfg, troveSpec,
                           callback=callback, depCheck=False)
        secondDir = os.path.join(os.getcwd(), '_OLD_ROOT_')
        shutil.copytree(extractDir, secondDir)

