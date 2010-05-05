#
# Copyright (c) 2004-2008 rPath, Inc.
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
import re

from conary import branch
from conary import checkin
from conary import conaryclient
from conary import errors
from conary import state
from conary import updatecmd
from conary import versions
from conary.deps import deps
from conary.lib import log, util

class DeriveCallback(checkin.CheckinCallback):
    def setUpdateJob(self, *args, **kw):
        # stifle update announcement for extract
        pass

def derive(repos, cfg, targetLabel, troveSpec, checkoutDir = None,
           extract = False, info = False, callback = None):
    """
        Creates a derived recipe. Note that is does not actually commit 
        anything to the repository.

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
        @param callback:
    """

    if callback is None:
        callback = DeriveCallback()
    troveName, versionSpec, flavor = conaryclient.cmdline.parseTroveSpec(
                                                                    troveSpec)

    if ":" in troveName:
        raise errors.ParseError('Cannot derive individual components: %s' %
                                troveName)

    nvfToDerive, = repos.findTrove(cfg.buildLabel, (troveName, versionSpec,
        flavor), cfg.flavor)
    troveToDerive = repos.getTrove(*nvfToDerive)

    client = conaryclient.ConaryClient(cfg,repos=repos)
    laterShadows = client._checkForLaterShadows(targetLabel, [troveToDerive])
    if laterShadows:
        msg = []
        for n, v, f, shadowedVer in laterShadows:
            msg.append('Cannot derive from earlier version. You are trying to '
                       'derive from %s=%s[%s] but %s=%s[%s] is already '
                       'shadowed on this label.'
                       % (n, shadowedVer, f, n, v, f))
            raise BranchError('\n\n'.join(msg))

    # displaying output along the screen allows there to be a record
    # of what operations were performed.  Since this command is
    # an aggregate of several commands I think that is appropriate,
    # rather than simply using a progress callback.
    log.info('Shadowing %s=%s[%s] onto %s' % (nvfToDerive[0],
                                              nvfToDerive[1],
                                              nvfToDerive[2],
                                              targetLabel))

    shadowedVersion = nvfToDerive[1].createShadow(targetLabel)
    shadowedVersion = shadowedVersion.getSourceVersion(False)
    troveName = troveName.split(':')[0]

    nvfs = list(troveToDerive.iterTroveList(strongRefs=True))
    trvs = repos.getTroves(nvfs,withFiles=False)
    hasCapsule = [ x for x in trvs if x.troveInfo.capsule.type() ]
    if hasCapsule:
        derivedRecipeType = 'DerivedCapsuleRecipe'
    else:
        derivedRecipeType = 'DerivedPackageRecipe'

    shadowBranch = shadowedVersion.branch()

    checkoutDir = checkoutDir or troveName
    if os.path.exists(checkoutDir):
        raise errors.CvcError("Directory '%s' already exists" % checkoutDir)
    os.mkdir(checkoutDir)

    log.info('Writing recipe file')
    recipeName = troveName + '.recipe'
    className = util.convertPackageNameToClassName(troveName)

    derivedRecipe = """
class %(className)sRecipe(%(recipeBaseClass)s):
    name = '%(name)s'
    version = '%(version)s'

    def setup(r):
        pass

""" % dict(className=className,
           name=troveName,
           version=shadowedVersion.trailingRevision().getVersion(),
           recipeBaseClass=derivedRecipeType)
    open(os.sep.join((checkoutDir,recipeName)), 'w').write(derivedRecipe)

    oldBldLabel = cfg.buildLabel
    cfg.buildLabel = targetLabel
    checkin.newTrove(repos,cfg,troveName,checkoutDir)
    cfg.buildLabel = oldBldLabel
    os.chdir(checkoutDir)

    conaryState = state.ConaryStateFromFile('CONARY')
    shadowedVersion.resetTimeStamps()
    sourceState = conaryState.getSourceState()
    sourceState.changeVersion(shadowedVersion)
    sourceState.changeBranch(shadowBranch)
    conaryState.write('CONARY')

    if extract:
        extractDir = os.path.join(os.getcwd(), '_ROOT_')
        log.info('extracting files from %s=%s[%s]' % (nvfToDerive))
        cfg.root = os.path.abspath(extractDir)
        cfg.interactive = False
        updatecmd.doUpdate(cfg, troveSpec,
                           callback=callback, depCheck=False)
        secondDir = os.path.join(os.getcwd(), '_OLD_ROOT_')
        shutil.copytree(extractDir, secondDir)

