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
import shutil

from conary import checkin
from conary import conaryclient
from conary import errors
from conary import state
from conary.lib import log, util
from conary.versions import Label
from conary.repository.changeset import ChangesetExploder


class DeriveCallback(checkin.CheckinCallback):
    def setUpdateJob(self, *args, **kw):
        # stifle update announcement for extract
        pass


def derive(repos, cfg, targetLabel, troveToDerive, checkoutDir=None,
           extract=False, info=False, callback=None):
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
        @param troveToDerive the trove to derive from
        @type (n,v,f) or a troveSpec
        @param checkoutDir: directory to create the checkout in.  If None,
                             defaults to currentDir + packageName.
        @param extract: If True, creates a subdirectory of the checkout named
                         _ROOT_ with the contents of the binary of the derived
                         package.
        @param callback:
    """

    if callback is None:
        callback = DeriveCallback()
    if isinstance(troveToDerive, tuple):
        troveName, versionSpec, flavor = troveToDerive
        versionSpec = str(versionSpec)
        #flavor = str(flavor)
        troveSpec = conaryclient.cmdline.toTroveSpec(troveName,
                                                     versionSpec,
                                                     flavor)
    else:
        troveSpec = troveToDerive
        troveName, versionSpec, flavor = conaryclient.cmdline.parseTroveSpec(
            troveSpec)

    if isinstance(targetLabel, str):
        targetLabel = Label(targetLabel)

    nvfToDerive, = repos.findTrove(cfg.buildLabel, (troveName, versionSpec,
                                                    flavor), cfg.flavor)
    troveToDerive = repos.getTrove(*nvfToDerive)

    if ":" in troveName:
        raise errors.ParseError('Cannot derive individual components: %s' %
                                troveName)

    client = conaryclient.ConaryClient(cfg, repos=repos)
    laterShadows = client._checkForLaterShadows(targetLabel, [troveToDerive])
    if laterShadows:
        msg = []
        for n, v, f, shadowedVer in laterShadows:
            msg.append('Cannot derive from earlier version. You are trying to '
                       'derive from %s=%s[%s] but %s=%s[%s] is already '
                       'shadowed on this label.'
                       % (n, shadowedVer, f, n, v, f))
            raise errors.BranchError('\n\n'.join(msg))

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
    trvs = repos.getTroves(nvfs, withFiles=False)
    hasCapsule = [x for x in trvs if x.troveInfo.capsule.type()]
    removeText = \
"""
        # This appliance uses PHP as a command interpreter but does
        # not include a web server, so remove the file that creates
        # a dependency on the web server
        r.Remove('/etc/httpd/conf.d/php.conf')
"""
    if hasCapsule:
        derivedRecipeType = 'DerivedCapsuleRecipe'
        removeText = ''
    else:
        derivedRecipeType = 'DerivedPackageRecipe'

    checkoutDir = checkoutDir or troveName
    if os.path.exists(checkoutDir):
        raise errors.CvcError("Directory '%s' already exists" % checkoutDir)
    os.mkdir(checkoutDir)

    log.info('writing recipe file')
    recipeName = troveName + '.recipe'
    className = util.convertPackageNameToClassName(troveName)

    derivedRecipe = """
class %(className)sRecipe(%(recipeBaseClass)s):
    name = '%(name)s'
    version = '%(version)s'

    def setup(r):
        '''
        In this recipe, you can make modifications to the package.

        Examples:

        # This appliance has high-memory-use PHP scripts
        r.Replace('memory_limit = 8M', 'memory_limit = 32M', '/etc/php.ini')
%(removeText)s
        # This appliance requires that a few binaries be replaced
        # with binaries built from a custom archive that includes
        # a Makefile that honors the DESTDIR variable for its
        # install target.
        r.addArchive('foo.tar.gz')
        r.Make()
        r.MakeInstall()

        # This appliance requires an extra configuration file
        r.Create('/etc/myconfigfile', contents='some data')
        '''
""" % dict(className=className,
           name=troveName,
           version=shadowedVersion.trailingRevision().getVersion(),
           recipeBaseClass=derivedRecipeType,
           removeText=removeText)
    open(os.sep.join((checkoutDir, recipeName)), 'w').write(derivedRecipe)

    oldBldLabel = cfg.buildLabel
    cfg.buildLabel = targetLabel
    checkin.newTrove(repos, cfg, troveName, checkoutDir)
    cfg.buildLabel = oldBldLabel
    oldcwd = os.getcwd()
    os.chdir(checkoutDir)

    conaryState = state.ConaryStateFromFile('CONARY')
    shadowedVersion.resetTimeStamps()
    sourceState = conaryState.getSourceState()
    sourceState.changeVersion(shadowedVersion)
    shadowBranch = shadowedVersion.branch()
    sourceState.changeBranch(shadowBranch)
    conaryState.write('CONARY')

    checkin.commit(repos, cfg, 'Initial checkin', forceNew=True)

    if extract:
        extractDir = os.path.join(os.getcwd(), '_ROOT_')
        log.info('extracting files from %s=%s[%s]' % (nvfToDerive))
        ts = [(nvfToDerive[0], (None, None), (nvfToDerive[1], nvfToDerive[2]),
               True)]
        cs = repos.createChangeSet(ts, recurse=True)
        ChangesetExploder(cs, extractDir)
        secondDir = os.path.join(os.getcwd(), '_OLD_ROOT_')
        shutil.copytree(extractDir, secondDir, symlinks=True)

    os.chdir(oldcwd)
