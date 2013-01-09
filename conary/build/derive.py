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


"""
Contains the functions which derive a package and commit the
resulting packages to the repository.
"""

import os
import stat

from conary.cmds import branch
from conary import checkin
from conary import state
from conary.conaryclient import cmdline
from conary.lib import log, util
from conary.versions import Label
from conary.repository.changeset import ChangesetExploder

class DeriveCallback(checkin.CheckinCallback):
    def setUpdateJob(self, *args, **kw):
        # stifle update announcement for extract
        pass

def derive(repos, cfg, targetLabel, troveSpec, checkoutDir=None,
           extract=False, info=False, callback=None):
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

    origDir = os.getcwd()
    try:
        if callback is None:
            callback = DeriveCallback()

        if isinstance(troveSpec, tuple):
            troveName, versionSpec, flavor = troveSpec
            versionSpec = str(versionSpec)
            troveSpec = cmdline.toTroveSpec(troveName, versionSpec, flavor)
        else:
            troveName, versionSpec, flavor = cmdline.parseTroveSpec(troveSpec)

        if isinstance(targetLabel, str):
            targetLabel = Label(targetLabel)

        troveName, versionSpec, flavor = cmdline.parseTroveSpec(troveSpec)
        result = repos.findTrove(cfg.buildLabel,
                                 (troveName, versionSpec, flavor),
                                 cfg.flavor)
        # findTrove shouldn't return multiple items for one package anymore
        # when a flavor is specified.
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
                              ['%s=%s[%s]'%troveToDerive],
                              makeShadow=True, sourceOnly=True,
                              binaryOnly=False, allowEmptyShadow=True,
                              info=info)
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

        nvfs = repos.getTrovesBySource(troveToDerive[0]+':source',
                                       troveToDerive[1].getSourceVersion())
        trvs = repos.getTroves(nvfs)
        hasCapsule = [ x for x in trvs if x.troveInfo.capsule.type() ]
        if hasCapsule:
            derivedRecipeType = 'DerivedCapsuleRecipe'
            removeText = ''
        else:
            derivedRecipeType = 'DerivedPackageRecipe'
            removeText = \
"""
        # This appliance uses PHP as a command interpreter but does
        # not include a web server, so remove the file that creates
        # a dependency on the web server
        r.Remove('/etc/httpd/conf.d/php.conf')
"""

        log.info('Rewriting recipe file')
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

        open(recipeName, 'w').write(derivedRecipe)

        log.info('Removing extra files from checkout')

        conaryState = state.ConaryStateFromFile('CONARY', repos)
        sourceState = conaryState.getSourceState()
        # clear the factory since we don't care about how the parent trove was
        # created
        sourceState.setFactory('')

        addRecipe=True
        for (pathId, path, fileId, version) in list(sourceState.iterFileList()):
            if path == recipeName:
                addRecipe = False
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

        if addRecipe:
            checkin.addFiles([recipeName])

        if extract:
            log.info('extracting files from %s=%s[%s]' % (troveToDerive))
            # extract to _ROOT_
            extractDir = os.path.join(os.getcwd(), '_ROOT_')
            ts = [ (troveToDerive[0], (None, None),
                    (troveToDerive[1], troveToDerive[2]), True) ]
            cs = repos.createChangeSet(ts, recurse = True)
            ChangesetExploder(cs, extractDir)
            # extract to _OLD_ROOT_
            secondDir = os.path.join(os.getcwd(), '_OLD_ROOT_')
            cs = repos.createChangeSet(ts, recurse = True)
            ChangesetExploder(cs, secondDir)

    finally:
        # restore the original directory before we started
        os.chdir(origDir)
