#
# Copyright (c) 2004-2006 rPath, Inc.
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

"""
Contains the functions which builds a recipe and commits the
resulting packages to the repository.
"""

import fcntl
import itertools
import os
import resource
import shutil
import signal
import sys
import tempfile
import textwrap
import time
import traceback

from conary import (callbacks, conaryclient, constants, files, trove, versions,
                    updatecmd)
from conary.build import buildinfo, buildpackage, lookaside, policy, use
from conary.build import recipe, grouprecipe, loadrecipe
from conary.build import errors as builderrors
from conary.build.nextversion import nextVersion
from conary.conarycfg import selectSignatureKey
from conary.deps import deps
from conary.lib import debugger, log, logger, sha1helper, util
from conary.local import database
from conary.repository import changeset, errors
from conary.conaryclient.cmdline import parseTroveSpec
from conary.state import ConaryStateFromFile

CookError = builderrors.CookError
RecipeFileError = builderrors.RecipeFileError

# -------------------- private below this line -------------------------
def _createComponent(repos, bldPkg, newVersion, ident):
    # returns a (trove, fileMap) tuple
    fileMap = {}
    p = trove.Trove(bldPkg.getName(), newVersion, bldPkg.flavor, None)
    # troves don't require things that are provided by themeselves - it 
    # just creates more work for no benefit.
    p.setRequires(bldPkg.requires - bldPkg.provides)
    p.setProvides(bldPkg.provides)

    linkGroups = {}
    for pathList in bldPkg.linkGroups.itervalues():
        linkGroupId = sha1helper.sha1String("\n".join(pathList))
        linkGroups.update({}.fromkeys(pathList, linkGroupId))

    size = 0

    for (path, (realPath, f)) in bldPkg.iteritems():
        if isinstance(f, files.RegularFile):
            flavor = f.flavor.deps
        else:
            flavor = None
        (pathId, fileVersion, oldFileId) = ident(path, newVersion, flavor)
	f.pathId(pathId)

        linkGroupId = linkGroups.get(path, None)
        if linkGroupId:
            f.linkGroup.set(linkGroupId)

        fileId = f.fileId()
        if not fileVersion:
            # no existing versions for this path
	    p.addFile(f.pathId(), path, newVersion, fileId)
	else:
            # check to see if the file we have now is the same as the
            # file in the previous version of the file (modes, contents, etc)
	    if oldFileId == fileId:
                # if it's the same, use old version
		p.addFile(f.pathId(), path, fileVersion, fileId)
	    else:
                # otherwise use the new version
		p.addFile(f.pathId(), path, newVersion, fileId)

        fileMap[f.pathId()] = (f, realPath, path)

        if f.hasContents:
            size += f.contents.size()

    p.setSize(size)
    p.computePathHashes()

    return (p, fileMap)

class _IdGen:
    def __call__(self, path, version, flavor):
	if self.map.has_key(path):
	    return self.map[path]

	pathid = sha1helper.md5String("%s %s" % (path, version.asString()))
	self.map[path] = (pathid, None, None)
	return (pathid, None, None)

    def __init__(self, map=None):
        if map is None:
            self.map = {}
        else:
            self.map = map

    def _processTrove(self, t, cs):
        for pathId, path, fileId, version in t.iterFileList():
            fileStream = files.ThawFile(cs.getFileChange(None, fileId),
                                        pathId)
            if self.map.has_key(path):
                assert(self.map[path][0] == pathId)
            self.map[path] = (pathId, version, fileStream.fileId())

    def merge(self, idDict):
        # merges the ids contained in idDict into this object; existing
        # id's are preferred
        idDict.update(self.map)
        self.map = idDict

# -------------------- public below this line -------------------------

class CookCallback(lookaside.ChangesetCallback, callbacks.CookCallback):

    def buildingChangeset(self):
        self._message('Building changeset...')

    def findingTroves(self, num):
        self._message('Finding %s troves...' % num)

    def gettingTroveDefinitions(self, num):
        self._message('Getting %s trove definitions...' % num)

    def buildingGroup(self, groupName, idx, total):
        self.setPrefix('%s (%s/%s): ' % (groupName, idx, total))

    def groupBuilt(self):
        self.clearPrefix()
        self.done()

    def groupResolvingDependencies(self):
        self._message('Resolving dependencies...')

    def groupCheckingDependencies(self):
        self._message('Checking dependency closure...')

    def groupCheckingPaths(self, current):
        self._message('Checking for path conflicts: %d' % (current))

    def groupDeterminingPathConflicts(self, total):
        self._message('Determining the %s paths involved in the path conflicts' % total)

    def __init__(self, *args, **kw):
        callbacks.CookCallback.__init__(self, *args, **kw)
        lookaside.ChangesetCallback.__init__(self, *args, **kw)


def _signTrove(trv, fingerprint):
    if fingerprint is not None:
        trv.addDigitalSignature(fingerprint)
    else:
        # if no fingerprint, just add sha1s
        trv.computeSignatures()

def signAbsoluteChangeset(cs, fingerprint=None):
    # adds signatures or at least sha1s (if fingerprint is None)
    # to an absolute changeset

    # go through all the trove change sets we have in this changeset.
    # use a list comprehension here as we will be modifying the newTroves
    # dictionary inside the changeset
    for troveCs in [ x for x in cs.iterNewTroveList() ]:
        # instantiate each trove from the troveCs so we can generate
        # the signature
        t = trove.Trove(troveCs)
        _signTrove(t, fingerprint)
        # create a new troveCs that has the new signature included in it
        newTroveCs = t.diff(None, absolute = 1)[0]
        # replace the old troveCs with the new one in the changeset
        cs.newTrove(newTroveCs)
    return cs

def getRecursiveRequirements(db, troveList, flavorPath):
    # gets the recursive requirements for the listed packages
    seen = set()
    while troveList:
        depSetList = []
        for trv in db.getTroves(list(troveList), withFiles=False):
            required = deps.DependencySet()
            oldRequired = trv.getRequires()
            [ required.addDep(*x) for x in oldRequired.iterDeps() 
              if x[0] != deps.AbiDependency ]
            depSetList.append(required)
        seen.update(troveList)
        sols = db.getTrovesWithProvides(depSetList, splitByDep=True)
        troveList = set()
        for depSetSols in sols.itervalues():
            for depSols in depSetSols:
                bestChoices = []
                # if any solution for a dep is satisfied by the installFlavor
                # path, then choose the solutions that are satisfied as 
                # early as possible on the flavor path.  Otherwise return
                # all solutions.
                for flavor in flavorPath:
                    bestChoices = [ x for x in depSols if flavor.satisfies(x[2])]
                    if bestChoices:
                        break
                if bestChoices:
                    depSols = set(bestChoices)
                else:
                    depSols = set(depSols)
                depSols.difference_update(seen)
                troveList.update(depSols)
    return seen

def cookObject(repos, cfg, recipeClass, sourceVersion,
               changeSetFile = None, prep=True, macros={},
               targetLabel = None, resume = None, alwaysBumpCount = False,
               allowUnknownFlags = False, allowMissingSource = False,
               ignoreDeps = False, logBuild = False,
               crossCompile = None, callback = None,
               requireCleanSources = False, downloadOnly = False):
    """
    Turns a recipe object into a change set, and sometimes commits the
    result.

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @type sourceVersion: the full conary verison of the recipeClass we are 
    cooking.  This source trove version should exist.  If you know what you
    are doing, you can create troves with non-existant source versions 
    by setting allowMissingSource 
    @param changeSetFile: if set, the changeset is stored in this file
    instead of committed to a repository
    @type changeSetFile: str
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param downloadOnly: If true, the lookaside is populated, and the None is
    returned instead of a changeset.
    @type downloadOnly: boolean
    @param macros: set of macros for the build
    @type macros: dict
    @param targetLabel: label to use for the cooked troves; it is used
    as a new shadow from whatever version was previously built
    default), the sourceVersion label is used
    @type targetLabel: versions.Label
    @param resume: indicates whether to resume the previous build.  If True,
    resume at the line of last breakage.  If an integer, resume at that line.
    If 'policy', rerun the policy only.  Note that resume is only valid when
    cooking a recipe from a file, not from the repository.  
    @type resume: bool or str
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    @param allowMissingSource: if True, do not complain if the sourceVersion
    specified does not point to an existing source trove.  Warning -- this
    can lead to strange trove setups in the repository
    @type logBuild: bool
    @param logBuild: if True, log the build to a file that will be included
    in the changeset
    @param allowChangeSet: allow build of this trove when the source version
    specified does not point to an existing source trove.  Warning -- this
    can lead to strange trove setups
    @type allowMissingSource: bool
    @param requireCleanSources: require that this build be clean - that its 
    sources all be from the repository.
    @rtype: list of strings
    """
    if not isinstance(recipeClass, (list, tuple)):
        recipeClasses = [recipeClass]
    else:
        recipeClasses = recipeClass
        recipeClass = recipeClass[0]
        # every recipe passed into build at once must have the same
        # name and version.
        assert(len(set((x.name, x.version) for x in recipeClasses)) == 1)

    if not callback:
        callback = callbacks.CookCallback()

    if not (hasattr(recipeClass, 'name') and hasattr(recipeClass, 'version')):
        raise CookError('recipe class must have name and version defined')
    if '-' in recipeClass.version:
        raise builderrors.RecipeFileError(
            "Version string %s has illegal '-' character" %recipeClass.version)

    if not use.Arch.keys():
        log.error('No architectures have been defined in %s -- '
                  'cooking is not possible' % ' '.join(cfg.archDirs)) 
        sys.exit(1)

    # check to make sure that policy exists
    policyFound = False
    for policyDir in cfg.policyDirs:
        if os.path.isdir(policyDir):
            policyFound = True
            break
    if not policyFound:
        log.error('No conary policy directories were found.  '
                  'You probably need to install\n'
                  'conary-policy.  Try "conary update conary-policy".')
	sys.exit(1)

    use.allowUnknownFlags(allowUnknownFlags)
    fullName = recipeClass.name

    srcName = fullName + ':source'

    if repos:
        try: 
            trove = repos.getTrove(srcName, sourceVersion, deps.Flavor(),
                                   withFiles = False)
            sourceVersion = trove.getVersion()
        except errors.TroveMissing:
            if not allowMissingSource and targetLabel != versions.CookLabel():
                raise RuntimeError, ('Cooking with non-existant source'
                                     ' version %s' % sourceVersion.asString())
        except errors.OpenError:
            if targetLabel != versions.CookLabel():
                raise
            if not sourceVersion.isOnLocalHost():
                log.warning('Could not open repository -- not attempting to'
                            ' share pathId information with the'
                            ' repository. This cook will create new versions'
                            ' for all files on update.')
                time.sleep(3)
                repos = None


    buildBranch = sourceVersion.branch()
    assert(not buildBranch.timeStamps() or max(buildBranch.timeStamps()) != 0)

    macros['buildbranch'] = buildBranch.asString()
    macros['buildlabel'] = buildBranch.label().asString()

    if targetLabel:
        signatureLabel = targetLabel
        signatureKey = selectSignatureKey(cfg, targetLabel)
    else:
        signatureKey = selectSignatureKey(cfg, sourceVersion.trailingLabel())

    db = database.Database(cfg.root, cfg.dbPath)
    type = recipeClass.getType()
    if recipeClass.getType() == recipe.RECIPE_TYPE_GROUP:
        ret = cookGroupObjects(repos, db, cfg, recipeClasses, sourceVersion,
                               macros = macros, targetLabel = targetLabel,
                               alwaysBumpCount = alwaysBumpCount,
                               callback = callback)
        needsSigning = True
    else:
        assert(len(recipeClasses) == 1) 
        buildFlavor = getattr(recipeClass, '_buildFlavor', cfg.buildFlavor)
        try:
            use.setBuildFlagsFromFlavor(recipeClass.name, buildFlavor,
                                        error=False, warn=True)
        except AttributeError, msg:
            log.error('Error setting build flags from flavor %s: %s' % (
                                            buildFlavor, msg))
            sys.exit(1)

        if type in (recipe.RECIPE_TYPE_INFO,
                      recipe.RECIPE_TYPE_PACKAGE):
            ret = cookPackageObject(repos, db, cfg, recipeClass, 
                                sourceVersion, 
                                prep = prep, macros = macros,
                                targetLabel = targetLabel,
                                resume = resume, 
                                alwaysBumpCount = alwaysBumpCount, 
                                ignoreDeps = ignoreDeps, 
                                logBuild = logBuild,
                                crossCompile = crossCompile,
                                requireCleanSources = requireCleanSources,
                                downloadOnly = downloadOnly,
                                signatureKey = signatureKey)
            needsSigning = False
        elif type == recipe.RECIPE_TYPE_REDIRECT:
            ret = cookRedirectObject(repos, db, cfg, recipeClass,
                                  sourceVersion,
                                  macros = macros, 
                                  targetLabel = targetLabel,
                                  alwaysBumpCount = alwaysBumpCount)
            needsSigning = True
        elif type == recipe.RECIPE_TYPE_FILESET:
            ret = cookFilesetObject(repos, db, cfg, recipeClass, 
                                    sourceVersion, 
                                    macros = macros, 
                                    targetLabel = targetLabel,
                                    alwaysBumpCount = alwaysBumpCount)
            needsSigning = True
        else:
            raise AssertionError

    # cook*Object returns None if using prep or downloadOnly
    if ret is None:
        return []

    (cs, built, cleanup) = ret

    if needsSigning:
        # sign the changeset
        signAbsoluteChangeset(cs, signatureKey)

    if changeSetFile:
        cs.writeToFile(changeSetFile)
    else:
        repos.commitChangeSet(cs, callback = callback)

    if cleanup:
        (fn, args) = cleanup
        fn(*args)

    return built

def cookRedirectObject(repos, db, cfg, recipeClass, sourceVersion, macros={},
		    targetLabel = None, alwaysBumpCount=False):
    """
    Turns a redirect recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and None (for compatibility with cookPackageObject).

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param macros: set of macros for the build
    @type macros: dict
    @rtype: tuple
    @param targetLabel: label to use for the cooked troves; it is used
    as a new branch from whatever version was previously built
    default), the label from sourceVersion is used
    @type targetLabel: versions.Label
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    @param redirect: if True, a redirect trove is built instead of a
    normal trove.
    """

    fullName = recipeClass.name

    recipeObj = recipeClass(repos, cfg, sourceVersion.branch(), cfg.flavor, 
                            macros)

    use.track(True)
    _callSetup(cfg, recipeObj)
    recipeObj.findTroves()
    use.track(False)

    log.info('Building %s=%s[%s]' % ( recipeClass.name,
                                      sourceVersion.branch().label(),
                                      use.usedFlagsToFlavor(recipeClass.name)))

    redirects = recipeObj.getRedirections()

    changeSet = changeset.ChangeSet()
    built = []

    flavors = set()
    for (fromName, fromFlavor), (toName, toBranch, toFlavor,
                                 subTroveList) in redirects.iteritems():
        flavors.add(fromFlavor)

    targetVersion = nextVersion(repos, db, fullName, sourceVersion, 
                                flavors, targetLabel, 
                                alwaysBumpCount=alwaysBumpCount)

    redirList = []
    childList = []
    for (fromName, fromFlavor), (toName, toBranch, toFlavor,
                                 subTroveList) in redirects.iteritems():
        redir = trove.Trove(fromName, targetVersion, fromFlavor, 
                            None, type = trove.TROVE_TYPE_REDIRECT)

        redirList.append(redir.getNameVersionFlavor())

        for subName in subTroveList:
            redir.addTrove(subName, targetVersion, fromFlavor)
            childList.append((subName, targetVersion, fromFlavor))

        if toName is not None:
            redir.addRedirect(toName, toBranch, toFlavor)

        redir.setBuildTime(time.time())
        redir.setSourceName(fullName + ':source')
        redir.setConaryVersion(constants.version)
        redir.setIsCollection(False)

        trvDiff = redir.diff(None, absolute = 1)[0]
        changeSet.newTrove(trvDiff)
        built.append((redir.getName(), redir.getVersion().asString(), 
                      redir.getFlavor()) )

    changeSet.setPrimaryTroveList(set(redirList) - set(childList))

    return (changeSet, built, None)

def cookGroupObjects(repos, db, cfg, recipeClasses, sourceVersion, macros={},
                     targetLabel = None, alwaysBumpCount=False, 
                     callback = callbacks.CookCallback()):
    """
    Turns a group recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and None (for compatibility with cookPackageObject).

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param macros: set of macros for the build
    @type macros: dict
    @rtype: tuple
    @param targetLabel: label to use for the cooked troves; it is used
    as a new branch from whatever version was previously built
    default), the label from sourceVersion is used
    @type targetLabel: versions.Label
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    @param redirect: if True, a redirect trove is built instead of a
    normal trove.
    """
    troveCache = grouprecipe.TroveCache(repos, callback)

    changeSet = changeset.ChangeSet()

    builtGroups = []
    groupNames = []
    groupFlavors = []
    for recipeClass in recipeClasses:
        fullName = recipeClass.name
        buildFlavor = getattr(recipeClass, '_buildFlavor', cfg.buildFlavor)
        use.setBuildFlagsFromFlavor(recipeClass.name, buildFlavor)
        recipeObj = recipeClass(repos, cfg, sourceVersion.branch().label(),
                                buildFlavor, macros)
        use.track(True)
        _callSetup(cfg, recipeObj)
        use.track(False)
        log.info('Building %s=%s[%s]' % ( recipeClass.name,
                                      sourceVersion.branch().label(),
                                      use.usedFlagsToFlavor(recipeClass.name)))

        flavors = [buildpackage._getUseFlavor(recipeObj)]

        grouprecipe.buildGroups(recipeObj, cfg, repos, callback, 
                                troveCache=troveCache)

        callback.buildingChangeset()

        for group in recipeObj.iterGroupList():
            flavors.extend(x[2] for x in group.iterTroveList())
        grpFlavor = deps.mergeFlavorList(flavors,
                                         deps.DEP_MERGE_TYPE_DROP_CONFLICTS)
        builtGroups.append((recipeObj, grpFlavor))
        groupNames.extend(recipeObj.getGroupNames())
        groupFlavors.append(grpFlavor)

    targetVersion = nextVersion(repos, db, groupNames, sourceVersion, 
                                groupFlavors, targetLabel,
                                alwaysBumpCount=alwaysBumpCount)
    buildTime = time.time()

    built = []
    for recipeObj, grpFlavor in builtGroups:
        for group in recipeObj.iterGroupList():
            groupName = group.name
            grpTrv = trove.Trove(groupName, targetVersion, grpFlavor, None)
            grpTrv.setRequires(group.getRequires())

            provides = deps.DependencySet()
            provides.addDep(deps.TroveDependencies, deps.Dependency(groupName))
            grpTrv.setProvides(provides)


            grpTrv.setBuildTime(buildTime)
            grpTrv.setSourceName(fullName + ':source')
            grpTrv.setSize(group.getSize())
            grpTrv.setConaryVersion(constants.version)
            grpTrv.setIsCollection(True)
            grpTrv.setLabelPath(recipeObj.getLabelPath())

            for (troveTup, explicit, byDefault, comps) in group.iterTroveListInfo():
                grpTrv.addTrove(byDefault = byDefault,
                                weakRef=not explicit, *troveTup)

            # add groups which were newly created by this group. 
            for name, byDefault, explicit in group.iterNewGroupList():
                grpTrv.addTrove(name, targetVersion, grpFlavor, 
                                byDefault = byDefault, 
                                weakRef = not explicit)

            grpDiff = grpTrv.diff(None, absolute = 1)[0]
            changeSet.newTrove(grpDiff)

            built.append((grpTrv.getName(), str(grpTrv.getVersion()),
                                            grpTrv.getFlavor()))

        for primaryName in recipeObj.getPrimaryGroupNames():
            changeSet.addPrimaryTrove(primaryName, targetVersion, grpFlavor)

    return (changeSet, built, None)

def cookFilesetObject(repos, db, cfg, recipeClass, sourceVersion, macros={},
		      targetLabel = None, alwaysBumpCount=False):
    """
    Turns a fileset recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and None (for compatibility with cookPackageObject).

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param macros: set of macros for the build
    @type macros: dict
    @param targetLabel: label to use for the cooked troves; it is used
    as a new branch from whatever version was previously built
    default), the sourceVersion's branch is used
    @type targetLabel: versions.Label
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    @rtype: tuple
    """

    fullName = recipeClass.name

    recipeObj = recipeClass(repos, cfg, sourceVersion.branch().label(), 
                            cfg.flavor, macros)
    _callSetup(cfg, recipeObj)

    log.info('Building %s=%s[%s]' % ( recipeClass.name,
                                      sourceVersion.branch().label(),
                                      use.usedFlagsToFlavor(recipeClass.name)))

    recipeObj.findAllFiles()

    changeSet = changeset.ChangeSet()

    l = []
    flavor = deps.Flavor()
    size = 0
    fileObjList = repos.getFileVersions([ (x[0], x[2], x[3]) for x in 
                                                recipeObj.iterFileList() ])
    for (pathId, path, fileId, version), fileObj in \
                        itertools.izip(recipeObj.iterFileList(), fileObjList):
	l.append((pathId, path, version, fileId, fileObj.flags.isConfig()))
        if fileObj.hasContents:
            size += fileObj.contents.size()

	if fileObj.hasContents:
	    flavor.union(fileObj.flavor())
	changeSet.addFile(None, fileId, fileObj.freeze())

	# Since the file is already in the repository (we just got it from
	# there, so it must be there!) leave the contents out. this
	# means that the change set we generate can't be used as the 
	# source of an update, but it saves sending files across the
	# network for no reason. For local builds we go back through this
        # list and grab the contents after we've determined the target
        # version

    targetVersion = nextVersion(repos, db, fullName, sourceVersion, flavor, 
                                targetLabel, alwaysBumpCount=alwaysBumpCount)

    fileset = trove.Trove(fullName, targetVersion, flavor, None)
    provides = deps.DependencySet()
    provides.addDep(deps.TroveDependencies, deps.Dependency(fullName))
    fileset.setProvides(provides)

    for (pathId, path, version, fileId, isConfig) in l:
	fileset.addFile(pathId, path, version, fileId)

    fileset.setBuildTime(time.time())
    fileset.setSourceName(fullName + ':source')
    fileset.setSize(size)
    fileset.setConaryVersion(constants.version)
    fileset.setIsCollection(False)
    fileset.computePathHashes()
    
    filesetDiff = fileset.diff(None, absolute = 1)[0]
    changeSet.newTrove(filesetDiff)
    changeSet.addPrimaryTrove(fullName, targetVersion, flavor)

    if targetVersion.isOnLocalHost():
        # We need the file contents. Go get 'em

        # pass (fileId, fileVersion)
        contentList = repos.getFileContents([ (x[3], x[2]) for x in l ])
        for (pathId, path, version, fileId, isConfig), contents in \
                                                itertools.izip(l, contentList):
            changeSet.addFileContents(pathId, changeset.ChangedFileTypes.file, 
                                      contents, isConfig)

    built = [ (fileset.getName(), fileset.getVersion().asString(), 
                                                fileset.getFlavor()) ]
    return (changeSet, built, None)

def cookPackageObject(repos, db, cfg, recipeClass, sourceVersion, prep=True, 
                      macros={}, targetLabel = None, 
                      resume = None, alwaysBumpCount=False, 
                      ignoreDeps=False, logBuild=False, crossCompile = None,
                      requireCleanSources = False, downloadOnly = False,
                      signatureKey = None):
    """
    Turns a package recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and a tuple with a function to call and its arguments, which should
    be called when the build root for the package can be safely removed
    (the changeset returned refers to files in that build root, so those
    files can't be removed until the changeset has been comitted or saved)

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param downloadOnly: If true, the lookaside is populated, and the None is
    returned instead of a changeset.
    @type downloadOnly: boolean
    @param macros: set of macros for the build
    @type macros: dict
    @param targetLabel: label to use for the cooked troves; if None (the
    default), the version used is the derived from sourceVersion
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    @param signatureKey: GPG fingerprint for trove signing. If None, only
    sha1 signatures are generated.
    @type signatureKey: string
    @rtype: tuple
    """
    # 1. create the desired files in destdir and package info
    enforceManagedPolicy = (cfg.enforceManagedPolicy
                            and targetLabel != versions.CookLabel()
                            and not prep and not downloadOnly)

    result  = _cookPackageObject(repos, cfg, recipeClass, 
                                 sourceVersion, prep=prep,
                                 macros=macros, resume=resume,
                                 ignoreDeps=ignoreDeps, 
                                 logBuild=logBuild, 
                                 crossCompile=crossCompile,
                                 enforceManagedPolicy=enforceManagedPolicy,
                                 requireCleanSources = requireCleanSources,
                                 downloadOnly = downloadOnly)
    if type(result) is not tuple:
        return

    (bldList, recipeObj, builddir, destdir, policyTroves) = result
    
    # 2. convert the package into a changeset ready for committal
    changeSet, built = _createPackageChangeSet(repos, db, cfg, bldList,
                           recipeObj, sourceVersion,
                           targetLabel=targetLabel,
                           alwaysBumpCount=alwaysBumpCount,
                           policyTroves=policyTroves,
                           signatureKey = signatureKey)

    return (changeSet, built, (recipeObj.cleanup, (builddir, destdir)))

def _cookPackageObject(repos, cfg, recipeClass, sourceVersion, prep=True, 
		       macros={}, resume = None, ignoreDeps=False, 
                       logBuild=False, crossCompile=None, 
                       enforceManagedPolicy=False,  requireCleanSources = False,
                       downloadOnly = False):
    """Builds the package for cookPackageObject.  Parameter meanings are 
       described there.
    """
    fullName = recipeClass.name

    lcache = lookaside.RepositoryCache(repos)

    if requireCleanSources:
        srcdirs = []
    else:
        srcdirs = [ os.path.dirname(recipeClass.filename),
                    cfg.sourceSearchDir % {'pkgname': recipeClass.name} ]
    recipeObj = recipeClass(cfg, lcache, srcdirs, macros, crossCompile)

    for k, v in cfg.environment.items():
        os.environ[k] = v % recipeObj.macros

    recipeObj.populateLcache()
    recipeObj.isatty(sys.stdout.isatty() and sys.stdin.isatty())
    recipeObj.sourceVersion = sourceVersion
    
    builddir = util.normpath(cfg.buildPath) + "/" + recipeObj.name
    use.track(True)
    if recipeObj._trackedFlags is not None:
        use.setUsed(recipeObj._trackedFlags)

    policyFiles = recipeObj.loadPolicy()
    db = database.Database(cfg.root, cfg.dbPath)
    policyTroves = set()
    unmanagedPolicyFiles = []
    for policyPath in policyFiles:
        troveList = list(db.iterTrovesByPath(policyPath))
        if troveList:
            for trove in troveList:
                policyTroves.add((trove.getName(), trove.getVersion(),
                                  trove.getFlavor()))
        else:
            unmanagedPolicyFiles.append(policyPath)
            ver = versions.VersionFromString('/local@local:LOCAL/0-0').copy()
            ver.resetTimeStamps()
            policyTroves.add((policyPath, ver, deps.Flavor()))
    del db
    if unmanagedPolicyFiles and enforceManagedPolicy:
        raise CookError, ('Cannot cook into repository with'
            ' unmanaged policy files: %s' %', '.join(unmanagedPolicyFiles))

    _callSetup(cfg, recipeObj)

    log.info('Building %s=%s[%s]' % ( recipeClass.name,
                                      sourceVersion.branch().label(),
                                      use.usedFlagsToFlavor(recipeClass.name)))

    if not downloadOnly:
        # no point in checking/recording buildreqs when we're not building.
        recipeObj.checkBuildRequirements(cfg, sourceVersion,
                                         raiseError=not (ignoreDeps or prep))

    bldInfo = buildinfo.BuildInfo(builddir)
    recipeObj.buildinfo = bldInfo

    destdir = ''
    maindir = ''
    if not resume:
        destdir = ''
	if os.path.exists(builddir):
	    shutil.rmtree(builddir)
    else:
        try:
            bldInfo.read()
            if 'destdir' in bldInfo:
                destdir = bldInfo.destdir
            if 'maindir' in bldInfo:
                maindir = bldInfo.maindir
        except:
            pass

    util.mkdirChain(builddir + '/' + recipeObj.mainDir())
    if not destdir:
        destdir = builddir + '/_ROOT_'
    util.mkdirChain(destdir)

    if logBuild:
        # turn on logging of this trove.  Log is packaged as part
        # of :debug component
        logPath = destdir + recipeObj.macros.buildlogpath
        # during the build, keep the log file in the same dir as buildinfo.
        # that will make it more accessible for debugging.  At the end of 
        # the build, copy to the correct location
        tmpLogPath = builddir + '/' + os.path.basename(logPath)
        # this file alone is not enough to make us build a package
        recipeObj._autoCreatedFileCount += 1
        util.mkdirChain(os.path.dirname(logPath))
        # touch the logPath file so that the build process expects
        # a file there for packaging
        open(logPath, 'w')
        try:
            logFile = logger.startLog(tmpLogPath)
        except OSError, err:
            if err.args[0] == 'out of pty devices':
                log.warning('*** No ptys found -- not logging build ***')
                logBuild = False
                # don't worry about cleaning up the touched log file --
                # it's in the build dir and will be erased when the build 
                # is finished
            else:
                raise
        if logBuild:
            if recipeObj.isatty():
                out = logFile
            else:
                out = sys.stdout
            logBuildEnvironment(logFile, sourceVersion, policyTroves,
                                recipeObj.macros, cfg)
    try:
        bldInfo.begin()
        bldInfo.destdir = destdir
        if maindir:
            recipeObj.mainDir(maindir)
        if resume is True:
            resume = bldInfo.lastline

        recipeObj.unpackSources(builddir, destdir, resume, downloadOnly=downloadOnly)

        # if we're only extracting or downloading, continue to the next recipe class.
        if prep or downloadOnly:
            return recipeObj

        cwd = os.getcwd()
        try:
            os.chdir(builddir + '/' + recipeObj.mainDir())
            recipeObj.doBuild(builddir, resume=resume)
            if resume and resume != "policy" and \
                          recipeObj.resumeList[-1][1] != False:
                log.info('Finished Building %s Lines %s, Not Running Policy', 
                                                       recipeClass.name, resume)
                return
            log.info('Processing %s', recipeClass.name)
            if not resume:
                # test suite policy does not work well with restart, and
                # is generally useful mainly when cooking into repo, where
                # restart is not allowed
                recipeObj.doProcess(policy.TESTSUITE)
            recipeObj.doProcess(policy.DESTDIR_PREPARATION)
            recipeObj.doProcess(policy.DESTDIR_MODIFICATION)
            # cannot restart after the beginning of policy.PACKAGE_CREATION
            bldInfo.stop()
            use.track(False)
            recipeObj.doProcess(policy.PACKAGE_CREATION)
            recipeObj.doProcess(policy.PACKAGE_MODIFICATION)
            recipeObj.doProcess(policy.ENFORCEMENT)
            recipeObj.doProcess(policy.ERROR_REPORTING)
        finally:
            os.chdir(cwd)
    
        grpName = recipeClass.name

        bldList = recipeObj.getPackages()
        if (not bldList or
            sum(len(x) for x in bldList) <= recipeObj._autoCreatedFileCount):
            # no components in packages, or no explicit files in components
            log.error('No files were found to add to package %s'
                      %recipeClass.name)
            return

    except Exception, msg:
        if logBuild:
            logFile.write('%s\n' % msg)
            logFile.write(''.join(traceback.format_exception(*sys.exc_info())))
            logFile.write('\n')
            logFile.close()
        if cfg.debugRecipeExceptions:
            traceback.print_exception(*sys.exc_info())
            debugger.post_mortem(sys.exc_info()[2])
        raise

    if logBuild and recipeObj._autoCreatedFileCount:
        logFile.close()
        if os.path.exists(logPath):
            os.unlink(logPath)
        if not cfg.cleanAfterCook:
            # leave the easily accessible copy in place in 
            # builddir
            shutil.copy2(tmpLogPath, logPath)
        else:
            os.rename(tmpLogPath, logPath)
        # update contents on the buildlog, since they changed
        buildlogpath = recipeObj.macros.buildlogpath
        recipeObj.autopkg.updateFileContents(
            recipeObj.macros.buildlogpath, logPath)
        recipeObj.autopkg.pathMap[buildlogpath].tags.set("buildlog")
    return bldList, recipeObj, builddir, destdir, policyTroves

def _createPackageChangeSet(repos, db, cfg, bldList, recipeObj, sourceVersion,
                            targetLabel=None, alwaysBumpCount=False,
                            policyTroves=None, signatureKey = None):
    """ Helper function for cookPackage object.  See there for most
        parameter definitions. BldList is the list of
        components created by cooking a package recipe.  RecipeObj is
        the instantiation of the package recipe.
    """
    # determine final version and flavor  - flavor is shared among
    # all components, so just grab the first one.
    flavor = bldList[0].flavor.copy()
    componentNames = [ x.name for x in bldList ]
    targetVersion = nextVersion(repos, db, componentNames, sourceVersion, 
                                flavor, targetLabel, 
                                alwaysBumpCount=alwaysBumpCount)

    buildTime = time.time()
    sourceName = recipeObj.__class__.name + ':source'

    buildReqs = set((x.getName(), x.getVersion(), x.getFlavor())
                    for x in recipeObj.buildReqMap.itervalues())
    buildReqs = getRecursiveRequirements(db, buildReqs, cfg.flavor)

    # create all of the package troves we need, and let each package provide
    # itself
    grpMap = {}
    for buildPkg in bldList:
        compName = buildPkg.getName()
        main, comp = compName.split(':')
        if main not in grpMap:
            grpMap[main] = trove.Trove(main, targetVersion, flavor, None)
            grpMap[main].setSize(0)
            grpMap[main].setSourceName(sourceName)
            grpMap[main].setBuildTime(buildTime)
            grpMap[main].setConaryVersion(constants.version)
            if policyTroves:
                grpMap[main].setPolicyProviders(policyTroves)
            grpMap[main].setLoadedTroves(recipeObj.getLoadedTroves())
            grpMap[main].setBuildRequirements(buildReqs)
	    provides = deps.DependencySet()
	    provides.addDep(deps.TroveDependencies, deps.Dependency(main))
	    grpMap[main].setProvides(provides)
            grpMap[main].setIsCollection(True)

    # look up the pathids used by our immediate predecessor troves.
    ident = _IdGen()

    searchBranch = targetVersion.branch()
    if targetLabel:
        # this keeps cook and emerge branchs from showing up
        searchBranch = searchBranch.parentBranch()

    if repos and not searchBranch.getHost() == 'local':
        versionDict = dict( [ (x, { searchBranch : None } ) for x in grpMap ] )
        versionDict = repos.getTroveLeavesByBranch(versionDict)
        if not versionDict and searchBranch.hasParentBranch():
            # there was no match on this branch; look uphill
            searchBranch = searchBranch.parentBranch()
            versionDict = dict((x, { searchBranch : None } ) for x in grpMap )
            versionDict = repos.getTroveLeavesByBranch(versionDict)

        log.info('looking up pathids from repository history')
        # look up the pathids for every file that has been built by
        # this source component, following our brach ancestry
        while True:
            d = repos.getPackageBranchPathIds(sourceName, searchBranch)
            ident.merge(d)

            if not searchBranch.hasParentBranch():
                break
            searchBranch = searchBranch.parentBranch()

    log.info('pathId lookup complete')

    built = []
    packageList = []
    perviousQuery = {}
    for buildPkg in bldList:
        # bldList only contains components
        compName = buildPkg.getName()
        main, comp = compName.split(':')
        assert(comp)
        grp = grpMap[main]

	(p, fileMap) = _createComponent(repos, buildPkg, targetVersion, ident)

	built.append((compName, p.getVersion().asString(), p.getFlavor()))

	packageList.append((None, p, fileMap))
        p.setSourceName(sourceName)
        p.setBuildTime(buildTime)
        p.setConaryVersion(constants.version)
        p.setIsCollection(False)

        _signTrove(p, signatureKey)

	byDefault = recipeObj.byDefault(compName)
        grp.addTrove(compName, p.getVersion(), p.getFlavor(),
                     byDefault = byDefault)
        if byDefault:
            grp.setSize(grp.getSize() + p.getSize())

    if not targetVersion.isOnLocalHost():
        # this keeps cook and emerge branchs from showing up
        searchBranch = targetVersion.branch()
        previousVersions = repos.getTroveLeavesByBranch(
                dict(
                    ( x[1].getName(), { targetVersion.branch() : [ flavor ] } )
                        for x in packageList ) )

        needTroves = []
        for name in previousVersions:
            prevVersion = previousVersions[name].keys()[0]
            prevFlavor = previousVersions[name][prevVersion][0]
            needTroves.append((name, prevVersion, prevFlavor))

        previousTroves = repos.getTroves(needTroves)
        previousTroveDict = dict( (x[0][0], x[1]) for x in
                                    itertools.izip(needTroves, previousTroves))

        relativePackageList = []
        needTroves = {}
        for empty, p, fileMap in packageList:
            if cfg.commitRelativeChangeset:
                oldTrove = previousTroveDict.get(p.getName(), None)
            else:
                oldTrove = None
            relativePackageList.append((oldTrove, p, fileMap))

        packageList = relativePackageList

    changeSet = changeset.CreateFromFilesystem(packageList)
    for packageName in grpMap:
        changeSet.addPrimaryTrove(packageName, targetVersion, flavor)

    for grp in grpMap.values():
        _signTrove(grp, signatureKey)
        grpDiff = grp.diff(None, absolute = 1)[0]
        changeSet.newTrove(grpDiff)

    return changeSet, built

def logBuildEnvironment(out, sourceVersion, policyTroves, macros, cfg):
    write = out.write

    write('Building %s=%s\n' % (macros.name, sourceVersion))
    write('using conary=%s\n' %constants.version)
    if policyTroves:
        write('and policy from:\n')
        wrap = textwrap.TextWrapper(
            initial_indent='    ',
            subsequent_indent='        ',
        )
        for troveTup in sorted(policyTroves):
            write(wrap.fill("'%s=%s[%s]'" %troveTup) + '\n')

    write('*' * 60 + '\n')
    write("Environment:\n")
    for key, value in sorted(os.environ.items()):
        write("%s=%s\n" % (key, value))

    write('*' * 60 + '\n')

    write("Use flags:\n")
    for flag in sorted(use.Use.keys()):
        write("%s\n" % (use.Use[flag]))

    write("*"*60 + '\n')

    write("Local flags:" + '\n')
    for flag in use.LocalFlags.keys():
        write("%s\n" % (use.LocalFlags[flag]))

    write("*"*60 +'\n')
    
    write("Arch flags:\n")
    for majarch in sorted(use.Arch.keys()):
        write("%s\n" % (use.Arch[majarch]))
        for subarch in sorted(use.Arch[majarch].keys()):
            write("%s\n" % (use.Arch[majarch][subarch]))

    write("*"*60 + '\n')

    write("Macros:" + '\n')
    for macro in sorted(macros.keys()):
        # important for this to be unexpanded, because at this point,
        # not some macros may not have an expansion
        write("%s: %s\n" % (macro, macros._get(macro)))

    write("*"*60 + '\n')

    write("Config:\n")
    cfg.setDisplayOptions(hidePasswords=True)
    for key in ('buildFlavor', 'buildLabel', 'contact', 'name', 'repositoryMap'):
        cfg.displayKey(key, out)
    write("*"*60 + '\n')

    write('START OF BUILD:\n\n')



def guessSourceVersion(repos, name, versionStr, buildLabel, 
                                                searchBuiltTroves=False):
    """ Make a reasonable guess at what a sourceVersion should be when 
        you don't have an actual source component from a repository to get 
        the version from.  Searches the repository for troves that are 
        relatively close to the desired trove, and grabbing their timestamp
        information.
        @param repos: repository client
        @type repos: NetworkRepositoryClient
        @param name: name of the trove being built
        @type name: str
        @param versionStr: the version stored in the recipe being built
        @type versionStr: str
        @param buildLabel: the label to search for troves matching the 
        @type buildLabel: versions.Label
        @param searchBuiltTroves: if True, search for binary troves  
        that match the desired trove's name, versionStr and label. 
        @type searchBuiltTroves: bool
    """
    srcName = name + ':source'
    sourceVerison = None
    if os.path.exists('CONARY'):
        conaryState = ConaryStateFromFile('CONARY', repos)
        if conaryState.hasSourceState():
            state = conaryState.getSourceState()
            if state.getName() == srcName and \
                            state.getVersion() != versions.NewVersion():
                stateVer = state.getVersion().trailingRevision().version
                if versionStr and stateVer != versionStr:
                    return state.getVersion().branch().createVersion(
                                versions.Revision('%s-1' % (versionStr)))
                return state.getVersion()
    # make an attempt at a reasonable version # for this trove
    # although the recipe we are cooking from may not be in any
    # repository
    if repos and buildLabel:
        versionDict = repos.getTroveLeavesByLabel(
                                    { srcName : { buildLabel : None } })
    else:
        versionDict = {}

    versionList = versionDict.get(srcName, {}).keys()
    if versionList:
        relVersionList  = [ x for x in versionList \
                if x.trailingRevision().version == versionStr ] 
        if relVersionList:
            relVersionList.sort()
            return relVersionList[-1]
        else:
            # we've got a reasonable branch to build on, but not
            # a sourceCount.  Reset the sourceCount to 1.
            versionList.sort()
            return versionList[-1].branch().createVersion(
                        versions.Revision('%s-1' % (versionStr)))
    if searchBuiltTroves:
        # XXX this is generally a bad idea -- search for a matching
        # built trove on the branch that our source version is to be
        # built on and reuse that branch.  But it's useful for cases
        # when you really know what you're doing and don't want to depend
        # on a source trove being in the repository.
        versionDict = repos.getTroveLeavesByLabel(
                                { name : { buildLabel : None } })
        versionList = versionDict.get(name, {}).keys()
        if versionList:
            relVersionList  = [ x for x in versionList \
                    if x.trailingRevision().version == versionStr ] 
            if relVersionList:
                relVersionList.sort()
                sourceVersion = relVersionList[-1].copy()
                sourceVersion.trailingRevision().buildCount = None
                return sourceVersion
            else:
                # we've got a reasonable branch to build on, but not
                # a sourceCount.  Reset the sourceCount to 1.
                versionList.sort()
                return versionList[-1].branch().createVersion(
                            versions.Revision('%s-1' % (versionStr)))
    return None

def getRecipeInfoFromPath(repos, cfg, recipeFile, buildFlavor=None):
    if buildFlavor is None:
        buildFlavor = cfg.buildFlavor

    if recipeFile[0] != '/':
        recipeFile = "%s/%s" % (os.getcwd(), recipeFile)

    pkgname = recipeFile.split('/')[-1].split('.')[0]


    try:
        use.setBuildFlagsFromFlavor(pkgname, buildFlavor, error=False)
    except AttributeError, msg:
        log.error('Error setting build flag values: %s' % msg)
        sys.exit(1)
    try:
        # make a guess on the branch to use since it can be important
        # for loading superclasses.
        sourceVersion = guessSourceVersion(repos, pkgname,
                                           None, cfg.buildLabel)
        if sourceVersion:
            branch = sourceVersion.branch()
        else:
            branch = None

        loader = loadrecipe.RecipeLoader(recipeFile, cfg=cfg, repos=repos,
                                         branch=branch, buildFlavor=buildFlavor)
        version = None
    except builderrors.RecipeFileError, msg:
        raise CookError(str(msg))

    recipeClass = loader.getRecipe()

    try:
        sourceVersion = guessSourceVersion(repos, recipeClass.name,
                                           recipeClass.version,
                                           cfg.buildLabel)
    except errors.OpenError:
        # pass this error here, we'll warn about the unopenable repository
        # later.
        sourceVersion = None

    if not sourceVersion:
        # just make up a sourceCount -- there's no version in 
        # the repository to compare against
        if not cfg.buildLabel:
            cfg.buildLabel = versions.LocalLabel()
        sourceVersion = versions.VersionFromString('/%s/%s-1' % (
                                               cfg.buildLabel.asString(),
                                               recipeClass.version))
        # the source version must have a time stamp
        sourceVersion.trailingRevision().resetTimeStamp()
    return loader, recipeClass, sourceVersion


def cookItem(repos, cfg, item, prep=0, macros={}, 
	     emerge = False, resume = None, allowUnknownFlags = False,
             showBuildReqs = False, ignoreDeps = False, logBuild = False,
             crossCompile = None, callback = None, requireCleanSources = None,
             downloadOnly = False):
    """
    Cooks an item specified on the command line. If the item is a file
    which can be loaded as a recipe, it's cooked and a change set with
    the result is saved. If that's not the case, the item is taken to
    be the name of a package, and the recipe is pulled from the :source
    component, built, and committed to the repository.

    @param repos: Repository to use for building
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param item: the item to cook
    @type item: str
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param downloadOnly: If true, the lookaside is populated, and the None is
    returned instead of a changeset.
    @type downloadOnly: boolean
    @param macros: set of macros for the build
    @type macros: dict
    """
    buildList = []
    changeSetFile = None
    targetLabel = None

    use.track(True)

    if isinstance(item, tuple):
        (name, versionStr, flavorList) = item
    else:
        (name, versionStr, flavor) = parseTroveSpec(item)
        flavorList = [flavor]

    use.allowUnknownFlags(allowUnknownFlags)
    recipeClassDict = {}
    loaders = []
    for flavor in flavorList:
        if flavor is not None:
            buildFlavor = deps.overrideFlavor(cfg.buildFlavor, flavor)
        else:
            buildFlavor = cfg.buildFlavor

        if name.endswith('.recipe') and os.path.isfile(name):
            if versionStr:
                raise CookError, \
                    ("Must not specify version string when cooking recipe file")

            loader, recipeClass, sourceVersion = \
                              getRecipeInfoFromPath(repos, cfg, name,
                                                    buildFlavor=buildFlavor)

            targetLabel = versions.CookLabel()
            if requireCleanSources is None:
                requireCleanSources = False

            changeSetFile = "%s-%s.ccs" % (recipeClass.name,
                                           recipeClass.version)
        else:
            if resume:
                raise CookError('Cannot use --resume argument when cooking in repository')
            if requireCleanSources is None:
                requireCleanSources = True

            if emerge:
                labelPath = cfg.installLabelPath
            else:
                labelPath = None

            try:
                use.setBuildFlagsFromFlavor(name, buildFlavor, error=False)
            except AttributeError, msg:
                log.error('Error setting build flag values: %s' % msg)
                sys.exit(1)

            try:
                (loader, sourceVersion) = \
                                loadrecipe.recipeLoaderFromSourceComponent(
                                            name, cfg, repos,
                                            versionStr=versionStr,
                                            labelPath = labelPath,
                                            buildFlavor=buildFlavor)[0:2]
            except builderrors.RecipeFileError, msg:
                raise CookError(str(msg))

            recipeClass = loader.getRecipe()
        loaders.append(loader)
        recipeClassDict.setdefault(sourceVersion, []).append(recipeClass)

        if showBuildReqs:
            if not recipeClass.getType() == recipe.RECIPE_TYPE_PACKAGE:
                raise CookError("--show-buildreqs is available only for PackageRecipe subclasses")
            recipeObj = recipeClass(cfg, None, [], lightInstance=True)
            sys.stdout.write('\n'.join(sorted(recipeObj.buildRequires)))
            sys.stdout.flush()
    if showBuildReqs:
        return None

    if emerge:
        (fd, changeSetFile) = tempfile.mkstemp('.ccs', "emerge-%s-" % name)
        os.close(fd)
        targetLabel = versions.EmergeLabel()

    built = None

    built = []
    if len(recipeClassDict) > 1 and changeSetFile:
        # this would involve reading the changeset from disk and merging 
        # or writing an interface for this very, very, unlikely case where the
        # version is based off of a use flavor.
        raise CookError("Cannot cook multiple versions of %s to change set" % name)
    for sourceVersion, recipeClasses in recipeClassDict.items():
        try:
            troves = cookObject(repos, cfg, recipeClasses, 
                                changeSetFile = changeSetFile,
                                prep = prep, macros = macros,
                                targetLabel = targetLabel,
                                sourceVersion = sourceVersion,
                                resume = resume,
                                allowUnknownFlags = allowUnknownFlags,
                                allowMissingSource=False, ignoreDeps=ignoreDeps,
                                logBuild=logBuild,
                                crossCompile=crossCompile,
                                callback=callback,
                                requireCleanSources=requireCleanSources,
                                downloadOnly = downloadOnly)
            if troves:
                built.extend(tuple(troves))
        except errors.RepositoryError, e:
            if emerge:
                os.unlink(changeSetFile)
            raise CookError(str(e))
    return tuple(built), changeSetFile

def cookCommand(cfg, args, prep, macros, emerge = False, 
                resume = None, allowUnknownFlags = False,
                showBuildReqs = False, ignoreDeps = False,
                profile = False, logBuild = True,
                crossCompile = None, cookIds=None, downloadOnly=False):
    # this ensures the repository exists
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    if cookIds:
        cookUid, cookGid = cookIds
    else:
        cookUid = cookGid = None

    if not os.getuid():
        if not cookUid or not cookGid:
            raise CookError('Do not cook as root')

    items = {}
    for idx, item in enumerate(args):
        (name, version, flavor) = parseTroveSpec(item)
        l = items.setdefault((name, version), (idx, set()))
        l[1].add(flavor)
    finalItems = []
    items = sorted(items.iteritems(), key=lambda x: x[1][0])

    for (name, version), (idx, flavorList) in items:
        # NOTE: most of the cook code is set up to allow
        # cooks to be shared when building multiple flavors
        # of the same trove.  However, troves with files in
        # them cannot handle creating one changeset w/ 
        # shared pathIds in them.  If this limitation of the 
        # changeset format ever gets fixed, we can remove this
        # check.
        if name.startswith('group-'):
            finalItems.append((name, version, flavorList))
        else:
            for flavor in flavorList:
                finalItems.append((name, version, [flavor]))

    for item in finalItems:
        # we want to fork here to isolate changes the recipe might make
        # in the environment (such as environment variables)
        # first, we need to ignore the tty output in the child process
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        # we need a pipe to enable communication with our child (mainly
        # for emerge)
        inpipe, outpipe = os.pipe()
        pid = os.fork()
        if not pid:
            # we have no need for the read side of the pipe
            os.close(inpipe)
            # make sure that the write side of the pipe is closed
            # when we fork/exec
            fcntl.fcntl(outpipe, fcntl.FD_CLOEXEC)

            if profile:
                import hotshot
                prof = hotshot.Profile('conary-cook.prof')
                prof.start()
            # child, set ourself to be the foreground process
            os.setpgrp()

            if cookGid:
                os.setgid(cookGid)
            if cookUid:
                os.setuid(cookUid)

            try:
                # the child should control stdin -- if stdin is a tty
                # that can be controlled, and we're not piping output
                # to some other process that should be controlling it
                # (like less).
                if sys.stdin.isatty() and sys.stdout.isatty():
                    os.tcsetpgrp(0, os.getpgrp())
            except AttributeError:
                # stdin might not even have an isatty method
                pass

	    # make sure we do not accidentally make files group-writeable
	    os.umask(0022)
	    # and if we do not create core files we will not package them
	    resource.setrlimit(resource.RLIMIT_CORE, (0,0))
            built = cookItem(repos, cfg, item, prep=prep, macros=macros,
                             emerge = emerge, resume = resume, 
                             allowUnknownFlags = allowUnknownFlags, 
                             showBuildReqs = showBuildReqs,
                             ignoreDeps = ignoreDeps, logBuild = logBuild,
                             crossCompile = crossCompile,
                             callback = CookCallback(),
                             downloadOnly = downloadOnly)
            components, csFile = built
            if not components:
                # --prep or --download or perhaps an error was logged
                if log.errorOccurred():
                    sys.exit(1)
                sys.exit(0)
            for component, version, flavor in sorted(components):
                print "Created component:", component, version,
                if flavor is not None:
                    print str(flavor).replace("\n", " "),
                print
            if csFile is None:
                print 'Changeset committed to the repository.'
            else:
                print 'Changeset written to:', csFile
                # send the changeset file over the pipe
                os.write(outpipe, csFile)
            if profile:
                prof.stop()
            sys.exit(0)
        else:
            # parent process, no need for the write side of the pipe
            os.close(outpipe)
            while 1:
                try:
                    (id, status) = os.waitpid(pid, os.WUNTRACED)
                    if os.WIFSTOPPED(status):
                        # if our child has been stopped (Ctrl+Z or similar)
                        # stop ourself
                        os.killpg(os.getpgrp(), os.WSTOPSIG(status))
                        # when we continue, place our child back
                        # in the foreground process group
                        os.tcsetpgrp(0, pid)
                        # tell the child to continue
                        os.kill(-pid, signal.SIGCONT)
                    else:
                        # if our child exited with a non-0 status, exit
                        # with that status
                        if os.WEXITSTATUS(status):
                            sys.exit(os.WEXITSTATUS(status))
                        break
                except KeyboardInterrupt:
                    # kill the entire process group
                    os.kill(-pid, signal.SIGINT)
        # see if the child process sent us a changeset filename over
        # the pipe
        csFile = os.read(inpipe, 1000)
        if emerge:
            # apply the changeset file written by the child
            if not csFile:
                log.error('The cook process did not return a changeset file')
                break
            print 'Applying changeset file %s' %csFile
            client = conaryclient.ConaryClient(cfg)
            try:
                cs = changeset.ChangeSetFromFile(csFile)
                job = [ (x[0], (None, None), (x[1], x[2]), True) for
                        x in cs.getPrimaryTroveList() ]
                callback = updatecmd.UpdateCallback()
                rc = client.updateChangeSet(job, recurse = True,
                                            resolveDeps = False,
                                            callback = callback,
                                            fromChangesets = [ cs ])
                client.applyUpdate(rc[0])
            except (conaryclient.UpdateError, errors.CommitError), e:
                log.error(e)
                log.error("Not committing changeset: please apply %s by "
                          "hand" % csFile)
            else:
                os.unlink(csFile)

        # make sure that we are the foreground process again
        try:
            # the child should control stdin -- if stdin is a tty
            # that can be controlled
            if sys.stdin.isatty():
                os.tcsetpgrp(0, os.getpgrp())
        except AttributeError:
            # stdin might not even have an isatty method
            pass

def _callSetup(cfg, recipeObj):
    try:
        return recipeObj.setup()
    except Exception, err:
        if cfg.debugRecipeExceptions:
            traceback.print_exception(*sys.exc_info())
            debugger.post_mortem(sys.exc_info()[2])
            raise CookError(str(err))

        filename = '<No File>'

        tb = sys.exc_info()[2]
        lastRecipeFrame = None
        while tb.tb_next:
            tb = tb.tb_next
            tbFileName = tb.tb_frame.f_code.co_filename
            if tbFileName.endswith('.recipe'):
                lastRecipeFrame = tb

        if not lastRecipeFrame:
            # too bad, we didn't find a file ending in .recipe in our 
            # stack.  Let's just assume the lowest frame is the right one.
            lastRecipeFrame = tb

        filename = lastRecipeFrame.tb_frame.f_code.co_filename
	linenum = lastRecipeFrame.tb_frame.f_lineno
        del tb, lastRecipeFrame
        raise CookError('%s:%s:\n %s: %s' % (filename, linenum, err.__class__.__name__, err))
