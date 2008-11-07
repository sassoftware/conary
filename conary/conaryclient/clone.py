#
# Copyright (c) 2005-2008 rPath, Inc.
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
"""
Implementation of "clone" + "promote" functionality.

Cloning creates a copy of a trove on a related branch, with the only link
back to the original branch being through the "clonedFrom" link.
"""
# NOTE FOR READING THE CODE: creating the copy is easy.  It's determining
# whether or not the clone is necessary that is complicated.  To that end 
# we have:
#
#   The chooser: The chooser contains the algorithm for determining whether
#                a particular trove should be cloned or not, and where it
#                should be cloned.
#
#   The leafMap: keeps track of the relevant current state of the repository -
#                what troves are at the leaves, and where they were cloned
#                from.
#
#   The cloneMap: keeps track of the relationship between troves we might clone
#                 and where they would be cloned to.
#
#   The cloneJob: keeps track of the actual clones we're going to perform
#                 as well as the clones we would perform but aren't because
#                 they have already been cloned.
#
# I've been thinking about combining the cloneMap and leafMap.

import itertools
import os
import tempfile
import time

from conary import callbacks
from conary import errors, files
from conary import trove
from conary import versions
from conary.build.nextversion import nextVersions
from conary.conarycfg import selectSignatureKey
from conary.deps import deps
from conary.lib import api, log
from conary.repository import changeset, filecontents
from conary.repository import trovesource
from conary.repository import errors as neterrors

V_LOADED = 0
V_BREQ = 1
V_REFTRV = 2

# don't change 
DEFAULT_MESSAGE = 1

class CloneJob(object):
    def __init__(self, options):
        self.cloneJob = {}
        self.preCloned = {}
        self.options = options

    def add(self, troveTup):
        self.cloneJob[troveTup] = None

    def alreadyCloned(self, troveTup):
        self.cloneJob.pop(troveTup, False)
        self.preCloned[troveTup] = True

    def target(self, troveTup, targetVersion):
        self.cloneJob[troveTup] = targetVersion

    def iterTargetList(self):
        return self.cloneJob.iteritems()

    def getTrovesToClone(self):
        return self.cloneJob.keys()

    def getPreclonedTroves(self):
        return self.preCloned.keys()

    def isEmpty(self):
        return not self.cloneJob

# target for the maximum number of files to handle in one pass
MAX_CLONE_FILES  = 5000
# threshhold for using a changeset instead of getting individual files
CHANGESET_MULTIPLE = 3

class ClientClone:
    __developer_api__ = True

    def createCloneChangeSet(self, targetBranch, troveList,
                             updateBuildInfo=True, message=DEFAULT_MESSAGE,
                             infoOnly=False, fullRecurse=False,
                             cloneSources=False, callback=None, 
                             trackClone=True, excludeGroups=False):
        targetMap = dict((x[1].branch(), targetBranch) for x in troveList)
        return self.createTargetedCloneChangeSet(targetMap,
                                               troveList,
                                               fullRecurse=fullRecurse,
                                               cloneSources=cloneSources,
                                               trackClone=trackClone,
                                               callback=callback,
                                               message=message,
                                               updateBuildInfo=updateBuildInfo,
                                               infoOnly=infoOnly,
                                               excludeGroups=excludeGroups)

    @api.developerApi
    def createTargetedCloneChangeSet(self, targetMap, troveList,
                                     updateBuildInfo=True, infoOnly=False,
                                     callback=None, message=DEFAULT_MESSAGE,
                                     trackClone=True, fullRecurse=True,
                                     cloneOnlyByDefaultTroves=False,
                                     cloneSources=True, excludeGroups=False):
        cloneOptions = CloneOptions(fullRecurse=fullRecurse,
                            cloneSources=cloneSources,
                            trackClone=trackClone,
                            callback=callback,
                            message=message,
                            cloneOnlyByDefaultTroves=cloneOnlyByDefaultTroves,
                            updateBuildInfo=updateBuildInfo,
                            infoOnly=infoOnly,
                            bumpGroupVersions=True,
                            excludeGroups=excludeGroups)
        chooser = CloneChooser(targetMap, troveList, cloneOptions)
        return self._createCloneChangeSet(chooser, cloneOptions)
    # bw compatibility
    createSiblingCloneChangeSet = createTargetedCloneChangeSet

    def createCloneChangeSetWithOptions(self, chooser, cloneOptions):
        return self._createCloneChangeSet(chooser, cloneOptions)

    def _createCloneChangeSet(self, chooser, cloneOptions):
        callback = cloneOptions.callback
        troveCache = TroveCache(self.repos, callback)

        cloneJob, cloneMap, leafMap = self._createCloneJob(cloneOptions,
                                                           chooser,
                                                           troveCache)
        if cloneJob.isEmpty():
            log.warning('Nothing to clone!')
            return False, None

        newTroveList = self._buildTroves(chooser, cloneMap, cloneJob,
                                         leafMap, troveCache, callback)
        if newTroveList is None:
            return False, None

        _logMe('new troves calculated')

        if cloneOptions.infoOnly:
            # build an absolute changeset. it's faster and easier.
            cs = changeset.ChangeSet()
            for oldVersion, newTrove in newTroveList:
                cs.newTrove(newTrove.diff(None, absolute = True)[0])
            callback.done()
            return True, cs

        finalCs = self._buildChangeSet(troveCache, newTroveList, callback)

        callback.prefix = ''
        callback.done()
        return True, finalCs

    def _buildChangeSet(self, troveCache, finalTroveList, callback):
        def _sameHost(v1, v2):
            return v1.trailingLabel().getHost() == v2.trailingLabel().getHost()

        # What should each TroveChangeSet be relative to? If the original
        # version was on the same server, great (because we don't have to
        # include any file contents!). Otherwise, look for something on the
        # target label because it is likely close to the new one.
        #
        # Note that what the diff is relative to is not the same as the
        # fromVersion in the finalTroveList. fromVersion is the version
        # we're cloning/shadowing from. The diff is always relative to
        # something in the target repository. We call the version the diff
        # is relative to the oldVersion.
        searchDict = {}
        for (fromVersion, finalTrove) in finalTroveList:
            if not _sameHost(fromVersion, finalTrove.getVersion()):
                name, version, flavor = finalTrove.getNameVersionFlavor()
                label = version.trailingLabel()
                searchDict.setdefault(name, {})
                searchDict[name].setdefault(label, [])
                searchDict[name][label].append(flavor)

        matches = self.repos.getTroveLeavesByLabel(searchDict)

        oldTrovesNeeded = []
        for (fromVersion, finalTrove) in finalTroveList:
            name, version, flavor = finalTrove.getNameVersionFlavor()
            if _sameHost(fromVersion, finalTrove.getVersion()):
                oldTrovesNeeded.append((name, fromVersion, flavor))
            else:
                match = None
                versionD = matches.get(name, {})
                for matchVersion, flavorList in versionD.iteritems():
                    if (matchVersion.trailingLabel() ==
                                version.trailingLabel()
                            and flavor in flavorList):
                        match = matchVersion

                if match is None:
                    # keep oldTrovesNeeded parallel to finalTroveList
                    oldTrovesNeeded.append(None)
                else:
                    oldTrovesNeeded.append((name, match, flavor))

        oldTroves = troveCache.getTroves(
            [ x for x in oldTrovesNeeded if x is not None ], withFiles=True)

        # we periodically write file contents to disk and merge in a new
        # changeset to save RAM. promotes can get large.
        #
        # Now to try and explain getting file streams and contents. If there
        # are few contents changed, we're better off using getFileVersions, but
        # if lots changes, we're better off just grabbing the whole bloody
        # changeset. If more than 1/3rd of the files changed, let's grab the
        # changeset. Note that this percent is completely arbitrary. We also
        # want to consolidate getFileVersions() and createChangeSet() calls.
        # Once we've found 5000 files to add to the current change set, we'll
        # add those, write the change set, merge it, and start again. Got all
        # that?
        finalCs = changeset.ReadOnlyChangeSet()
        cs = changeset.ChangeSet()
        fileCount = 0
        jobList = []
        jobFilesNeeded = []
        individualFilesNeeded = []

        # make sure we write out the final changeset
        lastTrove = finalTroveList[-1][1]
        for current, (oldTroveInfo, (fromVersion, finalTrove)) in \
                    enumerate(itertools.izip(oldTrovesNeeded, finalTroveList)):
            if oldTroveInfo is not None:
                oldTrove = oldTroves.pop(0)
                assert(_sameHost(oldTrove.getVersion(),
                       finalTrove.getVersion()))
            else:
                oldTrove = None

            # We can't trust filesNeeded diff returns here because it only
            # tells us about files whose fileId's have changed, but we need
            # to know about files whose versions changed as well (as those
            # may have moved servers). New files and changed files are
            # of interest (remember we're not necessarily diffing against
            # the fromVersion, so there could be new files).
            trvCs = finalTrove.diff(oldTrove,
                                    absolute = oldTrove is not None)[0]

            cs.newTrove(trvCs)

            # this is in the cache already, so there isn't any reason to
            # worry about optimizing the number of calls
            fromTrove = troveCache.getTrove((finalTrove.getName(),
                                fromVersion, finalTrove.getFlavor()))
            filesNeeded = []

            for pathId, path, newFileId, finalFileVersion in \
                                                    trvCs.getNewFileList():
                # oldFileId is None because this is a new file
                fromFileVersion = fromTrove.getFile(pathId)[2]
                filesNeeded.append((pathId, newFileId, None, fromFileVersion))

            for pathId, path, newFileId, finalFileVersion in \
                                                    trvCs.getChangedFileList():
                fromFileVersion = fromTrove.getFile(pathId)[2]
                if _sameHost(fromFileVersion, finalFileVersion):
                    # The server already has this file on it; no reason to
                    # commit it again
                    continue

                oldFileId = oldTrove.getFile(pathId)[1]
                filesNeeded.append((pathId, newFileId, oldFileId,
                                    fromFileVersion))


            if ((len(filesNeeded) * CHANGESET_MULTIPLE) >=
                                        finalTrove.fileCount()):
                # get the whole change set for this.
                jobList.append((finalTrove.getName(),
                                    (None, None),
                                    (fromVersion, finalTrove.getFlavor()),
                                True))
                # it's important that this have (pathId, newFileId) first
                # to ensure we're walking the changeset in the right order
                # after we sort it
                jobFilesNeeded += filesNeeded
            else:
                individualFilesNeeded += filesNeeded

            fileCount += len(filesNeeded)

            if finalTrove != lastTrove and fileCount < MAX_CLONE_FILES:
                continue

            callback.buildingChangeset(current + 1, len(finalTroveList))

            fileChangeSet = self.repos.createChangeSet(jobList,
                                    withFiles = True, withFileContents = True,
                                    recurse = False, callback = callback)
            jobFilesNeeded = sorted(set(jobFilesNeeded))
	    # fileId, pathId of the last file we saw. we don't need to
	    # include the same file contents twice (nor can we get them
	    # twice from fileChangeSet
            lastContents = (None, None)
            # walk the filesNeeded for the files we're getting from changesets
            for (pathId, newFileId, oldFileId, fromFileVersion) in \
                                jobFilesNeeded:
                # we could diff here, but why bother? we don't have anything
                # to diff against anyway
                filecs = fileChangeSet.getFileChange(None, newFileId)
                cs.addFile(oldFileId, newFileId, filecs)

                # A word on ptr types. This blindly copies them, assuming
                # that we'll copy the file which actually includes the
                # contents as well. If that assumption is wrong, then those
                # file contents are already in the repository so we don't
                # need them anyway. That leaves a changeset with broken
                # ptr links, but it commits just fine.

                if (files.frozenFileHasContents(filecs) and
			(pathId, newFileId) != lastContents):
                    # this copies the contents from the old changeset to the
                    # new without recompressing
                    (contType, contents) = fileChangeSet.getFileContents(
                                                        pathId, newFileId,
                                                        compressed = True)
                    cs.addFileContents(pathId, newFileId,
                                   contType, contents,
                                   files.frozenFileFlags(filecs).isConfig(),
                                   compressed = True)
		    lastContents = (pathId, newFileId)

            # now collect up the random files and handle those
            allFileObjects = self.repos.getFileVersions(
                [ (x[0], x[1], x[3]) for x in individualFilesNeeded ])
            contentsNeeded = []
            for fileObject, (pathId, newFileId, oldFileId, fromFileVersion) in \
                    itertools.izip(allFileObjects, individualFilesNeeded):
                diff, hash = changeset.fileChangeSet(pathId, None, fileObject)
                cs.addFile(oldFileId, newFileId, diff)
                if hash:
                    contentsNeeded.append(
                            ((pathId, fileObject.flags.isConfig()),
                             (newFileId, fromFileVersion)))

            allContents = self.repos.getFileContents(
                                [ x[1] for x in contentsNeeded ],
                                compressed = True,
                                callback = callback)
            for (contents, ((pathId, isCfg), (newFileId, fromFileVersion))) in \
                                itertools.izip(allContents, contentsNeeded):
                cs.addFileContents(pathId, newFileId,
                                   changeset.ChangedFileTypes.file,
                                   contents, isCfg,
                                   compressed = True)

            del fileChangeSet, allContents, allFileObjects

            fd, path = tempfile.mkstemp(prefix='conary-promote-')
            os.close(fd)
            cs.writeToFile(path)
            finalCs.merge(changeset.ChangeSetFromFile(path))
            os.remove(path)
            cs = changeset.ChangeSet()

            fileCount = 0
            jobList = []
            jobFilesNeeded = []
            individualFilesNeeded = []

        return finalCs

    def _createCloneJob(self, cloneOptions, chooser, troveCache):
        cloneJob = CloneJob(cloneOptions)
        cloneMap = CloneMap()
        chooser.setCloneMap(cloneMap)
        cloneOptions.callback.determiningCloneTroves()
        if cloneOptions.cloneOnlyByDefaultTroves:
            self._setByDefaultMap(chooser, troveCache)
        _logMe('determining troves to clone')
        self._determineTrovesToClone(chooser, cloneMap, cloneJob, troveCache,
                                     cloneOptions.callback)
        cloneOptions.callback.determiningTargets()

        _logMe('get existing leaves')
        leafMap = self._getExistingLeaves(cloneMap, troveCache, cloneOptions)
        _logMe('target sources')
        self._targetSources(chooser, cloneMap, cloneJob, leafMap, troveCache,
                            cloneOptions.callback)
        _logMe('target binaries')
        self._targetBinaries(cloneMap, cloneJob, leafMap, troveCache,
                             cloneOptions.callback)

        # some clones may rewrite the child troves (if cloneOnlyByDefaultTroves
        # is True).  We need to make sure that any precloned aren't having
        # the list of child troves changed.
        _logMe('recheck preclones')
        self._recheckPreClones(cloneJob, cloneMap, troveCache, chooser,
                               leafMap)
        troveTups = cloneJob.getTrovesToClone()
        unmetNeeds = self._checkNeedsFulfilled(troveTups, chooser, cloneMap,
                                               leafMap, troveCache, 
                                               cloneOptions.callback)
        if unmetNeeds:
            _logMe('could not clone')
            raise CloneIncomplete(unmetNeeds)
        _logMe('Got clone job')
        return cloneJob, cloneMap, leafMap

    def _setByDefaultMap(self, chooser, troveCache):
        """
            The byDefault map limits clones by the byDefault settings
            of the troves specified in the clone command (the primary
            troves).  Troves that are byDefault False in all primary
            troves are not included in the clone.
        """
        primaries = chooser.getPrimaryTroveList()
        troves = troveCache.getTroves(primaries, withFiles = False)
        byDefaultDict = dict.fromkeys(primaries, True)
        for trove in troves:
            # add all the troves that are byDefault True.
            # byDefault False ones we don't need to have in the dict.
            defaults = ((x[0], x[1]) for x in 
                        trove.iterTroveListInfo() if x[1])
            byDefaultDict.update(defaults)
        chooser.setByDefaultMap(byDefaultDict)

    def _determineTrovesToClone(self, chooser, cloneMap, cloneJob, troveCache,
                                callback):
        trvs = troveCache.getTroves(chooser.getPrimaryTroveList())
        toClone = []

        for trv in trvs:
            toClone.append(trv.getNameVersionFlavor())
            cloneMap.updateChildMap(trv)

        seen = set()
        toClone = chooser.getPrimaryTroveList()
        total = 0
        current = 0
        sourceByPackage = {}
        while toClone:
            total += len(toClone)
            needed = []

            callback.determiningCloneTroves(current, total)
            for info in toClone:
                if (trove.troveIsPackage(info[0])
                    and chooser.shouldPotentiallyClone(info) is False):
                    if (chooser.options.cloneOnlyByDefaultTroves
                        and chooser.isByDefault(info)):
                        needed.append(info)
                        seen.add(info)
                    else:
                        current += 1
                        continue
                elif info in seen:
                    current += 1
                else:
                    needed.append(info)
                    seen.add(info)

            srcsNeeded = [ (n,v,f) for n,v,f in needed if not
                    trove.troveIsComponent(n) and n not in sourceByPackage ]
            srcList = troveCache.getTroveInfo(
                            trove._TROVEINFO_TAG_SOURCENAME, srcsNeeded)
            sourceByPackage.update( (x[0],y()) for x, y in
                                        itertools.izip(srcsNeeded, srcList) )

            newToClone = []
            for troveTup in needed:
                current += 1
                callback.determiningCloneTroves(current, total)

                if troveTup[0].endswith(':source'):
                    sourceName = None
                elif trove.troveIsComponent(troveTup[0]):
                    try:
                        sourceName = sourceByPackage[troveTup[0].split(":")[0]]
                    except KeyError:
                        # XXX This can't happen because groups have to include
                        # packages, not components. However, the test suite
                        # hand builds groups which don't obey this rule. Just
                        # guess because it's good enough for the tests.
                        sourceName = troveTup[0].split(":")[0] + ":source"
                else:
                    sourceName = sourceByPackage[troveTup[0]]

                if chooser.shouldClone(troveTup, sourceName):
                    if not chooser.isExcluded(troveTup):
                        targetBranch = chooser.getTargetBranch(troveTup[1])
                        cloneMap.addTrove(troveTup, targetBranch, sourceName)
                        chooser.addSource(troveTup, sourceName)
                        cloneJob.add(troveTup)
                        for childTup in cloneMap.getChildren(troveTup):
                            chooser.addReferenceByCloned(childTup)
                    else:
                        # don't include this collection, instead
                        # only include child troves that aren't
                        # components of this collection.
                        for childTup in cloneMap.getChildren(troveTup):
                            if (childTup[0].split(':')[0],
                                childTup[1], childTup[2]) == troveTup:
                                chooser.addReferenceByUncloned(childTup)
                            else:
                                chooser.addReferenceByCloned(childTup)
                else:
                    if (chooser.options.cloneOnlyByDefaultTroves
                        and chooser.isByDefault(troveTup)
                        and chooser.isReferencedByCloned(troveTup)):
                        for childTup in cloneMap.getChildren(troveTup):
                            chooser.addReferenceByUncloned(childTup)

                    if trove.troveIsPackage(troveTup[0]):
                        # don't bother analyzing components for something
                        # we're not cloning
                        continue

                newToClone.extend(cloneMap.getChildren(troveTup))

            toClone = newToClone

    def _getExistingLeaves(self, cloneMap, troveCache, cloneOptions):
        """
            Gets the needed information about the current repository state
            to find out what clones may have already been performed
            (and should have their clonedFrom fields checked to be sure)
        """
        leafMap = LeafMap(cloneOptions)
        query = []
        for sourceTup, targetBranch in cloneMap.iterSourceTargetBranches():
            query.append((sourceTup[0], targetBranch, sourceTup[2]))

        for binTup, targetBranch in cloneMap.iterBinaryTargetBranches():
            query.append((binTup[0], targetBranch, binTup[2]))
        result = self.repos.findTroves(None, query,
                                       defaultFlavor = deps.parseFlavor(''),
                                       getLeaves=False, allowMissing=True,
                                       troveTypes=trovesource.TROVE_QUERY_ALL)
        if not result:
            return leafMap
        leafMap.addLeafResults(result)

        possiblePreClones = []
        for queryItem, tupList in result.iteritems():
            tupList = [ x for x in tupList if x[2] == queryItem[2] ]
            if not tupList:
                continue
            latest = sorted(tupList)[-1]
            if cloneMap.couldBePreClone(latest):
                possiblePreClones.append(latest)

        if not possiblePreClones:
            return leafMap
        leafMap.addClonedFromInfo(troveCache, possiblePreClones)
        return leafMap

    def _targetSources(self, chooser, cloneMap, cloneJob, leafMap, troveCache,
                       callback):
        hasTroves = self.repos.hasTroves(
                        [x[0] for x in cloneMap.iterSourceTargetBranches()])
        presentTroveTups = [x[0] for x in hasTroves.items() if x[1]]
        _logMe("Getting clonedFromInfo for sources")
        leafMap.addClonedFromInfo(troveCache, presentTroveTups)
        _logMe("done")

        total = len(list(cloneMap.iterSourceTargetBranches()))
        current = 0
        for sourceTup, targetBranch in cloneMap.iterSourceTargetBranches():
            current += 1
            callback.targetSources(current, total)
            if hasTroves[sourceTup]:
                newVersion = leafMap.isAlreadyCloned(sourceTup, targetBranch)
                if newVersion:
                    cloneMap.target(sourceTup, newVersion)
                    cloneJob.alreadyCloned(sourceTup)
                else:
                    newVersion = leafMap.hasAncestor(sourceTup, targetBranch,
                                                     self.repos)

                    if chooser.shouldClone(sourceTup):
                        if newVersion:
                            leafVersion = leafMap.getLeafVersion(sourceTup[0],
                                                                 targetBranch,
                                                                 sourceTup[2])
                            if newVersion == leafVersion:
                                cloneMap.target(sourceTup, newVersion)
                                cloneJob.alreadyCloned(sourceTup)
                                continue
                        newVersion = leafMap.createSourceVersion(sourceTup,
                                                                 targetBranch)
                        cloneMap.target(sourceTup, newVersion)
                        cloneJob.target(sourceTup, newVersion)
                    elif newVersion:
                        cloneMap.target(sourceTup, newVersion)
                        cloneJob.alreadyCloned(sourceTup)
                    else:
                        # should clone was false but the source trove exists -
                        # we could have done this clone.
                        raise CloneError(
                                     "Cannot find cloned source for %s=%s" \
                                          % (sourceTup[0], sourceTup[1]))
            else:
                newVersion = leafMap.hasAncestor(sourceTup, targetBranch, self.repos)
                if newVersion:
                    cloneMap.target(sourceTup, newVersion)
                    cloneJob.alreadyCloned(sourceTup)
                else:
                    # The source trove is not available to clone and either 
                    # this is not an uphill trove or the source is not 
                    # available on the uphill label.
                    raise CloneError(
                            "Cannot find required source %s on branch %s." \
                                     % (sourceTup[0], targetBranch))

    def _targetBinaries(self, cloneMap, cloneJob, leafMap, troveCache, callback):
        allBinaries = itertools.chain(*[x[1] for x in
                                        cloneMap.getBinaryTrovesBySource()])
        _logMe("Getting clonedFromInfo for binaries")
        leafMap.addClonedFromInfo(troveCache, allBinaries)
        _logMe("Actually targeting binaries")
        versionsToGet = []
        total = len(list(itertools.chain(*[x[0] for x in cloneMap.getBinaryTrovesBySource()])))
        current = 0
        for sourceTup, binaryList in cloneMap.getBinaryTrovesBySource():
            targetSourceVersion = cloneMap.getTargetVersion(sourceTup)
            if targetSourceVersion is None:
                raise errors.InternalConaryError(
                             "Cannot find cloned source for %s=%s" \
                                  % (sourceTup[0], sourceTup[1]))
            targetBranch = targetSourceVersion.branch()

            byVersion = {}
            for binaryTup in binaryList:
                current += 1
                callback.targetBinaries(current, total)
                byFlavor = byVersion.setdefault(binaryTup[1].getSourceVersion(),
                                                {})
                byFlavor.setdefault(binaryTup[2], []).append(binaryTup)

            for byFlavor in byVersion.itervalues():
                finalNewVersion = None
                for flavor, binaryList in byFlavor.iteritems():
                    # Binary list is a list of binaries all created from the
                    # same cook command.
                    newVersion = leafMap.isAlreadyCloned(binaryList,
                                                         targetBranch)
                    if (newVersion and 
                        (not finalNewVersion or finalNewVersion == newVersion)):
                        finalNewVersion = newVersion
                    else:
                        finalNewVersion = None
                        break
                if finalNewVersion:
                    for binaryTup in itertools.chain(*byFlavor.itervalues()):
                        cloneMap.target(binaryTup, finalNewVersion)
                        cloneJob.alreadyCloned(binaryTup)
                else:
                    binaryList = list(itertools.chain(*byFlavor.itervalues()))
                    versionsToGet.append((targetSourceVersion, binaryList))
        if not versionsToGet:
            return
        _logMe("getting new version for %s binaries" % (len(versionsToGet)))
        callback.targetBinaries()
        newVersions = leafMap.createBinaryVersions(self.repos,
                                                   versionsToGet)
        for newVersion, versionInfo in itertools.izip(newVersions,
                                                      versionsToGet):
            binaryList = versionInfo[1]
            for binaryTup in binaryList:
                cloneMap.target(binaryTup, newVersion)
                cloneJob.target(binaryTup, newVersion)

    def _checkNeedsFulfilled(self, troveTups, chooser, cloneMap, leafMap,
                             troveCache, callback):
        query = {}
        neededInfoTroveTups = {}
        callback.checkNeedsFulfilled()
        total = len(troveTups)
        current = 0

        _logMe("Checking needs are fulfilled for %s troves" % (len(troveTups)))
        troveCache.getTroves(troveTups, withFiles=False)
        for troveTup in troveTups:
            current += 1
            callback.checkNeedsFulfilled(current, total)
            trv = troveCache.getTrove(troveTup, withFiles=False)
            for mark, src in _iterAllVersions(trv):
                if (chooser.troveInfoNeedsRewrite(mark[0], src)
                    and not cloneMap.hasRewrite(src)):
                    if mark[0] == V_LOADED:
                        # Loaded troves are recorded with the flavor which
                        # was used to load the recipe, the flavor to use
                        # to get the trove from the repo is empty
                        neededInfoTroveTups.setdefault(
                                    (src[0], src[1], deps.ThawFlavor('')),
                                    []).append(mark)
                    else:
                        neededInfoTroveTups.setdefault(src, []).append(mark)

        _logMe("Checking clonedFrom info for %s needed troves" % (len(neededInfoTroveTups)))
        leafMap.addClonedFromInfo(troveCache, neededInfoTroveTups)

        total = len(neededInfoTroveTups)
        current = 0
        for troveTup in neededInfoTroveTups:
            callback.checkNeedsFulfilled(current, total)
            current += 1
            targetBranch = chooser.getTargetBranch(troveTup[1])
            if leafMap.isAlreadyCloned(troveTup, targetBranch):
                continue
            marks = neededInfoTroveTups[troveTup]

            queryItem = troveTup[0], targetBranch, troveTup[2]
            if queryItem not in query:
                query[queryItem] = troveTup, marks
            query[queryItem][1].extend(marks)
        results = self.repos.findTroves(None, query, None, bestFlavor=True,
                                        allowMissing=True)
        leafMap.addLeafResults(results)
        matches = []
        for queryItem, tupList in results.iteritems():
            sourceTup = query[queryItem][0]
            upstreamVersion = sourceTup[1].trailingRevision().getVersion()
            for troveTup in tupList:
                if (troveTup[1].trailingRevision().getVersion() == upstreamVersion
                    and sourceTup[2] == troveTup[2]):
                    matches.append(troveTup)
        _logMe("Checking clonedFrom info for %s matching nodes" % (len(matches)))
        leafMap.addClonedFromInfo(troveCache, matches)
        total = len(query)
        current = 0
        for queryItem, (sourceTup, markList) in query.items():
            current += 1
            callback.checkNeedsFulfilled(current, total)
            newVersion = leafMap.isAlreadyCloned(sourceTup, queryItem[1])
            if not newVersion:
                newVersion = leafMap.hasAncestor(sourceTup, queryItem[1], self.repos)
            if newVersion:
                cloneMap.target(sourceTup, newVersion)
                del query[queryItem]
        unmetNeeds = query.values()
        unmetNeeds = chooser.filterUnmetTroveInfoItems(unmetNeeds)
        return unmetNeeds

    def _recheckPreClones(self, cloneJob, cloneMap, troveCache, chooser, 
                          leafMap):
        # We only child for missing trove references, not build reqs for 
        # reclones.  Otherwise you could have to reclone when minor details
        # about the entironment have changed.
        troveTups = cloneJob.getPreclonedTroves()
        # match up as many needed targets for these clone as possible.
        _logMe("Rechecking %s preclones" % len(troveTups))
        needed = []
        fetch = []
        hasList = []
        for troveTup in troveTups:
            _logMe("Rechecking %s" % (troveTup,))
            if not trove.troveIsCollection(troveTup[0]):
                # this is only interested in missing references for included
                # troves. only collections have those
                continue
            newVersion = cloneMap.getTargetVersion(troveTup)
            clonedTup = (troveTup[0], newVersion, troveTup[2])
            needed += [ (troveTup, clonedTup) ]
            fetch += [ clonedTup ]
            hasList.append(clonedTup)
            hasList += [ (x[0], clonedTup[1], clonedTup[2]) for x in
                                    cloneMap.getChildren(troveTup) ]

        groupsNeeded = [ x[0] for x in needed if x[0][0].startswith('group-') ]
        groupsNeeded += [ x[1] for x in needed if x[0][0].startswith('group-') ]
        groupTroves = troveCache.getTroves(groupsNeeded)
        groupTroves = dict( itertools.izip(groupsNeeded, groupTroves) )

        hasTroves = troveCache.hasTroves(hasList)
        toReclone = []
        for (troveTup, clonedTup) in needed:
            if troveTup[0].startswith('group-'):
                trvChildren = list(
                    groupTroves[troveTup].iterTroveList(strongRefs = True,
                                                         weakRefs = True) )
            else:
                trvChildren = cloneMap.getChildren(troveTup)
                assert(trvChildren)

            if troveTup[0].startswith('group-'):
                clonedChildren = list(
                    groupTroves[clonedTup].iterTroveList(strongRefs = True,
                                                         weakRefs = True) )
            else:
                clonedChildren = []
                for x in cloneMap.getChildren(troveTup):
                    childTup = (x[0], clonedTup[1], clonedTup[2])
                    if hasTroves[childTup]:
                        clonedChildren.append(childTup)

            if self._shouldReclone(trvChildren, clonedChildren,
                        chooser, cloneMap):
                toReclone.append(troveTup)

        trovesBySource = cloneMap.getTrovesWithSameSource(toReclone)
        _logMe("Recloning %s troves" % len(trovesBySource))
        for binaryList in trovesBySource:
            sourceVersion = cloneMap.getSourceVersion(binaryList[0])
            targetSourceVersion = cloneMap.getTargetVersion(sourceVersion)
            newVersion = leafMap.createBinaryVersion(self.repos,
                                                     binaryList,
                                                     targetSourceVersion)
            for binaryTup in binaryList:
                cloneMap.target(binaryTup, newVersion)
                cloneJob.target(binaryTup, newVersion)

    def _shouldReclone(self, origTroveChildren, clonedTroveChildren, chooser,
                       cloneMap):
        childTroves = {}
        clonedChildTroves = {}
        for src in origTroveChildren:
            if chooser.troveInfoNeedsRewrite(V_REFTRV, src):
                targetBranch = chooser.getTargetBranch(src[1])
                childTroves[src[0], targetBranch, src[2]] = True
            elif chooser.troveInfoNeedsErase(V_REFTRV, src):
                continue
            else:
                childTroves[src[0], src[1].branch(), src[2]] = True

        for src in clonedTroveChildren:
            clonedChildTroves[src[0], src[1].branch(), src[2]] = True
        if childTroves == clonedChildTroves:
            return False
        return True

    def _buildTroves(self, chooser, cloneMap, cloneJob, leafMap, troveCache,
                     callback):
        # fill the trove cache with a single repository call
        allTroveList = []
        for troveTup, newVersion in cloneJob.iterTargetList():
            allTroveList.append(troveTup)
            targetBranch = newVersion.branch()
            leafVersion = leafMap.getLeafVersion(troveTup[0], targetBranch,
                                                 troveTup[2])
            if leafVersion:
                allTroveList.append((troveTup[0], leafVersion, troveTup[2]))

        # this getTroves populates troveCache.hasTroves simultaneously
        has = troveCache.hasTroves(allTroveList)
        toFetch = [ x for x, y in itertools.izip(allTroveList, has)
                            if y ]
        troveCache.getTroves(toFetch, withFiles=True)
        #del allTroveList, has

        current = 0
        finalTroves = []
        total = len(list(cloneJob.iterTargetList()))
        for troveTup, newVersion in cloneJob.iterTargetList():
            current += 1
            callback.rewriteTrove(current, total)
            trv = troveCache.getTrove(troveTup, withFiles=True)
            oldVersion = trv.getVersion()
            newTrv = self._rewriteTrove(trv, newVersion, chooser, cloneMap,
                                        cloneJob, leafMap, troveCache)
            if not newTrv:
                return None

            # make sure we haven't deleted all the child troves from 
            # a group.  This could happen, for example, if a group 
            # contains all byDefault False components.
            if trove.troveIsCollection(troveTup[0]):
                if not list(newTrv.iterTroveList(strongRefs=True)):
                    raise CloneError("Clone would result in empty collection "
                                     "%s=%s[%s]" % (troveTup))

            sigKeyId = selectSignatureKey(self.cfg,
                                        newTrv.getVersion().trailingLabel())

            if sigKeyId is not None:
                newTrv.addDigitalSignature(sigKeyId)
            else:
                # if no sigKeyId, just add sha1s
                newTrv.computeDigests()

            finalTroves.append((oldVersion, newTrv))

        return finalTroves

    def _rewriteTrove(self, trv, newVersion, chooser, cloneMap,
                      cloneJob, leafMap, troveCache):
        # make a copy so we don't corrupt the copy in the trove cache
        trv = trv.copy()

        filesNeeded = []
        troveName, troveVersion, troveFlavor = trv.getNameVersionFlavor()
        troveBranch = troveVersion.branch()
        targetBranch = newVersion.branch()

        needsNewVersions = []
        if cloneJob.options.trackClone:
            # cloned from tracks exactly where we cloned from
            trv.troveInfo.clonedFrom.set(troveVersion)
            # cloned from list lists all places we've cloned from,
            # with the most recent clone at the end
            trv.troveInfo.clonedFromList.append(troveVersion)

        # clone the labelPath
        labelPath = list(trv.getLabelPath())
        labelPathMap = [(x, cloneMap.getCloneTargetLabelsForLabel(x))
                         for x in labelPath]
        labelPath = _computeLabelPath(trv.getName(), labelPathMap)
        if labelPath:
            trv.setLabelPath(labelPath)

        trv.changeVersion(newVersion)
        trv.copyMetadata(trv) # flatten metadata

        for mark, src in _iterAllVersions(trv):
            if chooser.troveInfoNeedsRewrite(mark[0], src):
                newVersion = cloneMap.getTargetVersion(src)
                if newVersion is None:
                    continue
                _updateVersion(trv, mark, newVersion)
            elif chooser.troveInfoNeedsErase(mark[0], src):
                _updateVersion(trv, mark, None)
        if trove.troveIsFileSet(trv.getName()):
            needsRewriteFn = chooser.filesetFileNeedsRewrite
        else:
            needsRewriteFn = chooser.fileNeedsRewrite

        for (pathId, path, fileId, version) in trv.iterFileList():
            if needsRewriteFn(troveBranch, targetBranch, version):
                needsNewVersions.append((pathId, path, fileId))

        # need to be reversioned
        if needsNewVersions:
            leafVersion = leafMap.getLeafVersion(troveName, targetBranch, 
                                                 troveFlavor)
            if leafVersion and troveCache.hasTrove(troveName, leafVersion,
                                                   troveFlavor):
                oldTrv = troveCache.getTrove((troveName, leafVersion,
                                              troveFlavor),
                                              withFiles = True)
                # pathId, fileId -> fileVersion map
                fileMap = dict(((x[0], x[2]), x[3]) for x in
                                        oldTrv.iterFileList())
            else:
                fileMap = {}

            for (pathId, path, fileId) in needsNewVersions:
                ver = fileMap.get((pathId, fileId), newVersion)
                trv.updateFile(pathId, path, ver, fileId)

        infoOnly = cloneJob.options.infoOnly
        if trv.getName().endswith(':source') and not infoOnly:
            try:
                cl = cloneJob.options.callback.getCloneChangeLog(trv)
            except Exception, e:
                log.error(str(e))
                return None

            if cl is None:
                log.error("no change log message was given"
                          " for %s." % trv.getName())
                return None
            trv.changeChangeLog(cl)
        # reset the signatures, because all the versions have now
        # changed, thus invalidating the old sha1 hash
        trv.troveInfo.sigs.reset()
        if not infoOnly: # not computing signatures will 
                         # make sure this doesn't get committed
            trv.computeDigests()

        return trv

def _iterAllVersions(trv, rewriteTroveInfo=True):
    # return all versions which need rewriting except for file versions
    # and the version of the trove itself. file versions are handled
    # separately since we can clone even if the files don't already
    # exist on the target branch (we just add them), and trove versions
    # are always rewritten even when cloning to the same branch
    # (while other versions are not)

    if rewriteTroveInfo:
        for troveTuple in \
                    [ x for x in trv.troveInfo.loadedTroves.iter() ]:
            yield ((V_LOADED, troveTuple),
                   (troveTuple.name(), troveTuple.version(),
                    troveTuple.flavor()))

        for troveTuple in \
                    [ x for x in trv.troveInfo.buildReqs.iter() ]:
            yield ((V_BREQ, troveTuple),
                   (troveTuple.name(), troveTuple.version(),
                    troveTuple.flavor()))

    for troveInfo in [ x for x in trv.iterTroveList(strongRefs=True,
                                                    weakRefs=True) ]:
        yield ((V_REFTRV, troveInfo), troveInfo)

def _updateVersion(trv, mark, newVersion):
    """ 
        Update version for some piece of troveInfo.  If newVersion is None, 
        just erase this version.
    """
    kind = mark[0]

    if kind == V_LOADED:
        trv.troveInfo.loadedTroves.remove(mark[1])
        if newVersion:
            trv.troveInfo.loadedTroves.add(mark[1].name(), newVersion,
                                           mark[1].flavor())
    elif kind == V_BREQ:
        trv.troveInfo.buildReqs.remove(mark[1])
        if newVersion:
            trv.troveInfo.buildReqs.add(mark[1].name(), newVersion,
                                        mark[1].flavor())
    elif kind == V_REFTRV:
        (name, oldVersion, flavor) = mark[1]
        isStrong = trv.isStrongReference(name, oldVersion, flavor)
        byDefault = trv.includeTroveByDefault(name, oldVersion, flavor)
        trv.delTrove(name, oldVersion, flavor, False, 
                                               weakRef = not isStrong)
        if newVersion:
            if not trv.hasTrove(name, newVersion, flavor):
                trv.addTrove(name, newVersion, flavor,
                             byDefault = byDefault,
                             weakRef = not isStrong)
            else:
                # it's possible that this trove already exists in this group
                # this could happen if the trove has previously been cloned
                # and the group contains a reference to the cloned and
                # uncloned versions.  Afterwards there will just be one 
                # reference.
                if not isStrong:
                    return
                # delete a weak reference if it exists, there should only
                # be one reference to this package in this group.
                trv.delTrove(name, newVersion, flavor, missingOkay = True,
                                                       weakRef = True)
                trv.addTrove(name, newVersion, flavor,
                             byDefault = byDefault,
                             presentOkay = True,
                             weakRef = not isStrong)
    else:
        assert(0)

def _computeLabelPath(name, labelPathMap):
    newLabelPath = []
    for label, newLabels in labelPathMap:
        if len(newLabels) > 1:
            raise CloneError("Multiple clone targets for label %s"
                             " - cannot build labelPath for %s" % (label, name))
        elif newLabels:
            newLabel = newLabels.pop()
        else:
            newLabel = label
        if newLabel in newLabelPath:
            # don't allow duplicates
            continue
        newLabelPath.append(newLabel)
    return newLabelPath

class CloneOptions(object):
    def __init__(self, fullRecurse=True, cloneSources=True,
                 trackClone=True, callback=None,
                 message=DEFAULT_MESSAGE, cloneOnlyByDefaultTroves=False,
                 updateBuildInfo=True, infoOnly=False, bumpGroupVersions=False,
                 enforceFullBuildInfoCloning=False, excludeGroups=False):
        self.fullRecurse = fullRecurse
        self.cloneSources = cloneSources
        self.trackClone = trackClone
        if callback is None:
            callback = callbacks.CloneCallback()
        self.callback = callback
        self.message = message
        self.cloneOnlyByDefaultTroves = cloneOnlyByDefaultTroves
        self.updateBuildInfo = updateBuildInfo
        self.infoOnly = infoOnly
        self.bumpGroupVersions = bumpGroupVersions
        self.enforceFullBuildInfoCloning = enforceFullBuildInfoCloning
        self.excludeGroups = excludeGroups

class TroveCache(object):
    def __init__(self, repos, callback):
        self._hasTroves = {}
        self.troves = {True : {}, False : {}}
        self.repos = repos
        self.callback = callback

    def hasTrove(self, name, version, flavor):
        return self.hasTroves([(name, version, flavor)])[name, version, flavor]

    def hasTroves(self, troveTups):
        needed = [ x for x in troveTups if x not in self._hasTroves ]
        if needed:
            self._hasTroves.update(self.repos.hasTroves(needed))
        return dict((x, self._hasTroves[x]) for x in troveTups)

    def _get(self, troveTups, withFiles):
        cs = self.repos.createChangeSet(
                [ (x[0], (None, None), (x[1], x[2]), True) for x in troveTups],
                withFiles = withFiles, withFileContents = False,
                recurse = False)

        for x in troveTups:
            self.troves[withFiles][x] = cs.getNewTroveVersion(*x)
            if trove.troveIsCollection(x[0]):
                self.troves[not withFiles][x] = cs.getNewTroveVersion(*x)

    def getTroves(self, troveTups, withFiles=True):
        theDict = self.troves[withFiles]
        needed = [ x for x in troveTups if x not in theDict ]
        if needed:
            theOtherDict = self.troves[not withFiles]
            msg = getattr(self.callback, 'lastMessage', None)
            _logMe('getting %s troves from repos' % len(needed))

            self._get(troveTups, withFiles)

        # this prevents future hasTroves calls from calling the server
        self._hasTroves.update((x, True) for x in troveTups)

        return [ trove.Trove(theDict[x],
                 skipIntegrityChecks = (not withFiles)) for x in troveTups ]

    def getTrove(self, troveTup, withFiles=True):
        return self.getTroves([troveTup], withFiles=withFiles)[0]

    def getTroveInfo(self, *args):
        return self.repos.getTroveInfo(*args)

class CloneChooser(object):
    def __init__(self, targetMap, primaryTroveList, cloneOptions):
        # make sure there are no zeroed timeStamps - branches may be
        # user-supplied string
        newMap = {}
        for key, value in targetMap.iteritems():
            if isinstance(key, versions.Branch):
                key = key.copy()
                key.resetTimeStamps()
            if isinstance(value, versions.Branch):
                value = value.copy()
                value.resetTimeStamps()
            newMap[key] = value
        self.primaryTroveList = primaryTroveList
        self.targetMap = newMap
        self.byDefaultMap = None
        self.referencedByClonedMap = {}
        self.referencedByUnclonedMap = {}
        self.options = cloneOptions

 
    def getPrimaryTroveList(self):
        return self.primaryTroveList

    def setByDefaultMap(self, map):
        self.byDefaultMap = map

    def setCloneMap(self, cloneMap):
        self.cloneMap = cloneMap

    def addSource(self, troveTup, sourceName):
        if self.byDefaultMap is None:
            return
        noFlavor = deps.parseFlavor('')
        version = troveTup[1]
        sourceVersion = version.getSourceVersion(False)
        sourceTup = (sourceName, sourceVersion, noFlavor)
        self.byDefaultMap[sourceTup] = True

    def isByDefault(self, troveTup):
        if self.byDefaultMap is None:
            return True
        return troveTup in self.byDefaultMap

    def addReferenceByCloned(self, troveTup):
        self.referencedByClonedMap[troveTup] = True

    def addReferenceByUncloned(self, troveTup):
        self.referencedByUnclonedMap[troveTup] = True

    def isReferencedByCloned(self, troveTup):
        return troveTup in self.referencedByClonedMap

    def isExcluded(self, troveTup):
        return (self.options.excludeGroups
                and troveTup[0].startswith('group-')
                and not troveTup[0].endswith(':source'))

    def shouldPotentiallyClone(self, troveTup):
        """
            returns True if you definitely should clone this trove
            returns False if you definitely should not clone this trove
            returns None if it's undecided.
        """
        name, version, flavor = troveTup
        if self.byDefaultMap is not None:
            if troveTup not in self.byDefaultMap:
                return False
            if troveTup in self.referencedByUnclonedMap:
                # don't clone anything that's referenced by other packages
                # that are not being cloned.
                return False
        if (version.branch() not in self.targetMap and
            version.trailingLabel() not in self.targetMap
            and None not in self.targetMap):
            return False
        if name.endswith(':source'):
            if self.options.cloneSources:
                return True
        elif self.options.fullRecurse:
            return True

    def shouldClone(self, troveTup, sourceName=None):
        shouldClone = self.shouldPotentiallyClone(troveTup)
        if shouldClone is not None:
            return shouldClone
        return self._matchesPrimaryTrove(troveTup, sourceName)

    def _matchesPrimaryTrove(self, troveTup, sourceName):
        name, version, flavor = troveTup
        if name.endswith(':source'):
            return (name, version, flavor) in self.primaryTroveList
        if not sourceName:
            sourceName = trv.getName().split(':')[0] + ':source'
        sourcePackage = sourceName.split(':')[0]
        parentPackage = (sourcePackage, version, flavor)
        if parentPackage not in self.primaryTroveList:
            return False
        return True

    def getTargetBranch(self, version):
        sourceLabel = version.trailingLabel()
        sourceBranch = version.branch()
        if sourceBranch in self.targetMap:
            target = self.targetMap[sourceBranch]
        elif sourceLabel in self.targetMap:
            target = self.targetMap[sourceLabel]
        else:
            target = self.targetMap.get(None, None)
        if target is None:
            return None

        if isinstance(target, versions.Label):
            return sourceBranch.createSibling(target)
        elif isinstance(target, versions.Branch):
            return target
        assert(0)

    def troveInfoNeedsRewrite(self, kind, troveTup):
        targetBranch = self.getTargetBranch(troveTup[1])
        if not targetBranch:
            return False
        if self.byDefaultMap is not None and troveTup not in self.byDefaultMap:
            return False

        if kind == V_REFTRV:
            # only rewrite trove info if we're cloning that trove.
            # otherwise, assume it's correct.
            return troveTup in self.cloneMap.targetMap

        if targetBranch == troveTup[1].branch():
            # this means that we're merely pushing this trove to tip
            # on same branch
            return False
        return self.options.updateBuildInfo

    def filesetFileNeedsRewrite(self, troveBranch, targetBranch, fileVersion):
        targetMap = self.targetMap
        return (fileVersion.branch() in targetMap or
            fileVersion.trailingLabel() in targetMap
            or None in targetMap)

    def fileNeedsRewrite(self, troveBranch, targetBranch, fileVersion):
        if fileVersion.depth() == targetBranch.depth():
            # if the file is on /A and we're cloning to /C, then that needs
            # to be rewritten.  If we're on /C already, no rewriting necessary
            return fileVersion.branch() != targetBranch
        # if the fileVersion is at some level that's deeper than
        # the target branch - say, the file is on /A//B and the clone
        # is being made to /A, then the file must be rewritten.
        # If, instead, the file on /A and the clone is being made to 
        # /A//B, then the file is ok.
        return fileVersion.depth() > targetBranch.depth()

    def troveInfoNeedsErase(self, kind, troveTup):
        if kind != V_REFTRV:
            # we only erase trove references - all other types 
            # just let remain with their old, uncloned values.
            # This could change.
            return False
        return (self.byDefaultMap is not None
                and troveTup not in self.referencedByUnclonedMap
                and troveTup not in self.byDefaultMap)

    def filterUnmetTroveInfoItems(self, unmetTroveInfoItems):
        if self.options.enforceFullBuildInfoCloning:
            return unmetTroveInfoItems
        return [ (mark,troveTup) for (mark,troveTup) in unmetTroveInfoItems 
                  if mark[0] == V_REFTRV ]

class CloneMap(object):
    def __init__(self):
        self.targetMap = {}
        self.trovesByTargetBranch = {}
        self.trovesBySource = {}
        self.sourcesByTrove = {}
        self.childMap = {}

    def addTrove(self, troveTup, targetBranch, sourceName=None):
        name, version, flavor = troveTup
        if (name, targetBranch, flavor) in self.trovesByTargetBranch:
            if self.trovesByTargetBranch[name, targetBranch, flavor] == version:
                return
            otherVersion = self.trovesByTargetBranch[name, targetBranch, flavor]
            if not flavor.isEmpty():
                troveSpec = '%s[%s]' % (name, flavor)
            else:
                troveSpec = name

            versions = [ str(otherVersion), str(version) ]
            versions.sort()

            raise CloneError("Cannot clone multiple versions of %s"
                             " to branch %s at the same time.  Attempted to"
                             " clone versions %s and %s" % (troveSpec,
                                                            targetBranch,
                                                            versions[0],
                                                            versions[1]))

        self.trovesByTargetBranch[name, targetBranch, flavor] = version
        if name.endswith(':source'):
            self.trovesBySource.setdefault((name, version, flavor), [])
            return

        noFlavor = deps.parseFlavor('')
        sourceVersion = version.getSourceVersion(False)
        sourceTup = (sourceName, sourceVersion, noFlavor)
        self.addTrove(sourceTup, targetBranch)
        self.trovesBySource[sourceTup].append(troveTup)
        self.sourcesByTrove[troveTup] = sourceTup

    def iterSourceTargetBranches(self):
        for (name, targetBranch, flavor), version  \
           in self.trovesByTargetBranch.iteritems():
            if name.endswith(':source'):
                yield (name, version, flavor), targetBranch

    def iterBinaryTargetBranches(self):
        for (name, targetBranch, flavor), version  \
           in self.trovesByTargetBranch.iteritems():
            if not name.endswith(':source'):
                yield (name, version, flavor), targetBranch

    def getBinaryTrovesBySource(self):
        return self.trovesBySource.items()

    def getTrovesWithSameSource(self, troveTupleList):
        bySource = {}
        for troveTup in troveTupleList:
            sourceTup = self.sourcesByTrove[troveTup]
            bySource[sourceTup] = self.trovesBySource[sourceTup]
        return bySource.values()

    def getSourceVersion(self, troveTup):
        return self.sourcesByTrove[troveTup]

    def target(self, troveTup, targetVersion):
        oldBranch = troveTup[1].branch()
        targetBranch = targetVersion.branch()
        while targetBranch.depth() < oldBranch.depth():
            oldBranch = oldBranch.parentBranch()
        if not (targetBranch == oldBranch
                or targetBranch.isSibling(oldBranch)):
            raise CloneError("clone only supports cloning troves to sibling "
                             "branches, parents, and siblings of parent"
                             " branches")
        self.targetMap[troveTup] = targetVersion

    def getTargetVersion(self, troveTup):
        return self.targetMap.get(troveTup, None)

    def couldBePreClone(self, troveTup):
        info = (troveTup[0], troveTup[1].branch(), troveTup[2])
        if info in self.trovesByTargetBranch:
            return True
        return False

    def hasRewrite(self, troveTup):
        return troveTup in self.targetMap

    def getCloneTargetLabelsForLabel(self, label):
        matches = set()
        for troveTup, newVersion in self.targetMap.iteritems():
            if troveTup[1].trailingLabel() == label:
                matches.add(newVersion.trailingLabel())
        return matches

    def updateChildMap(self, trv):
        l = list(trv.iterTroveList(strongRefs=True, weakRefs=True))
        l.sort()
        self.childMap[trv.getNameVersionFlavor()] = set(l)
        for child in l:
            if trove.troveIsPackage(child[0]):
                if child not in self.childMap:
                    self.childMap[child] = set()
            elif trove.troveIsComponent(child[0]):
                pkg = child[0].split(":")[0]
                self.childMap[(pkg, child[1], child[2])].add(child)

    def getChildren(self, trvTuple):
        return self.childMap.get(trvTuple, set())

class LeafMap(object):
    def __init__(self, options):
        self.clonedFrom = {}
        self.branchMap = {}
        self.options = options

    def _addTrove(self, troveTup, clonedFrom=None):
        name, version, flavor = troveTup
        if clonedFrom is None:
            clonedFrom = set([troveTup[1]])
        self.clonedFrom[troveTup] = clonedFrom

    def _getClonedFrom(self, troveTup):
        if troveTup in self.clonedFrom:
            return self.clonedFrom[troveTup]
        return set([troveTup[1]])

    def addLeafResults(self, branchMap):
        self.branchMap.update(branchMap)

    def getLeafVersion(self, name, targetBranch, flavor):
        if (name, targetBranch, flavor) not in self.branchMap:
            return None
        troveList = [ x for x in self.branchMap[name, targetBranch, flavor] 
                      if x[2] == flavor ]
        if troveList:
            return sorted(troveList)[-1][1]
        return None

    @staticmethod
    def hasAncestor(troveTup, targetBranch, repos):
        newVersion = troveTup[1]
        if newVersion.branch() == targetBranch:
            # even if we're an unmodified shadow - if we're cloning to our
            # own branch we want to use other tests to determine if
            # the clone is necessary.
            return False
        while (newVersion.isShadow() and not newVersion.isModifiedShadow()
               and newVersion.branch() != targetBranch):
            newVersion = newVersion.parentVersion()
        if (newVersion.branch() == targetBranch and
            repos.hasTrove(troveTup[0], newVersion, troveTup[2])):
            return newVersion

        return False

    def isAlreadyCloned(self, troveTupleList, targetBranch):
        if not isinstance(troveTupleList, list):
            troveTupleList = [troveTupleList]
        finalTargetVersion = None
        for troveTup in troveTupleList:
            myClonedFrom = self._getClonedFrom(troveTup)
            name, version, flavor = troveTup
            targetVersion = self.getLeafVersion(name, targetBranch, flavor)
            if not targetVersion:
                return False

            targetTup = name, targetVersion, flavor
            targetClonedFrom = self._getClonedFrom(targetTup)
            if not myClonedFrom & targetClonedFrom:
                # either the version we're thinking about cloning is 
                # in the cloned from field or maybe we're both cloned
                # from the same place.
                return False

            if targetVersion != finalTargetVersion:
                if finalTargetVersion:
                    # conflict on clone version.
                    return False
                finalTargetVersion = targetVersion
        return finalTargetVersion

    def createSourceVersion(self, sourceTup, targetBranch):
        name, version, flavor = sourceTup
        targetBranchVersionList = [x[1] for x in 
                                   self.branchMap.get((name, targetBranch,   
                                                      flavor), [])]

        revision = version.trailingRevision().copy()
        desiredVersion = targetBranch.createVersion(revision).copy()
        # this could have too many .'s in it
        if desiredVersion.shadowLength() < revision.shadowCount():
            # this truncates the dotted version string
            revision.getSourceCount().truncateShadowCount(
                                        desiredVersion.shadowLength())
            desiredVersion = targetBranch.createVersion(revision)

        # the last shadow count is not allowed to be a 0
        if [ x for x in revision.getSourceCount().iterCounts() ][-1] == 0:
            desiredVersion.incrementSourceCount()
        # if 1-3.6 exists we don't want to be created 1-3.5.
        matchingUpstream = [ x.trailingRevision()
                             for x in targetBranchVersionList
                             if (x.trailingRevision().getVersion()
                                 == revision.getVersion()) ]
        if (revision in matchingUpstream
            and desiredVersion.shadowLength() > revision.shadowCount()):
            desiredVersion.incrementSourceCount()
            revision = desiredVersion.trailingRevision()

        if matchingUpstream:
            def _sourceCounts(revision):
                return list(revision.getSourceCount().iterCounts())
            shadowCounts = _sourceCounts(revision)
            matchingShadowCounts = [ x for x in matchingUpstream
                               if _sourceCounts(x)[:-1] == shadowCounts[:-1] ]
            if matchingShadowCounts:
                latest = sorted(matchingShadowCounts, key=_sourceCounts)[-1]
                if (revision in matchingShadowCounts
                    or _sourceCounts(latest) > _sourceCounts(revision)):
                    revision = latest.copy()
                    desiredVersion = targetBranch.createVersion(revision)
                    desiredVersion.incrementSourceCount()

        assert(not desiredVersion in targetBranchVersionList)
        return desiredVersion

    def createBinaryVersion(self, repos, binaryList, sourceVersion):
        # We should be able to avoid the repos calls made in here...
        # but it may not be worth it.
        return self.createBinaryVersions(repos, [(sourceVersion,
                                                  binaryList)])[0]

    def createBinaryVersions(self, repos, sourceBinaryList):
        # takes a (sourceVersion, troveTupList) ->
        #         (sourceVersion, pkgNames, flavorList) list.
        troveList = [(x[0],  # sourceVersion
                     set([y[0] for y in x[1]]), # all names
                     set([y[2] for y in x[1]])) # all flavors
                        for x in sourceBinaryList]
        bumpList = {True: [], False: []}
        for idx, item in enumerate(troveList):
            nameList = item[1]
            if (self.options.bumpGroupVersions
                and iter(nameList).next().startswith('group-')):
                bumpList[True].append((idx, item))
            else:
                bumpList[False].append((idx, item))
        allVersions = [None] * len(troveList)
        for bumpVersions, troveList in bumpList.items():
            indexes = [ x[0] for x in troveList ]
            troveList = [ x[1] for x in troveList ]
            newVersions = nextVersions(repos, None, troveList,
                                       alwaysBumpCount=bumpVersions)
            for idx, newVersion in itertools.izip(indexes, newVersions):
                allVersions[idx] = newVersion
        return allVersions

    def addClonedFromInfo(self, troveCache, tupList):
        """
            Recurse through clonedFrom information for the given tupList
            so that we can know all the troves in the cloned history for these
            troves.
        """
        # Note - this is a bit inefficient.  Without knowing what trove
        # we're going to compare these troves against in the "clonedFrom"
        # field, we could be doing lots of extra work.  However, this way
        # is very generic.
        clonedFromInfo = dict((x, set([x[1]])) for x in tupList)

        newToGet = {}
        hasTroves = {}
        trovesByHost = {}
        # sort by host so that if a particular repository is down
        # we can continue to look at the rest of the clonedFrom info.
        for troveTup in sorted(tupList):
            if troveTup[1].isInLocalNamespace():
                continue

            host = troveTup[1].trailingLabel().getHost()
            l = trovesByHost.setdefault(host, [])
            if (troveTup[0].split(":")[0], troveTup[1], troveTup[2]) not in l:
                l.append(troveTup)

        results = dict()
        for host, troveTups in trovesByHost.items():
            try:
                infoList = troveCache.getTroveInfo(
                                trove._TROVEINFO_TAG_CLONEDFROMLIST, troveTups)
            except errors.ConaryError, msg:
                log.debug('warning: Could not access host %s: %s' %
                                (host, msg))

            # handle old CLONEDFROM adequately if CLONEDFROMLIST doesn't
            # exist
            missingList = [ i for i, x in enumerate(infoList)
                                    if x is None ]

            try:
                cfList = troveCache.getTroveInfo(
                                trove._TROVEINFO_TAG_CLONEDFROM,
                                [ troveTups[x] for x in missingList ])
            except errors.ConaryError, msg:
                log.debug('warning: Could not access host %s: %s' %
                                (host, msg))

            for i, clonedFrom in itertools.izip(missingList, cfList):
                if clonedFrom:
                    infoList[i] = [ clonedFrom() ]
                else:
                    infoList[i] = None

            results.update(itertools.izip(troveTups, infoList))

        for troveTup in tupList:
            if troveTup[1].isInLocalNamespace():
                continue

            if troveTup not in results and trove.troveIsComponent(troveTup[0]):
                name = troveTup[0].split(":")[0]
            else:
                name = troveTup[0]

            clonedFromList = results[(name, troveTup[1], troveTup[2])]

            if clonedFromList:
                # Looks weird, but switches from a version stream to
                # a version object
                for clonedFrom in clonedFromList:
                    clonedFromInfo[troveTup].add(clonedFrom)

        for troveTup, clonedFrom in clonedFromInfo.iteritems():
            self._addTrove(troveTup, clonedFrom)

class CloneError(errors.ClientError):
    pass

class CloneIncomplete(CloneError):

    def __str__(self):
        l = []
        loadRecipes = []
        buildReqs = []
        refTroves = []

        for src, markList in self.needs:
            for mark in markList:
                what = "%s=%s[%s]" % (src[0], src[1], src[2])
                if mark[0] == V_LOADED:
                    loadRecipes.append(what)
                elif mark[0] == V_BREQ:
                    buildReqs.append(what)
                elif mark[0] == V_REFTRV:
                    refTroves.append(what)
        l.extend(["build requirement: %s" % x
                  for x in sorted(set(buildReqs))])
        l.extend(["loadRecipe:        %s" % x
                  for x in sorted(set(loadRecipes))])
        l.extend(["referenced trove:  %s" % x
                  for x in sorted(set(refTroves))])

        return "Clone cannot be completed because some troves are not " + \
               "available on the target branch.\n\t" + \
               "\n\t".join(l)

    def __init__(self, needs):
        CloneError.__init__(self)
        self.needs = needs

#start = time.time()
def _logMe(msg):
    return
    # Dead code
    start = 0
    secs = int(time.time() - start)
    mins = secs / 60
    secs = secs % 60
    if mins:
        timeStr = '%s mins, %s secs' % (mins, secs)
    else:
        timeStr = '%s secs' % (secs)

    print '\n%s (%s): %s' % (time.strftime('%X'), timeStr, msg)
