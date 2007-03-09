#
# Copyright (c) 2005-2006 rPath, Inc.
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

import itertools

from conary import callbacks
from conary import changelog
from conary import errors
from conary import trove
from conary import versions
from conary.build.nextversion import nextVersion
from conary.deps import deps
from conary.lib import log
from conary.lib import sha1helper
from conary.repository import changeset

V_LOADED = 0
V_BREQ = 1
V_REFTRV = 2

# don't change 
DEFAULT_MESSAGE = 1

class CloneJob(object):
    def __init__(self, trackClone=True, infoOnly=False, callback=None):
        self.cloneJob = {}
        self.trackClone = trackClone
        self.infoOnly = infoOnly
        self.callback = callback

    def add(self, troveTup):
        self.cloneJob[troveTup] = None

    def alreadyCloned(self, troveTup):
        self.cloneJob.pop(troveTup, False)

    def target(self, troveTup, targetVersion):

        self.cloneJob[troveTup] = targetVersion

    def iterTargetList(self):
        return self.cloneJob.iteritems()

    def isEmpty(self):
        return not self.cloneJob

class CurrentLeaves(object):
    # Then it could be passed into nextVersion.  Also,
    # we could make all the individual calls and know that the results
    # are cached from the large calls.
    def __init__(self):
        self.currentClonedFrom = {}
        self.branchMap = {}

    def addTrove(self, troveTup, clonedFrom=None):
        name, version, flavor = troveTup
        if clonedFrom is None:
            clonedFrom = set([version])
        else:
            clonedFrom = set([clonedFrom, version])
        self.currentClonedFrom[name, version.branch(), flavor] = \
                                                    (version, clonedFrom)

    def addLeafResults(self, branchMap):
        self.branchMap = branchMap

    def getLeafVersion(self, name, targetBranch, flavor):
        if (name, targetBranch, flavor) not in self.branchMap:
            return None
        troveList = self.branchMap[name, targetBranch, flavor]
        return sorted(troveList)[-1][1]

    def hasAncestor(self, troveTup, targetBranch, repos):
        newVersion = troveTup[1]
        while (newVersion.isShadow() and not newVersion.isModifiedShadow()
               and newVersion.branch() != targetBranch):
            newVersion = newVersion.parentVersion()
        if (newVersion.branch() == targetBranch and
            repos.hasTrove(troveTup[0], newVersion, troveTup[2])):
            return newVersion

        return False

    def isAlreadyCloned(self, troveList, targetBranch):
        if not isinstance(troveList, list):
            troveList = [troveList]
        finalTargetVersion = None
        for trove in troveList:
            name, version, flavor = trove.getNameVersionFlavor()
            targetInfo = name, targetBranch, flavor
            if targetInfo not in self.currentClonedFrom:
                return False
            targetVersion, clonedFrom = self.currentClonedFrom[targetInfo]
            if not (set([version, trove.troveInfo.clonedFrom()]) & clonedFrom):
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

        while desiredVersion in targetBranchVersionList:
            desiredVersion.incrementSourceCount()

        return desiredVersion

    def createBinaryVersion(self, repos, binaryList, sourceVersion):
        flavor = binaryList[0][2]
        # FIXME we should be able to avoid these repos calls here.
        return nextVersion(repos, None, [x[0] for x in binaryList], 
                           sourceVersion, flavor)

class TroveCache(object):
    def __init__(self, repos, callback):
        self.troves = {True : {}, False : {}}
        self.repos = repos
        self.callback = callback

    def getTroves(self, troveTups, withFiles=True):
        theDict = self.troves[withFiles]
        needed = [ x for x in troveTups if x not in theDict ]
        troves = self.repos.getTroves(troveTups, withFiles=withFiles,
                                      callback=self.callback)
        self.troves.update(itertools.izip(needed, troves))
        return [ self.troves[x] for x in troveTups]

    def getTrove(self, troveTup, withFiles=True):
        return self.getTroves([troveTup], withFiles=True)[0]

class CloneMap(object):
    def __init__(self, labelMap, primaryTroveList, fullRecurse=True, 
                 cloneSources=True, updateBuildInfo=True):
        self.primaryTroveList = primaryTroveList
        self.labelMap = labelMap
        self.targetMap = {}
        self.trovesByTargetBranch = {}
        self.trovesBySource = {}
        self.byDefaultMap = None

        self.cloneSources = cloneSources
        self.fullRecurse = fullRecurse
        self.updateBuildInfo = updateBuildInfo

    def getPrimaryTroveList(self):
        return self.primaryTroveList

    def getCloneTargetLabelsForLabel(self, label):
        matches = set()
        for troveTup, newVersion in self.targetMap.iteritems():
            if troveTup[1].trailingLabel() == label:
                matches.add(newVersion.trailingLabel())
        return matches

    def setByDefaultMap(self, map):
        self.byDefaultMap = map

    def shouldClone(self, troveTup, sourceName=None):
        name, version, flavor = troveTup
        if self.byDefaultMap is not None:
            if troveTup not in self.byDefaultMap:
                return False
        if name.endswith(':source') and self.cloneSources:
            return True
        if (version.trailingLabel() not in self.labelMap
            and None not in self.labelMap):
            return False
        if self.fullRecurse:
            return True
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
        if sourceLabel in self.labelMap:
            targetLabel = self.labelMap[sourceLabel]
        else:
            targetLabel = self.labelMap.get(None, None)
        if targetLabel is None: 
            return None
        return version.branch().createSibling(targetLabel)

    def addTrove(self, troveTup, sourceName=None):
        name, version, flavor = troveTup
        newBranch = self.getTargetBranch(troveTup[1])
        if (name, newBranch, flavor) in self.trovesByTargetBranch:
            if self.trovesByTargetBranch[name, newBranch, flavor] == version:
                return
            raise CloneError("Cannot clone multiple troves with same name and"
                             " flavor to branch %s: %s[%s]" % (newBranch,
                                                               name, flavor))
        self.trovesByTargetBranch[name, newBranch, flavor] = version
        if name.endswith(':source'):
            self.trovesBySource.setdefault((name, version, flavor), [])
            return

        noFlavor = deps.parseFlavor('')
        sourceVersion = version.getSourceVersion(False)
        sourceTup = (sourceName, sourceVersion, noFlavor)
        if self.byDefaultMap is not None:
            self.byDefaultMap[sourceTup] = True
        self.trovesBySource.setdefault(sourceTup, []).append(troveTup)
        self.trovesByTargetBranch[sourceName, newBranch, noFlavor] = sourceVersion

    def iterSourceTargetBranches(self):
        for (name, newBranch, flavor), version  \
           in self.trovesByTargetBranch.iteritems():
            if name.endswith(':source'):
                yield (name, version, flavor), newBranch

    def iterBinaryTargetBranches(self):
        for (name, newBranch, flavor), version  \
           in self.trovesByTargetBranch.iteritems():
            if not name.endswith(':source'):
                yield (name, version, flavor), newBranch

    def getBinaryTrovesBySource(self):
        return self.trovesBySource.items()

    def target(self, troveTup, targetVersion):
        oldBranch = troveTup[1].branch()
        newBranch = targetVersion.branch()
        if not (newBranch.isAncestor(oldBranch) or newBranch.isSibling(oldBranch)):
            raise CloneError("clone only supports cloning troves to "
                             "parent and sibling branches")
        self.targetMap[troveTup] = targetVersion

    def getTargetVersion(self, troveTup):
        return self.targetMap.get(troveTup, None)

    def couldBePreClone(self, troveTup):
        info = (troveTup[0], troveTup[1].branch(), troveTup[2])
        if info in self.trovesByTargetBranch:
            return True
        return False

    def fileNeedsRewrite(self, version):
        targetBranch = self.getTargetBranch(version)
        return targetBranch and version.branch() != targetBranch

    def troveInfoNeedsErase(self, mark, troveTup):
        kind = mark[0]
        if kind != V_REFTRV:
            # we only erase trove references - all other types 
            # just let remain with their old, uncloned values.
            # This could change.
            return False
        return (self.byDefaultMap is not None 
                and troveTup not in self.byDefaultMap)

    def troveInfoNeedsRewrite(self, mark, troveTup):
        targetBranch = self.getTargetBranch(troveTup[1])
        kind = mark[0]
        if not targetBranch:
            return False
        if self.byDefaultMap is not None and troveTup not in self.byDefaultMap:
            return False

        if kind == V_REFTRV:
            # only rewrite trove info if we're cloning that trove.
            # otherwise, assume it's correct.
            return troveTup in self.targetMap

        if targetBranch == troveTup[1].branch():
            # this means that we're merely pushing this trove to tip
            # on same branch
            return False
        return self.updateBuildInfo

    def hasRewrite(self, troveTup):
        return troveTup in self.targetMap

class BranchCloneMap(CloneMap):
    def __init__(self, branchMap, troveList, fullRecurse=True,
                 cloneSources=True):
        CloneMap.__init__(self, {}, troveList, fullRecurse=fullRecurse,
                          cloneSources=cloneSources)
        self.branchMap = branchMap

    def shouldClone(self, troveTup, sourceName=None):
        # FIXME: this needs to share more with CloneMap.
        name, version, flavor = troveTup
        if self.byDefaultMap is not None:
            if troveTup not in self.byDefaultMap:
                return False

        if troveTup[0].endswith(':source') and self.cloneSources:
            return True
        if self.fullRecurse:
            return True
        return self._matchesPrimaryTrove(troveTup, sourceName)

    def getTargetBranch(self, version):
        sourceBranch = version.branch()
        if sourceBranch in self.branchMap:
            return self.branchMap[version.branch()]
        return self.branchMap.get(None, None)

class ClientClone:

    def createCloneChangeSet(self, targetBranch, troveList,
                             updateBuildInfo=True, message=DEFAULT_MESSAGE,
                             infoOnly=False, fullRecurse=False,
                             cloneSources=False, callback=None, 
                             trackClone=True):
        if callback is None:
            callback = callbacks.CloneCallback()
        callback.determiningCloneTroves()

        # make sure there are no zeroed timeStamps - targetBranch may be
        # a user-supplied string
        targetBranch = targetBranch.copy()
        targetBranch.resetTimeStamps()

        cloneMap = BranchCloneMap({None:targetBranch}, troveList,
                                    fullRecurse=fullRecurse,
                                    cloneSources=cloneSources)
        return self._createCloneChangeSet(cloneMap, updateBuildInfo,
                                          message=message,
                                          infoOnly=infoOnly, callback=callback,
                                          trackClone=trackClone)

    def createSiblingCloneChangeSet(self, labelMap, troveList,
                                    updateBuildInfo=True, infoOnly=False,
                                    callback=None, message=DEFAULT_MESSAGE,
                                    trackClone=True,
                                    cloneOnlyByDefaultTroves=False):
        cloneMap = CloneMap(labelMap, troveList)
        return self._createCloneChangeSet(cloneMap, updateBuildInfo,
                            message=message,
                            infoOnly=infoOnly, callback=callback,
                            trackClone=trackClone,
                            cloneOnlyByDefaultTroves=cloneOnlyByDefaultTroves)

    def _createCloneChangeSet(self, cloneMap,
                              updateBuildInfo=True, message=DEFAULT_MESSAGE,
                              infoOnly=False, callback=None, 
                              trackClone=True, cloneOnlyByDefaultTroves=False):
        if callback is None:
            callback = callbacks.CloneCallback()
        callback.determiningCloneTroves()
        troveCache = TroveCache(self.repos, callback)

        cloneJob = CloneJob(trackClone=trackClone, infoOnly=infoOnly,
                            callback=callback)
        if cloneOnlyByDefaultTroves:
            self._setByDefaultMap(cloneMap, troveCache)
        self._determineTrovesToClone(cloneMap, cloneJob, troveCache)
        callback.determiningTargets()

        leafMap = self._getExistingLeaves(cloneMap, troveCache)
        self._targetSources(cloneMap, cloneJob, leafMap, troveCache)
        self._targetBinaries(cloneMap, cloneJob, leafMap, troveCache)
        if cloneJob.isEmpty():
            log.warning('Nothing to clone!')
            return False, None

        self._checkNeedsFulfilled(cloneMap, cloneJob, leafMap, troveCache)
        cs, newFilesNeeded = self._buildChangeSet(cloneMap, cloneJob, leafMap,
                                                  troveCache)
        if cs is None:
            return False, None
        if infoOnly:
            return True, cs
        callback.gettingCloneData()
        self._addCloneFiles(cs, newFilesNeeded, callback)
        callback.done()
        return True, cs

    def _setByDefaultMap(self, cloneMap, troveCache):
        primaries = cloneMap.getPrimaryTroveList()
        troves = troveCache.getTroves(primaries, withFiles = False)
        byDefaultDict = dict.fromkeys(primaries, True)
        for trove in troves:
            # add all the troves that are byDefault True.
            # byDefault False ones we don't need to have in the dict.
            defaults = ((x[0], x[1]) for x in 
                        trove.iterTroveListInfo() if x[1])
            byDefaultDict.update(defaults)
        cloneMap.setByDefaultMap(byDefaultDict)

    def _determineTrovesToClone(self, cloneMap, cloneJob, troveCache):
        seen = set()
        toClone = cloneMap.getPrimaryTroveList()
        while toClone:
            needed = []

            for info in toClone:
                if info[0].startswith("fileset"):
                    raise CloneError("File sets cannot be cloned")

                if info not in seen:
                    needed.append(info)
                    seen.add(info)

            troves = troveCache.getTroves(needed, withFiles = False)
            newToClone = []
            for info, trv in itertools.izip(needed, troves):
                troveTup = trv.getNameVersionFlavor()
                if troveTup[0].endswith(':source'):
                    sourceName = None
                else:
                    sourceName = _getSourceName(trv)
                if cloneMap.shouldClone(troveTup, sourceName):
                    cloneMap.addTrove(troveTup, sourceName)
                    cloneJob.add(troveTup)
                newToClone.extend(trv.iterTroveList(strongRefs=True))

            toClone = newToClone

    def _getExistingLeaves(self, cloneMap, troveCache):
        leafMap = CurrentLeaves()
        query = []
        for sourceTup, targetBranch in cloneMap.iterSourceTargetBranches():
            query.append((sourceTup[0], targetBranch, sourceTup[2]))

        for binTup, targetBranch in cloneMap.iterBinaryTargetBranches():
            query.append((binTup[0], targetBranch, binTup[2]))
        result = self.repos.findTroves(None, query,
                                       defaultFlavor = deps.parseFlavor(''),
                                       getLeaves=False, allowMissing=True)
        if not result:
            return leafMap
        leafMap.addLeafResults(result)

        possiblePreClones = []
        for queryItem, tupList in result.iteritems():
            tupList = [ x for x in tupList if x[2] == queryItem[2] ]
            latest = sorted(tupList)[-1]
            if cloneMap.couldBePreClone(latest):
                possiblePreClones.append(latest)

        if not possiblePreClones:
            return leafMap

        troves = troveCache.getTroves(possiblePreClones, withFiles=False)
        for trove in troves:
            leafMap.addTrove(trove.getNameVersionFlavor(), 
                             trove.troveInfo.clonedFrom())
        return leafMap

    def _targetSources(self, cloneMap, cloneJob, leafMap, troveCache):
        hasTroves = self.repos.hasTroves(
                            [x[0] for x in cloneMap.iterSourceTargetBranches()])
        troveCache.getTroves([x[0] for x in hasTroves.items() if x[1]])
        for sourceTup, targetBranch in cloneMap.iterSourceTargetBranches():
            if hasTroves[sourceTup]:
                sourceTrove = troveCache.getTrove(sourceTup, withFiles=False)
                newVersion = leafMap.isAlreadyCloned([sourceTrove], targetBranch)
                if newVersion:
                    cloneMap.target(sourceTup, newVersion)
                    cloneJob.alreadyCloned(sourceTup)
                elif cloneMap.shouldClone(sourceTup):
                    newVersion = leafMap.createSourceVersion(sourceTup,
                                                             targetBranch)
                    cloneMap.target(sourceTup, newVersion)
                    cloneJob.target(sourceTup, newVersion)
                else:
                    newVersion = leafMap.hasAncestor(sourceTup, targetBranch,
                                                     self.repos)
                    if newVersion:
                        cloneMap.target(sourceTup, newVersion)
                        cloneJob.alreadyCloned(sourceTup)
                    else:
                        # should clone was false but the source trove exists - 
                        # we could have done this clone.
                        raise CloneError("Cannot find cloned source for %s=%s" \
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
                    raise CloneError("Cannot find required source %s on branch %s." \
                                     % (sourceTup[0], targetBranch))

    def _targetBinaries(self, cloneMap, cloneJob, leafMap, troveCache):
        for sourceTup, binaryList in cloneMap.getBinaryTrovesBySource():
            targetSourceVersion = cloneMap.getTargetVersion(sourceTup)
            targetBranch = targetSourceVersion.branch()

            byFlavor = {}
            for binaryTup in binaryList:
                byFlavor.setdefault(binaryTup[2], []).append(binaryTup)

            for flavor, binaryList in byFlavor.iteritems():
                # Binary list is a list of binaries all created from the
                # same cook command.
                binaryTroves = troveCache.getTroves(binaryList, withFiles=False)
                newVersion = leafMap.isAlreadyCloned(binaryTroves,
                                                     targetBranch)
                if newVersion:
                    for binaryTup in binaryList:
                        cloneMap.target(binaryTup, newVersion)
                        cloneJob.alreadyCloned(binaryTup)
                else:
                    newVersion = leafMap.createBinaryVersion(self.repos,
                                                         binaryList,
                                                         targetSourceVersion)
                    for binaryTup in binaryList:
                        cloneMap.target(binaryTup, newVersion)
                        cloneJob.target(binaryTup, newVersion)

    def _checkNeedsFulfilled(self, cloneMap, cloneJob, leafMap, troveCache):
        query = {}
        neededInfoTroveTups = {}

        for troveTup, newVersion in cloneJob.iterTargetList():
            for mark, src in _iterAllVersions(
                                troveCache.getTrove(troveTup, withFiles=False)):
                if (cloneMap.troveInfoNeedsRewrite(mark, src)
                    and not cloneMap.hasRewrite(src)):
                    neededInfoTroveTups.setdefault(src, []).append(mark)
        neededTroves = troveCache.getTroves(neededInfoTroveTups, withFiles=False)
        for trv, troveTup in itertools.izip(neededTroves, neededInfoTroveTups):
            targetBranch = cloneMap.getTargetBranch(troveTup[1])
            if leafMap.isAlreadyCloned(trv, targetBranch):
                continue
            marks = neededInfoTroveTups[troveTup]

            queryItem = troveTup[0], targetBranch, troveTup[2]
            if queryItem not in query:
                query[queryItem] = troveTup, marks
            query[queryItem][1].extend(marks)
        results = self.repos.findTroves(None, query, None, bestFlavor=True, 
                                        allowMissing=True)
        matches = []
        for queryItem, tupList in results.iteritems():
            sourceTup = query[queryItem][0]
            upstreamVersion = sourceTup[1].trailingRevision().getVersion()
            for troveTup in tupList:
                if (troveTup[1].trailingRevision().getVersion() == upstreamVersion
                    and sourceTup[2] == troveTup[2]):
                    matches.append(troveTup)
        troves = troveCache.getTroves(matches, withFiles = False)
        for trove in troves:
            leafMap.addTrove(trove.getNameVersionFlavor(), 
                             trove.troveInfo.clonedFrom())
        for queryItem, (sourceTup, markList) in query.items():
            trv = troveCache.getTrove(sourceTup, withFiles=False)
            newVersion = leafMap.isAlreadyCloned(trv, queryItem[1])
            if not newVersion:
                newVersion = leafMap.hasAncestor(sourceTup, queryItem[1], self.repos)
            if newVersion:
                cloneMap.target(sourceTup, newVersion)
                del query[queryItem]

        if query:
            raise CloneIncomplete(query.values())

    def _buildChangeSet(self, cloneMap, cloneJob, leafMap, troveCache):
        allFilesNeeded = []
        cs = changeset.ChangeSet()
        allTroveList = [x[0] for x in cloneJob.iterTargetList()]
        allTroves = troveCache.getTroves(allTroveList, withFiles=True)
        for troveTup, newVersion in cloneJob.iterTargetList():
            trv = troveCache.getTrove(troveTup, withFiles=True)
            newFilesNeeded = self._rewriteTrove(trv, newVersion, cloneMap, 
                                                cloneJob, leafMap, troveCache)
            if newFilesNeeded is None:
                return None, None
            allFilesNeeded.extend(newFilesNeeded)
            # make sure we haven't deleted all the child troves from 
            # a group.  This could happen, for example, if a group 
            # contains all byDefault False components.
            if trove.troveIsCollection(troveTup[0]):
                if not list(trv.iterTroveList(strongRefs=True)):
                    raise CloneError("Clone would result in empty collection %s=%s[%s]" % (troveTup))
            trvCs = trv.diff(None, absolute = True)[0]
            cs.newTrove(trvCs)
            if ":" not in trv.getName():
                cs.addPrimaryTrove(trv.getName(), trv.getVersion(),
                                   trv.getFlavor())
        return cs, allFilesNeeded

    def _rewriteTrove(self, trv, newVersion, cloneMap, 
                      cloneJob, leafMap, troveCache):
        filesNeeded = []
        troveName, troveFlavor = trv.getName(), trv.getFlavor()
        targetBranch = newVersion.branch()

        needsNewVersions = []
        # if this is a clone of a clone, use the original clonedFrom value
        # so that all clones refer back to the source-of-all-clones trove
        if trv.troveInfo.clonedFrom() is None and cloneJob.trackClone:
            trv.troveInfo.clonedFrom.set(trv.getVersion())

        # clone the labelPath 
        labelPath = list(trv.getLabelPath())
        labelPathMap = [(x, cloneMap.getCloneTargetLabelsForLabel(x))
                         for x in labelPath]
        labelPath = _computeLabelPath(trv.getName(), labelPathMap)
        if labelPath:
            trv.setLabelPath(labelPath)

        trv.changeVersion(newVersion)

        # look through files which aren't already on the right host for
        # inclusion in the change set
        newVersionHost = newVersion.trailingLabel().getHost()
        for (pathId, path, fileId, version) in trv.iterFileList():
            if version.getHost() != newVersionHost:
                filesNeeded.append((pathId, fileId, version))

        for mark, src in _iterAllVersions(trv):
            if cloneMap.troveInfoNeedsRewrite(mark, src):
                newVersion = cloneMap.getTargetVersion(src)
                _updateVersion(trv, mark, newVersion)
            elif cloneMap.troveInfoNeedsErase(mark, src):
                _updateVersion(trv, mark, None)

        for (pathId, path, fileId, version) in trv.iterFileList():
            if cloneMap.fileNeedsRewrite(version):
                needsNewVersions.append((pathId, path, fileId))

        # need to be reversioned
        if needsNewVersions:
            leafVersion = leafMap.getLeafVersion(troveName, targetBranch, 
                                                 troveFlavor)
            if leafVersion:
                # FIXME: parallelize this
                oldTrv = troveCache.getTrove((troveName, leafVersion,
                                              troveFlavor), withFiles = True)
                # pathId, fileId -> fileVersion map
                fileMap = dict(((x[0], x[2]), x[3]) for x in
                                        oldTrv.iterFileList())
            else:
                fileMap = {}

            for (pathId, path, fileId) in needsNewVersions:
                ver = fileMap.get((pathId, fileId), newVersion)
                trv.updateFile(pathId, path, ver, fileId)

        if trv.getName().endswith(':source') and not cloneJob.infoOnly:
            try:
                cl = cloneJob.callback.getCloneChangeLog(trv)
            except:
                log.error(str(cl))
                return None

            if cl is None:
                log.error("no change log message was given"
                          " for %s." % trv.getName())
                return None
            trv.changeChangeLog(cl)
        # reset the signatures, because all the versions have now
        # changed, thus invalidating the old sha1 hash
        trv.troveInfo.sigs.reset()
        if not cloneJob.infoOnly: # not computing signatures will 
                                  # make sure this doesn't get committed
            trv.computeSignatures()

        return filesNeeded

    def _addCloneFiles(self, cs, newFilesNeeded, callback):
        newFilesNeeded.sort()
        fileObjs = self.repos.getFileVersions(newFilesNeeded)
        contentsNeeded = []
        pathIdsNeeded = []
        fileObjsNeeded = []
        for ((pathId, newFileId, newFileVersion), fileObj) in \
                            itertools.izip(newFilesNeeded, fileObjs):
            (filecs, contentsHash) = changeset.fileChangeSet(pathId, None,
                                                             fileObj)

            cs.addFile(None, newFileId, filecs)
            if fileObj.hasContents:
                contentsNeeded.append((newFileId, newFileVersion))
                pathIdsNeeded.append(pathId)
                fileObjsNeeded.append(fileObj)

        contents = self.repos.getFileContents(contentsNeeded, callback=callback)
        for pathId, (fileId, fileVersion), fileCont, fileObj in \
                itertools.izip(pathIdsNeeded, contentsNeeded, contents, 
                               fileObjsNeeded):

            cs.addFileContents(pathId, fileId, changeset.ChangedFileTypes.file,
                               fileCont, cfgFile = fileObj.flags.isConfig(), 
                               compressed = False)


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
            trv.addTrove(name, newVersion, flavor, byDefault = byDefault,
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

def _getSourceName(trove):
    sourceName = trove.getSourceName()
    if sourceName is None:
        sourceName = trove.getName().split(':')[0] + ':source'
    return sourceName

class CloneError(errors.ClientError):
    pass

class CloneIncomplete(CloneError):

    def __str__(self):
        l = []
        for src, markList in self.needs:
            for mark in markList:
                what = "%s=%s[%s]" % (src[0], src[1].asString(), str(src[2]))
                if mark[0] == V_LOADED:
                    l.append("loadRecipe:        %s" % what)
                elif mark[0] == V_BREQ:
                    l.append("build requirement: %s" % what)
                elif mark[0] == V_REFTRV:
                    l.append("referenced trove:  %s" % what)

        return "Clone cannot be completed because some troves are not " + \
               "available on the target branch.\n\t" + \
               "\n\t".join(l)

    def __init__(self, needs):
        CloneError.__init__(self)
        self.needs = needs
