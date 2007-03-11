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

class ClientClone:

    def createCloneChangeSet(self, targetBranch, troveList,
                             updateBuildInfo=True, message=DEFAULT_MESSAGE,
                             infoOnly=False, fullRecurse=False,
                             cloneSources=False, callback=None, 
                             trackClone=True):
        """
        """
        if callback is None:
            callback = callbacks.CloneCallback()
        callback.determiningCloneTroves()

        # make sure there are no zeroed timeStamps - targetBranch may be
        # a user-supplied string
        targetBranch = targetBranch.copy()
        targetBranch.resetTimeStamps()

        cloneOptions = CloneOptions(
                            fullRecurse=fullRecurse,
                            cloneSources=cloneSources,
                            trackClone=trackClone,
                            callback=callback,
                            message=message,
                            updateBuildInfo=updateBuildInfo,
                            infoOnly=infoOnly)


        chooser = BranchCloneChooser({None:targetBranch}, troveList,
                                      cloneOptions)
        return self._createCloneChangeSet(chooser, cloneOptions)

    def createSiblingCloneChangeSet(self, labelMap, troveList,
                                    updateBuildInfo=True, infoOnly=False,
                                    callback=None, message=DEFAULT_MESSAGE,
                                    trackClone=True,
                                    cloneOnlyByDefaultTroves=False,
                                    cloneSources=True):
        cloneOptions = CloneOptions(fullRecurse=True,
                            cloneSources=cloneSources,
                            trackClone=trackClone,
                            callback=callback,
                            message=message,
                            cloneOnlyByDefaultTroves=cloneOnlyByDefaultTroves,
                            updateBuildInfo=updateBuildInfo,
                            infoOnly=infoOnly)


        chooser = CloneChooser(labelMap, troveList, cloneOptions)
        return self._createCloneChangeSet(chooser, cloneOptions)

    def _createCloneChangeSet(self, chooser, cloneOptions):
        callback = cloneOptions.callback
        callback.determiningCloneTroves()
        troveCache = TroveCache(self.repos, callback)

        cloneJob, cloneMap, leafMap = self._createCloneJob(cloneOptions,
                                                           chooser,
                                                           troveCache)
        if cloneJob.isEmpty():
            log.warning('Nothing to clone!')
            return False, None

        cs, newFilesNeeded = self._buildChangeSet(chooser, cloneMap, cloneJob,
                                                  leafMap, troveCache)
        if cs is None:
            return False, None

        if cloneOptions.infoOnly:
            return True, cs
        callback.gettingCloneData()
        self._addCloneFiles(cs, newFilesNeeded, callback)
        callback.done()
        return True, cs

    def _createCloneJob(self, cloneOptions, chooser, troveCache):
        cloneJob = CloneJob(cloneOptions)
        cloneMap = CloneMap()
        chooser.setCloneMap(cloneMap)
        if cloneOptions.cloneOnlyByDefaultTroves:
            self._setByDefaultMap(chooser, troveCache)
        self._determineTrovesToClone(chooser, cloneMap, cloneJob, troveCache)
        cloneOptions.callback.determiningTargets()

        leafMap = self._getExistingLeaves(cloneMap, troveCache)
        self._targetSources(chooser, cloneMap, cloneJob, leafMap, troveCache)
        self._targetBinaries(cloneMap, cloneJob, leafMap, troveCache)

        # some clones may rewrite the child troves (if cloneOnlyByDefaultTroves
        # is True).  We need to make sure that any precloned aren't having
        # the list of child troves changed.
        self._recheckPreClones(cloneJob, cloneMap, troveCache, chooser,
                               leafMap)

        troveTups = cloneJob.getTrovesToClone()
        unmetNeeds = self._checkNeedsFulfilled(troveTups, chooser, cloneMap,
                                               leafMap, troveCache)
        if unmetNeeds:
            raise CloneIncomplete(unmetNeeds)


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

    def _determineTrovesToClone(self, chooser, cloneMap, cloneJob, troveCache):
        seen = set()
        toClone = chooser.getPrimaryTroveList()
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
                if chooser.shouldClone(troveTup, sourceName):
                    targetBranch = chooser.getTargetBranch(troveTup[1])
                    cloneMap.addTrove(troveTup, targetBranch, sourceName)
                    chooser.addSource(troveTup, sourceName)
                    cloneJob.add(troveTup)
                newToClone.extend(trv.iterTroveList(strongRefs=True))

            toClone = newToClone

    def _getExistingLeaves(self, cloneMap, troveCache):
        """
            Gets the needed information about the current repository state
            to find out what clones may have already been performed
            (and should have their clonedFrom fields checked to be sure)
        """
        leafMap = LeafMap()
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
        self._addClonedFromInfo(troveCache, leafMap, possiblePreClones)
        return leafMap

    def _addClonedFromInfo(self, troveCache, leafMap, tupList):
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
        toGet = dict((x, [x]) for x in tupList)

        while toGet:
            newToGet = {}
            hasTroves = {}
            hasTrovesByHost = {}
            # sort by host so that if a particular repository is down
            # we can continue to look at the rest of the clonedFrom info.
            for troveTup in toGet:
                host = troveTup[1].trailingLabel().getHost()
                hasTrovesByHost.setdefault(host, []).append(troveTup)

            for host, troveTups in hasTrovesByHost.items():
                try:
                    results = troveCache.hasTroves(troveTups)
                except neterrors.OpenError, msg:
                    log.warning('Could not access host %s: %s' % (host, msg))
                    results = dict((x, False) for x in troveTups)
                hasTroves.update(results)
            troves = troveCache.getTroves([ x for x in toGet if hasTroves[x]], 
                                           withFiles=False)
            for trove in troves:
                troveTup = trove.getNameVersionFlavor()
                origTups = toGet[troveTup]
                clonedFrom = trove.troveInfo.clonedFrom()
                if clonedFrom:
                    for origTup in origTups:
                        clonedFromInfo[origTup].add(clonedFrom)

                    l = newToGet.setdefault(
                                (troveTup[0], clonedFrom, troveTup[2]), [])
                    l.extend(origTups)
            toGet = newToGet
        for troveTup, clonedFrom in clonedFromInfo.iteritems():
            leafMap.addTrove(troveTup, clonedFrom)

    def _targetSources(self, chooser, cloneMap, cloneJob, leafMap, troveCache):
        hasTroves = self.repos.hasTroves(
                        [x[0] for x in cloneMap.iterSourceTargetBranches()])
        presentTroveTups = [x[0] for x in hasTroves.items() if x[1]]
        self._addClonedFromInfo(troveCache, leafMap, presentTroveTups)

        for sourceTup, targetBranch in cloneMap.iterSourceTargetBranches():
            if hasTroves[sourceTup]:
                newVersion = leafMap.isAlreadyCloned(sourceTup, targetBranch)
                if newVersion:
                    cloneMap.target(sourceTup, newVersion)
                    cloneJob.alreadyCloned(sourceTup)
                elif chooser.shouldClone(sourceTup):
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

    def _targetBinaries(self, cloneMap, cloneJob, leafMap, troveCache):
        allBinaries = itertools.chain(*[x[1] for x in
                                        cloneMap.getBinaryTrovesBySource()])
        self._addClonedFromInfo(troveCache, leafMap, allBinaries)
        for sourceTup, binaryList in cloneMap.getBinaryTrovesBySource():
            targetSourceVersion = cloneMap.getTargetVersion(sourceTup)
            targetBranch = targetSourceVersion.branch()

            byFlavor = {}
            for binaryTup in binaryList:
                byFlavor.setdefault(binaryTup[2], []).append(binaryTup)

            for flavor, binaryList in byFlavor.iteritems():
                # Binary list is a list of binaries all created from the
                # same cook command.
                newVersion = leafMap.isAlreadyCloned(binaryList,
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

    def _checkNeedsFulfilled(self, troveTups, chooser, cloneMap, leafMap,
                             troveCache):
        query = {}
        neededInfoTroveTups = {}

        for troveTup in troveTups:
            trv = troveCache.getTrove(troveTup, withFiles=False)
            for mark, src in _iterAllVersions(trv):
                if (chooser.troveInfoNeedsRewrite(mark, src)
                    and not cloneMap.hasRewrite(src)):
                    neededInfoTroveTups.setdefault(src, []).append(mark)

        self._addClonedFromInfo(troveCache, leafMap, neededInfoTroveTups)

        for troveTup in neededInfoTroveTups:
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
        self._addClonedFromInfo(troveCache, leafMap, matches)
        for queryItem, (sourceTup, markList) in query.items():
            newVersion = leafMap.isAlreadyCloned(sourceTup, queryItem[1])
            if not newVersion:
                newVersion = leafMap.hasAncestor(sourceTup, queryItem[1], self.repos)
            if newVersion:
                cloneMap.target(sourceTup, newVersion)
                del query[queryItem]
        return query.values()

    def _recheckPreClones(self, cloneJob, cloneMap, troveCache, chooser, 
                          leafMap):
        # We only child for missing trove references, not build reqs for 
        # reclones.  Otherwise you could have to reclone when minor details
        # about the entironment have changed.
        troveTups = cloneJob.getPreclonedTroves()
        toReclone = []
        # match up as many needed targets for these clone as possible.
        for troveTup in troveTups:
            if troveTup[0].endswith(':source'):
                # we don't need to worry about recloning sources.
                # sources don't have troveInfo that we want to rewrite.
                continue
            newVersion = cloneMap.getTargetVersion(troveTup)
            clonedTup = (troveTup[0], newVersion, troveTup[2])
            trv, clonedTrv = troveCache.getTroves([troveTup, clonedTup],
                                                  withFiles=False)
            if self._shouldReclone(trv, clonedTrv, chooser, cloneMap):
                toReclone.append(troveTup)

        trovesBySource = cloneMap.getTrovesWithSameSource(toReclone)
        for binaryList in trovesBySource:
            sourceVersion = cloneMap.getSourceVersion(binaryList[0])
            targetSourceVersion = cloneMap.getTargetVersion(sourceVersion)
            newVersion = leafMap.createBinaryVersion(self.repos,
                                                     binaryList,
                                                     targetSourceVersion)
            for binaryTup in binaryList:
                cloneMap.target(binaryTup, newVersion)
                cloneJob.target(binaryTup, newVersion)

    def _shouldReclone(self, origTrove, clonedTrove, chooser, cloneMap):
        childTroves = {}
        clonedChildTroves = {}
        for mark, src in _iterAllVersions(origTrove, rewriteTroveInfo=False):
            if chooser.troveInfoNeedsRewrite(mark, src):
                targetBranch = chooser.getTargetBranch(src[1])
                childTroves[src[0], targetBranch, src[2]] = True
            elif chooser.troveInfoNeedsErase(mark, src):
                continue
            else:
                childTroves[src[0], src[1].branch(), src[2]] = True

        for mark, src in _iterAllVersions(clonedTrove, rewriteTroveInfo=False):
            clonedChildTroves[src[0], src[1].branch(), src[2]] = True
        if childTroves == clonedChildTroves:
            return False
        return True

    def _buildChangeSet(self, chooser, cloneMap, cloneJob, leafMap, troveCache):
        allFilesNeeded = []
        cs = changeset.ChangeSet()
        allTroveList = [x[0] for x in cloneJob.iterTargetList()]
        allTroves = troveCache.getTroves(allTroveList, withFiles=True)
        for troveTup, newVersion in cloneJob.iterTargetList():
            trv = troveCache.getTrove(troveTup, withFiles=True)
            newFilesNeeded = self._rewriteTrove(trv, newVersion, chooser,
                                                cloneMap, cloneJob, leafMap,
                                                troveCache)
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

    def _rewriteTrove(self, trv, newVersion, chooser, cloneMap,
                      cloneJob, leafMap, troveCache):
        filesNeeded = []
        troveName, troveFlavor = trv.getName(), trv.getFlavor()
        targetBranch = newVersion.branch()

        needsNewVersions = []
        # if this is a clone of a clone, use the original clonedFrom value
        # so that all clones refer back to the source-of-all-clones trove
        if cloneJob.options.trackClone:
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
            if chooser.troveInfoNeedsRewrite(mark, src):
                newVersion = cloneMap.getTargetVersion(src)
                _updateVersion(trv, mark, newVersion)
            elif chooser.troveInfoNeedsErase(mark, src):
                _updateVersion(trv, mark, None)

        for (pathId, path, fileId, version) in trv.iterFileList():
            if chooser.fileNeedsRewrite(version):
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

        infoOnly = cloneJob.options.infoOnly
        if trv.getName().endswith(':source') and not infoOnly:
            try:
                cl = cloneJob.options.callback.getCloneChangeLog(trv)
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
        if not infoOnly: # not computing signatures will 
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

class CloneOptions(object):
    def __init__(self, fullRecurse=True, cloneSources=True,
                       trackClone=True, callback=None,
                       message=DEFAULT_MESSAGE, cloneOnlyByDefaultTroves=False,
                       updateBuildInfo=True, infoOnly=False):
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

class TroveCache(object):
    def __init__(self, repos, callback):
        self.troves = {True : {}, False : {}}
        self.repos = repos
        self.callback = callback

    def hasTroves(self, troveTups):
        # FIXME: cache this.
        return self.repos.hasTroves(troveTups)

    def getTroves(self, troveTups, withFiles=True):
        theDict = self.troves[withFiles]
        needed = [ x for x in troveTups if x not in theDict ]
        troves = self.repos.getTroves(troveTups, withFiles=withFiles,
                                      callback=self.callback)
        self.troves.update(itertools.izip(needed, troves))
        return [ self.troves[x] for x in troveTups]

    def getTrove(self, troveTup, withFiles=True):
        return self.getTroves([troveTup], withFiles=True)[0]

class CloneChooser(object):
    def __init__(self, labelMap, primaryTroveList, cloneOptions):
        self.primaryTroveList = primaryTroveList
        self.labelMap = labelMap
        self.byDefaultMap = None
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

    def shouldClone(self, troveTup, sourceName=None):
        name, version, flavor = troveTup
        if self.byDefaultMap is not None:
            if troveTup not in self.byDefaultMap:
                return False
        if name.endswith(':source') and self.options.cloneSources:
            return True
        if (version.trailingLabel() not in self.labelMap
            and None not in self.labelMap):
            return False
        if self.options.fullRecurse:
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
            return troveTup in self.cloneMap.targetMap

        if targetBranch == troveTup[1].branch():
            # this means that we're merely pushing this trove to tip
            # on same branch
            return False
        return self.options.updateBuildInfo

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

class BranchCloneChooser(CloneChooser):
    def __init__(self, branchMap, troveList, cloneOptions):
        CloneChooser.__init__(self, {}, troveList, cloneOptions)
        self.branchMap = branchMap

    def shouldClone(self, troveTup, sourceName=None):
        # FIXME: this needs to share more with CloneMap.
        name, version, flavor = troveTup
        if self.byDefaultMap is not None:
            if troveTup not in self.byDefaultMap:
                return False

        if troveTup[0].endswith(':source') and self.options.cloneSources:
            return True
        if self.options.fullRecurse:
            return True
        return self._matchesPrimaryTrove(troveTup, sourceName)

    def getTargetBranch(self, version):
        sourceBranch = version.branch()
        if sourceBranch in self.branchMap:
            return self.branchMap[version.branch()]
        return self.branchMap.get(None, None)


class CloneMap(object):
    def __init__(self):
        self.targetMap = {}
        self.trovesByTargetBranch = {}
        self.trovesBySource = {}
        self.sourcesByTrove = {}

    def addTrove(self, troveTup, targetBranch, sourceName=None):
        name, version, flavor = troveTup
        if (name, targetBranch, flavor) in self.trovesByTargetBranch:
            if self.trovesByTargetBranch[name, targetBranch, flavor] == version:
                return
            raise CloneError("Cannot clone multiple troves with same name and"
                             " flavor to branch %s: %s[%s]" % (targetBranch,
                                                               name, flavor))
        self.trovesByTargetBranch[name, targetBranch, flavor] = version
        if name.endswith(':source'):
            self.trovesBySource.setdefault((name, version, flavor), [])
            return

        noFlavor = deps.parseFlavor('')
        sourceVersion = version.getSourceVersion(False)
        sourceTup = (sourceName, sourceVersion, noFlavor)
        self.trovesBySource.setdefault(sourceTup, []).append(troveTup)
        self.sourcesByTrove[troveTup] = sourceTup
        self.trovesByTargetBranch[sourceName, targetBranch, noFlavor] = sourceVersion

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
        if not (targetBranch.isAncestor(oldBranch) 
                or targetBranch.isSibling(oldBranch)):
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

    def hasRewrite(self, troveTup):
        return troveTup in self.targetMap

    def getCloneTargetLabelsForLabel(self, label):
        matches = set()
        for troveTup, newVersion in self.targetMap.iteritems():
            if troveTup[1].trailingLabel() == label:
                matches.add(newVersion.trailingLabel())
        return matches


class LeafMap(object):
    def __init__(self):
        self.clonedFrom = {}
        self.branchMap = {}

    def addTrove(self, troveTup, clonedFrom=None):
        name, version, flavor = troveTup
        if clonedFrom is None:
            clonedFrom = set([troveTup[1]])
        self.clonedFrom[troveTup] = clonedFrom

    def getClonedFrom(self, troveTup):
        if troveTup in self.clonedFrom:
            return self.clonedFrom[troveTup]
        return set([troveTup[1]])

    def addLeafResults(self, branchMap):
        self.branchMap.update(branchMap)

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

    def isAlreadyCloned(self, troveTupleList, targetBranch):
        if not isinstance(troveTupleList, list):
            troveTupleList = [troveTupleList]
        finalTargetVersion = None
        for troveTup in troveTupleList:
            myClonedFrom = self.getClonedFrom(troveTup)
            name, version, flavor = troveTup
            targetVersion = self.getLeafVersion(name, targetBranch, flavor)
            if not targetVersion:
                return False

            targetTup = name, targetVersion, flavor
            targetClonedFrom = self.getClonedFrom(targetTup)
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

        while desiredVersion in targetBranchVersionList:
            desiredVersion.incrementSourceCount()

        return desiredVersion

    def createBinaryVersion(self, repos, binaryList, sourceVersion):
        flavor = binaryList[0][2]
        # We should be able to avoid the repos calls made in here...
        # but it may not be worth it.
        return nextVersion(repos, None, [x[0] for x in binaryList],
                           sourceVersion, flavor)


