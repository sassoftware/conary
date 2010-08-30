# Copyright (c) 2010 rPath, Inc.
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

from conary import trove, versions
from conary.conaryclient import troveset, update
from conary.deps import deps
from conary.lib import log
from conary.repository import searchsource, trovecache, trovesource

class SystemModelTroveCache(trovecache.TroveCache):

    def __init__(self, db, repos, changeSetList = [], callback = None):
        self.db = db
        self.repos = repos
        self.callback = callback
        if changeSetList:
            csSource = trovesource.ChangesetFilesTroveSource(db)
            csSource.addChangeSets(changeSetList)
            troveSource = trovesource.SourceStack(csSource, repos)
        else:
            troveSource = repos

        trovecache.TroveCache.__init__(self, troveSource)

    def _caching(self, troveTupList):
        log.info("loading %d trove(s) from the repository, one of which is %s",
                  len(troveTupList), troveTupList[0])

class SysModelTupleSetMethods(object):

    def remove(self, removeTroveSet = None):
        return self._action(ActionClass = RemoveAction,
                            removeTroveSet = removeTroveSet)

    def replace(self, replaceTroveSet = None):
        return self._action(replaceTroveSet,
                            ActionClass = SysModelReplaceAction)

    def union(self, *troveSetList):
        return self._action(ActionClass = SysModelUnionAction, *troveSetList)

    def update(self, replaceTroveSet = None):
        return self._action(replaceTroveSet,
                            ActionClass = SysModelUpdateAction)

class SysModelDelayedTroveTupleSet(SysModelTupleSetMethods,
                                   troveset.DelayedTupleSet):

    pass

class SysModelDelayedTupleSetAction(troveset.DelayedTupleSetAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelInitialTroveTupleSet(SysModelTupleSetMethods,
                                   troveset.TroveTupleSet):

    def __init__(self, *args, **kwargs):
        troveTuple = kwargs.pop('troveTuple', None)
        troveset.TroveTupleSet.__init__(self, *args, **kwargs)
        if troveTuple is not None:
            self._setInstall(set(troveTuple))
        self.realized = True

class RemoveAction(SysModelDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, removeTroveSet = None):
        SysModelDelayedTupleSetAction.__init__(self, primaryTroveSet,
                                               removeTroveSet)
        self.removeTroveSet = removeTroveSet

    def __call__(self, data):
        removeSet = (self.removeTroveSet._getOptionalSet() |
                     self.removeTroveSet._getInstallSet())
        self.outSet._setInstall(self.primaryTroveSet._getInstallSet()
                                    - removeSet)
        self.outSet._setOptional(self.primaryTroveSet._getOptionalSet()
                                    | removeSet)

class SysModelFindAction(troveset.FindAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelFlattenAction(SysModelDelayedTupleSetAction):

    prefilter = troveset.FetchAction

    def __call__(self, data):
        installs = []
        available = []

        for refTrove, inInstall, explicit in self.primaryTroveSet._walk(
                                            data.troveCache, recurse = True):
            if inInstall:
                installs.append(refTrove)
            else:
                available.append(refTrove)

        self.outSet._setInstall(installs)
        self.outSet._setOptional(available)

class SysModelGetOptionalAction(SysModelDelayedTupleSetAction):

    def __call__(self, data):
        self.outSet._setOptional(self.primaryTroveSet._getOptionalSet())

class SysModelReplaceAction(troveset.ReplaceAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelUnionAction(troveset.UnionAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelUpdateAction(troveset.UpdateAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelSearchPathTroveSet(troveset.SearchPathTroveSet):

    def find(self, *troveSpecs):
        return self._action(ActionClass = SysModelFindAction, *troveSpecs)

class SystemModelClient(object):

    def systemModelGraph(self, sysModel):
        collections = set()
        for op in sysModel.systemItems:
            for troveTup in op:
                name = troveTup[0]
                if trove.troveIsComponent(name):
                    collections.add(name.split(':')[0])
                elif trove.troveIsGroup(name):
                    collections.add(name)

        # create the initial search path from the installLabelPath
        reposTroveSet = self._createRepositoryTroveSet()

        # now build new search path elements
        searchPathItems = []
        for searchItem in sysModel.searchPath:
            partialTup = searchItem.item
            if isinstance(partialTup, versions.Label):
                repos = troveset.SearchSourceTroveSet(
                        searchsource.NetworkSearchSource(self.getRepos(),
                                                         [ partialTup ],
                                                         self.cfg.flavor))
                searchPathItems.append(repos)
            elif partialTup[0] is not None:
                result = self.repos.findTrove(self.cfg.installLabelPath,
                                              partialTup, self.cfg.flavor)
                assert(len(result) == 1)
                ts = SysModelInitialTroveTupleSet(troveTuple = result,
                                                  graph = reposTroveSet.g)
                # get the trove itself
                fetched = ts._action(ActionClass = troveset.FetchAction)
                flattened = fetched._action(ActionClass = SysModelFlattenAction)
                searchPathItems.append(flattened)
            else:
                assert(0)

        searchPathItems.append(reposTroveSet)
        searchTroveSet = SysModelSearchPathTroveSet(searchPathItems,
                                                    graph = reposTroveSet.g)

        import systemmodel
        finalTroveSet = SysModelInitialTroveTupleSet(graph = searchTroveSet.g)
        for op in sysModel.systemItems:
            matches = searchTroveSet.find(*[ x for x in op ])
            if isinstance(op, systemmodel.InstallTroveOperation):
                finalTroveSet = finalTroveSet.union(matches)

                growSearchPath = False
                for troveSpec in op:
                    if troveSpec[0] in collections:
                        growSearchPath = True

                if growSearchPath:
                    flatten = matches._action(ActionClass =
                                                SysModelFlattenAction)
                    searchTroveSet = SysModelSearchPathTroveSet(
                            [ flatten, searchTroveSet ],
                            graph = searchTroveSet.g)
            elif isinstance(op, systemmodel.EraseTroveOperation):
                removeSet = searchTroveSet.find(*[ x for x in op ])
                finalTroveSet = finalTroveSet.remove(removeSet)
            elif isinstance(op, systemmodel.ReplaceTroveOperation):
                replaceSet = searchTroveSet.find(*[ x for x in op])
                finalTroveSet = finalTroveSet.replace(replaceSet)
            elif isinstance(op, systemmodel.UpdateTroveOperation):
                updateSet = searchTroveSet.find(*[ x for x in op])
                finalTroveSet = finalTroveSet.update(updateSet)
            else:
                assert(0)

        finalTroveSet.searchPath = searchTroveSet

        return finalTroveSet

    def _createRepositoryTroveSet(self):
        g = troveset.OperationGraph()
        repos = troveset.SearchSourceTroveSet(
                searchsource.NetworkSearchSource(self.getRepos(),
                                                 [],
                                                 self.cfg.flavor))
        return SysModelSearchPathTroveSet([ repos ], graph = g)

    def _updateFromTroveSetGraph(self, uJob, troveSet, split = True,
                            fromChangesets = [], criticalUpdateInfo=None,
                            applyCriticalOnly = False, restartInfo = None,
                            callback = None):
        """
        Populates an update job based on a set of trove update and erase
        operations.If self.cfg.autoResolve is set, dependencies
        within the job are automatically closed. Returns a mapping with
        suggestions for possible dependency resolutions.

        @param uJob: A L{conary.local.database.UpdateJob} object
        @type uJob: L{conary.local.database.UpdateJob}
        @param split: Split large update operations into separate jobs.
                      This must be true (False broke how we
                      handle users and groups, which requires info- packages
                      to be installed first and in separate jobs) if you
                      intend to install the job. We allow False here because
                      we don't need to do these calculations when the jobs
                      are being constructed for other reasons.
        @type split: bool
        @param fromChangesets: When specified, this list of
        L{changeset.ChangeSetFromFile} objects is used as the source of troves,
        instead of the repository.
        @type fromChangesets: list
        @param criticalUpdateInfo: Settings and data needed for critical
        updates
        @type criticalUpdateInfo: L{CriticalUpdateInfo}
        @param applyCriticalOnly: apply only the critical update.
        @type applyCriticalOnly: bool
        @param restartInfo: If specified, overrides itemList. It specifies the
        location where the rest of an update job run was stored (after
        applying the critical update).
        @type restartInfo: string
        @rtype: dict

        @raise ConaryError: if a C{sync} operation was requested, and
            relative changesets were specified.

        @raise InternalConaryError: if a jobset was inconsistent.

        @raise UpdateError: Generic update error.

        @raise MissingTrovesError: if one of the requested troves could not
            be found.

        @raise other: Callbacks may generate exceptions on their own. See
            L{applyUpdateJob} for an explanation of the behavior of exceptions
            within callbacks.
        """
        if criticalUpdateInfo is None:
            criticalUpdateInfo = update.CriticalUpdateInfo()

        searchPath = troveSet.searchPath
        troveCache = SystemModelTroveCache(self.getDatabase(),
                                           self.getRepos(),
                                           changeSetList = fromChangesets,
                                           callback = callback)

        # we need to explicitly fetch this before we can walk it
        preFetch = troveSet._action(ActionClass = troveset.FetchAction)
        availForDeps = preFetch._action(ActionClass = SysModelGetOptionalAction)
        preFetch.g.realize(troveset.ActionData(troveCache, self.cfg.flavor[0]))

        existsTrv = trove.Trove("@update", versions.NewVersion(),
                                deps.Flavor(), None)
        targetTrv = trove.Trove("@update", versions.NewVersion(),
                                deps.Flavor(), None)

        for tup in self.db.iterAllTroves():
            existsTrv.addTrove(*tup)

        for tup, inInstall, explicit in preFetch._walk(troveCache, recurse = True):
            if inInstall:
                targetTrv.addTrove(*tup[0:3])

        job = targetTrv.diff(existsTrv)[2]

        depResolveSource = searchPath._getResolveSource()
        resolveMethod = depResolveSource.getResolveMethod()

        uJob.setSearchSource(self.getSearchSource())

        pathHashCache = {}

        resolveDeps = split = True
        if resolveDeps or split:
            check = self.db.getDepStateClass(troveCache,
               findOrdering = split,
               ignoreDepClasses = self.cfg.ignoreDependencies)

            linkedJobs = self._findOverlappingJobs(job, troveCache,
                                      pathHashCache = pathHashCache)

            criticalJobs = criticalUpdateInfo.findCriticalJobs(job)
            finalJobs = criticalUpdateInfo.findFinalJobs(job)
            criticalOnly = criticalUpdateInfo.isCriticalOnlyUpdate()

            log.info("resolving dependencies")
            result = check.depCheck(job,
                                    linkedJobs = linkedJobs,
                                    criticalJobs = criticalJobs,
                                    finalJobs = finalJobs,
                                    criticalOnly = criticalOnly)

            suggMap = {}
            while True:
                unsatisfied = result.unsatisfiedList
                unsatisfied += [ x[0:2] for x in result.unresolveableList ]

                if not resolveMethod.prepareForResolution(unsatisfied):
                    break

                sugg = resolveMethod.resolveDependencies()
                newJob = resolveMethod.filterSuggestions(
                                    result.unsatisfiedList, sugg, suggMap)
                newTroves = []

                for (name, oldInfo, newInfo, isAbsolute) in newJob:
                    assert(isAbsolute)
                    log.info("adding new trove %s", name)
                    targetTrv.addTrove(name, newInfo[0], newInfo[1])

                job = targetTrv.diff(existsTrv)[2]

                log.info("resolving dependencies")
                result = check.depCheck(job,
                                        linkedJobs = linkedJobs,
                                        criticalJobs = criticalJobs,
                                        finalJobs = finalJobs,
                                        criticalOnly = criticalOnly)

            log.info("job dependency closed; %s jobs resulted", len(job))

            # if any of the things we're about to install or remove use
            # capsules we cannot split the job
            if not split:
                splitJob = [ job ]
                criticalUpdates = []
            else:
                splitJob = result.getChangeSetList()
                criticalUpdates = result.getCriticalUpdates()

            if result.unsatisfiedList:
                raise update.DepResolutionFailure(
                            self.cfg, result.unsatisfiedList,
                            suggMap, result.unresolveableList, splitJob,
                            criticalUpdates)
            elif result.unresolveableList:
                # this can't happen because dep resolution empties
                # the unresolveableList into the unsatisfiedList to try
                # and find matches
                assert(0)
        else:
            (depList, suggMap, cannotResolve, splitJob, keepList,
             criticalUpdates) = ( [], {}, [], [ job ], [], [] )

        infoCs = troveCache.troveSource.createChangeSet(job, withFiles = False,
                                                        callback = callback)
        uJob.getTroveSource().addChangeSet(infoCs)

        ts = uJob.getTroveSource()
        troveSourceCallback = lambda job: ts.createChangeSet([ job ],
                                                         withFiles = False)[0]

        # this prevents us from using the changesetList as a searchSource
        self._processJobList(job, uJob, troveSourceCallback)
        self._combineJobs(uJob, splitJob, criticalUpdates)
        uJob.setTransactionCounter(self.db.getTransactionCounter())


