
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

import itertools

from conary import errors, trove, versions
from conary.conaryclient import troveset, update
from conary.deps import deps
from conary.lib import log, util
from conary.repository import searchsource, trovecache, trovesource

class SysModelActionData(troveset.ActionData):

    def __init__(self, troveCache, flavor, repos, cfg):
        troveset.ActionData.__init__(self, troveCache, flavor)
        self.repos = repos
        self.cfg = cfg

class SystemModelTroveCache(trovecache.TroveCache):

    def __init__(self, db, repos, changeSetList = [], callback = None):
        self.db = db
        self.repos = repos
        self.callback = callback
        self.componentMap = {}
        self._startingSizes = (0, 0)
        if changeSetList:
            csSource = trovesource.ChangesetFilesTroveSource(db)
            csSource.addChangeSets(changeSetList)
            troveSource = trovesource.SourceStack(csSource, repos)
        else:
            troveSource = repos

        trovecache.TroveCache.__init__(self, troveSource)

    def _caching(self, troveTupList):
        local = [ x for x in troveTupList if x[1].isOnLocalHost() ]

        if local:
            troves = self.db.getTroves(local)

            gotTups = []
            gotTrvs = []
            for troveTup, trv in itertools.izip(local, troves):
                if trv is None:
                    continue

                gotTups.append(troveTup)
                gotTrvs.append(trv)
                troveTupList.remove(troveTup)

            self._addToCache(gotTups, gotTrvs)

        if troveTupList:
            log.info("loading %d trove(s) from the repository, "
                     "one of which is %s", len(troveTupList), troveTupList[0])

    def _cached(self, troveTupList, troveList):
        for tup, trv in itertools.izip(troveTupList, troveList):
            for name, version, flavor in trv.iterTroveList(strongRefs = True,
                                                           weakRefs = True):
                if not trove.troveIsComponent(name):
                    continue

                pkgName = name.split(':')[0]
                tup = (pkgName, version, flavor)
                l = self.componentMap.get(tup)
                if l is None:
                    l = []
                    self.componentMap[tup] = l
                l.append(name)

    def cacheComponentMap(self, pkgList):
        need = [ x for x in pkgList if
                  (not self.troveIsCached(x) and x not in self.componentMap) ]
        self.cacheTroves(need)

    def cacheModified(self):
        return (len(self.cache), len(self.depCache)) != self._startingSizes

    def getPackageComponents(self, troveTup):
        if self.troveIsCached(troveTup):
            trv = self.getTrove(withFiles = False, *troveTup)
            return  [ x[0] for x in trv.iterTroveList(strongRefs = True,
                                                      weakRefs = True) ]

        return self.componentMap[troveTup]

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

    def __init__(self, *args, **kwargs):
        troveset.DelayedTupleSet.__init__(self, *args, **kwargs)
        self._flat = False

    def _walk(self, *args, **kwargs):
        if self._flat:
            # this is easy
            result = []

            for (troveTup) in self._getInstallSet():
                result.append( (troveTup, True, True) )

            for (troveTup) in self._getOptionalSet():
                result.append( (troveTup, False, True) )

            return result

        return troveset.DelayedTupleSet._walk(self, *args, **kwargs)

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

class SysModelExcludeTrovesAction(SysModelDelayedTupleSetAction):

    def __init__(self, *args, **kwargs):
        self.excludeTroves = kwargs.pop('excludeTroves')
        SysModelDelayedTupleSetAction.__init__(self, *args, **kwargs)

    def __call__(self, data):
        installSet = set()
        optionalSet = set()
        for troveTup, inInstall, isExplicit in (
                     self.primaryTroveSet._walk(data.troveCache,
                                     newGroups = False,
                                     recurse = True)):
            if self.excludeTroves.match(troveTup[0]):
                if inInstall or isExplicit:
                    optionalSet.add(troveTup)
            elif isExplicit:
                if inInstall:
                    installSet.add(troveTup)
                else:
                    optionalSet.add(troveTup)

        self.outSet._setInstall(installSet)
        self.outSet._setOptional(optionalSet)

class SysModelFindAction(troveset.FindAction):

    resultClass = SysModelDelayedTroveTupleSet

    # this not only finds, but it fetches and finds as well. it's a pretty
    # convienent way of handling redirects

    def __call__(self, actionList, data):
        troveset.FindAction.__call__(self, actionList, data)

        fetchActions = []
        for action in actionList:
            action.outSet.realized = True
            newAction = SysModelFetchAction(action.outSet, all = True)
            newAction.getResultTupleSet(action.primaryTroveSet.g)
            fetchActions.append(newAction)

        SysModelFetchAction.__call__(fetchActions[0], fetchActions, data)

        redirects = []
        for action in actionList:
            installSet = set()
            optionalSet = set()

            for troveTup, inInstall in ( itertools.chain(
                    itertools.izip( action.outSet.installSet,
                                    itertools.repeat(True)),
                    itertools.izip( action.outSet.optionalSet,
                                    itertools.repeat(True)) ) ):

                assert(data.troveCache.troveIsCached(troveTup))

                trv = data.troveCache.getTrove(withFiles = False, *troveTup);
                if trv.isRedirect():
                    log.info("following redirect %s=%s[%s]", *troveTup)
                    redirects.append( (troveTup, inInstall) )
                elif inInstall:
                    installSet.add(troveTup)
                else:
                    optionalSet.add(troveTup)

            action.outSet.installSet.clear()
            action.outSet.optionalSet.clear()
            # caller gets to set this for us
            action.realized = False

            self._redirects(data, redirects, optionalSet, installSet)

            action.outSet._setOptional(optionalSet)
            action.outSet._setInstall(installSet)

    def _redirects(self, data, redirectList, optionalSet, installSet):
        q = util.IterableQueue()
        # this tells the code to fetch troves
        q.add((None, None))
        seen = set()
        atEnd = False
        for (troveTup, inInstall) in itertools.chain(redirectList, q):
            if troveTup is None:
                data.troveCache.getTroves(seen, withFiles = False)
                if not atEnd:
                    q.add((None, None))
                atEnd = True
                continue

            atEnd = False
            trv = data.troveCache.getTrove(withFiles = False, *troveTup)

            if not trv.isRedirect():
                if inInstall:
                    installSet.add(troveTup)
                else:
                    optionalSet.add(troveTup)

                continue

            targets = [ (x[0], str(x[1].label()), x[2])
                                    for x in trv.iterRedirects() ]

            if not targets:
                # this is a remove redirect. that's easy, just keep going
                continue

            matches = data.repos.findTroves([], targets, data.cfg.flavor)
            for matchList in matches.itervalues():
                for match in matchList:
                    if match in seen:
                        raise update.UpdateError, \
                            "Redirect loop found which includes " \
                            "troves %s, %s" % (troveTup[0], match[0])

                    seen.add(match)
                    q.add((match, inInstall))


class SysModelFetchAction(troveset.FetchAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelFinalFetchAction(SysModelFetchAction):

    def _fetch(self, actionList, data):
        troveTuples = set()

        for action in actionList:
            troveTuples.update(troveTup for troveTup, inInstall, isExplicit in
                                 action.primaryTroveSet._walk(data.troveCache,
                                                 newGroups = False,
                                                 recurse = True)
                            if (inInstall and
                                (trove.troveIsGroup(troveTup[0]) or isExplicit)
                               ) )

        data.troveCache.getTroves(troveTuples, withFiles = False)

class SysModelFlattenAction(SysModelDelayedTupleSetAction):

    prefilter = SysModelFetchAction

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
        self.outSet._flat = True

class SysModelReplaceAction(troveset.ReplaceAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelUnionAction(troveset.UnionAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelUpdateAction(troveset.UpdateAction):

    resultClass = SysModelDelayedTroveTupleSet

class SysModelSearchPathTroveSet(troveset.SearchPathTroveSet):

    def _getResolveSource(self, filterFn):
        # don't bother with items in the install set; those are being installed
        # already so aren't a good choice for suggestions
        sourceList = []
        for ts in self.troveSetList:
            if isinstance(ts, troveset.TroveTupleSet):
                sourceList.append(ts._getResolveSource(filterFn = filterFn))
            elif isinstance(ts, SysModelSearchPathTroveSet):
                sourceList.append(ts._getResolveSource(filterFn = filterFn))
            else:
                sourceList.append(ts._getResolveSource())

        return searchsource.SearchSourceStack(*sourceList)

    def find(self, *troveSpecs):
        return self._action(ActionClass = SysModelFindAction, *troveSpecs)

    def hasOptionalTrove(self, troveTup):
        for ts in self.troveSetList:
            if isinstance(ts, troveset.TroveTupleSet):
                if troveTup in ts._getOptionalSet():
                    return True

        return False

class SystemModelClient(object):

    def systemModelGraph(self, sysModel, changeSetList = []):
        collections = set()
        for op in sysModel.systemItems:
            for troveTup in op:
                name = troveTup[0]
                if trove.troveIsComponent(name):
                    collections.add(name.split(':')[0])
                elif trove.troveIsGroup(name):
                    collections.add(name)

        if changeSetList:
            csTroveSource = trovesource.ChangesetFilesTroveSource(
                                                        self.getDatabase())
            csTroveSource.addChangeSets(changeSetList)
            csSearchSource = searchsource.SearchSource(csTroveSource,
                                                       self.cfg.flavor)
            csTroveSet = troveset.SearchSourceTroveSet(csSearchSource)
        else:
            csTroveSet = None

        # create the initial search path from the installLabelPath
        reposTroveSet = self._createRepositoryTroveSet(csTroveSet = csTroveSet)
        dbTroveSet = self._createDatabaseTroveSet(graph = reposTroveSet.g,
                                                  csTroveSet = csTroveSet)

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
                fetched = ts._action(ActionClass = SysModelFetchAction)
                flattened = fetched._action(ActionClass = SysModelFlattenAction)
                searchPathItems.append(flattened)
            else:
                assert(0)

        searchPathItems.append(reposTroveSet)
        searchPathTroveSet = SysModelSearchPathTroveSet(searchPathItems,
                                                        graph = reposTroveSet.g)
        searchTroveSet = searchPathTroveSet

        finalTroveSet = SysModelInitialTroveTupleSet(graph = searchTroveSet.g)
        for op in sysModel.systemItems:
            searchSpecs = []
            localSpecs = []
            for troveSpec in op:
                if (troveSpec.version is not None and
                                    troveSpec.version[0] == '/'):
                    try:
                        verObj = versions.VersionFromString(troveSpec.version)
                        if verObj.isInLocalNamespace():
                            localSpecs.append(troveSpec)
                            break

                    except (errors.VersionStringError, errors.ParseError):
                        pass

                searchSpecs.append(troveSpec)

            if searchSpecs:
                searchMatches = searchTroveSet.find(*searchSpecs)
            else:
                searchMatches = None

            if localSpecs:
                localMatches = dbTroveSet.find(*localSpecs)
            else:
                localMatches = None

            if searchMatches and localMatches:
                matches = searchMatches.union(localMatches)
            elif searchMatches:
                matches = searchMatches
            else:
                matches = localMatches

            growSearchPath = True
            if isinstance(op, sysModel.InstallTroveOperation):
                finalTroveSet = finalTroveSet.union(matches)
            elif isinstance(op, sysModel.EraseTroveOperation):
                growSearchPath = False
                finalTroveSet = finalTroveSet.remove(matches)
            elif isinstance(op, sysModel.ReplaceTroveOperation):
                finalTroveSet = finalTroveSet.replace(matches)
            elif isinstance(op, sysModel.UpdateTroveOperation):
                finalTroveSet = finalTroveSet.update(matches)
            else:
                assert(0)

            if growSearchPath:
                growSearchPath = False
                for troveSpec in op:
                    if troveSpec.name in collections:
                        growSearchPath = True

                if growSearchPath:
                    flatten = matches._action(ActionClass =
                                                SysModelFlattenAction)
                    searchTroveSet = SysModelSearchPathTroveSet(
                            [ flatten, searchTroveSet ],
                            graph = searchTroveSet.g)

        finalTroveSet.searchPath = searchPathTroveSet

        return finalTroveSet

    def _createRepositoryTroveSet(self, csTroveSet = None):
        if csTroveSet is None:
            path = []
        else:
            path = [ csTroveSet ]

        g = troveset.OperationGraph()
        repos = troveset.SearchSourceTroveSet(
                searchsource.NetworkSearchSource(self.getRepos(),
                                                 [],
                                                 self.cfg.flavor))
        path.append(repos)

        return SysModelSearchPathTroveSet(path, graph = g)

    def _createDatabaseTroveSet(self, graph = None, csTroveSet = None):
        if csTroveSet is None:
            path = []
        else:
            path = [ csTroveSet ]

        db = self.getDatabase()
        dbSearchSource = searchsource.SearchSource(db, self.cfg.flavor)
        dbTroveSet = troveset.SearchSourceTroveSet(dbSearchSource)
        path.append(dbTroveSet)

        return SysModelSearchPathTroveSet(path, graph = graph)

    def _processSysmodelJobList(self, origJobList, updJob, troveCache):
        # this is just like _processJobList, but it's forked to use the
        # sysmodel trove cache instead of a changeset with all of the troves
        # in it
        missingTroves = list()
        removedTroves = list()
        rollbackFence = False

        # this only accesses old troves in the database
        self._addJobPreEraseScripts(origJobList, updJob)

        # removals are uninteresting from now on here
        jobList = [ x for x in origJobList if x[2][0] is not None ]

        # we get the sizes here only because the size of redirects and
        # removed troves isn't set (so it shows up as None); it would
        # be nice if we could explicitly get the trove types from the
        # repository, but we can't right now
        newTroves = [ (x[0], x[2][0], x[2][1]) for x in jobList ]
        newTroveSizes = troveCache.getTroveInfo(
                                trove._TROVEINFO_TAG_SIZE, newTroves)
        missingSize = [ troveTup for (troveTup, size) in
                            itertools.izip(newTroves, newTroveSizes)
                            if size is None ]

        scripts = troveCache.getTroveInfo(trove._TROVEINFO_TAG_SCRIPTS,
                                          newTroves)
        compatibilityClasses = troveCache.getTroveInfo(
                                          trove._TROVEINFO_TAG_COMPAT_CLASS,
                                          newTroves)
        neededTroves = [ troveTup for (troveTup, script, compatClass)
                         in itertools.izip(newTroves, scripts,
                                           compatibilityClasses)
                         if script is not None or compatClass is not None ]

        if hasattr(troveCache, 'cacheTroves'):
            troveCache.cacheTroves(set(missingSize + neededTroves))

        for job, newTroveTup, scripts, compatClass in itertools.izip(
                        jobList, newTroves, scripts, compatibilityClasses):
            if newTroveTup in missingSize:
                trv = troveCache.getTroves([newTroveTup])[0]
                if trv.type() == trove.TROVE_TYPE_REMOVED:
                    if trv.troveInfo.flags.isMissing():
                        missingTroves.append(job)
                    else:
                        removedTroves.append(job)
                else:
                    assert 0, "Trove has no size"

            oldCompatClass = None

            if scripts:
                preScript = None
                if job[1][0] is not None:
                    action = "preupdate"
                    # check for preupdate scripts
                    oldCompatClass = self.db.getTroveCompatibilityClass(
                                                job[0], job[1][0], job[1][1])
                    preScript = scripts.preUpdate.script()
                    if preScript:
                        troveObj = troveCache.getTroves([ newTroveTup ],
                                                        withFiles = False)[0]
                else:
                    action = "preinstall"
                    oldCompatClass = None
                    preSript = scripts.preInstall.script()
                    if preScript:
                        troveObj = troveCache.getTroves([ newTroveTup ],
                                                        withFiles = False)[0]

                if compatClass:
                    compatClass = compatClass()

                if preScript:
                    updJob.addJobPreScript(job, preScript, oldCompatClass,
                                           compatClass,
                                           action = action, troveObj = troveObj)

                postRollbackScript = scripts.postRollback.script()
                if postRollbackScript and job[1][0] is not None:
                    # Add the post-rollback script that will be saved on the
                    # rollback stack
                    # CNY-2844: do not run rollbacks for installs
                    updJob.addJobPostRollbackScript(job, postRollbackScript,
                                                    compatClass, oldCompatClass)

            if compatClass:
                trv = troveCache.getTroves([ newTroveTup ])[0]
                # this is awful
                troveCs = trv.diff(None, absolute = True)[0]
                rollbackFence = rollbackFence or \
                    troveCs.isRollbackFence(update = (job[1][0] is not None),
                                    oldCompatibilityClass = oldCompatClass)

        updJob.setInvalidateRollbacksFlag(rollbackFence)
        return missingTroves, removedTroves

    def _closePackages(self, cache, trv, newTroves = None):
        packagesAdded = set()
        if newTroves is None:
            newTroves = list(trv.iterTroveList(strongRefs = True))
        for n, v, f in newTroves:
            if trove.troveIsComponent(n):
                packageN = n.split(':')[0]
                if not trv.hasTrove(packageN, v, f):
                    log.info("adding package %s for component %s",
                             packageN, (n, v, f))
                    trv.addTrove(packageN, v, f)
                    packagesAdded.add( (packageN, v, f) )

        cache.cacheComponentMap(packagesAdded)

        return packagesAdded

    def _updateFromTroveSetGraph(self, uJob, troveSet, troveCache,
                            split = True, fromChangesets = [],
                            criticalUpdateInfo=None, applyCriticalOnly = False,
                            restartInfo = None):
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
            L{update.ClientUpdate.applyUpdateJob} for an explanation of
            the behavior of exceptions within callbacks.
        """

        def _updateJob(origJob, addedTroves):
            newJob = []
            for oneJob in origJob:
                if oneJob[1][0] is None:
                    newJob.append(oneJob)
                    continue

                oldTroveTup = (oneJob[0], oneJob[1][0], oneJob[1][1])
                if oldTroveTup not in added:
                    newJob.append(oneJob)
                    continue

                if oneJob[2][0] is not None:
                    return None

                added.remove(oldTroveTup)

            for troveTup in added:
                newJob.append( (troveTup[0], (None, None),
                                troveTup[1:], True) )

            return newJob

        if criticalUpdateInfo is None:
            criticalUpdateInfo = update.CriticalUpdateInfo()

        searchPath = troveSet.searchPath

        # we need to explicitly fetch this before we can walk it
        preFetch = troveSet._action(ActionClass = SysModelFinalFetchAction)
        # handle exclude troves
        final = preFetch._action(excludeTroves = self.cfg.excludeTroves,
                                    ActionClass = SysModelExcludeTrovesAction)
        depSearch = SysModelSearchPathTroveSet([ preFetch, searchPath ],
                                               graph = preFetch.g)
        depSearch.g.realize(SysModelActionData(troveCache,
                                              self.cfg.flavor[0],
                                              self.repos, self.cfg))

        existsTrv = trove.Trove("@update", versions.NewVersion(),
                                deps.Flavor(), None)
        targetTrv = trove.Trove("@update", versions.NewVersion(),
                                deps.Flavor(), None)

        pins = set()
        for tup, pinned in self.db.iterAllTroves(withPins = True):
            existsTrv.addTrove(*tup)
            if pinned:
                pins.add(tup)
                targetTrv.addTrove(*tup)

        for tup, inInstall, explicit in final._walk(troveCache, recurse = True):
            if inInstall and tup[0:3] not in pins:
                targetTrv.addTrove(*tup[0:3])

        self._closePackages(troveCache, targetTrv)
        job = targetTrv.diff(existsTrv, absolute = False)[2]

        # don't resolve against local troves (we can do this because either
        # they're installed and show up in the unresolveable list or they
        # aren't installed and we don't know about them) or troves which are
        # in the install set (since they're already in the install set,
        # adding them to the install set won't help)
        depResolveSource = depSearch._getResolveSource(
                        filterFn = lambda n, v, f :
                            (v.isOnLocalHost() or
                             targetTrv.isStrongReference(n,v,f)))
        resolveMethod = depResolveSource.getResolveMethod()

        uJob.setSearchSource(self.getSearchSource())
        # this is awful
        jobTroveSource = uJob.getTroveSource()
        jobTroveSource.addChangeSets(fromChangesets,
                                     includesFileContents = True)

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
                added = set()
                for (needingTup, neededDeps, neededTupList) in \
                                                result.unresolveableList:
                    for neededTup in neededTupList:
                        if (neededTup not in added and
                                searchPath.hasOptionalTrove(neededTup)):
                            log.info("keeping installed trove for deps %s",
                                     neededTup)
                            added.add(neededTup)

                if not added:
                    unsatisfied = result.unsatisfiedList
                    unsatisfied += [ x[0:2] for x in result.unresolveableList ]

                    while (resolveMethod.prepareForResolution(unsatisfied) and
                           not added):
                        sugg = resolveMethod.resolveDependencies()
                        newJob = resolveMethod.filterSuggestions(
                                            result.unsatisfiedList, sugg, suggMap)
                        newTroves = []

                        for (name, oldInfo, newInfo, isAbsolute) in newJob:
                            assert(isAbsolute)
                            log.info("adding for dependency %s", name)
                            added.add((name, newInfo[0], newInfo[1]))

                if not added:
                    break

                for troveTup in added:
                    targetTrv.addTrove(*troveTup)

                added.update(self._closePackages(troveCache, targetTrv,
                                                 newTroves = added))

                # try to avoid a diff here
                job = _updateJob(job, added)
                if job is None:
                    job = targetTrv.diff(existsTrv, absolute = False)[2]

                log.info("resolving dependencies")
                criticalJobs = criticalUpdateInfo.findCriticalJobs(job)
                finalJobs = criticalUpdateInfo.findFinalJobs(job)
                criticalOnly = criticalUpdateInfo.isCriticalOnlyUpdate()

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
                criticalUpdates = [ splitJob[x] for x in
                                        result.getCriticalUpdates() ]

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

        # this prevents us from using the changesetList as a searchSource
        log.info("processing job list")
        self._processSysmodelJobList(job, uJob, troveCache)
        log.info("combining jobs")
        self._combineJobs(uJob, splitJob, criticalUpdates)
        log.info("combining jobs")
        uJob.setTransactionCounter(self.db.getTransactionCounter())

        # remove things from the suggMap which are in the already installed
        # set
        for neededSet in suggMap.itervalues():
            for troveTup in set(neededSet):
                if existsTrv.hasTrove(*troveTup):
                    neededSet.remove(troveTup)

        for needingTroveTup, neededSet in suggMap.items():
            if not neededSet:
                del suggMap[needingTroveTup]

        return suggMap
