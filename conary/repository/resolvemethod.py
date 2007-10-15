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

import itertools

from conary.lib import log
from conary.repository import errors as repoerrors
from conary.repository import trovesource
from conary.deps import deps

class DepResolutionMethod(object):
    """Abstract base class for dependency resolution methods.
       These classes wraps around the actual method used to
       find resolutions for dependencies.
    """
    def __init__(self, cfg, db, flavor=None):
        self.cfg = cfg
        self.db = db
        if flavor is None and cfg:
            flavor = self.cfg.flavor
        if isinstance(flavor, deps.Flavor):
            flavor = [flavor]
        self.flavor = flavor
        self.flavorPreferences = []

    def setFlavorPreferences(self, flavorPreferences):
        self.flavorPreferences = flavorPreferences

    def setTroveSource(self, troveSource):
        self.troveSource = troveSource

    def prepareForResolution(self, depList):
        """
            Must be called prior to requesting dep resolution.
            Returns False if there is no point in doing more dep resolution.
        """
        raise NotImplementedError

    def resolveDependencies(self):
        """
            Attempts to resolve the dependencies passed into 
            prepareForResolution.
        """
        raise NotImplementedError

    def filterDependencies(self, depList):
        return depList

    def filterSuggestions(self, depList, sugg, suggMap):
        """
            Given a list of several suggestions for one dependency,
            pick the dep that matches the best.
        """
        troves = set()

        for (troveTup, depSet) in depList:
            choicesBySolution = {}
            seen = set()
            if depSet in sugg:
                suggList = set()
                choicesAndDep = itertools.izip(sugg[depSet],
                                               depSet.iterDeps(sort=True))
                for choiceList, (depClass, dep) in choicesAndDep:
                    troveNames = set(x[0] for x in choiceList)

                    if self.db:
                        affTroveDict = \
                            dict((x, self.db.trovesByName(x))
                                 for x in troveNames)
                    else:
                        affTroveDict = dict.fromkeys(troveNames, {})

                    # iterate over flavorpath -- use suggestions
                    # from first flavor on flavorpath that gets a match
                    for installFlavor in self.flavor:
                        choice = self.selectResolutionTrove(troveTup, dep,
                                                            depClass,
                                                            choiceList,
                                                            installFlavor,
                                                            affTroveDict)
                        if choice:
                            suggList.add(choice)
                            l = suggMap.setdefault(troveTup, set())
                            l.add(choice)

                            if choice not in seen:
                                if choice not in choicesBySolution:
                                    d = deps.DependencySet()
                                    choicesBySolution[choice] = d
                                else:
                                    d = choicesBySolution[choice]
                                d.addDep(depClass, dep)
                            break

                if choicesBySolution:
                    for choice, depSet in sorted(choicesBySolution.iteritems()):
                        seen.add(choice)
                        depSet = str(depSet).split('\n')
                        if len(depSet) > 5:
                            depSet = depSet[0:5] + ['...']
                        depSet = '\n               '.join(depSet)
                        log.debug('Resolved:\n' 
                                  '    %s=%s/%s[%s]\n'
                                  '    Required:  %s\n'
                                  '    Adding: %s=%s/%s[%s]',
                                     troveTup[0], troveTup[1].trailingLabel(), troveTup[1].trailingRevision(),troveTup[2], depSet, choice[0], choice[1].trailingLabel(), choice[1].trailingRevision(), choice[2])

                troves.update([ (x[0], (None, None), x[1:], True)
                                for x in suggList ])


        return troves


    def selectResolutionTrove(self, requiredBy, dep, depClass,
                              troveTups, installFlavor, affFlavorDict):
        """ determine which of the given set of troveTups is the 
            best choice for installing on this system.  Because the
            repository didn't try to determine which flavors are best for 
            our system, we have to filter the troves locally.  
        """
        # we filter the troves in the following ways:
        # 1. prefer troves that match affinity flavor + are on the affinity
        # label. (And don't drop an arch)
        # 2. fall back to troves that match the install flavor.

        # If we don't match an affinity flavor + label, then use flavor
        # preferences and flavor scoring to select the best flavor.
        # We'll have to check 

        # Within these two categories:
        # 1. filter via flavor preferences for each trove (this may result
        # in an older version for some troves)
        # 2. only leave the latest version for each trove
        # 3. pick the best flavor out of the remaining
        affinityMatches = []
        affinityFlavors = []
        otherMatches = []
        otherFlavors = []


        troveNames = set([x[0] for x in troveTups])
        allAffinityTroves = list(itertools.chain(*[affFlavorDict[x]
                                                 for x in troveNames]))
        db = trovesource.SimpleTroveSource(allAffinityTroves)
        repos = trovesource.SimpleTroveSource(troveTups)
        repos.searchWithFlavor()
        repos.setFlavorPreferenceList(self.flavorPreferences)
        if installFlavor is not None and installFlavor.isEmpty():
            installFlavor = None

        # search for resolutions that would update an installed package.
        results = repos.findTroves(None, [(x, None, None)
                                     for x in troveNames], installFlavor,
                                     getLeaves=False,
                                     affinityDatabase=db,
                                     allowMissing=True)
        if results:
            flavoredList = []
            troveTups = list(itertools.chain(*results.itervalues()))
            for troveTup in troveTups:
                if troveTup in allAffinityTroves:
                    continue
                affTups = affFlavorDict[troveTup[0]]
                if affTups:
                    for affTup in affTups:
                        affFlavor = deps.overrideFlavor(installFlavor, affTup[2],
                                        mergeType = deps.DEP_MERGE_TYPE_PREFS)
                        if affFlavor.satisfies(troveTup[2]):
                            flavoredList.append((affFlavor, troveTup))
                else:
                    flavoredList.append((installFlavor, troveTup))
        else:
            # fall back to searching for things that could be installed
            # side-by-side.
            results = repos.findTroves(None, [(x, None, None)
                                     for x in troveNames], installFlavor,
                                     getLeaves=False,
                                     allowMissing=True)
            troveTups = list(itertools.chain(*results.itervalues()))
            flavoredList = [ (installFlavor, x) for x in troveTups ]

        return self._selectMatchingResolutionTrove(requiredBy, dep,
                                                   depClass, flavoredList)

    def _selectMatchingResolutionTrove(self, requiredBy, dep, depClass,
                                       flavoredList):
        # finally, filter by latest then score.
        trovesByNL = {}
        for installFlavor, (n,v,f) in flavoredList:
            l = v.trailingLabel()
            myTimeStamp = v.timeStamps()[-1]
            if installFlavor is None:
                myScore = 0
            else:
                # FIXME: we should cache this scoring from before.
                myScore = installFlavor.score(f)

            if (n,l) in trovesByNL:
                curScore, curTimeStamp, curTup = trovesByNL[n,l]
                if curTimeStamp > myTimeStamp:
                    continue
                if curTimeStamp == myTimeStamp:
                    if myScore < curScore:
                        continue

            trovesByNL[n,l] = (myScore, myTimeStamp, (n,v,f))

        scoredList = sorted(trovesByNL.itervalues())
        if not scoredList:
            return None
        else:
            # highest score, then latest timestamp, then name.
            return scoredList[-1][-1]

    def filterResolutionsPostUpdate(self, db, jobSet, troveSource):
        # Now that we know how conary would line up these dependencies
        # to installed troves.
        # We can't resolve deps in a way that would cause conary to
        # switch the branch of a trove.
        badJobs = [ x for x in jobSet
                            if (x[1][0] and
                                x[1][0].trailingLabel() != x[2][0].trailingLabel()) ]
        badJobs += [ x for x in jobSet \
                     if (x[1][0] and \
                         deps.getInstructionSetFlavor(x[1][1]) \
                         != deps.getInstructionSetFlavor(x[2][1])) ]
        if badJobs:
            jobSet.difference_update(badJobs)
            oldTroves = db.getTroves(
                  [ (x[0], x[1][0], x[1][1]) for x in badJobs ],
                  withFiles = False)
            newTroves = troveSource.getTroves(
                  [ (x[0], x[2][0], x[2][1]) for x in badJobs ],
                  withFiles = False)
            for job, oldTrv, newTrv in itertools.izip(badJobs,
                                                      oldTroves,
                                                      newTroves):
                if oldTrv.compatibleWith(newTrv):
                    jobSet.add((job[0], (None, None), job[2], False))
        return jobSet

    def searchLeavesOnly(self):
        pass

    def searchLeavesFirst(self):
        pass

    def searchAllVersions(self):
        pass

class BasicResolutionMethod(DepResolutionMethod):
    def __init__(self, cfg, db, flavor=None):
        DepResolutionMethod.__init__(self, cfg, db, flavor)
        self.depList = None

    def prepareForResolution(self, depList):
        newDepList = [x[1] for x in depList]
        if not newDepList or newDepList == self.depList:
            return False

        self.depList = newDepList
        return True

    def resolveDependencies(self):
        return self.troveSource.resolveDependencies(None, self.depList)


RESOLVE_ALL = 0
RESOLVE_LEAVES_FIRST = 1
RESOLVE_LEAVES_ONLY = 2


class DepResolutionByLabelPath(DepResolutionMethod):
    def __init__(self, cfg, db, installLabelPath, flavor=None, 
                 searchMethod=RESOLVE_ALL):
        DepResolutionMethod.__init__(self, cfg, db, flavor)
        self.index = 0
        self.depList = None
        self.fullDepList = None
        self.searchMethod = searchMethod
        self.setLabelPath(installLabelPath)

    def searchLeavesOnly(self):
        self.searchMethod = RESOLVE_LEAVES_ONLY
        self._updateLabelPath()

    def searchLeavesFirst(self):
        self.searchMethod = RESOLVE_LEAVES_FIRST
        self._updateLabelPath()

    def searchAllVersions(self):
        self.searchMethod = RESOLVE_ALL
        self._updateLabelPath()

    def setLabelPath(self, labelPath):
        self.installLabelPath = labelPath
        self._updateLabelPath()

    def _updateLabelPath(self):
        labelPath = self.installLabelPath
        if not labelPath:
            self._labelPathWithLeaves = []
            return

        l = []
        if self.searchMethod in (RESOLVE_LEAVES_ONLY, RESOLVE_LEAVES_FIRST):
            l = [ (x, True) for x in labelPath ]
        if self.searchMethod in (RESOLVE_ALL, RESOLVE_LEAVES_FIRST):
            l += [ (x, False) for x in labelPath ]
        self._labelPathWithLeaves = l

    def prepareForResolution(self, depList):
        if not depList:
            return False

        self.fullDepList = depList
        newDepList = [ x[1] for x in depList ]
        if newDepList != self.depList:
            self.index = 0
            self.depList = newDepList
        else:
            self.index += 1

        if self.index < len(self._labelPathWithLeaves):
            return True
        else:
            return False

    def resolveDependencies(self):
        try:
            label, leavesOnly = self._labelPathWithLeaves[self.index]
            if hasattr(self.troveSource, 'resolveDependenciesWithFilter'):
                return self.troveSource.resolveDependenciesWithFilter(label,
                                self.fullDepList, self.filterSuggestions,
                                leavesOnly=leavesOnly)
            else:
                return self.troveSource.resolveDependencies(label,
                                self.depList, leavesOnly=leavesOnly)
        except repoerrors.OpenError, err:
            log.warning('Could not access %s for dependency resolution: %s' % (
                                self._labelPathWithLeaves[self.index][0], err))
            # return an empty result.
            results = {}
            for depSet in self.depList:
                results[depSet] = [ [] for x in depSet.iterDeps() ]
            return results

class DepResolutionByTroveList(DepResolutionMethod):
    def __init__(self, cfg, db, troveList, flavor=None):
        DepResolutionMethod.__init__(self, cfg, db, flavor)
        assert(troveList)
        self.troveList = troveList
        self.depList = None
        self.db = db
        self.cfg = cfg

    def prepareForResolution(self, depList):
        newDepList = [x[1] for x in depList]
        if not newDepList or newDepList == self.depList:
            return False

        self.depList = newDepList
        return True

    def resolveDependencies(self):
        return self.troveSource.resolveDependenciesByGroups(self.troveList,
                                                            self.depList)


class ResolutionStack(DepResolutionMethod):
    def __init__(self, *sources):
        self.sources = []
        for source in sources:
            self.addSource(source)
        self.reset()

    def filterSuggestions(self, depList, sugg, suggMap):
        return self.sources[self.sourceIndex].filterSuggestions(depList,
                                                            sugg, suggMap)

    def __iter__(self):
        return iter(self.sources)

    def addSource(self, source):
        if isinstance(source, ResolutionStack):
            for subSource in source.iterSources():
                self.addSource(subSource)
            return

        if source not in self:
            self.sources.append(source)

    def reset(self):
        self.depList = None
        self.sourceIndex = 0

    def prepareForResolution(self, depList):
        if not depList:
            return False

        newDepList = [ x[1] for x in depList ]
        if newDepList != self.depList:
            self.depList = newDepList
            self.sourceIndex = 0

        while self.sourceIndex < len(self.sources):
            source = self.sources[self.sourceIndex]
            if source.prepareForResolution(depList):
                return True
            self.sourceIndex += 1

        return False

    def resolveDependencies(self):
        return self.sources[self.sourceIndex].resolveDependencies()

    def searchLeavesOnly(self):
        for source in self.sources:
            source.searchLeavesOnly()

    def searchLeavesFirst(self):
        for source in self.sources:
            source.searchLeavesFirst()

    def searchAllVersions(self):
        for source in self.sources:
            source.searchAllVersions()


def stack(sources):
    return ResolutionStack(*sources)
