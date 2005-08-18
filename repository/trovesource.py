import findtrove

class AbstractTroveSource:
    """ Provides the interface necessary for performing
        findTrove operations on arbitrary sets of troves.
        As long as the subclass provides the following methods,
        findTrove will be able to search it.  You can set the 
        type of searching findTrove will default to here as 
        well.
    """

    def getTroveLeavesByLabel(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroveVersionsByLabel(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroveLeavesByBranch(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroveVersionsByBranch(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroveVersionFlavors(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroves(self, troveList, withFiles = True):
        raise NotImplementedError

    def getTrove(self, name, version, flavor, withFiles = True):
        return self.getTroves((name, version, flavor), withFiles)[0]

    def getTroveVersionList(self, name, withFlavors=False):
        raise NotImplementedError

    def findTroves(self, labelPath, troves, defaultFlavor, acrossSources=True, 
                   acrossFlavors=True, affinityDatabase=None, 
                   allowMissing=False):
        troveFinder = findtrove.TroveFinder(self, labelPath, 
                                            defaultFlavor, acrossSources,
                                            acrossFlavors, affinityDatabase,
                                            getLeaves=False, bestFlavor=False,
                                            allowNoLabel=True)
        return troveFinder.findTroves(troves, allowMissing)

    def findTrove(self, labelPath, (name, versionStr, flavor), 
                  defaultFlavor=None, acrossSources = True, 
                  acrossFlavors = True, affinityDatabase = None):
        res = self.findTroves(labelPath, ((name, versionStr, flavor),),
                              defaultFlavor, acrossSources, acrossFlavors,
                              affinityDatabase)
        return res[(name, versionStr, flavor)]

# constants mostly stolen from netrepos/netserver
_GET_TROVE_ALL_VERSIONS = 1
_GET_TROVE_VERY_LATEST  = 2         # latest of any flavor

_GET_TROVE_NO_FLAVOR          = 1     # no flavor info is returned
_GET_TROVE_ALL_FLAVORS        = 2     # all flavors (no scoring)
_GET_TROVE_BEST_FLAVOR        = 3     # the best flavor for flavorFilter
_GET_TROVE_ALLOWED_FLAVOR     = 4     # all flavors which are legal

_CHECK_TROVE_REG_FLAVOR    = 1          # use exact flavor and ensure trove 
                                        # flavor is satisfied by query flavor
_CHECK_TROVE_STRONG_FLAVOR = 2          # use strong flavors and reverse sense

_GTL_VERSION_TYPE_NONE    = 0
_GTL_VERSION_TYPE_LABEL   = 1
_GTL_VERSION_TYPE_VERSION = 2
_GTL_VERSION_TYPE_BRANCH  = 3

class SimpleTroveSource(AbstractTroveSource):
    """ A simple implementation of most of the methods needed 
        for findTrove - all of the methods are implemplemented
        in terms of trovesByName, which is left for subclasses to 
        implement.
    """
    defaultFlavorCheck = _CHECK_TROVE_STRONG_FLAVOR
    
    def __init__(self, defaultFlavorCheck=None):
        if defaultFlavorCheck is not None:
            self.defaultFlavorCheck = defaultFlavorCheck

    def trovesByName(self, name):
        raise NotImplementedError

    def getTroveVersionList(self, name, withFlavors=False):
        if withFlavors:
            return [ x[1:] for x in self.trovesByName(name) ]
        else:
            return [ x[1] for x in self.trovesByName(name) ]

    def _toQueryDict(self, troveList):
        d = {}
        for (n,v,f) in troveList:
            d.setdefault(n, {}).setdefault(v, []).append(f)
        return d

    def _getTrovesByType(self, troveSpecs, 
                      versionType=_GTL_VERSION_TYPE_NONE,
                      latestFilter=_GET_TROVE_ALL_VERSIONS, 
                      bestFlavor=True,
                      flavorCheck=None):
        """ Implements the various getTrove methods by grabbing
            information from trovesByName.  Note it takes an 
            extra parameter over the netrepos/netserver version - 
            flavorCheck - which, if set to _GET_TROVE_STRONG_FLAVOR,
            does a strong comparison against the listed troves - 
            the specified flavor _must_ exist in potentially matching 
            troves, and sense reversal is not allowed - e.g. 
            ~!foo is not an acceptable match for a flavor request of 
            ~foo.  The mode is useful when the troveSpec is specified
            by the user and is matching against a limited set of troves.
        """
        if bestFlavor:
            flavorFilter = _GET_TROVE_BEST_FLAVOR
        else:
            flavorFilter = _GET_TROVE_ALLOWED_FLAVOR
        if flavorCheck is None:
            flavorCheck = self.defaultFlavorCheck

        allTroves = {}
        for name, versionQuery in troveSpecs.iteritems():
            troves = self._toQueryDict(self.trovesByName(name))
            if not troves:
                continue
            if not versionQuery:
                allTroves[name] = troves[name]
                continue
            versionList = []
            for version in troves[name].iterkeys():
                if versionType == _GTL_VERSION_TYPE_LABEL:
                    theLabel = version.branch().label()
                    if theLabel not in versionQuery:
                        continue
                    versionList.append((version, theLabel))
                elif versionType == _GTL_VERSION_TYPE_BRANCH:
                    theBranch = version.branch()
                    if theBranch not in versionQuery:
                        continue
                    versionList.append((version, theBranch))
                elif versionType == _GTL_VERSION_TYPE_VERSION:
                    if version not in versionQuery:
                        continue
                    versionList.appned((version, version))
                else:
                    assert(False)
            if latestFilter == _GET_TROVE_VERY_LATEST:
                versionList = sort(versionList)[-1:]

            for version, queryKey in versionList:
                flavorList = troves[name][version]
                flavorQuery = versionQuery[queryKey]

                if flavorFilter == _GET_TROVE_ALL_FLAVORS:
                    troveFlavors = flavorList
                elif flavorQuery is None:
                    troveFlavors = flavorList
                else:
                    troveFlavors = set() 
                    if flavorCheck == _CHECK_TROVE_STRONG_FLAVOR:
                        strongFlavors = [x.toStrongFlavor() for x in flavorList]
                        flavorList = zip(strongFlavors, flavorList)

                    for qFlavor in flavorQuery:
                        if flavorCheck == _CHECK_TROVE_STRONG_FLAVOR:
                            scores = ((x[0].score(qFlavor), x[1]) \
                                                        for x in flavorList)
                        else:
                            scores = ((qFlavor.score(x), x) for x in flavorList)

                        scores = [ x for x in scores if x[0] is not False]
                        if scores:
                            if flavorFilter == _GET_TROVE_BEST_FLAVOR:
                                troveFlavors.add(max(scores)[1])
                            elif flavorFilter == _GET_TROVE_ALLOWED_FLAVOR:
                                troveFlavors.update([x[1] for x in scores])
                            else:
                                assert(false)
                if troveFlavors:
                    allTroves.setdefault(name, {})[version] = list(troveFlavors)
        return allTroves

    def getTroveLeavesByLabel(self, troveSpecs, bestFlavor=True):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_LABEL, 
                                     _GET_TROVE_VERY_LATEST, bestFlavor)

    def getTroveVersionsByLabel(self, troveSpecs, bestFlavor=True):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_LABEL, 
                                     _GET_TROVE_ALL_VERSIONS, bestFlavor)

    def getTroveLeavesByBranch(self, troveSpecs, bestFlavor=True):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_BRANCH,
                                     _GET_TROVE_VERY_LATEST, bestFlavor)

    def getTroveVersionsByBranch(self, troveSpecs, bestFlavor):
        return self._getTrovesByType(_GTL_VERSION_TYPE_BRANCH, 
                                     _GET_TROVE_ALL_VERSIONS, bestFlavor)

    def getTroveVersionFlavors(self, troveSpecs, bestFlavor=True):
        return self._getTrovesByType(troveSpecs, 
                                     _GTL_VERSION_TYPE_VERSION, 
                                     _GET_TROVE_ALL_VERSIONS, 
                                     bestFlavor)

class TroveListTroveSource(SimpleTroveSource):

    def __init__(self, source, troveTups, withDeps=False):
        troveTups = [ x for x in troveTups ]
        self.deps = {}
        self.labels = set()
        self._trovesByName = {}
        self.source = source
        self.sourceTups = troveTups[:]

        for (n,v,f) in troveTups:
            self._trovesByName.setdefault(n, []).append((n,v,f))

        foundTups = set()

        while troveTups:
            self._trovesByName.setdefault(n, []).append((n,v,f))
            newTroves = source.getTroves(troveTups)
            foundTups.update(newTroves)
            troveTups = []
            for newTrove in newTroves:
                for tup in newTrove.iterTroveList():
                    self._trovesByName.setdefault(tup[0], []).append(tup)
                    if tup not in foundTups:
                        troveTups.append(tup)
                    self.labels.add(tup[1].branch().label())

    def getSourceTroves(self):
        return self.getTroves(self.sourceTups)

    def getTroves(self, troveTups, withFiles=False):
        return self.source.getTroves(troveTups, withFiles)

    def getLabelList(self):
        return self.labels

    def trovesByName(self, name):
        return self._trovesByName.get(name, [])



class GroupRecipeSource(SimpleTroveSource):

    def __init__(self, source, groupRecipe):
        self.deps = {}
        self.labels = set()
        self._trovesByName = {}
        self.source = source
        self.sourceTups = groupRecipe.troves

        for (n,v,f) in self.sourceTups:
            self._trovesByName.setdefault(n, []).append((n,v,f))
            self.labels.add(v.branch().label())

    def getSourceTroves(self):
        return self.getTroves(self.sourceTups)

    def getTroves(self, troveTups, withFiles=False):
        return self.source.getTroves(troveTups, withFiles)

    def getLabelList(self):
        return self.labels

    def trovesByName(self, name):
        return self._trovesByName.get(name, []) 
