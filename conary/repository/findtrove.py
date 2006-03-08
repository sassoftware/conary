#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import itertools

from conary.deps import deps
from conary import versions, errors

######################################
# Query Types
# findTroves divides queries up into a set of sub queries, depending on 
# how the trove is to be found
# Below are the five different types of queries that can be created 
# from findTroves

QUERY_BY_VERSION           = 0
QUERY_BY_BRANCH            = 1
QUERY_BY_LABEL_PATH        = 2
QUERY_REVISION_BY_LABEL    = 3
QUERY_REVISION_BY_BRANCH   = 4
QUERY_SENTINEL             = 5

queryTypes = range(QUERY_SENTINEL)

#################################
# VersionStr Types 
# Different version string types, plus affinity troves if available, 
# result in different queries

VERSION_STR_NONE                 = 0
VERSION_STR_FULL_VERSION         = 1 # branch + trailing revision
VERSION_STR_BRANCH               = 2 # branch
VERSION_STR_LABEL                = 3 # host@namespace:tag
VERSION_STR_BRANCHNAME           = 4 # @namespace:tag
VERSION_STR_TAG                  = 5 # :tag
VERSION_STR_REVISION             = 6 # troveversion-sourcecount[-buildcount]
VERSION_STR_TROVE_VER            = 7 # troveversion (no source or build count)
VERSION_STR_HOST                 = 8 # host@

class Query:
    def __init__(self, defaultFlavorPath, labelPath, 
                 acrossLabels, acrossFlavors, getLeaves, bestFlavor):
        self.map = {}
        self.defaultFlavorPath = defaultFlavorPath
        if not self.defaultFlavorPath:
            self.query = [{}]
        else:
            self.query = [{} for x in defaultFlavorPath ]
        self.labelPath = labelPath
        self.acrossLabels = acrossLabels
        self.acrossFlavors = acrossFlavors
        self.getLeaves = getLeaves
        self.bestFlavor = bestFlavor

    def reset(self):
        for dct in self.query:
            dct.clear()
        self.map.clear()

    def hasName(self, name):
        return name in self.map

    def hasTroves(self):
        return bool(self.map)

    def findAll(self, troveSource, missing, finalMap):
        raise NotImplementedError

    def filterTroveMatches(self, name, versionFlavorDict):
        return versionFlavorDict

    def overrideFlavors(self, flavor):
        """ override the flavors in the defaultFlavorPath with flavor,
            replacing instruction set entirely if given.
        """
        if not self.defaultFlavorPath:
            return [flavor]
        flavors = []
        for defaultFlavor in self.defaultFlavorPath:
            flavors.append(deps.overrideFlavor(defaultFlavor, flavor, 
                                        mergeType = deps.DEP_MERGE_TYPE_PREFS)) 
        return flavors

    def addQuery(self, troveTup, *params):
        raise NotImplementedError

    def addMissing(self, missing, name):
        troveTup = self.map[name][0]
        missing[troveTup] = self.missingMsg(name)
            
    def missingMsg(self, name):
        raise NotImplementedError
    
class QueryByVersion(Query):

    def __init__(self, *args, **kw):
        Query.__init__(self, *args, **kw)
        self.queryNoFlavor = {}

    def reset(self):
        Query.reset(self)
        self.queryNoFlavor = {}

    def addQuery(self, troveTup, version, flavorList):
        name = troveTup[0]
        self.map[name] = [troveTup]
        if not flavorList:
            self.queryNoFlavor[name] = { version : None }
        else:
            for i, flavor in enumerate(flavorList):
                self.query[i][name] = {version : [flavor] } 

    def addQueryWithAffinity(self, troveTup, version, affinityTroves):
        flavors = [x[2] for x in affinityTroves]
        f = flavors[0]
        for otherFlavor in flavors:
            if otherFlavor != f:
                # bail if there are two affinity flavors
                f = None
                break
        if f is None:
            flavorList = self.defaultFlavorPath
        else:
            flavorList = self.overrideFlavors(f)  

        self.addQuery(troveTup, version, flavorList)

    def findAll(self, troveSource, missing, finalMap):
        self._findAllNoFlavor(troveSource, missing, finalMap)
        self._findAllFlavor(troveSource, missing, finalMap)

    def _findAllFlavor(self, troveSource, missing, finalMap):
        namesToFind = set(self.query[0])
        foundNames = set()
        for query in self.query:
            # delete any found names - don't search for them again
            for name in foundNames:
                query.pop(name, None)
            res = troveSource.getTroveVersionFlavors(query, 
                                                     bestFlavor=self.bestFlavor)
            for name in res:
                matches = self.filterTroveMatches(name, res[name])
                if not matches: 
                    continue
                foundNames.add(name)
                namesToFind.remove(name)
                pkgList = []
                for version, flavorList in matches.iteritems():
                    pkgList.extend((name, version, f) for f in flavorList)
                finalMap[self.map[name][0]] = pkgList

        for name in namesToFind:
            self.addMissing(missing, name)

    def _findAllNoFlavor(self, troveSource, missing, finalMap):
        res = troveSource.getTroveVersionFlavors(self.queryNoFlavor, 
                                                 bestFlavor=False)
        for name in self.queryNoFlavor:
            if name not in res or not res[name]:
                self.addMissing(missing, name)
                continue
            matches = self.filterTroveMatches(name, res[name])
            if not matches: 
                continue
            pkgList = []
            for version, flavorList in matches.iteritems():
                pkgList.extend((name, version, f) for f in flavorList)
            finalMap[self.map[name][0]] = pkgList

    def missingMsg(self, name):
        versionStr = self.map[name][0][1]
        return "version %s of %s was not found" % (versionStr, name)

class QueryByLabelPath(Query):

    def __init__(self, *args, **kw):
        Query.__init__(self, *args, **kw)
        self.query = {}
    
    def reset(self):
        self.query = {}
        self.map = {}

    def addQuery(self, troveTup, labelPath, flavorList):
        name = troveTup[0]
        self.map[name] = [troveTup, labelPath]

        if self.acrossLabels:
            if not flavorList:
                self.query[name] = [ dict.fromkeys(labelPath, None)]
            elif self.acrossFlavors:
                # create one big query: {name : [{label  : [flavor1, flavor2],
                #                                 label2 : [flavor1, flavor2]}
 
                d = {}
                for label in labelPath:
                    d[label] = flavorList[:]
                self.query[name] = [d]
            else:
                # create a set of queries like {name : [{label  : [flavor1],
                #                                        label2 : [flavor1]},
                #                                       {label : [flavor2],
                #                                        label2 : [flavor2]}
                # -- if flavor1 is found on label1 or label2, stop searching
                # on that label for this name.  Otherwise, continue searching 
                # using flavor1
                self.query[name] = []
                for flavor in flavorList:
                    d = {}
                    self.query[name].append(d)
                    for label in labelPath:
                        d[label] = [flavor]
        else:
            self.query[name] = []
            if not flavorList:
                for label in labelPath:
                    self.query[name].append({label : None})
            elif self.acrossFlavors:
                # create a set of queries:
                #  query[name] = [ {label  : [flavor1, flavor2],
                #                   label2 : [flavor1, flavor2]},
                for label in labelPath:
                    self.query[name].append({label : flavorList[:]})
            else:
                # create a set of queries:
                # query[name] = [ {label: [flavor1]}, {label: [flavor2]}, 
                #                 {label2 : [flavor1}, {label2: [flavor2]} --
                # search label 1 for all flavors on the flavorPath before
                # searching label 2
                for label in labelPath:
                    for flavor in flavorList:
                        self.query[name].append({label : [flavor]})

    def addQueryWithAffinity(self, troveTup, labelPath, affinityTroves):
        name = troveTup[0]
        self.map[name] = [troveTup, labelPath]

        for label in labelPath:
            flavors = []
            for (afName, afVersion, afFlavor) in affinityTroves:
                if afVersion.branch().label() == label:
                    flavors.append(afFlavor)
            if not flavors:
                f = None
            else:
                f = flavors[0]
                for otherFlavor in flavors:
                    if otherFlavor != f:
                        f = None
                        break
            if f is None:
                flavorList = self.defaultFlavorPath
            else:
                flavorList = self.overrideFlavors(f)  
            self.addQuery(troveTup, labelPath, flavorList) 

    def callQueryFunction(self, troveSource, query):
        if self.getLeaves:
            return troveSource.getTroveLeavesByLabel(query, 
                                                     bestFlavor=self.bestFlavor)
        else:
            return troveSource.getTroveVersionsByLabel(query, 
                                                   bestFlavor=self.bestFlavor)

        
    def findAll(self, troveSource, missing, finalMap):

        index = 0
        namesToFind = set(self.query)
        foundNames = set()
        if self.acrossLabels:
            foundNameLabels = set()
        # self.query[name] is an ordered list of queries to use 
        # for that name.  If name is found using one query, then
        # stop searching for that name (unless acrossLabels 
        # is used, in which case a name/label pair must be found)
        while self.query:
            labelQuery = {}

            # compile a query from all of the query[name] components  
            for name in self.query.keys():
                try:
                    req = self.query[name][index]
                except IndexError:
                    if name not in foundNames:
                        self.addMissing(missing, name)
                        namesToFind.remove(name)
                    del(self.query[name])
                    continue

                if self.acrossLabels:
                    # if we're searching across repositories, 
                    # we are trying to find one match per label
                    # if we've already found a match for a label, 
                    # remove it
                    for label in req.keys():
                        if (name, label) in foundNameLabels:
                            req.pop(label)
                elif name in foundNames:
                    continue
                labelQuery[name] = req

            if not labelQuery:
                break

            # call the query
            res = self.callQueryFunction(troveSource, labelQuery)

            for name in res:
                if not res[name]:
                    continue
                # filter the query -- this is overridden in 
                # QueryByLabelRevision
                matches = self.filterTroveMatches(name, res[name])
                if not matches: 
                    continue

                # found name, don't search for it any more
                foundNames.add(name)
                namesToFind.remove(name)

                pkgList = []
                for version, flavorList in matches.iteritems():
                    pkgList.extend((name, version, f) for f in flavorList)

                    if self.acrossLabels:
                        foundNameLabels.add((name, 
                                             version.branch().label()))
                finalMap.setdefault(self.map[name][0], []).extend(pkgList)
            index +=1

    def missingMsg(self, name):
        # collapse all the labels searched in the queries to a unique list
        labelPath = self.map[name][1]
        if labelPath:
            return "%s was not found on path %s" \
                    % (name, ', '.join(x.asString() for x in labelPath))
        else:
            return "%s was not found" % name

class QueryByBranch(Query):

    def __init__(self, *args, **kw):
        Query.__init__(self, *args, **kw)
        self.queryNoFlavor = {}
        self.affinityFlavors = {}
        # localTroves are troves that, through affinity, are assigned to
        # a local branch.  Since there's no repository associated with 
        # local branches, there's no chance of an update available.  
        # We merely return an empty set of troves, showing that there were
        # no updates found for that trove.
        self.localTroves = set()

    def reset(self):
        Query.reset(self)
        self.queryNoFlavor.clear()
        self.affinityFlavors.clear()
        self.localTroves.clear()

    def addQuery(self, troveTup, branch, flavorList):
        name = troveTup[0]
        if not flavorList:
            self.queryNoFlavor[name] = { branch : None }
        else:
            for i, flavor in enumerate(flavorList):
                if name not in self.query[i]:
                    self.query[i][name] = { branch: []}
                elif branch not in self.query[i][name]:
                    self.query[i][name][branch] = []
                self.query[i][name][branch].append(flavor)
        self.map[name] = [ troveTup ]

    def addQueryWithAffinity(self, troveTup, branch, affinityTroves):
        if branch:
            # use the affinity flavor if it's the same for all troves, 
            # otherwise revert to the default flavor
            flavors = [x[2] for x in affinityTroves]
            f = flavors[0]
            for otherFlavor in flavors:
                if otherFlavor != f:
                    f = None
                    break
            if f is None:
                flavorList = self.defaultFlavorPath
            else:
                flavorList = self.overrideFlavors(f)

            self.addQuery(troveTup, branch, flavorList)
        else:
            flavor = troveTup[2]
            for dummy, afVersion, afFlavor in affinityTroves:
                if afVersion.isOnLocalHost():
                    self._addLocalTrove(troveTup)
                    continue

                if flavor:
                    flavorList = self.overrideFlavors(flavor)
                else:
                    flavorList = self.overrideFlavors(afFlavor)

                self.addQuery(troveTup, afVersion.branch(), flavorList)

    def _addLocalTrove(self, troveTup):
        name = troveTup[0]
        self.map[name] = [ troveTup ]
        self.localTroves.add(name)

    def findAll(self, troveSource, missing, finalMap):
        self._findAllNoFlavor(troveSource, missing, finalMap)
        self._findAllFlavor(troveSource, missing, finalMap)
        self._findLocalTroves(finalMap)

    def callQueryFunction(self, troveSource, query):
        if self.getLeaves:
            return troveSource.getTroveLeavesByBranch(query,
                                                     bestFlavor=self.bestFlavor)
        else:
            return troveSource.getTroveVersionsByBranch(query)

    def _findAllFlavor(self, troveSource, missing, finalMap):
        # list of names not yet found
        namesToFind = set(self.query[0])

        # name, branch tuples that have been found -- if a trove
        # is being sought on multiple branches, we still only want to 
        # return one name per branch 
        foundBranches = set()

        # names that have been found -- as long as a name has been found
        # with one branch/flavor, do not give a missing message
        foundNames = set()

        for query in self.query:
            for name, branch in foundBranches:
                query[name].pop(branch, None)
                if not query[name]:
                    del query[name]
            if not query:
                break
            res = self.callQueryFunction(troveSource, query)
            if not res:
                continue
            for name in res:
                matches = self.filterTroveMatches(name, res[name])

                if not matches:
                    continue

                foundNames.add(name)
                try:
                    namesToFind.remove(name)
                except KeyError:
                    pass
                pkgList = []
                for version, flavorList in matches.iteritems():
                    pkgList.extend((name, version, f) for f in flavorList)
                    foundBranches.add((name, version.branch()))
                finalMap.setdefault(self.map[name][0], []).extend(pkgList)
        for name in namesToFind:
            self.addMissing(missing, name)

    def _findAllNoFlavor(self, troveSource, missing, finalMap):
        if not self.queryNoFlavor:
            return
        if self.getLeaves:
            res = troveSource.getTroveLeavesByBranch(self.queryNoFlavor, 
                                                     bestFlavor=False)
        else:
            res = troveSource.getTroveVersionsByBranch(self.queryNoFlavor)

        for name in self.queryNoFlavor:
            if name not in res or not res[name]:
                self.addMissing(missing, name)
                continue

            matches = self.filterTroveMatches(name, res[name])
            if not matches: 
                continue

            pkgList = []
            for version, flavorList in res[name].iteritems():
                pkgList.extend((name, version, f) for f in flavorList)
            finalMap[self.map[name][0]] = pkgList

    def _findLocalTroves(self, finalMap):
        for name in self.localTroves:
            finalMap.setdefault(self.map[name][0], [])

    def missingMsg(self, name):
        flavor = self.map[name][0][2]
        if name in self.queryNoFlavor:
            branches = self.queryNoFlavor[name].keys()
        else:
            branches = self.query[0][name].keys()
        return "%s was not found on branches %s" \
                % (name, ', '.join(x.asString() for x in branches))

class QueryRevisionByBranch(QueryByBranch):

    def addQuery(self, troveTup, branch, flavorList):
        # QueryRevisionByBranch is only reached when a revision is specified
        # for findTrove and an affinity trove was found.  flavorList should
        # not be empty.
        assert(flavorList is not None)
        QueryByBranch.addQuery(self, troveTup, branch, flavorList)

    def callQueryFunction(self, troveSource, query):
        return troveSource.getTroveVersionsByBranch(query,
                                                    bestFlavor=self.bestFlavor)

    def filterTroveMatches(self, name, versionFlavorDict):
        versionFlavorDict = QueryByBranch.filterTroveMatches(self, name, 
                                                             versionFlavorDict)
        versionStr = self.map[name][0][1]
        try:
            verRel = versions.Revision(versionStr)
        except errors.ParseError:
            verRel = None

        results = {}
        for version in reversed(sorted(versionFlavorDict.iterkeys())):
            if verRel:
                if version.trailingRevision() != verRel:
                    continue
            else:
                if version.trailingRevision().version != versionStr:
                    continue
            if self.getLeaves:
                return { version: versionFlavorDict[version] }
            else:
                results[version] = versionFlavorDict[version]
        return results

    def missingMsg(self, name):
        branch = self.query[0][name].keys()[0]
        versionStr = self.map[name][0][1]
        return "revision %s of %s was not found on branch %s" \
                                    % (versionStr, name, branch.asString())

class QueryRevisionByLabel(QueryByLabelPath):

    queryFunctionName = 'getTroveVersionsByLabel'

    def callQueryFunction(self, troveSource, query):
        return troveSource.getTroveVersionsByLabel(query,
                                                   bestFlavor=self.bestFlavor)

    def filterTroveMatches(self, name, versionFlavorDict):
        """ Take the results found in QueryByLabelPath.findAll for name
            and filter them based on if they match the given revision
            for name.  Return a versionFlavorDict
        """
        versionFlavorDict = QueryByLabelPath.filterTroveMatches(self, name, 
                                                            versionFlavorDict)

        matching = {}
        matchingLabels = set()

        versionStr = self.map[name][0][1].split('/')[-1]
        try:
            verRel = versions.Revision(versionStr)
        except errors.ParseError:
            verRel = None
        for version in reversed(sorted(versionFlavorDict.iterkeys())):
            if verRel:
                if version.trailingRevision() != verRel:
                    continue
            else:
                if version.trailingRevision().version \
                                                != versionStr:
                    continue
            if self.getLeaves and not self.acrossLabels:
                # there should be only one label in this versionFlavorDict --
                # so, optimize to return first result found
                return {version: versionFlavorDict[version]}

            if self.getLeaves:
                label = version.branch().label()
                if label in matchingLabels:
                    continue
                matchingLabels.add(label)
            matching[version] = versionFlavorDict[version]
        return matching

    def missingMsg(self, name):
        labelPath = self.map[name][1]
        versionStr = self.map[name][0][1].split('/')[-1]
        if labelPath:
            return "revision %s of %s was not found on label(s) %s" \
                    % (versionStr, name, 
                       ', '.join(x.asString() for x in labelPath))
        else:
            return "revision %s of %s was not found" \
                    % (versionStr, name)

##############################################
# 
# query map from enumeration to classes that define how to grab 
# the related troves

queryTypeMap = { QUERY_BY_BRANCH            : QueryByBranch,
                 QUERY_BY_VERSION           : QueryByVersion,
                 QUERY_BY_LABEL_PATH        : QueryByLabelPath, 
                 QUERY_REVISION_BY_LABEL    : QueryRevisionByLabel, 
                 QUERY_REVISION_BY_BRANCH   : QueryRevisionByBranch,
               }

def getQueryClass(tag):
    return queryTypeMap[tag]


##########################################################


class TroveFinder:
    """ find troves by sorting them into query types by the version string
        and then calling those query types.   
    """

    def findTroves(self, troveSpecs, allowMissing=False):
        troveSource = self.troveSource
        finalMap = {}

        while troveSpecs:
            self.remaining = []

            for troveSpec in troveSpecs:
                self.addQuery(troveSpec)

            missing = {}

            for query in self.query.values():
                if query.hasTroves():
                    query.findAll(troveSource, missing, finalMap)
                    query.reset()

            if missing and not allowMissing:
                if len(missing) > 1:
                    missingMsgs = [ missing[x] for x in troveSpecs if x in missing]
                    raise errors.TroveNotFound, '%d troves not found:\n%s\n' \
                            % (len(missing), '\n'.join(x for x in missingMsgs))
                else:
                    raise errors.TroveNotFound, missing.values()[0]

            troveSpecs = self.remaining

        return finalMap

    def addQuery(self, troveTup):
        (name, versionStr, flavor) = troveTup
        if not self.labelPath:
            hasLabelPath = False

            # need a branch or a full label
            if versionStr and (versionStr[0] == '/' or '@' in versionStr[1:]):
                hasLabelPath = True

            if not hasLabelPath and not self.allowNoLabel:
                raise errors.TroveNotFound, \
                    "fully qualified version or label " + \
                    "expected instead of %s" % versionStr

        affinityTroves = []
        if self.affinityDatabase:
            try:
                affinityTroves = self.affinityDatabase.findTrove(None, 
                                                                 (troveTup[0],
                                                                  None, None))
            except errors.TroveNotFound:
                pass
        
        type = self._getVersionType(troveTup)
        sortFn = self.getVersionStrSortFn(type)
        sortFn(self, troveTup, affinityTroves) 

    ########################
    # The following functions translate from the version string in the
    # trove spec to the type of query that will actually find the trove(s)
    # corresponding to this trove spec.  We call this sorting the trovespec
    # into the correct query.

    def _getVersionType(self, troveTup):
        """
        Return a string that describes this troveTup's versionStr
        The string returned corresponds to a function name for sorting on 
        that versionStr type.
        """
        name = troveTup[0]
        versionStr = troveTup[1]
        if not versionStr:
            return VERSION_STR_NONE
        if isinstance(versionStr, versions.Version):
            return VERSION_STR_FULL_VERSION
        elif isinstance(versionStr, versions.Branch):
            return VERSION_STR_BRANCH

        firstChar = versionStr[0]
        lastChar = versionStr[-1]
        if firstChar == '/':
            try:
                version = versions.VersionFromString(versionStr)
            except errors.ParseError, e:
                raise errors.TroveNotFound, str(e)
            if isinstance(version, versions.Branch):
                return VERSION_STR_BRANCH
            else:
                return VERSION_STR_FULL_VERSION

        slashCount = versionStr.count('/')

        if slashCount > 1:
            # if we've got a version string, and it doesn't start with a
            # /, only one / is allowed
            raise errors.TroveNotFound, \
                    "incomplete version string %s not allowed" % versionStr
        elif firstChar == '@':
            return VERSION_STR_BRANCHNAME
        elif firstChar == ':':
            return VERSION_STR_TAG
        elif versionStr.split('/')[0][-1] == '@':
            return VERSION_STR_HOST
        elif versionStr.count('@'):
            return VERSION_STR_LABEL
        else:
            if slashCount:
                # if you've specified a prefix, it must have some identifying
                # mark and not just be foo/1.2
                raise errors.TroveNotFound, ('Illegal version prefix %s'
                                                 ' for %s' % (versionStr, name))
            for char in ' ,':
                if char in versionStr:
                    raise errors.ParseError, \
                        ('%s requests illegal version/revision %s' 
                                                % (name, versionStr))
            if '-' in versionStr:
                # attempt to parse the versionStr
                try:
                    versions.Revision(versionStr)
                except errors.ParseError, err:
                    raise errors.TroveNotFound(str(err))
                return VERSION_STR_REVISION
            return VERSION_STR_TROVE_VER

    def _getLabelPath(self, troveTup):
        if self.labelPath:
            return self.labelPath
        else:
            return [ x.branch().label() \
                     for x in self.troveSource.getTroveVersionList(troveTup[0])]

    def sortNoVersion(self, troveTup, affinityTroves):
        name, versionStr, flavor = troveTup
        if affinityTroves:
            if self.query[QUERY_BY_BRANCH].hasName(name):
                self.remaining.append(troveTup)
                return
            self.query[QUERY_BY_BRANCH].addQueryWithAffinity(troveTup, None, 
                                                             affinityTroves)
        elif self.query[QUERY_BY_LABEL_PATH].hasName(name):
            self.remaining.append(troveTup)
            return
        else:
            flavorList = self.mergeFlavors(flavor)
            labelPath = self._getLabelPath(troveTup)
            self.query[QUERY_BY_LABEL_PATH].addQuery(troveTup,
                                                     labelPath, 
                                                     flavorList)

    def sortBranch(self, troveTup, affinityTroves):
        name, versionStr, flavor = troveTup
        if self.query[QUERY_BY_BRANCH].hasName(name):
            self.remaining.append(troveTup)
            return

        if isinstance(versionStr, versions.Branch):
            branch = versionStr
        else:
            branch = versions.VersionFromString(versionStr)

        if flavor is None and affinityTroves:
            self.query[QUERY_BY_BRANCH].addQueryWithAffinity(troveTup, branch, 
                                                             affinityTroves)
        else:
            flavorList = self.mergeFlavors(flavor)
            self.query[QUERY_BY_BRANCH].addQuery(troveTup, branch, flavorList)

    def sortFullVersion(self, troveTup, affinityTroves):
        name, versionStr, flavor = troveTup
        if self.query[QUERY_BY_VERSION].hasName(name):
            self.remaining.append(troveTup)
            return
        if isinstance(versionStr, versions.Version):
            version = versionStr
        else:
            version = versions.VersionFromString(versionStr)

        if flavor is None and affinityTroves:
            self.query[QUERY_BY_VERSION].addQueryWithAffinity(troveTup, 
                                                              version,
                                                              affinityTroves)
        else:
            flavorList = self.mergeFlavors(flavor)
            self.query[QUERY_BY_VERSION].addQuery(troveTup, version, flavorList)



    def sortLabel(self, troveTup, affinityTroves):
        try:
            label = versions.Label(troveTup[1].split('/', 1)[0])
            newLabelPath = [ label ]
        except errors.ParseError:
            raise errors.TroveNotFound, \
                                "invalid version %s" % troveTup[1]
        return self._sortLabel(newLabelPath, troveTup, affinityTroves)

    def sortBranchName(self, troveTup, affinityTroves):
        # just a branch name was specified
        labelPath = self._getLabelPath(troveTup)

        repositories = [ x.getHost() for x in labelPath ]
        versionStr = troveTup[1].split('/', 1)[0]
        newLabelPath = []
        for serverName in repositories:
            newLabelPath.append(versions.Label("%s%s" %
                                               (serverName, versionStr)))
        return self._sortLabel(newLabelPath, troveTup, affinityTroves)
        
    def sortTag(self, troveTup, affinityTroves):
        labelPath = self._getLabelPath(troveTup)
        repositories = [(x.getHost(), x.getNamespace()) \
                         for x in labelPath ]
        newLabelPath = []
        versionStr = troveTup[1].split('/', 1)[0]
        for serverName, namespace in repositories:
            newLabelPath.append(versions.Label("%s@%s%s" %
                               (serverName, namespace, versionStr)))
        return self._sortLabel(newLabelPath, troveTup, affinityTroves)

    def sortHost(self, troveTup, affinityTroves):
        labelPath = self._getLabelPath(troveTup)
        repositories = [(x.getNamespace(), x.getLabel()) \
                         for x in labelPath ]
        newLabelPath = []
        serverName = troveTup[1].split('/', 1)[0]
        for nameSpace, branchName in repositories:
            newLabelPath.append(versions.Label("%s%s:%s" %
                               (serverName, nameSpace, branchName)))
        return self._sortLabel(newLabelPath, troveTup, affinityTroves)

    def _sortLabel(self, labelPath, troveTup, affinityTroves):
        name, verStr, flavor = troveTup
        revision = verStr.count('/') != 0 
        if revision:
            queryType = QUERY_REVISION_BY_LABEL
        else:
            queryType = QUERY_BY_LABEL_PATH

        if self.query[queryType].hasName(troveTup[0]): 
            self.remaining.append(troveTup)
            return
        if flavor is None and affinityTroves:
            self.query[queryType].addQueryWithAffinity(troveTup, labelPath, 
                                                       affinityTroves)
        else:
            flavorList = self.mergeFlavors(flavor)
            self.query[queryType].addQuery(troveTup, labelPath, flavorList)

    def sortTroveVersion(self, troveTup, affinityTroves):
        name = troveTup[0]
        flavor = troveTup[2]
        if flavor is None and affinityTroves:
            if self.query[QUERY_REVISION_BY_BRANCH].hasName(name):
                self.remaining.append(troveTup)
                return
            self.query[QUERY_REVISION_BY_BRANCH].addQueryWithAffinity(troveTup,
                                                          None, affinityTroves)
        elif self.query[QUERY_REVISION_BY_LABEL].hasName(name):
            self.remaining.append(troveTup)
            return
        else:
            flavorList = self.mergeFlavors(flavor)
            labelPath = self._getLabelPath(troveTup)
            self.query[QUERY_REVISION_BY_LABEL].addQuery(troveTup, labelPath,
                                                         flavorList)

    def getVersionStrSortFn(self, versionStrType):
        return self.versionStrToSortFn[versionStrType]

    def mergeFlavors(self, flavor):
        """ Merges the given flavor with the flavorPath - if flavor 
            doesn't contain use flags, then include the defaultFlavor's 
            use flags.  If flavor doesn't contain an instruction set, then 
            include the flavorpath's instruction set(s)
        """
        if flavor is None:
            return self.defaultFlavorPath
        if not self.defaultFlavorPath:
            return [flavor]
        return [ deps.overrideFlavor(x, flavor) for x in self.defaultFlavorPath ]

    def __init__(self, troveSource, labelPath, defaultFlavorPath, 
                 acrossLabels, acrossFlavors, affinityDatabase, 
                 getLeaves=True, bestFlavor=True,
                 allowNoLabel=False):

        self.troveSource = troveSource
        self.affinityDatabase = affinityDatabase
        self.acrossLabels = acrossLabels
        self.acrossFlavors = acrossFlavors
        if labelPath and not hasattr(labelPath, '__iter__'):
            labelPath = [ labelPath ]
        self.labelPath = labelPath
        self.getLeaves = getLeaves
        self.bestFlavor = bestFlavor
        self.allowNoLabel = allowNoLabel

        if defaultFlavorPath is not None and not isinstance(defaultFlavorPath,
                                                            list):
            defaultFlavorPath = [defaultFlavorPath]
        self.defaultFlavorPath = defaultFlavorPath


        self.remaining = []
        self.query = {}
        for queryType in queryTypes:
            self.query[queryType] = getQueryClass(queryType)(defaultFlavorPath, 
                                                             labelPath, 
                                                             acrossLabels,
                                                             acrossFlavors,
                                                             getLeaves,
                                                             bestFlavor)
    # class variable for TroveFinder
    #
    # set up map from a version string type to the source fn to use
    versionStrToSortFn = \
             { VERSION_STR_NONE         : sortNoVersion,
               VERSION_STR_FULL_VERSION : sortFullVersion,
               VERSION_STR_BRANCH       : sortBranch,
               VERSION_STR_LABEL        : sortLabel,
               VERSION_STR_BRANCHNAME   : sortBranchName,
               VERSION_STR_TAG          : sortTag,
               VERSION_STR_HOST         : sortHost,
               VERSION_STR_REVISION     : sortTroveVersion,
               VERSION_STR_TROVE_VER    : sortTroveVersion }

