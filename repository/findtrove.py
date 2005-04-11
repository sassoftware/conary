
from deps import deps
import repository
import versions


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

class Query:
    def __init__(self, defaultFlavor, labelPath, acrossRepositories):
        self.query = {}
        self.map = {}
        self.defaultFlavor = defaultFlavor
        self.labelPath = labelPath
        self.acrossRepositories = acrossRepositories

    def reset(self):
        self.query = {}
        self.map = {}

    def hasName(self, name):
        return name in self.map

    def hasTroves(self):
        return bool(self.map)

    def findAll(self, repos, missing, finalMap):
        raise NotImplementedError

    def addQuery(self, troveTup, *params):
        raise NotImplementedError

    def addMissing(self, missing, name):
        troveTup = self.map[name]
        missing[troveTup] = self.missingMsg(name)
            
    def missingMsg(self, name):
        versionStr = self.map[name][1]
        if not versionStr:
            return ("%s was not on found on path %s" \
                    % (name, ', '.join(x.asString() for x in self.labelPath)))
        elif self.labelPath:
            return ("version %s of %s was not on found on path %s" \
                    % (versionStr, name, 
                       ', '.join(x.asString() for x in labelPath)))
        else:
            return "version %s of %s was not on found" % (versionStr, name)

class QueryByVersion(Query):

    def __init__(self, defaultFlavor, labelPath, acrossRepositories):
        Query.__init__(self, defaultFlavor, labelPath, acrossRepositories)
        self.queryNoFlavor = {}

    def reset(self):
        Query.reset(self)
        self.queryNoFlavor = {}

    def addQuery(self, troveTup, version, flavor):
        name = troveTup[0]
        self.map[name] = troveTup
        if flavor is None:
            self.queryNoFlavor[name] = { version : [ None ] }
        else:
            self.query[name] = { version : [flavor] }

    def addQueryWithAffinity(self, troveTup, version, flavor, affinityTroves):
        assert(flavor is None)
        flavors = [x[2] for x in affinityTroves]
        f = flavors[0]
        for otherFlavor in flavors:
            if otherFlavor != f:
                f = self.defaultFlavor
                break
        f = deps.overrideFlavor(self.defaultFlavor, f, 
                                mergeType = deps.DEP_MERGE_TYPE_PREFS)
        self.addQuery(troveTup, version, f)

    def findAll(self, repos, missing, finalMap):
        self._findAllNoFlavor(repos, missing, finalMap)
        self._findAllFlavor(repos, missing, finalMap)

    def _findAllFlavor(self, repos, missing, finalMap):
        res = repos.getTroveVersionFlavors(self.query, bestFlavor=True)
        for name in self.query:
            if name not in res or not res[name]:
                self.addMissing(missing, name)
                continue
            pkgList = []
            for version, flavorList in res[name].iteritems():
                pkgList.extend((name, version, f) for f in flavorList)
            finalMap[self.map[name]] = pkgList

    def _findAllNoFlavor(self, repos, missing, finalMap):
        res = repos.getTroveVersionFlavors(self.queryNoFlavor, bestFlavor=False)
        for name in self.queryNoFlavor:
            if name not in res or not res[name]:
                self.addMissing(missing, name)
                continue
            pkgList = []
            for version, flavorList in res[name].iteritems():
                pkgList.extend((name, version, f) for f in flavorList)
            finalMap[self.map[name]] = pkgList

    def missingMsg(self, name):
        versionStr = self.map[name][1]
        return "version %s of %s was not on found" % (versionStr, name)

class QueryByLabelPath(Query):

    def addQuery(self, troveTup, labelPath, flavor):
        name = troveTup[0]
        self.map[name] = troveTup
        if flavor is None:
            flavorList = None
        else:
            flavorList = [flavor]
        if self.acrossRepositories:
            self.query[name] = [ dict.fromkeys(labelPath, flavorList)]
        else:
            self.query[name] = []
            for label in labelPath:
                self.query[name].append({label : flavorList})

    def addQueryWithAffinity(self, troveTup, labelPath, affinityTroves):
        name = troveTup[0]
        self.map[name] = troveTup
        if self.acrossRepositories:
            # d is the label : flavor dict for this trove
            d = {}
            self.query[name] = [d]
        else:
            # lst is a list of {label : flavor} dicts for this trove
            lst = []
            self.query[name] = lst

        for label in labelPath:
            flavors = []
            for (afName, afVersion, afFlavor) in affinityTroves:
                if afVersion.branch().label() == label:
                    flavors.append(afFlavor)
            if not flavors:
                f = self.defaultFlavor
            else:
                f = flavors[0]
                for otherFlavor in flavors:
                    if otherFlavor != f:
                        f = self.defaultFlavor
                        break
                f = deps.overrideFlavor(self.defaultFlavor, f, 
                           mergeType = deps.DEP_MERGE_TYPE_PREFS)
            if f:
                flavorList = [f]
            else:
                flavorList = None

            if self.acrossRepositories:
                # acrossRepositories - 
                # mesh this query into d
                d[label] = flavorList
            else:
                # not acrossRepositories - 
                # append this query onto lst
                lst.append({label :  flavorList})
        
    def findAll(self, repos, missing, finalMap):
        index = 0
        while self.query:
            query = {}
            for name in self.query.keys():
                try:
                    req = self.query[name][index]
                except IndexError:
                    self.addMissing(missing, name)
                    del self.query[name]
                    continue
                else:
                    query[name] = req

            res = repos.getTroveLeavesByLabel(query, bestFlavor=True)

            for name in res:
                if not res[name]:
                    continue
                del self.query[name]
                pkgList = []
                for version, flavorList in res[name].iteritems():
                    pkgList.extend((name, version, f) for f in flavorList)
                finalMap[self.map[name]] = pkgList
            index +=1

    def missingMsg(self, name):
        labelPath = [ x.keys()[0] for x in self.query[name] ]
        return "%s was not on found on path %s" \
                % (name, ', '.join(x.asString() for x in labelPath))

class QueryByBranch(Query):

    def __init__(self, defaultFlavor, labelPath, acrossRepositories):
        Query.__init__(self, defaultFlavor, labelPath, acrossRepositories)
        self.queryNoFlavor = {}

    def reset(self):
        Query.reset(self)
        self.queryNoFlavor = {}

    def addQuery(self, troveTup, branch, flavor):
        name = troveTup[0]
        if flavor is None:
            self.queryNoFlavor[name] = { branch : [ None ] }
        else:
            self.query[name] = { branch : [ flavor ] }
        self.map[name] = troveTup 

    def addQueryWithAffinity(self, troveTup, branch, flavor, 
                                                     affinityTroves):
        if branch:
            # use the affinity flavor if it's the same for all troves, 
            # otherwise revert to the default flavor
            flavors = [x[2] for x in affinityTroves]
            f = flavors[0]
            for otherFlavor in flavors:
                if otherFlavor != f:
                    f = self.defaultFlavor
                    break
            f = deps.overrideFlavor(self.defaultFlavor, flavor, 
                        mergeType = deps.DEP_MERGE_TYPE_PREFS)
            self.addQuery(troveTup, branch, f)
        else:
            name = troveTup[0]
            self.map[name] = troveTup 
            self.query[name] = {}
            for dummy, afVersion, afFlavor in affinityTroves:
                f = deps.overrideFlavor(self.defaultFlavor, afFlavor, 
                                   mergeType = deps.DEP_MERGE_TYPE_PREFS)
                self.query[name].setdefault(afVersion.branch(), []).append(f)

    def findAll(self, repos, missing, finalMap):
        self._findAllNoFlavor(repos, missing, finalMap)
        self._findAllFlavor(repos, missing, finalMap)

    def _findAllFlavor(self, repos, missing, finalMap):
        res = repos.getTroveLeavesByBranch(self.query, bestFlavor=True)
        for name in self.query:
            if name not in res or not res[name]:
                self.addMissing(missing, name)
                continue
            pkgList = []
            for version, flavorList in res[name].iteritems():
                pkgList.extend((name, version, f) for f in flavorList)
            finalMap[self.map[name]] = pkgList

    def _findAllNoFlavor(self, repos, missing, finalMap):
        res = repos.getTroveLeavesByBranch(self.queryNoFlavor, bestFlavor=False)
        for name in self.queryNoFlavor:
            if name not in res or not res[name]:
                self.addMissing(missing, name)
                continue
            pkgList = []
            for version, flavorList in res[name].iteritems():
                pkgList.extend((name, version, f) for f in flavorList)
            finalMap[self.map[name]] = pkgList

    def missingMsg(self, name):
        flavor = self.map[name][2]
        if flavor is None:
            branches = self.queryNoFlavor[name].keys()
        else:
            branches = self.query[name].keys()
        return "%s was not on found on branches %s" \
                % (name, ', '.join(x.asString() for x in branches))

class QueryRevisionByBranch(Query):

    def addQuery(self, troveTup, branch, flavor):
        self.map[troveTup[0]] = troveTup
        self.query[troveTup[0]] = { branch : [flavor] } 

    def addQueryWithAffinity(self, troveTup, flavor, affinityTroves):
        assert(not flavor)
        name = troveTup[0]
        self.map[name] = troveTup
        self.query[name] = {}
        for dummy, afVersion, afFlavor in affinityTroves:
            f = deps.overrideFlavor(self.defaultFlavor, afFlavor, 
                       mergeType = deps.DEP_MERGE_TYPE_PREFS)
            branch = afVersion.branch()
            self.query[name].setdefault(branch, []).append(f)

    def findAll(self, repos, missing, finalMap):
        res = repos.getTroveVersionsByBranch(self.query, bestFlavor=True)
        for name in self.query:
            versionStr = self.map[name][1]
            try:
                verRel = versions.Revision(versionStr)
            except versions.ParseError, e:
                verRel = None
            found = False
            for version in reversed(sorted(res[name].iterkeys())):
                if verRel:
                    if version.trailingRevision() != verRel:
                        continue
                else:
                    if version.trailingRevision().version != versionStr:
                        continue
                found = True
                pkgList = [(name, version, x) \
                                for x in res[name][version]]
                finalMap[self.map[name]] = pkgList
                break
            if not found:
                self.addMissing(missing, name)

    def missingMsg(self, name):
        branch = self.query[name].keys()[0]
        versionStr = self.map[name][1]
        return "revision %s of %s was found on branch %s" \
                                    % (versionStr, name, branch.asString())


class QueryRevisionByLabel(Query):

    def addQuery(self, troveTup, flavor):
        name = troveTup[0]
        self.map[name] = troveTup
        if self.acrossRepositories:
            self.query[name] = [dict.fromkeys(self.labelPath, [flavor])]
        else:
            lst = []
            self.query[name] = lst
            for label in self.labelPath:
                lst.append({label :  [flavor]})

    def findAll(self, repos, missing, finalMap):
        index = 0
        while self.query:
            query = {}
            for name in self.query.keys():
                try:
                    req = self.query[name][index]
                except IndexError:
                    self.addMissing(missing, name)
                    del self.query[name]
                    continue
                query[name] = req

            # map [ None ] flavor to None
            for verSet in query.itervalues():
                for version, flavorList in verSet.items():
                    if flavorList == [ None ]:
                        verSet[version] = None
                    else:
                        assert(None not in flavorList)

            res = repos.getTroveVersionsByLabel(query, bestFlavor=True)

            for name in res:
                if not res[name]:
                    continue
                versionStr = self.map[name][1]
                try:
                    verRel = versions.Revision(versionStr)
                except versions.ParseError, e:
                    verRel = None
                # get the labest matching version on the first matching
                # label
                for version in reversed(sorted(res[name].iterkeys())):
                    if verRel:
                        if version.trailingRevision() != verRel:
                            continue
                    else:
                        if version.trailingRevision().version \
                                                        != versionStr:
                            continue
                    del self.query[name]
                    pkgList = [(name, version, x) \
                                    for x in res[name][version]]
                    finalMap[self.map[name]] = pkgList
                    break
            index += 1

    def missingMsg(self, name):
        labelPath = [ x.keys()[0] for x in self.query[name] ]
        versionStr = self.map[name][1]
        return "revision %s of %s was not found on label(s) %s" \
                % (versionStr, name, 
                   ', '.join(x.asString() for x in labelPath))

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

    def findTroves(self, repos, troveSpecs, allowMissing=False):
        finalMap = {}

        while troveSpecs:
            self.remaining = []

            for troveSpec in troveSpecs:
                self.addQuery(troveSpec)

            missing = {}

            for query in self.query.values():
                query.findAll(repos, missing, finalMap)
                query.reset()

            if missing and not allowMissing:
                if len(missing) > 1:
                    missingMsgs = [ missing[x] for x in troveSpecs if x in missing]
                    raise repository.TroveNotFound, '%d troves not found:\n%s\n' \
                            % (len(missing), '\n'.join(x for x in missingMsgs))
                else:
                    raise repository.TroveNotFound, missing.values()[0]

            troveSpecs = self.remaining

        return finalMap

    def addQuery(self, troveTup):
        (name, versionStr, flavor) = troveTup
        if not self.labelPath and versionStr[0] != "/":
            raise repository.TroveNotFound, \
                "fully qualified version or label " + \
                "expected instead of %s" % versionStr

        affinityTroves = []
        if self.affinityDatabase:
            try:
                affinityTroves = self.affinityDatabase.findTrove(None, 
                                                                 troveTup[0])
            except repository.TroveNotFound:
                pass
        
        # set up flavor for all cases except when 
        # 1. there is no flavor and 2. there are affinity troves
        # if we didn't do this here, this code would be repeated
        # in every Query class
        if flavor is not None:
            f = flavor
        elif not affinityTroves:
            f = self.defaultFlavor
        else:
            f = None

        type = self._getVersionType(troveTup)
        sortFn = self.getVersionStrSortFn(type)
        sortFn(self, troveTup, affinityTroves, f) 

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
        firstChar = versionStr[0]
        if firstChar == '/':
            try:
                version = versions.VersionFromString(versionStr)
            except versions.ParseError, e:
                raise repository.TroveNotFound, str(e)
            if isinstance(version, versions.Branch):
                return VERSION_STR_BRANCH
            else:
                return VERSION_STR_FULL_VERSION
        elif versionStr.find('/') != -1:
            # if we've got a version string, and it doesn't start with a
            # /, no / is allowed
            raise repository.TroveNotFound, \
                    "incomplete version string %s not allowed" % versionStr
        elif firstChar == '@':
            return VERSION_STR_BRANCHNAME
        elif firstChar == ':':
            return VERSION_STR_TAG
        elif versionStr.count('@'):
            return VERSION_STR_LABEL
        else:
            for char in ' ,':
                if char in versionStr:
                    raise RuntimeError, \
                        ('%s reqests illegal version/revision %s' 
                                                % (name, versionStr))
            if '-' in versionStr:
                try:
                    verRel = versions.Revision(versionStr)
                    return VERSION_STR_REVISION
                except ParseError, msg:
                    raise repository.TroveNotFound, str(msg)
            return VERSION_STR_TROVE_VER

    def sortNoVersion(self, troveTup, affinityTroves, f):
        name = troveTup[0]
        if affinityTroves:
            if self.query[QUERY_BY_BRANCH].hasName(name):
                self.remaining.append(troveTup)
                return
            self.query[QUERY_BY_BRANCH].addQueryWithAffinity(troveTup, None, f,
                                                             affinityTroves)
        elif self.query[QUERY_BY_LABEL_PATH].hasName(name):
            self.remaining.append(troveTup)
            return
        else:
            self.query[QUERY_BY_LABEL_PATH].addQuery(troveTup,
                                                     self.labelPath, f)

    def sortBranch(self, troveTup, affinityTroves, f):
        name, version, flavor = troveTup
        if self.query[QUERY_BY_BRANCH].hasName(name):
            self.remaining.append(tup)
            return
        if flavor is None and affinityTroves:
            self.query[QUERY_BY_BRANCH].addQueryWithAffinity(troveTup, branch, 
                                                             f, affinityTroves)
        branch = versions.VersionFromString(troveTup[1])
        self.query[QUERY_BY_BRANCH].addQuery(troveTup, branch, f)

    def sortFullVersion(self, troveTup, affinityTroves, f):
        name, versionStr, flavor = troveTup
        if self.query[QUERY_BY_VERSION].hasName(name):
            self.remaining.append(tup)
            return
        version = versions.VersionFromString(versionStr)
        if flavor is None and affinityTroves:
            self.query[QUERY_BY_VERSION].addQueryWithAffinity(troveTup, 
                                                              version, f, 
                                                              affinityTroves)
        self.query[QUERY_BY_VERSION].addQuery(troveTup, version, f)

    def sortLabel(self, troveTup, affinityTroves, f):
        try:
            label = versions.Label(troveTup[1])
            newLabelPath = [ label ]
        except versions.ParseError:
            raise repository.TroveNotFound, \
                                "invalid version %s" % versionStr
        return self._sortLabel(newLabelPath, troveTup, affinityTroves, f)

    def sortBranchName(self, troveTup, affinityTroves, f):
        # just a branch name was specified
        repositories = [ x.getHost() for x in self.labelPath ]
        versionStr = troveTup[1]
        newLabelPath = []
        for serverName in repositories:
            newLabelPath.append(versions.Label("%s%s" %
                                               (serverName, versionStr)))
        return self._sortLabel(newLabelPath, troveTup, affinityTroves, f)
        
    def sortTag(self, troveTup, affinityTroves, f):
        repositories = [(x.getHost(), x.getNamespace()) \
                         for x in self.labelPath ]
        newLabelPath = []
        versionStr = troveTup[1]
        for serverName, namespace in repositories:
            newLabelPath.append(versions.Label("%s@%s%s" %
                               (serverName, namespace, versionStr)))
        return self._sortLabel(newLabelPath, troveTup, affinityTroves, f)

    def _sortLabel(self, labelPath, troveTup, affinityTroves, f):
        if self.query[QUERY_BY_LABEL_PATH].hasName(troveTup[0]): 
            self.remaining.append(troveTup)
            return
        if not f and affinityTroves:
            self.query[QUERY_BY_LABEL_PATH].addQueryWithAffinity(troveTup, 
                                                    labelPath, affinityTroves)
        else:
            self.query[QUERY_BY_LABEL_PATH].addQuery(troveTup, labelPath, f)

    def sortTroveVersion(self, troveTup, affinityTroves, f):
        name = troveTup[0]
        if affinityTroves:
            if self.query[QUERY_REVISION_BY_BRANCH].hasName(name):
                self.remaining.append(tup)
                return
            self.query[QUERY_REVISION_BY_BRANCH].addQueryWithAffinity(troveTup,
                                                            f, affinityTroves)
        elif self.query[QUERY_REVISION_BY_LABEL].hasName(name):
            self.remaining.append(troveTup)
            return
        else:
            self.query[QUERY_REVISION_BY_LABEL].addQuery(troveTup, f)

    def getVersionStrSortFn(self, versionStrType):
        return self.versionStrToSortFn[versionStrType]

    def __init__(self, labelPath, defaultFlavor, acrossRepositories,
                 affinityDatabase):
        self.affinityDatabase = affinityDatabase
        self.defaultFlavor = defaultFlavor
        self.acrossRepositories = acrossRepositories
        if labelPath and not type(labelPath) == list:
            labelPath = [ labelPath ]

        self.labelPath = labelPath

        self.remaining = []
        self.query = {}
        for queryType in queryTypes:
            self.query[queryType] = getQueryClass(queryType)(defaultFlavor, 
                                                             labelPath, 
                                                             acrossRepositories)
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
               VERSION_STR_REVISION     : sortTroveVersion,
               VERSION_STR_TROVE_VER    : sortTroveVersion }

