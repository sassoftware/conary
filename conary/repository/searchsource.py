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
"""
    A SearchSource is a TroveSource + information about how to search it
    (using findTroves).  This allows the user to abstract away information
    about whether the trove source will work without an installLabelPath
    or not, what flavor to use, etc.

    It also makes it easier to stack sources (see TroveSourceStack findTroves
    for an example of the pain of stacking trove sources.).

    Finally a SearchSource is closely tied to a resolve method.  This resolve
    method resolves dependencies against the SearchSource.  A SearchSource
    stack that searches against a trove list first and then against
    an installLabelPath will have a resolve method that works the same way
    (see resolvemethod.py for implementation).

    Currently, there are 3 types of SearchSources.

        NetworkSearchSource(repos, installLabelPath, flavor, db=None)
        - searches the network on the given installLabelPath.

        TroveSearchSource(repos, troveList, flavor=None, db=None)
        - searches the given trove list.

        SearchSourceStack(*sources)
        - searches the sources in order.

    For all of these sources, you simply call findTroves(troveSpecs),
    without passing in flavor or installLabelPath.

    You can also create a searchSourceStack by calling 
    createSearchSourceStackFromStrings.
"""

import itertools

from conary import trove
from conary import versions
from conary import errors as baseerrors

from conary.repository import changeset
from conary.repository import errors
from conary.repository import resolvemethod
from conary.repository import trovesource

class AbstractSearchSource(object):
    # used for doing isinstance/issubclass checks.
    def getTroveSource(self):
        raise NotImplementedError

    def _filterSpecsForSource(self, troveSpecs):
        return [], dict(zip(troveSpecs, [[x] for x in troveSpecs]))

    def findTrove(self, troveSpec, useAffinity=False, **kw):
        raise NotImplementedError

    def findTroves(self, troveSpecs, useAffinity=False, **kw):
        raise NotImplementedError

    def getResolveMethod(self):
        raise NotImplementedError

class SearchSource(AbstractSearchSource):
    def __init__(self, source, flavor, db=None):
        source.searchWithFlavor()
        self.source = source
        self.db = db
        self.flavor = flavor
        self.installLabelPath = None

        # pass through methods that are valid in both the searchSource
        # and its underlying trove source.
        for method in ('getTroveLeavesByLabel', 'getTroveVersionsByLabel',
                       'getTroveLeavesByBranch', 'getTroveVersionsByBranch',
                       'getTroveVersionFlavors', 'getMetadata', 'hasTroves',
                       'createChangeSet', 'iterFilesInTrove', 'getFileVersion',
                       'getTrove', 'getTroves'):
            if hasattr(source, method):
                setattr(self, method, getattr(source, method))

    def getTroveSource(self):
        """
            Returns the source that this stack is wrapping, if there is one.
        """
        return self.source

    def findTrove(self, troveSpec, useAffinity=False, **kw):
        """
            Finds the trove matching the given (name, versionSpec, flavor)
            troveSpec.  If useAffinity is True, uses the associated database
            for branch/flavor affinity.
        """
        res = self.findTroves([troveSpec], useAffinity=useAffinity, **kw)
        return res[troveSpec]

    def findTroves(self, troveSpecs, useAffinity=False, **kw):
        """
            Finds the trove matching the given list of 
            (name, versionSpec, flavor) troveSpecs.  If useAffinity is True,
            uses the associated database for label/flavor affinity.
        """
        if useAffinity:
            kw['affinityDatabase'] = self.db
        return self.source.findTroves(self.installLabelPath, troveSpecs,
                                      self.flavor, **kw)

    def getResolveMethod(self):
        """
            Returns the dep resolution method
        """
        m = resolvemethod.BasicResolutionMethod(None, self.db, self.flavor)
        m.setTroveSource(self.source)
        return m


class NetworkSearchSource(SearchSource):
    """
        Search source using an installLabelPath.
    """
    def __init__(self, repos, installLabelPath, flavor, db=None,
                 resolveSearchMethod=resolvemethod.RESOLVE_ALL):
        SearchSource.__init__(self, repos, flavor, db)
        self.installLabelPath = installLabelPath
        self.resolveSearchMethod = resolveSearchMethod

    def _filterSpecsForSource(self, troveSpecs):
        troveSpecMap = {}
        rejected = []
        for name, versionStr, flavor in troveSpecs:
            labelStrs = self._getLabelsFromStr(versionStr)
            if not labelStrs:
                rejected.append((name, versionStr, flavor))
            else:
                for labelStr in labelStrs:
                    troveSpecMap.setdefault((name, labelStr, flavor), []).append(
                                                            (name, versionStr, flavor))
        return rejected, troveSpecMap

    def _getLabelsFromStr(self, versionStr):
        if not versionStr:
            return [versionStr]
        if not isinstance(versionStr, str):
            versionStr = str(versionStr)

        firstChar = versionStr[0]
        lastChar = versionStr[-1]
        if firstChar == '/':
            try:
                version = versions.VersionFromString(versionStr)
            except baseerrors.ParseError, e:
                raise errors.TroveNotFound, 'Error parsing version "%s": %s' % (versionStr, str(e))
            if isinstance(version, versions.Branch):
                label = version.label()
            else:
                label = version.trailingLabel()
            if label in self.installLabelPath:
                return [versionStr]
            else:
                return None
        if firstChar == '@':
            if '/' in versionStr:
                item, remainder = versionStr[1:].split('/')
                namespace, tag = item.split(':', 1)
                return [ '%s/%s' % (x, remainder) for x in self.installLabelPath
                         if (x.getNamespace(), x.getLabel()) == (namespace, tag) ]
            else:
                namespace, tag = versionStr[1:].split(':', 1)
                return [ str(x) for x in self.installLabelPath
                         if (x.getNamespace(), x.getLabel()) == (namespace, tag) ]
        if firstChar == ':':
            if '/' in versionStr:
                tag, remainder = versionStr[1:].split('/')
                return [ '%s/%s' % (x, remainder) for x in self.installLabelPath if x.getLabel() == tag ]
            else:
                tag = versionStr[1:]
                return [ str(x) for x in self.installLabelPath if x.getLabel() == tag ]
        elif lastChar == '@':
            host = versionStr[:-1]
            return [ str(x) for x in self.installLabelPath if x.getHost() == host ]
        elif '@' in versionStr:
            if '/' in versionStr:
                label, remainder = versionStr.split('/')
                return [ '%s/%s' % (x, remainder) for x in self.installLabelPath 
                         if str(x) == label ]
            return [ str(x) for x in self.installLabelPath if str(x) == versionStr ]
        # version/revision only are all ok - they don't modify the label we search on.
        return [ versionStr ]

    def getResolveMethod(self):
        """
            Resolves using the given installLabelPath.
        """
        searchMethod = self.resolveSearchMethod
        m =  resolvemethod.DepResolutionByLabelPath(None, self.db,
                                                    self.installLabelPath,
                                                    self.flavor,
                                                    searchMethod=searchMethod)
        m.setTroveSource(self.source)
        return m

class TroveSearchSource(SearchSource):
    """
        Search source using a list of troves.  Accepts either
        a list of trove tuples or a list of trove objects.
    """
    def __init__(self, troveSource, troveList, flavor=None, db=None):
        if not isinstance(troveList, (list, tuple)):
            troveList = [troveList]

        if troveList and not isinstance(troveList[0], trove.Trove):
            troveTups = troveList
            troveList = troveSource.getTroves(troveList, withFiles=False)
        else:
            troveTups = [ x.getNameVersionFlavor() for x in troveList ]
        troveSource = trovesource.TroveListTroveSource(troveSource, troveTups)
        troveSource.searchWithFlavor()
        SearchSource.__init__(self, troveSource, flavor, db)
        self.troveList = troveList

    def getResolveMethod(self):
        """
            Returns a dep resolution method that will resolve dependencies
            against these troves.
        """
        m = resolvemethod.DepResolutionByTroveList(None, self.db,
                                                   self.troveList,
                                                   self.flavor)
        m.setTroveSource(self.source)
        return m

class SearchSourceStack(trovesource.SourceStack, AbstractSearchSource):
    """
        Created by SearchSourceStack(*sources)

        Method for searching a stack of sources.  Call in the same way
        as a single searchSource:
            findTroves(troveSpecs, useAffinity=False)
    """
    def __init__(self, *args, **kw):
        trovesource.SourceStack.__init__(self, *args)
        AbstractSearchSource.__init__(self)
        self.resolveSearchMethod =  kw.pop('resolveSearchMethod',
                                           resolvemethod.RESOLVE_ALL)


    def getTroveSource(self):
        if len(self.sources) == 1:
            return self.sources[0].getTroveSource()
        return trovesource.stack(*[ x.getTroveSource() for x in self.sources])


    def findTrove(self, troveSpec, useAffinity=False, **kw):
        """
            Finds the trove matching the given (name, versionSpec, flavor)
            troveSpec.  If useAffinity is True, uses the associated database
            for branch/flavor affinity.
        """
        res = self.findTroves([troveSpec], useAffinity=useAffinity, **kw)
        return res[troveSpec]

    def findTroves(self, troveSpecs, useAffinity=False, allowMissing=False,
                    **kw):
        """
            Finds the trove matching the given list of
            (name, versionSpec, flavor) troveSpecs.  If useAffinity is True,
            uses the associated database for branch/flavor affinity.
        """
        troveSpecs = list(troveSpecs)
        reposSpecs = {}
        results = {}
        networkSource = None
        for source in self.sources:
            if isinstance(source, NetworkSearchSource):
                networkSource = source
            newTroveSpecs, specsToUse = source._filterSpecsForSource(troveSpecs)
            foundTroves = source.findTroves(specsToUse, allowMissing=True)
            for troveSpec in specsToUse:
                for origSpec in specsToUse[troveSpec]:
                    if troveSpec in foundTroves:
                        results.setdefault(origSpec, []).extend(foundTroves[troveSpec])
                    else:
                        newTroveSpecs.append(origSpec)
            troveSpecs = newTroveSpecs
        if troveSpecs:
            if networkSource:
                # All the explicit search sources are exhausted.  Fall back
                # to searching the repository without any label restrictions.
                results.update(networkSource.findTroves(troveSpecs,
                                                        useAffinity=useAffinity,
                                                        allowMissing=allowMissing,
                                                        **kw))
            elif not allowMissing:
                # search again with allowMissing=False to raise the appropriate
                # exception (only troves that weren't found before will be in
                # this list.
                results.update(self.sources[-1].findTroves(troveSpecs,
                                            useAffinity=useAffinity,
                                            allowMissing=False, **kw))
        return results

    def getResolveMethod(self):
        methods = []
        if self.resolveSearchMethod == resolvemethod.RESOLVE_LEAVES_FIRST:
            # special handling for resolveLeavesFirst stack:
            # first search only the leaves for _everything_
            # then go back and search the remainder.
            # If we just left this up to the individual resolveMethods
            # then for source [a,b,c] it would search a-leaves only
            # a-rest, b-leaves only, b-rest, where we want a-leaves, b-leaves,
            # c-leaves, etc.
            for source in self.sources:
                method = source.getResolveMethod()
                if hasattr(method, 'searchLeavesOnly'):
                    method.searchLeavesOnly()
                methods.append(method)
            for source in self.sources:
                method = source.getResolveMethod()
                if hasattr(method, 'searchLeavesOnly'):
                    method.searchAllVersions()
                    methods.append(method)
            return resolvemethod.stack(methods)
        else:
            return resolvemethod.stack(
                                [x.getResolveMethod() for x in self.sources])

def stack(*sources):
    """ create a search source that will search first source1, then source2 """
    return SearchSourceStack(*sources)

def createSearchPathFromStrings(searchPath):
    """
        Creates a list of items that can be passed into createSearchSource.

        Valid items in the searchPath include:
            1. troveSpec (foo=:devel)
            2. string for label (conary.rpath.com@rpl:devel)
            3. label objects or list of label objects.
    """
    from conary.conaryclient import cmdline
    from conary import conarycfg
    labelList = []
    finalPath = []
    if not isinstance(searchPath, (list, tuple)):
        searchPath = [searchPath]
    for item in searchPath:
        if isinstance(item, conarycfg.CfgLabelList):
            item = tuple(item)
        elif isinstance(item, versions.Label):
            labelList.append(item)
            continue
        elif isinstance(item, str):
            if '=' in item:
                # only troveSpecs have = in them
                item = [ cmdline.parseTroveSpec(item) ]
            elif '@' in item:
                try:
                    item = versions.Label(item)
                except baseerrors.ParseError, err:
                    raise baseerrors.ParseError(
                                            'Error parsing label "%s": %s' % (item, err))
                labelList.append(item)
                continue
            else:
                item = [cmdline.parseTroveSpec(item)]
        else:
            raise baseerrors.ParseError('Unknown searchPath item "%s"' % item)
        # labels don't get here, so we know that this is not part of a
        # labelPath
        if labelList:
            finalPath.append(labelList)
            labelList = []
        finalPath.append(item)
    if labelList:
        finalPath.append(tuple(labelList))
    return tuple(finalPath)

def createSearchSourceStackFromStrings(searchSource, searchPath, flavor,
                                       db=None, fallBackToRepos=True):
    """
        Creates a list of items that can be passed into createSearchSource.

        Valid items in the searchPath include:
            1. troveSpec (foo=:devel)
            2. string for label (conary.rpath.com@rpl:devel)
            3. label objects or list of label objects.
    """
    try:
        strings = searchPath
        searchPath = createSearchPathFromStrings(searchPath)
        return createSearchSourceStack(searchSource, searchPath, flavor, db,
                                       fallBackToRepos=fallBackToRepos)
    except baseerrors.ConaryError, err:
        raise baseerrors.ConaryError('Could not create search path "%s": %s' % (
                                     ' '.join(strings), err))

def createSearchSourceStack(searchSource, searchPath, flavor, db=None,
                            resolveLeavesFirst=True, troveSource=None, 
                            useAffinity=True, fallBackToRepos=True):
    """
        Creates a searchSourceStack based on a searchPath.

        Valid parameters include:
            * a label object
            * a trove tuple
            * a trove object
            * a list of any of the above.
    """
    if troveSource is None:
        troveSource = searchSource.getTroveSource()
    if resolveLeavesFirst:
        searchMethod = resolvemethod.RESOLVE_LEAVES_FIRST
    else:
        searchMethod = resolvemethod.RESOLVE_ALL
    searchStack = SearchSourceStack(
                    resolveSearchMethod=searchMethod)

    hasNetworkSearchSource = False
    for item in searchPath:
        if not isinstance(item, (list, tuple)):
            item = [item]
        if isinstance(item[0], versions.Label):
            searchStack.addSource(NetworkSearchSource(troveSource,
                                              item, flavor, db,
                                              resolveSearchMethod=searchMethod))
            hasNetworkSearchSource = True
        elif isinstance(item[0], trove.Trove):
            s = TroveSearchSource(searchSource.getTroveSource(), item, flavor)
            searchStack.addSource(s)
        elif isinstance(item[0], (list, tuple)):
            if not isinstance(item[0][1], versions.Version):
                item = searchSource.findTroves(item, useAffinity=useAffinity)
                item = list(itertools.chain(*item.itervalues()))
            s = TroveSearchSource(searchSource.getTroveSource(), item, flavor)
            searchStack.addSource(s)
        else:
            raise baseerrors.ParseError('unknown search path item %s' % (item,))
    if fallBackToRepos and not hasNetworkSearchSource:
        searchStack.addSource(NetworkSearchSource(troveSource, [], flavor, db,
                                                  resolveSearchMethod=searchMethod))
    return searchStack
