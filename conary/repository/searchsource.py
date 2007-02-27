#
# Copyright (c) 2004-2007 rPath, Inc.
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

from conary import trove
from conary import versions
from conary import errors as baseerrors

from conary.repository import changeset
from conary.repository import errors
from conary.repository import resolvemethod
from conary.repository import trovesource

class SearchSource(object):
    def __init__(self, source, flavor, db=None):
        source.searchWithFlavor()
        self.source = source
        self.db = db
        self.flavor = flavor

        for method in ('getTroveLeavesByLabel', 'getTroveVersionsByLabel',
                       'getTroveLeavesByBranch', 'getTroveVersionsByBranch',
                       'getTroveVersionFlavors', 'getMetadata'):
            if hasattr(source, method):
                setattr(self, method, getattr(source, method))

    def getTroveSource(self):
        return self.source

    def findTrove(self, troveSpec, useAffinity=False, **kw):
        res = self.findTroves([troveSpec], useAffinity=useAffinity, **kw)
        return res[troveSpec]

    def findTroves(self, troveSpecs, useAffinity=False, **kw):
        if useAffinity:
            kw['affinityDatabase'] = self.db
        return self.source.findTroves(None, troveSpecs, self.flavor, **kw)

    def resolveDependencies(self, label, depList):
        return self.source.resolveDependencies(label, depList)

    def hasTroves(self, troveList):
        return self.source.hasTroves(troveList)

    def getTrove(self, name, version, flavor, withFiles = True):
        trv = self.getTroves([(name, version, flavor)], withFiles=withFiles)[0]
        return trv

    def getTroves(self, troveList, *args, **kw):
        allowMissing = kw.pop('allowMissing', True)
        troves =  self.source.getTroves(troveList, *args, **kw)
        if allowMissing:
            return troves
        for idx, trove in enumerate(troves):
            if trove is None:
                raise errors.TroveMissing(troveList[idx][0], troveList[idx][1])
        return troves

    def createChangeSet(self, *args, **kw):
        return self.source.createChangeSet(*args, **kw)

    def getResolveMethod(self):
        m = resolvemethod.BasicResolutionMethod(None, self.db, self.flavor)
        m.setTroveSource(self.source)
        return m

    def iterFilesInTrove(self, *args, **kw):
        return self.source.iterFilesInTrove(*args, **kw)

    def getFileVersion(self, *args, **kw):
        return self.source.getFileVersion(*args, **kw)


class NetworkSearchSource(SearchSource):
    def __init__(self, repos, installLabelPath, flavor, db=None):
        SearchSource.__init__(self, repos, flavor, db)
        self.installLabelPath = installLabelPath

    def getResolveMethod(self):
        m =  resolvemethod.DepResolutionByLabelPath(None, self.db,
                                                      self.installLabelPath, 
                                                      self.flavor)
        m.setTroveSource(self.source)
        return m

    def findTroves(self, troveSpecs, useAffinity=False, allowMissing=False,
                   **kw):
        if useAffinity:
            kw['affinityDatabase'] = self.db
        return self.source.findTroves(self.installLabelPath, troveSpecs,
                                      self.flavor, allowMissing=allowMissing,
                                      **kw)


class TroveSearchSource(SearchSource):
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
        m = resolvemethod.DepResolutionByTroveList(None, self.db,
                                                   self.troveList,
                                                   self.flavor)
        m.setTroveSource(self.source)
        return m

class SearchSourceStack(trovesource.SourceStack):

    def findTrove(self, troveSpec, useAffinity=False, **kw):
        res = self.findTroves([troveSpec], useAffinity=useAffinity, **kw)
        return res[troveSpec]

    def findTroves(self, troveSpecs, useAffinity=False, allowMissing=False,
                    **kw):
        troveSpecs = list(troveSpecs)
        results = {}
        for source in self.sources[:1]:
            foundTroves = source.findTroves(troveSpecs, allowMissing=True)
            newTroveSpecs = []
            for troveSpec in troveSpecs:
                if troveSpec in foundTroves:
                    results[troveSpec] = foundTroves[troveSpec]
                else:
                    newTroveSpecs.append(troveSpec)
            troveSpecs = newTroveSpecs

        results.update(self.sources[-1].findTroves(troveSpecs,
                                              useAffinity=useAffinity,
                                              allowMissing=allowMissing, **kw))
        return results

    def getResolveMethod(self):
        return resolvemethod.stack(
                            [x.getResolveMethod() for x in self.sources])

def stack(*sources):
    """ create a trove source that will search first source1, then source2 """
    return SearchSourceStack(*sources)

def createSearchPathFromStrings(searchPath):
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

def createSearchPathSourceFromStrings(searchSource, searchPath, flavor,
                                      db=None):
    searchPath = createSearchPathFromStrings(searchPath)
    return createSearchPathSource(searchSource, searchPath, flavor, db)

def createSearchPathSource(searchSource, searchPath, flavor, db=None):
    troveSource = searchSource.getTroveSource()
    searchStack = SearchSourceStack()
    for item in searchPath:
        if not isinstance(item, (list, tuple)):
            item = [item]
        if isinstance(item[0], versions.Label):
            searchStack.addSource(NetworkSearchSource(troveSource,
                                                      item, flavor, db))
        elif isinstance(item[0], trove.Trove):
            s = TroveSearchSource(searchSource.getTroveSource(), item,
                                  flavor)
            searchStack.addSource(s)
        elif isinstance(item[0], (list, tuple)):
            if not isinstance(item[0][1], versions.Version):
                item = searchSource.findTroves(item)
                item = list(itertools.chain(*item.itervalues()))
            s = TroveSearchSource(searchSource.getTroveSource(), item,
                                  flavor)
            searchStack.addSource(s)
        else:
            raise baseerrors.ParseError('unknown search path item %s' % (item,))
    return searchStack

