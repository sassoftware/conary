#
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

from conary import errors, trove, versions
from conary.conaryclient import troveset
from conary.repository import searchsource

class AbstractModelCompiler(object):

    """
    Converts SystemModel objects into TroveSet graphs.
    """

    SearchPathTroveSet = None

    FlattenAction = None
    RemoveAction = None

    FetchAction = troveset.FetchAction
    InitialTroveTupleSet = troveset.StaticTroveTupleSet
    ReplaceAction = troveset.ReplaceAction
    UnionAction = troveset.UnionAction
    UpdateAction = troveset.UpdateAction

    def __init__(self, flavor, repos, graph):
        self.flavor = flavor
        self.repos = repos
        self.g = graph

    def build(self, sysModel, reposTroveSet, dbTroveSet):
        collections = set()
        for op in sysModel.systemItems:
            for troveTup in op:
                name = troveTup[0]
                if trove.troveIsComponent(name):
                    collections.add(name.split(':')[0])
                elif trove.troveIsGroup(name):
                    collections.add(name)

        # now build new search path elements
        #import epdb;epdb.serve()
        searchPathItems = []
        for searchItem in sysModel.searchPath:
            partialTup = searchItem.item
            if isinstance(partialTup, versions.Label):
                repos = troveset.SearchSourceTroveSet(
                        searchsource.NetworkSearchSource(self.repos,
                                                         [ partialTup ],
                                                         self.flavor))
                searchPathItems.append(repos)
            elif partialTup[0] is not None:
                result = self.repos.findTroves([],
                                              [ partialTup ], self.flavor,
                                              allowMissing = True)
                if not result:
                    raise errors.TroveSpecsNotFound( [ partialTup ] )
                result = result[partialTup]
                assert(len(result) == 1)
                ts = self.InitialTroveTupleSet(troveTuple = result,
                                               graph = self.g)
                # get the trove itself
                fetched = ts._action(ActionClass = self.FetchAction)
                flattened = fetched._action(ActionClass = self.FlattenAction)
                searchPathItems.append(flattened)
            else:
                assert(0)

        searchPathItems.append(reposTroveSet)

        searchPathTroveSet = self.SearchPathTroveSet(searchPathItems,
                                                     graph = self.g)
        searchTroveSet = searchPathTroveSet

        finalTroveSet = self.InitialTroveTupleSet(graph = searchTroveSet.g)
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
                matches = searchMatches._action(localMatches,
                                                ActionClass = self.UnionAction)
            elif searchMatches:
                matches = searchMatches
            else:
                matches = localMatches

            growSearchPath = True
            if isinstance(op, sysModel.InstallTroveOperation):
                finalTroveSet = finalTroveSet._action(matches,
                                        ActionClass = self.UnionAction)
            elif isinstance(op, sysModel.EraseTroveOperation):
                growSearchPath = False
                finalTroveSet = finalTroveSet._action(matches,
                                        ActionClass = self.RemoveAction)
            elif isinstance(op, sysModel.ReplaceTroveOperation):
                finalTroveSet = finalTroveSet._action(matches,
                                        ActionClass = self.ReplaceAction)
            elif isinstance(op, sysModel.UpdateTroveOperation):
                finalTroveSet = finalTroveSet._action(matches,
                                        ActionClass = self.UpdateAction)
            else:
                assert(0)

            if growSearchPath:
                growSearchPath = False
                for troveSpec in op:
                    if troveSpec.name in collections:
                        growSearchPath = True

                if growSearchPath:
                    flatten = matches._action(ActionClass =
                                                self.FlattenAction)
                    searchTroveSet = self.SearchPathTroveSet(
                            [ flatten, searchTroveSet ],
                            graph = searchTroveSet.g)

        finalTroveSet.searchPath = searchPathTroveSet

        return finalTroveSet

