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
    Converts CM objects into TroveSet graphs.
    """

    SearchPathTroveSet = None

    FlattenAction = None
    RemoveAction = None

    FetchAction = troveset.FetchAction
    EraseFindAction = troveset.FindAction
    FindAction = troveset.FindAction
    InitialTroveTupleSet = troveset.StaticTroveTupleSet
    PatchAction = troveset.PatchAction
    UnionAction = troveset.UnionAction
    OptionalAction = troveset.OptionalAction
    UpdateAction = troveset.UpdateAction

    def __init__(self, flavor, repos, graph):
        self.flavor = flavor
        self.repos = repos
        self.g = graph

    def build(self, model, reposTroveSet, dbTroveSet):
        collections = set()
        for op in model.modelOps:
            if isinstance(op, model.SearchOperation):
                continue

            for troveTup in op:
                name = troveTup[0]
                if (isinstance(op, model.OfferTroveOperation) or
                    trove.troveIsComponent(name)):
                    collections.add(name.split(':')[0])
                elif trove.troveIsGroup(name):
                    collections.add(name)

        # this represents the path from "search" lines
        newSearchPath = []
        totalSearchSet = reposTroveSet
        rebuildTotalSearchSet = False
        # the "total search" searches the current troveset first, then the
        # search path. we only reset this when an operation changed the
        # working troveset in a way which would affect later operations,
        # after searchTroveSet chagnes
        # changed the current troveset in a way which a

        # finalTroveSet is the current working set of what's been selected
        # so far
        finalTroveSet = self.InitialTroveTupleSet(graph = reposTroveSet.g)

        for op in model.modelOps:
            if isinstance(op, model.SearchOperation):
                partialTup = op.item
                if isinstance(partialTup, versions.Label):
                    newSearchTroveSet = troveset.SearchSourceTroveSet(
                            searchsource.NetworkSearchSource(self.repos,
                                                             [ partialTup ],
                                                             self.flavor),
                            graph = reposTroveSet.g)
                    newSearchSet = newSearchTroveSet
                elif partialTup[0] is not None:
                    newSearchSet = reposTroveSet.find(partialTup)
                else:
                    assert(0)

                newSearchPath.insert(0, newSearchSet)
                rebuildTotalSearchSet = True
                continue

            searchSpecs = []
            localSpecs = []
            for troveSpec in op:
                if (troveSpec.version is not None and
                                    troveSpec.version[0] == '/'):
                    try:
                        verObj = versions.VersionFromString(troveSpec.version)
                        if verObj.isInLocalNamespace():
                            localSpecs.append(troveSpec)
                            continue

                    except (errors.VersionStringError, errors.ParseError):
                        pass

                searchSpecs.append(troveSpec)

            if isinstance(op, model.EraseTroveOperation):
                newMatches = []
                for spec in searchSpecs:
                    newMatches.append(
                        finalTroveSet._action(spec,
                                          ActionClass = self.EraseFindAction,
                                          index = op.getLocation(spec) ) )

                if len(newMatches) > 1:
                    eraseMatches = newMatches[0]._action(
                        ActionClass = self.UnionAction,
                        index = op.getLocation(), *newMatches[1:])
                else:
                    eraseMatches = newMatches[0]

                finalTroveSet = finalTroveSet._action(eraseMatches,
                        ActionClass=self.RemoveAction,
                        index = op.getLocation())
                continue

            if rebuildTotalSearchSet:
                totalSearchSet = self.SearchPathTroveSet( newSearchPath +
                                                           [ totalSearchSet ],
                                                         graph = self.g)
                newSearchPath = []
                rebuildTotalSearchSet = False

            if searchSpecs:
                newMatches = []
                for spec in searchSpecs:
                    newMatches.append(
                        totalSearchSet._action(spec,
                                               ActionClass = self.FindAction,
                                               index = op.getLocation(spec) ) )
                if len(newMatches) > 1:
                    searchMatches = newMatches[0]._action(
                        ActionClass = self.UnionAction,
                        index = op.getLocation(), *newMatches[1:])
                else:
                    searchMatches = newMatches[0]
            else:
                searchMatches = None

            if localSpecs:
                localMatches = dbTroveSet.find(*localSpecs)
            else:
                localMatches = None

            if searchMatches and localMatches:
                matches = searchMatches._action(localMatches,
                                                ActionClass = self.UnionAction,
                                                index = op.getLocation())
            elif searchMatches:
                matches = searchMatches
            else:
                matches = localMatches

            if isinstance(op, model.InstallTroveOperation):
                finalTroveSet = finalTroveSet._action(matches,
                                        ActionClass = self.UnionAction,
                                        index = op.getLocation())
            elif isinstance(op, model.PatchTroveOperation):
                finalTroveSet = finalTroveSet._action(matches,
                                        ActionClass = self.PatchAction,
                                        index = op.getLocation())
            elif isinstance(op, model.UpdateTroveOperation):
                finalTroveSet = finalTroveSet._action(matches,
                                        ActionClass = self.UpdateAction,
                                        index = op.getLocation())
            elif isinstance(op, model.OfferTroveOperation):
                finalTroveSet = finalTroveSet._action(matches,
                                        ActionClass = self.OptionalAction,
                                        index = op.getLocation())
            else:
                assert(0)

            newSearchPath.insert(0, matches)

            for troveSpec in op:
                if troveSpec.name in collections:
                    rebuildTotalSearchSet = True
                    break

        if newSearchPath:
            totalSearchSet = self.SearchPathTroveSet( newSearchPath +
                                                       [ totalSearchSet ],
                                                     graph = self.g)

        finalTroveSet.searchPath = totalSearchSet

        return finalTroveSet

