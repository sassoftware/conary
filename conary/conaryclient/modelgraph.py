#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


from conary import errors, trove, versions
from conary.conaryclient import troveset
from conary.repository import searchsource

class AbstractModelCompiler(object):

    """
    Converts CM objects into TroveSet graphs.
    """

    SearchPathTroveSet = None

    FetchAction = troveset.FetchAction
    EraseFindAction = troveset.FindAction
    FindAction = troveset.FindAction
    InitialTroveTupleSet = troveset.StaticTroveTupleSet
    PatchAction = troveset.PatchAction
    RemoveAction = troveset.RemoveAction
    UnionAction = troveset.UnionAction
    OptionalAction = troveset.OptionalAction
    UpdateAction = troveset.UpdateAction
    IncludeAction = troveset.IncludeAction

    def __init__(self, flavor, repos, graph, reposTroveSet, dbTroveSet):
        self.flavor = flavor
        self.repos = repos
        self.g = graph
        self.reposTroveSet = reposTroveSet
        self.dbTroveSet = dbTroveSet

    def _splitFind(self, actionClass, searchSet, specList, op):
        if not specList:
            return None

        matches = []
        for spec in specList:
            matches.append(
                searchSet._action(spec, ActionClass = actionClass,
                                  index = op.getLocation(spec) ) )
        if len(matches) > 1:
            matchSet = matches[0]._action(ActionClass = self.UnionAction,
                                          index = op.getLocation(),
                                          *matches[1:])
        else:
            matchSet = matches[0]

        return matchSet

    def build(self, model):
        finalTroveSet = self.InitialTroveTupleSet(graph = self.g)
        return self.augment(model, self.reposTroveSet, finalTroveSet)

    def augment(self, model, totalSearchSet, finalTroveSet):
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
        rebuildTotalSearchSet = False
        # the "total search" searches the current troveset first, then the
        # search path. we only reset this when an operation changed the
        # working troveset in a way which would affect later operations,
        # after searchTroveSet chagnes
        # changed the current troveset in a way which a

        # finalTroveSet is the current working set of what's been selected
        # so far

        for op in model.modelOps:
            if isinstance(op, model.SearchOperation):
                partialTup = op.item
                if isinstance(partialTup, versions.Label):
                    newSearchTroveSet = troveset.SearchSourceTroveSet(
                            searchsource.NetworkSearchSource(self.repos,
                                                             [ partialTup ],
                                                             self.flavor),
                            graph = self.g)
                    newSearchSet = newSearchTroveSet
                elif partialTup[0] is not None:
                    newSearchSet = self.reposTroveSet.find(partialTup)
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
                eraseMatches = self._splitFind(self.EraseFindAction,
                                               finalTroveSet, searchSpecs, op)

                finalTroveSet = finalTroveSet._action(eraseMatches,
                        ActionClass=self.RemoveAction,
                        index = op.getLocation())
                continue

            if isinstance(op, model.IncludeOperation):
                # we need a complete total search set to pass into the sub
                # ops, since they have their compilation deferred
                rebuildTotalSearchSet = True

            if rebuildTotalSearchSet:
                totalSearchSet = self.SearchPathTroveSet( newSearchPath +
                                                           [ totalSearchSet ],
                                                         graph = self.g)
                newSearchPath = []
                rebuildTotalSearchSet = False

            searchMatches = self._splitFind(self.FindAction, totalSearchSet,
                                            searchSpecs, op)
            localMatches = self._splitFind(self.FindAction, self.dbTroveSet,
                                           localSpecs, op)

            if searchMatches and localMatches:
                matches = searchMatches._action(localMatches,
                                                ActionClass = self.UnionAction,
                                                index = op.getLocation())
            elif searchMatches:
                matches = searchMatches
            else:
                matches = localMatches

            if isinstance(op, model.IncludeOperation):
                assert(not localMatches)
                finalTroveSet = finalTroveSet._action(
                                matches, totalSearchSet,
                                compiler = self,
                                ActionClass = self.IncludeAction,
                                SearchPathClass = self.SearchPathTroveSet)
                totalSearchSet = finalTroveSet.finalSearchSet
                continue
            elif isinstance(op, model.InstallTroveOperation):
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
