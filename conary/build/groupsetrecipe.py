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
#

import itertools, re

from conary import trove, versions
from conary.build import defaultrecipes, macros, use
from conary.build.errors import CookError
from conary.build.grouprecipe import _BaseGroupRecipe, _SingleGroup
from conary.build.recipe import loadMacros
from conary.conaryclient import troveset
from conary.conaryclient.resolve import PythonDependencyChecker
from conary.repository import netclient, searchsource
from conary.deps import deps

class GroupSetTroveCache(object):

    def __init__(self, groupRecipe, cache):
        self.cache = cache
        self.groupRecipe = groupRecipe
        self.depCache = {}

    def __getattr__(self, name):
        return getattr(self.cache, name)

    def iterTroveList(self, troveTup, strongRefs=False, weakRefs=False):
        raise NotImplementedError

    def iterTroveListInfo(self, troveTup):
        if isinstance(troveTup[1], versions.NewVersion):
            sg = self.groupRecipe._getGroup(troveTup[0])

            for x in sg.iterTroveListInfo():
                yield (x[0], x[2], x[1])

            for name, byDefault, explicit in sg.iterNewGroupList():
                yield (name, versions.NewVersion(),
                       self.groupRecipe.flavor), byDefault, explicit
        else:
            for x in self.cache.iterTroveListInfo(troveTup):
                yield x

class GroupActionData(troveset.ActionData):

    def __init__(self, troveCache, groupRecipe):
        troveset.ActionData.__init__(self, troveCache, groupRecipe.flavor)
        self.groupRecipe = groupRecipe

class GroupTupleSetMethods(object):

    _explainObjectName = 'TupleSet'

    def depsNeeded(self, resolveSource, failOnUnresolved = True):
        """
        troveset.depsNeeded(resolveSource, failOnUnresolved = True)

        Looks for unresolved dependencies in the trove set. Missing
        dependencies (and their dependencies) are looked for in
        the resolveSource. If there are unresolvable dependencies,
        an error is raised unless failOnUnresolved is specified.
        """
        if isinstance(resolveSource, troveset.SearchPathTroveSet):
            newList = []
            for ts in resolveSource.troveSetList:
                if isinstance(ts, troveset.TroveTupleSet):
                    ts = ts._action(ActionClass = troveset.FetchAction)
                newList.append(ts)

            resolveSource = troveset.SearchPathTroveSet(newList,
                                                        graph = self.g)
        elif isinstance(resolveSource, troveset.TroveTupleSet):
            resolveSource = resolveSource._action(
                                    ActionClass = troveset.FetchAction)

        fetched = self._action(ActionClass = troveset.FetchAction)

        return fetched._action(resolveSource,
                                     failOnUnresolved = failOnUnresolved,
                                     ActionClass = DepsNeededAction)

    def difference(self, other):
        """
        troveset.difference(other)
        trovset - other

        A new trove set is return which includes the members of the original
        set which are not in other. The install and optional split of
        other is irrelevent for deciding if a trove should be included
        in the result.
        """
        if type(other) == str:
            findSet = self.find(other)
            return self._action(findSet, ActionClass = GroupDifferenceAction,
                                edgeList = [ None, '-' ] )

        return self._action(other, ActionClass = GroupDifferenceAction)

    __sub__ = difference
    remove = difference

    def find(self, *troveSpecs):
        """
        troveset.find(troveSpec1, troveSpec2, ..., troveSpecN)
        troveset[findSpec]

        The troveset is searched for troves which match the given troveSpecs.
        The install/optional value is preserved from the troveset being
        searched.
        """
        return self._action(ActionClass = GroupFindAction, *troveSpecs)

    def findByName(self, namePattern, emptyOkay = False):
        """
        troveset.findByName(nameRegularExpression, emptyOkay = False)

        The troveset is searched for troves whose name matches
        nameRegularExpression. The install/optional value is preserved from the
        troveset being searched.
        """
        return self._action(namePattern, emptyOkay = emptyOkay,
                            ActionClass = FindByNameAction)

    __getitem__ = find

    def components(self, *componentList):
        """
        B{C{troveset.components()}} - Returns the components recursively
        included in all members of the troveset. Components of packages
        to be installed, which the package specifies should be installed,
        are in the return set as installable. All other components are
        marked as optional.

        SYNOPSIS
        ========

        C{troveset.components()}

        DESCRIPTION
        ===========


        PARAMETERS
        ==========

        None

        EXAMPLES
        ========

        C{repos['glibc'].components()}

        Returns the components of the default glibc found in the repos
        object.
        """
        return self._action(ActionClass = ComponentsAction, *componentList)

    def flatten(self):
        """
        troveset.flatten()

        The troveset returned consists of any trove referenced by the original
        troveset, directly or indirectly. The install/optioanl value is
        inherited from the troveset called.
        """
        return self._action(ActionClass = FlattenAction)

    def getInstall(self):
        """
        troveset.getInstall()

        A new troveset is returned which includes only the members of
        this troveset which are marked as install; optional members are
        omitted. All members of the returned set are marked as install.
        """
        return self._action(ActionClass = GetInstalledAction)

    def getOptional(self):
        """
        troveset.getOptional()

        A new troveset is returned which includes only the members of
        this troveset which are marked as optional; install members are
        omitted. All members of the returned set are marked as optional.
        """
        return self._action(ActionClass = GetOptionalAction)

    def isEmpty(self):
        """
        troveset.isEmpty()

        An exception is raised if the troveset contains any members; otherwise
        an identical troveset is created and returned (and may be ignored).
        """
        return self._action(ActionClass = IsEmptyAction)

    def isNotEmpty(self):
        """
        troveset.isEmpty()

        An exception is raised if the troveset contains no members; otherwise
        an empty troveset is returned (and may be ignored).
        """
        return self._action(ActionClass = IsNotEmptyAction)

    def makeInstall(self, installTroveSet = None):
        """
        troveset.makeInstall(installTroveSet = None)

        If a troveset is given as an argument, all members of the argument
        are included in the result as install members. Any members of this
        troveset which are optional, and are not in the installTroveSet,
        are also optional in the result.

        If installTroveSet is not passed, the return value includes all
        members of this troveset as install members.
        """
        return self._action(ActionClass = MakeInstallAction,
                            installTroveSet = installTroveSet)

    def makeOptional(self, optionalTroveSet = None):
        """
        troveset.makeOptional(optionalTroveSet = None)

        If a troveset is given as an argument, all members of the argument
        are included in the result as optional members. Any members of this
        troveset which are install, and are not in the optionalTroveSet,
        are also included to install in the result.

        If optionalTroveSet is not passed, the return value includes all
        members of this troveset as optional members.
        """
        return self._action(ActionClass = MakeOptionalAction,
                            optionalTroveSet = optionalTroveSet)

    def members(self):
        """
        troveset.members()

        All troves directly included by the troves in this troveset
        are returned as a new troveset. They are optional in the result
        only if they are optional in every member of this troveset which
        includes them.
        """
        return self._action(ActionClass = MembersAction)

    def packages(self, *packageList):
        """
        troveset.packages()

        Return all packages and filesets referenced directly or indirectly
        by this troveset. They are optional in the result only if they
        are optional in every member of this troveset which includes them.
        """
        return self._action(ActionClass = PackagesAction, *packageList)

    def union(self, *troveSetList):
        """
        troveset.union(other1, other2, ..., otherN)
        troveset + other1 + other2
        troveset | other1 | other2

        Return a troveset which includes all of the members of this trove
        as well as all of the members of the arguments. Troves are optional
        only if they are optional in all the trovesets they are part of.
        """
        return self._action(ActionClass = GroupUnionAction, *troveSetList)

    def replace(self, replaceSet):
        """
        troveset.replace(replaceSet)

        Look for items in this troveset (recursively) which can reasonably
        be replaced by members in replaceSet. The install/optional values
        are inherited from replaceSet. Any items in replaceSet which do not
        appear to replace members of this troveset are included as optioanl
        in the result. Members of this troveset which are outdated are
        included as optional in this group as well to prevent their
        installation.
        """
        return self._action(replaceSet, ActionClass = GroupReplaceAction)

    def update(self, updateSet):
        """
        troveset.replace(updateSet)

        Look for items in this troveset (recursively) which are updated
        by members in updateSet.
        """
        return self._action(updateSet, ActionClass = GroupUpdateAction)


    def createGroup(self, name, checkPathConflicts = True):
        """
        troveset.createGroup(name, checkPathConflicts = True)

        Create a new group in the repository whose members are defined
        by this troveset, and call it name (which must begin with group-).
        Returns a troveset which references the newly created group,
        allowing it to be included in other trovesets (and hence, groups).
        """
        return self._action(name, checkPathConflicts = checkPathConflicts,
                            ActionClass = CreateNewGroupAction)

    def _createGroup(self, name, checkPathConflicts = True):
        return self._action(name, ActionClass = CreateGroupAction,
                            checkPathConflicts = checkPathConflicts)

    __add__ = union
    __or__ = union

class GroupDelayedTroveTupleSet(GroupTupleSetMethods,
                                troveset.DelayedTupleSet):

    pass

class GroupSearchPathTroveSet(troveset.SearchPathTroveSet):

    def find(self, *troveSpecs):
        return self._action(ActionClass = GroupFindAction, *troveSpecs)

    __getitem__ = find

class GroupSearchSourceTroveSet(troveset.SearchSourceTroveSet):

    _explainObjectName = 'Repository'

    def find(self, *troveSpecs):
        """
        repos.find(troveSpec1, troveSpec2, ..., troveSpecN)
        repos[findSpec]

        The repository is searched for troves which match the given
        troveSpecs. All matches are returned in the install portion
        of the return value.
        """
        return self._action(ActionClass = GroupFindAction, *troveSpecs)

    __getitem__ = find

    def latestPackages(self):
        """
        Returns a troveset consisting of the latest packages and filesets on
        the default search label. The troves returned are those which best
        match the default flavor. Any troves which have a redirect as their
        latest version will be ignored.
        """
        return self._action(ActionClass = LatestPackagesFromSearchSourceAction)

class GroupFindAction(troveset.FindAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupDelayedTupleSetAction(troveset.DelayedTupleSetAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupDifferenceAction(troveset.DifferenceAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupUnionAction(troveset.UnionAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupReplaceAction(troveset.ReplaceAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupUpdateAction(troveset.UpdateAction):

    resultClass = GroupDelayedTroveTupleSet

class ComponentsAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, *componentNames):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.componentNames = set(componentNames)

    def __call__(self, data):
        installSet = set()
        optionalSet = set()

        for (troveTup), inInstall, explicit in \
                        self.primaryTroveSet._walk(data.troveCache):
            if not trove.troveIsComponent(troveTup[0]):
                continue

            componentName = troveTup[0].split(':')[1]
            if componentName in self.componentNames:
                if inInstall:
                    installSet.add(troveTup)
                else:
                    optionalSet.add(troveTup)

        self.outSet._setInstall(installSet)
        self.outSet._setOptional(optionalSet)

class CreateGroupAction(GroupDelayedTupleSetAction):

    prefilter = troveset.FetchAction

    def __init__(self, primaryTroveSet, name, checkPathConflicts = True):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.name = name
        self.checkPathConflicts = checkPathConflicts

    def __call__(self, data):
        grp = SG(data.groupRecipe.name,
                 checkPathConflicts = self.checkPathConflicts)
        data.groupRecipe._addGroup(self.name, grp)
        data.groupRecipe._setDefaultGroup(grp)

        self._create(data.groupRecipe.defaultGroup,
                     self.primaryTroveSet, self.outSet, data)

    def _create(self, sg, ts, outSet, data):
        sg.populate(ts, data.troveCache)

        outSet._setInstall([ (sg.name, versions.NewVersion(),
                              data.groupRecipe.flavor) ])
        outSet.realized = True

    def __str__(self):
        return self.name

class CreateNewGroupAction(CreateGroupAction):

    def __init__(self, primaryTroveSet, name, checkPathConflicts = True):
        CreateGroupAction.__init__(self, primaryTroveSet, name,
                                   checkPathConflicts = checkPathConflicts)

    def __call__(self, data):
        newGroup = SG(self.name, checkPathConflicts = self.checkPathConflicts)
        data.groupRecipe._addGroup(self.name, newGroup)
        self._create(newGroup, self.primaryTroveSet, self.outSet, data)

class DepsNeededAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, resolveTroveSet,
                 failOnUnresolved = True):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet,
                                            resolveTroveSet)
        self.failOnUnresolved = failOnUnresolved
        self.resolveTroveSet = resolveTroveSet

    def __call__(self, data):
        checker = PythonDependencyChecker(
                        data.troveCache,
                        ignoreDepClasses = [ deps.AbiDependency,
                                             deps.RpmLibDependencies ])

        troveList = []
        for (troveTuple, isInstall, isExplicit) in \
                    self.primaryTroveSet._walk(data.troveCache,
                                newGroups = False, recurse = True):
            if isInstall:
                troveList.append(troveTuple)

        jobSet = [ (n, (None, None), (v, f), False) for (n,v,f) in troveList ]

        checker.addJobs(jobSet)
        resolveMethod = (self.resolveTroveSet._getResolveSource().
                                    getResolveMethod())

        failedDeps, suggMap = checker.resolve(resolveMethod)

        if self.failOnUnresolved and failedDeps:
            raise CookError("Unresolved Deps:\n" +
                "\n".join(
                [ "\t%s=%s[%s] requires %s" % (name, version, flavor, dep)
                  for ((name, version, flavor), dep) in failedDeps ]))

        installSet = set()
        for requiredBy, requiredSet in suggMap.iteritems():
            installSet.update(requiredSet)

        self.outSet._setInstall(installSet)

class GetInstalledAction(GroupDelayedTupleSetAction):

    def __call__(self, data):
        self.outSet._setInstall(self.primaryTroveSet._getInstallSet())

class GetOptionalAction(GroupDelayedTupleSetAction):

    def __call__(self, data):
        self.outSet._setOptional(self.primaryTroveSet._getOptionalSet())

class FindByNameAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, namePattern, emptyOkay = False):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.namePattern = namePattern
        self.emptyOkay = emptyOkay

    def __call__(self, data):

        def _gather(troveTupleSet, nameRegex):
            s = set()
            for troveTup in troveTupleSet:
                if nameRegex.match(troveTup[0]):
                    s.add(troveTup)

            return s

        r = re.compile(self.namePattern + '\\Z')
        install = _gather(self.primaryTroveSet._getInstallSet(), r)
        self.outSet._setInstall(install)
        optional = _gather(self.primaryTroveSet._getOptionalSet(), r)
        self.outSet._setOptional(optional)

        if (not self.emptyOkay and not install and not optional):
            raise CookError("findByName() matched no trove names")

class IsEmptyAction(GroupDelayedTupleSetAction):

    def __call__(self, data):
        if (self.primaryTroveSet._getInstallSet() or
            self.primaryTroveSet._getOptionalSet()):

            raise CookError("Trove set is not empty")

        # self.outSet is already empty

class IsNotEmptyAction(GroupDelayedTupleSetAction):

    def __call__(self, data):
        if (not self.primaryTroveSet._getInstallSet() and
            not self.primaryTroveSet._getOptionalSet()):

            raise CookError("Trove set is empty")

        self.outSet._setInstall(self.primaryTroveSet._getInstallSet())
        self.outSet._setOptional(self.primaryTroveSet._getOptionalSet())

class LatestPackagesFromSearchSourceAction(GroupDelayedTupleSetAction):

    def __call__(self, data):
        troveSource = self.primaryTroveSet.searchSource.getTroveSource()

        # data hiding? what's that
        flavor = self.primaryTroveSet.searchSource.flavor
        labelList = self.primaryTroveSet.searchSource.installLabelPath

        d = { None : {} }
        for label in labelList:
            d[None][label] = [ flavor ]

        matches = troveSource.getTroveLatestByLabel(
                                d, troveTypes = netclient.TROVE_QUERY_NORMAL,
                                bestFlavor = True)

        fullTupList = []
        for name in matches:
            if not (trove.troveIsPackage(name) or trove.troveIsFileSet(name)):
                continue

            for version in matches[name]:
                for flavor in matches[name][version]:
                    fullTupList.append( (name, version, flavor) )

        sourceNames = data.troveCache.getTroveInfo(
                                trove._TROVEINFO_TAG_SOURCENAME, fullTupList)
        bySource = {}
        for sourceName, troveTup in itertools.izip(sourceNames, fullTupList):
            bySource.setdefault(sourceName(), []).append(troveTup)

        resultTupList = []
        for sourceName, tupList in bySource.iteritems():
            if len(sourceName) > 2:
                mostRecent = sorted([ x[1] for x in tupList ])[-1]
                resultTupList += [ x for x in tupList if x[1] == mostRecent ]
            else:
                resultTupList += tupList

        self.outSet._setInstall(resultTupList)

class MakeInstallAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, installTroveSet = None):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet,
                                            installTroveSet)
        self.installTroveSet = installTroveSet

    def __call__(self, data):
        if self.installTroveSet:
            self.outSet._setOptional(self.primaryTroveSet._getOptionalSet())
            self.outSet._setInstall(
                    (self.installTroveSet._getInstallSet() |
                     self.installTroveSet._getOptionalSet()))
        else:
            self.outSet._setInstall(self.primaryTroveSet._getInstallSet() |
                                    self.primaryTroveSet._getOptionalSet())

class MakeOptionalAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, optionalTroveSet = None):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet,
                                            optionalTroveSet)
        self.optionalTroveSet = optionalTroveSet

    def __call__(self, data):
        if self.optionalTroveSet:
            self.outSet._setInstall(self.primaryTroveSet._getInstallSet())
            self.outSet._setOptional(
                    (self.optionalTroveSet._getInstallSet() |
                     self.optionalTroveSet._getOptionalSet()))
        else:
            self.outSet._setOptional(self.primaryTroveSet._getInstallSet() |
                                     self.primaryTroveSet._getOptionalSet())

class MembersAction(GroupDelayedTupleSetAction):

    prefilter = troveset.FetchAction
    justStrong = True

    def __call__(self, data):
        for (troveTuple, installSet) in itertools.chain(
                itertools.izip(self.primaryTroveSet._getInstallSet(),
                               itertools.repeat(True)),
                itertools.izip(self.primaryTroveSet._getOptionalSet(),
                               itertools.repeat(False))):
            installs = []
            available = []

            for (refTrove, byDefault, isStrong) in \
                        data.troveCache.iterTroveListInfo(troveTuple):
                if self.justStrong and not isStrong:
                    continue

                if byDefault:
                    installs.append(refTrove)
                elif not byDefault:
                    available.append(refTrove)

            self.outSet._setInstall(installs)
            self.outSet._setOptional(available)

class FlattenAction(MembersAction):

    justStrong = False

class PackagesAction(GroupDelayedTupleSetAction):

    prefilter = troveset.FetchAction

    def __init__(self, primaryTroveSet):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)

    def __call__(self, data):
        installSet = set()
        optionalSet = set()

        for (troveTup), inInstall, explicit in \
                        self.primaryTroveSet._walk(data.troveCache,
                                                   newGroups = False,
                                                   recurse = True):

            if (not trove.troveIsPackage(troveTup[0]) and
                not trove.troveIsFileSet(troveTup[0])):
                continue

            if inInstall:
                installSet.add(troveTup)
            else:
                optionalSet.add(troveTup)

        self.outSet._setInstall(installSet)
        self.outSet._setOptional(optionalSet)

class SG(_SingleGroup):

    def __init__(self, *args, **kwargs):
        _SingleGroup.__init__(self, *args, **kwargs)
        self.autoResolve = False
        self.depCheck = False
        self.imageGroup = False

    def populate(self, troveSet, troveCache):
        seen = set()

        for troveTup, byDefault, explicit in troveSet._walk(troveCache):
            if not explicit:
                continue

            seen.add(troveTup)
            if isinstance(troveTup[1], versions.NewVersion):
                self.addNewGroup(troveTup[0], byDefault = byDefault,
                                 explicit = True)
            else:
                self.addTrove(troveTup, explicit = True, byDefault = byDefault,
                              components = [])

        for troveTup, byDefault, explicit in troveSet._walk(troveCache,
                                                            recurse = True):
            if troveTup in seen:
                # if it's explicit, it's already been seen
                continue

            seen.add(troveTup)

            if isinstance(troveTup[1], versions.NewVersion):
                self.addNewGroup(troveTup[0], byDefault = byDefault,
                                 explicit = False)
            else:
                self.addTrove(troveTup, explicit = False, byDefault = byDefault,
                              components = [])

    def iterAddSpecs(self):
        return []

    def iterAddAllSpecs(self):
        return []

    def iterReplaceSpecs(self):
        return []

    def iterDifferenceSpecs(self):
        return []

    def iterNewGroupDifferenceList(self):
        return []

    def iterCopiedFrom(self):
        return []

    def getComponentsToMove(self):
        return []

    def getRequires(self):
        return deps.DependencySet()

class _GroupSetRecipe(_BaseGroupRecipe):

    Flags = use.LocalFlags
    internalAbstractBaseClass = 1

    def __init__(self, repos, cfg, label, flavor, laReposCache, srcdirs=None,
                 extraMacros={}, lightInstance = False):

        klass = self._getParentClass('_BaseGroupRecipe')
        klass.__init__(self, laReposCache = laReposCache,
                       srcdirs = srcdirs,
                       lightInstance = lightInstance,
                       cfg = cfg)

        self.troveSource = repos
        self.repos = repos

        self.labelPath = [ label ]
        self.buildLabel = label
        self.flavor = flavor
        self.searchSource = searchsource.NetworkSearchSource(
                repos, self.labelPath, flavor)
        self.macros = macros.Macros(ignoreUnknown=lightInstance)
        self.world = GroupSearchSourceTroveSet(self.searchSource)
        self.g = troveset.OperationGraph()

        baseMacros = loadMacros(cfg.defaultMacros)
        self.macros.update(baseMacros)
        for key in cfg.macros:
            self.macros._override(key, cfg['macros'][key])
        self.macros.name = self.name
        self.macros.version = self.version
        if '.' in self.version:
            self.macros.major_version = '.'.join(self.version.split('.')[0:2])
        else:
            self.macros.major_version = self.version
        if extraMacros:
            self.macros.update(extraMacros)

    def _realizeGraph(self, cache, callback):
        data = GroupActionData(troveCache = GroupSetTroveCache(self, cache),
                               groupRecipe = self)
        self.g.realize(data)

    def getLabelPath(self):
        return self.labelPath

    def getSearchFlavor(self):
        return self.flavor

    def iterReplaceSpecs(self):
        return []

    def getResolveTroveSpecs(self):
        return []

    def getChildGroups(self, groupName = None):
        return []

    def getGroupMap(self, groupName = None):
        return {}

    def _getSearchSource(self):
        return self.troveSource

    def getSearchPath(self):
        return [ ]

    def writeDotGraph(self, path):
        self.g.generateDotFile(path, edgeFormatFn = lambda a,b,c: c)

    def Group(self, ts, checkPathConflicts = True):
        """
        r.Group(troveSet, checkPathConflicts = True)

        Set the passed trove set as the contents of the primary group
        being built. The return value is a trove set which references
        the primary group (which can be used to create other groups
        which reference the primary group).
        """
        return ts._createGroup(self.name,
                               checkPathConflicts = checkPathConflicts)

    def Repository(self, labelList, flavor):
        if type(labelList) == tuple:
            labelList = list(tuple)
        elif type(labelList) != list:
            labelList = [ labelList ]

        for i, label in enumerate(labelList):
            if type(label) == str:
                labelList[i] = versions.Label(label)
            elif not isinstance(label, versions.Label):
                raise CookError("String label or Label object expected")

        if type(flavor) == str:
            flavor = deps.parseFlavor(flavor)

        searchSource = searchsource.NetworkSearchSource(
                                            self.repos, labelList, flavor)
        return GroupSearchSourceTroveSet(searchSource, graph = self.g)

    def SearchPath(self, *troveSets):
        """
        Build an object which searches multiple other objects in the
        order specified. Troves can be looked up in the result, and the
        result can also be used for resolving dependencies.
        """
        return GroupSearchPathTroveSet(troveSets, graph = self.g)

from conary.build.packagerecipe import BaseRequiresRecipe
exec defaultrecipes.GroupSetRecipe
