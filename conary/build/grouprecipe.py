#
# Copyright (c) 2004-2006 rPath, Inc.
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
import copy
from itertools import chain, izip

from conary.build.recipe import Recipe, RECIPE_TYPE_GROUP
from conary.build.errors import RecipeFileError, GroupPathConflicts
from conary.build.errors import GroupDependencyFailure, GroupCyclesError
from conary.build.errors import GroupAddAllError
from conary.build import macros
from conary.build import use
from conary import conaryclient
from conary.deps import deps
from conary.lib import graph, log, util
from conary.repository import errors, trovesource
from conary import trove
from conary import versions


class _BaseGroupRecipe(Recipe):
    """ Defines a group recipe as collection of groups and provides
        operations on those groups.
    """
    ignore = 1
    def __init__(self):
        self.groups = {}
        self.defaultGroup = None

    def _addGroup(self, groupName, group):
        if groupName in self.groups:
            raise RecipeFileError, 'Group %s defined twice' % groupName
        self.groups[groupName] = group

    def _hasGroup(self, groupName):
        return groupName in self.groups

    def _getGroup(self, groupName):
        group = self.groups.get(groupName, None)
        if not group:
            raise RecipeFileError, "No such group '%s'" % groupName
        return group

    def _getGroups(self, groupName):
        if groupName is None:
            return [self.defaultGroup]
        elif isinstance(groupName, (list, tuple)):
            return [self._getGroup(x) for x in groupName]
        else:
            return [self._getGroup(groupName)]

    def _setDefaultGroup(self, group):
        self.defaultGroup = group

    def _getDefaultGroup(self):
        if not self.defaultGroup:
            return self.groups.get(self.name, None)
        return self.defaultGroup

    def iterGroupList(self):
        return self.groups.itervalues()

    def getGroupNames(self):
        return self.groups.keys()

    def getPrimaryGroupNames(self):
        """ 
        Return the list of groups in this GroupRecipe that are not included in 
        any other groups.
        """
        unseen = set(self.getGroupNames())

        for group in self.iterGroupList():
            unseen.difference_update([x[0] for x in group.iterNewGroupList()])
        return unseen




class GroupRecipe(_BaseGroupRecipe):
    """
        Provides the recipe interface for creating a group.
    """
    Flags = use.LocalFlags
    ignore = 1
    _recipeType = RECIPE_TYPE_GROUP

    depCheck = False
    autoResolve = False
    checkOnlyByDefaultDeps = True
    checkPathConflicts = True

    def __init__(self, repos, cfg, label, flavor, extraMacros={}):
        self.repos = repos
        self.cfg = cfg
        self.labelPath = [ label ]
        self.flavor = flavor
        self.macros = macros.Macros()
        self.macros.update(extraMacros)

        self.replaceSpecs = []

        _BaseGroupRecipe.__init__(self)
        group = self.createGroup(self.name, depCheck = self.depCheck, 
                         autoResolve = self.autoResolve, 
                         checkOnlyByDefaultDeps = self.checkOnlyByDefaultDeps,
                         checkPathConflicts = self.checkPathConflicts,
                         byDefault = True)
        self._setDefaultGroup(group)

    def _parseFlavor(self, flavor):
        assert(flavor is None or isinstance(flavor, str))
        if flavor is None:
            return None
        flavorObj = deps.parseFlavor(flavor)
        if flavorObj is None:
            raise ValueError, 'invalid flavor: %s' % flavor
        return flavorObj

    def Requires(self, requirement, groupName = None):
        for group in self._getGroups(groupName):
            group.addRequires(requirement)
    
    def add(self, name, versionStr = None, flavor = None, source = None,
            byDefault = None, ref = None, components = None, groupName = None):
        flavor = self._parseFlavor(flavor)
        for group in self._getGroups(groupName):
            group.addSpec(name, versionStr = versionStr, flavor = flavor,
                          source = source, byDefault = byDefault, ref = ref,
                          components = components)

    # maintain addTrove for backwards compatability
    addTrove = add

    def remove(self, name, versionStr = None, flavor = None, groupName = None):
        """ Remove a trove added to this group, either by an addAll
            line or by an addTrove line. 
        """
        flavor = self._parseFlavor(flavor)
        for group in self._getGroups(groupName):
            group.removeSpec(name, versionStr = versionStr, flavor = flavor)

    def removeComponents(self, componentList, groupName = None):
        if not isinstance(componentList, (list, tuple)):
            componentList = [ componentList ]
        for group in self._getGroups(groupName):
            group.removeComponents(componentList)

    def setByDefault(self, byDefault = True, groupName = None):
        for group in self._getGroups(groupName):
            group.setByDefault(byDefault)

    def addAll(self, name, versionStr = None, flavor = None, ref = None,
                                                            recurse=True, 
                                                            groupName = None):
        """ Add all of the troves directly contained in the given 
            reference to groupName.  For example, if the cooked group-foo 
            contains references to the troves 
            foo1=<version>[flavor] and foo2=<version>[flavor],
            the lines followed by
            r.addAll(name, versionStr, flavor)
            would be equivalent to you having added the addTrove lines
            r.add('foo1', <version>) 
            r.add('foo2', <version>) 
        """
        flavor = self._parseFlavor(flavor)
        for group in self._getGroups(groupName):
            group.addAll(name, versionStr, flavor, ref = ref, recurse = recurse)

    def addNewGroup(self, name, groupName = None, byDefault = True):
        if not self._hasGroup(name):
            raise RecipeFileError, 'group %s has not been created' % name

        for group in self._getGroups(groupName):
            group.addNewGroup(name, byDefault, explicit = True)

    def setDefaultGroup(self, groupName):
        self._setDefaultGroup(self._getGroup(groupName))

    def addReference(self, name, versionStr = None, flavor = None, ref = None):
        flavor = self._parseFlavor(flavor)
        return GroupReference(((name, versionStr, flavor),), ref)

    def replace(self, name, newVersionStr = None, newFlavor = None, ref = None, 
                groupName = None):
        newFlavor = self._parseFlavor(newFlavor)
        if groupName is None:
            self.replaceSpecs.append(((name, newVersionStr, newFlavor), ref))
        else:
            for group in self._getGroups(groupName):
                group.replaceSpec(name, newVersionStr, newFlavor, ref)

    def iterReplaceSpecs(self):
        return iter(self.replaceSpecs)

    def setLabelPath(self, *path):
        self.labelPath = [ versions.Label(x) for x in path ]

    def getLabelPath(self):
        return self.labelPath

    def getSearchFlavor(self):
        return self.flavor

    def getChildGroups(self, groupName):
        return [ (self._getGroup(x[0]), x[1], x[2]) for x in self._getGroup(groupName).iterNewGroupList() ]

    def createGroup(self, groupName, depCheck = False, autoResolve = False,
                    byDefault = None, checkOnlyByDefaultDeps = None,
                    checkPathConflicts = None):
        if self._hasGroup(groupName):
            raise RecipeFileError, 'group %s was already created' % groupName
        elif not groupName.startswith('group-'):
            raise RecipeFileError, 'group names must start with "group-"'

        origGroup = self._getDefaultGroup()
        if byDefault is None:
            byDefault = origGroup.byDefault

        if checkOnlyByDefaultDeps is None:
            checkOnlyByDefaultDeps = origGroup.checkOnlyByDefaultDeps

        newGroup = SingleGroup(groupName, depCheck, autoResolve, 
                                checkOnlyByDefaultDeps, 
                                checkPathConflicts, byDefault)
        self._addGroup(groupName, newGroup)
        return newGroup


class SingleGroup(object):
    def __init__(self, name, depCheck, autoResolve, checkOnlyByDefaultDeps,
                 checkPathConflicts, byDefault = True):        
        assert(isinstance(byDefault, bool))
        self.name = name
        self.depCheck = depCheck
        self.autoResolve = autoResolve
        self.checkOnlyByDefaultDeps = checkOnlyByDefaultDeps
        self.checkPathConflicts = checkPathConflicts
        self.byDefault = byDefault


        self.addTroveList = []
        self.removeTroveList = []
        self.removeComponentList = set()
        self.addReferenceList = []
        self.replaceTroveList = []
        self.newGroupList = {}

        self.requires = deps.DependencySet()

        self.troves ={}
        self.childTroves = {}
        self.size = None

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.name)

    def addRequires(self, requirement):
        self.requires.addDep(deps.TroveDependencies, 
                             deps.Dependency(requirement))

    def getRequires(self):
        return self.requires

    def addSpec(self, name, versionStr = None, flavor = None, source = None,
                byDefault = None, ref = None, components=None):
        self.addTroveList.append(((name, versionStr, flavor), source, 
                                 byDefault, ref, components)) 

    def removeSpec(self, name, versionStr = None, flavor = None):
        self.removeTroveList.append((name, versionStr, flavor))

    def removeComponents(self, componentList):
        self.removeComponentList.update(componentList)

    def replaceSpec(self, name, newVersionStr = None, newFlavor = None, 
                    ref = None):
        self.replaceTroveList.append(((name, newVersionStr, newFlavor), ref))

    def addAll(self, name, versionStr, flavor, ref, recurse):
        self.addReferenceList.append(((name, versionStr, flavor), ref, recurse))

    def getComponentsToRemove(self):
        return self.removeComponentList

    def iterAddSpecs(self):
        return iter(self.addTroveList)

    def iterRemoveSpecs(self):
        return iter(self.removeTroveList)

    def iterReplaceSpecs(self):
        return iter(self.replaceTroveList)

    def iterAddAllSpecs(self):
        return iter(self.addReferenceList)

    def addNewGroup(self, name, byDefault = None, explicit = True):
        if name in self.newGroupList:
            oldByDefault, oldExplicit = self.newGroupList[name]
            byDefault = oldByDefault or byDefault
            explicit = oldExplicit or explicit

        self.newGroupList[name] = (byDefault, explicit)

    def iterNewGroupList(self):
        for name, (byDefault, explicit) in self.newGroupList.iteritems():
            yield name, byDefault, explicit

    def hasNewGroup(self, name):
        return name in self.newGroupList

    def setByDefault(self, byDefault):
        self.byDefault = byDefault

    def getByDefault(self):
        return self.byDefault

    # below here are function used to get/set the troves found 
    #  

    def addTrove(self, troveTup, explicit, byDefault, components):
        assert(isinstance(byDefault, bool))

        if troveTup in self.troves:
            # if you add a trove twice, once as explicit and once 
            # as implict, make sure it stays explicit, same w/ 
            # byDefault.
            (oldExplicit, oldByDefault, oldComponents) = self.troves[troveTup]
            explicit = explicit or oldExplicit
            byDefault = byDefault or oldByDefault
            if oldComponents:
                components = components + oldComponents

        self.troves[troveTup] = (explicit, byDefault, components)

    def delTrove(self, name, version, flavor):
        (explicit, byDefault, comps) = self.troves[name, version, flavor]
        if explicit: 
            del self.troves[name, version, flavor]
        else:
            self.troves[name, version, flavor] = (False, False, comps)

    def setSize(self, size):
        self.size = size

    def getSize(self):
        return self.size

    def iterTroveList(self, strongRefs=False, weakRefs=False):
        if not (strongRefs or weakRefs):
            strongRefs = weakRefs = True

        for troveTup, (explicit, byDefault, comps) in self.troves.iteritems():
            if explicit and strongRefs:
                yield troveTup
            elif not explicit and weakRefs:
                yield troveTup

    def isExplicit(self, name, version, flavor):
        return self.troves[name, version, flavor][0]

    def includeTroveByDefault(self, name, version, flavor):
        return self.troves[name, version, flavor][1]

    def getComponents(self, name, version, flavor):
        return self.troves[name, version, flavor][2]

    def iterTroveListInfo(self):
        for troveTup, (explicit, byDefault, comps) in self.troves.iteritems():
            yield troveTup, explicit, byDefault, comps

    def iterDefaultTroveList(self):
        for troveTup, (explicit, byDefault, comps) in self.troves.iteritems():
            if byDefault:
                yield troveTup

    def hasTrove(self, name, version, flavor):
        return (name, version, flavor) in self.troves

    def isEmpty(self):
        return bool(not self.troves and not self.newGroupList)


class GroupReference:
    """ A reference to a set of troves, created by a trove spec, that 
        can be searched like a repository using findTrove.  Hashable
        by the trove spec(s) given.  Note the references can be 
        recursive -- This reference could be relative to another 
        reference, passed in as the upstreamSource.
    """
    def __init__(self, troveSpecs, upstreamSource=None):
        self.troveSpecs = troveSpecs
        self.upstreamSource = upstreamSource

    def __hash__(self):
        return hash((self.troveSpecs, self.upstreamSource))

    def findSources(self, repos, labelPath, flavorPath):
        """ Find the troves that make up this trove reference """
        if self.upstreamSource is None:
            source = repos
        else:
            source = self.upstreamSource

        results = source.findTroves(labelPath, self.troveSpecs, flavorPath)
        troveTups = [ x for x in chain(*results.itervalues())]
        self.sourceTups = troveTups
        self.source = trovesource.TroveListTroveSource(source, troveTups)
        self.source.searchAsRepository()

    def findTroves(self, *args, **kw):
        return self.source.findTroves(*args, **kw)

    def getTroves(self, *args, **kw):
        return self.source.getTroves(*args, **kw)

    def getSourceTroves(self):
        """ Returns the list of troves that form this reference 
            (without their children).
        """
        return self.getTroves(self.sourceTups, withFiles=False)


class TroveCache(dict):
    """ Simple cache for relevant information about troves needed for 
        recipes in case they are needed again for other recipes.
    """
    def __init__(self, repos):
        self.repos = repos
        self.troveInfo = {}
        
    def cacheTroves(self, troveTupList):
        troveTupList = [x for x in troveTupList if x not in self]
        if not troveTupList:
            return
        troves = self.repos.getTroves(troveTupList, withFiles=False)

        for troveTup, trv in izip(troveTupList, troves):
            isRedirect = trv.isRedirect()
            self[troveTup] = trv
            self.getChildren(troveTup, trv)

    def getChildren(self, troveTup, trv):
        """ Retrieve children,  and, if necessary, children's children)
            from repos.  Children's children should only be necessary 
            if the group doesn't have weak references (i.e. is old).
        """
        childTroves = []
        hasWeak = False
        
        childColls = []
        for childTup, byDefault, isStrong in trv.iterTroveListInfo():
            if not isStrong:
                hasWeak = True
            if trove.troveIsCollection(childTup[0]):
                childColls.append((childTup, byDefault, isStrong))

        # recursively cache these child troves.
        self.cacheTroves([x[0] for x in childColls])

        # FIXME: unforunately, there are a very few troves out there that
        # do not recursively descend when creating weak reference lists.
        # Since that's the case, we can't trust weak reference lists :/
        #if hasWeak:
        #    return

        newColls = []
        for childTup, byDefault, isStrong in childColls:

            childTrv = self[childTup]
            for childChildTup, childByDefault, _ in childTrv.iterTroveListInfo():
                # by this point, we can be sure that any collections
                # are recursively complete.
                # They should be trustable for the rest of the recipe.
                if not byDefault:
                    childByDefault = False
                if not isStrong and not trv.hasTrove(*childChildTup):
                    trv.addTrove(byDefault=childByDefault, 
                                 weakRef=True, *childChildTup)


    def getSize(self, troveTup):
        return self[troveTup].getSize()

    def isRedirect(self, troveTup):
        return self[troveTup].isRedirect()

    def iterTroveList(self, troveTup, strongRefs=False, weakRefs=False):
        for troveTup, byDefault, isStrong in self[troveTup].iterTroveListInfo():
            if isStrong:
                if strongRefs:
                    yield troveTup
            elif weakRefs:
                yield troveTup

    def iterTroveListInfo(self, troveTup):
        return(self[troveTup].iterTroveListInfo())

    def getPathHashes(self, troveTup):
        return self[troveTup].getPathHashes()

    def includeByDefault(self, troveTup, childTrove):
        return self[troveTup].includeTroveByDefault(*childTrove)


def buildGroups(recipeObj, cfg, repos):
    """ 
        Main function for finding, adding, and checking the troves requested
        for the the groupRecipe.
    """
    def _sortGroups(groupList):
        """
            Sorts groupList so that if group a includes group b, group b
            is before a in the returned list.  Also checks for cyclic group
            inclusion.
        """
        g = graph.DirectedGraph()

        groupsByName = {}
        
        for group in groupList:
            groupsByName[group.name] = group
            g.addNode(group.name)

            for childName, byDefault, explicit in group.iterNewGroupList():
                # this should ensure that the child is listed before
                # this group.
                g.addEdge(childName, group.name)

        cycles = [ x for x in g.getStronglyConnectedComponents() if len(x) > 1 ]
        if cycles:
            raise GroupCyclesError(cycles)

        return [ groupsByName[x] for x in g.getTotalOrdering() ]


    cache = TroveCache(repos)

    labelPath = recipeObj.getLabelPath()
    flavor = recipeObj.getSearchFlavor()

    # find all the groups needed for all groups in a few massive findTroves
    # calls.
    replaceSpecs = list(recipeObj.iterReplaceSpecs())
    troveMap = findTrovesForGroups(repos, recipeObj.iterGroupList(), 
                                   replaceSpecs,
                                   labelPath, flavor)
    troveTupList = list(chain(*chain(*(x.values() for x in troveMap.itervalues()))))
    cache.cacheTroves(troveTupList)

    groupsWithConflicts = {}

    newGroups = processAddAllDirectives(recipeObj, troveMap, cache, repos)

    groupList = _sortGroups(recipeObj.iterGroupList())

    for group in groupList:
        for (troveSpec, ref) in replaceSpecs:
            group.replaceSpec(*(troveSpec + (ref,)))

    for group in groupList:
        childGroups = recipeObj.getChildGroups(group.name)

        # check to see if any of our children groups have conflicts,
        # if so, we won't bother building up this group since it's 
        # bound to have a conflict as well.
        badGroup = False
        for childGroup, byDefault, isExplicit in childGroups:
            if childGroup.name in groupsWithConflicts:
                badGroup = True
                # mark this group as having a conflict
                groupsWithConflicts[group.name] = []
                break
        if badGroup:
            continue

        # add troves to this group.
        addTrovesToGroup(group, troveMap, cache, childGroups, repos)

        if group.autoResolve:
            resolveGroupDependencies(group, cache, cfg, 
                                     repos, labelPath, flavor)

        if group.depCheck:
            failedDeps = checkGroupDependencies(group, cfg)
            if failedDeps:
                raise GroupDependencyFailure(group.name, failedDeps)

        addPackagesForComponents(group, repos, cache)
        checkForRedirects(group, repos, cache)

        conflicts = calcSizeAndCheckHashes(group, cache)

        if conflicts:
            groupsWithConflicts[group.name] = conflicts

        if group.isEmpty():
            raise RecipeFileError('%s has no troves in it' % group.name)

    if groupsWithConflicts:
        raise GroupPathConflicts(groupsWithConflicts)



def findTrovesForGroups(repos, groupList, replaceSpecs, labelPath, 
                        searchFlavor):
    toFind = {}
    troveMap = {}

    for troveSpec, refSource in replaceSpecs:
        toFind.setdefault(refSource, set()).add(troveSpec)

    for group in groupList:
        for (troveSpec, source, byDefault, 
             refSource, components) in group.iterAddSpecs():
            toFind.setdefault(refSource, set()).add(troveSpec)

        for (troveSpec, ref, recurse) in group.iterAddAllSpecs():
            toFind.setdefault(ref, set()).add(troveSpec)

    results = {}

    for troveSource, troveSpecs in toFind.iteritems():
        if troveSource is None:
            source = repos
        else:
            source = troveSource
            troveSource.findSources(repos,  labelPath, searchFlavor),

        try:
            results[troveSource] = source.findTroves(labelPath, 
                                                     toFind[troveSource], 
                                                     searchFlavor)
        except errors.TroveNotFound, e:
            raise RecipeFileError, str(e)

    return results
    
def processAddAllDirectives(recipeObj, troveMap, cache, repos):
    for group in list(recipeObj.iterGroupList()):
        groupsByName = dict((x.name, x) for x in recipeObj.iterGroupList())
        for troveSpec, refSource, recurse in group.iterAddAllSpecs():
            for troveTup in troveMap[refSource][troveSpec]:
                processOneAddAllDirective(group, troveTup,  recurse, 
                                          recipeObj, cache, repos)
            

def processOneAddAllDirective(parentGroup, troveTup, recurse, recipeObj, cache,
                              repos): 
    topTrove = repos.getTrove(withFiles=False, *troveTup)

    if recurse:
        groupTups = [ x for x in topTrove.iterTroveList(strongRefs=True, 
                                                     weakRefs=True) \
                                        if x[0].startswith('group-') ]

        trvs = repos.getTroves(groupTups, withFiles=False)

        groupTrvDict = dict(izip(groupTups, trvs))

        if len(set(x[0] for x in groupTups)) != len(groupTups):
            raise GroupAddAllError(parentGroup, troveTup, groupTups)
        

    createdGroups = set()
    groupsByName = dict((x.name, x) for x in recipeObj.iterGroupList())
    
    stack = [(topTrove, parentGroup)]
    troveTups = []

    while stack:
        trv, parentGroup = stack.pop()
        for troveTup in trv.iterTroveList(strongRefs=True):
            byDefault = trv.includeTroveByDefault(*troveTup)

            if recurse and troveTup[0].startswith('group-'):
                name = troveTup[0]
                childGroup = groupsByName.get(name, None)
                if not childGroup:

                    childGroup = recipeObj.createGroup(
        name, 
        depCheck               = parentGroup.depCheck,
        autoResolve            = parentGroup.autoResolve,
        checkOnlyByDefaultDeps = parentGroup.checkOnlyByDefaultDeps,
        checkPathConflicts     = parentGroup.checkPathConflicts)

                    groupsByName[name] = childGroup


                parentGroup.addNewGroup(name, byDefault=byDefault, 
                                        explicit = True)

                if troveTup not in createdGroups:
                    stack.append((groupTrvDict[troveTup], childGroup))
                    createdGroups.add(troveTup)
            else:
                parentGroup.addTrove(troveTup, True, byDefault, [])
                troveTups.append(troveTup)

    cache.cacheTroves(troveTups)
 
 

def addTrovesToGroup(group, troveMap, cache, childGroups, repos):
    def _componentMatches(troveName, compList):
        return ':' in troveName and troveName.split(':', 1)[1] in compList

    # add explicit troves
    for (troveSpec, source, byDefault, 
         refSource, components) in group.iterAddSpecs():
        troveTupList = troveMap[refSource][troveSpec]

        if byDefault is None:
            byDefault = group.getByDefault()

        for troveTup in troveTupList:
            group.addTrove(troveTup, True, byDefault, components)

    # remove/replace explicit troves
    removeSpecs = list(group.iterRemoveSpecs())
    replaceSpecs = list(group.iterReplaceSpecs())
    if removeSpecs or replaceSpecs:
        groupAsSource = trovesource.GroupRecipeSource(repos, group)
        groupAsSource.searchAsDatabase()

        # remove troves 
        results = groupAsSource.findTroves(None, removeSpecs, allowMissing=True)

        troveTups = chain(*results.itervalues())
        for troveTup in troveTups:
            group.delTrove(*troveTup)

        # replace troves
        for troveSpec, ref in replaceSpecs:
            toRemove = dict(((x[0][0], None, None), x) for x in replaceSpecs)
            results = groupAsSource.findTroves(None, toRemove, 
                                                allowMissing=True)
            troveTups = chain(*results.itervalues())
            for troveTup in troveTups:
                byDefault = group.includeTroveByDefault(*troveTup)
                components = group.getComponents(*troveTup)

                group.delTrove(*troveTup)
                for newTup in troveMap[ref][troveSpec]:
                    group.addTrove(newTup, True, byDefault, [])

    # add implicit troves
    # first from children of explicit troves.
    componentsToRemove = group.getComponentsToRemove()
    for (troveTup, explicit, 
         byDefault, components) in list(group.iterTroveListInfo()):
        assert(explicit)

        for (childTup, byDefault, _) in cache.iterTroveListInfo(troveTup):
            childName = childTup[0]
            if componentsToRemove and _componentMatches(childName,
                                                        componentsToRemove):
                byDefault = False
                    
            if components:
                if _componentMatches(childName, components):
                    byDefault = True
                else:
                    byDefault = False

            group.addTrove(childTup, False, byDefault, [])

    # add implicit troves from new groups (added with r.addNewGroup())
    for childGroup, childByDefault, grpIsExplicit in childGroups:
        if grpIsExplicit:
            for (troveTup, explicit, childChildByDefault, comps) \
                                        in childGroup.iterTroveListInfo():
                childChildByDefault = childByDefault and childChildByDefault
                if childChildByDefault and componentsToRemove:
                    if _componentMatches(troveTup[0], componentsToRemove):
                        childChildByDefault = False

                group.addTrove(troveTup, False, childChildByDefault, [])

        for (childChildName, childChildByDefault, _) \
                                        in childGroup.iterNewGroupList():
            # we need to also keep track of what groups the groups we've
            # created include, so the weak references can be added 
            # to the trove.
            childChildByDefault = childByDefault and childChildByDefault
            group.addNewGroup(childChildName, childChildByDefault, 
                              explicit = False)


    # remove implicit troves
    if removeSpecs:
        groupAsSource = trovesource.GroupRecipeSource(repos, group)
        groupAsSource.searchAsDatabase()
        results = groupAsSource.findTroves(None, removeSpecs, allowMissing=True)

        troveTups = chain(*results.itervalues())
        for troveTup in findAllWeakTrovesToRemove(group, troveTups, cache):
            group.delTrove(*troveTup)

def findAllWeakTrovesToRemove(group, primaryErases, cache):
    # we only remove weak troves if either a) they are primary 
    # removes or b) they are referenced only by troves being removed
    primaryErases = list(primaryErases)
    toErase = set(primaryErases)
    seen = set()
    parents = {}

    troveQueue = util.IterableQueue()

    
    # create temporary parents info for all troves.  Unfortunately
    # we don't have this anywhere helpful like we do in the erase
    # on the system in conaryclient.update
    for troveTup in chain(group.iterTroveList(strongRefs=True), troveQueue):
        for childTup in cache.iterTroveList(troveTup, strongRefs=True):
            parents.setdefault(childTup, []).append(troveTup)
            if trove.troveIsCollection(childTup[0]):
                troveQueue.add(childTup)

    for troveTup in chain(primaryErases, troveQueue):
        # BFS through erase troves.  If any of the parents is not
        # also being erased, keep the trove.
        if not trove.troveIsCollection(troveTup[0]):
            continue

        for childTup in cache.iterTroveList(troveTup, strongRefs=True):
            if childTup in toErase:
                continue

            keepTrove = False
            for parentTup in parents[childTup]:
                # check to make sure there are no other references to this
                # trove that we're not erasing.  If there are, we want to
                # keep this trove.
                if parentTup == troveTup:
                    continue
                if parentTup not in toErase:
                    keepTrove = True
                    break

            if not keepTrove:
                toErase.add(childTup)
                troveQueue.add(childTup)
    return toErase
    

def checkForRedirects(group, repos, troveCache):
    redirectTups = []
    for troveTup in group.iterTroveList(strongRefs=True, weakRefs=False):
        if troveCache.isRedirect(troveTup):
           redirectTups.append(troveTup)

    if not redirectTups:
        return

    redirectTroves = repos.getTroves(redirectTups)
    missingTargets = {}
    for trv in redirectTroves:
        targets = []
        name = trv.getName()
        for (subName, subVersion, subFlavor) in trv.iterTroveList(
                                                            strongRefs=True):
            if (":" not in subName and ":" not in name) or \
               (":"     in subName and ":"     in name):
               targets.append((subName, subVersion, subFlavor))
            missing = [ x for x in targets if not group.hasTrove(*x) ]
            if missing:
                l = missingTargets.setdefault(trv, [])
                l += missing

    errmsg = []
    if not missingTargets:
        for troveTup in redirectTups:
            group.delTrove(*troveTup)
        return

    for trv in sorted(missingTargets):
        (n,v,f) = (trv.getName(),trv.getVersion(),trv.getFlavor())
        errmsg.append('\n%s=%s[%s]:' % (n, v.asString(),
                                        deps.formatFlavor(f)))
        errmsg.extend([(' -> %s=%s[%s]' % (n, v.asString(),
                                           deps.formatFlavor(f))) 
                            for (n,v,f) in sorted(missingTargets[trv])])
    raise RecipeFileError, ("""\
If you include a redirect in this group, you must also include the
target of the redirect.

The following troves are missing targets:
%s
""" % '\n'.join(errmsg))


def addPackagesForComponents(group, repos, troveCache):
    """
    Add the containing packages for any components added to group.
    Then switch the components to being implicit, but byDefault=True, while
    other non-specified components are byDefault=False. 
    """
    packages = {}

    for (n,v,f), explicit, byDefault, comps in group.iterTroveListInfo():
        if not explicit:
            continue
        if ':' in n:
            pkg = n.split(':', 1)[0]
            packages.setdefault((pkg, v, f), {})[n] = byDefault

    # if the user mentions both foo and foo:runtime, don't remove
    # direct link to foo:runtime
    troveTups = [ x for x in packages if not group.hasTrove(*x)]
    hasTroves = repos.hasTroves(troveTups)
    troveTups = [ x for x in troveTups if hasTroves[x] ]

    if not troveTups:
        return

    troveCache.cacheTroves(troveTups)

    for troveTup in troveTups:
        addedComps = packages[troveTup]

        byDefault = bool([x for x in addedComps.iteritems() if x[1]])
        group.addTrove(troveTup, True, byDefault, []) 

        for comp, byDefault, isStrong in troveCache.iterTroveListInfo(troveTup):
            if comp[0] in addedComps:
                byDefault = addedComps[comp[0]]
                # delete the strong reference to this trove, so that 
                # the trove can be added as a weak reference
                group.delTrove(*comp)
            else:
                byDefault = False
                

            group.addTrove(comp, False, byDefault, [])



def resolveGroupDependencies(group, cache, cfg, repos, labelPath, flavor):
    """ 
        Add in any missing dependencies to group
    """

    # set up configuration
    cfg = copy.deepcopy(cfg)
    cfg.dbPath  = ':memory:'
    cfg.root = ':memory:'
    cfg.installLabelPath = labelPath
    cfg.autoResolve = True
    cfg.flavor = [ flavor ]

    # set up a conaryclient to do the dep solving
    client = conaryclient.ConaryClient(cfg)

    if group.checkOnlyByDefaultDeps:
        troveList = list(group.iterDefaultTroveList())
    else:
        troveList = list(group.iterTroveList())
    
    # build a list of the troves that we're checking so far
    troves = [ (n, (None, None), (v, f), True) for (n,v,f) in troveList]

    # set verbosity to WARNING to avoid the conflicting meaning of the 
    # DEBUG flag in update code vs. cook code
    log.setVerbosity(log.WARNING)
    updJob, suggMap = client.updateChangeSet(troves, recurse = True,
                                             resolveDeps = True,
                                             test = True,
                                             checkPathConflicts=False)
    log.setVerbosity(log.DEBUG)

    for trove, needs in suggMap.iteritems():
        print "trove:%s" % trove[0]
        for item in needs:
            print "\t", item[0], item[1].trailingRevision()

    neededTups = list(chain(*suggMap.itervalues()))

    byDefault = group.getByDefault()
    for troveTup in neededTups:
        group.addTrove(troveTup, True, byDefault, [])

    cache.cacheTroves(neededTups)

        

def checkGroupDependencies(group, cfg):
    if group.checkOnlyByDefaultDeps:
        troveList = group.iterDefaultTroveList()
    else:
        troveList = group.iterTroveList()

    jobSet = [ (n, (None, None), (v, f), False) for (n,v,f) in troveList]

    cfg = copy.deepcopy(cfg)
    cfg.dbPath = ':memory:'
    cfg.root   = ':memory:'

    client = conaryclient.ConaryClient(cfg)
    if group.checkOnlyByDefaultDeps:
        cs = client.createChangeSet(jobSet, recurse = True, withFiles = False)
    else:
        cs = client.repos.createChangeSet(jobSet, recurse = True, 
                                          withFiles = False)

    jobSet = cs.getJobSet()
    trvSrc = trovesource.ChangesetFilesTroveSource(client.db)
    trvSrc.addChangeSet(cs, includesFileContents = False)
    failedDeps = client.db.depCheck(jobSet, trvSrc)[0]
    return failedDeps

def calcSizeAndCheckHashes(group, troveCache):
    def _getHashConflicts(group, troveCache):
        # afaict, this is just going to be slow no matter what I do.
        # I try to at least not have to iterate through any lists more
        # than once.
        allPathHashes = {}

        isColl = trove.troveIsCollection
        neededInfo = [x for x in group.iterTroveListInfo() \
                                if (x[1] or x[2]) and not isColl(x[0][0]) ]


        for (troveTup, explicit, byDefault, components) in neededInfo:
            if not byDefault:
                continue
            pathHashes = troveCache.getPathHashes(troveTup)
            if pathHashes is None:
                continue
            for pathHash in pathHashes:
                allPathHashes.setdefault(pathHash, []).append(troveTup)

        conflicts = set(tuple(x) for x in allPathHashes.itervalues() if len(x) > 1)
        return conflicts

    size = 0
    validSize = True

    implicit = []
    allPathHashes = []
    checkPathConflicts = group.checkPathConflicts

    # FIXME: perhaps this should be a config options?
    checkNotByDefaultPaths = False

    isColl = trove.troveIsCollection
    neededInfo = [ x for x in group.iterTroveListInfo() \
                            if (x[1] or x[2]) and not isColl(x[0][0]) ]

    troveCache.cacheTroves(x[0] for x in neededInfo)

    for troveTup, explicit, byDefault, comps in neededInfo:
        trvSize = troveCache.getSize(troveTup)
        if trvSize is None:
            validSize = False
            size = None
        elif validSize and byDefault:
            size += trvSize

        if checkPathConflicts:
            pathHashes = troveCache.getPathHashes(troveTup)
            allPathHashes.extend(pathHashes)

    group.setSize(size)

    if checkPathConflicts:
        pathHashCount = len(allPathHashes)
        allPathHashes = set(allPathHashes)
        if pathHashCount != len(allPathHashes):
            conflicts = _getHashConflicts(group, troveCache)
            return conflicts
