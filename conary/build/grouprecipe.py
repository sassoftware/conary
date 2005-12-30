#
# Copyright (c) 2004-2005 rPath, Inc.
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
from itertools import chain, izip
import tempfile

from conary.build.recipe import Recipe, RECIPE_TYPE_GROUP
from conary.build.errors import RecipeFileError
from conary.build import macros
from conary.build import use
from conary import conaryclient
from conary.deps import deps
from conary.lib import log, util
from conary.repository import errors, trovesource
from conary import versions

class SingleGroup:

    def add(self, name, versionStr = None, flavor = None, source = None,
            byDefault = None, ref = None, components=None):
        self.addTroveList.append((name, versionStr, flavor, source, 
				  byDefault, ref, components)) 
    # maintain addTrove for backwards compat.
    addTrove = add
    
    def remove(self, name, versionStr = None, flavor = None):
        self.removeTroveList.append((name, versionStr, flavor))

    def removeComponents(self, componentList):
        self.removeComponentList.update(componentList)

    def addAll(self, reference, byDefault = None):
        self.addReferenceList.append((reference, byDefault))

    def addNewGroup(self, name, byDefault = None):
	self.newGroupList.append([ name, byDefault ])

    def setByDefault(self, byDefault):
        assert(isinstance(byDefault, bool))
	self.byDefault = byDefault

    def findTroves(self, troveMap, repos, childGroups):
        self._findTroves(troveMap)
        self._removeTroves(repos)
        self._addImplicitTroves(childGroups)
        self._removeImplicitTroves(repos)
        self._checkForRedirects(repos)

    def autoResolveDeps(self, cfg, repos, labelPath):
        if self.autoResolve:
            self._resolveDependencies(cfg, repos, labelPath)

    def checkDependencies(self, cfg):
        if self.depCheck:
            failedDeps = self._checkDependencies(cfg)
            if failedDeps:
                return failedDeps

    def calcSize(self):
        self.size = 0
        validSize = True
        for (n,v,f), (explicit, size, byDefault) in self.troves.iteritems():
            l = self.troveVersionFlavors.setdefault(n,[])
            l.append((v,f,byDefault,explicit))
            if not explicit:
                continue
            if size is None:
                validSize = False
                self.size = None
            if validSize:
                self.size += size

    def _findTroves(self, troveMap):
        """ given a trove map which already contains a dict for all queries
            needed for all groups cooked, pick out those troves that 
            are relevant to this group.
        """
        validSize = True

        for (name, versionStr, flavor, source, 
                byDefault, refSource, components) in self.addTroveList:
            troveList = troveMap[refSource][name, versionStr, flavor]

            if byDefault is None:
                byDefault = self.byDefault
            
            for (troveTup, size, isRedirect, childTroves) in troveList:
                self._foundTrove(troveTup, True, byDefault, isRedirect, size,
                                 components, childTroves)


        # these are references which were used in addAll() commands
        for refSource, byDefault in self.addReferenceList:
            srcTroveList = refSource.getSourceTroves()

            # get byDefault dict for troves 
            byDefaultDict = []
            for sourceTrove in srcTroveList:
                byDefaultDict += [ x for x in 
                                   sourceTrove.iterTroveList(strongRefs=True,
                                                             weakRefs=True)
                                   if sourceTrove.includeTroveByDefault(*x) ]

            troveTups = [ x for x in chain(
                    *[x.iterTroveList(strongRefs=True) for x in srcTroveList])]
            troveList = refSource.getTroves(troveTups, withFiles=False)

            byDefaultDict = set(byDefaultDict)

            if byDefault is None:
                byDefault = self.byDefault

            for (troveTup, trv) in izip(troveTups, troveList):
                if trv.isRedirect():
                    childTroves = []
                else:
                    childTroves = [ (x, x in byDefaultDict)
                                     for x in trv.iterTroveList(weakRefs=True, 
                                                            strongRefs=True) ]
                self._foundTrove(troveTup, True, byDefault, trv.isRedirect(),
                                 trv.getSize(), [], childTroves)



    def _foundTrove(self, troveTup, explicit, byDefault, 
                    isRedirect=False, size=None, components=None, 
                    childTroves=None):
        if explicit:
            if isRedirect:
                # we check later to ensure that all redirects added 
                # by addTrove lines (or other means) are removed
                # by removeTrove lines later.
                self.redirects.add(troveTup)
            
        info =  self.troves.get(troveTup, None)
        if info:
            self.troves[troveTup] = (info[0] or explicit, 
                                     info[1] or size, info[2] or byDefault)
            if childTroves is not None:
                # FIXME: childTrove byDefault information should be merged
                self.childTroves[troveTup] = (components, childTroves)
        else:
            self.troves[troveTup] = (explicit, size, byDefault)
            if childTroves is not None:
                self.childTroves[troveTup] = (components, childTroves)

    def getDefaultTroves(self):
        return [ x[0] for x in self.troves.iteritems() if x[1][2] ]

    def _resolveDependencies(self, cfg, repos, labelPath):
        """ adds the troves needed to to resolve all open dependencies 
            in this group.  Will raise an error if not all dependencies
            can be resolved.  
        """
        #FIXME: this should probably be able to resolve against
        # other trove source than the repository.

        # set up configuration
        oldDbPath = cfg.dbPath
        cfg.setValue('dbPath', ':memory:')
        oldRoot = cfg.root
        tmpDir = tempfile.mkdtemp()
        cfg.setValue('root', tmpDir)

        oldInstallLabelPath = cfg.installLabelPath
        resolveLabelPath = labelPath
        cfg.installLabelPath = labelPath
        oldAutoResolve = cfg.autoResolve
        cfg.autoResolve = True
        oldFlavor = cfg.flavor
        cfg.flavor = [ cfg.buildFlavor ]
        # set up a conaryclient to do the dep solving
        client = conaryclient.ConaryClient(cfg)

        if self.checkOnlyByDefaultDeps:
            troveList = self.getDefaultTroves()
        else:
            troveList = list(self.troves) 
        
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

        # restore config
        cfg.setValue('dbPath', oldDbPath)
        cfg.setValue('root', oldRoot)
        util.rmtree(tmpDir)

        cfg.installLabelPath = oldInstallLabelPath
        cfg.autoResolve = oldAutoResolve
        cfg.flavor = oldFlavor
        for trove, needs in suggMap.iteritems():
            print "trove:%s" % trove[0]
            for item in needs:
                print "\t", item[0], item[1].trailingRevision()

        neededTups = set(chain(*suggMap.itervalues()))
        troves = repos.getTroves(neededTups, withFiles=False)
        for troveTup, trv in izip(neededTups, troves):
            self._foundTrove(troveTup, True, self.byDefault,
                             trv.isRedirect(), trv.getSize(), [], [])
            
    def _checkDependencies(self, cfg):
        if self.checkOnlyByDefaultDeps:
            troveList = self.getDefaultTroves()
        else:
            troveList = list(self.troves)

        jobSet = [ (n, (None, None), (v, f), False) for (n,v,f) in troveList]

        oldDbPath = cfg.dbPath
        cfg.setValue('dbPath', ':memory:')
        oldRoot = cfg.root
        cfg.setValue('root', ':memory:')

        client = conaryclient.ConaryClient(cfg)
        if self.checkOnlyByDefaultDeps:
            cs = client.createChangeSet(jobSet, 
                                              recurse = True, withFiles=False)
        else:
            cs = client.repos.createChangeSet(jobSet, recurse = True, 
                                              withFiles=False)

        jobSet = cs.getJobSet()
        trvSrc = trovesource.ChangesetFilesTroveSource(client.db)
        trvSrc.addChangeSet(cs, includesFileContents = False)
        failedDeps = client.db.depCheck(jobSet, trvSrc)[0]
        cfg.setValue('dbPath', oldDbPath)
        cfg.setValue('root', oldRoot)
        return failedDeps

    def _removeTroves(self, source):
        if not self.removeTroveList:
            return
        groupSource = trovesource.GroupRecipeSource(source, self)
        groupSource.searchAsDatabase()
        results = groupSource.findTroves(None, self.removeTroveList,
                                         allowMissing=True)
        troveTups = chain(*results.itervalues())
        for troveTup in troveTups:
            del self.troves[troveTup]
            self.redirects.discard(troveTup)
            del self.childTroves[troveTup]


    def addPackagesForComponents(self, troveSource):
        packages = {}
        for (n,v,f), (explicit, size, byDefault) in self.troves.iteritems():
            if not explicit:
                continue
            if ':' in n:
                pkg = n.split(':', 1)[0]
                packages.setdefault((pkg, v, f), {})[n] = byDefault

        # if the user mentions both foo and foo:runtime, don't remove
        # direct link to foo:runtime
        troveTups = [ x for x in packages if x not in self.troves ]

        if not troveTups:
            return

        # XXX we need a hasTroves method
        # it's possible the package does not exist on the same branch as 
        # the child, in that case, don't switch to using parent.
        troves = []
        for troveTup in troveTups:
            try:
                trv = troveSource.getTrove(withFiles=False, *troveTup) 
                troves.append((troveTup, trv))
            except errors.TroveMissing:
                pass
                
        for troveTup, trv in troves:
            compNames = packages[troveTup]
            for compName in compNames:
                del self.troves[compName, troveTup[1], troveTup[2]]

            # child trove is byDefault True only if it was added explicitly
            # and was added byDefault=True 
            childTroves = [ (x, x[0] in compNames and compNames[x[0]])
                             for x in trv.iterTroveList(strongRefs=True) ]

            # parent trove is byDefault True if at least one of its 
            # components is.
            byDefault = bool([ x for x in childTroves if x[1]])

            self._foundTrove(troveTup, True, byDefault, False, trv.getSize())
            for childTup, childByDefault in childTroves:
                self._foundTrove(childTup, False, childByDefault and byDefault, 
                                 False, trv.getSize())
            


    def _addImplicitTroves(self, childGroups):
        componentsToRemove = self.removeComponentList

        for childGroup, byDefault in childGroups:
            for trove, info in childGroup.troves.iteritems():
                childByDefault = byDefault and info[-1]
                if childByDefault and componentsToRemove:
                    parts = trove[0].split(':', 1)
                    if len(parts) != 1:
                        if parts[1] in componentsToRemove:
                            childByDefault = False
                    
                self._foundTrove(trove, False, childByDefault)
        
        for components, childTroves in self.childTroves.itervalues():
            for (childTrove, byDefault) in childTroves:
                
                if componentsToRemove:
                    parts = childTrove[0].split(':', 1)
                    if len(parts) != 1:
                        if parts[1] in componentsToRemove:
                            byDefault = False
                        
                if components:
                    if childTrove in components:    
                        byDefault = True
                    else:
                        byDefault = False

                self._foundTrove(childTrove, False, byDefault)

    def _removeImplicitTroves(self, source):
        if not self.removeTroveList:
            return
        groupSource = trovesource.GroupRecipeSource(source, self)
        groupSource.searchAsDatabase()
        results = groupSource.findTroves(None, self.removeTroveList, 
                                         allowMissing=True)
        troveTups = chain(*results.itervalues())
        for troveTup in troveTups:
            info = self.troves[troveTup]
            self.troves[troveTup] = (info[0], info[1], False)

    def _checkForRedirects(self, repos):
        if self.redirects:
            redirects = repos.getTroves(self.redirects)
            missingTargets = {}
            for trv in redirects:
                targets = []
                name = trv.getName()
                # see processRedirectHack
                # we ignore non-primaries
                for (subName, subVersion, subFlavor) in trv.iterTroveList(strongRefs=True):
                    if (":" not in subName and ":" not in name) or \
                       (":"     in subName and ":"     in name):
                       targets.append((subName, subVersion, subFlavor))
                missing = [ x for x in targets if x not in self.troves ]
                if missing:
                    l = missingTargets.setdefault(trv, [])
                    l += missing


            errmsg = []
            if not missingTargets:
                for troveTup in self.redirects:
                    del self.troves[troveTup]
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

    def getRequires(self):
        return self.requires

    def getTroveList(self):
	return self.troveVersionFlavors

    def getNewGroupList(self):
	return self.newGroupList

    def hasTroves(self):
        return bool(self.newGroupList or self.getTroveList())

    def __init__(self, depCheck, autoResolve, checkOnlyByDefaultDeps,
                 byDefault = True):

        self.troves = {}
        self.redirects = set()
        self.addTroveList = []
        self.addReferenceList = []
        self.removeTroveList = []
        self.removeComponentList = set()
        self.newGroupList = []
        self.requires = deps.DependencySet()
	self.troveVersionFlavors = {}
        self.childTroves = {}

        self.depCheck = depCheck
        self.autoResolve = autoResolve
        self.checkOnlyByDefaultDeps = checkOnlyByDefaultDeps
        self.byDefault = byDefault

    def Requires(self, requirement):
        self.requires.addDep(deps.TroveDependencies, 
                             deps.Dependency(requirement))

class _GroupReference:
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


class GroupRecipe(Recipe):
    Flags = use.LocalFlags
    depCheck = False
    autoResolve = False
    checkOnlyByDefaultDeps = True
    ignore = 1
    _recipeType = RECIPE_TYPE_GROUP

    def Requires(self, requirement, groupName = None):
        if requirement[0] == '/':
            raise RecipeFileError, 'file requirements not allowed in groups'
        if groupName is None: groupName = self.name

        self.groups[groupName].Requires(requirement)

    def _parseFlavor(self, flavor):
        assert(flavor is None or isinstance(flavor, str))
        if flavor is None:
            return None
        flavorObj = deps.parseFlavor(flavor)
        if flavorObj is None:
            raise ValueError, 'invalid flavor: %s' % flavor
        return flavorObj

    def _parseGroupNames(self, groupName):
        if groupName is None:
            return [self.defaultGroup]
        elif not isinstance(groupName, (list, tuple)):
            return [groupName]
        else:
            return groupName

    def add(self, name, versionStr = None, flavor = None, source = None,
            byDefault = None, groupName = None, ref=None):
        groupNames = self._parseGroupNames(groupName)
        flavor = self._parseFlavor(flavor)
        # track this trove in the GroupRecipe so that it can be found
        # as a group with the rest of the troves.
        self.toFind.setdefault(ref, set()).add((name, versionStr, flavor))
        if ref is not None:
            self.sources.add(ref)

        for groupName in groupNames:
            self.groups[groupName].add(name, versionStr = versionStr,
                                                flavor = flavor,
                                                source = source,
                                                byDefault = byDefault, 
                                                ref = ref)
    # maintain addTrove for backwards compatability
    addTrove = add

    def setGroupByDefault(self, byDefault=True, groupName=None):
        """ Set whether troves added to this group are installed by default 
            or not.  (This default value can be overridden by the byDefault
            parameter to individual addTrove commands).  If you set the 
            byDefault value for the main group, you set it for any 
            future groups created.
        """
        groupNames = self._parseGroupNames(groupName)
        for groupName in groupNames:
            self.groups[groupName].setGroupByDefault(byDefault)

    def removeComponents(self, componentNames, groupName=None):
        if isinstance(componentNames, str):
            componentNames = [componentNames]

        groupNames = self._parseGroupNames(groupName)
        for groupName in groupNames:
            self.groups[groupName].removeComponents(componentNames)

    def addAll(self, reference, groupName=None):
        """ Add all of the troves directly contained in the given 
            reference to groupName.  For example, if the cooked group-foo 
            contains references to the troves 
            foo1=<version>[flavor] and foo2=<version>[flavor],
            the lines 
            ref = r.addReference('group-foo')
            followed by
            r.addAll(ref)
            would be equivalent to you having added the addTrove lines
            r.addTrove('foo1', <version>) 
            r.addTrove('foo2', <version>) 
        """
        assert(reference is not None)
        self.sources.add(reference)

        groupNames = self._parseGroupNames(groupName)
        for groupName in groupNames:
            self.groups[groupName].addAll(reference)

    def remove(self, name, versionStr=None, flavor=None, 
                    groupName=None):
        """ Remove a trove added to this group, either by an addAll
            line or by an addTrove line. 
        """
        groupNames = self._parseGroupNames(groupName)
        flavor = self._parseFlavor(flavor)
        for groupName in groupNames:
            self.groups[groupName].remove(name, versionStr, flavor)

    def setDefaultGroup(self, groupName=None):
        if groupName is None:
            self.defaultGroup = self.name
        self.defaultGroup = groupName

    def addReference(self, name, versionStr=None, flavor=None, ref=None):
        flavor = self._parseFlavor(flavor)
        return _GroupReference(((name, versionStr, flavor),), ref)

    def addNewGroup(self, name, groupName = None, byDefault = True):
        groupNames = self._parseGroupNames(groupName)
	if not self.groups.has_key(name):
	    raise RecipeFileError, 'group %s has not been created' % name

        for groupName in groupNames:
            self.groups[groupName].addNewGroup(name, byDefault)

    def getRequires(self, groupName = None):
        if groupName is None: groupName = self.name
        return self.groups[groupName].getRequires()

    def getTroveList(self, groupName = None):
        if groupName is None: groupName = self.name
	return self.groups[groupName].getTroveList()

    def getNewGroupList(self, groupName = None):
        if groupName is None: groupName = self.name
	return self.groups[groupName].getNewGroupList()

    def getSize(self, groupName = None):
        if groupName is None: groupName = self.name
        return self.groups[groupName].size

    def setLabelPath(self, *path):
        self.labelPath = [ versions.Label(x) for x in path ]

    def createGroup(self, groupName, depCheck = False, autoResolve = False,
                    byDefault = None, checkOnlyByDefaultDeps = None):
        if self.groups.has_key(groupName):
            raise RecipeFileError, 'group %s was already created' % groupName
        if not groupName.startswith('group-'):
            raise RecipeFileError, 'group names must start with "group-"'
        if byDefault is None:
            byDefault = self.groups[self.name].byDefault
        if checkOnlyByDefaultDeps is None:
            checkOnlyByDefaultDeps  = self.groups[self.name].checkOnlyByDefaultDeps

        self.groups[groupName] = SingleGroup(depCheck, autoResolve, 
                                             checkOnlyByDefaultDeps, byDefault)

    def getGroupNames(self):
        return self.groups.keys()

    def _orderGroups(self):
        """ Order the groups so that each group is after any group it 
            contains.  Raises an error if a cycle is found.
        """
        # boy using a DFS for such a small graph seems like overkill.
        # but its handy since we're also trying to find a cycle at the same
        # time.
        children = {}
        groupNames = self.getGroupNames()
        for groupName in groupNames:
            children[groupName] = \
                    set([x[0] for x in self.getNewGroupList(groupName)])

        timeStamp = 0

        # the different items in the seen dict
        START = 0   # time at which the node was first visited
        FINISH = 1  # time at which all the nodes child nodes were finished
                    # with
        PATH = 2    # path to get to this node from wherever it was 
                    # started.
        seen = dict((x, [None, None, []]) for x in groupNames)

        for groupName in groupNames:
            if seen[groupName][START]: continue
            stack = [groupName]

            while stack:
                timeStamp += 1
                node = stack[-1]

                if seen[node][FINISH]:
                    # we already visited this node through 
                    # another path that was longer.  
                    stack = stack[:-1]
                    continue
                childList = []
                if not seen[node][START]:
                    seen[node][START] = timeStamp

                    if children[node]:
                        path = seen[node][PATH] + [node]
                        for child in children[node]:
                            if child in path:
                                cycle = path[path.index(child):] + [child]
                                raise RecipeFileError('cycle in groups: %s' % cycle)

                            if not seen[child][START]:
                                childList.append(child)

                if not childList:
                    # we've finished with all this nodes children 
                    # mark it as done
                    seen[node][FINISH] = timeStamp
                    stack = stack[:-1]
                else:
                    path = seen[node][PATH] + [node]
                    for child in childList:
                        seen[child] = [None, None, path]
                        stack.append(child)

        groupsByLastSeen = ( (seen[x][FINISH], x) for x in groupNames)
        return [x[1] for x in sorted(groupsByLastSeen)]

    def getPrimaryGroupNames(self):
        """ 
        Return the list of groups in this GroupRecipe that are not included in 
        any other groups.
        """
        unseen = set(self.getGroupNames())

        for groupName in self.getGroupNames():
            unseen.difference_update([x[0] for x in self.groups[groupName].getNewGroupList()])
        return unseen

    def getChildGroups(self, groupName):
        return [ (self.groups[x[0]], x[1]) \
                  for x in self.groups[groupName].getNewGroupList() ]

    def findAllTroves(self):
        if self.toFind is not None:
            # find all troves needed by all included groups together, at 
            # once.  We then pass that information into the individual
            # groups.
            self._findSources()
            self._findTroves()
            self.toFind = None

        groupNames = self._orderGroups()

        for groupName in groupNames:
            groupObj = self.groups[groupName]


            childGroups = self.getChildGroups(groupName)
            # assign troves to this group
            groupObj.findTroves(self.troveSpecMap, self.repos, childGroups)


            # include those troves when doing dependency resolution/checking
            groupObj.autoResolveDeps(self.cfg, self.repos, self.labelPath)


            failedDeps = groupObj.checkDependencies(self.cfg)
            if failedDeps:
                return groupName, failedDeps

            groupObj.addPackagesForComponents(self.repos)

            groupObj.calcSize()

            if not groupObj.hasTroves():
                raise RecipeFileError('%s has no troves in it' % groupName)


    def _findSources(self):
        for troveSource in self.sources:
            if troveSource is None:
                continue
            troveSource.findSources(self.repos, self.labelPath, self.flavor)

    def _findTroves(self):
        """ Finds all the troves needed by all groups, and then 
            stores the information for retrieval by the individual 
            groups (stored in troveSpecMap).
        """
        repos = self.repos
        cfg = self.cfg

        troveTups = set()

        results = {}
        for troveSource, toFind in self.toFind.iteritems():
            try:
                if troveSource is None:
                    source = repos
                else:
                    source = troveSource

                results[troveSource] = source.findTroves(self.labelPath, 
                                                         toFind, 
                                                         cfg.buildFlavor)
            except errors.TroveNotFound, e:
                raise RecipeFileError, str(e)
            for result in results.itervalues():
                troveTups.update(chain(*result.itervalues()))

        troveTupList = list(troveTups)

        troves = repos.getTroves(troveTupList, withFiles=False)

        foundTroves = dict(izip(troveTupList, troves))

        implicitOn = []
        implicitOff = []
        for trv in troves:
            for info in trv.iterTroveList(weakRefs=True,    
                                          strongRefs=True):
                byDefault = trv.includeTroveByDefault(*info)
                if byDefault:
                    implicitOn.append(info)
                else:
                    implicitOff.append(info)

        implicitTrv = dict.fromkeys(implicitOn, True)
        for info in implicitOff:
            implicitTrv.setdefault(info, False)

        def _getChildTroves(trv):
            if trv.isRedirect():
                return []
            return [ (x, trv.includeTroveByDefault(*x))
                     for x in trv.iterTroveList(weakRefs=True, 
                                                strongRefs=True) ] 

        troveSpecMap = {}
        # store the pertinent information in troveSpecMap
        # keyed off of source, then troveSpec
        # note - redirect troves are not allowed in group recipes.
        # we track whether a trove is a redirect because it's possible
        # it could be added at one point (say, by an overly general
        # addTrove line) and then removed afterwards by a removeTrove.
        for troveSource, toFind in self.toFind.iteritems():
            d = {}
            for troveSpec in toFind:
                d[troveSpec] = [ (x,
                                  foundTroves[x].getSize(), 
                                  foundTroves[x].isRedirect(),
                                  _getChildTroves(foundTroves[x]))
                                    for x in results[troveSource][troveSpec] ]
            troveSpecMap[troveSource] = d
        self.troveSpecMap = troveSpecMap

    def __init__(self, repos, cfg, label, flavor, extraMacros={}):
	self.repos = repos
	self.cfg = cfg
	self.labelPath = [ label ]
	self.flavor = flavor
        self.macros = macros.Macros()
        self.macros.update(extraMacros)

        self.toFind = {}
        self.troveSpecMap = {}
        self.foundTroves = {}
        self.sources = set()
        self.defaultGroup = self.name

        self.groups = {}
        self.groups[self.name] = SingleGroup(self.depCheck, self.autoResolve,   
                                             self.checkOnlyByDefaultDeps)
