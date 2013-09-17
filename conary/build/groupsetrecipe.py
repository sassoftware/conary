#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import inspect, itertools, re, time

from conary import trove, versions
from conary.build import defaultrecipes, use
from conary.build import lookaside
from conary.build.errors import CookError
from conary.build.grouprecipe import _BaseGroupRecipe, _SingleGroup
from conary.conaryclient.cmdline import parseTroveSpec
from conary.conaryclient import cml, modelgraph, troveset
from conary.conaryclient.resolve import PythonDependencyChecker
from conary.lib import log
from conary.local import deptable
from conary.repository import errors, netclient, searchsource
from conary.deps import deps

def findRecipeLineNumber():
    line = None

    for frame in inspect.stack():
        if frame[1].endswith('.recipe'):
            line = frame[2]
            break

    return line

class GroupSetTroveCache(object):

    def __init__(self, groupRecipe, cache):
        self.cache = cache
        self.groupRecipe = groupRecipe
        self.depCache = {}

    def __getattr__(self, name):
        return getattr(self.cache, name)

    def _cached(self, troveTupList, troveList):
        # this avoids the call to recursively get children
        # GroupSet.TroveCache needs
        pass

    def getRepos(self):
        return self.cache.troveSource

    def cacheTroves(self, troveList):
        return self.cache.cacheTroves(troveList, _cached = self._cached)

    def getTrove(self, n, v, f, withFiles = False):
        raise NotImplementedError

    def getTroves(self, troveList, withFiles = False):
        return self.cache.getTroves(troveList, _cached = self._cached,
                                    withFiles = withFiles)

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

    def troveReferencesTrove(self, troveTup, troveRef):
        if isinstance(troveTup[1], versions.NewVersion):
            sg = self.groupRecipe._getGroup(troveTup[0])
            return sg.hasTrove(*troveRef)

        return self.cache.troveReferencesTrove(troveTup, troveRef)

class GroupActionData(troveset.ActionData):

    def __init__(self, troveCache, groupRecipe):
        troveset.ActionData.__init__(self, troveCache, groupRecipe.flavor)
        self.groupRecipe = groupRecipe


class GroupTupleSetMethods(object):
    # used mainly in a TroveSet context, so document it there from user POV
    '''
    NAME
    ====
    B{C{TroveSet}} - collection of trove references

    DESCRIPTION
    ===========
    A B{TroveSet} is an immutable collection of references to
    specific troves from a Conary repository, and set operations
    on those collections.  Each trove reference in a TroveSet is a
    three-tuple of B{name}, B{version}, B{flavor}, along with an
    attribute, C{isInstalled}, that describes whether the trove
    is considered B{installed} or B{optional}.  Each TroveSet is
    immutable.  TroveSet operations return new TroveSets; they do
    not modify existing TroveSets.

    METHODS
    =======
    The following methods are available in C{TroveSet} objects:

        - L{components} : Recursively search for components
        - L{createGroup} : Create a binary group
        - L{depsNeeded} : Get troves satisfying dependencies
        - L{difference} : Subtract one TroveSet from another (C{-})
        - L{dump} : Debugging: print the contents of the TroveSet
        - L{find} : Search the TroveSet for specified troves
        - L{findByName} : Find troves by regular expression
        - L{findBySourceName} : Find troves by the name of the source
          package from which they were built
        - L{flatten} : Resolve trove references recursively
        - L{getInstall} : Get only install troves from set
        - L{getOptional} : Get only optional troves from set
        - L{isEmpty} : Assert that the TroveSet is entirely empty
        - L{isNotEmpty} : Assert that the TroveSet contains something
        - L{makeInstall} : Make all troves install, or add all provided
          troves as install troves
        - L{makeOptional} : Make all troves optional, or add all provided
          troves as optional troves
        - L{members} : Resolve exactly one level of trove references,
          return only those resolved references
        - L{packages} : Resolve trove references recursively, return packages
        - L{patch} : Replace troves in the TroveSet with matching-named
          troves from the replacement set
        - L{union} : Get the union of all provided TroveSets (C{|}, C{+})
        - L{update} : Replace troves in the TroveSet with all troves from
          the replacement set

    Except for C{dump}, which prints debugging information, each of these
    methods returns a new TroveSet.
    '''
    _explainObjectName = 'TroveSet'

    def depsNeeded(self, resolveSource = None, failOnUnresolved = True):
        """
        NAME
        ====
        B{C{TroveSet.depsNeeded}} - Get troves satisfying dependencies

        SYNOPSIS
        ========
        C{troveset.depsNeeded(resolveSource=None, failOnUnresolved=True)}

        DESCRIPTION
        ===========
        Looks for unresolved dependencies in the trove set.  Those unmet
        dependencies (and their dependencies, recursively) are sought in
        the C{resolveSource}, which must be a C{TroveSet}, C{Repository}, or
        C{SearchPath}.  If there are unresolvable dependencies, it raises
        an error unless C{failOnUnresolved=False}.  Returns a troveset
        containing the troves that were used to resolve the dependencies.
        This is not a union operation; the contents of the returned
        troveset do not include the contents of the original troveset.

        If no C{resolveSource} is provided, then depsNeeded asserts that
        there are no unresolved dependencies.

        PARAMETERS
        ==========
            - L{resolveSource} : Source against which to resolve dependencies,
              or None to assert that all dependencies are met.
            - L{failOnUnresolved} (C{True}) : Whether to fail if not all
              dependencies can be resolved.

        EXAMPLES
        ========
        There are several significant use cases for C{depsNeeded}.

        The first use case is perhaps the most obvious; creating a group
        that is dependency-complete:

        mygrp = repos['group-standard'] + repos['other-package']
        mygrp += mygrp.depsNeeded(repos)
        groupStandard = mygrp.createGroup('group-standard')

        A second use case is to enforce that dependencies can be
        resolved.  If C{failOnUnresolved} is left to the default C{True}
        and the resulting troveset is not used, this becomes an assertion
        that all the dependencies for the original troveset not provided
        within the original troveset can be found within the specified
        search path.

        std = repos['group-standard']
        mygrp = std + myrepos['mypackage']
        # mypackage has added only dependencies resolved in group-packages
        mygrp.depsNeeded(repos['group-packages'])
        groupStandard = mygrp.createGroup('group-standard')

        A third use case is partial dependency closure.  The
        C{failOnUnresolved} option can be set if you want to resolve
        all the dependencies possible, with the understanding that
        other dependencies will be resolved in another context.
        This is normally useful only when that other context is
        outside of the current group cook.

        """
        fetched = self._action(ActionClass = troveset.FetchAction)
        if resolveSource:
            if isinstance(resolveSource, troveset.SearchPathTroveSet):
                resolveSource = resolveSource._action(
                                        ActionClass = GroupSearchPathFetch)
            elif isinstance(resolveSource, troveset.DelayedTupleSet):
                resolveSource = resolveSource._action(
                                        ActionClass = troveset.FetchAction)

        return fetched._action(resolveSource,
                                     failOnUnresolved = failOnUnresolved,
                                     ActionClass = DepsNeededAction)

    def difference(self, other):
        """
        NAME
        ====
        B{C{TroveSet.difference}} - Subtract one TroveSet from another (C{-})

        SYNOPSIS
        ========
        C{troveset.difference(other)}
        C{troveset - other}

        DESCRIPTION
        ===========
        Returns a new troveset which includes the members of the
        original set which are not in the troveset C{other}. The
        isInstall values of the troves in troveset C{other} are
        ignored when deciding if those troves should be included in
        the result.
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
        NAME
        ====
        B{C{TroveSet.find}} - Search the TroveSet for specified troves

        SYNOPSIS
        ========
        C{troveset.find('troveSpec1', 'troveSpec2', ..., 'troveSpecN')}
        C{troveset['troveSpec']}

        DESCRIPTION
        ===========
        Returns a new C{troveset} containing all troves from the original
        troveset which match the given C{troveSpec}(s).  The original
        troveset's isInstall settings are preserved for each returned
        trove.  The contents of the TroveSet are sought recursively.

        EXAMPLES
        ========
        C{groupOS = repos['group-os']}
        C{allGlibcVersions = groupOS.find('glibc')}
        C{compatGlibc = groupOS['glibc=@rpl:1-compat']}

        This sets C{groupOS} to be a TroveSet containing the version of
        C{group-os} found on the default label for Repository C{repos}.
        It then finds all versions/flavors of glibc referenced (there
        could be more than one) and creates an C{allGlibcVersions}
        TroveSet that contains references to all of them, and another
        C{compatGlibc} that contains refernces to all flavors of glibc
        that are on a label matching C{@rpl:1-compat}.
        """
        return self._action(ActionClass = GroupTroveSetFindAction, *troveSpecs)

    def findByName(self, namePattern, emptyOkay = False):
        """
        NAME
        ====
        B{C{TroveSet.findByName}} - Find troves by regular expression

        SYNOPSIS
        ========
        C{troveset.findByName(nameRegularExpression, emptyOkay = False)}

        DESCRIPTION
        ===========
        The original troveset is searched for troves whose names match
        C{nameRegularExpression}, and matching troves are returned in
        a new troveset.  The isInstall value is preserved from the
        original troveset being searched.  Unlike C{find}, the original
        troveset is not searched recursively; use C{troveset.flatten()}
        explicitly if you need to search recursively.

        PARAMETERS
        ==========
            - L{emptyOkay} : Unless set to C{True}, raise an exception if
              no troves are found.
        
        EXAMPLES
        ========
        C{allGnomePackages = allPackages.findByName('^gnome-')}

        Returns a troveset containing all troves in the troveset
        C{allPackages} with a name starting with C{gnome-}

        C{allTroves = repos['group-os'].flatten()}
        C{allGroups = allTroves.findByName('^group-')}
        C{allOtherTroves = allTroves - allGroups}
        """
        return self._action(namePattern, emptyOkay = emptyOkay,
                            ActionClass = FindByNameAction)

    def findBySourceName(self, sourceName):
        """
        NAME
        ====
        B{C{TroveSet.findBySourceName}} - Find troves by the name of the source
        package from which they were built

        SYNOPSIS
        ========
        C{troveset.findBySourceName(sourceName)}

        DESCRIPTION
        ===========
        The original troveset is searched for troves which were built
        from source trove called C{sourceName}, and all matching
        troves are returned in a new troveset.  The isInstall value is
        preserved from the original troveset being searched.  Unlike
        C{find}, the original troveset is not searched recursively;
        use C{troveset.flatten()} explicitly if you need to search
        recursively.

        EXAMPLES
        ========
        C{allTroves = repos['group-os'].flatten()}
        C{allPGPackages = allTroves.findBySourceName('postgresql')}

        Returns a troveset containing all troves in the troveset
        C{allTroves} that were built from C{postgresql:source}
        """
        return self._action(sourceName,
                            ActionClass = FindBySourceNameAction)

    __getitem__ = find

    def components(self, *componentList):
        """
        NAME
        ====
        B{C{TroveSet.components}} - Returns named components included in
        all members of the troveset.

        SYNOPSIS
        ========
        C{troveset.components(I{componentName1}, I{componentName2}, ...)}

        DESCRIPTION
        ===========
        Returns components included in all members of the troveset, found
        recursively, where the component name (C{runtime}, C{lib}, C{data},
        etc.) matches one of the component names provided.  The C{isInstalled}
        setting for each component in the returned troveset is determined
        only by whether the component is installed or optional in the
        package that contains it.  This does not implicitly recurse, so
        to find components of packages in a group, use C{flatten()}

        EXAMPLES
        ========
        C{groupOs = repos['group-os'].flatten()}
        C{allDebugInfo = groupOs.components('debuginfo')}

        Returns a TroveSet referencing all the C{debuginfo} components of
        all packages referenced in C{group-os} as found in the C{repos}
        object.

        C{groupDist = repos['group-dist'].flatten()}
        C{docTroves = groupDist.components('doc', 'supdoc')}

        Returns a TroveSet referencing all the C{doc} and C{supdoc}
        components of all packages referenced in C{group-dist} as found
        in the C{repos} object.
        """
        return self._action(ActionClass = ComponentsAction, *componentList)

    def flatten(self):
        """
        NAME
        ====

        B{C{TroveSet.flatten}} - Returns all troves, recursively

        SYNOPSIS
        ========
        C{troveset.flatten()}

        DESCRIPTION
        ===========
        The troveset returned consists of any existing trove referenced
        by the original troveset, directly or indirectly via groups.
        The C{isInstall} setting for each troves is inherited from
        the original troveset, not from the troves referenced.  (The
        only troves that will not be returned are references to binary
        groups being built out of the recipe, as returned by the
        C{TroveSet.createGroup()} method.)

        This is useful for creating flattened groups (removing group
        structure present in upstream groups but not desired in the
        groups being built) and for creating trovesets to use to look
        up specific troves (for example, C{find} and C{findByName}).

        EXAMPLES
        ========
        C{platGrp = repos['group-appliance-platform'].flatten()}

        Returns all the non-group troves included directly in
        group-appliance-platform, as well as those included only within
        group-core (included in group-appliance-platform), and those
        included only within group-bootable, included only because it
        is included within group-core.  Does not include any of those
        groups; only the members of the groups.
        """
        return self._action(ActionClass = FlattenAction)

    def getInstall(self):
        """
        NAME
        ====
        B{C{TroveSet.getInstall}} - Returns only install members

        SYNOPSIS
        ========
        C{troveset.getInstall()}

        DESCRIPTION
        ===========
        Returns a new troveset which includes only the members of
        this troveset which are marked as install; optional members are
        omitted. All members of the returned set are marked as install.
        """
        return self._action(ActionClass = GetInstalledAction)

    def getOptional(self):
        """
        NAME
        ====
        B{C{TroveSet.getOptional}} - Returns only optional members

        SYNOPSIS
        ========
        C{troveset.getOptional()}

        DESCRIPTION
        ===========
        Returns a new troveset which includes only the members of
        this troveset which are marked as optional; install members are
        omitted. All members of the returned set are marked as optional.
        """
        return self._action(ActionClass = GetOptionalAction)

    def isEmpty(self):
        """
        NAME
        ====
        B{C{TroveSet.isEmpty}} - Assert that troveset is empty

        SYNOPSIS
        ========
        C{troveset.isEmpty()}

        DESCRIPTION
        ===========
        Raises an exception is raised if the troveset contains any members.
        Otherwise, returns an identical (empty) troveset that may be ignored.
        """
        return self._action(ActionClass = IsEmptyAction)

    def isNotEmpty(self):
        """
        NAME
        ====
        B{C{TroveSet.isNotEmpty}} - Assert that troveset is not empty

        SYNOPSIS
        ========
        C{troveset.isNotEmpty()}

        DESCRIPTION
        ===========
        Raises an exception is raised if the troveset contains no members.
        Otherwise, returns an identical troveset that may be ignored.
        """
        return self._action(ActionClass = IsNotEmptyAction)

    def makeInstall(self, installTroveSet = None):
        """
        NAME
        ====
        B{C{TroveSet.makeInstall}} - Make all troves install, or add all
        provided troves as install troves

        SYNOPSIS
        ========
        C{troveset.makeInstall(installTroveSet = None)}

        DESCRIPTION
        ===========
        If C{installTroveSet} troveset is provided as an argument, all
        members of that other troveset are included in the result as
        install members.  Any members of the original troveset which
        are optional, and are not in C{installTroveSet}, are also
        optional in the result.

        If C{installTroveSet} is not provided, the troveset returned
        includes all members of the original troveset as install members.

        PARAMETERS
        ==========
            - L{installTroveSet} : TroveSet providing all its members as install
        """
        return self._action(ActionClass = MakeInstallAction,
                            installTroveSet = installTroveSet)

    def makeOptional(self, optionalTroveSet = None):
        """
        NAME
        ====
        B{C{TroveSet.makeOptional}} - Make all troves optional, or add all
        provided troves as optional troves

        SYNOPSIS
        ========
        C{troveset.makeOptional(optionalTroveSet = None)}

        DESCRIPTION
        ===========
        If C{optionalTroveSet} troveset is provided as an argument, all
        members of that other troveset are included in the result as
        optional members.  Any members of the original troveset which
        are install troves, and are not in C{optionalTroveSet}, are also
        install troves in the returned troveset.

        If C{optionalTroveSet} is not provided, the troveset returned
        includes all members of the original troveset as optional members.

        PARAMETERS
        ==========
            - L{optionalTroveSet} : TroveSet providing all its members as optional
        """
        return self._action(ActionClass = MakeOptionalAction,
                            optionalTroveSet = optionalTroveSet)

    def members(self):
        """
        NAME
        ====
        B{C{TroveSet.members}} - Returns all members of the troveset

        SYNOPSIS
        ========
        C{troveset.members()}

        DESCRIPTION
        ===========
        All troves directly included by the troves in this troveset
        are returned as a new troveset. They are optional in the result
        only if they are optional in every member of this troveset which
        includes them.
        """
        return self._action(ActionClass = MembersAction)

    def packages(self, *packageList):
        """
        NAME
        ====
        B{C{TroveSet.packages}} - Return recursively-search package references

        SYNOPSIS
        ========
        C{troveset.packages()}

        DESCRIPTION
        ===========
        Return all packages and filesets referenced directly or indirectly
        by this troveset. They are optional in the result only if they
        are optional in every member of this troveset which includes them.
        """
        return self._action(ActionClass = PackagesAction, *packageList)

    def scripts(self):
        """
        NAME
        ====
        B{C{TroveSet.scripts}} - Return scripts for a trove

        SYNOPSIS
        ========
        C{troveset.scripts()}

        DESCRIPTION
        ===========
        Returns a Scripts object which includes all of the scripts for the
        trove included in this TroveSet. If this TroveSet is empty or contains
        multiple troves, an exception is raised.

        EXAMPLES
        ========
        This creates a new group which includes the scripts from a group
        which is already in the repository.

        existingGrp = repos['group-standard']
        thisGrpContents = repos['pkg']
        r.Group(thisGrpContents, scripts = existingGrp.scripts()
        """
        stubTroveSet = self._action(ActionClass = ScriptsAction)
        return stubTroveSet.groupScripts

    def union(self, *troveSetList):
        """
        NAME
        ====
        B{C{TroveSet.union}} - Get the union of all provided TroveSets (C{|}, C{+})

        SYNOPSIS
        ========
        C{troveset.union(other1, other2, ..., otherN)}
        C{troveset + other1 + other2}
        C{troveset | other1 | other2}

        DESCRIPTION
        ===========
        Return a troveset which includes all of the members of this trove
        as well as all of the members of the arguments. Troves are optional
        only if they are optional in all the trovesets they are part of.
        """
        return self._action(ActionClass = GroupUnionAction, *troveSetList)

    def patch(self, patchSet):
        """
        NAME
        ====
        B{C{TroveSet.patch}} - Replace troves with matching-name troves

        SYNOPSIS
        ========
        C{troveset.patch(patchSet)}

        DESCRIPTION
        ===========
        Look (recursively) for items in this troveset which can
        reasonably be replaced by members found in the patchSet.
        The isInstall values are inherited from the original troveset.
        Any items in patchSet which do not appear to replace
        members of this troveset are included as optional in the
        result.  Members of the original troveset which are outdated
        by members of the patchSet are also included as optional
        in the returned troveset, to prevent them from inadvertently
        showing up as install troves due to other operations.

        This is a recursive union operation in which only troves
        which are installed in the original set are installed in
        the resulting set, and all other troves are available.

        The difference between C{TroveSet.update} and C{TroveSet.patch} is
        how new troves introduced in C{patchSet} but not present in the
        original set are handled.  With C{TroveSet.patch}, the new
        troves from C{patchSet} are not installed in the result; with
        C{TroveSet.update}, the new troves are installed in the result if
        they are installed in the C{updateSet}.

        PARAMETERS
        ==========
            - L{patchSet} : TroveSet containing potential replacements

        EXAMPLES
        ========
        This operation is intended to implement the appropriate
        behavior for applying a group specifying a set of updated
        packages.  For example, if only the postgresql client is
        in the current install set, and group-CVE-2015-1234 contains
        both the postgresql client and server in different packages,
        then the patch operation will mark the existing postgresql
        client as optional, add the new postgresql client as install,
        and add the new postgresql server as optional in the returned
        troveSet.

        base = repos['group-standard']
        update = base.patch(repos['group-CVE-2015-1234'])
        groupStandard = update.createGroup('group-standard')

        """
        return self._action(patchSet, ActionClass = GroupPatchAction)

    def update(self, updateSet):
        """
        NAME
        ====
        B{C{TroveSet.update}} - Replace troves in the TroveSet with
        all troves from the replacement set

        SYNOPSIS
        ========
        C{troveset.update(updateSet)}

        DESCRIPTION
        ===========
        Returns a troveset that is a recusive union of the original
        troveset and C{updateSet}, except that only where the names of
        troves overlap, the versions from C{updateSet} are used, though
        the choice of isInstall is honored from the original set.

        The difference between C{TroveSet.update} and C{TroveSet.patch} is
        how new troves introduced in C{updateSet} but not present in the
        original set are handled.  With C{TroveSet.patch}, the new
        troves from C{patchSet} are not installed in the result; with
        C{TroveSet.update}, the new troves are installed in the result if
        they are installed in the C{updateSet}.

        PARAMETERS
        ==========
            - L{updateSet} : TroveSet providing all its contents

        EXAMPLES
        ========
        This is commonly used to update to new package versions while
        preserving the semantics of a source group.  This might be used
        to apply a "hotfix".  So if you are building a group based on
        a specific version of a platform, and do not wish to move to
        a new version of the platform, except that you want to inclue
        a specific new package that implements a necessary fix, this
        is most likely the correct operation.

        base = repos['group-standard']
        # Use latest conary to resolve CNY-98765 until resolved
        update = base.update(repos['conary=centos.rpath.com@rpath:centos-5'])
        groupStandard = update.createGroup('group-standard')

        """
        return self._action(updateSet, ActionClass = GroupUpdateAction)


    def createGroup(self, name, checkPathConflicts = True, scripts = None,
                    imageGroup = False):
        """
        NAME
        ====
        
        B{C{TroveSet.createGroup}} - Create a binary group

        SYNOPSIS
        ========
        C{troveset.createGroup(name, checkPathConflicts=True, scripts=None)}

        DESCRIPTION
        ===========
        Create a new group whose members are defined by this
        troveset, and call it C{name} (which must begin with
        "C{group-}").

        Returns a troveset which references this newly created group,
        which allows it to be included in other trovesets, and hence,
        other groups.

        PARAMETERS
        ==========
         - C{checkPathConflicts} : Raise an error if any paths overlap (C{True})
         - C{imageGroup} : (False) Designate that this group is a image group.
           Image Group policies will be executed separately on this group.
         - C{scripts} : Attach one or more scripts specified by a C{Scripts}
           object (C{None})
        """
        return self._action(name, checkPathConflicts = checkPathConflicts,
                            ActionClass = CreateNewGroupAction,
                            imageGroup = imageGroup,
                            scripts = scripts)

    def _createGroup(self, name, checkPathConflicts = True, scripts = None,
                     imageGroup = False):
        return self._action(name, ActionClass = CreateGroupAction,
                            checkPathConflicts = checkPathConflicts,
                            imageGroup = imageGroup,
                            scripts = scripts)

    __add__ = union
    __or__ = union

class GroupDelayedTroveTupleSet(GroupTupleSetMethods,
                                troveset.DelayedTupleSet):

    def __init__(self, *args, **kwargs):
        self._dump = False
        self._lineNumStr = ''
        index = findRecipeLineNumber()
        if index is not None:
            kwargs['index'] = index
            self._lineNumStr = ':' + str(index)

        troveset.DelayedTupleSet.__init__(self, *args, **kwargs)

    def beenRealized(self, data):
        def display(tupleSet):
            if not tupleSet:
                log.info("\t\t(empty)")
                return

            for (name, version, flavor) in sorted(tupleSet):
                if isinstance(version, versions.NewVersion):
                    log.info("\t\t%s (newly created)" % name)
                else:
                    log.info("\t\t%s=%s/%s[%s]"
                                    % (name, version.trailingLabel(),
                                       version.trailingRevision(), flavor))

        troveset.DelayedTupleSet.beenRealized(self, data)

        if self._dump or data.groupRecipe._dumpAll:
            log.info("TroveSet contents for action %s" % str(self.action) +
                     self._lineNumStr)
            log.info("\tInstall")
            display(self._getInstallSet())
            log.info("\tOptional")
            display(self._getOptionalSet())

        if data.groupRecipe._trackDict:
            matches = []
            foundMatch = False

            try:
                matches = self._findTroves(data.groupRecipe._trackDict.keys())
            except errors.TroveNotFound:
                matches = {}

            if matches:
                log.info("Tracking matches found in results for action %s"
                         % str(self.action) + self._lineNumStr)
                for (parsedSpec, matchList) in matches.iteritems():
                    log.info("\tMatches for %s"
                                    % data.groupRecipe._trackDict[parsedSpec])
                    display(matchList)

    def dump(self):
        self._dump = True
        return self

class GroupLoggingDelayedTroveTupleSet(GroupDelayedTroveTupleSet):

    def realize(self, *args):
        mark = time.time()

        if isinstance(self.action, GroupIncludeAction):
            log.info("Including %s" % " ".join(
                        "%s=%s[%s]" % nvf for nvf in
                               self.action.includeSet._getInstallSet()))
        else:
            log.info("Running action %s" % str(self.action) + self._lineNumStr)
        GroupDelayedTroveTupleSet.realize(self, *args)
        runtime = time.time() - mark
        if runtime > 0.1:
            if isinstance(self.action, GroupIncludeAction):
                log.info("\tinclude processing took %.1fs" % runtime)
            else:
                log.info("\ttook %.1fs" % runtime)

class GroupSearchPathTroveSet(troveset.SearchPathTroveSet):
    '''
    NAME
    ====
    B{C{SearchPath}} - Collection of troves in which to search

    SYNOPSIS
    ========
    C{sp = r.SearchPath(TroveSet | Repository, ...)}

    DESCRIPTION
    ===========
    An object which searches multiple C{TroveSet} or C{Repository} objects
    in the order specified.  Troves can be looked up in that C{SearchPath}
    object with the C{find} method, and the C{SearchPath} object can also
    be used for resolving dependencies.

    METHODS
    =======
        - L{find} : Search the SearchPath for specified troves
    '''
    _explainObjectName = 'SearchPath'

    def find(self, *troveSpecs):
        '''
        NAME
        ====
        B{C{SearchPath.find}} - Search the SearchPath for specified troves

        SYNOPSIS
        ========
        C{searchpath.find('troveSpec1', 'troveSpec2', ..., 'troveSpecN')}
        C{searchpath['troveSpec']}

        DESCRIPTION
        ===========
        The B{SearchPath} is searched for troves which match the given
        troveSpecs.  All matches are included as installed in the
        returned C{TroveSet}.

        Each C{troveSpec} has the same format as a trove referenced on
        the command line: C{name=version[flavor]}

            - L{name} : Required: the full name of the trove
            - L{version} : Optional: Any legal full or partial version,
              with or without a full or partial label.
            - L{flavor} : Optional: The flavor to match, composed with
              the Repository flavor and the build configuration flavor.
        '''
        return self._action(ActionClass = GroupFindAction, *troveSpecs)

    __getitem__ = find

class GroupSearchSourceTroveSet(troveset.SearchSourceTroveSet):
    # This is really GroupSetRecipe.Repository, documented here
    # for the benefit of Repository.find and Repository.latestPackages
    # Specifically, the synopsis describes GroupSetRecipe.Repository,
    # not this underlying object.
    '''
    NAME
    ====
    B{C{Repository}} - Source of trove references

    SYNOPSIS
    ========
    C{r.Repository(defaultLabelList, baseFlavor)}

    DESCRIPTION
    ===========
    A B{Repository} object is used to look troves up in a repository,
    and provide references to those troves as B{TroveSet} objects.
    It has a list of default labels (or a single default label) and
    a default flavor; these are used when no label or flavor is provided
    to the B{find} method.

    METHODS
    =======
        - L{find} : Search the repository for specified troves
        - L{latestPackages} : All the latest normal packages on the
          default label(s)
    '''
    _explainObjectName = 'Repository'

    def find(self, *troveSpecs):
        '''
        NAME
        ====
        B{C{Repository.find}} - Search the Repository for specified troves

        SYNOPSIS
        ========
        C{repos.find('troveSpec1', 'troveSpec2', ..., 'troveSpecN')}
        C{repos['troveSpec']}

        DESCRIPTION
        ===========
        The B{Repository} is searched for troves which match the given
        troveSpecs.  All matches are included as installed in the
        returned C{TroveSet}.

        Each C{troveSpec} has the same format as a trove referenced on
        the command line: C{name=version[flavor]}

            - L{name} : Required: the full name of the trove
            - L{version} : Optional: Any legal full or partial version,
              with or without a full or partial label.
            - L{flavor} : Optional: The flavor to match, composed with
              the Repository flavor and the build configuration flavor.
        '''
        return self._action(ActionClass = GroupFindAction, *troveSpecs)

    __getitem__ = find

    def latestPackages(self):
        '''
        NAME
        ====
        B{C{Repository.latestPackages}} - Get latest normal packages of the
        default flavor on the default label

        SYNOPSIS
        ========
        C{repos.latestPackages()}

        DESCRIPTION
        ===========
        Returns a B{TroveSet} consisting of the latest packages and
        filesets on the default search label.  The troves returned are
        those which best match the default flavor.  Any troves which
        have a redirect as their latest version are not included in
        the returned TroveSet, nor are groups or components.

        A package is considered latest only if it is built from the
        latest source from which some binaries have been built.  So
        if the C{foo:source} package previously built both the C{foo}
        and C{bar} packages, but the most recent binary version of
        the C{bar} package is built from a C{foo:source} that did not
        build a C{bar} package, the C{bar} package previously built
        from C{foo:source} will not be considered latest.  (Thus, a
        redirect from C{bar} to nothing is not required here.)
        '''
        return self._action(ActionClass = LatestPackagesFromSearchSourceAction)

class GroupFindAction(troveset.FindAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupDelayedTupleSetAction(troveset.DelayedTupleSetAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupDifferenceAction(troveset.DifferenceAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupUnionAction(troveset.UnionAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupPatchAction(troveset.PatchAction):

    resultClass = GroupDelayedTroveTupleSet

class GroupUpdateAction(troveset.UpdateAction):

    resultClass = GroupDelayedTroveTupleSet

class ComponentsAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, *componentNames):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.componentNames = set(componentNames)

    def componentsAction(self, data):
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

        return True

    __call__ = componentsAction

class CopyAction(GroupDelayedTupleSetAction):

    def copyAction(self, data):
        self.outSet._setInstall(self.primaryTroveSet._getInstallSet())
        self.outSet._setOptional(self.primaryTroveSet._getOptionalSet())

        return True

    __call__ = copyAction

class CreateGroupAction(GroupDelayedTupleSetAction):

    prefilter = troveset.FetchAction

    def __init__(self, primaryTroveSet, name, checkPathConflicts = True,
                 imageGroup = False, scripts = None):
        if hasattr(scripts, "ts"):
            GroupDelayedTupleSetAction.__init__(self, primaryTroveSet,
                                                scripts.ts)
        else:
            GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)

        self.name = name
        self.checkPathConflicts = checkPathConflicts
        self.imageGroup = imageGroup
        self.scripts = scripts

    def createGroupAction(self, data):
        grp = SG(data.groupRecipe.name,
                 checkPathConflicts = self.checkPathConflicts,
                 imageGroup = self.imageGroup)

        data.groupRecipe._addGroup(self.name, grp)
        data.groupRecipe._setDefaultGroup(grp)

        self._create(data.groupRecipe.defaultGroup,
                     self.primaryTroveSet, self.outSet, data)

        return True

    __call__ = createGroupAction

    def _create(self, sg, ts, outSet, data):
        if self.scripts is not None:
            for script, scriptName in self.scripts.iterScripts():
                sg.addScript(scriptName, script.contents, script.fromClass)

        sg.populate(ts, data.troveCache)

        outSet._setInstall([ (sg.name, versions.NewVersion(),
                              data.groupRecipe.flavor) ])
        outSet.realized = True

    def __str__(self):
        return self.name

class CreateNewGroupAction(CreateGroupAction):

    resultClass = GroupLoggingDelayedTroveTupleSet

    def __init__(self, primaryTroveSet, name, checkPathConflicts = True,
                 scripts = None, imageGroup = False):
        CreateGroupAction.__init__(self, primaryTroveSet, name,
                                   checkPathConflicts = checkPathConflicts,
                                   imageGroup = imageGroup,
                                   scripts = scripts)

    def createNewGroupAction(self, data):
        newGroup = SG(self.name, checkPathConflicts = self.checkPathConflicts,
                      imageGroup = self.imageGroup)
        data.groupRecipe._addGroup(self.name, newGroup)
        self._create(newGroup, self.primaryTroveSet, self.outSet, data)

        return True

    __call__ = createNewGroupAction

class DelayedSearchPathTroveSet(GroupSearchPathTroveSet):

    def __init__(self, troveSetList = None, graph = None, index = None,
                 action = None):
        troveset.SearchPathTroveSet.__init__(self, troveSetList = troveSetList,
                                             graph = graph, index = index)
        self.action = action
        assert(not self.troveSetList)

    def realize(self, data):
        result = self.action(data)
        self.realized = True
        return True

class GroupSearchPathFetch(troveset.DelayedTupleSetAction):

    resultClass = DelayedSearchPathTroveSet

    def __init__(self, *args, **kwargs):
        troveset.DelayedTupleSetAction.__init__(self, *args, **kwargs)

    def groupSearchPathFetch(self, data):
        actionList = []
        needed = self.primaryTroveSet.troveSetList[:]
        while needed:
            ts = needed.pop(0)

            if isinstance(ts, troveset.TroveTupleSet):
                f = troveset.FetchAction(ts)
                actionList.append(f)
            elif isinstance(ts, troveset.SearchPathTroveSet):
                needed += ts.troveSetList

        troveset.FetchAction._fetch(actionList, data)

        self.outSet.setTroveSetList(self.primaryTroveSet.troveSetList)

        return True

    __call__ = groupSearchPathFetch

class DepsNeededAction(GroupDelayedTupleSetAction):

    resultClass = GroupLoggingDelayedTroveTupleSet

    def __init__(self, primaryTroveSet, resolveTroveSet,
                 failOnUnresolved = True):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet,
                                            resolveTroveSet)
        self.failOnUnresolved = failOnUnresolved
        self.resolveTroveSet = resolveTroveSet

    def depsNeededAction(self, data):
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
        if self.resolveTroveSet:
            # might be nice to share a single depDb across all instances
            # of this class?
            resolveMethod = (self.resolveTroveSet._getResolveSource(
                                    depDb = deptable.DependencyDatabase()).
                                        getResolveMethod())
        else:
            resolveMethod = None

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

        return True

    __call__ = depsNeededAction

class GetInstalledAction(GroupDelayedTupleSetAction):

    def getInstalledAction(self, data):
        self.outSet._setInstall(self.primaryTroveSet._getInstallSet())

        return True

    __call__= getInstalledAction

class GetOptionalAction(GroupDelayedTupleSetAction):

    def getOptionalAction(self, data):
        self.outSet._setOptional(self.primaryTroveSet._getOptionalSet())

        return True

    __call__= getOptionalAction

class FindByNameAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, namePattern, emptyOkay = False):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.namePattern = namePattern
        self.emptyOkay = emptyOkay

    def findByNameAction(self, data):

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

        return True

    __call__= findByNameAction

class FindBySourceNameAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, sourceName):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.sourceName = sourceName

    def findBySourceNameAction(self, data):
        troveTuples = (
            list(itertools.izip(itertools.repeat(True),
                           self.primaryTroveSet._getInstallSet())) +
            list(itertools.izip(itertools.repeat(False),
                           self.primaryTroveSet._getOptionalSet())) )

        sourceNames = data.troveCache.getTroveInfo(
                                trove._TROVEINFO_TAG_SOURCENAME,
                                [ x[1] for x in troveTuples ])

        installs = []
        optional = []
        for (isInstallSet, troveTup), sourceName in \
                itertools.izip(troveTuples, sourceNames):
            if sourceName() != self.sourceName:
                continue

            if isInstallSet:
                installs.append(troveTup)
            else:
                optional.append(troveTup)

        self.outSet._setInstall(installs)
        self.outSet._setOptional(optional)

        if (not installs and not optional):
            raise CookError("findBySourceName() matched no trove names")

        return True

    __call__ = findBySourceNameAction

class IsEmptyAction(GroupDelayedTupleSetAction):

    def isEmptyAction(self, data):
        if (self.primaryTroveSet._getInstallSet() or
            self.primaryTroveSet._getOptionalSet()):

            raise CookError("Trove set is not empty")

        # self.outSet is already empty

        return True

    __call__ = isEmptyAction

class IsNotEmptyAction(GroupDelayedTupleSetAction):

    def isNotEmptyAction(self, data):
        if (not self.primaryTroveSet._getInstallSet() and
            not self.primaryTroveSet._getOptionalSet()):

            raise CookError("Trove set is empty")

        self.outSet._setInstall(self.primaryTroveSet._getInstallSet())
        self.outSet._setOptional(self.primaryTroveSet._getOptionalSet())

        return True

    __call__ = isNotEmptyAction

class GroupIncludeAction(troveset.IncludeAction):

    resultClass = GroupLoggingDelayedTroveTupleSet

class LatestPackagesFromSearchSourceAction(GroupDelayedTupleSetAction):

    resultClass = GroupLoggingDelayedTroveTupleSet

    def latestPackageFromSearchSourceAction(self, data):
        troveSource = self.primaryTroveSet.searchSource.getTroveSource()

        # data hiding? what's that
        flavor = self.primaryTroveSet.searchSource.flavor
        labelList = self.primaryTroveSet.searchSource.installLabelPath

        d = { None : {} }
        for label in labelList:
            d[None][label] = [ flavor ]

        matches = troveSource.getTroveLatestByLabel(d,
                troveTypes=netclient.TROVE_QUERY_PRESENT, bestFlavor=True)

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

        return True

    __call__ = latestPackageFromSearchSourceAction

class MakeInstallAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, installTroveSet = None):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet,
                                            installTroveSet)
        self.installTroveSet = installTroveSet

    def makeInstallAction(self, data):
        if self.installTroveSet:
            self.outSet._setOptional(self.primaryTroveSet._getOptionalSet())
            self.outSet._setInstall(
                    (self.installTroveSet._getInstallSet() |
                     self.installTroveSet._getOptionalSet()))
        else:
            self.outSet._setInstall(self.primaryTroveSet._getInstallSet() |
                                    self.primaryTroveSet._getOptionalSet())

        return True

    __call__ = makeInstallAction

class MakeOptionalAction(GroupDelayedTupleSetAction):

    def __init__(self, primaryTroveSet, optionalTroveSet = None):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet,
                                            optionalTroveSet)
        self.optionalTroveSet = optionalTroveSet

    def makeOptionalAction(self, data):
        if self.optionalTroveSet:
            self.outSet._setInstall(self.primaryTroveSet._getInstallSet())
            self.outSet._setOptional(
                    (self.optionalTroveSet._getInstallSet() |
                     self.optionalTroveSet._getOptionalSet() |
                     self.primaryTroveSet._getOptionalSet()))
        else:
            self.outSet._setOptional(self.primaryTroveSet._getInstallSet() |
                                     self.primaryTroveSet._getOptionalSet())

        return True

    __call__ = makeOptionalAction

class MembersAction(GroupDelayedTupleSetAction):

    prefilter = troveset.FetchAction
    justStrong = True
    includeTop = False

    def membersAction(self, data):
        for (troveTuple, installSet) in itertools.chain(
                itertools.izip(self.primaryTroveSet._getInstallSet(),
                               itertools.repeat(True)),
                itertools.izip(self.primaryTroveSet._getOptionalSet(),
                               itertools.repeat(False))):
            installs = []
            available = []

            if self.includeTop:
                if installSet:
                    installs.append(troveTuple)
                else:
                    available.append(troveTuple)

            for (refTrove, byDefault, isStrong) in \
                        data.troveCache.iterTroveListInfo(troveTuple):
                if self.justStrong and not isStrong:
                    continue

                if byDefault:
                    installs.append(refTrove)
                else:
                    available.append(refTrove)

            self.outSet._setInstall(installs)
            self.outSet._setOptional(available)

        return True

    __call__ = membersAction

class FlattenAction(MembersAction):

    justStrong = False
    includeTop = True

    @classmethod
    def Create(klass, primaryTroveSet):
        if hasattr(primaryTroveSet, "_flattened"):
            return primaryTroveSet._flattened

        resultSet = super(FlattenAction, klass).Create(primaryTroveSet)
        primaryTroveSet._flattened = resultSet
        return resultSet

class GroupTroveSetFindAction(troveset.FindAction):

    prefilter = FlattenAction
    resultClass = GroupDelayedTroveTupleSet

    def _applyFilters(self, l):
        assert(len(l) == 1)
        if hasattr(l[0], "_flattened"):
            return [ l[0]._flattened ]

        result = troveset.FindAction._applyFilters(self, l)
        l[0]._flattened = result[0]
        return result

class PackagesAction(GroupDelayedTupleSetAction):

    prefilter = troveset.FetchAction

    def __init__(self, primaryTroveSet):
        GroupDelayedTupleSetAction.__init__(self, primaryTroveSet)

    def packagesAction(self, data):
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

        return True

        return True

    __call__ = packagesAction

class ScriptsAction(GroupDelayedTupleSetAction):

    prefilter = troveset.FetchAction

    def __init__(self, *args, **kwargs):
        GroupDelayedTupleSetAction.__init__(self, *args, **kwargs)

    def getResultTupleSet(self, *args, **kwargs):
        ts = GroupDelayedTupleSetAction.getResultTupleSet(self, *args, **kwargs)
        ts.groupScripts = GroupScripts()
        # this loop is gross. we use it to get the right dependencies on things
        # which use the scripts though
        ts.groupScripts.ts = ts
        return ts

    def scriptsAction(self, data):
        totalSet = (self.primaryTroveSet._getInstallSet() |
                    self.primaryTroveSet._getOptionalSet())
        if not totalSet:
            raise CookError("Empty trove set for scripts()")
        elif len(totalSet) > 1:
            raise CookError("Multiple troves in trove set for scripts()")

        troveTup = list(totalSet)[0]
        trv = data.troveCache.getTroves([ troveTup ])[0]
        groupScripts = self.outSet.groupScripts

        for scriptName in GroupScripts._scriptNames:
            trvScript = getattr(trv.troveInfo.scripts, scriptName[:-7])
            if not trvScript.script():
                continue

            selfScript = getattr(groupScripts, scriptName[:-7])
            selfScript.set(trvScript.script())

        return True

    __call__ = scriptsAction

class SG(_SingleGroup):

    def __init__(self, *args, **kwargs):
        _SingleGroup.__init__(self, *args, **kwargs)
        self.autoResolve = False
        self.depCheck = False

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

class ModelCompiler(modelgraph.AbstractModelCompiler):

    SearchPathTroveSet = GroupSearchPathTroveSet
    FlattenAction = FlattenAction
    IncludeAction = GroupIncludeAction

class GroupScript(object):
    '''
    NAME
    ====
    B{C{Script}} - Specify script contents and compatibility class

    SYNOPSIS
    ========
    C{scriptObj = r.Script('#!/bin/sh...'I{, [fromClass = 1]})}

    DESCRIPTION
    ===========
    A B{C{Script}} object holds the contents, and optionally the
    compatibility class, of a script that can then be attached to
    one or more groups.  The C{Scripts} object associates the
    script with the type, and C{Group} and C{TroveSet.createGroup}
    each take an optional C{scripts=} parameter to associate a
    C{Scripts} object with a group being created.

    EXAMPLE
    =======
    Create a script that attaches to multiple groups as multiple types::
     
     myTroves = repos.find(...)
     fixup = r.Script("""#!/bin/sh
         [ -x /opt/me/fixme ] && /opt/me/fixme""")
     fixscripts = r.Scripts(preUpdate=fixup, preRollback=fixup)
     r.Group(myTroves, scripts=fixscripts)
    '''
    _explainObjectName = 'Script'

    def __init__(self, contents, fromClass = None):
        self.contents = contents
        self.fromClass = fromClass

    def set(self, contents, fromClass = None):
        self.contents = contents
        self.fromClass = fromClass

class GroupScripts(object):
    '''
    NAME
    ====
    B{C{Scripts}} - Associate scripts with types

    SYNOPSIS
    ========
    C{scripts = r.Scripts(postInstall = script, preRollback = script, ...)}

    DESCRIPTION
    ===========
    A C{Script} object holds the contents, and optionally the
    compatibility class, of a script that can then be attached to
    one or more groups.  The B{C{Scripts}} object associates the
    script with the type, and C{Group} and C{TroveSet.createGroup}
    each take an optional C{scripts=} parameter to associate a
    C{Scripts} object with a group being created.

    PARAMETERS
    ==========
    Each of the parameters specifies a script type and takes a C{Script}
    to associate with that script type.

     - C{postInstall} : Specifies a script to run after the installation
       of any group to which this script is attached.
     - C{preRollback} : Specifies a script to run before the rollback
       of any group to which this script is attached.
     - C{postRollback} : Specifies a script to run after the rollback
       of any group to which this script is attached.
     - C{preUpdate} : Specifies a script to run before the update
       of any group to which this script is attached.
     - C{postUpdate} : Specifies a script to run after the update
       of any group to which this script is attached.

    EXAMPLE
    =======
    Create a script that attaches to multiple groups as multiple types::
     
     innerTroves = repos.find(...)
     myTroves = repos.find(...)
     fixup = r.Script("""#!/bin/sh
         [ -x /opt/me/fixme ] && /opt/me/fixme""")
     fixscripts = r.Scripts(preUpdate=fixup, preRollback=fixup)
     innerGroup = innerTroves.createGroup('group-inner', scripts=fixscripts)
     r.Group(myTroves + innerGroup, scripts=fixscripts)

    In general, you will not want to attach the same script to multiple
    groups that will be updated at the same time.  Conary will not
    "de-duplicate" the scripts, and they will be run more than once
    if you do so.
    '''
    _explainObjectName = 'Scripts'
    _scriptNames = ('postInstallScripts', 'preRollbackScripts',
                    'postRollbackScripts', 'preUpdateScripts',
                    'postUpdateScripts')

    def __init__(self, **kwargs):
        for scriptName in self._scriptNames:
            contents = kwargs.pop(scriptName[:-7], None)
            if contents is None:
                contents = GroupScript(None)
            setattr(self, scriptName[:-7], contents)

        if kwargs:
            raise TypeError("GroupScripts() got an unexpected keyword "
                           "argument '%s'" % kwargs.keys()[0])

    def iterScripts(self):
        for scriptName in self._scriptNames:
            script = getattr(self, scriptName[:-7])
            if script is not None and script.contents is not None:
                yield script, scriptName

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
        self.Script = GroupScript
        self.Scripts = GroupScripts

        self.labelPath = [ label ]
        self.buildLabel = label
        self.flavor = flavor
        self.searchSource = searchsource.NetworkSearchSource(
                repos, self.labelPath, flavor)
        self.g = troveset.OperationGraph()
        self.world = GroupSearchSourceTroveSet(self.searchSource,
                                               graph = self.g)

        self.fileFinder = lookaside.FileFinder(self.name, self.laReposCache,
                                               localDirs=self.srcdirs,
                                               multiurlMap=self.multiurlMap,
                                               mirrorDirs=cfg.mirrorDirs,
                                               cfg=cfg)
 

        self._dumpAll = False
        self._trackDict = {}

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

    def _findSources(self, *args, **kwargs):
        # GroupSetRecipe does not implement recursive builds, so just
        # return an empty list -- this allows rmake builds of
        # GroupSetRecipe groups to work.
        return []

    def _realizeGraph(self, cache, callback):
        data = GroupActionData(troveCache = GroupSetTroveCache(self, cache),
                               groupRecipe = self)
        self.g.realize(data)

        ordering = self.g.getTotalOrdering()

        nv = versions.NewVersion()

        allTroveTups = set()
        for node in ordering:
            if not isinstance(node, troveset.TroveTupleSet):
                continue

            allTroveTups.update(node._getInstallSet())
            allTroveTups.update(node._getOptionalSet())

        for outerName in self.getGroupNames():
            allTroveTups.remove( (outerName, nv, self.flavor) )

        for outerName in self.getPrimaryGroupNames():
            grp = self._getGroup(outerName)
            grp.setBuildRefs(allTroveTups
                                - set(grp.iterTroveList(strongRefs = True,
                                                        weakRefs = True))
                                - set((x, nv, self.flavor)
                                        for x in grp.iterNewGroupList()))

    def dumpAll(self):
        '''
        NAME
        ====
        B{C{dumpAll}} - Display copious output describing each action.

        SYNOPSYS
        ========
        C{r.dumpAll()}

        DESCRIPTION
        ===========
        Causes a GroupSetRecipe to print a textual listing of the
        entire contents of each TroveSet as it is populated.

        C{dumpAll} is a debugging tool and does not return a TroveSet.
        '''
        self._dumpAll = True

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
        '''
        NAME
        ====
        B{C{GroupSetRecipe.writeDotGraph}} - write "dot" graph for recipe

        SYNOPSIS
        ========
        C{r.writeDotGraph('path')}

        DESCRIPTION
        ===========
        Writes a description of the internal graph represenstation of
        the elements of the GroupSetRecipe in C{dot} format.  This
        graph can be converted to SVG format using the dot command:

        C{dot -Tsvg outputfile > outputfile.svg}

        The resulting SVG file may be viewed in any tool capable of
        displaying SVG files, including many Web browsers.

        C{writeDotGraph} is a debugging tool and does not return a TroveSet.
        '''
        self.g.generateDotFile(path, edgeFormatFn = lambda a,b,c: c)

    def Group(self, ts, checkPathConflicts = True, scripts = None,
              imageGroup = False):
        '''
        NAME
        ====
        B{C{GroupSetRecipe.Group}} - Create primary group object

        SYNOPSIS
        ========
        C{r.Group(troveSet, checkPathConflicts=True, scripts=None)}

        DESCRIPTION
        ===========
        Set the passed B{TroveSet} as the contents of the primary
        group being built; the group that has the same name as
        the source component. The return value is a troveset which
        references the newly-created primary group.  This returned
        TroveSet can be used to create other groups which reference
        the primary group.

        PARAMETERS
        ==========
         - C{checkPathConflicts} : Raise an error if any paths
           overlap (C{True})
         - C{imageGroup} : (False) Designate that this group is a image group.
           Image Group policies will be executed separately on this group.
         - C{scripts} : Attach one or more scripts specified by a C{Scripts}
           object (C{None})
        '''
        return ts._createGroup(self.name,
                               checkPathConflicts = checkPathConflicts,
                               scripts = scripts, imageGroup = imageGroup)

    def Repository(self, labelList, flavor):
        # Documented in GroupSearchSourceTroveSet as "Repository" so that
        # Repository.find and Repository.latestPackages documentation
        # shows up in cvc explain
        '''
        See Repository.
        '''
        if type(labelList) == tuple:
            labelList = list(tuple)
        elif type(labelList) != list:
            labelList = [ labelList ]

        for i, label in enumerate(labelList):
            if type(label) == str:
                labelList[i] = versions.Label(label)
            elif not isinstance(label, versions.Label):
                raise CookError('String label or Label object expected, got %r'%
                                label)

        if type(flavor) == str:
            flavor = deps.parseFlavor(flavor)

        searchSource = searchsource.NetworkSearchSource(
                                            self.repos, labelList, flavor)
        return GroupSearchSourceTroveSet(searchSource, graph = self.g)

    def SearchPath(self, *troveSets):
        # Documented in GroupSearchPathTroveSet as "SearchPath" so that
        # SearchPath.find documentation shows up in cvc explain
        '''
        See SearchPath.
        '''
        notTroveSets = [repr(x) for x in troveSets
                        if not isinstance(x, troveset.TroveSet)]
        if notTroveSets:
            raise CookError('Invalid arguments %s: SearchPath arguments must be'
                            ' Repository or TroveSet' %', '.join(notTroveSets))
        return GroupSearchPathTroveSet(troveSets, graph = self.g)

    def CML(self, modelText, searchPath = None):
        """
        NAME
        ====
        B{C{GroupSetRecipe.CML}} - Build TroveSet from CML specification


        SYNOPSIS
        ========
        C{r.CML(modelText, searchPath=None)}

        DESCRIPTION
        ===========
        Builds a TroveSet from a specification in Conary Modelling
        Lanuage (CML). The optional C{searchPath} initializes the
        search path; search lines from the system model are prepended
        to any provided C{searchPath}.

        Returns a standard troveset with an extra attribute called
        C{searchPath}, which is a TroveSet representing the final
        SearchPath from the model.  This search path is normally used
        for dependency resolution.

        PARAMETERS
        ==========
         - C{modelText} (Required) : the text of the model in CML
         - C{searchPath} (Optional) : an initial search path, a fallback
           sought after any items provided in the model.

        EXAMPLE
        =======
        To build a group from a system defined in CML, provide
        the contents of the /etc/conary/system-model file as the
        C{modelText}.  This may be completely literal (leading white
        space is ignored in CML)::

         ts = r.CML('''
             search group-os=conary.rpath.com@rpl:2/2.0.1-0.9-30
             install group-appliance-platform
             install httpd
             install mod_ssl
         ''')
         needed = ts.depsNeeded(ts.searchPath)
         finalSet = ts + needed

        If you are using a product definition and want to use the
        search path it provides as the context for the model, it
        might look like this::

         repo = r.Repository('conary.rpath.com@rpl:2', r.flavor)
         # Fetch latest packages on build label first
         searchPathList = [ r.Repository(r.macros.buildlabel, r.flavor) ]
         if 'productDefinitionSearchPath' in r.macros:
             # proper build with product definition
             searchPathList.extend([repo[x] for x in
                 r.macros.productDefinitionSearchPath.split('\\\\n')])
         else:
             # local test build against specific version
             searchPathList.extend(
                 repo['group-os=conary.rpath.com@rpl:2/2.0.1-0.9-30'])
         searchPath = r.SearchPath(*searchPathList)

         ts = r.CML('''
             install group-appliance-platform
             install httpd
             install mod_ssl
         ''', searchPath=searchPath)
         needed = ts.depsNeeded(ts.searchPath)
         finalSet = ts + needed
        """
        if searchPath is None:
            searchSource = searchsource.NetworkSearchSource(
                                            self.repos, [], self.flavor)
            searchPath = GroupSearchSourceTroveSet(searchSource,
                                                   graph = self.g)

        model = cml.CML(None)
        lineNum = findRecipeLineNumber()
        if isinstance(modelText, str):
            modelText = modelText.split('\n')
        model.parse(modelText, context = '(recipe):%d' % lineNum)

        comp = ModelCompiler(self.flavor, self.repos, self.g, searchPath, None)
        sysModelSet = comp.build(model)

        result = sysModelSet._action(ActionClass = CopyAction)
        result.searchPath = sysModelSet.searchPath

        return result

    def track(self, troveSpec):
        '''
        NAME
        ====
        B{C{GroupSetRecipe.track}}

        SYNOPSIS
        ========
        C{r.track('troveSpec')}

        DESCRIPTION
        ===========
        Prints out actions that match the provided C{troveSpec}.  Usually
        used when a trove is unexpectedly present or missing in one or
        more TroveSets (or their resulting groups), in order to learn why
        the trove is present or missing.

        C{track} is a debugging tool and does not return a TroveSet.
        '''
        self._trackDict[parseTroveSpec(troveSpec)] = troveSpec

from conary.build.packagerecipe import BaseRequiresRecipe
exec defaultrecipes.GroupSetRecipe
