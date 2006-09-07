#
# Copyright (c) 2004-2006 rPath, Inc.
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
#
import copy
from itertools import chain, izip

from conary.build.recipe import Recipe, RECIPE_TYPE_GROUP
from conary.build.errors import RecipeFileError, CookError, GroupPathConflicts
from conary.build.errors import GroupDependencyFailure, GroupCyclesError
from conary.build.errors import GroupAddAllError
from conary.build import macros
from conary.build import use
from conary import conaryclient
from conary import callbacks
from conary.deps import deps
from conary import errors
from conary.lib import graph, log, util
from conary.repository import trovesource
from conary import trove
from conary import versions


class _BaseGroupRecipe(Recipe):
    """ Defines a group recipe as collection of groups and provides
        operations on those groups.
    """
    internalAbstractBaseClass = 1
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
    NAME
    ====

    B{C{r.GroupRecipe()}} - Provides the recipe interface for creating a group.

    SYNOPSIS
    ========

    See USER COMMANDS Section

    DESCRIPTION
    ===========
    The C{r.GroupRecipe} class provides the interface for creation of groups
    in a Conary recipe.  A group refers to a collection of troves; the troves
    may be related in purpose to provide a useful functionality, such as a
    group of media-related troves to provide encoding, decoding, and playback
    facilities for various media, for example.  Groups are not required to
    consist of troves with related functionality however, and may contain a
    collection of any arbitrary troves.

    Most C{r.GroupRecipe} user commands accept a B{groupName}
    parameter. This parameter  specifies the group a particular command
    applies to. For example, C{r.add('foo', groupName='group-bar')}
    attempts to add the trove I{foo} to the group I{group-bar}.

    The group specified by B{groupName} must exist, or be created before
    troves may be added to it. The B{groupName} parameter may also be a list
    of groups in which case the command will be applied to all groups.  If
    B{groupName} is not specified, or is None, then the command will apply to
    the current default group.

    PARAMETERS
    ==========
    Several parameters may be set at the time of group creation.  Although
    these parameters are typically passed to C{r.createNewGroup()} for the
    base group, they should be set as variables in the recipe class.

    Note: Setting these parameters affects not only the value for the base
    group, but also the default value for all newly created groups. For
    example, if B{autoResolve} is set to C{True} in the base group, all other
    groups created will have autoResolve set to C{True} by default.

    The following parameters are accepted by C{r.GroupRecipe} with default
    values indicated in parentheses when applicable:

    B{depCheck} : (False) If set to C{True}, Conary will check for dependency
    closure in this group, and raise an error if closure is not found.

    B{autoResolve} : (False) If set to C{True}, Conary will include any extra
    troves needed to make this group dependency complete.

    B{checkOnlyByDefaultDeps} : (True) By default, Conary checks only the
    dependencies of the troves in a group that are installed by default.
    Conary will check the dependencies of B{byDefault} C{False} troves as well
    if this parameter is set to C{True}.

    B{checkPathConflicts} : (True) Conary checks for path conflicts in each
    group by default to ensure that the group can be installed without path
    conflicts.  Setting this parameter to C{False} will disable the check.

    USER COMMANDS
    =============
    The following user commands are applicable in Conary group recipes:

        - L{add} : Adds a trove to a group

        - L{addAll} : Add all troves directly contained in a given reference
        to groupName

        - L{addNewGroup} : Adds one newly created group to another newly
        created group

        - L{addReference} : Adds a reference to a trove

        - L{createGroup} : Creates a new group

        - L{remove} : Removes a trove

        - L{removeComponents} : Define components which should not be
        installed

        - L{Requires} : Defines a runtime requirement for group

        - L{replace} : Replace troves

        - L{setByDefault} : Set troves to be added to group by default

        - L{setDefaultGroup} : Defines default group

        - L{setLabelPath} : Specify the labelPath to search for troves

    """
    Flags = use.LocalFlags
    internalAbstractBaseClass = 1
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
        """
        NAME
        ====

        B{C{r.Requires()}} - Defines a runtime requirement for group

        SYNOPSIS
        ========

        C{r.Requires(I{requirement}, [I{groupName}])}

        DESCRIPTION
        ===========

        The C{r.Requires} command causes a group to have a runtime requirement
        of the trove requirement.

        PARAMETERS
        ==========

        The C{r.Requires()} command accepts the following parameters,
        with default values shown in parentheses:

        B{requirement} : (None) Specifies the group runtime requirement

        B{groupName} : (None) The name of the group to affect

        EXAMPLES
        ========

        C{r.Requires('tmpwatch')}

	Uses C{r.Requires} to specify that the trove C{tmpwatch} must be 
        installed in order for the group to be installed.
        """
        for group in self._getGroups(groupName):
            group.addRequires(requirement)

    def add(self, name, versionStr = None, flavor = None, source = None,
            byDefault = None, ref = None, components = None, groupName = None,
            use = True):
        """
        NAME
        ====

        B{C{r.add()}} - Adds a trove to a group

        SYNOPSIS
        ========

        C{r.add(I{name}, [I{versionStr},] [I{flavor},] [I{source},] [I{byDefault},] [I{ref},] [I{components},] [I{groupName}])}

        DESCRIPTION
        ===========

        The C{r.add()} command is used to add a trove to a group.

        PARAMETERS
        ==========

        The C{r.add()} command accepts the following parameters, with
        default values shown in parentheses:

        B{byDefault} : (None, or value of B{createNewGroup}) Specifies whether
        to include a trove  by defaultt. Defaults to the B{byDefault} setting
        as  defined with B{createNewGroup}.

        B{components} : (None) Specify a set of trove components to include.
        Only relevant when adding packages.  Specified as a list,
        such as C{r.add('foo', components=['runtime', 'lib'])}.

        B{flavor} : (None) A flavor limiter such as that passed to
        B{repquery} which determines the trove returned.

        B{groupName} : (None) The group to add trove to.

        B{name} : (None) Specifies the name of trove to add- This parameter is
        required.

        B{ref} : (None) Trove reference to search for this trove in. See
        C{r.addReference} for more information.

        B{source} : (None) Specifies the source from which this trove
        originates for programs which read group recipes.
        This parameter's explicit use is generally unnecessary.

        B{versionStr} : (None) A version specifier like that passed to

        B{repquery} which determines the trove returned.
        
        B{use}: (True) A Use flag, or boolean, or a tuple of Use flags, and/or
        boolean values which determine whether the trove(s) are added to the
        group

        EXAMPLES
        ========

        C{r.add('gzip:runtime')}

        Adds the C{gzip:runtime} trove to the current group.
        """
        if not use:
            return
        flavor = self._parseFlavor(flavor)
        for group in self._getGroups(groupName):
            group.addSpec(name, versionStr = versionStr, flavor = flavor,
                          source = source, byDefault = byDefault, ref = ref,
                          components = components)

    # maintain addTrove for backwards compatability
    addTrove = add

    def remove(self, name, versionStr = None, flavor = None, groupName = None):
        """
        NAME
        ====

        B{C{r.remove()}} - Removes a trove

        SYNOPSIS
        ========

        C{r.remove(I{name}, [I{flavor},] [I{groupName},] [I{versionStr}])}

        DESCRIPTION
        ===========

        The C{r.remove} command removes a trove from the group which was
        previously added with C{r.addAll}, or C{addTrove} commands.

        Note: If the trove is not included explicitly, such as by C{r.add()},
        but rather implicitly, as a component in a package which has been
        added, then removing the trove only changes its B{byDefault} setting,
        so that installing this group will not install the trove.

        Troves may be removed from a super group which are present due to an
        included subgroup. For example, the group I{group-os} is a top
        level group, and includes I{group-dist}, which in turn, includes
        package I{foo}.

        Using C{r.remove('foo', groupName='group-os')} prevents installation
        of package I{foo} during the installation of the group I{group-os}.

        PARAMETERS
        ==========

        The C{r.remove()} command accepts the following parameters, with
        default values shown in parentheses:

        B{flavor} : (None) A flavor limiter such as that passed to
        B{repquery} which determines the trove returned.

        B{groupName} : (None) The name of the group to remove trove from

        B{name} : (None) The name of the trove to be removed. This parameter
        is required.

        B{versionStr} : (None) A version specifier like that passed to
        B{repquery} which determines the trove returned.

        EXAMPLES
        ========

        C{r.remove('kernel:configs', flavor='kernel.smp')}

        Removes the trove C{kernel:configs} from the current group for the
        flavor C{kernel.smp}.
        """
        flavor = self._parseFlavor(flavor)
        for group in self._getGroups(groupName):
            group.removeSpec(name, versionStr = versionStr, flavor = flavor)

    def removeComponents(self, componentList, groupName = None):
        """
        NAME
        ====

        B{C{r.removeComponents()}} - Define components which should not be
        installed by default

        SYNOPSIS
        ========

        C{r.removeComponents(I{componentList}, [I{groupName}])}

        DESCRIPTION
        ===========

        The C{r.removeComponents} command specifies components which should
        not be installed by default when installing the group.

        PARAMETERS
        ==========

        The C{r.removeComponents()} command accepts the following parameters,
        with default values shown in parentheses:

        B{componentList} : (None) A list of components which should not be
        installed by default when the group is installed

        B{groupName} : (None) The name of the group to affect

        EXAMPLES
        ========

        C{r.removeComponents(['devel', 'devellib'])}

        Uses C{r.RemoveComponents} to specify that the C{:devel} and
        C{:devellib} components should not be installed by default.
        """
        if not isinstance(componentList, (list, tuple)):
            componentList = [ componentList ]
        for group in self._getGroups(groupName):
            group.removeComponents(componentList)

    def setByDefault(self, byDefault = True, groupName = None):
        """
        NAME
        ====

        B{C{r.setByDefault()}} - Set troves to be added to group by default

        SYNOPSIS
        ========

        C{r.setByDefault(I{byDefault}, [I{groupName}])}

        DESCRIPTION
        ===========

        The C{r.setByDefault} command specifies whether troves are added to
        the group by default.

        PARAMETERS
        ==========

        The C{r.setByDefault()} command accepts the following parameters,
        with default values shown in parentheses:

        B{byDefault} : (Current group setting) Whether to add troves to this
        group byDefault C{True}, or byDefault C{False} by default.

        B{groupName} : (None) The name of the group to affect

        EXAMPLES
        ========

        C{r.setByDefault(False, groupName='group-ftools')}

        Specifies troves are not added to the group C{group-ftools} by default.
        """
        for group in self._getGroups(groupName):
            group.setByDefault(byDefault)

    def addAll(self, name, versionStr = None, flavor = None, ref = None,
                           recurse=True, groupName = None, use = True):
        """
        NAME
        ====

        B{C{r.addAll()}} - Add all troves directly contained in a given
        reference to groupName

        SYNOPSIS
        ========

        C{r.addAll(I{name}, [I{flavor},] [I{groupName},] [I{recurse},]
        [I{ref},] [I{versionStr}])}

        DESCRIPTION
        ===========

        The C{r.addAll()} command used to add all troves directly contained
        in a given reference to B{groupName} to the recipe.

        For example, if the cooked I{group-foo} contains references to the
        troves  C{foo1=<version>[flavor]}, and C{foo2=<version>[flavor]}, the
        entries followed by C{r.addAll(name, versionStr, flavor)} would be
        equivalent to adding the C{r.addTrove} lines:

        C{r.add('foo1', <version>)}
        C{r.add('foo2', <version>)}.

        PARAMETERS
        ==========

        The C{r.addAll()} command accepts the following parameters, with
        default values shown in parentheses:

        B{groupName} : (None) The group to add trove to

        B{recurse} : (True) If True, and the trove you specify with B{addAll}
        contains groups, new groups will be created in the recipe that match
        those contained groups, and the C{r.addAll()} command is recursed on
        those groups.

        Note: If the subgroups already exist in the group, those preexisting
        groups will be used.  Otherwise, the default settings will be used
        when creating any new groups.

        B{ref}: (None) Trove reference to search in for this trove. See
        C{r.addReference()} for more information.

        B{use}: (True) A Use flag, or boolean, or a tuple of Use flags, and/or
        boolean values which determine whether the trove(s) are added to the
        group

        EXAMPLES
        ========

        C{r.addAll('group-core', 'conary.rpath.com@rpl:1')}

	Uses C{r.addAll} to add the troves referenced by C{group-core} to
	the recipe for version string 'conary.rpath.com@rpl:1'.
        """
        if not use:
            return
        flavor = self._parseFlavor(flavor)

        for group in self._getGroups(groupName):
            group.addAll(name, versionStr, flavor, ref = ref, recurse = recurse)

    def addNewGroup(self, name, groupName = None, byDefault = True, use = True):
        """
        NAME
        ====

        B{C{r.addNewGroup()}} - Adds one newly created group to another newly
        created group

        SYNOPSIS
        ========

        C{r.addNewGroup(I{name,} [I{byDefault},] [I{groupName}])}

        DESCRIPTION
        ===========

        The C{r.addNewGroup()} commmand is used to add one newly created group
        to another newly created group.

        PARAMETERS
        ==========

        The C{r.addNewGroup()} command accepts the following parameters, with
        default values shown in parentheses:

        B{name} : (None) The name of group to add

        B{byDefault}: (True) Whether to add this group by default.

        B{groupName} : (Current group name) The name(s) of group(s) to add
        this trove to.

        B{use}: (True) A Use flag, or boolean, or a tuple of Use flags, and/or
        boolean values which determine whether the trove(s) are added to the
        group


        EXAMPLES
        ========

        C{r.addNewGroup('group-3d', groupName='group-graphics')}

        Adds the group C{group-3d} to the group C{group-graphics}.
        """
        if not use:
            return
        #FIXME: this should default to whatever the current byDefault default
        # is!
        if not self._hasGroup(name):
            raise RecipeFileError, 'group %s has not been created' % name

        for group in self._getGroups(groupName):
            if group.name == name:
                raise RecipeFileError, 'group %s cannot contain itself' % name
            group.addNewGroup(name, byDefault, explicit = True)

    def setDefaultGroup(self, groupName):
        """
        NAME
        ====

        B{C{r.setDefaultGroup()}} - Defines default group

        SYNOPSIS
        ========

        C{r.setDefaultGroup(I{groupName})}

        DESCRIPTION
        ===========

        The C{r.setDefaultGroup} command specifies the current group which all
        commands will apply to if no B{groupName} is specified as a parameter
        to a given command.

        PARAMETERS
        ==========

        The C{r.setDefaultGroup()} command accepts the following parameters,
        with default values shown in parentheses:

        B{groupName} : (None) The name of the group to specify as the default.

        EXAMPLES
        ========

        C{r.setDefaultGroup('group-consmod')}

        Defines the default group as C{group-consmod}.
        """
        self._setDefaultGroup(self._getGroup(groupName))

    def addReference(self, name, versionStr = None, flavor = None, ref = None):
        """
        NAME
        ====

        B{C{r.addReference}} - Adds a reference to a trove

        SYNOPSIS
        ========

        C{r.addReference(I{name}, [I{flavor},] [I{ref},] [I{versionStr}])}

        DESCRIPTION
        ===========

        The C{r.addReference} command adds a reference to a trove,
        (usually a group trove) which may then be passed to future invocations
        of C{r.add} or C{r.addAll} commands as the reference parameter.

        Passing in a reference will cause affected commands to search for the
        trove to be added in the reference.

        PARAMETERS
        ==========

        The C{r.addReference()} command accepts the following parameters, with
        default values shown in parentheses:

        B{flavor} : (None) A flavor limiter such as that passed to
        B{repquery} which determines the trove returned.

        B{name} : (None) The name of the reference to add

        B{ref} : (None) Trove reference to search for this trove in. See
        C{r.addReference} for more information.

        B{versionStr} : (None) A version specifier like that passed to
        B{repquery} which determines the trove returned.

        EXAMPLES
        ========

	C{coreRef = r.addReference('group-core', 'conary.rpath.com@rpl:1')}
	C{r.add('tmpwatch', ref=coreRef)}

        Uses C{r.addReference} to Define C{coreRef} as a reference to the
	group-trove C{group-core} for version string 'conary.rpath.com@rpl:1',
	and then uses an C{r.add} invocation to add C{tmpwatch} using the
	C{coreRef} reference.
        """
        flavor = self._parseFlavor(flavor)
        return GroupReference(((name, versionStr, flavor),), ref)

    def replace(self, name, newVersionStr = None, newFlavor = None, ref = None,
                groupName = None):
        """
        NAME
        ====

        B{C{r.replace()}} - Replace troves

        SYNOPSIS
        ========

        C{r.replace(I{name}, [I{groupName},] [I{newFlavor},] [I{newVersionStr}])}

        DESCRIPTION
        ===========

        The C{r.replace} command replaces all troves with a particular name 
        with a new version of the trove.

        Note: By default, C{r.replace()} affects B{all} groups; this behavior
        is different from other group commands.

        PARAMETERS
        ==========

        The C{r.replace()} command accepts the following parameters,
        with default values shown in parentheses:

        B{name} : (None) Specify name of the trove to replace

        B{groupName} : (None) The name of the group to affect

        B{newFlavor} : (None) The new flavor to add

        B{newVersionStr} : (None) The new version to add

        B{ref} : (None) The trove reference to search for the trove in

        EXAMPLES
        ========

        r.replace('distro-release')

	Uses C{r.replace} to remove all instances of the C{distro-release}
	trove, and replaces them with a new version of C{distro-release}.
        """
        newFlavor = self._parseFlavor(newFlavor)
        if groupName is None:
            self.replaceSpecs.append(((name, newVersionStr, newFlavor), ref))
        else:
            for group in self._getGroups(groupName):
                group.replaceSpec(name, newVersionStr, newFlavor, ref)

    def iterReplaceSpecs(self):
        return iter(self.replaceSpecs)

    def setLabelPath(self, *path):
        """
        NAME
        ====

        B{C{r.setLabelPath()}} - Specify the labelPath to search for troves

        SYNOPSIS
        ========

        C{r.setLabelPath(I{pathspec})}

        DESCRIPTION
        ===========

        The C{r.setLabelPath} command specifies the labelPath used to search
        for troves.

        PARAMETERS
        ==========

        The C{r.setLabelPath()} command accepts the following parameters,
        with default values shown in parentheses:

        B{pathspec} : (None) The path to set as labelPath

        EXAMPLES
        ========

        C{r.setLabelPath('myproject.rpath.org@rpl:1', 'conary.rpath.com@rpl:1')}

	Uses C{r.setLabelPath} to specify troves are to be sought in the
	LabelPaths 'myproject.rpath.org@rpl:1' and 'conary.rpath.com@rpl:1'.
        """
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
        """
        NAME
        ====

        B{C{r.createGroup()}} - Creates a new group

        SYNOPSIS
        ========

        C{r.createGroup(I{groupName}, [I{autoResolve},] [I{byDefault},] [I{checkOnlyByDefaultDeps},] [I{checkPathConflicts},] [I{depCheck}])}

        DESCRIPTION
        ===========

        The C{r.createGroup} command creates a new group.

        PARAMETERS
        ==========

        The C{r.createGroup()} command accepts the following parameters, with
        default values shown in parentheses:

        B{autoResolve} : (current group setting) Whether to resolve
        dependencies for this group.

        B{byDefault} : (Current group setting) Whether to add troves to this
        group byDefault C{True}, or byDefault C{False} by default.

        B{checkOnlyByDefaultDeps} :  (Current group setting) Whether to
        include byDefault C{False} troves in this group.

        B{checkPathConflicts} :  (Current group setting) Whether to check path
        conflicts for this group.

        B{depCheck} : (Current group setting) Whether to check for dependency
        closure for this group.

        B{groupName} : (None) The name of the group to be created. Must start
        with 'group-'.

        EXAMPLES
        ========

        C{r.createGroup('group-ftools')}

        Creates the group C{group-ftools}.

        C{r.createGroup('group-multiplay', autoResolve=False)}

        Creates the group C{group-multiplay} and specifies no dependencies are
        resolved automatically for this group.
        """
        if self._hasGroup(groupName):
            raise RecipeFileError, 'group %s was already created' % groupName
        elif not groupName.startswith('group-'):
            raise RecipeFileError, 'group names must start with "group-"'

        origGroup = self._getDefaultGroup()
        if byDefault is None:
            byDefault = origGroup.byDefault

        if checkOnlyByDefaultDeps is None:
            checkOnlyByDefaultDeps = origGroup.checkOnlyByDefaultDeps

        if checkPathConflicts is None:  
            checkPathConflicts = origGroup.checkPathConflicts

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
        self.addAllTroveList = []

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

    def addNewGroup(self, name, byDefault = None, explicit = True,
                    childDefaults=None):

        if not childDefaults:
            childDefaults = []
        elif not isinstance(childDefaults, list):
            childDefaults = [ childDefaults ]

        if name in self.newGroupList:
            (oldByDefault, oldExplicit,
             oldChildDefaults) = self.newGroupList[name]
            byDefault = oldByDefault or byDefault
            explicit = oldExplicit or explicit
            childDefaults = childDefaults + oldChildDefaults

        self.newGroupList[name] = (byDefault, explicit, childDefaults)

    def iterNewGroupList(self):
        for (name, (byDefault, explicit, childDefaults)) \
                                            in self.newGroupList.iteritems():
            yield name, byDefault, explicit

    def hasNewGroup(self, name):
        return name in self.newGroupList

    def setByDefault(self, byDefault):
        self.byDefault = byDefault

    def getByDefault(self):
        return self.byDefault

    def checkAddAllForByDefault(self, parent, troveTup):
        """
            @param parent: name of parent new group or troveTup of the
            parent trove that may have been added using addAll()
            @param troveTup: child (n,v,f) tuple to be checked for a byDefault
            settings.
            @return True, None, or False, depending on whether an addAll
            function has a byDefault True or False setting for troveTup.
        """
        # parent is a troveTup or a new group that may have been
        # added/created with r.addAll().  When r.addAll() is used,
        # the original version of the trove may have settings related
        # to child weak references.  We want to duplicate those in the
        # new group.  This function returns

        includeByDefault = None
        if isinstance(parent, str) and parent.startswith('group-'):
            byDefaultTroves = self.newGroupList[parent][2]
        else:
            byDefaultTroves = self.troves[parent][3]

        for trv in byDefaultTroves:
            if trv.hasTrove(*troveTup):
                includeByDefault = trv.includeTroveByDefault(*troveTup)
                if includeByDefault:
                    return True
        return includeByDefault

    # below here are function used to get/set the troves found
    #

    def addTrove(self, troveTup, explicit, byDefault, components,
                 childDefaults=None):
        assert(isinstance(byDefault, bool))
        if not childDefaults:
            childDefaults = []
        elif not isinstance(childDefaults, list):
            childDefaults = [childDefaults]


        if troveTup in self.troves:
            # if you add a trove twice, once as explicit and once
            # as implict, make sure it stays explicit, same w/
            # byDefault.
            (oldExplicit, oldByDefault, oldComponents, oldChildDefaults) = self.troves[troveTup]
            explicit = explicit or oldExplicit
            byDefault = byDefault or oldByDefault
            if oldComponents:
                components = components + oldComponents
            childDefaults = oldChildDefaults + childDefaults

        self.troves[troveTup] = (explicit, byDefault, components, childDefaults)

    def delTrove(self, name, version, flavor):
        (explicit, byDefault, comps, childByDefaults) \
                                        = self.troves[name, version, flavor]
        if explicit:
            del self.troves[name, version, flavor]
        else:
            self.troves[name, version, flavor] = (False, False, comps,
                                                  childByDefaults)

    def setSize(self, size):
        self.size = size

    def getSize(self):
        return self.size

    def iterTroveList(self, strongRefs=False, weakRefs=False):
        if not (strongRefs or weakRefs):
            strongRefs = weakRefs = True

        for troveTup, (explicit, byDefault,
                        comps, childByDefaults) in self.troves.iteritems():
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
        for troveTup, (explicit, byDefault, comps,
                                 childByDefaults) in self.troves.iteritems():
            yield troveTup, explicit, byDefault, comps

    def iterDefaultTroveList(self):
        for troveTup, (explicit, byDefault, comps, childByDefaults) \
                                                  in self.troves.iteritems():
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
    def __init__(self, repos, callback=None):
        self.repos = repos
        if not callback:
            callback = callbacks.CookCallback()
        self.callback = callback

    def cacheTroves(self, troveTupList):
        troveTupList = [x for x in troveTupList if x not in self]
        if not troveTupList:
            return
        self.callback.gettingTroveDefinitions(len(troveTupList))
        troves = self.repos.getTroves(troveTupList, withFiles=False,
                                      callback = self.callback)

        for troveTup, trv in izip(troveTupList, troves):
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
                if isStrong and not trv.hasTrove(*childChildTup):
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


def buildGroups(recipeObj, cfg, repos, callback):
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


    if callback is None:
        callback = callbacks.CookCallback()

    cache = TroveCache(repos, callback)

    labelPath = recipeObj.getLabelPath()
    flavor = recipeObj.getSearchFlavor()

    # find all the groups needed for all groups in a few massive findTroves
    # calls.
    replaceSpecs = list(recipeObj.iterReplaceSpecs())
    log.info('Getting initial set of troves for'
             ' building all %s groups' % (len(recipeObj.iterGroupList())))
    troveMap = findTrovesForGroups(repos, recipeObj.iterGroupList(),
                                   replaceSpecs,
                                   labelPath, flavor, callback)
    troveTupList = list(chain(*chain(*(x.values() for x in troveMap.itervalues()))))
    cache.cacheTroves(troveTupList)
    log.info('Troves cached.')

    groupsWithConflicts = {}

    newGroups = processAddAllDirectives(recipeObj, troveMap, cache, repos)

    groupList = _sortGroups(recipeObj.iterGroupList())

    for group in groupList:
        for (troveSpec, ref) in replaceSpecs:
            group.replaceSpec(*(troveSpec + (ref,)))

    for groupIdx, group in enumerate(groupList):
        log.info('Building %s (%s of %s)...' % (group.name, groupIdx + 1,
                                                len(groupList)))
        callback.buildingGroup(group.name, groupIdx + 1, len(groupList))

        childGroups = recipeObj.getChildGroups(group.name)

        # check to see if any of our children groups have conflicts,
        # if so, we won't bother building up this group since it's
        # bound to have a conflict as well.
        badGroup = False
        for childGroup, byDefault, isExplicit in childGroups:
            if byDefault and childGroup.name in groupsWithConflicts:
                badGroup = True
                # mark this group as having a conflict
                groupsWithConflicts[group.name] = []
                break
        if badGroup:
            continue

        # add troves to this group.
        addTrovesToGroup(group, troveMap, cache, childGroups, repos)

        log.debug('Troves in %s:' % group.name)
        for troveTup, isStrong, byDefault, _ in sorted(group.iterTroveListInfo()):
            extra = ''
            if not byDefault:
                extra += '[NotByDefault]'
            if not isStrong:
                extra += '[Weak]'
            log.debug(' %s=%s[%s] %s' % (troveTup + (extra,)))

        if group.autoResolve:
            callback.done()
            log.info('Resolving dependencies...')
            resolveGroupDependencies(group, cache, cfg, 
                                     repos, labelPath, flavor, callback)

        if group.depCheck:
            callback.done()
            log.info('Checking for dependency closure...')
            failedDeps = checkGroupDependencies(group, cfg, cache, callback)
            if failedDeps:
                raise GroupDependencyFailure(group.name, failedDeps)

        addPackagesForComponents(group, repos, cache)
        checkForRedirects(group, repos, cache, cfg.buildFlavor)

        callback.done()
        log.info('Calculating size and checking hashes...')
        conflicts = calcSizeAndCheckHashes(group, cache, callback)

        if conflicts:
            groupsWithConflicts[group.name] = conflicts

        if group.isEmpty():
            raise CookError('%s has no troves in it' % group.name)

        callback.groupBuilt()
        log.info('%s built.\n' % group.name)

    if groupsWithConflicts:
        raise GroupPathConflicts(groupsWithConflicts)



def findTrovesForGroups(repos, groupList, replaceSpecs, labelPath, 
                        searchFlavor, callback):
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

        for (troveSpec, ref) in group.iterReplaceSpecs():
            toFind.setdefault(ref, set()).add(troveSpec)

    results = {}

    callback.findingTroves(len(list(chain(*toFind.itervalues()))))
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
            raise CookError, str(e)

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
                                        explicit = True, childDefaults = trv)

                if troveTup not in createdGroups:
                    stack.append((groupTrvDict[troveTup], childGroup))
                    createdGroups.add(troveTup)
            else:
                parentGroup.addTrove(troveTup, True, byDefault, [],
                                     childDefaults=trv)
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
            log.info('Removing %s=%s[%s]' % troveTup)
            group.delTrove(*troveTup)
            groupAsSource.delTrove(*troveTup)

        # replace troves
        toReplaceSpecs = dict(((x[0][0], None, None), x) for x in replaceSpecs)

        toReplace = groupAsSource.findTroves(None, toReplaceSpecs,
                                            allowMissing=True)
        replaceSpecsByName = {}
        for troveSpec, ref in replaceSpecs:
            replaceSpecsByName.setdefault(troveSpec[0], []).append((troveSpec,
                                                                    ref))

        for troveName, replaceSpecs in replaceSpecsByName.iteritems():
            troveTups = toReplace.get((troveName, None, None), [])

            if not troveTups:
                continue

            allComponents = set()
            byDefault = False
            for troveTup in troveTups:
                log.info('Removing %s=%s[%s] due to replaceSpec' % troveTup)
                if allComponents is not None:
                    components = group.getComponents(*troveTup)
                    if not components:
                        allComponents = None
                    else:
                        allComponents.update(components)

                byDefault = byDefault or group.includeTroveByDefault(*troveTup)
                group.delTrove(*troveTup)
                groupAsSource.delTrove(*troveTup)

            for troveSpec, ref in replaceSpecs:
                for newTup in troveMap[ref][troveSpec]:
                    log.info('Adding %s=%s[%s] due to replaceSpec' % newTup)
                    group.addTrove(newTup, True, byDefault, allComponents)
                    groupAsSource.addTrove(*newTup)

    # add implicit troves
    # first from children of explicit troves.
    componentsToRemove = group.getComponentsToRemove()


    for (troveTup, explicit,
         byDefault, components) in list(group.iterTroveListInfo()):
        assert(explicit)

        if cache.isRedirect(troveTup):
            # children of redirect troves are special, and not included.
            continue

        for (childTup, childByDefault, _) in cache.iterTroveListInfo(troveTup):
            childName = childTup[0]

            addAllDefault = group.checkAddAllForByDefault(troveTup, childTup)
            if addAllDefault is not None:
                childByDefault = addAllDefault
            else:
                childByDefault = childByDefault and byDefault

            if componentsToRemove and _componentMatches(childName,
                                                        componentsToRemove):
                childByDefault = False

            if components:
                if _componentMatches(childName, components):
                    childByDefault = byDefault
                else:
                    childByDefault = False

            group.addTrove(childTup, False, childByDefault, [])

    # add implicit troves from new groups (added with r.addNewGroup())
    for childGroup, childByDefault, grpIsExplicit in childGroups:
        if grpIsExplicit:
            for (troveTup, explicit, childChildByDefault, comps) \
                                        in childGroup.iterTroveListInfo():
                addAllByDefault = group.checkAddAllForByDefault(childGroup.name,
                                                                troveTup)
                if addAllByDefault is not None:
                    childChildByDefault = addAllByDefault
                else:
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
        for troveTup in findAllWeakTrovesToRemove(group, troveTups, cache,
                                                  childGroups):
            group.delTrove(*troveTup)

def findAllWeakTrovesToRemove(group, primaryErases, cache, childGroups):
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
    groups = [group] + [ x[0] for x in childGroups ] 
    for thisGroup in groups:
        for troveTup in chain(thisGroup.iterTroveList(strongRefs=True), 
                              troveQueue):
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


def checkForRedirects(group, repos, troveCache, buildFlavor):
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

        allTargets = [ (x[0], str(x[1]), x[2])
                                for x in trv.iterRedirects() ]
        matches = repos.findTroves([], allTargets, buildFlavor)
        for troveList in matches.values():
            targets += troveList
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
    raise CookError, ("""\
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



def resolveGroupDependencies(group, cache, cfg, repos, labelPath, flavor, 
                             callback):
    """ 
        Add in any missing dependencies to group
    """
    callback.groupResolvingDependencies()

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
        troveList = group.iterDefaultTroveList()
    else:
        troveList = group.iterTroveList()
    
    # build a list of the troves that we're checking so far
    troves = [ (n, (None, None), (v, f), True) for (n,v,f) in troveList
                if not ((n,v,f) in cache and cache.isRedirect((n,v,f)))]

    # there's nothing worse than seeing a bunch of nice group debugging
    # information and then having your screen filled up with all 
    # of the update code's debug mess.  Until that logging is moved
    # to it's own private location, turn it off.
    resetVerbosity = (log.getVerbosity() == log.LOWLEVEL)
    if resetVerbosity:
        log.setVerbosity(log.DEBUG)
    updJob, suggMap = client.updateChangeSet(troves, recurse = False,
                                             resolveDeps = True,
                                             test = True,
                                             checkPathConflicts=False)
    if resetVerbosity:
        log.setVerbosity(log.LOWLEVEL)

    for trove, needs in suggMap.iteritems():
        if cfg.fullVersions:
            verStr = trove[1]
        else:
            verStr = trove[1].trailingRevision()

        if cfg.fullFlavors:
            flavorStr = '[%s]' % trove[2]
        else:
            flavorStr = ''

        log.info("%s=%s%s resolves deps by including:" % (trove[0], verStr, 
                                                          flavorStr))

        for item in needs:
            if cfg.fullVersions:
                verStr = item[1]
            else:
                verStr = item[1].trailingRevision()

            if cfg.fullFlavors:
                flavorStr = '[%s]' % item[2]
            else:
                flavorStr = ''

            log.info("\t%s=%s%s" % (item[0], verStr, flavorStr))

    neededTups = list(chain(*suggMap.itervalues()))

    byDefault = group.getByDefault()
    for troveTup in neededTups:
        if group.hasTrove(*troveTup):
            explicit = False
        else:
            explicit = True
        group.addTrove(troveTup, explicit, byDefault, [])

    cache.cacheTroves(neededTups)
    callback.done()

def checkGroupDependencies(group, cfg, cache, callback):
    callback.groupCheckingDependencies()
    if group.checkOnlyByDefaultDeps:
        troveList = group.iterDefaultTroveList()
    else:
        troveList = group.iterTroveList()

    jobSet = [ (n, (None, None), (v, f), False) for (n,v,f) in troveList
                if not ((n,v,f) in cache and cache.isRedirect((n,v,f))) ]

    cfg = copy.deepcopy(cfg)
    cfg.dbPath = ':memory:'
    cfg.root   = ':memory:'

    client = conaryclient.ConaryClient(cfg)
    if group.checkOnlyByDefaultDeps:
        cs = client.createChangeSet(jobSet, recurse = False, withFiles = False,
                                    callback = callback)
    else:
        cs = client.repos.createChangeSet(jobSet, recurse = False,
                                          withFiles = False,
                                          callback = callback)

    jobSet = cs.getJobSet()
    trvSrc = trovesource.ChangesetFilesTroveSource(client.db)
    trvSrc.addChangeSet(cs, includesFileContents = False)
    failedDeps = client.db.depCheck(jobSet, trvSrc)[0]
    callback.done()
    return failedDeps

def calcSizeAndCheckHashes(group, troveCache, callback):
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
        # we've got the sets of conflicting troves, now
        # determine the set of conflicting files
        trovesWithFiles = {}

        conflictsWithFiles = []
        for conflictSet in conflicts:
            needed = [ x for x in conflictSet if x not in trovesWithFiles ]
            troves = troveCache.repos.getTroves(needed, withFiles=True)
            trovesWithFiles.update(dict(izip(needed, troves)))
            conflicting = set(x[1] for x \
                              in trovesWithFiles[conflictSet[0]].iterFileList())
            for tup in conflictSet[1:]:
                conflicting &= set(x[1] for x in \
                                trovesWithFiles[tup].iterFileList())

            conflictsWithFiles.append((conflictSet, conflicting))

        return conflictsWithFiles

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

    if checkPathConflicts:
        count = 0
        callback.groupCheckingPaths(count)

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

            count += 1
            if count % 10 == 0:
                callback.groupCheckingPaths(len(allPathHashes))


    group.setSize(size)

    if checkPathConflicts:
        callback.groupCheckingPaths(len(allPathHashes))
        pathHashCount = len(allPathHashes)
        allPathHashes = set(allPathHashes)
        uniquePathHashCount = len(allPathHashes)
        if pathHashCount != uniquePathHashCount:
            numConflicts = pathHashCount - uniquePathHashCount
            callback.groupDeterminingPathConflicts(numConflicts)
            conflicts = _getHashConflicts(group, troveCache)
            return conflicts
        else:
            callback.done()
