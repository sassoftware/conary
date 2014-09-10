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


BaseRequiresRecipe = '''
class BaseRequiresRecipe(Recipe):
    """
    NAME
    ====
    B{C{BaseRequiresRecipe}} - Base class which provides basic buildRequires
    for all recipes that follow the PackageRecipe approach to instantiating
    a destination directory.

    SYNOPSIS
    ========
    C{BaseRequiresRecipe} is inherited by the other *PackageRecipe,
    DerivedPackageRecipe and *InfoRecipe super classes.

    DESCRIPTION
    ===========
    The C{BaseRequiresRecipe} class provides Conary recipes with references to
    the essential troves which offer Conary's packaging requirements.
    (python, sqlite, and conary)

    Other PackageRecipe classes such as C{AutoPackageRecipe} inherit the
    buildRequires offered by C{BaseRequiresRecipe}.
    """
    name = "baserequires"
    internalAbstractBaseClass = 1
    buildRequires = [
        'bash:runtime',
        'conary-build:lib',
        'conary-build:python',
        'conary-build:runtime',
        'conary:python',
        'conary:runtime',
        'coreutils:runtime',
        'dev:runtime',
        'filesystem:runtime',
        'findutils:runtime',
        'gawk:runtime',
        'grep:runtime',
        'python:lib',
        'python:runtime',
        'sed:runtime',
        'setup:runtime',
        'sqlite:lib',
    ]
    _recipeType = None
'''

PackageRecipe = '''class PackageRecipe(SourcePackageRecipe, BaseRequiresRecipe):
    """
    NAME
    ====
    B{C{PackageRecipe}} - Base class which provides Conary functionality

    SYNOPSIS
    ========
    C{PackageRecipe} is inherited by the other *PackageRecipe super classes

    DESCRIPTION
    ===========
    The C{PackageRecipe} class provides Conary recipes with references to
    the essential troves which offer Conary's packaging requirements.
    (python, sqlite, gzip, bzip2, tar, cpio, and patch)

    Other PackageRecipe classes such as C{AutoPackageRecipe} inherit the
    functionality offered by C{PackageRecipe}.

    EXAMPLE
    =======
    A sample class that uses PackageRecipe to download source code from
    a web site, unpack it, run "make", then run "make install"::

        class ExamplePackage(PackageRecipe):
            name = 'example'
            version = '1.0'

            def setup(r):
                r.addArchive('http://code.example.com/example/')
                r.Make()
                r.MakeInstall()
    """
    name = 'package'
    internalAbstractBaseClass = 1
    buildRequires = [
        'bzip2:runtime',
        'gzip:runtime',
        'tar:runtime',
        'cpio:runtime',
        'patch:runtime',
    ]'''

groupDescription = '''A group refers to a collection of references to specific troves
    (specific name, specific version, and specific flavor); the troves
    may define all the software required to install a system, or sets of
    troves that are available for a system, or other groups.  Each group
    may contain any kind of trove, including other groups, and groups
    may reference other groups built at the same time as well as other
    groups that exist in a repository.'''

GroupRecipe = '''
class GroupRecipe(_GroupRecipe, BaseRequiresRecipe):
    """
    NAME
    ====
    B{C{r.GroupRecipe()}} - Provides the original type of recipe interface
    for creating groups.

    DESCRIPTION
    ===========
    The C{r.GroupRecipe} class provides the original interface for creating
    groups that are stored in a Conary repository.

    ''' + groupDescription + '''

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
    these parameters are typically passed to C{r.createGroup()} for the
    base group, they should be set as variables in the recipe class.

    Note: Setting these parameters affects not only the value for the base
    group, but also the default value for all newly created groups. For
    example, if B{autoResolve} is set to C{True} in the base group, all other
    groups created will have autoResolve set to C{True} by default.
    B{imageGroup} is an exception to this rule; it will not propogate to
    sub groups.

    The following parameters are accepted by C{r.GroupRecipe} with default
    values indicated in parentheses when applicable:

    B{depCheck} : (False) If set to C{True}, Conary will check for dependency
    closure in this group, and raise an error if closure is not found.

    B{autoResolve} : (False) If set to C{True}, Conary will include any extra
    troves needed to make this group dependency complete.

    B{checkOnlyByDefaultDeps} : (True) Conary only checks the
    dependencies of troves that are installed by default, referenced in the
    group.  If set to C{False}, Conary will also check the dependencies of
    B{byDefault} C{False} troves.  Doing this, however, will prevent groups
    with C{autoResolve}=C{True} from changing the C{byDefault} status of
    required troves.

    B{checkPathConflicts} : (True) Conary checks for path conflicts in each
    group by default to ensure that the group can be installed without path
    conflicts.  Setting this parameter to C{False} will disable the check.

    B{imageGroup} : (True) Indicates that this group defines a complete,
    functioning system, as opposed to a group representing a system
    component or a collection of multiple groups that might or might not
    collectively define a complete, functioning system.
    Image group policies will be executed separately for each image group.
    This setting is recorded in the troveInfo for the group. This setting
    does not propogate to subgroups.

    METHODS
    =======
    The following methods are applicable in Conary group recipes:

        - L{add} : Adds a trove to a group

        - L{addAll} : Add all troves directly contained in a given reference
        to groupName

        - L{addNewGroup} : Adds one newly created group to another newly
        created group

        - L{addReference} : (Deprecated) Adds a reference to a trove

        - L{createGroup} : Creates a new group

        - L{copyComponents}: Add components to one group by copying them
        from the components in another group

        - L{moveComponents}: Add components to one group, removing them
        from the other in the process.

        - L{remove} : Removes a trove

        - L{removeComponents} : Define components which should not be
        installed

        - L{removeItemsAlsoInGroup}: removes troves in the group specified
        that are also in the current group

        - L{removeItemsAlsoInNewGroup}: removes troves in the group specified
        that are also in the current group

        - L{Requires} : Defines a runtime requirement for group

        - L{requireLatest} : Raise an error if add* commands resolve to older
        trove than the latest on branch. This can occur when a flavor of
        a trove exists that is not the latest version.

        - L{replace} : Replace troves

        - L{setByDefault} : Set troves to be added to group by default

        - L{setDefaultGroup} : Defines default group

        - L{setSearchPath} : Specify the searchPath to search for troves

    """
    name = 'group'
    internalAbstractBaseClass = 1
'''

GroupSetRecipe = '''
class GroupSetRecipe(_GroupSetRecipe, BaseRequiresRecipe):
    """
    NAME
    ====
    B{C{r.GroupSetRecipe()}} - Provides a set-oriented recipe interface
    for creating groups.

    DESCRIPTION
    ===========
    The C{r.GroupSetRecipe} class provides a set-oriented interface for
    creating groups that are stored in a Conary repository.

    ''' + groupDescription + '''

    In a C{GroupSetRecipe}, almost all the operations are operations
    on sets of references to troves, called B{TroveSets}.  Each trove
    reference in a TroveSet is a three-tuple of B{name}, B{version},
    B{flavor}, along with an attribute, C{isInstalled}, that describes
    whether the trove is considered B{installed} or B{optional}.  Each
    TroveSet is immutable.  TroveSet operations return new TroveSets;
    they do not modify existing TroveSets.

    A TroveSet is created either by reference to other TroveSets or
    by reference to a Repository.  A C{GroupSetRecipe} must have at
    least one C{Repository} object.  A C{Repository} object has a
    default search label list and default flavor, but can be used to
    find any trove in any accessible Conary repository.

    Repositories and TroveSets can be combined in order in a C{SearchPath}
    object.  A C{SearchPath} object can be used both for looking up
    troves and as a source of troves for dependency resolution.
    TroveSets in a SearchPath are searched recursively only when used
    to look up dependencies; only the troves mentioned explicitly are
    searched using C{find}.  (Use C{TroveSet.flatten()} if you want to
    search a TroveSet recursively using C{find}.)

    Finally, the ultimate purpose of a group recipe is to create a
    new binary group or set of groups.  TroveSets have a C{createGroup}
    method that creates binary groups from the TroveSets.  (The binary
    group with the same name as the source group can be created using
    the C{Group} method, which itself calls C{createGroup}.)  In the binary
    groups created by C{Group} or C{createGroup}, the C{byDefault} flag
    is used to indicate B{installed} (C{byDefault=True}) or B{optional}
    (C{byDefault=False}).

    In summary, C{Repository} objects are the source of all references
    to troves in TroveSets, directly or indirectly.  The TroveSets are
    manipulated in various ways until they represent the desired groups,
    and then those groups are built with C{createGroup} (or C{Group}).

    METHODS
    =======
    The following recipe methods are available in Conary group set recipes:

        - L{Repository} : Creates an object representing a respository
          with a default search label list and flavor.
        - L{SearchPath} : Creates an object in which to search for
          troves or dependencies.
        - L{Group} : Creates the primary group object.
        - L{Script} : Creates a single script object.
        - L{Scripts} : Associates script objects with script types.
        - L{CML} : Converts a system model to a TroveSet.
        - L{dumpAll} : Displays copious output describing each action.
        - L{track} : Displays less copious output describing specific
          troves.
        - L{writeDotGraph} : Writes "dot" graph of recipe structure.

    The following methods are available in C{Repository} objects:

        - C{Repository.find} : Search the Repository for specified troves
        - C{Repository.latestPackages} : Get latest normal packages of the
          default flavor on the default label

    The following methods are available in C{SearchPath} objects:

        - C{SearchPath.find} : Search the SearchPath for specified troves 

    The following methods are available in C{TroveSet} objects:

        - C{TroveSet.components} : Recursively search for named components
        - C{TroveSet.createGroup} : Create a binary group
        - C{TroveSet.depsNeeded} : Get troves satisfying dependencies
        - C{TroveSet.difference} : Subtract one TroveSet from another (C{-})
        - C{TroveSet.dump} : Debugging: print the contents of the TroveSet
        - C{TroveSet.find} : Search the TroveSet for specified troves
        - C{TroveSet.findByName} : Find troves by regular expression
        - C{TroveSet.findBySourceName} : Find troves by the name of the source
          package from which they were built
        - C{TroveSet.flatten} : Resolve trove references recursively
        - C{TroveSet.getInstall} : Get only install troves from set
        - C{TroveSet.getOptional} : Get only optional troves from set
        - C{TroveSet.isEmpty} : Assert that the TroveSet is entirely empty
        - C{TroveSet.isNotEmpty} : Assert that the TroveSet contains something
        - C{TroveSet.makeInstall} : Make all troves install, or add all
          provided troves as install troves
        - C{TroveSet.makeOptional} : Make all troves optional, or add all
          provided troves as optional troves
        - C{TroveSet.members} : Resolve exactly one level of trove
          references, return only those resolved references
        - C{TroveSet.packages} : Resolve trove references recursively,
          return packages
        - C{TroveSet.patch} : Replace troves in the TroveSet with
          matching-named troves from the replacement set
        - C{TroveSet.union} : Get the union of all provided TroveSets (C{|}, C{+})
        - C{TroveSet.update} : Replace troves in the TroveSet with
          all troves from the replacement set

    Except for C{TroveSet.dump}, which prints debugging information,
    each of these C{Repository}, C{SearchPath}, and C{TroveSet} methods
    returns a C{TroveSet}.

    EXAMPLE
    =======
    This is an example recipe that uses the search path included in
    a product definition, if available, to provide a stable search.
    It adds to the base C{group-appliance-platform} the httpd, mod_ssl,
    and php packages, as well as all the required dependencies::

     class GroupMyAppliance(GroupSetRecipe):
         name = 'group-my-appliance'
         version = '1.0'

         def setup(r):
             r.dumpAll()
             repo = r.Repository('conary.rpath.com@rpl:2', r.flavor)
             searchPathList = [ r.Repository(r.macros.buildlabel, r.flavor) ]
             if 'productDefinitionSearchPath' in r.macros:
                 # proper build with product definition
                 searchPathList.extend([repo[x] for x in
                     r.macros.productDefinitionSearchPath.split('\\\\n')])
             else:
                 # local test build
                 searchPathList.append(
                     repo['group-os=conary.rpath.com@rpl:2'])
             searchPath = r.SearchPath(*searchPathList)

             base = searchPath['group-appliance-platform']
             additions = searchPath.find(
                 'httpd',
                 'mod_ssl',
                 'php')
             # We know that base is dependency-closed and consistent
             # with the searchPath, so just get the extra deps we need
             deps = (additions + base).depsNeeded(searchPath)

             r.Group(base + additions + deps)

    Next, an example of building a platform derived from another platform,
    adding all packages defined locally to the group::

     class GroupMyPlatform(GroupSetRecipe):
         name = 'group-my-platform'
         version = '1.0'

         def setup(r):
             centOS = r.Repository('centos.rpath.com@rpath:centos-5', r.flavor)
             local = r.Repository('repo.example.com@example:centos-5', r.flavor)
             pkgs = centOS['group-packages']
             std = centOS['group-standard']
             localPackages = localRepo.latestPackages()
             std += localPackages
             pkgs += localPackages
             stdGrp = std.createGroup('group-standard')
             pkgGrp = pkgs.createGroup('group-packages')
             r.Group(stdGrp + pkgGrp)
    """
    name = 'groupset'
    internalAbstractBaseClass = 1
'''


BuildPackageRecipe = '''class BuildPackageRecipe(PackageRecipe):
    """
    NAME
    ====
    B{C{BuildPackageRecipe}} - Build packages requiring Make and shell
    utilities

    SYNOPSIS
    ========
    C{class I{className(BuildPackageRecipe):}}

    DESCRIPTION
    ===========
    The C{BuildPackageRecipe} class provides recipes with capabilities for
    building packages which require the C{make} utility, and additional,
    standard shell tools, (coreutils) and the programs needed to run
    C{configure}. (findutils, C{gawk}, C{grep}, C{sed}, and diffutils)

    C{BuildPackageRecipe} inherits from C{PackageRecipe}, and therefore
    includes all the build requirements of  C{PackageRecipe}.

    EXAMPLE
    =======
    C{class DocbookDtds(BuildPackageRecipe):}

    Uses C{BuildPackageRecipe} to define the class for a Docbook Document Type
    Definition collection recipe.
    """
    name = 'buildpackage'
    internalAbstractBaseClass = 1
    buildRequires = [
        'coreutils:runtime',
        'make:runtime',
        'mktemp:runtime',
        # all the rest of these are for configure
        'file:runtime',
        'findutils:runtime',
        'gawk:runtime',
        'grep:runtime',
        'sed:runtime',
        'diffutils:runtime',
    ]
    Flags = use.LocalFlags'''

CPackageRecipe = '''class CPackageRecipe(BuildPackageRecipe):
    """
    NAME
    ====
    B{C{CPackageRecipe}} - Build packages consisting of binaries built from C
    source code

    SYNOPSIS
    ========
    C{class I{className(CPackageRecipe):}}

    DESCRIPTION
    ===========
    The C{CPackageRecipe} class provides the essential build requirements
    needed for packages consisting of binaries built from C source code, such
    as the linker and C library. C{CPacakgeRecipe} inherits from
    C{BuildPackageRecipe}, and therefore includes all the build requirements of
    C{BuildPackageRecipe}.

    Most package recipes which are too complex for C{AutoPackageRecipe}, and
    consist of applications derived from C source code which do not require
    additional shell utilities as build requirements use the
    C{CPackageRecipe} class.

    EXAMPLE
    =======
    C{class Bzip2(CPackageRecipe):}

    Defines the class for a C{bzip2} recipe using C{AutoPackageRecipe}.
    """
    name = 'cpackage'
    internalAbstractBaseClass = 1
    buildRequires = [
        'binutils:runtime',
        'binutils:lib',
        'binutils:devellib',
        'gcc:runtime',
        'gcc:lib',
        'gcc:devel',
        'gcc:devellib',
        'glibc:runtime',
        'glibc:lib',
        'glibc:devellib',
        'glibc:devel',
        'libgcc:lib',
        'libgcc:devellib',
        'debugedit:runtime',
        'elfutils:runtime',
    ]
    Flags = use.LocalFlags'''

AutoPackageRecipe = '''class AutoPackageRecipe(CPackageRecipe):
    """
    NAME
    ====
    B{C{AutoPackageRecipe}} - Build simple packages with auto* tools

    SYNOPSIS
    ========
    C{class I{className(AutoPackageRecipe):}}

    DESCRIPTION
    ===========
    The  C{AutoPackageRecipe} class provides a simple means for the
    creation of packages from minimal recipes, which are built from source
    code using the auto* tools, such as C{automake}, and C{autoconf}.

    Processing in the C{AutoPackageRecipe} class is a simple workflow modeled
    after building software from source code, and is essentially comprised of
    these steps:

        1. Unpack source archive
        2. C{configure}
        3. C{make}
        4. C{make install}
        5. Applying Conary policy (optional)

    With C{AutoPackageRecipe} the recipe writer does not necessarily need to
    define the C{Configure}, C{Make}, or C{MakeInstall} methods, which allows
    for very compact, and simple recipes.

    The recipe's child classes should define the C{unpack()} method in order
    to populate the source list.

    Invoke the C{policy} method, with necessary policy parameters, and
    keywords in your recipe to enforce Conary policy in the package.

    If the standard C{Configure()}, C{Make()}, and C{MakeInstall()} methods
    are insufficient for your package requirements, you should define your own
    methods to override them.

    Of the three methods, C{Configure}, and C{Make} are least likely to be
    insufficient, and require overriding for the majority of recipes using
    C{AutoPackageRecipe}.

    EXAMPLE
    =======
    C{class Gimp(AutoPackageRecipe):}

    Defines the class for a GNU Image Manipulation Program (Gimp) recipe using
    C{AutoPackageRecipe}.
    """
    Flags = use.LocalFlags
    name = 'autopackage'
    internalAbstractBaseClass = 1

    def setup(r):
        r.unpack()
        r.configure()
        r.make()
        r.makeinstall()
        r.policy()

    def unpack(r):
        pass
    def configure(r):
        r.Configure()
    def make(r):
        r.Make()
    def makeinstall(r):
        r.MakeInstall()
    def policy(r):
        pass'''

UserInfoRecipe = '''class UserInfoRecipe(UserGroupInfoRecipe,
        BaseRequiresRecipe):
    """
    NAME
    ====
    B{C{UserInfoRecipe}} - Build user info packages

    SYNOPSIS
    ========
    C{UserInfoRecipe} is used to create packages that define a system user

    DESCRIPTION
    ===========
    The C{UserInfoRecipe} class provides an interface to define a system
    user through the C{r.User} method.  The C{r.User} method is also
    available in the C{PackageRecipe} class.

    EXAMPLE
    =======
    A sample class that uses C{UserInfoRecipe} to define a user::

        class ExamplePackage(UserInfoRecipe):
            name = 'info-example'
            version = '1.0'

            def setup(r):
                r.User('example', 500)
    """
    name = 'userinfo'
    internalAbstractBaseClass = 1'''

RedirectRecipe = '''class RedirectRecipe(_RedirectRecipe, BaseRequiresRecipe):
    name = 'redirect'
    internalAbstractBaseClass = 1'''

FilesetRecipe = '''class FilesetRecipe(_FilesetRecipe, BaseRequiresRecipe):
    name = 'fileset'
    internalAbstractBaseClass = 1'''


GroupInfoRecipe = '''class GroupInfoRecipe(UserGroupInfoRecipe,
        BaseRequiresRecipe):
    """
    NAME
    ====
    B{C{GroupInfoRecipe}} - Build group info packages

    SYNOPSIS
    ========
    C{GroupInfoRecipe} is used to create packages that define a system group

    DESCRIPTION
    ===========
    The C{GroupInfoRecipe} class provides an interface to define a system
    group through the C{r.Group} method.  The C{r.Group} method is also
    available in the C{PackageRecipe} class.

    The C{GroupInfoRecipe} class should be used if a system group must exist
    independently from any system users.

    EXAMPLE
    =======
    A sample class that uses C{GroupInfoRecipe} to define a group::

        class ExamplePackage(GroupInfoRecipe):
            name = 'info-example'
            version = '1.0'

            def setup(r):
                r.Group('example', 500)
    """
    name = 'groupinfo'
    internalAbstractBaseClass = 1'''

DerivedPackageRecipe = '''class DerivedPackageRecipe(AbstractDerivedPackageRecipe, BaseRequiresRecipe):
    """
    NAME
    ====
    B{C{DerivedPackageRecipe}} - Build derived packages

    SYNOPSIS
    ========
    C{DerivedPackageRecipe} is used to modify shadows of existing binary
    packages

    DESCRIPTION
    ===========
    The C{DerivedPackageRecipe} class provides an interface to modify the
    contents of a shadowed binary trove without recooking from source.

    To use this recipe class, first shadow the upstream package, then change
    the recipe.

    EXAMPLE
    =======
    A sample class that uses DerivedPackageRecipe to replace contents of
    a config file::

        class ExamplePackage(DerivedPackageRecipe):
            name = 'example'
            version = '1.0'

            def setup(r):
                r.Replace('foo', 'bar', '/etc/example.conf')
    """
    name = 'derivedpackage'
    internalAbstractBaseClass = 1'''

CapsuleRecipe = '''class CapsuleRecipe(AbstractCapsuleRecipe, BaseRequiresRecipe):
    """
    NAME
    ====
    B{C{CapsuleRecipe}} - Build Capsule packages

    SYNOPSIS
    ========
    C{CapsuleRecipe} is used to create a package that contains an unmodified,
    foreign package.

    DESCRIPTION
    ===========
    The C{CapsuleRecipe} class provides an interface to create a capsule
    package.  A capsule package encapsulates an unmodified, foreign package that
    is created by another packaging system.  Currently only RPM is supported.
    When a capsule package is installed or updated, the actual install or update
    is done by Conary calling the other packaging system.

    EXAMPLE
    =======
    A sample class that uses CapsuleRecipe to create a Conary capsule package
    containing a single RPM::

        class ExamplePackage(CapsuleRecipe):
            name = 'example'
            version = '1.0'

            def setup(r):
                r.addCapsule('foo.rpm')
    """
    name = 'capsule'
    internalAbstractBaseClass = 1
    buildRequires = [
        'bzip2:runtime',
        'gzip:runtime',
        'tar:runtime',
        'cpio:runtime',
        'patch:runtime',
    ]'''

DerivedCapsuleRecipe = '''class DerivedCapsuleRecipe(AbstractDerivedCapsuleRecipe, BaseRequiresRecipe):
    """
    NAME
    ====
    B{C{DerivedCapsuleRecipe}} - Build derived capsule packages

    SYNOPSIS
    ========
    C{DerivedCapsuleRecipe} is used to modify shadows of existing binary
    capsule packages

    DESCRIPTION
    ===========
    The C{DerivedCapsuleRecipe} class provides an interface to modify the
    contents of a binary trove which contains a capsule without
    recooking from source.

    To use this recipe class, first shadow the upstream package, then change
    the recipe.

    Note that the Remove build action is not supported for files defined within
    a capsule.

    EXAMPLE
    =======
    A sample class that uses DerivedCapsuleRecipe to replace contents of
    a config file::

        class ExampleCapsule(DerivedCapsuleRecipe):
            name = 'example'
            version = '1.0'

            def setup(r):
                r.Replace('foo', 'bar', '/etc/example.conf')
    """
    name = 'derivedcapsule'
    internalAbstractBaseClass = 1'''


recipeNames = {'baserequires': 'BaseRequiresRecipe',
               'package': 'PackageRecipe',
               'buildpackage': 'BuildPackageRecipe',
               'cpackage': 'CPackageRecipe',
               'autopackage': 'AutoPackageRecipe',
               'userinfo': 'UserInfoRecipe',
               'groupinfo': 'GroupInfoRecipe',
               'derivedpackage': 'DerivedPackageRecipe',
               'group': 'GroupRecipe',
               'groupset': 'GroupSetRecipe',
               'redirect': 'RedirectRecipe',
               'fileset': 'FilesetRecipe',
               'capsule': 'CapsuleRecipe',
               'derivedcapsule': 'DerivedCapsuleRecipe',
               }

packageNames = dict([(x[1], x[0]) for x in recipeNames.iteritems()])

import sys
defaultRecipes = dict([(x[0], sys.modules[__name__].__dict__[x[1]]) for x in recipeNames.iteritems()])
