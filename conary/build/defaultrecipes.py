#
# Copyright (c) 2009 rPath, Inc.
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

GroupRecipe = '''
class GroupRecipe(_GroupRecipe, BaseRequiresRecipe):
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

    B{imageGroup} | (True) Indicates that this group defines a complete,
    functioning system, as opposed to a group representing a system
    component or a collection of multiple groups that might or might not
    collectively define a complete, functioning system.
    Image group policies will be executed separately for each image group.
    This setting is recorded in the troveInfo for the group. This setting
    does not propogate to subgroups.

    USER COMMANDS
    =============
    The following user commands are applicable in Conary group recipes:

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
    A sample class that uses C{UserInfoRecipe} to define a user

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
    A sample class that uses C{GroupInfoRecipe} to define a group

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
    a config file:

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

    SYNOPSIS
    ========

    DESCRIPTION
    ===========

    EXAMPLE
    =======

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

recipeNames = {'baserequires': 'BaseRequiresRecipe',
               'package': 'PackageRecipe',
               'buildpackage': 'BuildPackageRecipe',
               'cpackage': 'CPackageRecipe',
               'autopackage': 'AutoPackageRecipe',
               'userinfo': 'UserInfoRecipe',
               'groupinfo': 'GroupInfoRecipe',
               'derivedpackage': 'DerivedPackageRecipe',
               'group': 'GroupRecipe',
               'redirect': 'RedirectRecipe',
               'fileset': 'FilesetRecipe',
               'capsule': 'CapsuleRecipe',
               }

packageNames = dict([(x[1], x[0]) for x in recipeNames.iteritems()])

import sys
defaultRecipes = dict([(x[0], sys.modules[__name__].__dict__[x[1]]) for x in recipeNames.iteritems()])
