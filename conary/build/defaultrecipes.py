BaseRequiresRecipe = '''class BaseRequiresRecipe(AbstractPackageRecipe):
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
    ]'''

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

recipeNames = {'baserequires': 'BaseRequiresRecipe',
               'package': 'PackageRecipe',
               'buildpackage': 'BuildPackageRecipe',
               'cpackage': 'CPackageRecipe',
               'autopackage': 'AutoPackageRecipe',
               'userinfo': 'UserInfoRecipe',
               'groupinfo': 'GroupInfoRecipe',
               'derivedpackage': 'DerivedPackageRecipe',
               }

packageNames = dict([(x[1], x[0]) for x in recipeNames.iteritems()])

import sys
defaultRecipes = dict([(x[0], sys.modules[__name__].__dict__[x[1]]) for x in recipeNames.iteritems()])
