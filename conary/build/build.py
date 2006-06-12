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
"""
Module used by recipes to direct the build and installation of
software packages.  Classes from this module are not used directly;
instead, they are used through eponymous interfaces in recipe.

Most of the paths that these classes use have a special property;
if they are relative paths, they are interpreted relative to the
main build directory (the C{%(builddir)s}); if they are absolute paths,
they are interpreted relative to the destination root directory
(the C{%(destdir)s}).

The class descriptions contain usage examples for quick reference.

@var _permmap: A mapping of common integer file modes to their octal
equivalents.  This is used to check for common mistakes when specifying
the permissions on files in classes derived from _PutFile.
"""

import os
import re
import shutil
import stat
import sys
import tempfile
import textwrap

#conary imports
from conary.build import action
from conary.lib import fixedglob, log, util
from conary.build.use import Use

# make sure that the decimal value really is unreasonable before
# adding a new translation to this file.
_permmap = {
    1755: 01755,
    2755: 02755,
    4755: 04755,
    4711: 04711,
    4510: 04510,
    755: 0755,
    750: 0750,
    644: 0644,
    640: 0640,
}

class BuildAction(action.RecipeAction):
    """
    Pure virtual class which inherits from action.RecipeAction but
    passes macros to the C{do()} method.
    """
    def __init__(self, recipe, *args, **keywords):
	"""
	@keyword use: Optional argument; Use flag(s) telling whether
	to actually perform the action.
	@type use: None, Use flag, or sequence of Use flags
	"""
	# enforce pure virtual status
        assert(self.__class__ is not BuildAction)
	action.RecipeAction.__init__(self, recipe, *args, **keywords)

    def doAction(self):
	if self.debug:
	    from conary.lib import debugger
	    debugger.set_trace()
	if self.use:
	    if self.linenum is None:
		self.do(self.recipe.macros)
	    else:
                self.recipe.buildinfo.lastline = self.linenum
		oldexcepthook = sys.excepthook
		sys.excepthook = action.genExcepthook(self)
		self.do(self.recipe.macros)
		sys.excepthook = oldexcepthook

    def do(self, macros):
        """
        Do the build action

        @param macros: macro set to be used for expansion
        @type macros: macros.Macros
        """
        raise AssertionError, "do method not implemented"


class BuildCommand(BuildAction, action.ShellCommand):
    """
    Pure virtual class which implements the do method,
    based on the shell command built from a template.
    """
    def __init__(self, recipe, *args, **keywords):
	# enforce pure virtual status
        assert(self.__class__ is not BuildCommand)
	BuildAction.__init__(self, recipe, *args, **keywords)
	action.ShellCommand.__init__(self, recipe, *args, **keywords)

    def do(self, macros):
        """
	Method which is used if the ShellCommand instance is invoked
        during build
        @param macros: macros which will be expanded through dictionary
        substitution in self.command
        @type macros: macros.Macros
        @return: None
        @rtype: None
	"""
        util.execute(self.command %macros)


class Run(BuildCommand):
    """
    NAME
    ====

    B{C{r.Run()}} - Run a shell command

    SYNOPSIS
    ========

    C{r.Run(I{cmd}, [I{dir},] [I{filewrap},] [I{wrapdir}])}

    DESCRIPTION
    ===========

    The C{r.Run()} class is called from within a Conary recipe to run a shell
    command with simple macro substitution.

    KEYWORDS
    ========

    The C{r.Run()} class accepts the following keywords, with default values
    shown in parentheses when applicable:

    B{dir} : (None) Directory in which to run the command. Relative dirs are
    relative to the build directory, and absolute dirs are relative to the
    destination directory.

    B{filewrap} : (False) If set to C{True}, a C{LD_PRELOAD} wrapper will look
    in C{%(destdir)s} for some common file operations.  Occasionally useful to
    avoid the need to modify programs that need to be run after the build, and
    assume that they are not run until after installation.

    B{wrapdir} : (None) If set, points to a directory. Similar to C{filewrap},
    except it limits the C{%(destdir)s} substitution to only the tree under
    the given directory.

    EXAMPLES
    ========

    C{r.Run('find %(destdir)s -name "floppyd*" -print | xargs rm')}

    Locate instances of the filespec C{floppyd*} in the C{%(destdir)s}, and
    remove them.
    """
    keywords = {'dir': '', 'filewrap': False, 'wrapdir': None}
    template = "%%(envcmd)s%%(cdcmd)s%(args)s"

    def __init__(self, *args, **kwargs):
        """
        @keyword dir: Directory in which to run the command
	An absolute C{dir} value will be considered relative to 
        C{%(destdir)s}, whereas a relative C{dir} value will be considered
        relative to C{%(builddir)s}.

	@keyword filewrap: If set to C{True}, a C{LD_PRELOAD} wrapper will
            look in C{%(destdir)s} for some common file operations.
            Occasionally useful to avoid the need to modify programs that need
            to be run after the build, and assume that they are not run until
            after installation.
        @keyword wrapdir: If set, points to a directory. Similar to
            C{filewrap}, except it limits the C{%(destdir)s} substitution to
            only the tree under the given directory.
        """
        BuildCommand.__init__(self, *args, **kwargs)

    def do(self, macros):
	macros = macros.copy()

        envStr = ''
        if self.wrapdir:
            self.filewrap = True
            envStr += 'export WRAPDIR=%(wrapdir)s; '
            macros.wrapdir = self.wrapdir
        if self.filewrap:
            basedir = '/'.join(sys.modules[__name__].__file__.split('/')[:-2])
            localcopy = '/'.join((basedir, 'lib', 'filename_wrapper.so'))
            if os.path.exists(localcopy):
                macros.fnw = localcopy
            else:
                macros.fnw = '%(libdir)s/conary/filename_wrapper.so'
            envStr += (' export LD_PRELOAD=%(fnw)s;'
                       ' export DESTDIR=%(destdir)s;')
        macros.envcmd = envStr

        if self.dir:
            macros.cdcmd = 'cd %s; ' % (action._expandOnePath(self.dir, macros))
	else:
	    macros.cdcmd = ''

        util.execute(self.command %macros)

class Automake(BuildCommand):
    """
    NAME
    ====

    B{C{r.Automake()}} - Re-runs aclocal, autoconf, and automake

    SYNOPSIS
    ========

    C{r.Automake()}

    DESCRIPTION
    ===========

    The C{r.Automake()} class is called from within a Conary recipe to re-run
    the C{aclocal}, C{autoconf}, and C{automake} commands.

    KEYWORDS
    ========

    The C{r.Automake()} class accepts the following keywords, with default
    values shown in parentheses when applicable:

    B{aclocalArgs} : (None) Arguments to the C{aclocal} program

    B{autoConfArgs} : (None) Arguments to the C{autoconf} program

    B{autoMakeArgs} : (None) Arguments to the C{automake} program.

    B{automakeVer} : (None) Specifies C{automake} version

    B{m4Dir} : (None) Specifies directory for C{m4} macro processor

    B{preAutoconf} : (None) Commands to be run prior to C{autoconf}

    B{skipMissingSubDir} : (False) Raise an error if C{subDir} does not exist,
    (by default) and if set to C{True} skip the action when C{subDir} does not
    exist.

    B{subDir}: (None) Directory in which to re-run C{aclocal}, C{autoconf},
    and C{automake}

    EXAMPLES
    ========

    C{r.Automake(autoMakeArgs='--add-missing --foreign')}

    Demonstrates calling C{r.Automake()} and passing in the C{--add-missing}
    and C{--foreign} arguments to the C{automake} program.
    """
    # note: no use of %(args)s -- to which command would it apply?
    template = ('cd %%(actionDir)s; '
                'aclocal %%(m4DirArgs)s %(aclocalArgs)s; '
		'%(preAutoconf)s autoconf %(autoConfArgs)s; '
		'automake%(automakeVer)s %(autoMakeArgs)s')
    keywords = {'autoConfArgs': '',
                'autoMakeArgs': '',
		'aclocalArgs': '',
		'preAutoconf': '',
                'm4Dir': '',
		'automakeVer': '',
                'subDir': '',
                'skipMissingSubDir': False,
               }

    def do(self, macros):
	macros = macros.copy()
        macros.actionDir = action._expandOnePath(self.subDir, macros,
             macros.builddir, error=not self.skipMissingSubDir)
        if not os.path.exists(macros.actionDir):
            assert(self.skipMissingSubDir)
            return

        if self.m4Dir:
	    macros.m4DirArgs = '-I %s' %(self.m4Dir)
	else:
	    macros.m4DirArgs = ''
        util.execute(self.command %macros)


class Configure(BuildCommand):
    """
    NAME
    ====

    B{C{r.Configure()}} - Runs autoconf configure script

    SYNOPSIS
    ========

    C{r.Configure(I{extra args}, [I{configureName},] [I{objDir},] [I{preConfigure},] [I{skipMissingSubDir},] [I{subDir}])}

    DESCRIPTION
    ===========

    The C{r.Configure()} class is called from within a Conary recipe to run an
    autoconf configure script, giving it the default paths as defined by the
    macro set: C{r.Configure(extra args)}.

    It provides many common arguments, set correctly to values provided by
    system macros. If any of these arguments do not work for a program, then
    use the C{r.ManualConfigure()} class instead.


    KEYWORDS
    ========

    The C{r.Configure()} class accepts the following keywords, with default
    values shown in parentheses when applicable:

    B{configureName} : (None) The name of the configure command. Normally
    C{configure}, but occasionally C{Configure} or something else.

    B{objDir} : (None) Make an object directory before running C{configure}.
    This is useful for applications which do not support running configure
    from the same directory as the sources (srcdir != objdir). It can contain
    macro references.

    B{preConfigure} : (None) Extra shell script which is inserted in front of
    the C{configure} command.

    B{skipMissingSubDir} : (False) Raise an error if C{subDir} does not exist,
    (by default) and if set to C{True} skip the action when C{subDir} does not
    exist.

    B{subDir} : (None) Directory in which to run C{configure}

    EXAMPLES
    ========

    C{r.Configure('--sbindir=%(essentialsbindir)s')}

    Demonstrates calling C{r.Configure()} and specifying the
    C{%(essentialsbindir)s} directory with an C{--sbindir=} argument.
    """
    # note that template is NOT a tuple, () is used merely to group strings
    # to avoid trailing \ characters on every line
    template = (
	'cd %%(actionDir)s; '
	'%%(mkObjdir)s '
    'CLASSPATH="%%(classpath)s"'
	' CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
	' CPPFLAGS="%%(cppflags)s"'
	' LDFLAGS="%%(ldflags)s" CC=%%(cc)s CXX=%%(cxx)s'
	' %(preConfigure)s %%(configure)s'
	# XXX host/build/target here
	' --prefix=%%(prefix)s'
	' --exec-prefix=%%(exec_prefix)s'
	' --bindir=%%(bindir)s'
	' --sbindir=%%(sbindir)s'
	' --sysconfdir=%%(sysconfdir)s'
	' --datadir=%%(datadir)s'
	' --includedir=%%(includedir)s'
	' --libdir=%%(libdir)s'
	' --libexecdir=%%(libexecdir)s'
	' --localstatedir=%%(localstatedir)s'
	' --sharedstatedir=%%(sharedstatedir)s'
	' --mandir=%%(mandir)s'
	' --infodir=%%(infodir)s'
        ' %%(bootstrapFlags)s'
	'  %(args)s')
    keywords = {'preConfigure': '',
		'configureName': 'configure',
                'objDir': '',
                'bootstrapFlags': '--target=%(target)s --host=%(host)s --build=%(build)s',
		'subDir': '',
                'skipMissingSubDir': False,
               }

    def __init__(self, recipe, *args, **keywords):
        """
        @keyword configureName: The name of the configure command. Normally,
            C{configure} but occasionally C{Configure} or something else.
        @keyword objDir: Make an object directory before running C{configure}.
            This is useful for applications which do not support running
            configure from the same directory as the sources
            (srcdir != objdir). It can contain macro references.
        @keyword preConfigure: Extra shell script which is inserted in front
            of the C{configure} command.
        @keyword skipMissingSubDir: Raise an error if C{subDir} does not
            exist, (by default) and if set to C{True} skip the action when
            C{subDir} does not exist.
        @keyword subDir: Directory in which to run C{configure}
        """
        BuildCommand.__init__(self, recipe, *args, **keywords)

    def do(self, macros):
	macros = macros.copy()
        macros.actionDir = action._expandOnePath(self.subDir, macros,
             macros.builddir, error=not self.skipMissingSubDir)
        if not os.path.exists(macros.actionDir):
            assert(self.skipMissingSubDir)
            return

        if self.objDir:
	    objDir = self.objDir %macros
            macros.mkObjdir = 'mkdir -p %s; cd %s;' %(objDir, objDir)
            macros.cdObjdir = 'cd %s;' %objDir
	    macros.configure = '../%s' % self.configureName
        else:
            macros.mkObjdir = ''
            macros.cdObjdir = ''
            macros.configure = './%s' % self.configureName
        # using the get method avoids adding bootstrap flag to tracked flags
        # (if the flag is really significant, it will be checked elsewhere)
        if Use.bootstrap._get():
            macros.bootstrapFlags = self.bootstrapFlags
        else:
            macros.bootstrapFlags = ''
        try:
            util.execute(self.command %macros)
        except RuntimeError, info:
            if not self.recipe.isatty():
                # When conary is being scripted, logs might be
                # redirected to a file, and it might be easier to
                # see config.log output in that logfile than by
                # inspecting the build directory
                # Each file line will have the filename prepended
                # The "|| :" makes it OK if there is no config.log
                util.execute('cd %(actionDir)s; %(cdObjdir)s'
                             'find . -name config.log | xargs grep -H . || :'
                             %macros)
            raise

class ManualConfigure(Configure):
    """
    NAME
    ====

    B{C{r.ManualConfigure()}} - Runs make without functional DESTDIR

    SYNOPSIS
    ========

    C{r.ManualConfigure(I{--limited-args})}

    DESCRIPTION
    ===========

    The C{r.ManualConfigure()} class is called from within a Conary recipe in
    a manner similar to C{r.Configure} except all arguments to the configure
    script must be provided explicitly.

    No arguments are given beyond those explicitly provided.

    EXAMPLES
    ========

    C{r.ManualConfigure('--prefix=/usr --shared')}

    Calls C{r.ManualConfigure()} and specifies the C{--prefix} and C{--shared}
    arguments to the configure script.
    """
    template = ('cd %%(actionDir)s; '
                '%%(mkObjdir)s '
                'CLASSPATH="%%(classpath)s"'
                ' CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
                ' CPPFLAGS="%%(cppflags)s"'
                ' LDFLAGS="%%(ldflags)s" CC=%%(cc)s CXX=%%(cxx)s'
	        ' %(preConfigure)s %%(configure)s %(args)s')

class Make(BuildCommand):
    """
    NAME
    ====

    B{C{r.Make()}} - Runs make with system defaults

    SYNOPSIS
    ========

    C{r.Make(I{makeargs}, [I{forceFlags},] [I{makeName},] [I{preMake},] [I{skipMissingSubDir},] [I{subDir}])}

    DESCRIPTION
    ===========

    The C{r.Make()} class is called from within a Conary recipe to execute the
    C{make} utility with system defaults.  The environment variables
    C{CFLAGS}, C{LDFLAGS}, C{CXXFLAGS}, and so on are set to the system
    default values, as are the variables C{mflags} and C{parallelmflags}.

    If the package C{Makefile} explicitly sets the *FLAGS variables,
    and you wish to change them, you will have to override them,
    either explicitly in the recipe with C{r.Make('CFLAGS="%(cflags)s"')},
    etc., or forcing them all to the system defaults by passing in the
    C{forceFlags=True} argument.

    If your package does not build correctly with parallelized C{make},
    you should disable parallel C{make} by using C{r.disableParallelMake()}
    in your recipe.  If your package can do parallel builds but needs some
    other mechanism, then you can modify C{parallelmflags} as necessary in
    your recipe.  You can use C{r.MakeParallelSubdir()} if the top-level
    C{make} is unable to handle parallelization but all subdirectories are.

    KEYWORDS
    ========

    The C{r.Make()} class accepts the following keywords, with default values
    shown in parentheses when applicable:

    B{forceFlags} : If set, unconditionally override the Makefile
    definitions of *FLAGS (that is, CFLAGS, CXXFLAGS, LDFLAGS) by
    passing them on the command line as well as in the environment.

    B{makeName} : (C{make}) The name of the make command; normally C{make} but
    occasionally 'qmake' or something else.

    B{preMake} : (None) String to be inserted before the C{make} command.
    Use preMake if you need to set an environment variable. The preMake
    keyword cannot contain a ; character.

    B{skipMissingSubDir} : (False) Raises an error if subDir does not exist.
    If True, skip the action if subDir does not exist.

    B{subDir} : (The build directory) The directory to enter before running
    C{make}

    EXAMPLES
    ========

    C{r.Make("PARALLELMFLAGS='%(parallelmflags)s'", subDir=objDir)}

    Demonstrates calling C{r.Make()}, and setting the environment variable
    C{PARALLELMFLAGS} equal to the current value of C{%(parallelmflags)s},
    and requesting a change into the C{objDir} subdirectory before executing
    make.

    C{r.Make('check', subDir='tests')}

    Demonstrates calling C{r.Make()} with the C{check} argument to the
    C{make} command while also changing to the subdirectory C{tests} prior
    to executing C{make}.
    """
    # Passing environment variables to Make makes them defined if
    # there is no makefile definition; if they are defined in the
    # makefile, then it takes a command-line argument to override
    # them.
    template = ('cd %%(actionDir)s; '
	        'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
		' CPPFLAGS="%%(cppflags)s" CLASSPATH="%%(classpath)s" '
		' LDFLAGS="%%(ldflags)s" CC=%%(cc)s CXX=%%(cxx)s'
                ' %(preMake)s %(makeName)s %%(overrides)s'
		' %%(mflags)s %%(parallelmflags)s %(args)s')
    keywords = {'preMake': '',
                'subDir': '',
                'skipMissingSubDir': False,
		'forceFlags': False,
                'makeName': 'make'}

    def __init__(self, recipe, *args, **keywords):
        """
        @keyword forceFlags: boolean; if set, unconditionally override the
            C{Makefile} definitions of *FLAGS
            (that is, CFLAGS, CXXFLAGS, LDFLAGS)
        @keyword makeName: The name of the make command; normally C{make} but
            occasionally 'qmake' or something else.
        @keyword preMake: string to be inserted before the C{make} command.
            Use preMake if you need to set an environment variable. The
            preMake keyword cannot contain a C{;} character.
        @keyword skipMissingSubDir: Raises an error if subDir does not exist.
            If True, skip the action if subDir does not exist.
        @keyword subDir: The directory to enter before running C{make}
        """
	BuildCommand.__init__(self, recipe, *args, **keywords)
        if 'preMake' in keywords:
            if ';' in keywords['preMake']:
                log.error(TypeError, 'preMake argument cannot contain ;')

    def do(self, macros):
	macros = macros.copy()
	if self.forceFlags:
	    # XXX should this be just '-e'?
	    macros['overrides'] = ('CFLAGS="%(cflags)s"'
                                   ' CXXFLAGS="%(cflags)s %(cxxflags)s"'
			           ' CPPFLAGS="%(cppflags)s"'
	                           ' LDFLAGS="%(ldflags)s"')
	else:
	    macros['overrides'] = ''
        macros.actionDir = action._expandOnePath(self.subDir, macros,
             macros.builddir, error=not self.skipMissingSubDir)
        if not os.path.exists(macros.actionDir):
            assert(self.skipMissingSubDir)
            return

	BuildCommand.do(self, macros)

class MakeParallelSubdir(Make):
    """
    NAME
    ====

    B{C{r.MakeParallelSubdir()}} - Runs make with parallelmflags applied only to
    sub-make processes

    SYNOPSIS
    ========

    C{r.MakeParallelSubdir(I{makeargs})}

    DESCRIPTION
    ===========

    The C{r.MakeParallelSubdir()} class is called from within a Conary recipe
    to execute the C{make} utility with system defaults for parallelmflags
    only applied to sub-make processes.

    C{r.MakeParallelSubdir()} is used exactly like C{r.Make} in cases where
    the top-level C{Makefile} does not work correctly with parallel C{make},
    but the lower-level Makefiles do work correctly with parallel C{make}.
    """
    template = ('cd %%(actionDir)s; '
	        'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
		' CPPFLAGS="%%(cppflags)s" CLASSPATH="%%(classpath)s" '
		' LDFLAGS="%%(ldflags)s" CC=%%(cc)s CXX=%%(cxx)s'
                ' %(preMake)s make %%(overrides)s'
		' %%(mflags)s '
                ' MAKE="make %%(mflags)s %%(parallelmflags)s" %(args)s')

class MakeInstall(Make):
    """
    NAME
    ====

    B{C{r.MakeInstall()}} - Runs make utility with install target

    SYNOPSIS
    ========

    C{r.MakeInstall(I{makeargs}, [I{rootVar},] [I{installtarget}])}

    DESCRIPTION
    ===========

    The C{r.MakeInstall()} class is called from within a Conary recipe to run
    the C{make} utility, automatically set C{DESTDIR}, and provide the install
    target.

    If your package does not have C{DESTDIR} or an analog, use
    C{r.MakePathsInstall()} instead, or as a last option, C{r.Make()}.

    KEYWORDS
    ========

    The C{r.MakeInstall()} class accepts the following keywords, with default
    values shown in parentheses when applicable:

    B{rootVar} : (C{DESTDIR}) The install root

    B{installtarget} : (C{install}) The install target to C{make}

    EXAMPLES
    ========

    C{r.MakeInstall(rootVar='BUILDROOT')}

    Demonstrates C{r.MakeInstall()}, specifying C{BUILDROOT} as the rootVar
    instead of the default C{DESTDIR}.

    C{r.MakeInstall('LIBTOOL=%(bindir)s/libtool')}

    Demonstrates using C{r.MakeInstall()}, and setting the environment variable
    C{LIBTOOL} to C{%(bindir)s/libtool}.
    """
    template = ('cd %%(actionDir)s; '
	        'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
		' CPPFLAGS="%%(cppflags)s" CLASSPATH="%%(classpath)s" '
		' LDFLAGS="%%(ldflags)s" CC=%%(cc)s CXX=%%(cxx)s'
                ' %(preMake)s make %%(overrides)s'
		' %%(mflags)s %%(rootVarArgs)s'
		' %(installtarget)s %(args)s')
    keywords = {'rootVar': 'DESTDIR',
		'installtarget': 'install'}

    def do(self, macros):
	macros = macros.copy()
        if self.rootVar:
	    macros.update({'rootVarArgs': '%s=%s'
	                  %(self.rootVar, macros['destdir'])})
	else:
	    macros['rootVarArgs'] = ''
	Make.do(self, macros)

class MakePathsInstall(Make):
    """
    NAME
    ====

    B{C{r.MakePathsInstall()}} - Runs make without functional DESTDIR

    SYNOPSIS
    ========

    C{r.MakePathsInstall(I{makeargs})}

    DESCRIPTION
    ===========

    The C{r.MakePathsInstall()} class is called from within a Conary recipe
    when there is no single functional C{DESTDIR} or similar definition, but
    enough of the de-facto standard variables such as C{prefix}, C{bindir},
    and so on are honored by the Makefile to make a destdir installation
    successful

    EXAMPLES
    ========
    C{r.MakePathsInstall('mandir=%(destdir)s/%(mandir)s/man1')}

    Calls C{r.MakePathsinstall()} and additionally sets the C{mandir}
    make variable to C{%(destdir)s/%(mandir)s/man1}.
    """
    template = (
	'cd %%(actionDir)s; '
	'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
	' CPPFLAGS="%%(cppflags)s" CLASSPATH="%%(classpath)s" '
	' LDFLAGS="%%(ldflags)s" CC=%%(cc)s CXX=%%(cxx)s'
	' %(preMake)s make %%(overrides)s'
	' %%(mflags)s'
	' prefix=%%(destdir)s/%%(prefix)s'
	' exec-prefix=%%(destdir)s/%%(exec_prefix)s'
	' bindir=%%(destdir)s/%%(bindir)s'
	' sbindir=%%(destdir)s/%%(sbindir)s'
	' sysconfdir=%%(destdir)s/%%(sysconfdir)s'
	' datadir=%%(destdir)s/%%(datadir)s'
	' includedir=%%(destdir)s/%%(includedir)s'
	' libdir=%%(destdir)s/%%(libdir)s'
	' libexecdir=%%(destdir)s/%%(libexecdir)s'
	' localstatedir=%%(destdir)s/%%(localstatedir)s'
	' sharedstatedir=%%(destdir)s/%%(sharedstatedir)s'
	' mandir=%%(destdir)s/%%(mandir)s'
	' infodir=%%(destdir)s/%%(infodir)s'
	' %(installtarget)s %(args)s')
    keywords = {'installtarget': 'install'}


class Ant(BuildCommand):
    """
    NAME
    ====

    B{C{r.Ant()}} - Runs c{ant}

    SYNOPSIS
    ========

    C{r.Ant(I{antargs}, [I{verbose}], [I{options}], [I{subdir}])}

    DESCRIPTION
    ===========

    The C{r.Ant()} class is called from within a Conary recipe to execute the 
    C{ant} utility.

    EXAMPLES
    ========

    C{r.Ant('jar javadoc')}

    Demonstrates calling C{r.Ant()}, with the C{jar} and C{javadoc} arguments
    to C{ant}.

    """
    keywords = {'subdir': '',
                'verbose': True,
                'options': '-lib %(javadir)s'}
    template = '%%(cdcmd)s CLASSPATH=%%(classpath)s %%(antcmd)s %%(antoptions)s %%(args)s'

    def do(self, macros):
        macros = macros.copy()
        if self.subdir: macros.cdcmd = 'cd %s;' % (self.subdir % macros)
        else: macros.cdcmd = ''
        if self.options: macros.antoptions = self.options
        if self.verbose: macros.antoptions += ' -v'
        macros.antcmd = 'ant'
        macros.args = ' '.join(self.arglist)
        BuildCommand.do(self, macros)


class JavaCompile(BuildCommand):
    """
    NAME
    ====

    B{C{r.JavaCompile()}} - Runs C{javac}

    SYNOPSIS
    ========

    C{r.JavaCompile(I{directory}, [I{javacmd}], [I{javaArgs}])}

    DESCRIPTION
    ===========

    The C{r.JavaCompile()} class is called from within a Conary recipe to 
    execute the command defined by C{javacmd}, normally C{javac}.

    EXAMPLES
    ========

    C{r.JavaCompile('/path/to/java/files', javacmd='ecj')}

    Demonstrates calling C{r.JavaCompile()}, to compile all java files in a 
    directory using C{ecj}.

    """

    keywords = {'javacmd': 'javac',
                'javaArgs': ''}
    template = 'CLASSPATH=%%(classpath)s %(javacmd)s %%(dir)s %(javaArgs)s'

    def do(self, macros):
        macros = macros.copy()
        assert(len(self.arglist) == 1)
        macros.dir = self.arglist[0] % macros
        assert(os.path.exists('%(builddir)s/%(dir)s' % macros))
        filelist = []
        util.execute('%s %s' % (self.command % macros, ' '.join(filelist)))



class CompilePython(BuildCommand):
    """
    NAME
    ====

    B{C{r.CompilePython()}} - Builds compiled and optimized Python bytecode files

    SYNOPSIS
    ========

    C{r.CompilePython([I{/dir0},] [I{/dir1}])}

    DESCRIPTION
    ===========

    The C{r.CompilePython()} is called from within a Conary recipe to compile
    optimized and compiled Python bytecode files. The paths specified must be
    absolute paths which are interpreted relative to C{%(destdir)s} in order
    for the paths compiled into the bytecode files to be correct.

    EXAMPLES
    ========

    C{r.CompilePython('%(varmmdir)s')}

    The above example demonstrates calling C{r.CompilePython()}, and specifying
    the absolute path defined by C{%(varmmdir)s}.
    """
    template = (
	"""python -c 'from compileall import *; compile_dir("""
	""""%%(destdir)s/%%(dir)s", 10, "%%(dir)s")'; """
	"""python -O -c 'from compileall import *; compile_dir("""
	""""%%(destdir)s/%%(dir)s", 10, "%%(dir)s")'"""
    )

    def do(self, macros):
	macros = macros.copy()
	destdir = macros['destdir']
	destlen = len(destdir)
	for arg in self.arglist:
	    # arg will always have a leading /, so no os.sep needed
            # Expand macros in the arg string.
            arg = arg % macros
            # XXX Replace this with an exception since it can be reached by
            # user error
            assert(arg[0] == '/')
	    for directory in util.braceGlob(destdir+arg):
		macros['dir'] = directory[destlen:]
		util.execute(self.command %macros)


class PythonSetup(BuildCommand):
    """
    NAME
    ====

    B{C{r.PythonSetup()}} - Moves files

    SYNOPSIS
    ========

    C{r.PythonSetup(I{srcname}, I{destname})}

    DESCRIPTION
    ===========

    The C{r.PythonSetup()} class is called from within a Conary recipe to
    invoke setup.py in the correct way to use python-setuptools to install
    without building a C{.egg} file regardless of whether this version of
    setup.py was written to use disttools or setuptools.

    If a different name is used for the disttools or setuptools script, pass
    that name as the argument.

    KEYWORDS
    ========

    The C{r.PythonSetup()} class accepts the following keywords, with default
    values shown in parentheses when applicable:

    B{action} : (C{install}) The main argument to pass to C{setup.py}

    B{bootstrap} : (False) Avoids reporting errors that are unavoidable
    when building bootstrap packages.

    B{dir}: (C{%(builddir)s}) Directory in which to find the setup.py file,
    defaults to the build directory.

    B{rootDir} (C{%(destdir)s}) The directory to pass to setup.py via the
    C{--root} option

    EXAMPLES
    ========

    C{r.PythonSetup(bootstrap=True)}

    Calls C{r.PythonSetup()} and specifies C{bootstrap} to be C{True}.
    """
    template = (
        '%%(cdcmd)s'
        '%%(pythonsetup)s'
        ' %%(action)s'
        ' --single-version-externally-managed'
        ' --root=%%(rootdir)s'
    )
    keywords = {
        'action': 'install',
        'bootstrap': False,
        'dir': '',
        'rootDir': '',
    }

    def do(self, macros):
	if 'python-setuptools:python' not in self.recipe.buildRequires:
            if not self.bootstrap:
                self.recipe.reportErrors(
                    "Must add 'python-setuptools:python' to buildRequires")
	macros = macros.copy()
        if self.dir:
            rundir = action._expandOnePath(self.dir, macros)
            macros.cdcmd = 'cd %s; ' % rundir
	else:
            rundir = macros.builddir
	    macros.cdcmd = ''

        if self.rootDir:
            macros.rootdir = '%(destdir)s/' + self.rootDir
        else:
            macros.rootdir = '%(destdir)s'

        if self.arglist:
            macros.setup = self.arglist[0]
        else:
            macros.setup = 'setup.py'

        macros.action = self.action

        # now figure out which kind of setup.py this is
        if re.compile('(import setuptools|from setuptools import)').search(file('%s/%s' %(rundir, macros.setup)).read()):
            macros.pythonsetup = 'python %(setup)s '
        else:
            # hack to use setuptools instead of disttools
            macros.pythonsetup = (
                '''python -c "import setuptools;execfile('%(setup)s')"''')

        util.execute(self.command %macros)


class Ldconfig(BuildCommand):
    """
    NAME
    ====

    B{C{r.Ldconfig()}} - Runs ldconfig in a directory

    SYNOPSIS
    ========

    C{r.Ldconfig(I{/dir0})}

    DESCRIPTION
    ===========

    The C{r.Ldconfig()} class is called from within a Conary recipe to execute
    the C{ldconfig} program in a subdirectory.

    Used mainly when a package does not set up all the appropriate
    symlinks for a library.  Conary packages should include all the
    appropriate symlinks in the packages.

    This is not a replacement for marking a file as a shared library.
    C{ldconfig} still needs to be run after libraries are installed.
    Note that C{ldconfig} will automatically be run for all system libraries
    as defined by the C{SharedLibrary} policy, so C{r.Ldconfig} needs
    to be called only for libraries that are not marked as shared
    libraries by the C{SharedLibrary} policy.

    EXAMPLES
    ========

    C{r.Ldconfig('%(libdir)s/')}

    Demonstrates calling C{r.Ldconfig()} to execute C{ldconfig} in the
    C{%(libdir)s/} subdirectory.
    """
    template = '%%(essentialsbindir)s/ldconfig -n %%(destdir)s/%(args)s'
    def do(self, macros):
        BuildCommand.do(self, macros)
        # since we already did this, don't do it again in policy
        try:
            self.recipe.NormalizeLibrarySymlinks(exceptions=self.arglist)
        except AttributeError:
            pass


class _FileAction(BuildAction):
    keywords = {'component': None}

    def __init__(self, recipe, *args, **keywords):
        BuildAction.__init__(self, recipe, *args, **keywords)
        # Add the specified package to the list of packages created by this
        # recipe
        if self.component and self.component.find(':') != -1:
            package = self.component.split(':')[0]
            if package:
                recipe.packages[package] = True

    def chmod(self, destdir, path, mode=None):
        isDestFile =  path.startswith(destdir)
	if not mode:
	    mode=self.mode
	if mode >= 0:
            # fixup obviously broken permissions
            destPath = path
            if isDestFile:
                destPath = path[len(destdir):]
	    if _permmap.has_key(mode):
                log.warning('odd permission %o for path %s, correcting to 0%o:'
                            ' add initial "0"?',
                            mode, destPath, _permmap[mode])
		mode = _permmap[mode]
	    isdir = os.path.isdir(path)
            if isDestFile:
                if isdir and (mode & 0700) != 0700:
                    # regardless of what permissions go into the package,
                    # we need to be able to traverse this directory as
                    # the non-root build user
                    os.chmod(path, (mode & 01777) | 0700)
                    # not literalRegex because setModes is per-path
                    # internal-only
                    self.recipe.setModes(mode, destPath)
                else:
                    os.chmod(path, mode & 01777)
                    if mode & 06000:
                        # not literalRegex, see above
                        self.recipe.setModes(mode, destPath)
                if isdir and mode != 0755:
                    self.recipe.ExcludeDirectories(
                        exceptions=util.literalRegex(destPath).replace(
                        '%', '%%'))
                # set explicitly, do not warn
                try:
                    self.recipe.WarnWriteable(
                        exceptions=util.literalRegex(destPath).replace(
                        '%', '%%'))
                except AttributeError:
                    pass
            else:
                if mode & 06000:
                    raise RuntimeError, \
                    "Cannot set setuid/gid file mode %o on %s" % (mode, path)
                os.chmod(path, mode & 01777)

    def setComponents(self, destdir, paths):
	"""
	XXX fixme
	A component without a : or with a leading : is relative to the main package name.
	A component with a trailing : is a package name
	"""
        if not self.component:
            return

        package = None
        component = None
	if type(paths) is str:
	    paths = (paths,)
        if self.component.find(':') != -1:
            (package, component) = self.component.split(':')
        else:
            component = self.component
	for path in paths:
            if not path.startswith(destdir):
                raise RuntimeError, ('can only set component for paths in '
                                     'destdir, "%s" is not.' % path)
	    path = re.escape(util.normpath(path[len(destdir):]))
	    if component:
		self.recipe.ComponentSpec(component, path)
	    if package:
		self.recipe.PackageSpec(package, path)

class Desktopfile(BuildCommand, _FileAction):
    """
    NAME
    ====

    B{C{r.Desktopfile()}} - Properly installs desktop files

    SYNOPSIS
    ========

    C{r.Desktopfile(I{filename}, [I{category},] [I{vendor}])}

    DESCRIPTION
    ===========

    The C{r.Desktopfile()} class is called from within a Conary recipe to
    install a desktop file in C{/usr/share/applications} while also setting a
    category and vendor.

    Proper build requirements for desktop files are also enforced by
    C{r.Desktopfile()}. The C{filename} argument is interpreted only relative
    to C{%(builddir)s}, and never relative to C{%(destdir)s}.

    KEYWORDS
    ========

    The C{r.Desktopfile()} class accepts the following keywords:

    B{category} : (None) A category name for the desktop file

    B{vendor} : (C{net}) A vendor name for the desktop file.

    EXAMPLES
    ========

    C{r.Desktopfile('thunderbird.desktop')}

    Demonstrates creation of the desktop file C{thunderbird.desktop} with
    C{r.Desktopfile()}.
    """
    template = ('cd %%(builddir)s; '
		'desktop-file-validate %(args)s ; '
		'desktop-file-install --vendor %(vendor)s'
		' --dir %%(destdir)s/%%(datadir)s/applications'
		' %%(category)s'
		' %(args)s')
    keywords = {'vendor': 'net',
		'category': None}


    def do(self, macros):
	if not Use.desktop:
	    return
	if 'desktop-file-utils:runtime' not in self.recipe.buildRequires:
	    # Unfortunately, we really cannot do this automagically
            self.recipe.reportErrors(
                "Must add 'desktop-file-utils:runtime' to buildRequires"
                " for file(s) %s", ', '.join(self.arglist))
	macros = self.recipe.macros.copy()
        if self.category:
	    macros['category'] = '--add-category "%s"' %self.category
        else:
            macros['category'] = ''
	BuildCommand.do(self, macros)
	for file in self.arglist:
	    self.setComponents(macros.destdir,
                           macros.destdir + '%(datadir)s/applications'+file)


class Environment(BuildAction):
    """
    NAME
    ====

    B{C{r.Environment()}} - Set an environment variable

    SYNOPSIS
    ========

    C{r.Environment(I{'VARIABLE'}, I{'value'})}

    DESCRIPTION
    ===========

    The C{r.Environment()} class is called from within a Conary recipe to set
    an environment variable after all macros have been set.


    KEYWORDS
    ========

    The C{r.Environment()} class accepts the following keywords:

    B{use} : Optional argument of Use flag(s) telling whether to actually
    perform the action.

    EXAMPLES
    ========

    C{r.Environment('MOZ_THUNDERBIRD', '1')}

    Demonstrates calling C{r.Environment()} to set the environment variable
    C{MOZ_THUNDERBIRD} to C{1}.
    """
    def __init__(self, recipe, *args, **keywords):
	assert(len(args)==2)
	self.variable = args[0]
	self.value = args[1]
	action.RecipeAction.__init__(self, recipe, [], **keywords)
    def do(self, macros):
	os.environ[self.variable] = self.value % macros


class ClassPath(BuildAction):
    """
    NAME
    ====

    B{C{r.ClassPath()}} - Set the CLASSPATH environment variable

    SYNOPSIS
    ========

    C{r.ClassPath(I{'jars'})}

    DESCRIPTION
    ===========

    The C{r.ClassPath()} class is called from within a Conary recipe to set
    the CLASSPATH environment variable using C{r.classpath}.

    EXAMPLES
    ========

    C{r.Environment('junit', 'servlet-api')}
    """
    def do(self, macros):
        macros.classpath = self.recipe.classpath
        for jar in self.arglist:
            if macros.classpath:
                macros.classpath += ':'
            if '/' not in jar:
                log.info(('Adding %%(javadir)s/%s.jar to java class path' % jar) % macros)
                macros.classpath += '%%(javadir)s/%s.jar' % jar
            else:
                log.info('Adding %s.jar to java class path' % jar)
                macros.classpath += '%s.jar' % jar


class SetModes(_FileAction):
    """
    NAME
    ====

    B{C{r.SetModes()}} - Sets modes on files

    SYNOPSIS
    ========

    C{r.SetModes(I{file}, [I{file}, I{...},] I{mode})}

    DESCRIPTION
    ===========

    The C{r.SetModes()} class is called from within a Conary recipe to set
    modes on files in the C{%(destdir)s} or C{%(builddir)s} directories.

    For a file to be setuid in the repository, it needs to have its mode
    explicitly provided in the recipe.  File installation classes which
    provide a mode are sufficient, but for files installed by Makefiles,
    C{r.SetModes()} provides a specific intentional listing of their modes.

    Additionally, C{r.SetModes()} can be used to change arbitrary
    file modes in the destination directory or build directory. Relative
    paths are relative to the build directory.

    EXAMPLES
    ========

    C{r.SetModes('%(sbindir)s/sendmail', 02755)}

    Calls C{r.SetModes()} on the file C{%(sbindir)s/sendmail}, setting it to
    mode C{02755}.
    """

    def __init__(self, recipe, *args, **keywords):
        _FileAction.__init__(self, recipe, *args, **keywords)
	split = len(args) - 1
	self.paths = args[:split]
	self.mode = args[split]
	# raise error while we can still tell what is wrong...
	if type(self.mode) is not int:
	    self.init_error(TypeError, 'mode %s is not integer' % str(self.mode))

    def do(self, macros):
	files = []
        files = action._expandPaths(self.paths, macros, error=True)
	for f in files:
	    log.info('changing mode for %s to %o' %(f, self.mode))
	    self.chmod(macros.destdir, f)
	    self.setComponents(macros.destdir, f)

class _PutFiles(_FileAction):
    keywords = { 'mode': -1 }

    def do(self, macros):
        dest = action._expandOnePath(self.toFile, macros)
	util.mkdirChain(os.path.dirname(dest))

        fromFiles = action._expandPaths(self.fromFiles, macros)
        if not os.path.isdir(dest) and len(fromFiles) > 1:
            raise TypeError, 'multiple files specified, but destination "%s" is not a directory' %dest
        for source in fromFiles:
            self._do_one(source, dest, macros.destdir)

    def _do_one(self, source, dest, destdir):
	if os.path.isdir(source) and not self.move:
	    # deep copy of target dir
	    # foo/bar/a -> /blah should give /blah/a rather than /blah/foo/bar/a
	    dest = util.joinPaths(dest, os.path.basename(source))
	    util.mkdirChain(dest)
	    for sourcefile in os.listdir(source):
		thissrc = util.joinPaths(source, sourcefile)
		self._do_one(thissrc, dest, destdir)
	    return

	if os.path.isdir(dest):
	    dest = util.joinPaths(dest, os.path.basename(source))

	mode = self.mode
	if mode == -2:
	    # any executable bit on in source means 0755 on target, else 0644
	    sourcemode = os.lstat(source)[stat.ST_MODE]
	    if sourcemode & 0111:
		mode = 0755
	    else:
		mode = 0644

	if self.move:
            log.info('renaming %s to %s', source, dest)
            os.rename(source, dest)
	else:
            log.info('copying %s to %s', source, dest)
            shutil.copy2(source, dest)
	self.setComponents(destdir, dest)
	self.chmod(destdir, dest, mode=mode)


    def __init__(self, recipe, *args, **keywords):
        _FileAction.__init__(self, recipe, *args, **keywords)
	self.fromFiles = args[:-1]
	self.toFile = args[-1]
	# raise error while we can still tell what is wrong...
	if len(self.fromFiles) > 1:
	    if not self.toFile.endswith('/') or os.path.isdir(self.toFile):
		raise TypeError, 'too many targets for non-directory %s' %self.toFile

class Install(_PutFiles):
    """
    NAME
    ====

    B{C{r.Install()}} - Copies file while setting permissions

    SYNOPSIS
    ========

    C{r.Install(I{srcfile}, I{destfile})}

    DESCRIPTION
    ===========

    The C{r.Install()} class is called from within a Conary recipe to copy
    files from a source to destination as with C{r.Copy} with the exception
    that C{r.Install} also fixes the file's modes.

    Mostly, C{r.Install()} is used to install files from the C{%(builddir)s}
    to the C{%(destdir)s}. The argument C{srcfile} is normally a relative
    path, and C{destfile} is normally an absolute path.

    Note that a trailing slash on C{destfile} means to create the directory
    if necessary.  Source files with no execute permission will default
    to mode 0644; Source files with any execute permission will default
    to mode 0755.  If that rule doesn't suffice, use C{mode=0}I{octalmode}
    to set the mode explicitly.

    EXAMPLES
    ========

    C{r.Install('am-utils.conf', '%(sysconfdir)s/amd.conf')}

    Demonstrates calling C{r.Install()} to install the file
    C{am-utils.conf} to the location C{%(sysconfdir)s/amd.conf}.
    """
    keywords = { 'mode': -2 }

    def __init__(self, recipe, *args, **keywords):
	_PutFiles.__init__(self, recipe, *args, **keywords)
	self.source = ''
	self.move = 0

class Copy(_PutFiles):
    """
    NAME
    ====

    B{C{r.Copy()}} - Copies files without changing the mode

    SYNOPSIS
    ========

    C{r.Copy(I{srcfile}, I{destfile})}

    DESCRIPTION
    ===========

    The C{r.Copy()} class is called from within a Conary recipe to copy files
    from a source directory to a destination directory without changing the
    mode of the file(s).

    Note that a trailing slash on a destination file means to create the
    directory if necessary, and use the basename of C{srcname} for the name
    of the file created in the destination directory.  The mode of C{srcfile}
    is used for C{destfile} unless you set C{mode=0}I{octalmode}.

    EXAMPLES
    ========

    C{r.Copy('mm_cfg.py', 'Mailman/mm_cfg.py.dist')}

    Demonstrates calling C{r.Copy()} and specifying the file C{mm_cfg.py} be
    copied to C{Mailman/mm_cfg.py.dist}.
    """
    def __init__(self, recipe, *args, **keywords):
	_PutFiles.__init__(self, recipe, *args, **keywords)
	self.source = '%(destdir)s'
	self.move = 0

class Move(_PutFiles):
    """
    NAME
    ====

    B{C{r.Move()}} - Moves files

    SYNOPSIS
    ========

    C{r.Move(I{srcname}, I{destname})}

    DESCRIPTION
    ===========

    The C{r.Move()} class is called from within a Conary recipe to move files.

    Note that a trailing slash on the C{destfile} means to create the directory
    if necessary, and use the basename of C{srcname} for the name of
    the file created in the destination directory.  The mode is preserved,
    unless you explicitly set the new mode with C{mode=0}I{octalmode}.

    EXAMPLES
    ========

    C{r.Move('%(sbindir)s/lpc', '%(sbindir)s/lpc.cups')')}

    Calls C{r.Move()} to move the file C{%(sbindir)s/lpc} to
    C{%(sbindir)s/lpc.cups}.
    """
    def __init__(self, recipe, *args, **keywords):
	_PutFiles.__init__(self, recipe, *args, **keywords)
	self.source = '%(destdir)s'
	self.move = 1

class Symlink(_FileAction):
    """
    NAME
    ====

    B{C{r.Symlink()}} - Creates a symbolic link

    SYNOPSIS
    ========

    C{r.Symlink(I{realfile}, I{symlink})}

    DESCRIPTION
    ===========

    The C{r.Symlink()} class is called from within a Conary recipe to create     a symbolic link to a file.

    Multiple symlinks can be created if the destination path C{realfile} is a
    directory. The destination path is determined to be a directory if it
    already exists, or if the path ends with a slash (C{/}) character.

    KEYWORDS
    ========

    The C{r.Symlink()} class accepts the following keywords, with default
    values shown in parentheses when applicable:

    EXAMPLES
    ========

    C{r.Symlink('enable', '%(bindir)s/cups-enable')}

    Calls C{r.Symlink()} to create a symbolic link named C{enable} from
    the C{realfile} C{%(bindir)s/cups-enable}.
    """
    # This keyword is preserved only for compatibility for existing
    # recipes; DanglingSymlinks policy should enforce non-dangling
    # status when it matters.
    keywords = { 'allowDangling': True }

    def do(self, macros):
	dest = action._expandOnePath(self.toFile, macros)

        if dest.endswith(os.sep):
            util.mkdirChain(dest)
        else:
            util.mkdirChain(os.path.dirname(dest))

        targetIsDir = os.path.isdir(dest)
        if targetIsDir:
            destdir = dest
        else:
            destdir = os.path.dirname(dest)

        sources = []
        for fromFile in self.fromFiles:
            sources.extend(util.braceExpand(fromFile %macros))

        # do glob expansion and path verification on all of the source paths
        expandedSources = []
        if destdir.startswith(macros.destdir):
            baseDir = macros.destdir
        else:
            baseDir = macros.builddir

        for source in sources:
            # if the symlink contains a /, concatenate in order to glob
            if source[0] == '/':
                expand = baseDir + source
            else:
                expand = util.joinPaths(baseDir, source)

            sources = fixedglob.glob(expand)
            for expanded in sources:
                if os.sep in source:
                    thisSource = util.joinPaths(os.path.dirname(source),
				                os.path.basename(expanded))
                else:
                    thisSource = os.path.basename(expanded)
                expandedSources.append(thisSource)
        sources = expandedSources

        if len(sources) > 1 and not targetIsDir:
            raise TypeError, 'creating multiple symlinks, but destination is not a directory'

        for source in sources:
            if targetIsDir:
                to = util.joinPaths(dest, os.path.basename(source))
            else:
                to = dest
            self.setComponents(macros.destdir, to)
	    if os.path.exists(to) or os.path.islink(to):
		os.remove(to)
            log.info('creating symlink %s -> %s' %(to, source))
	    if source[0] == '.':
		log.warning('back-referenced symlink %s should probably be replaced by absolute symlink (start with "/" not "..")', source)
	    os.symlink(util.normpath(source), to)

    def __init__(self, recipe, *args, **keywords):
        """
        Create a new Symlink instance

        @keyword fromFiles: paths(s) to which symlink(s) will be created
        @type fromFiles: str or sequence of str
        @keyword toFile: path to create the symlink, or a directory in which
                       to create multiple symlinks
        @type toFile: str
        """
        _FileAction.__init__(self, recipe, *args, **keywords)
	split = len(args) - 1
	self.fromFiles = args[:split]
	self.toFile = args[split]
	# raise error while we can still tell what is wrong...
	if not self.fromFiles:
	    self.init_error(TypeError, 'not enough arguments')
	if len(self.fromFiles) > 1:
	    if not self.toFile.endswith('/') or os.path.isdir(self.toFile):
		self.init_error(TypeError, 'too many targets for non-directory %s' %self.toFile)

class Link(_FileAction):
    """
    NAME
    ====

    B{C{r.Link()}} - Creates a hard link

    SYNOPSIS
    ========

    C{r.Link(I{newname(s)}, I{existingname})}

    DESCRIPTION
    ===========

    The C{r.Link()} class is called from within a Conary recipe to install a
    hard link.

    Note: The use of hard links is strongly discouraged in most cases.
    Hardlinks are limited to the same directory and symbolic links should
    always be chosen in preference to them.  You should not use hard links
    unless the situation deems using them B{absolutely} necessary.

    EXAMPLES
    ========

    C{r.Link('mumble', 'passwd')}

    Demonstrates calling C{r.Link()} to create a hard link from the file
    C{passwd} to the file C{mumble}.
    """
    def do(self, macros):
	d = macros['destdir']
        self.existingpath = self.existingpath % macros
        if self.existingpath and self.existingpath[0] != '/':
            self.init_error(TypeError,
                'hardlink %s must be located in destdir' %self.existingpath)
	e = util.joinPaths(d, self.existingpath)
	if not os.path.exists(e):
	    raise TypeError, 'hardlink target %s does not exist' %self.existingpath
	for name in self.newnames:
	    newpath = util.joinPaths(self.basedir, name)
	    n = util.joinPaths(d, newpath)
	    self.setComponents(d, n)
	    if os.path.exists(n) or os.path.islink(n):
		os.remove(n)
	    os.link(e, n)

    def __init__(self, recipe, *args, **keywords):
        """
        Create a new Link instance::
	    self.Link(newname, [newname, ...,] existingpath)
        """
        _FileAction.__init__(self, recipe, *args, **keywords)
	self.newnames = args[:-1]
	self.existingpath = args[-1]

	# raise error while we can still tell what is wrong...
	for name in self.newnames:
	    if name.find('/') != -1:
		self.init_error(TypeError, 'hardlink %s crosses directories' %name)
	self.basedir = os.path.dirname(self.existingpath)

class Remove(BuildAction):
    """
    NAME
    ====

    B{C{r.Remove()}} - Removes files

    SYNOPSIS
    ========

    C{r.Remove(I{filename}, I{...} || I{dirname}, I{...} [I{recursive}])}

    DESCRIPTION
    ===========

    The C{r.Remove()} class is called from within a Conary recipe to remove
    one or more files.

    EXAMPLES
    ========

    C{r.Remove('/lib/modules/%(kver)s/modules.*')}

    Calls C{r.Remove()} to remove the C{modules.*} files from the
    C{/lib/modules/%(kver)s/} subdirectory.

    """
    keywords = { 'recursive': False }

    def do(self, macros):
	for path in action._expandPaths(self.filespecs,
                                        macros, braceGlob=False):
	    if self.recursive:
		util.rmtree(path, ignore_errors=True)
	    else:
		util.remove(path)

    def __init__(self, recipe, *args, **keywords):
        BuildAction.__init__(self, recipe, **keywords)
	if type(args[0]) is tuple:
	    self.filespecs = args[0]
	else:
	    self.filespecs = args

class Replace(BuildAction):

    """
    NAME
    ====

    B{C{r.Replace()}} - Substitute text in a file

    SYNOPSIS
    ========

    C{r.Replace(I{old}, I{new}, I{path+} || (I{old}, I{new})+, I{path}+)}

    DESCRIPTION
    ===========

    The C{r.Replace()} class is called from within a Conary recipe to
    substitute text *old* with *new* in a file using Python regular
    expression rules

    Note that C{r.Replace()} cannot do multi-line substitutions.  For more
    complicated replacements, C{sed} is appropriate.  However, C{r.Replace()}
    performs error checking that C{sed} does not.

    By default, C{r.Replace()} will raise an error if a file passed into
    C{r.Replace()} is not modified by any of the regular expressions given.
    The allowNoChange keyword can be used to turn off that behavior.

    The lines matched by Replace can be restricted by the {lines} keyword.
    Lines may consist of a tuple *(begin, end)* or a single integer *line* or a
    regular expression of lines to match.  Lines are indexed starting with 1.

    Remember that python will interpret C{\1}-C{\7} as octal characters.
    You must either escape the backslash: C{\\1} or make the string raw by
    prepending C{r} to the string (e.g. C{r.Replace('(a)', r'\1bc'))}

    KEYWORDS
    ========

    The C{r.Replace()} class accepts the following keywords, with default
    values shown in parentheses when applicable:

    B{allowNoChange} : (False) Do not raise an error if C{I{pattern}} did
    not apply

    B{lines:} : (None) Determines the lines to which the replacement applies

    EXAMPLES
    ========

    C{r.Replace('-lgphoto2', '-lgphoto2 -lgphoto2_port', 'gphoto2-config')}

    Calls C{r.Replace()} to change C{-lgphoto2} to 
    C{-lgphoto2 -lgphoto2_port} in the path C{gphoto2-config}.
    """


    keywords = { 'allowNoChange' : False,
                 'lines'         : None }

    octalchars = re.compile('[\1\2\3\4\5\6\7]')

    def __init__(self, recipe, *args, **keywords):
        """
        @keyword lines: Determines the lines to which the replacement applies
        @type lines: tuple (start, end) of lines to match or int, the single
        line to match, or str, a regexp of lines to match. (default: all)
        @keyword allowNoChange: do not raise an error if C{I{pattern}} did
                                not apply
        @type allowNoChange: bool (default: False)
        """
        BuildAction.__init__(self, recipe, **keywords)
        if not args:
	    self.init_error(TypeError, 'not enough arguments')
        if isinstance(args[0], (list, tuple)):
            # command is in Replace((pattern, sub)+, file+) format
            self.regexps = []
            while args and isinstance(args[0], (list, tuple)):
                self.regexps.append(args[0])
                args = args[1:]
        else:
            # command is in Replace(pattern, sub, file+) format
            if len(args) < 2:
                self.init_error(TypeError, 'not enough arguments')
            self.regexps = ([args[0], args[1]],)
            args = args[2:]
        for pattern, sub in self.regexps:
            if self.octalchars.match(sub):
                self.init_error(TypeError,
                    r'Illegal octal character in substitution string "%s":'
                    r' prepend "r", as in r"\1"' %sub)

        if not args:
	    self.init_error(TypeError,
                            'not enough arguments: no file glob supplied')
        self.paths = args[:]

        if [ x for x in self.paths if not x ]:
	    self.init_error(TypeError,
                            'empty file path specified to Replace')

        self.min = self.max = self.lineMatch = None
        if self.lines:
            if isinstance(self.lines, (list, tuple)):
                self.min, self.max = (self.lines)
            elif isinstance(self.lines, int):
                self.min = self.max = self.lines
            elif isinstance(self.lines, str):
                self.lineMatch = re.compile(self.lines)
        if self.min is not None and min(self.min, self.max, 1) != 1:
            self.init_error(RuntimeError,
                            "Replace() line indices start at 1, like sed")

    def _lineMatches(self, index, line):
        min, max, lineMatch = self.min, self.max, self.lineMatch
        if ((not min or index >= min) and (not max or index <= max)
             and (not lineMatch or lineMatch.search(line))):
                return True
        return False

    def do(self, macros):
        paths = action._expandPaths(self.paths, macros, error=True)
        log.info("Replacing '%s' in %s",
                  "', '".join(["' -> '".join(x) for x in self.regexps ] ),
                  ' '.join(paths))
        if not paths:
            if self.allowNoChange:
                log.warning("Did not find any matching files for file globs")
                return
            else:
                raise RuntimeError, \
                        "Did not find any matching files for file globs"

        regexps = []
        for pattern, sub in self.regexps:
            regexps.append((re.compile(pattern % macros), sub % macros))

        unchanged = []
        for path in paths:
            if not util.isregular(path):
                log.warning("%s is not a regular file, not applying Replace")
                continue

            fd, tmppath = tempfile.mkstemp(suffix='rep',
                                           prefix=os.path.basename(path),
                                           dir=os.path.dirname(path))
            try:
                foundMatch = False
                index = 1
                for line in open(path):
                    if self._lineMatches(index, line):
                        for (regexp, sub) in regexps:
                            line, count = regexp.subn(sub, line)
                            if count:
                                foundMatch = True

                    os.write(fd, line)
                    index += 1

                if foundMatch:
                    mode = os.stat(path)[stat.ST_MODE]
                    os.rename(tmppath, path)
                    os.chmod(path, mode)

            finally:
                if os.path.exists(tmppath):
                    os.remove(tmppath)
                os.close(fd)
            if not foundMatch:
                unchanged.append(path)

        if unchanged:
            msg = ("The following files were not modified during the "
                   "replacement: %s" % '\n'.join(unchanged))
            if self.allowNoChange:
                log.warning(msg)
            else:
                raise RuntimeError, msg

class Doc(_FileAction):
    """
    NAME
    ====

    B{C{r.Doc()}} - Installs documentation files

    SYNOPSIS
    ========

    C{r.Doc(I{filename}, [I{subdir=/path}])}

    DESCRIPTION
    ===========

    The C{r.Doc()} class is called from within a Conary recipe to Install
    documentation files from the C{%(builddir)s} into
    C{%(destdir)s/%(thisdocdir)s}.

    Specify a single file or directory of files for the C{filename} parameter.
    The C{subdir=path} keyword argument can be used to create a subdirectory
    of C{%(destdir)s/%(thisdocdir)s} where files may subsequently be located.

    KEYWORDS
    ========

    The C{r.Doc()} class accepts the following keywords:

    B{subdir} : Specify a subdirectory to create before placing documentation
    files into it.

    EXAMPLES
    ========

    C{r.Doc('doc/kbd.FAQ*.html', subdir='html')}

    Demonstrates installing C{doc/kbd.FAQ*.html} files into the C{html}
    subdirectory after first creating the C{html} subdirectory using
    C{r.Doc()}.

    C{r.Doc('pam_smb.conf.example')}

    Demonstrates using C{r.Doc} to place the file C{pam_smb.conf.example}
    into C{%(destdir)s/%(thisdocdir)s}.

    C{r.Doc("html/")}

    Demonstrates using C{r.Doc} to place the subdirectory C{html} from
    C{%(builddir)s} into C{%(destdir)s/%(thisdocdir)s}. 
    """
    keywords = {'subdir':  '',
		'mode': 0644,
		'dirmode': 0755}

    def do(self, macros):
	macros = macros.copy()
	destlen = len(macros['destdir'])
	if self.subdir:
	    macros['subdir'] = '/%s' % self.subdir
	else:
	    macros['subdir'] = ''
	base = '%(thisdocdir)s%(subdir)s/' %macros
	dest = macros.destdir + base
	util.mkdirChain(os.path.dirname(dest))
	for path in action._expandPaths(self.paths, macros, error=True):
	    for newpath in util.copytree(path %macros, dest, True,
					 filemode=self.mode,
					 dirmode=self.dirmode):
		self.setComponents(macros.destdir, newpath)

    def __init__(self, recipe, *args, **keywords):
        _FileAction.__init__(self, recipe, *args, **keywords)
	if type(args[0]) is tuple:
	    self.paths = args[0]
	else:
	    self.paths = args

class Create(_FileAction):
    """
    NAME
    ====

    B{C{r.Create()}} - Creates a file

    SYNOPSIS
    ========

    C{r.Create(I{emptyfile}, [I{contents},] [I{macros},] [I{mode}])}

    DESCRIPTION
    ===========

    The C{r.Create()} class is called from within a Conary recipe to create a
    file. The file may be created empty, or with with contents specified
    optionally.

    Without B{contents} specified, C{r.Create()} behaves like C{touch foo}.
    If B{contents} is specified C{r.Create} acts more like
    C{cat > foo <<EOF ... EOF}. If B{contents} is not empty, then a newline
    will be implicitly appended unless B{contents} already ends in a newline.

    KEYWORDS
    ========

    The C{r.Create()} class accepts the following keywords:

    B{contents} : The (optional) contents of the file

    B{macros}  : Whether to interpolate macros into the contents

    B{mode} : The mode of the file (defaults to 0644)

    EXAMPLES
    ========

    C{r.Create('%(localstatedir)s/log/acpid', mode=0640)}

    Demonstrates calling C{r.Create()} specifying the creation of
    C{%(localstatedir)s/log/acpid} with mode C{0640}.
    """
    keywords = {'contents': '',
		'macros': True,
		'mode': 0644}
    def do(self, macros):
	if self.macros:
	    contents = self.contents %macros
	else:
	    contents = self.contents
	if contents and contents[-1] != '\n':
	    contents += '\n'
	for bracepath in action._expandPaths(self.paths, macros,
                                                         braceGlob=False):
	    for fullpath in util.braceExpand(bracepath):
		util.mkdirChain(os.path.dirname(fullpath))
		f = file(fullpath, 'w')
		f.write(contents)
		f.close()
		self.setComponents(macros.destdir, fullpath)
		self.chmod(macros.destdir, fullpath)

    def __init__(self, recipe, *args, **keywords):
        """
        @keyword contents: The (optional) contents of the file
        @keyword macros: Whether or not to interpolate macros into the contents
        @keyword mode: The mode of the file (defaults to 0644)
        """
        _FileAction.__init__(self, recipe, *args, **keywords)
	if type(args[0]) is tuple:
	    self.paths = args[0]
	else:
	    self.paths = args

class MakeDirs(_FileAction):
    """
    NAME
    ====

    B{C{r.MakeDirs()}} - Creates directories

    SYNOPSIS
    ========

    C{r.MakeDirs(I{dir(s)}, [I{component}], [I{mode}])}

    DESCRIPTION
    ===========

    The C{r.MakeDirs()} class is called from within a Conary recipe to create
    directories.

    KEYWORDS
    ========

    The C{r.MakeDirs()} class accepts the following keywords, with default
    values shown in parentheses when applicable:

    B{component} : (None) Set to component name if package is responsible for the
    directory.

    B{mode} : (0755) Specify directory access permissions

    EXAMPLES
    ========

    C{r.MakeDirs('/misc', component='runtime')}

    Demonstrates C{r.MakeDirs()} creating the C{/misc} directory while
    specifying the C{:runtime} component is responsible for this directory.

    C{r.MakeDirs('/afs', mode=0700)}

    Demonstrates C{r.MakeDirs()} creating the C{/afs} directory and setting
    access permissions to C{0700}.
    """
    keywords = { 'mode': 0755 }

    def do(self, macros):
        for path in action._expandPaths(self.paths, macros, braceGlob=False):
            dirs = util.braceExpand(path)
            for d in dirs:
                log.info('creating directory %s', d)
		self.setComponents(macros.destdir, d.replace('%', '%%'))
                util.mkdirChain(d)
                self.chmod(macros.destdir, d)

    def __init__(self, recipe, *args, **keywords):
        _FileAction.__init__(self, recipe, *args, **keywords)
	if type(args[0]) is tuple:
	    self.paths = args[0]
	else:
	    self.paths = args

class TestSuite(_FileAction):
    """
    NAME
    ====

    B{C{r.TestSuite()}} - Creates a script to run package test suite

    SYNOPSIS
    ========

    C{r.TestSuite(I{dir}, I{command})}

    DESCRIPTION
    ===========

    The C{r.TestSuite()} class is called from within a Conary recipe to create
    a script to run the package's test suite.

    TestSuite also modifies Makefiles in order to compile binaries needed for
    testing at cook time, while allowing the actual test suite to run at a
    later point.  It does this if the command to be run is of the form
    C{make  <target>}, in which case all of the target's dependencies are
    built, and the Makefile is edited to then remove those dependencies from
    the target.

    If the command is a make command, the arguments
    C{-o Makefile -o config.status} are added to help ensure that
    C{automake} does not try to regenerate the Makefile at test time.

    KEYWORDS
    ========

    The C{r.TestSuite()} class accepts the following keywords, with default
    values shown in parentheses when applicable:

    None

    EXAMPLES
    ========

    C{r.TestSuite('testsuite/', 'runtest')}

    Calls C{r.TestSuite()} to execute the test suite C{runtest} in the
    directory C{testsuite/}.
    """

    commandScript = '''#!/bin/sh -x
# run this script to execute the test suite
failed=0
for test in conary-testsuite-*; do
    ./$test || failed=$?
done
exit $failed
'''

    testSuiteScript = '''#!/bin/sh -x
# testsuite
pushd ./%(dir)s
    failed=0
    subdirs="%(subdirs)s"
    for subdir in $subdirs; do
	pushd $subdir;
	    %(command)s || failed=$?
	popd;
    done
    %(command)s || failed=$?
popd
exit $failed
'''

    idnum = 0

    command_path = '%(thistestdir)s/conary-test-command'
    testsuite_path = '%(thistestdir)s/conary-testsuite-%(identifier)s'
    keywords = {'ignore'    : [],
		'recursive' : False,
		'autoBuildMakeDependencies' : True,
		'subdirs'   : [] }


    def __init__(self, recipe, *args, **keywords):
        """
        @keyword ignore: A list of files to tell make not to rebuild.
	In addition to this key, make is told to never rebuild Makefile
	or config.status.  Default: []
        @keyword recursive:  If True, modify all the Makefiles below the given directory.  Default: False
        @keyword subdirs: Modify the Makefiles in the given subdirs.  Default: []
        """
        _FileAction.__init__(self, recipe, *args, **keywords)
	if len(args) > 2:
	    raise TypeError, ("TestSuite must be passed a dir to run in and"
		    "the command will execute this package's test suite")
	self.mode=0755
	self.component = ':test'
	if len(args) == 0:
	    self.dir == '.'
	else:
	    self.dir = args[0]
	if len(args) < 2:
	    self.command = 'make check-TESTS'
	else:
	    self.command = args[1]
	if self.subdirs and not isinstance(self.subdirs, (tuple, list)):
	    self.subdirs = [self.subdirs]
	# turn on test component
	recipe.TestSuiteLinks(build=True)

    def writeCommandScript(self):
        destdir = self.macros.destdir
	path = self.command_path % self.macros
	fullpath = destdir + path
	if not os.path.exists(fullpath):
	    util.mkdirChain(os.path.dirname(fullpath))
	    f = open(fullpath, 'w')
	    f.write(self.commandScript)
	    self.chmod(destdir, fullpath)
	    self.setComponents(destdir, fullpath)

    def writeTestSuiteScript(self):
	idnum = TestSuite.idnum
	TestSuite.idnum = idnum + 1
	self.macros.identifier = str(idnum)
	path = self.testsuite_path % self.macros
	fullpath = self.macros.destdir + path
	self.macros.subdirs = ' '.join(self.subdirs)
	if not os.path.exists(fullpath):
	    f = open(fullpath, 'w')
	    f.write(self.testSuiteScript % self.macros)
	    self.chmod(self.macros.destdir, self.macros.destdir + path)

    def mungeMakeCommand(self):
        """
        Munge the make command to ignore files that do not need to be rebuilt.
        """
	if self.ignore is not None:
	    ignoreFiles = [ 'Makefile', 'config.status' ]
	    ignoreFiles.extend(self.ignore)
	    ignoreOpts = ' -o ' + ' -o '.join(ignoreFiles)
	    makeCmd = 'export MAKE="make' + ignoreOpts + '"\n'
	    self.command = makeCmd + '$MAKE' + self.command[4:]
	    self.macros.command = self.command

    def buildMakeDependencies(self, dir, command):
        """
        Build, and remove from Makefiles, appropriate make dependencies,
        so that when the test suite is run, the makefile does not
        spuriously require the capability to build any
        already-built test executables.
        """
	makefile = dir + '/Makefile'
        makeTarget = self.makeTarget
	if self.recursive:
	    files = os.listdir(dir)
	    base = util.normpath(self.macros.builddir + os.sep + self.dir)
	    baselen = len(base) + 1 # for appended /
	    for file in files:
		fullpath = '/'.join([dir,file])
		if os.path.isdir(fullpath) and os.path.exists(fullpath + '/Makefile'):
		    self.subdirs.append(fullpath[baselen:])
		    self.buildMakeDependencies(fullpath, command)
	util.execute(r"sed -i 's/^%s\s*:\(:\?\)\s*\(.*\)/conary-pre-%s:\1 \2\n\n%s:\1/' %s" % (makeTarget, makeTarget, makeTarget, makefile))
	util.execute('cd %s; make %s conary-pre-%s' % (dir, ' '.join(self.makeArgs), makeTarget))

    def do(self, macros):
	self.macros = macros.copy()
	self.macros.dir = self.dir
	self.macros.command = self.command
	self.writeCommandScript()
	command = self.command % macros
	if command[:4] == 'make':
            self.mungeMakeCommand()
            if self.autoBuildMakeDependencies:
                self.makeArgs = []
                potentialTargets  = command[5:].split()
                targets = []
                for t in potentialTargets:
                    if t.find('=') == -1:
                        targets.append(t)
                    else:
                        self.makeArgs.append(t)
                for t in targets:
                    self.makeTarget = t
                    self.buildMakeDependencies(util.normpath(self.macros.builddir + os.sep + self.dir), self.command)
	self.writeTestSuiteScript()


class ConsoleHelper(BuildAction):
    """
    NAME
    ====

    B{C{r.ConsoleHelper()}} - Set up consolehelper symlinks, control files, and dependency

    SYNOPSIS
    ========

    C{r.ConsoleHelper(I{linkname}, I{realprogram}, [I{consoleuser=bool(False)},] [I{timestamp=bool(False)},] [I{pamfile=path},] [I{targetuser=user(root)},] [I{session=bool},] [I{fallback=bool},] [I{noxoption=optstring},] [I{otherlines=linelist}])}

    DESCRIPTION
    ===========

    The C{r.ConsoleHelper()} class is called from within a Conary recipe to
    set up C{consolehelper} symlinks, control files, and dependency for an
    application.

    The C{linkname} and C{realprogram} paths are relative to destdir.

    Setting C{consoleuser} to C{True} allows the console user to access
    the service without providing a password; for best security, it
    defaults to False.

    Setting C{timestamp} to C{True} allows recently authenticated users
    to access the service without providing a password; for best security,
    it defaults to False.

    The default C{pamfile} contains auth via C{pam_rootok}, then
    C{pam_stack service=system-auth} and everything else as
    C{pam_permit}.  Otherwise, you can just provide the name of a file,
    relative to the builddir, to use.  If C{pamfile} is set, then
    C{consoleuser} is ignored.

    The C{session}, C{fallback}, and C{noxoption} default to None,
    which means that no line is generated in the C{console.apps}
    file.  The C{otherlines} option is the catchall for options
    otherwise unhandled by C{ConsoleHelper}, and is simply a list
    of text lines (without newline characters) to place in the
    C{console.apps} file.  It is assumed that C{session=True} implies
    that the application uses X.

    KEYWORDS
    ========

    The C{r.ConsoleHelper()} class accepts the following keywords, with
    default values shown in parentheses when applicable:

    B{consoleuser} : (False) Allows the console user to access service
    without password if set to C{True}.

    B{fallback} : (None) By default, no entry is added to the C{console.apps}
    file

    B{noxoption}': (None) By default, no entry is added to the C{console.apps}
    file

    B{otherlines}: Additional options catchall

    B{pamfile} : Specify a C{pamfile} relative to the builddir. If used,
    I{consoluser} is ignored.

    B{session} : (None) By default, no entry is added to the C{console.apps}
    file

    B{targetuser} : (root) User service is executed as

    B{timestamp} : (False) If set to C{True}, allows recently authenticated user
    access to the service without entering a password

    EXAMPLES
    ========

    C{r.ConsoleHelper('%(bindir)s/xmtr', '%(prefix)s/X11R6/bin/xmtr', session=True)}

    Demonstrates calling C{r.ConsoleHelper()} and specifying the link name
    C{%(bindir)s/xmtr}, program name C{%(prefix)s/X11R6/bin/xmtr}, and using
    the argument C{session=True}.
    """
    keywords = {
        'consoleuser': False,
        'timestamp': False,
        'pamfile': None,
        'targetuser': 'root',
        'session': None,
        'fallback': None,
        'noxoption': None,
        'otherlines': None
    }

    def do(self, macros):
        self.linkname = self.linkname %macros
        self.realprogram = self.realprogram %macros
        programname = os.path.basename(self.linkname)

        dest = macros.destdir+self.linkname
        if dest.endswith(os.sep):
            util.mkdirChain(dest)
        else:
            util.mkdirChain(os.path.dirname(dest))
        os.symlink(util.normpath(('%(bindir)s/consolehelper'%macros)),
                   macros.destdir+self.linkname)

        destpath = '%(destdir)s%(sysconfdir)s/pam.d/' %macros
        util.mkdirChain(os.path.dirname(destpath))
        destpath += programname
        if self.pamfile:
            util.copyfile(self.pamfile, destpath)
        else:
            contents = [
                '#%PAM-1.0',
                'auth       sufficient   pam_rootok.so',
            ]
            if self.consoleuser is True:
                contents.append('auth       sufficient   pam_console.so')
            if self.timestamp is True:
                contents.append('auth       sufficient   pam_timestamp.so')
            contents.extend([
                'auth       required     pam_stack.so service=system-auth',
                'account    required     pam_permit.so'
            ])
            if self.session is True or self.timestamp is True:
                contents.append('session    required     pam_permit.so')
                if self.session is True:
                    contents.append('session    optional     pam_xauth.so')
                if self.timestamp is True:
                    contents.append('session    optional     pam_timestamp.so')
            f = file(destpath, 'w')
            f.writelines([ x+'\n' for x in contents])
            f.close()
            os.chmod(destpath, 0644)


        destpath = '%(destdir)s%(sysconfdir)s/security/console.apps/' %macros
        util.mkdirChain(os.path.dirname(destpath))
        destpath += programname
        contents = [
            'PROGRAM='+self.realprogram,
            'USER='+self.targetuser,
        ]
        boolMap = {True: 'true', False: 'false'}
        if self.session is not None:
            contents.append('SESSION='+boolMap[self.session])
        if self.fallback is not None:
            contents.append('FALLBACK='+boolMap[self.fallback])
        if self.noxoption is not None:
            contents.append('NOXOPTION='+self.noxoption)
        if self.otherlines is not None:
            contents.extend(self.otherlines)

        f = file(destpath, 'w')
        f.writelines([ x+'\n' for x in contents])
        f.close()
        os.chmod(destpath, 0644)

    def __init__(self, recipe, *args, **keywords):
        BuildAction.__init__(self, recipe, **keywords)
        assert(len(args)==2)
        self.linkname = args[0]
        self.realprogram = args[1]
        # automatically depend on consolehelper
        # cannot use %(bindir)s here, do not have macros...
        recipe.Requires('/usr/bin/consolehelper', self.linkname)


class XInetdService(_FileAction):
    """
    NAME
    ====

    B{C{r.XInetdService()}} - Creates a file in /etc/xinetd.d

    SYNOPSIS
    ========

    C{r.XInetdService(I{name}, I{description},
    [server=I{'/path/to/server',}] [server_args=I{'--args'}]
    [protocol=I{'protocol'},] [port=I{'portnumber'},]
    [default=I{bool(False)},] [type=I{'type'},] [id=I{'bool(False)'},]
    [socket_type=I{'socket_type'},] [user=I{'user},] [group=I{'group'},]
    [wait=I{bool(False)},] [disable=I{bool(true)},]
    [log_on_success=I{'VALUES'},] [log_on_failure=I{'VALUES'},]
    [filename=I{'/etc/xinetd.d/something'},] [mode=I{0644},]
    [otherlines=[I{'list'}, 'of', 'I{lines'}],])}

    DESCRIPTION
    ===========

    The C{r.XInetdService()} class is called from within a Conary recipe to
    create a file in /etc/xinetd.d for running an application from the
    C{xinetd} daemon.

    Specify only the arguments that you absolute need to specify.
    The C{otherlines} argument should be a list of lines, and should
    not include any leading tabs or trailing newlines.  The arguments
    are generally as defined in the xinetd.conf man page, with the
    exception that the arguments listed as C{I{bool(...)}} are translated
    from Python boolean values to "yes" and "no".  C{default} is the
    chkconfig default value and C{description} is the chkconfig
    description; they are encoded as comments.  Do not include comment
    characters in either, and do not wrap the description; it will be
    nicely wrapped for you according to chkconfig's rules.

    KEYWORDS
    ========

    The C{r.XInetdService()} class accepts the following keywords, with
    default values shown in parentheses when applicable:

    B{serviceName} : (None) Specifies the name of the service, and must be
    identical to the string passed to C{xinetd} when the remote service
    requestor first makes the connection (Per RFC 1078)

    B{description} : (None) Specifies a human-readable description of the
    service

    B{server} : (None) Specifies the program that will be executed for this
    service

    B{server_args} : (None) Specifies additional command-line arguments to the
    program defined by B{server}

    B{protocol} : (None) Specifies the protocol to be used by the service from
    the listing of valid protocols contained in C{/etc/protocols}

    B{port} : (None) Specifies the service port.
 
    B{default} : (False) Specifies the C{chkconfig} default value

    B{type} : (None) Specifies whether the server or C{xinetd} will handle
    the initial protocol handshake

    B{socket_type} : (None) Specifies type of socket the service should
    use.

    B{id} : (False) Specifies unique identifier for the service

    B{wait} : (False) Specifies whether service is single-threaded or
    multi-threaded, and whether the server program accepts the connection, or
    C{xinetd} accepts the connection

    B{disable} : (True) Specifies whether the service should be disabled

    B{user} : (None) Specifies the User ID for the server process

    B{group} : (None) Specifies the Group ID for the server process

    B{log_on_success} : (None) Specifies information to be logged when
    a server is started, and when a server exits

    B{log_on_failure} : (None) Specifies information to log when a server
    cannot be started
 
    B{filename} : (None) Specifies service definition filename
 
    B{mode} : (0644) Specifies permissions of service definition file

    B{otherlines} : (None) Specifies additional service definition lines

    EXAMPLES
    ========
    C{r.XInetdService('swat', 'SWAT is the Samba Web Admin Tool. Use swat to
    configure your Samba server. To use SWAT, connect to port 901 with your
    favorite web browser.', port='901', socket_type='stream', wait=False,
    otherlines=['only_from        = 127.0.0.1', 'log_on_failure  += USERID'],
    user='root', server='%(sbindir)s/swat')}

    Calls C{r.XInetdService()} to create a C{swat} entry in the
    C{/etc/xinetd.d} directory so that the Samba Web Administration Tool
    (C{swat}) may run from the C{xinetd} service daemon.
    """
    keywords = {
        'serviceName':    None,
        'description':    None,
        'server':         None,
        'server_args':    None,
        'protocol':       None,
        'port':           None,
        'default':        False,
        'type':           None,
        'socket_type':    None,
        'id':             False,
        'wait':           False,
        'disable':        True,
        'user':           None,
        'group':          None,
        'log_on_success': None,
        'log_on_failure': None,
        'filename':       None,
        'mode':           0644,
        'otherlines':     None,
    }

    def do(self, macros):
        c = [
            "# default: %(default)s",
            "%(description_text)s",
            "",
            "service %(serviceName)s",
            "{",
        ]

        if self.protocol:
            c.append("\tprotocol\t= %(protocol)s")

        if self.port:
            c.append("\tport\t\t= %(port)s")

        if self.type:
            c.append("\ttype\t\t= %(type)s")

        if self.server:
            c.append("\tserver\t\t= %(server)s")

        if self.server_args:
            c.append("\tserver_args\t= %(server_args)s")

        if not self.socket_type:
            c.append("\tsocket_type\t= %(socket_type)s")

        if self.id:
            c.append("\tid\t\t= %(serviceName)s-(socket_type)s")

        if self.wait:
            self.wait = 'yes'
        else:
            self.wait = 'no'
        c.append("\twait\t\t= %(wait)s")

        if self.disable:
            self.disable = 'yes'
        else:
            self.disable = 'no'
        c.append("\tdisable\t\t= %(disable)s")

        if self.user:
            c.append("user\t\t= %(user)s")

        if self.group:
            c.append("group\t\t= %(group)s")

        if self.log_on_success:
            c.append("log_on_success\t= %(log_on_success)s")

        if self.log_on_failure:
            c.append("log_on_failure\t= %(log_on_failure)s")

        if self.otherlines:
            c.extend(["\t%s"%x for x in self.otherlines])

        c.append("}")

        if not self.filename:
            if self.id:
                self.filename = '/'.join((
                    macros.sysconfdir, 'xinetd.d',
                    '-'.join((self.serviceName, self.socket_type))))
            else:
                self.filename = '/'.join((
                    macros.sysconfdir, 'xinetd.d', self.serviceName))

        dest = macros.destdir+self.filename
	util.mkdirChain(os.path.dirname(dest))
        f = file(dest, 'w')
        f.write('\n'.join(c) %self.__dict__ %macros)
        f.write('\n')
        self.chmod(macros.destdir, dest)


    def __init__(self, recipe, *args, **keywords):
        _FileAction.__init__(self, recipe, **keywords)
        assert(len(args) == 2)
        self.serviceName = args[0]
        self.description = args[1]

        if not self.type and not self.server:
	    self.init_error(TypeError, 'at least one of type or server must be specified')

        if self.server_args and not self.server:
	    self.init_error(TypeError, 'server_args was specified, but server was not')

        if not self.socket_type:
            if self.protocol:
                if self.protocol in ('tcp'):
                    self.socket_type = 'stream'
                elif self.protocol in ('udp'):
                    self.socket_type = 'dgram'
                else:
                    self.init_error(TypeError, 'unknown socket_type for protocol %s'%self.protocol)
            else:

                    self.init_error(TypeError, 'socket_type or protocol must be specified')

        if self.id and not self.socket_type:
	    self.init_error(TypeError, 'id requires socket_type to be specified')

        # chkconfig has somewhat odd formatting requirements
        w = textwrap.TextWrapper(
            initial_indent    = "# description: ",
            subsequent_indent = "#              ",
            break_long_words  = False)
        self.description_text = ' \\\n'.join(w.wrap(self.description))



class XMLCatalogEntry(BuildCommand):
    """
    NAME
    ====

    B{C{r.XMLCatalogEntry()}} - Adds an entry to the XML catalog file catalog
    file

    SYNOPSIS
    ========

    C{r.XMLCatalogEntry([I{catalogFile},] [I{type},] [I{orig},] [I{replace}])}

    DESCRIPTION
    ===========

    The C{r.XMLCatalogEntry()} class is called from within a Conary recipe to
    add an entry to the XML catalog file C{catalogFile}.

    If the catalog default directory (C{/etc/xml}) is nonexistent,
    C{r.XMLCatalogEntry} will create it. If the catalog itself is nonexistent,
    it will be created as well.

    KEYWORDS
    ========

    The C{r.XMLCatalogEntry()} class accepts the following keywords, with
    default values shown in parentheses when applicable:

    B{catalogDir} : (C{'%(sysconfdir)s/xml'}) The directory where the catalog
    file is located.

    EXAMPLES
    ========

    C{r.XMLCatalogEntry('docbook-xsl.xml', 'rewriteSystem',
    'http://docbook.sourceforge.net/release/xsl/1.65.1',
    'file://%(datadir)s/sgml/docbook/xsl-stylesheets-1.65.1')}

    Calls C{r.XMLCatalogEntry()} on the catalog file C{docbook-xsl.xml},
    specifying a type of C{rewriteSystem}, and an original entry of
    C{http://docbook.sourceforge.net/release/xsl/1.65.1} to be replaced by
    C{file://%(datadir)s/sgml/docbook/xsl-stylesheets-1.65.1}.
    """

    template = (
        '%%(createcmd)s'
        ' xmlcatalog --noout --add '
        ' "%(type)s"'
        ' "%(orig)s"'
        ' "%(replace)s"'
        ' "%%(destdir)s/%(catalogDir)s/%(catalogFile)s"'
    )

    keywords = {
        'catalogDir' : '%(sysconfdir)s/xml'
    }

    def __init__(self, recipe, *args, **keywords):
        assert(len(args)==4)
        self.catalogFile = args[0]
        self.type        = args[1]
        self.orig        = args[2]
        self.replace     = args[3]

        BuildCommand.__init__(self, recipe, *args, **keywords)

    def do(self, macros):
        if 'libxml2:runtime' not in self.recipe.buildRequires:
            self.recipe.reportErrors(
                "Must add 'libxml2:runtime' to buildRequires")
        macros = macros.copy()

        catalogDirectory = "%%(destdir)s/%s" % self.catalogDir
        catalogDirectory = catalogDirectory % macros
        if not os.path.exists(catalogDirectory):
            os.makedirs(catalogDirectory)

        catalogFile = "%%(destdir)s/%s/%s" % (self.catalogDir, self.catalogFile)
        catalogFile = catalogFile % macros
        if not os.path.isfile(catalogFile):
            macros.createcmd = 'xmlcatalog --noout --create %s &&' % catalogFile
        else:
            macros.createcmd = ''

        util.execute(self.command % macros)


class SGMLCatalogEntry(BuildCommand):
    """
    NAME
    ====

    B{C{r.SGMLCatalogEntry()}} - Adds an entry to the SGML catalog file

    SYNOPSIS
    ========

    C{r.SGMLCatalogEntry(I{catalogFile} || I{catalogFile}, I{catalogReference})}

    DESCRIPTION
    ===========

    The C{r.SGMLCatalogEntry()} class is called from within a Conary recipe to
    add an entry to the SGML catalog file.

    If the catalog directory (by default, C{/etc/sgml}) is nonexistent, it is
    created.  If the catalog file does not exist,it is created.

    KEYWORDS
    ========

    The C{r.SGMLCatalogEntry()} class accepts the following keywords, with
    default values shown in parentheses when applicable:

    B{catalogDir} : (C{'%(sysconfdir)s/sgml'}) The directory where the catalog
    file is located.

    B{use} : Optional arguments of Use flag(s) telling whether to actually
    perform the action.

    EXAMPLES
    ========

    C{r.SGMLCatalogEntry('sgml-common.cat', '%(datadir)s/xml/qaml/catalog')}

    Calls C{r.SGMLCatalogEntry()} adding the entry C{sgml-common.cat} to the
    catalog reference C{%(datatdir)s/xml/qaml/catalog}.
    """

    template = (
        'xmlcatalog --sgml --add'
        ' %%(tempFileName)s'
        ' %(catalogReference)s > '
        ' %%(destdir)s/%(catalogDir)s/%(catalogFile)s'
    )

    keywords = {
        'catalogDir' : '%(sysconfdir)s/sgml'
    }


    def __init__(self, recipe, *args, **keywords):
        assert(len(args)==2)
        self.catalogFile = args[0]
        self.catalogReference = args[1]

        BuildCommand.__init__(self, recipe, *args, **keywords)


    def do(self, macros):
        if 'libxml2:runtime' not in self.recipe.buildRequires:
            self.recipe.reportErrors(
                "Must add 'libxml2:runtime' to buildRequires")

        macros = macros.copy()

        catalogDirectory = "%%(destdir)s/%s" % self.catalogDir
        catalogDirectory = catalogDirectory % macros
        if not os.path.exists(catalogDirectory):
            os.makedirs(catalogDirectory)

        cleanTemp = False
        catalogName = '%%(destdir)s/%s/%s' % (self.catalogDir,
                                              self.catalogFile)
        catalogName = catalogName % macros
        if os.path.exists(catalogName) and util.isregular(catalogName):
            fd, tempPath = tempfile.mkstemp(suffix='cat',
                                prefix=os.path.basename(catalogName),
                                dir=os.path.dirname(catalogName))
            os.close(fd)
            util.copyfile(catalogName, tempPath)
            macros.tempFileName = tempPath
            cleanTemp = True
        else:
            macros.tempFileName = catalogName

        util.execute(self.command % macros)

        if cleanTemp:
            os.remove(tempPath)
