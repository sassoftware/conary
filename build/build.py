#
# Copyright (c) 2004 Specifix, Inc.
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
"""
Module used by recipes to direct the build and installation of
software packages.  Classes from this module are not used directly;
instead, they are used through eponymous interfaces in recipe.

The class descriptions contain usage examples for quick reference.

@var _permmap: A mapping of common integer file modes to their octal
equivalents.  This is used to check for common mistakes when specifying
the permissions on files in classes derived from _PutFile.
"""

import os
from lib import util
import fixedglob
from lib import log
import re
import stat
import sys
import action
from use import Use

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
	    from lib import epdb
	    epdb.set_trace()
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
    Run a shell command with simple macro substitution: C{r.Run('echo foo')}
    """
    template = "%(args)s"


class Automake(BuildCommand):
    """
    Re-runs aclocal, autoconf, and automake: C{r.Automake()}
    """
    # note: no use of %(args)s -- to which command would it apply?
    template = ('cd %%(builddir)s/%(subDir)s; '
                'aclocal %%(m4DirArgs)s %(aclocalArgs)s; '
		'%(preAutoconf)s autoconf %(autoConfArgs)s; '
		'automake%(automakeVer)s %(autoMakeArgs)s')
    keywords = {'autoConfArgs': '',
                'autoMakeArgs': '',
		'aclocalArgs': '',
		'preAutoconf': '',
                'm4Dir': '',
		'automakeVer': '',
                'subDir': ''}
    
    def do(self, macros):
	macros = macros.copy()
        if self.m4Dir:
	    macros.m4DirArgs = '-I %s' %(self.m4Dir)
	else:
	    macros.m4DirArgs = ''
        util.execute(self.command %macros)


class Configure(BuildCommand):
    """Runs an autoconf configure script, giving it the default paths as
    defined by the macro set: C{r.Configure(I{extra args})}
    
    It provides many common arguments, set correctly to
    values provided by system macros.  If any of these arguments do
    not work for a program, then use the C{ManualConfigure} class instead.
    """
    # note that template is NOT a tuple, () is used merely to group strings
    # to avoid trailing \ characters on every line
    template = (
	'cd %%(builddir)s/%%(subDir)s; '
	'%%(mkObjdir)s '
	'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
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
                'bootstrapFlags': '--target=%(target)s --host=%(target)s --build=%(build)s',
		'subDir': ''}

    def __init__(self, recipe, *args, **keywords):
        """
        Create a new Configure instance used to run the autoconf configure
        command with default parameters

        @keyword objDir: make an object directory before running configure.
        This is useful for applications which do not support running configure
        from the same directory as the sources (srcdir != objdir).
	It can contain macro references.
	@keyword subDir: relative subdirectory in which to run configure
        @keyword preConfigure: Extra shell script which is inserted in front of
        the configure command.
	@keyword configureName: the name of the configure command; normally
	C{configure} but occasionally C{Configure} or something else.
        """
        BuildCommand.__init__(self, recipe, *args, **keywords)
         
    def do(self, macros):
	macros = macros.copy()
        if self.objDir:
	    objDir = self.objDir %macros
            macros.mkObjdir = 'mkdir -p %s; cd %s;' %(objDir, objDir)
	    macros.configure = '../%s' % self.configureName
        else:
            macros.mkObjdir = ''
            macros.configure = './%s' % self.configureName
        # __getitem__ method avoids adding bootstrap flag to tracked flags
        # (if the flag is really significant, it will be checked elsewhere)
        if Use['bootstrap']:
            macros.bootstrapFlags = self.bootstrapFlags
        else:
            macros.bootstrapFlags = ''
	if self.subDir:
	    macros.subDir = self.subDir
	else:
	    macros.subDir = ''
        util.execute(self.command %macros)

class ManualConfigure(Configure):
    """
    Works exactly like the C{Configure} class,
    except that all the arguments to the configure script have to be
    provided explicitly: C{r.ManualConfigure(I{'--limited-args'})}

    No arguments are given beyond those explicitly provided.
    """
    template = ('cd %%(builddir)s/%%(subDir)s; '
                '%%(mkObjdir)s '
	        '%(preConfigure)s %%(configure)s %(args)s')

class Make(BuildCommand):
    """
    Runs the make utility with system defaults: C{r.Make(I{makeargs})}

    It sets C{CFLAGS}, C{LDFLAGS}, C{CXXFLAGS}, etc. as environment
    variables to the system defaults, and also uses the system default
    for C{mflags} and C{parallelmflags}.

    If the package Makefile explicitly sets the *C{FLAGS} variables,
    then if you want to change them you will have to override them,
    either explicitly in the recipe with C{r.Make('CFLAGS="%(cflags)s"')},
    etc., or forcing them all to the system defaults by passing in the
    C{forceFlags=True} argument.

    If your package does not build correctly with parallelized make,
    you should disable parallel make by using C{r.disableParallelMake()}
    in your recipe.  If your package can do parallel builds but needs some
    other mechanism, then you can modify C{parallelmflags} as necessary in
    your recipe.  You can use C{r.MakeParallelSubdir()} if the top-level
    make is unable to handle parallelization but all subdirectories are.
    """
    # Passing environment variables to Make makes them defined if
    # there is no makefile definition; if they are defined in the
    # makefile, then it takes a command-line argument to override
    # them.
    template = ('cd %%(builddir)s/%(subDir)s; '
	        'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
		' CPPFLAGS="%%(cppflags)s"'
		' LDFLAGS="%%(ldflags)s" CC=%%(cc)s CXX=%%(cxx)s'
                ' %(preMake)s make %%(overrides)s'
		' %%(mflags)s %%(parallelmflags)s %(args)s')
    keywords = {'preMake': '',
                'subDir': '',
		'forceFlags': False}

    def __init__(self, recipe, *args, **keywords):
        """
        @keyword preMake: string to be inserted before the "make" command.
        Use preMake if you need to set an environment variable.  The
        preMake keyword cannot contain a ;
        @keyword subDir: the subdirectory to enter before running "make"
	@keyword forceFlags: boolean; if set, unconditionally override
	the Makefile definitions of *FLAGS (i.e. CFLAGS, CXXFLAGS, LDFLAGS)
        """
	BuildCommand.__init__(self, recipe, *args, **keywords)
        if 'preMake' in keywords:
            if ';' in keywords['preMake']:
                error(TypeError, 'preMake argument cannot contain ;')

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
	BuildCommand.do(self, macros)

class MakeParallelSubdir(Make):
    """
    Runs the make utility like C{Make}, but with system default for
    parallelmflags only applied to sub-make processes:
    C{r.MakeParallelSubdir(I{makeargs})}
    """
    template = ('cd %%(builddir)s/%(subDir)s; '
	        'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
		' CPPFLAGS="%%(cppflags)s"'
		' LDFLAGS="%%(ldflags)s" CC=%%(cc)s CXX=%%(cxx)s'
                ' %(preMake)s make %%(overrides)s'
		' %%(mflags)s '
                ' MAKE="make %%(mflags)s %%(parallelmflags)s" %(args)s')

class MakeInstall(Make):
    """
    Like the Make class, except that it automatically sets C{DESTDIR}
    and provides the C{install} target: C{r.MakeInstall(I{makeargs})}

    If your package does not have C{DESTDIR} or an analog, use 
    C{MakePathsInstall} instead, or as a last option, C{Make}.
    """
    template = ('cd %%(builddir)s/%(subDir)s; '
	        'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
		' CPPFLAGS="%%(cppflags)s"'
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
    Used when there is no single functional C{DESTDIR} or similar definition,
    but enough of the de-facto standard variables (C{prefix}, C{bindir}, etc)
    are honored by the Makefile to make a destdir installation successful:
    C{r.MakePathsInstall(I{makeargs})}
    """
    template = (
	'cd %%(builddir)s/%(subDir)s; '
	'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s %%(cxxflags)s"'
	' CPPFLAGS="%%(cppflags)s"'
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


class CompilePython(BuildCommand):
    """
    Builds compiled and optimized compiled python files:
    C{r.CompilePython(I{/dir1}, I{/dir2})}
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
	    for directory in util.braceGlob(destdir+arg %macros):
		macros['dir'] = directory[destlen:]
		util.execute(self.command %macros)


class Ldconfig(BuildCommand):
    """
    Runs C{ldconfig}: C{r.Ldconfig(I{/dir1})}

    Used mainly when a package does not set up all the appropriate
    symlinks for a library.  Conary packages should include all the
    appropriate symlinks in the packages.  This is not a replacement
    for marking a file as a shared library; C{ldconfig} still needs
    to be run after libraries are installed.
    """
    template = '%%(essentialsbindir)s/ldconfig -n %%(destdir)s/%(args)s'


class _FileAction(BuildAction):
    keywords = {'component': None}

    def chmod(self, destdir, path, mode=None):
	if not mode:
	    mode=self.mode
	if mode >= 0:
            # fixup obviously broken permissions
	    if _permmap.has_key(mode):
                log.warning('odd permission %o, correcting to %o: add initial "0"?' \
                            %(mode, _permmap[mode]))
		mode = _permmap[mode]
	    isdir = os.path.isdir(destdir+os.sep+path)
	    if isdir and (mode & 0700) != 0700:
		# regardless of what permissions go into the package,
		# we need to be able to traverse this directory as
		# the non-root build user
		os.chmod(destdir+os.sep+path, (mode & 01777) | 0700)
		self.recipe.AddModes(mode, util.literalRegex(path))
	    else:
		os.chmod(destdir+os.sep+path, mode & 01777)
		if mode & 06000:
		    self.recipe.AddModes(mode, util.literalRegex(path))
	    if isdir and mode != 0755:
		self.recipe.ExcludeDirectories(exceptions=util.literalRegex(path).replace('%', '%%'))
	    # set explicitly, do not warn
	    self.recipe.WarnWriteable(exceptions=util.literalRegex(path).replace('%', '%%'))

    def setComponents(self, paths):
	"""
	XXX fixme
	A component without a : or with a leading : is relative to the main package name.
	A component with a trailing : is a package name
	"""
	if type(paths) is str:
	    paths = (paths,)
	for path in paths:
	    package = None
	    component = None
	    if self.component:
		if self.component.find(':') != -1:
		    (package, component) = self.component.split(':')
		else:
		    component = self.component
	    path = re.escape(util.normpath(path))
	    if component:
		self.recipe.ComponentSpec(component, path)
	    if package:
		self.recipe.PackageSpec(package, path)


class Desktopfile(BuildCommand, _FileAction):
    """
    Properly installs desktop files in C{/usr/share/applications/},
    including setting category (and vendor, if necessary), if the
    target has enabled building desktop files: C{r.Desktopfile(I{filename})}

    It also enforces proper build requirements for desktop files.
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
	    log.error("Must add 'desktop-file-utils:runtime' to buildRequires")
	macros = self.recipe.macros.copy()
        if self.category:
	    macros['category'] = '--add-category "%s"' %self.category
        else:
            macros['category'] = ''
	BuildCommand.do(self, macros)
	for file in self.arglist:
	    self.setComponents('%(datadir)s/applications'+file)


class Environment(BuildAction):
    """
    Set an environment variable after all macros have been set;
    for each environment variable you need to set, call:
    C{r.Environment('I{VARIABLE}', 'I{value}')}
    """
    def __init__(self, recipe, *args, **keywords):
	assert(len(args)==2)
	self.variable = args[0]
	self.value = args[1]
	action.RecipeAction.__init__(self, recipe, [], **keywords)
    def do(self, macros):
	os.environ[self.variable] = self.value % macros


class SetModes(_FileAction):
    """
    Sets modes on files in the %(destdir)s:
    C{r.SetModes(I{file}[, I{file} ...], I{mode})}

    In order for a file to be setuid in the repository, it needs to
    have its mode explicitly provided in the recipe.  If any file
    installation class that provides a mode is used, that will be
    sufficient, but for files that are installed by makefiles, a
    specific, intentional listing of their mode must be provided.
    The SetModes class provides the mechanism for that.

    In addition, of course, it can be used to change arbitrary
    file modes in the destdir.
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
	dest = macros['destdir']
	for path in self.paths:
	    files.extend(util.braceGlob(dest+os.sep+path %macros))
	for f in files:
	    f = util.normpath(f[len(dest):])
	    log.debug('changing mode for %s to %o' %(f, self.mode))
	    self.chmod(dest, f)
	    self.setComponents(f)

class _PutFiles(_FileAction):
    keywords = { 'mode': -1 }

    def do(self, macros):
	reldest = self.toFile % macros
	if reldest[0] != '/':
	    raise TypeError, 'Inappropriately relative destination %s: destination must start with "/"' % reldest
	dest = macros['destdir'] + reldest
	self.destlen = len(macros['destdir'])
	util.mkdirChain(os.path.dirname(dest))

	for fromFile in self.fromFiles:
	    sources = (self.source + fromFile) %macros
	    sourcelist = util.braceGlob(sources)
	    if not os.path.isdir(dest) and len(sourcelist) > 1:
		raise TypeError, 'multiple files specified, but destination "%s" is not a directory' %dest
	    for source in sourcelist:
		self._do_one(source, dest, macros)

    def _do_one(self, source, dest, macros):
	if os.path.isdir(source) and not self.move:
	    # deep copy of target dir
	    # foo/bar/a -> /blah should give /blah/a rather than /blah/foo/bar/a
	    dest = util.joinPaths(dest, os.path.basename(source))
	    util.mkdirChain(dest)
	    for sourcefile in os.listdir(source):
		thissrc = util.joinPaths(source, sourcefile)
		self._do_one(thissrc, dest, macros)
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
	    util.rename(source, dest)
	else:
	    util.copyfile(source, dest)
	self.setComponents(dest[self.destlen:])
	self.chmod(macros['destdir'], dest[self.destlen:], mode=mode)
	

    def __init__(self, recipe, *args, **keywords):
        _FileAction.__init__(self, recipe, *args, **keywords)
	split = len(args) - 1
	self.fromFiles = args[:split]
	self.toFile = args[split]
	# raise error while we can still tell what is wrong...
	if len(self.fromFiles) > 1:
	    if not self.toFile.endswith('/') or os.path.isdir(self.toFile):
		raise TypeError, 'too many targets for non-directory %s' %self.toFile

class Install(_PutFiles):
    """
    Installs files from C{%(builddir)s} to C{%(destdir)s}:
    C{r.Install(I{srcfile}, I{destfile})}

    Note that a trailing C{/} on destfile means to create the directory
    if necessary.  Source files with no execute permission will default
    to mode 0644; Source files with any execute permission will default
    to mode 0755.  If that rule doesn't suffice, use C{mode=0I{octalmode}}
    to set the mode explicitly.
    """
    keywords = { 'mode': -2 }

    def __init__(self, recipe, *args, **keywords):
	_PutFiles.__init__(self, recipe, *args, **keywords)
	self.source = ''
	self.move = 0

class Copy(_PutFiles):
    """
    Copies files within C{%(destdir)s}:
    C{r.Copy(I{srcfile}, I{destfile})}

    Note that a trailing C{/} on destfile means to create the directory
    if necessary.  The mode of C{I{srcfile}} used for C{I{destfile}}
    unless you set C{mode=0I{octalmode}}.
    """
    def __init__(self, recipe, *args, **keywords):
	_PutFiles.__init__(self, recipe, *args, **keywords)
	self.source = '%(destdir)s'
	self.move = 0

class Move(_PutFiles):
    """
    Moves files within C{%(destdir)s}:
    C{r.Move(I{srcname}, I{destname})}

    Note that a trailing C{/} on destfile means to create the directory
    if necessary.  The mode is preserved, unless you explicitly set the
    new mode with C{mode=0I{octalmode}}.
    """
    def __init__(self, recipe, *args, **keywords):
	_PutFiles.__init__(self, recipe, *args, **keywords)
	self.source = '%(destdir)s'
	self.move = 1

class Symlink(_FileAction):
    """
    Create symbolic links: C{c.Symlink(I{contents}, I{destfile})}
    
    Multiple symlinks can be created if the destination path is a directory.
    The destination path is determined to be a directory if it already
    exists or if the path ends with a C{/} character.
    """
    keywords = { 'allowDangling': False }

    def do(self, macros):
	dest = macros['destdir'] + self.toFile %macros

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
        for source in sources:
            # if the symlink contains a /, concatenate in order to glob
            if source.startswith(os.sep):
                expand = macros['destdir'] + source
            else:
                expand = util.joinPaths(destdir, source)
            sources = fixedglob.glob(expand)
            if not sources and not self.allowDangling:
                raise TypeError, 'symlink to "%s" would be dangling' %source
            for expanded in sources:
                if os.sep in source:
                    expandedSources.append(
			util.joinPaths(os.path.dirname(source),
				       os.path.basename(expanded)))
                else:
                    expandedSources.append(os.path.basename(expanded))
        sources = expandedSources
        
        if len(sources) > 1 and not targetIsDir:
            raise TypeError, 'creating multiple symlinks, but destination is not a directory'

        for source in sources:
            if targetIsDir:
                to = util.joinPaths(dest, os.path.basename(source))
		self.setComponents(
		    util.joinPaths(self.toFile %macros,
			           os.path.basename(source)))
            else:
                to = dest
		self.setComponents(self.toFile %macros)
	    if os.path.exists(to) or os.path.islink(to):
		os.remove(to)
            log.debug('creating symlink %s -> %s' %(to, source))
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
        @keyword allowDangling: Optional argument; set to True to allow the
        creation of dangling symlinks
        @type allowDangling: bool
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
    Install a hard link (discouraged): C{r.Link(I{newname(s)}, I{existingname})}
    
    Much more limited than a symlink, hard links are only permitted
    within the same directory, you cannot create a hard link into another
    directory.  Use symlinks in preference to hard links unless it is
    ABSOLUTELY necessary to use a hard link!
    """
    def do(self, macros):
	d = macros['destdir']
	e = util.joinPaths(d, self.existingpath)
	if not os.path.exists(e):
	    raise TypeError, 'hardlink target %s does not exist' %self.existingpath
	for name in self.newnames:
	    newpath = util.joinPaths(self.basedir, name)
	    n = util.joinPaths(d, newpath)
	    self.setComponents(newpath)
	    if os.path.exists(n) or os.path.islink(n):
		os.remove(n)
	    os.link(e, n)

    def __init__(self, recipe, *args, **keywords):
        """
        Create a new Link instance::
	    self.Link(newname, [newname, ...,] existingpath)
        """
        _FileAction.__init__(self, recipe, *args, **keywords)
	split = len(args) - 1
	self.newnames = args[:split]
	self.existingpath = args[split]
	# raise error while we can still tell what is wrong...
	for name in self.newnames:
	    if name.find('/') != -1:
		self.init_error(TypeError, 'hardlink %s crosses directories' %name)
	self.basedir = os.path.dirname(self.existingpath)

class Remove(BuildAction):
    """
    Removes files from within the C{%(destdir)s}: C{r.Remove(I{filename(s)})}
    """
    keywords = { 'recursive': False }

    def do(self, macros):
	for filespec in self.filespecs:
	    if self.recursive:
		util.rmtree("%s/%s" %(macros['destdir'], filespec %macros),
                            ignore_errors=True)
	    else:
		util.remove("%s/%s" %(macros['destdir'], filespec %macros))

    def __init__(self, recipe, *args, **keywords):
        BuildAction.__init__(self, recipe, **keywords)
	if type(args[0]) is tuple:
	    self.filespecs = args[0]
	else:
	    self.filespecs = args

class Doc(_FileAction):
    """
    Installs documentation files from the C{%(builddir)s}
    into C{%(destdir)s/%(thisdocdir)s}: C{r.Doc(I{file(s)})}

    The C{subdir=I{path}} keyword argument creates a subdirectory
    under C{%(thisdocdir)s} to put the files in.
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
	for path in self.paths:
	    for newpath in util.copytree(path %macros, dest, True,
					 filemode=self.mode,
					 dirmode=self.dirmode):
		self.setComponents(newpath[destlen:])

    def __init__(self, recipe, *args, **keywords):
        _FileAction.__init__(self, recipe, *args, **keywords)
	if type(args[0]) is tuple:
	    self.paths = args[0]
	else:
	    self.paths = args

class Create(_FileAction):
    """
    Creates a file in C{%(destdir)s}: C{r.Create(I{emptyfile})}
    
    Without C{contents} specified it is rather like C{touch foo};
    with C{contents} specified it is more like C{cat > foo <<EOF ... EOF}.
    If C{contents} is not empty, then a newline will be implicitly
    appended unless C{contents} already ends in a newline.
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
	for bracepath in self.paths:
	    for path in util.braceExpand(bracepath %macros):
		fullpath = util.joinPaths(macros['destdir'], path)
		util.mkdirChain(os.path.dirname(fullpath))
		f = file(fullpath, 'w')
		f.write(contents)
		f.close()
		self.setComponents(path)
		self.chmod(macros['destdir'], path)
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
    Creates directories in C{%(destdir)s}: C{r.MakeDirs(I{dir(s)})}

    Set C{component} only if the package should be responsible for
    the directory.
    """
    keywords = { 'mode': 0755 }

    def do(self, macros):
        for path in self.paths:
            path = path %macros
            dirs = util.braceExpand(path)
            for d in dirs:
		if d[0] != '/':
		    raise TypeError, 'Inappropriately relative directory %s: directories must start with "/"' %d
                dest = macros['destdir'] + d
                log.debug('creating directory %s', d)
		self.setComponents(d.replace('%', '%%'))
                util.mkdirChain(dest)
                self.chmod(macros['destdir'], d)

    def __init__(self, recipe, *args, **keywords):
        _FileAction.__init__(self, recipe, *args, **keywords)
	if type(args[0]) is tuple:
	    self.paths = args[0]
	else:
	    self.paths = args

class TestSuite(_FileAction):
    """
    The TestSuite class creates a script to run this package's test suite: 
    C{TestSuite(I{<dir>}, I{<command>})}.  TestSuite also modifies Makefiles 
    in order to compile binaries needed for testing at cook time, while 
    allowing the actual testsuite to run at a later point.  It does this
    if the command to be run is of the form C{make I{<target>}}, in which
    case all of the target's dependencies are built, and the makefile is 
    edited to then remove those dependencies from the target.  Also, if 
    the command is a make command, the arguments C{-o Makefile -o config.status}
    are added to help ensure that automake does not try to regenerate
    the Makefile at test time. 
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
	path = self.command_path % self.macros
	fullpath = self.macros.destdir + path
	if not os.path.exists(fullpath):
	    util.mkdirChain(os.path.dirname(fullpath))
	    f = open(fullpath, 'w')
	    f.write(self.commandScript)
	    self.chmod(self.macros.destdir, path)
	    self.setComponents(path)

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
	    self.chmod(self.macros.destdir, path)
    
    def mungeMakeCommand(self):
        """ Munge the make command to ignore files
            that do not need to be rebuilt """
	if self.ignore is not None:
	    ignoreFiles = [ 'Makefile', 'config.status' ]
	    ignoreFiles.extend(self.ignore)
	    ignoreOpts = ' -o ' + ' -o '.join(ignoreFiles)
	    makeCmd = 'export MAKE="make' + ignoreOpts + '"\n'
	    self.command = makeCmd + '$MAKE' + self.command[4:]
	    self.macros.command = self.command

    def buildMakeDependencies(self, dir, command):
        """ build and remove make dependencies so that 
            when the test suite is run, the makefile does 
            spuriously require the capability to build any 
            already built test executables """
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
	util.execute(r"sed -i -e 's/^%s\s*:\s*\(.*\)/conary-pre-%s: \1\n\n%s:/'  %s" % (makeTarget, makeTarget, makeTarget, makefile))
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
