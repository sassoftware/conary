#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
"""
Module used by recipes to direct the build and installation of
software packages.

@var _permmap: A mapping of common integer file modes to their octal
equivalents.  This is used to check for common mistakes when specifying
the permissions on files in classes derived from _PutFile.
"""

import os
import util
import string

# make sure that the decimal value really is unreasonable before
# adding a new translation to this file.
_permmap = {
    1755: 01755,
    4755: 04755,
    755: 0755,
    750: 0750,
    644: 0644,
    640: 0640,
}

class ShellCommand:
    """Base class for shell-based commands. ShellCommand is an abstract class
    and can not be made into a working instance. Only derived classes which
    define the C{template} static class variable will work properly.

    Note: when creating templates, be aware that they are evaulated
    twice, in the context of two different dictionaries.
     - keys from keywords should have a # single %, as should "args".
     - keys passed in through the macros argument will need %% to
       escape them for delayed evaluation.  This will include at
       least %%(builddir)s for all, and doInstall will also get
       %%(destdir)s
    
    @ivar self.command: Shell command to execute. This is built from the
    C{template} static class variable in derived classes.
    @type self.command: str
    @cvar keywords: The keywords and default values accepted by the class at
    initialization time.
    @cvar template: The string template used to build the shell command.
    """
    def __init__(self, *args, **keywords):
        """Create a new ShellCommand instance that can be used to run
        a simple shell statement
        @param args: arguments to __init__ are stored for later substitution
        in the shell command if it contains %(args)s
        @param keywords: keywords are replaced in the shell command
        through dictionary substitution
        @raise TypeError: If a keyword is passed to __init__ which is not
        accepted by the class.
        @rtype: ShellCommand
        """
        assert(self.__class__ is not ShellCommand)
        # initialize initialize our keywords to the defaults
        self.__dict__.update(self.keywords)
        # check to make sure that we don't get a keyword we don't expect
        for key in keywords.keys():
            if key not in self.keywords.keys():
                raise TypeError, ("%s.__init__() got an unexpected keyword argument "
                                  "'%s'" % (self.__class__.__name__, key))
        # copy the keywords into our dict, overwriting the defaults
        self.__dict__.update(keywords)
        self.args = string.join(args)
        # pre-fill in the preMake and arguments
        self.command = self.template % self.__dict__

    def doInstall(self, macros):
        """Method which is used if the ShellCommand instance is invoked 
        during installation
        @param macros: macros which will be expanded through dictionary
        substitution in self.command
        @type macros: recipe.Macros
        @return: None
        @rtype: None"""
        util.execute(self.command %macros)

    def doBuild(self, macros):
        """Method which is used if the ShellCommand instance is invoked 
        during build
        @param macros: macros which will be expanded through dictionary
        substitution in self.command
        @type macros: recipe.Macros
        @return: None
        @rtype: None"""
        util.execute(self.command %macros)



class Automake(ShellCommand):
    # note: no use of %(args)s -- which command would it apply to?
    template = ('cd %%(builddir)s; '
                'aclocal %%(m4DirArgs)s %(acLocalArgs)s; '
		'%(preAutoconf)s autoconf %(autoConfArgs)s; '
		'automake%(automakeVer)s %(autoMakeArgs)s')
    keywords = {'autoConfArgs': '',
                'autoMakeArgs': '',
		'acLocalArgs': '',
		'preAutoconf': '',
                'm4Dir': '',
		'automakeVer': ''}
    
    def doBuild(self, macros):
	macros = macros.copy()
        if self.m4Dir:
	    macros.update({'m4DirArgs': '-I %s' %(self.m4Dir)})
        util.execute(self.command %macros)


class Configure(ShellCommand):
    """The Configure class runs an autoconf configure script with the
    default paths as defined by the macro set passed into it when doBuild
    is invoked.
    """
    template = (
	'cd %%(builddir)s; '
	'%%(mkObjdir)s '
	'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s"'
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
	'  %(args)s')
    keywords = {'preConfigure': '',
                'objDir': ''}

    def __init__(self, *args, **keywords):
        """Create a new Configure instance used to run the autoconf configure
        command with default parameters
        @keyword mkObjdir: make an object directory before running configure.
        This is useful for applications which do not support running configure
        from the same directory as the sources (srcdir != objdir)
        @keyword preConfigure: Extra shell script which is inserted in front of
        the configure command.
        """
        ShellCommand.__init__(self, *args, **keywords)
         
    def doBuild(self, macros):
	macros = macros.copy()
        if self.objDir:
            macros['mkObjdir'] = 'mkdir -p %s; cd %s;' \
	                         %(self.objDir, self.objDir)
	    macros['configure'] = '../configure'
        else:
            macros['configure'] = './configure'
        util.execute(self.command %macros)

class ManualConfigure(Configure):
    template = ('cd %%(builddir)s; '
                '%%(mkObjdir)s '
	        '%(preConfigure)s %%(configure)s %(args)s')

class Make(ShellCommand):
    template = ('cd %%(builddir)s; '
                '%(preMake)s make %%(mflags)s %%(parallelmflags)s %(args)s')
    keywords = {'preMake': ''}

class MakeInstall(ShellCommand):
    template = ('cd %%(builddir)s; '
                '%(preMake)s make %%(mflags)s %%(rootVarArgs)s'
		' %(installtarget)s %(args)s')
    keywords = {'rootVar': 'DESTDIR',
                'preMake': '',
		'installtarget': 'install'}

    def doInstall(self, macros):
	macros = macros.copy()
        if self.rootVar:
	    macros.update({'rootVarArgs': '%s=%s'
	                  %(self.rootVar, macros['destdir'])})
	util.execute(self.command %macros)

class GNUMakeInstall(ShellCommand):
    """For use at least when there is no single functional DESTDIR or similar"""
    template = (
	'cd %%(builddir)s; '
	'%(preMake)s make %%(mflags)s'
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
    keywords = {'preMake': '',
		'installtarget': 'install'}


class _PutFiles:
    def doInstall(self, macros):
	dest = macros['destdir'] + self.toFile %macros
	util.mkdirChain(os.path.dirname(dest))

	for fromFile in self.fromFiles:
	    sources = (self.source + fromFile) %macros
	    sourcelist = util.braceGlob(sources)
	    if dest[-1:] != '/' and len(sourcelist) > 1:
		raise TypeError, 'singleton destination %s requires singleton source'
	    for source in sourcelist:
		thisdest = dest
		if dest[-1:] == '/':
		    thisdest = dest + os.path.basename(source)
		if self.move:
		    util.rename(source, thisdest)
		else:
		    util.copyfile(source, thisdest)
		if self.mode >= 0:
		    os.chmod(thisdest, self.mode)

    def __init__(self, fromFiles, toFile, mode):
	self.toFile = toFile
	if type(fromFiles) is str:
	    self.fromFiles = (fromFiles,)
	else:
	    self.fromFiles = fromFiles
	# notice obviously broken permissions
	if mode >= 0:
	    if _permmap.has_key(mode):
		print 'odd permission %o, correcting to %o: add initial "0"?' \
		      %(mode, _permmap[mode])
		mode = _permmap[mode]
	self.mode = mode
    

class InstallFiles(_PutFiles):
    def __init__(self, fromFiles, toFile, mode = 0644):
	_PutFiles.__init__(self, fromFiles, toFile, mode)
	self.source = ''
	self.move = 0

class MoveFiles(_PutFiles):
    def __init__(self, fromFiles, toFile, mode = -1):
	_PutFiles.__init__(self, fromFiles, toFile, mode)
	self.source = '%(destdir)s'
	self.move = 1

class InstallSymlinks:

    def doInstall(self, macros):
	dest = macros['destdir'] + self.toFile %macros
	util.mkdirChain(os.path.dirname(dest))

	if self.toFile.endswith('/'):
	    # only if toFiles ends in / can fromFile be brace-expanded
	    if type(self.fromFiles) is str:
		self.fromFiles = (self.fromFiles,)
	    sources = []
	    for fromFile in self.fromFiles:
		sources.extend(util.braceExpand(fromFile %macros))
	else:
	    if os.path.exists(dest) or os.path.islink(dest):
		os.remove(dest)

	if type(self.fromFiles) is str:
	    if self.toFile.endswith('/'):
		dest = dest + os.path.basename(self.fromFiles)
	    print '+ creating symlink from %s to %s' %(dest, self.fromFiles)
	    os.symlink(self.fromFiles %macros, dest)
	    return

	for source in sources:
	    print '+ creating symlink from %s to %s' %(dest, source)
	    os.symlink(source, dest+os.path.basename(source))

    def __init__(self, fromFiles, toFile):
	# raise error early
	if not type(fromFiles) is str:
	    if not toFile.endswith('/'):
		raise TypeError, 'too many targets for non-directory %s' %toFile
	self.fromFiles = fromFiles
	self.toFile = toFile

class RemoveFiles:

    def doInstall(self, macros):
	for filespec in self.filespecs:
	    if self.recursive:
		util.rmtree("%s/%s" %(macros['destdir'], filespec %macros))
	    else:
		util.remove("%s/%s" %(macros['destdir'], filespec %macros))

    def __init__(self, filespecs, recursive=0):
	if type(filespecs) is str:
	    self.filespecs = (filespecs,)
	else:
	    self.filespecs = filespecs
	self.recursive = recursive

class InstallDocs:

    def doInstall(self, macros):
	macros = macros.copy()
	if self.subdir:
	    macros['subdir'] = '/%s' % self.subdir
	if self.devel:
	    dest = '%(destdir)s/%(develdocdir)s/%(name)s-%(version)s/%(subdir)s/' %macros
	else:
	    dest = '%(destdir)s/%(docdir)s/%(name)s-%(version)s/%(subdir)s/' %macros
	util.mkdirChain(os.path.dirname(dest))
	for path in self.paths:
	    util.copytree(path, dest, True)

    def __init__(self, paths, devel=False, subdir=''):
	if type(paths) is str:
	    self.paths = (paths,)
	else:
	    self.paths = paths
	self.devel = devel
	self.subdir = subdir
