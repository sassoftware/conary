#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
"""
Module used by recipes to direct the build and installation of
software packages.  Classes from this module are not used directly;
instead, they are used through eponymous interfaces in recipe.

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

class BuildAction(util.Action):
    def __init__(self, *args, **keywords):
	"""
	@param use: Optional argument; Use flag(s) telling whether
	to actually perform the action.
	@type use: None, Use flag, or tuple/list of Use flags
	"""
	# enforce pure virtual status
        assert(self.__class__ is not BuildCommand)
	# dictionary of common keywords
	self.commonkeywords = {
	    'use': None
	}
	util.Action.__init__(self, *args, **keywords)
	# change self.use to be a simple flag
	self.use = util.checkUse(self.use)

    def doBuild(self, macros):
	if self.use:
	    self.do(macros)

class BuildCommand(BuildAction, util.ShellCommand):
    """
    Pure virtual class which implements the default doBuild method
    required of build classes based on the shell command built from
    a template.
    """
    def __init__(self, *args, **keywords):
	# enforce pure virtual status
        assert(self.__class__ is not BuildCommand)
	BuildAction.__init__(self, *args, **keywords)
	util.ShellCommand.__init__(self, *args, **keywords)

    def do(self, macros):
        """
	Method which is used if the ShellCommand instance is invoked 
        during build
        @param macros: macros which will be expanded through dictionary
        substitution in self.command
        @type macros: recipe.Macros
        @return: None
        @rtype: None
	"""
        if self.use: util.execute(self.command %macros)


class Run(BuildCommand):
    """
    Just run a command with simple macro substitution
    """
    template = "%(args)s"


class Automake(BuildCommand):
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
    
    def do(self, macros):
	macros = macros.copy()
        if self.m4Dir:
	    macros.update({'m4DirArgs': '-I %s' %(self.m4Dir)})
        util.execute(self.command %macros)


class Configure(BuildCommand):
    """The Configure class runs an autoconf configure script with the
    default paths as defined by the macro set passed into it when doBuild
    is invoked.
    """
    # note that template is NOT a tuple, () is used merely to group strings
    # to avoid trailing \ characters on every line
    template = (
	'cd %%(builddir)s; '
	'%%(mkObjdir)s '
	'%%(cdSubDir)s '
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
                'objDir': '',
		'subDir': ''}

    def __init__(self, *args, **keywords):
        """Create a new Configure instance used to run the autoconf configure
        command with default parameters
        @keyword objDir: make an object directory before running configure.
        This is useful for applications which do not support running configure
        from the same directory as the sources (srcdir != objdir)
	@keyword subDir: relative subdirectory in which to run configure
        @keyword preConfigure: Extra shell script which is inserted in front of
        the configure command.
        """
        BuildCommand.__init__(self, *args, **keywords)
         
    def do(self, macros):
	macros = macros.copy()
        if self.objDir:
            macros['mkObjdir'] = 'mkdir -p %s; cd %s;' \
	                         %(self.objDir, self.objDir)
	    macros['configure'] = '../configure'
        else:
            macros['configure'] = './configure'
	if self.subDir:
	    macros['cdSubDir'] = 'cd %s;' %self.subDir
        util.execute(self.command %macros)

class ManualConfigure(Configure):
    template = ('cd %%(builddir)s; '
                '%%(mkObjdir)s '
	        '%(preConfigure)s %%(configure)s %(args)s')

class Make(BuildCommand):
    template = ('cd %%(builddir)s; '
	        'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s"'
                ' %(preMake)s make %%(mflags)s %%(parallelmflags)s %(args)s')
    keywords = {'preMake': ''}

class MakeInstall(BuildCommand):
    template = ('cd %%(builddir)s; '
	        'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s"'
                ' %(preMake)s make %%(mflags)s %%(rootVarArgs)s'
		' %(installtarget)s %(args)s')
    keywords = {'rootVar': 'DESTDIR',
                'preMake': '',
		'installtarget': 'install'}

    def do(self, macros):
	macros = macros.copy()
        if self.rootVar:
	    macros.update({'rootVarArgs': '%s=%s'
	                  %(self.rootVar, macros['destdir'])})
	util.execute(self.command %macros)

class GNUMakeInstall(BuildCommand):
    """For use at least when there is no single functional DESTDIR or similar"""
    template = (
	'cd %%(builddir)s; '
	'CFLAGS="%%(cflags)s" CXXFLAGS="%%(cflags)s"'
	' %(preMake)s make %%(mflags)s'
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



class InstallDesktopfile(BuildCommand):
    template = ('desktop-file-install --vendor %(vendor)s'
		' --dir %%(destdir)s/%%(datadir)s/applications'
		' %%(category)s'
		' %(args)s')
    keywords = {'vendor': 'net',
		'categories': None}

    def doBuild(self, macros):
	macros = macros.copy()
        if self.categories:
	    macros['category'] = '--add-category %s' %self.categories
	BuildCommand.doBuild(self, macros)


class _PutFiles(BuildAction):
    def do(self, macros):
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

    def __init__(self, fromFiles, toFile, mode, use):
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
	self.use = util.checkUse(use)
    

class InstallFiles(_PutFiles):
    def __init__(self, fromFiles, toFile, mode = 0644, use=None):
	_PutFiles.__init__(self, fromFiles, toFile, mode, use)
	self.source = ''
	self.move = 0

class MoveFiles(_PutFiles):
    def __init__(self, fromFiles, toFile, mode = -1, use=None):
	_PutFiles.__init__(self, fromFiles, toFile, mode, use)
	self.source = '%(destdir)s'
	self.move = 1

class InstallSymlinks(BuildAction):

    def do(self, macros):
	dest = macros['destdir'] + self.toFile %macros
	util.mkdirChain(os.path.dirname(dest))

	if self.toFile.endswith('/'):
	    # only if toFiles ends in / can fromFile be brace-expanded
	    if type(self.fromFiles) is str:
		self.fromFiles = (self.fromFiles,)
	    sources = []
	    for fromFile in self.fromFiles:
		sources.extend(util.braceExpand(fromFile %macros))

	if type(self.fromFiles) is str:
	    if self.toFile.endswith('/'):
		dest = dest + os.path.basename(self.fromFiles)
            source = self.fromFiles %macros
	    print '+ creating symlink %s -> %s' %(dest, source)
	    if os.path.exists(dest) or os.path.islink(dest):
		os.remove(dest)
	    os.symlink(source, dest)
	    return

	for source in sources:
	    dest = macros['destdir'] + (self.toFile + os.path.basename(source)) %macros 
	    print '+ creating symlink %s -> %s' %(dest, source)
	    if os.path.exists(dest) or os.path.islink(dest):
		os.remove(dest)
	    os.symlink(source, dest)

    def __init__(self, fromFiles, toFile, use=None):
	# raise error early
	if type(fromFiles) is not str:
	    if not toFile.endswith('/'):
		raise TypeError, 'too many targets for non-directory %s' %toFile
	self.fromFiles = fromFiles
	self.toFile = toFile
	self.use = util.checkUse(use)

class RemoveFiles(BuildAction):

    def do(self, macros):
	for filespec in self.filespecs:
	    if self.recursive:
		util.rmtree("%s/%s" %(macros['destdir'], filespec %macros))
	    else:
		util.remove("%s/%s" %(macros['destdir'], filespec %macros))

    def __init__(self, filespecs, recursive=0, use=None):
	if type(filespecs) is str:
	    self.filespecs = (filespecs,)
	else:
	    self.filespecs = filespecs
	self.recursive = recursive
	self.use = util.checkUse(use)

class InstallDocs(BuildAction):

    def do(self, macros):
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

    def __init__(self, paths, devel=False, subdir='', use=None):
	if type(paths) is str:
	    self.paths = (paths,)
	else:
	    self.paths = paths
	self.devel = devel
	self.subdir = subdir
	self.use = util.checkUse(use)
