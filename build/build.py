#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import os
import shutil
import util
import string

# Note: when creating templates, be aware that they are evaulated
# twice, in the context of two different dictionaries.
#  o  keys from keywords should have a # single %, as should "args".
#  o  keys passed in through the macros argument will need %% to
#     escape them for delayed evaluation.  This will include at
#     least %%(builddir)s for all, and doInstall will also get
#     %%(destdir)s

# make sure that the decimal value really is unreasonable before
# adding a new translation to this file.
permmap = {
    1755: 01755,
    4755: 04755,
    755: 0755,
    750: 0750,
    644: 0644,
    640: 0640,
}

class ShellCommand:
    def __init__(self, *args, **keywords):
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
        util.execute(self.command %macros)

    def doBuild(self, macros):
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
    template = ('cd %%(builddir)s; '
                '%%(mkObjdir)s '
		'%(preConfigure)s %%(configure)s '
		'  --prefix=%%(prefix)s '
		'  --bindir=%%(bindir)s '
                '  --sysconfdir=%%(sysconfdir)s '
		'  --datadir=%%(datadir)s '
		'  --mandir=%%(mandir)s --infodir=%%(infodir)s '
		'  %(args)s')
    keywords = {'preConfigure': '',
                'objDir': ''}
    
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

class GNUConfigure(Configure):
    """For use at least when there is no single functional DESTDIR or similar"""
    template = (
	'cd %%(builddir)s; '
	'CFLAGS=%%(cflags)s CXXFLAGS=%%(cflags)s'
	' %%(mkObjdir)s'
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

class _PutFile:
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
	    if permmap.has_key(mode):
		print 'odd permission %o, correcting to %o: add initial "0"?' \
		      %(mode, permmap[mode])
		mode = permmap[mode]
	self.mode = mode
    

class InstallFile(_PutFile):
    def __init__(self, fromFiles, toFile, perms = 0644):
	_PutFile.__init__(self, fromFiles, toFile, perms)
	self.source = ''
	self.move = 0

class MoveFile(_PutFile):
    def __init__(self, fromFiles, toFile, perms = -1):
	_PutFile.__init__(self, fromFiles, toFile, perms)
	self.source = '%(destdir)s'
	self.move = 1

class InstallSymlink:

    def doInstall(self, macros):
	dest = macros['destdir'] + self.toFile %macros
	util.mkdirChain(os.path.dirname(dest))
	if os.path.exists(dest) or os.path.islink(dest):
	    os.remove(dest)
	os.symlink(self.fromFile %macros, dest)

    def __init__(self, fromFile, toFile):
	self.fromFile = fromFile
	self.toFile = toFile

class RemoveFiles:

    def doInstall(self, macros):
	if self.recursive:
	    util.rmtree("%s/%s" %(macros['destdir'], self.filespec %macros))
	else:
	    util.remove("%s/%s" %(macros['destdir'], self.filespec %macros))

    def __init__(self, filespec, recursive=0):
	self.filespec = filespec
	self.recursive = recursive

