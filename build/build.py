#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import os
import shutil
import util
import string

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

    def execute(self, command):
        print '+', command
        rc = os.system(command)
        if rc:
            raise RuntimeError, ('Shell command "%s" returned '
                                 'non-zero status %d' % (command, rc))


class Automake(ShellCommand):
    template = ('cd %%s; aclocal %%s ; %(preAutoconf) autoconf %(autoConfArgs); automake %(autoMakeArgs)'
                ' %(args)s')
    keywords = {'autoConfArgs': '--force',
                'autoMakeArgs': '--copy --force',
		'preAutoconf': '',
                'm4Dir': ''}
    
    def doBuild(self, dir):
	m4DirArgs = ''
        if self.m4Dir:
	    m4DirArgs = '-I %s' %(self.m4Dir)
        self.execute(self.command % (dir, m4DirArgs))


class Configure(ShellCommand):
    template = ('cd %%s; %%s %(preConfigure)s %%s --prefix=/usr '
                '--sysconfdir=/etc %(args)s')
    keywords = {'preConfigure': '',
                'objDir': ''}
    
    def doBuild(self, dir):
        if self.objDir:
            configure = '../configure'
            mkObjdir = 'mkdir -p %s; cd %s;' %(self.objDir, self.objDir)
        else:
            configure = './configure'
            mkObjdir = ''
        self.execute(self.command % (dir, mkObjdir, configure))

class ManualConfigure(Configure):
    template = 'cd %%s; %%s %(preConfigure)s %%s %(args)s'

class Make(ShellCommand):
    template = 'cd %%s; %(preMake)s make %(args)s'
    keywords = {'preMake': ''}
    
    def doBuild(self, dir):
        self.execute(self.command % (dir))

class MakeInstall(ShellCommand):
    template = "cd %%s; %(preMake)s make %(rootVar)s=%%s install %(args)s"
    keywords = {'rootVar': 'DESTDIR',
                'preMake': ''}

    def doInstall(self, dir, root):
	self.execute(self.command % (dir, root))

class InstallFile:

    def doInstall(self, dir, root):
	dest = root + self.toFile
	util.mkdirChain(os.path.dirname(dest))

	shutil.copyfile(self.toFile, dest)
	os.chmod(dest, self.mode)

    def __init__(self, fromFile, toFile, perms = 0644):
	self.toFile = toFile
	self.file = fromFile
	self.mode = perms

class InstallSymlink:

    def doInstall(self, dir, root):
	os.link(root + self.fromFile, root + self.toFile)

    def __init__(self, fromFile, toFile):
	self.fromFile = fromFile
	self.toFile = toFile

class RemoveFiles:

    def doInstall(self, dir, root):
	if self.recursive:
	    os.system("rm -rf %s/%s" %(dir, self.filespec))
	else:
	    os.system("rm -f %s/%s" %(dir, self.filespec))

    def __init__(self, filespec, recursive=0):
	self.filespec = filespec
	self.recursive = recursive
