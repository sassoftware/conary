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
        os.system(command)

class Configure(ShellCommand):
    template = ('cd %%s; %(preConfigure)s ./configure --prefix=/usr '
                '--sysconfdir=/etc %(extraFlags)s')
    keywords = {'preConfigure': '',
                'extraFlags': ''}
    
    def doBuild(self, dir):
        self.execute(self.command % dir)

class ManualConfigure(Configure):
    template = 'cd %%s; %(preConfigure)s ./configure %(extraFlags)s'

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
