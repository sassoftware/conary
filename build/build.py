#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import os
import shutil
import util

class ManualConfigure:

    def doBuild(self, dir):
	os.system("cd %s; ./configure %s" % (dir, self.extraflags))

    def __init__(self, extraflags=""):
        self.extraflags = extraflags

class Configure:

    def doBuild(self, dir):
	os.system("cd %s; ./configure --prefix=/usr --sysconfdir=/etc %s" % (dir, self.extraflags))

    def __init__(self, extraflags=""):
        self.extraflags = extraflags

class Make:

    def doBuild(self, dir):
	os.system("cd %s; make" % dir)

class MakeInstall:

    def doInstall(self, dir, root):
	os.system("cd %s; make %s=%s install" % (dir, self.rootVar, root))

    def __init__(self, rootVar = "DESTDIR"):
	self.rootVar = rootVar

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
