import os

class Make:

    def doBuild(self, dir):
	os.system("cd %s; make" % dir)

class MakeInstall:

    def doInstall(self, dir, root):
	os.system("cd %s; make %s=%s install" % (dir, self.rootVar, root))

    def __init__(self, rootVar = "DESTDIR"):
	self.rootVar = rootVar

