#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import re
import os
import policy

"""
Module used by recipes to effect packaging policy; things like setting
hints, flags, and dependencies.
Classes from this module are not used directly; instead, they are used
through eponymous interfaces in recipe.
"""

def _markConfig(recipe, filename):
    packages = recipe.autopkg.packages
    for package in packages.keys():
	if packages[package].has_key(filename):
	    print 'config:', filename
	    packages[package][filename].isConfig(True)


class EtcConfig(policy.Policy):
    """
    Mark all files below /etc as config files
    """
    invariantsubtree = [ '%(sysconfdir)s' ]

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    _markConfig(self.recipe, file)


class Config(policy.Policy):
    """
    Mark only explicit inclusions as config files
    """
    keywords = {
	'inclusions': None
    }

    def __init__(self, *args, **keywords):
        """
        @keyword inclusions: regexp(s) specifying files to be included.
        Do not mention files in /etc, which are already covered by the
        EtcConfig class.
        @type inclusions: None, regexp string, sequence of regexp strings.
        """
        policy.Policy.__init__(self, *args, **keywords)
        

    def doProcess(self, recipe):
	self.configREs = []
	if self.inclusions:
	    if not isinstance(self.inclusions, (tuple, list)):
		self.inclusions = (self.inclusions,)
	    for inclusion in self.inclusions:
		self.configREs.append(re.compile(inclusion %self.macros))
	policy.Policy.__init__(self, recipe)

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    for configRE in self.configREs:
		if configRE.search(file):
		    _markConfig(self.recipe, file)


class ParseManifest(policy.Policy):
    """
    """
    keywords = {
	'path': None
    }

    def do(self):
	if not self.path:
	    return
	if not self.path.startswith('/'):
	    self.path = self.macros['builddir'] + os.sep + self.path
        f = open(self.path)
        for line in f:
            line = line.strip()
            fields = line.split(')')

            attr = fields[0].lstrip('%attr(').split(',')
            perms = attr[0].strip()
            owner = attr[1].strip()
            group = attr[2].strip()

            fields[1] = fields[1].strip()
            if fields[1].startswith('%dev('):
                dev = fields[1][5:].split(',')
                devtype = dev[0]
                major = dev[1]
                minor = dev[2]
                target = fields[2].strip()
                self.recipe.addDevice(target, devtype, int(major), int(minor),
                                      owner, group, int(perms, 0))
            elif fields[1].startswith('%dir '):
                target = fields[1][5:]
		# XXX not sure what we should do here...
                dironly = 1
            else:
                target = fields[1].strip()

class MakeDevices(policy.Policy):
    """
    Make device nodes
    """
    def do(self):
        for device in self.recipe.getDevices():
            self.recipe.autopkg.addDevice(*device)

class AddModes(policy.Policy):
    """
    Apply suid/sgid modes
    """
    def doFile(self, path):
	if path in self.recipe.fixmodes:
	    mode = self.recipe.fixmodes[path]
	    packages = self.recipe.autopkg.packages
	    for package in packages.keys():
		if packages[package].has_key(path):
		    print 'suid/sgid:', path
		    packages[package][path].perms(addbits=mode)


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	EtcConfig(),
	Config(),
	ParseManifest(),
	MakeDevices(),
	AddModes(),
    ]
