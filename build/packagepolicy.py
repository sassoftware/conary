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

class EtcConfig(policy.Policy):
    """
    Mark all files below /etc as config files
    """
    invariantsubtree = [ '%(sysconfdir)s' ]

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    for package in self.recipe.packages:
		if package.has_key(file):
		    print 'config:', file
		    package[file].isConfig(True)


class Config(policy.Policy):
    """
    Mark only explicit inclusions as config files
    @param inclusions: regexp(s) specifying files to be included.
    Do not mention files in /etc, which are already covered by the
    EtcConfig class.
    @type inclusions: None, regexp string, sequence of regexp strings.
    """
    keywords = {
	'inclusions': None
    }

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
		    for package in self.recipe.packages:
			if package.has_key(file):
			    print 'config:', file
			    package[file].isConfig(True)


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	EtcConfig(),
	Config(),
    ]
