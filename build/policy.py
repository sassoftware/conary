#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util

"""
"""


class Policy(util.Action):
    """
    Pure virtual superclass for all policy actions.
    """
    def __init__(self, *args, **keywords):
	"""
	@param exceptions: Optional argument; regexp(s) specifying
	files to ignore while taking the policy action.
	@type exceptions: None, regular expression string, or
	tuple/list of regular expressions
	@param use: Optional argument; Use flag(s) telling whether
	to actually perform the action.
	@type use: None, Use flag, or tuple/list of Use flags
	"""
	# enforce pure virtual status
	assert(self.__class__ is not Policy)
	# dictionary of common keywords
	self.commonkeywords = {
	    'use': None,
	    'exceptions': None
	}
	util.Action.__init__(self, *args, **keywords)
	# change self.use to be a simple flag
	self.use = util.checkUse(self.use)

    def doPolicy(self, macros):
	"""
	Invocation instance
        @param macros: macros which will be expanded through dictionary
        substitution in self.command
        @type macros: recipe.Macros
        @return: None
        @rtype: None
	"""
	# XXX what about exceptions?
	if self.use:
	    self.do(macros)


class SanitizeSonames(Policy):
    pass




def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return []
