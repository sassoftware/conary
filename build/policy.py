#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import re
import os

"""
Module used by recipes to modify the state of the installed %(destdir)s
Classes from this module are not used directly; instead, they are used
through eponymous interfaces in recipe.
"""


class Policy(util.Action):
    """
    Pure virtual superclass for all policy actions.  Policy actions
    that operate on the entire %(destdir)s implement the do member;
    Policy actions that operate on a per-file basis implement the
    doFile member.  The doFile function is never called for files
    that match an exception regexp; do functions must implement
    their own exception regexp handling.
    """
    def __init__(self, *args, **keywords):
	"""
	@param exceptions: Optional argument; regexp(s) specifying
	files to ignore while taking the policy action.  It will be
	interpolated against recipe macros before being used.
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

    def doProcess(self, recipe):
	"""
	Invocation instance
        @param macros: macros which will be expanded through dictionary
        substitution in self.command
        @type macros: recipe.Macros
        @return: None
        @rtype: None
	"""
	self.recipe = recipe
	self.macros = recipe.macros

	# change self.use to be a simple flag
	self.use = util.checkUse(self.use)

	# compile the exceptions
	self.exceptionREs = []
	if self.exceptions:
	    if not isinstance(self.exceptions, (tuple, list)):
		self.exceptions = (self.exceptions,)
	    for exception in self.exceptions:
		self.exceptionREs.append(re.compile(exception %self.macros))

	# dispatch if/as appropriate
	if self.use:
	    if self.__class__.__dict__.has_key('do'):
		self.do()
	    elif self.__class__.__dict__.has_key('doFile'):
		os.path.walk(self.macros['destdir'], _walkFile, self)


# internal helpers

def _policyException(policyObj, filespec):
    for re in policyObj.exceptionREs:
	if re.search(filespec):
	    return True
    return False
    

def _walkFile(policyObj, dirname, names):
    # chop off bit not useful for comparison
    path=dirname[len(policyObj.macros['destdir']):]
    for name in names:
	thispath = path+name
	if not _policyException(policyObj, thispath):
	    policyObj.doFile(thispath)


# the real thing

class SanitizeSonames(Policy):
    """
    make sure that .so -> SONAME -> fullname
    """
    def do(self):
	pass

class RemoveExtraLibs(Policy):
    """
    Kill .la files and any other similar garbage
    """
    def __init__(self, *args, **keywords):
	Policy.__init__(self, *args, **keywords)
	self.re = re.compile('\.la')
    def doFile(self, path):
	if self.re.search(path):
	    util.remove(self.macros['destdir']+path)

class Strip(Policy):
    """
    strip executables without creating debuginfo subpackage
    """
    def do(self):
	pass

class StripToDebug(Policy):
    """
    move debugging information out of binaries into debuginfo subpackage
    """
    def do(self):
	pass

class NormalizeManPages(Policy):
    def do(self):
	pass

class NormalizeInfoPages(Policy):
    """
    compress info files and remove dir file (unless is exception)
    """
    def do(self):
	dir = self.macros['infodir']+'/dir'
	fsdir = self.macros['destdir']+dir
	if os.path.exists(fsdir):
	    if not _policyException(self, dir):
		util.remove(fsdir)
	# XXX finish the job!

class RemoveTimeStamps(Policy):
    """
    Remove time/date stamps from compressed files and archives
    XXX msw will have to implement this, I have no idea how
    """
    def do(self):
	pass

class GenerateDependencies(Policy):
    """
    ignore stupid places like /usr/share/doc
    allow packager to specify other places to ignore
    do only first-level dependencies; stored transitive symbol dependencies go stale
    """
    def do(self):
	pass


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	SanitizeSonames(),
	RemoveExtraLibs(),
	StripToDebug(),
	NormalizeManPages(),
	NormalizeInfoPages(),
	RemoveTimeStamps(),
	GenerateDependencies(),
    ]
