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

    The following variables apply automatically to the doFile member;
    classes implementing the do member instead may use them as well,
    but should use the same rules if they do use them.  All of them
    have self.macros applied before use.

    @var invariantsubtrees: if invariantsubtrees is not empty,
    then it is a list of subtrees (relative to %(destdir)s) to
    walk INSTEAD of walking the entire %(destdir)s tree.

    @var invariantinclusions: if invariantinclusions is not empty,
    then only files matching a regular expression in it are
    considered to be passed to to the doFile member.  Any exclusions
    including invariants, are applied after invariantinclusions
    are applied.

    @var invariantexceptions: subclasses may set to a list of
    exception regular expressions that are always applied regardless
    of what other exceptions may be provided by the recipe; these
    exceptions being applied is an invariant condition of the doFile
    member.
    """
    invariantsubtree = []
    invariantexceptions = []
    invariantinclusions = []

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

	# compile the inclusions
	self.inclusionREs = []
	for inclusion in self.invariantinclusions:
	    self.inclusionREs.append(re.compile(inclusion %self.macros))

	# compile the exceptions
	self.exceptionREs = []
	for exception in self.invariantexceptions:
	    self.exceptionREs.append(re.compile(exception %self.macros))
	if self.exceptions:
	    if not isinstance(self.exceptions, (tuple, list)):
		self.exceptions = (self.exceptions,)
	    for exception in self.exceptions:
		self.exceptionREs.append(re.compile(exception %self.macros))

	# dispatch if/as appropriate
	if self.use:
	    if hasattr(self.__class__, 'do'):
		self.do()
	    elif hasattr(self.__class__, 'doFile'):
		if not self.invariantsubtree:
		    self.invariantsubtree.append('/')
		for subtree in self.invariantsubtree:
		    os.path.walk('%(destdir)s/'+subtree %self.macros,
				 _walkFile, self)


# internal helpers

def _policyException(policyObj, filespec):
    for re in policyObj.exceptionREs:
	if re.search(filespec):
	    return True
    return False

def _policyInclusion(policyObj, filespec):
    if not policyObj.inclusionREs:
	# empty list is '.*'
	return True
    for re in policyObj.inclusionREs:
	if re.search(filespec):
	    return True
    return False

def _walkFile(policyObj, dirname, names):
    # chop off bit not useful for comparison
    path=dirname[len(policyObj.macros['destdir']):]
    for name in names:
	thispath = path + os.sep + name
	if _policyInclusion (policyObj, thispath) and \
	   not _policyException(policyObj, thispath):
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
    invariantinclusion = ['\.la$']

    def doFile(self, path):
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

class NormalizeGzip(Policy):
    """
    re-gzip .gz files with -9 -n
    Ignore man/info pages, we'll get them separately while fixing
    other things
    """
    invariantexceptions = [
	'%(mandir)s/man.*/.*',
	'%(infodir)s/.*',
    ]
    invariantinclusions = [
	'.*\.gz'
    ]
    def doFile(self, path):
	util.execute('gunzip %s' %path);
	util.execute('gzip -n -9 %s' %path[:-3])

class NormalizeManPages(Policy):
    def do(self):
	pass

class NormalizeInfoPages(Policy):
    """
    compress info files and remove dir file
    """
    def do(self):
	dir = self.macros['infodir']+'/dir'
	fsdir = self.macros['destdir']+dir
	if os.path.exists(fsdir):
	    if not _policyException(self, dir):
		util.remove(fsdir)
	if os.path.isdir('%(destdir)s/%(infodir)s' %self.macros):
	    infofiles = os.listdir('%(destdir)s/%(infodir)s' %self.macros)
	    for file in infofiles:
		syspath = '%(destdir)s/%(infodir)s/' %self.macros + file
		path = '%(infodir)s/' %self.macros + file
		if not _policyException(self, path):
		    if file.endswith('.gz'):
			util.execute('gunzip %s' %syspath)
			syspath = syspath[:-3]
		    util.execute('gzip -n -9 %s' %syspath)


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	SanitizeSonames(),
	RemoveExtraLibs(),
	StripToDebug(),
	NormalizeGzip(),
	NormalizeManPages(),
	NormalizeInfoPages(),
    ]
