#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import re
import os

"""
Base class used for destdirpolicy and packagepolicy
"""


class Policy(util.Action):
    """
    Pure virtual superclass for all policy actions.  Policy actions
    that operate on the entire %(destdir)s implement the @C{do} method;
    Policy actions that operate on a per-file basis implement the
    @C{doFile} method.  The @C{doFile} function is never called for files
    that match an exception regexp; @C{do} functions must implement
    their own exception regexp handling.

    The class variables below apply automatically to the @C{doFile}
    method; classes implementing the @C{do} method instead may use them as
    well, but should use the same rules if they do use them.  All of
    them have @C{self.macros} applied before use.

    @cvar invariantsubtree: if invariantsubtree is not empty,
    then it is a list of subtrees (relative to %(destdir)s) to
    walk INSTEAD of walking the entire %(destdir)s tree.

    @cvar invariantinclusions: if invariantinclusions is not empty,
    then only files matching a regular expression in it are
    considered to be passed to to the @C{doFile} method.  Any exclusions
    including invariants, are applied after invariantinclusions
    are applied.

    @cvar invariantexceptions: subclasses may set to a list of
    exception regular expressions that are always applied regardless
    of what other exceptions may be provided by the recipe; these
    exceptions being applied is an invariant condition of the @C{doFile}
    method.
    """
    invariantsubtree = []
    invariantexceptions = []
    invariantinclusions = []

    keywords = {
        'use': None,
        'exceptions': None
    }

    def __init__(self, *args, **keywords):
	"""
	@keyword exceptions: Optional argument; regexp(s) specifying
	files to ignore while taking the policy action.  It will be
	interpolated against recipe macros before being used.
	@type exceptions: None, regular expression string, or
	tuple/list of regular expressions
	@keyword use: Optional argument; Use flag(s) telling whether
	to actually perform the action.
	@type use: None, Use flag, or tuple/list of Use flags
	"""
	# enforce pure virtual status
	assert(self.__class__ is not Policy)
	util.Action.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	The default way to update a class is to override any provided
	keywords.  Subclasses which have the ability to provide more
	intelligent handling can override this method.  This method
	is invoked automatically by recipe.py when a recipe references
	a policy object.  It acts rather like __init__ except that it
	can meaningfully be called more than once for an object.
	"""
	self.addArgs(*args, **keywords)

    def doProcess(self, recipe):
	"""
	Invocation instance
        @keyword macros: macros which will be expanded through dictionary
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
		    os.path.walk(('%(destdir)s'+subtree) %self.macros,
				 _walkFile, self)


# internal helpers

def _walkFile(policyObj, dirname, names):
    # chop off bit not useful for comparison
    destdirlen = len(policyObj.macros['destdir'])
    path=dirname[destdirlen:]
    for name in names:
       thispath = path + os.sep + name
       if policyInclusion (policyObj, thispath) and \
          not policyException(policyObj, thispath):
           policyObj.doFile(util.normpath(thispath))

# external helpers

def policyException(policyObj, filespec):
    for re in policyObj.exceptionREs:
	if re.search(filespec):
	    return True
    return False

def policyInclusion(policyObj, filespec):
    if not policyObj.inclusionREs:
	# empty list is '.*'
	return True
    for re in policyObj.inclusionREs:
	if re.search(filespec):
	    return True
    return False
