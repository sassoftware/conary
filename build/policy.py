#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import filter
import os

"""
Base class used for destdirpolicy and packagepolicy
"""


class Policy(util.Action):
    """
    Pure virtual superclass for all policy actions.  Policy actions
    that operate on the entire C{%(destdir)s} implement the C{do} method;
    Policy actions that operate on a per-file basis implement the
    C{doFile} method.  The C{doFile} function is never called for files
    that match an exception regexp; C{do} functions must implement
    their own exception regexp handling.

    The class variables below apply automatically to the C{doFile}
    method; classes implementing the C{do} method instead may use them as
    well, but should use the same rules if they do use them.  All of
    them have C{self.macros} applied before use.

    @cvar invariantsubtrees: if invariantsubtrees is not empty,
    then it is a list of subtrees (relative to C{%(destdir)s}) to
    walk INSTEAD of walking the entire C{%(destdir)s} tree.

    @cvar invariantinclusions: if invariantinclusions is not empty,
    then only files matching a filter in it are considered to be passed
    to to the C{doFile} method.  Any exceptions, including invariants,
    are applied after invariantinclusions are applied.

    @cvar invariantexceptions: subclasses may set to a list of
    exception filters that are always applied regardless of what other
    exceptions may be provided by the recipe; these exceptions being
    applied is an invariant condition of the C{doFile} method.
    """
    invariantsubtrees = []
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
	@type exceptions: None, filter string/tuple, or
	tuple/list of filter strings/tuples
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

	Some keyword arguments (at least C{exceptions}) should be
	appended rather than replaced.
	"""
	exceptions = keywords.pop('exceptions', None)
	if exceptions:
	    if not self.exceptions:
		self.exceptions = []
	    self.exceptions.append(exceptions)
	self.addArgs(*args, **keywords)

    def filterExpression(self, expression, name=None):
	"""
	@param expression: regular expression or tuple of
	(regex, [setmode, [unsetmode]])
	Create tuple that represents arguments to filter.Filter.__init__
	"""
	if type(expression) is str:
	    return (expression, self.macros)
	if type(expression) is not list:
	    expression = list(expression)
	expression[1:1] = [self.macros]
	if name:
	    while len(expression) < 4:
		expression.append(None)
	    expression.append(name)
	return expression

    def compileFilters(self, expressionList, filterList):
	for expression in expressionList:
	    expression = self.filterExpression(expression)
	    filterList.append(filter.Filter(*expression))

    def doProcess(self, recipe):
	"""
	Invocation instance
        @param recipe: holds the recipe object, which is used for
	the macro set and package objects.
        @return: None
        @rtype: None
	"""
	self.recipe = recipe
	self.macros = recipe.macros

	# is runtime check implemented?
	if hasattr(self.__class__, 'test'):
	    if not self.test():
		return

	# change self.use to be a simple flag
	self.use = util.checkUse(self.use)

	# compile the inclusions
	self.inclusionFilters = []
	self.compileFilters(self.invariantinclusions, self.inclusionFilters)

	# compile the exceptions
	self.exceptionFilters = []
	self.compileFilters(self.invariantexceptions, self.exceptionFilters)
	if self.exceptions:
	    if not isinstance(self.exceptions, (tuple, list)):
		# turn a plain string into a sequence
		self.exceptions = (self.exceptions,)
	    self.compileFilters(self.exceptions, self.exceptionFilters)

	# dispatch if/as appropriate
	if self.use:
	    if hasattr(self.__class__, 'do'):
		self.do()
	    elif hasattr(self.__class__, 'doFile'):
		if not self.invariantsubtrees:
		    self.invariantsubtrees.append('/')
		for self.currentsubtree in self.invariantsubtrees:
		    os.path.walk(
			('%(destdir)s'+self.currentsubtree) %self.macros,
			self.walkDir, None)

    def walkDir(self, ignore, dirname, names):
	# chop off bit not useful for comparison
	destdirlen = len(self.macros['destdir'])
	path=dirname[destdirlen:]
	for name in names:
	   thispath = util.normpath(path + os.sep + name)
	   if self.policyInclusion(thispath) and \
	      not self.policyException(thispath):
	       self.doFile(thispath)

    def policyInclusion(self, filespec):
	if not self.inclusionFilters:
	    # empty list is '.*'
	    return True
	for f in self.inclusionFilters:
	    if f.match(filespec):
		return True
	return False

    def policyException(self, filespec):
	for f in self.exceptionFilters:
	    if f.match(filespec):
		return True
	return False


class PolicyError(Exception):
    """
    Base class from which policy error classes inherit
    """
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
