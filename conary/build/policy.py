#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
Base classes used for destdirpolicy and packagepolicy.
"""
import os

from conary.lib import util, log
from conary.build import filter
from conary.build import action



class Policy(action.RecipeAction):
    """
    Abstract superclass for all policy actions.  Policy actions
    that operate on the entire C{%(destdir)s} implement the C{do} method;
    Policy actions that operate on a per-file basis implement the
    C{doFile} method.  The C{doFile} function is never called for files
    that match an exception regexp; C{do} functions must implement
    their own exception regexp handling.

    The class variables below apply automatically to the C{doFile}
    method; classes implementing the C{do} method instead may use them as
    well, but should use the same rules if they do use them.  All of
    them have C{self.macros} applied before use.

    @cvar invariantsubtrees: if C{invariantsubtrees} is not empty,
    then it is a list of subtrees (relative to C{%(destdir)s}) to
    walk INSTEAD of walking the entire C{%(destdir)s} tree.  Any
    C{subtrees} are appended to C{invariantsubtrees}.

    @cvar invariantinclusions: if C{invariantinclusions} is not empty,
    then only files matching a filter in it are considered to be passed
    to to the C{doFile} method.  Any exceptions, including invariants,
    are applied after C{invariantinclusions} are applied; this means
    that all exceptions OVERRULE every type of inclusion.

    @cvar invariantexceptions: subclasses may set to a list of
    exception filters that are always applied regardless of what other
    exceptions may be provided by the recipe; these exceptions being
    applied is an invariant condition of the C{doFile} method.

    @cvar recursive: if True, walk entire subtrees; if False,
    work only on contents of listed directories (C{invariantsubtrees}
    and C{subtrees}).
    @type recursive: boolean
    """
    invariantsubtrees = []
    invariantexceptions = []
    invariantinclusions = []
    recursive = True

    keywords = {
        'use': None,
        'exceptions': None,
        'inclusions': None,
	'subtrees': None,
    }


    rootdir = '%(destdir)s'

    def __init__(self, recipe, **keywords):
	"""
	@keyword exceptions: Optional argument; regexp(s) specifying
	files to ignore while taking the policy action.  It will be
	interpolated against recipe macros before being used.
	@type exceptions: None, filter string/tuple, or
	tuple/list of filter strings/tuples
	@keyword use: Optional argument; Use flag(s) telling whether
	to actually perform the action.
	@type use: None, Use flag, or tuple/list of Use flags
        @keyword subtree: Subtree to which to limit the policy, or it
        it already is limited (invariantsubtrees), then additional
        subtrees to consider.
        @type subtree: string or sequence of strings
        @keyword inclusions: C{FileFilter}s to which to limit the policy,
        or if it already is limited (invariantinclusion) then additional
        C{FileFilter}s to include within the general limitation.
        @type inclusions: C{FileFilter} strings, C{FileFilter} tuples,
        or list (not tuple) of C{FileFilter} strings or C{FileFilter} tuples.
	"""
	# enforce abstract base class status
	assert(self.__class__ is not Policy)

	action.RecipeAction.__init__(self, None, [], **keywords)
	self.recipe = recipe


    def updateArgs(self, *args, **keywords):
	"""
	The default way to update a class is to override any provided
	keywords.  Subclasses which have the ability to provide more
	intelligent handling can override this method.  This method
	is invoked automatically by recipe.py when a recipe references
	a policy object.  It acts rather like __init__ except that it
	can meaningfully be called more than once for an object.

	Some keyword arguments (at least C{exceptions} and C{subtrees})
	should be appended rather than replaced.
	"""
	exceptions = keywords.pop('exceptions', None)
	if exceptions:
	    if not self.exceptions:
		self.exceptions = []
            if type(exceptions) in (list, tuple):
                self.exceptions.extend(exceptions)
            else:
	        self.exceptions.append(exceptions)
	subtrees = keywords.pop('subtrees', None)
	if subtrees:
	    if not self.subtrees:
		self.subtrees = []
            if type(subtrees) in (list, tuple):
	        self.subtrees.extend(subtrees)
            else:
	        self.subtrees.append(subtrees)

	inclusions = keywords.pop('inclusions', [])
	if (args or inclusions) and not self.inclusions:
            self.inclusions = []

        if inclusions:
            if type(inclusions) == list:
                self.inclusions.extend(inclusions)
            else:
                self.inclusions.append(inclusions)

        if args:
	    self.inclusions.extend(args)

	self.addArgs(**keywords)

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
	else:
	    while len(expression) < 5:
		expression.append(None)
	expression.append(self.rootdir)
	return expression

    def compileFilters(self, expressionList, filterList):
        seen = []
	for expression in expressionList:
            if expression in seen:
                # only put each expression on the list once
                continue
            seen.append(expression)
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

	self.rootdir = self.rootdir % self.macros

	if hasattr(self.__class__, 'preProcess'):
	    self.preProcess()

	# is runtime check implemented?
	if hasattr(self.__class__, 'test'):
	    if not self.test():
		return

	# change self.use to be a simple flag
	self.use = action.checkUse(self.use)

	# compile the exceptions
	self.exceptionFilters = []
	self.compileFilters(self.invariantexceptions, self.exceptionFilters)
	if self.exceptions:
	    if not isinstance(self.exceptions, (tuple, list)):
		# turn a plain string into a sequence
		self.exceptions = (self.exceptions,)
	    self.compileFilters(self.exceptions, self.exceptionFilters)

	# compile the inclusions
	self.inclusionFilters = []
	self.compileFilters(self.invariantinclusions, self.inclusionFilters)
	if not self.inclusions:
	    # an empty list, as opposed to None, means nothing is included
	    if isinstance(self.inclusions, (tuple, list)):
		return
	else:
	    if not isinstance(self.inclusions, (tuple, list)):
		# turn a plain string into a sequence
		self.inclusions = (self.inclusions,)
	    self.compileFilters(self.inclusions, self.inclusionFilters)

	# dispatch if/as appropriate
	if self.use:
	    self.do()

	if hasattr(self.__class__, 'postProcess'):
	    self.postProcess()

    def do(self):
	# calls doFile on all appropriate files -- can be overridden by
	# subclasses
	if self.subtrees:
	    self.invariantsubtrees.extend(self.subtrees)
	if not self.invariantsubtrees:
	    self.invariantsubtrees.append('/')
	for self.currentsubtree in self.invariantsubtrees:
	    fullpath = (self.rootdir+self.currentsubtree) %self.macros
	    if self.recursive:
		os.path.walk(fullpath, self.walkDir, None)
	    else:
		# only one level
		if os.path.isdir(fullpath):
		    self.walkDir(None, fullpath, os.listdir(fullpath))

    def walkDir(self, ignore, dirname, names):
	# chop off bit not useful for comparison
	rootdirlen = len(self.rootdir)
	path=dirname[rootdirlen:]
	for name in names:
	   thispath = util.normpath(path + os.sep + name)
	   if self._pathAllowed(thispath):
	       self.doFile(thispath)

    def _pathAllowed(self, path):
        if self.policyInclusion(path) and not self.policyException(path):
            return True
        return False

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

    # warning and error reporting

    def _addClassName(self, args):
        args = list(args)
        args[0] = ': '.join((self.__class__.__name__, args[0]))
        return args

    def dbg(self, *args, **kwargs):
        args = self._addClassName(args)
        log.debug(*args, **kwargs)

    def info(self, *args, **kwargs):
        args = self._addClassName(args)
        log.info(*args, **kwargs)

    def warn(self, *args, **kwargs):
        args = self._addClassName(args)
        log.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        args = self._addClassName(args)
        log.error(*args, **kwargs)
        self.recipe.reportErrors(*args, **kwargs)


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
