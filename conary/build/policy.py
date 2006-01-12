#
# Copyright (c) 2004-2006 rPath, Inc.
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
Base classes and data used for all policy
"""

import imp
import os
import sys

from conary.lib import util, log, graph
from conary.build import action, filter


# buckets (enum -- but may possibly work someday as bitmask for policy
# that could run more than once in different contexts)
TESTSUITE            = 1 << 0
DESTDIR_PREPARATION  = 1 << 1
DESTDIR_MODIFICATION = 1 << 2
PACKAGE_CREATION     = 1 << 3
PACKAGE_MODIFICATION = 1 << 4
ENFORCEMENT          = 1 << 5
ERROR_REPORTING      = 1 << 6

# requirements (sparse bitmask, 5 sensible states)
REQUIRED               = 1 << 0
ORDERED                = 1 << 1
PRIOR                  = 1 << 2
REQUIRED_PRIOR         = REQUIRED|ORDERED|PRIOR
REQUIRED_SUBSEQUENT    = REQUIRED|ORDERED
CONDITIONAL_PRIOR      = ORDERED|PRIOR
CONDITIONAL_SUBSEQUENT = ORDERED

# file trees
NO_FILES = 0 << 0
DESTDIR  = 1 << 0
BUILDDIR = 1 << 1
DIR      = DESTDIR|BUILDDIR
PACKAGE  = 1 << 2


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
    bucket = None
    invariantsubtrees = []
    invariantexceptions = []
    invariantinclusions = []
    recursive = True
    filetree = DESTDIR
    rootdir = None

    keywords = {
        'use': None,
        'exceptions': None,
        'inclusions': None,
	'subtrees': None,
    }

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
        if self.rootdir is None and self.filetree:
            self.rootdir = {
                DESTDIR: '%(destdir)s',
                BUILDDIR: '%(builddir)s',
                PACKAGE: '',
            }[self.filetree]


    def postInit(self):
        """
        Hook for initialization that cannot happen until after all
        policies have been loaded into the recipe and thus cannot
        happen in __init__; mainly for policies that need to pass
        information to other policies at initialization time.
        """
        pass

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

        if self.rootdir:
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
        if not self.filetree:
            return

        if self.filetree & PACKAGE:
            pkg = self.recipe.autopkg
            for thispath in sorted(pkg.pathMap):
                if self._pathAllowed(thispath):
                    # FIXME: spinner?
                    self.doFile(thispath)
            return

        assert(self.filetree & DIR)
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
               # FIXME: spinner?
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


# External policy classes (do not create classes for internal policy buckets)
class DestdirPolicy(Policy):
    bucket = DESTDIR_MODIFICATION

class PackagePolicy(Policy):
    bucket = PACKAGE_MODIFICATION

class EnforcementPolicy(Policy):
    bucket = ENFORCEMENT


class _K:
    pass
classType = type(_K)

# loading, sorting, and initializing policy modules
def loadPolicy(recipeObj):
    # path -> module
    policyPathMap = {}
    # bucket -> ordered list of policy objects
    policies = {
        TESTSUITE: [],
        DESTDIR_PREPARATION: [],
        DESTDIR_MODIFICATION: [],
        PACKAGE_CREATION: [],
        PACKAGE_MODIFICATION: [],
        ENFORCEMENT: [],
        ERROR_REPORTING: [],
    }
    # bucket -> dict of policy classes
    policyMap = dict((b, {}) for b in policies.keys())
    # name -> policy classes
    policyNameMap = {}

    # Load pluggable policy
    for policyDir in recipeObj.cfg.policyDirs:
        if not os.path.isdir(policyDir):
            continue
        for filename in os.listdir(policyDir):
            fullpath = os.sep.join((policyDir, filename))
            if not filename.endswith('.py') or not util.isregular(fullpath):
                continue
            # do not load shared libraries as policy!
            desc = [x for x in imp.get_suffixes() if x[0] == '.py'][0]
            f = file(fullpath)
            modname = filename[:-3]
            m = imp.load_module(modname, f, fullpath, desc)
            f.close()
            policyPathMap[fullpath] = m

            for symbolName in m.__dict__:
                policyCls = m.__dict__[symbolName]
                if type(policyCls) is not classType:
                    continue
                if symbolName[0].isupper() and issubclass(policyCls, Policy):
                    policyNameMap[symbolName] = policyCls

    # Load conary internal policy
    import conary.build.destdirpolicy
    import conary.build.packagepolicy
    for pt in 'destdirpolicy', 'packagepolicy':
        m = sys.modules['conary.build.'+pt]
        for symbolName in m.__dict__.keys():
            policyCls = m.__dict__[symbolName]
            if type(policyCls) is not classType:
                continue
            if symbolName[0] != '_' and issubclass(policyCls, Policy):
                policyNameMap[symbolName] = policyCls

    # Enforce dependencies
    missingDeps = []
    for policyCls in policyNameMap.values():
        if hasattr(policyCls, 'requires'):
            for reqName, reqType in policyCls.requires:
                if reqType & REQUIRED and reqName not in policyNameMap:
                    missingDeps.append((policyCls.__name__, reqName))
    if missingDeps:
        raise PolicyError, '\n'.join(
            ('policy %s missing required policy %s' %(x,y)
             for x, y in missingDeps))

    # Sort and initialize
    for policyName, policyCls in policyNameMap.iteritems():
        policyMap[policyCls.bucket][policyName]=policyNameMap[policyName]
    for bucket in policyMap.keys():
        dg = graph.DirectedGraph()
        for policyCls in policyMap[bucket].values():
            dg.addNode(policyCls)
            if hasattr(policyCls, 'requires'):
                for reqName, reqType in policyCls.requires:
                    if reqType & ORDERED and reqName in policyMap[bucket]:
                        if reqType & PRIOR:
                            dg.addEdge(policyNameMap[reqName], policyCls)
                        else:
                            dg.addEdge(policyCls, policyNameMap[reqName])

        # test for dependency loops
        depLoops = [x for x in dg.getStronglyConnectedComponents()
                    if len(x) > 1]
        if depLoops:
            # convert to names
            depLoops = [sorted(x.__name__ for x in y) for y in depLoops]
            raise PolicyError, '\n'.join(
                'found dependency loop: %s' %', '.join(y)
                 for y in depLoops)
                
        # store an ordered list of initialized policy objects
        policies[bucket] = [x(recipeObj) for x in dg.getTotalOrdering(
            nodeSort=lambda a, b: cmp(a[1].__name__, b[1].__name__))]

    return policyPathMap, policies



class PolicyError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
