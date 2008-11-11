#
# Copyright (c) 2004-2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Provides superclasses for build and policy.
"""
from conary.lib import fixedfnmatch
import itertools
import os
import re
import sys
import string
import traceback

from conary.lib import debugger, log, util
from conary.local import database

# build.py and policy.py need some common definitions

def checkUse(use):
    """
    Determines whether to take an action, based on system configuration
    @param use: Flags telling whether to take action
    @type use: None, boolean, or tuple of booleans
    """
    if use is None:
	return True
    if type(use) is not tuple:
	use = (use,)
    for usevar in use:
	if not usevar:
	    return False
    return True

class _AnyDict(dict):
    """A dictionary that returns None for any key that is accessed.  Used
    internally to verify dictionary format string expansion"""
    def __getitem__(self, key):
        return None

class Action:
    """
    Pure virtual base class for all actions -- classes which are
    instantiated with data, and later asked to take an action based
    on that data.

    @cvar keywords: The keywords and default values accepted by the class
    """

    keywords = { 'debug' : False } 

    def __init__(self, *args, **keywords):
        assert(self.__class__ is not Action)
	# keywords will be in the class object, not the instance
	if not hasattr(self.__class__, 'keywords'):
	    self.keywords = {}
        self._applyDefaults()
	self.addArgs(*args, **keywords)
        # verify that there are not broken format strings
        d = _AnyDict()
        for arg in args:
            if type(arg) is str and '%' in arg:
                try:
                    arg % d
                except ValueError, msg:
                    log.error('invalid macro substitution in "%s", missing "s"?' %arg)
                    raise

    def doAction(self):
	if self.debug:
	    debugger.set_trace()
	self.do()

    def do(self):
	pass

    def _applyDefaults(self):
        """
        Traverse the class hierarchy, picking up default keywords.  We
        ascend to the topmost class and pick up the keywords as we work
        back to our class, to allow proper overriding.
        """
        baselist = [self.__class__]
        bases = list(self.__class__.__bases__)
        while bases:
	    parent = bases.pop()
	    bases.extend(list(parent.__bases__))
            baselist.append(parent)
        baselist.reverse()
        for base in baselist:
            if 'keywords' in base.__dict__:
                self.__dict__.update(base.__dict__['keywords'])

    def addArgs(self, *args, **keywords):
        # check to make sure that we don't get a keyword we don't expect
        for key in keywords.keys():
            # XXX this is not the best test, but otherwise we have to
            # keep a dictionary of all of the keywords (including the parent
            # keywords)
            if key not in self.__dict__.keys():
                raise TypeError, ("%s.__init__() got an unexpected keyword argument "
                                  "'%s'" % (self.__class__.__name__, key))
        # copy the keywords into our dict, overwriting the defaults
        self.__dict__.update(keywords)

def genExcepthook(self):
    def excepthook(type, exc_msg, tb):
        cfg = self.recipe.cfg
        sys.excepthook = sys.__excepthook__
        if cfg.debugRecipeExceptions:
            lines = traceback.format_exception(type, exc_msg, tb)
            print string.joinfields(lines, "")
        if self.linenum is not None:
            prefix = "%s:%s:" % (self.file, self.linenum)
            prefix_len = len(prefix)
            if str(exc_msg)[:prefix_len] != prefix:
                exc_message = "%s:%s: %s: %s" % (self.file, self.linenum, 
                                              type.__name__, exc_msg)
            print exc_message

        if self.recipe.buildinfo:
            try:
                buildinfo = self.recipe.buildinfo
                buildinfo.error = exc_message
                buildinfo.file = self.file
                buildinfo.lastline = self.linenum
                buildinfo.stop()
            except:
                log.warning("could not write out to buildinfo")

        if cfg.debugRecipeExceptions and self.recipe.isatty():
            debugger.post_mortem(tb, type, exc_msg)
        else:
            sys.exit(1)
    return excepthook


class RecipeAction(Action):
    """
    Action class which accepts the use= keyword to control execution,
    and which assumes that the action is being called from within a recipe.
    The action stores the line in the recipe file which calls it, in order
    to allow for that line number to be reported when raising an exception.
    """

    keywords = {
        'use': None
    }

    # define which types of recipe an action is available for
    _packageAction = True
    _groupAction = False

    # using an action may suggest the addition of a build requirement
    # (like r.Make requiring make)
    _actionTroveBuildRequires = set([])
    _actionPathBuildRequires = set([])

    def __init__(self, recipe, *args, **keywords):
        assert(self.__class__ is not RecipeAction)
	self._getLineNum()
	Action.__init__(self, *args, **keywords)
	self.recipe = recipe
	# change self.use to be a simple flag
	self.use = checkUse(self.use)

    def _addActionPathBuildRequires(self, buildRequires):
        # We do not want dynamically added requirements to modify the class
        if id(self._actionPathBuildRequires) == \
           id(self.__class__._actionPathBuildRequires):
            self._actionPathBuildRequires = set(self._actionPathBuildRequires)
        self._actionPathBuildRequires.update(buildRequires)

    def _addActionTroveBuildRequires(self, buildRequires):
        # We do not want dynamically added requirements to modify the class
        if id(self._actionTroveBuildRequires) == \
           id(self.__class__._actionTroveBuildRequires):
            self._actionTroveBuildRequires = set(self._actionTroveBuildRequires)
        self._actionTroveBuildRequires.update(buildRequires)

    # virtual method for actually executing the action
    def doAction(self):
	if self.debug:
	    debugger.set_trace()
	if self.use:
            try:
                if self.linenum is None:
                    self.do()
                else:
                    oldexcepthook = sys.excepthook
                    sys.excepthook = genExcepthook(self)
                    if self.recipe.buildinfo:
                        self.recipe.buildinfo.lastline = self.linenum
                    self.do()
                    sys.excepthook = oldexcepthook

            finally:
                # we need to provide suggestions even in the failure case
                self.doSuggestAutoBuildReqs()
	else:
            # any invariant suggestions should be provided even if not self.use
            self.doSuggestAutoBuildReqs()

    def doSuggestAutoBuildReqs(self):
        if not hasattr(self.recipe, "buildRequires"):
            # Most likely group recipe
            return
        if hasattr(self.recipe, "getRepos"):
            repos = self.recipe.getRepos()
        else:
            repos = None

        paths = []
        buildRequires = self.recipe._getTransitiveBuildRequiresNames()
        for cmd in self._actionPathBuildRequires:
            # Catch the case "python setup.py"
            cmdarr = cmd.split(' ')
            # Try to catch the command "ENVVAR=val make": skip all words that
            # have an equal sign in them
            c = cmd
            for x in cmdarr:
                if '=' not in x:
                    c = x
                    break
            # If the above for loop didn't find anything remotely resembling a
            # command, use the original one
            c = c % self.recipe.macros
            fullPath = util.checkPath(c)
            if (not fullPath) and repos:
                if not c.startswith('/'):
                    candidatePaths = [os.path.join(x, c)
                        for x in os.getenv('PATH', '').split(os.path.pathsep)]
                else:
                    candidatePaths = [c]
                foundProvider = False
                for label in self.recipe.cfg.installLabelPath:
                    trvDict = repos.getTroveVersionsByPath(candidatePaths,
                                                           label)
                    trvs = [x for x in trvDict.values() if x]
                    if trvs:
                        foundProvider = True
                        self._addActionTroveBuildRequires([trvs[0][0][0]])
                        break
                if not foundProvider:
                    log.warning('Failed to find possible build requirement'
                        ' for path "%s"' % c)
                continue
            paths.append(fullPath)
        if not hasattr(self.recipe, '_pathLookupCache'):
            pathCache = self.recipe._pathLookupCache = _pathLookupCache()
        else:
            pathCache = self.recipe._pathLookupCache
        suggestsMap = pathCache.getTrovesByPaths(self._getDb(), paths)
        suggests = set()
        for k, v in suggestsMap.items():
            suggests.update(v)
        # Add the trove requirements
        suggests.update(self._actionTroveBuildRequires)
        # Tell reportExcessBuildRequires that all these are necessary
        if not hasattr(self.recipe, 'reportExcessBuildRequires'):
            return
        self.recipe.reportExcessBuildRequires(suggests)
        # Remove build requires that were already added
        suggests = suggests - set(buildRequires)
        if suggests:
            log.warning('Some missing buildRequires %s' %(sorted(suggests)))
            self.recipe.reportMissingBuildRequires(sorted(suggests))

    def doPrep(self):
	pass

    def do(self):
	pass

    def _getLineNum(self):
	"""Gets the line number and file name of the place where the 
	   Action is instantiated, which is important for returning
	   useful error messages"""

	# Moves up the frame stack to outside of Action class --
	# also passes by __call__ function, used by helper functions
	# internally to instantiate Actions.  
	#
	# Another alternative would be to look at filepath until we 
	# reach outside of conary source tree
	f = sys._getframe(1) # get frame above this one

	while f != None:
	    if f.f_code.co_argcount == 0:  # break if non-class fn
		break

	    firstargname = f.f_code.co_varnames[0]
	    firstarg = f.f_locals[firstargname]
	    if not isinstance(firstarg, Action): 
	       if f.f_code.co_name != '__call__':  
		   break			 
	    f = f.f_back # go up a frame

	assert f is not None 
	self.file = f.f_code.co_filename
	self.linenum = f.f_lineno
	if not self.file:
	    self.file = '<None>'

    def init_error(self, type, msg):
	"""
	    use in action __init__ to add lineno to exceptions
	    raised.  Usually this is handled automatically, 
	    but it is (almost) impossible to wrap init calls.
	    Actually, this probably could be done by changing 
	    recipe helper, but until that is done use this funciton
	"""
	
	raise type, "%s:%s: %s: %s" % (self.file, self.linenum,
					   type.__name__, msg)

    def _getDb(self):
        if not hasattr(self.recipe, '_db') or self.recipe._db is None:
            self.recipe._db = database.Database(self.recipe.cfg.root,
                                                self.recipe.cfg.dbPath)
        return self.recipe._db

# XXX look at ShellCommand versus Action
class ShellCommand(RecipeAction):
    """Base class for shell-based commands. ShellCommand is an abstract class
    and can not be made into a working instance. Only derived classes which
    define the C{template} static class variable will work properly.

    Note: when creating templates, be aware that they are evaulated
    twice, in the context of two different dictionaries.
     - keys from keywords should have a # single %, as should "args".
     - keys passed in through the macros argument will need %% to
       escape them for delayed evaluation; for example,
       %%(builddir)s and %%(destdir)s
    
    @ivar self.command: Shell command to execute. This is built from the
    C{template} static class variable in derived classes.
    @type self.command: str
    initialization time.
    @cvar template: The string template used to build the shell command.
    """
    def __init__(self, recipe, *args, **keywords):
        """Create a new ShellCommand instance that can be used to run
        a simple shell statement
        @param args: arguments to __init__ are stored for later substitution
        in the shell command if it contains %(args)s
        @param keywords: keywords are replaced in the shell command
        through dictionary substitution
        @raise TypeError: If a keyword is passed to __init__ which is not
        accepted by the class.
        @rtype: ShellCommand
        """
	# enforce pure virtual status
        assert(self.__class__ is not ShellCommand)
	self.recipe = recipe
	self.arglist = args
        self.args = string.join(args)
        # fill in anything in the template that might be specified
        # as a keyword.  Keywords only because a part of this class
        # instance's dictionary if Action._applyDefaults is called.
        # this is the case for build.BuildCommand instances, for example.
        self.command = self.template % self.__dict__
        # verify that there are not broken format strings
        d = _AnyDict()
        self.command % d
        for arg in args:
            if type(arg) is str and '%' in arg:
                arg % d

    def addArgs(self, *args, **keywords):
	# append new arguments as well as include keywords
        self.args = self.args + string.join(args)
	RecipeAction.addArgs(self, *args, **keywords)


def _expandOnePath(path, macros, defaultDir=None, braceGlob=False, error=False):
    if braceGlob:
        return _expandPaths([path], macros, defaultDir, True, error)
    if defaultDir is None:
        defaultDir = macros.builddir

    path = path % macros
    if path and path[0] == '/':
        if path.startswith(macros.destdir):
            log.warning(
                "remove destdir from path name %s;"
                " absolute paths are automatically relative to destdir"
                %path)
        else:
            path = macros.destdir + path
    else:
        path = os.path.join(defaultDir, path)

    if error:
        if not os.path.exists(path):
            raise RuntimeError, "No such file '%s'" % path
    return path

def matchRegexp(baseDir, pattern, regexpFlags):
    if pattern[-1] != '$':
        pattern = pattern + '$'
    results = []
    for root, dirs, fileNames in os.walk(baseDir):
        for pathItem in itertools.chain(dirs + fileNames):
            path = root + '/' + pathItem
            if re.match(pattern, path):
                results.append(path)
    return results

class Regexp(object):
    def __init__(self, pattern):
        self.pattern = pattern

    def __hash__(self):
        return hash(self.pattern)

    def __eq__(self, expr):
        if isinstance(expr, Glob):
            return expr() == self.pattern
        elif isinstance(expr, Regexp):
            return expr.pattern == self.pattern
        return expr == self.pattern

    def __repr__(self):
        return 'Regexp(%r)' % self.pattern

    def __str__(self):
        return "Regexp(%r)" % self.pattern.replace('%', '%%')

class Glob(object):
    def __init__(self, recipe, pattern):
        self.macros = recipe.macros
        self.pattern = pattern

    def __repr__(self):
        return "Glob(%r)" % self.pattern

    def __str__(self):
        return "Glob(%r)" % self.pattern.replace('%', '%%')

    def __eq__(self, expr):
        if isinstance(expr, Glob):
            return expr() == self()
        elif isinstance(expr, Regexp):
            return expr.pattern == self()
        return expr == self()

    def __hash__(self):
        return hash(self())

    def __call__(self):
        """
        Translate a shell PATTERN to a regular expression, substituting macros.

        There is no way to quote meta-characters.
        """
        # macros must be substituted first, so that they can be properly
        # escaped
        try:
            pat = self.pattern % self.macros
        except ValueError, msg:
            log.error('invalid macro substitution in "%s", missing "s"?' % \
                    self.pattern)
            raise
        return '^' + fixedfnmatch.translate(pat)

def _expandPaths(paths, macros, defaultDir=None, braceGlob=True, error=False):
    """
    Expand braces, globs, and macros in path names, and root all path names
    to either the build dir or dest dir.  Relative paths (not starting with
    a /) are relative to builddir.  All absolute paths to are relative to 
    destdir.  
    """
    destdir = macros.destdir
    if defaultDir is None:
        defaultDir = macros.builddir
    expPaths = []
    for item in paths:
        if isinstance(item, Regexp):
            isRegexp = True
            path = item.pattern
        elif isinstance(item, Glob):
            isRegexp = False
            braceGlob = True
            path = item.pattern
        else:
            isRegexp = False
            path = item
        path = path % macros
        if path[0] == '/':
            if path.startswith(destdir):
                log.warning(
                    "remove destdir from path name %s;"
                    " absolute paths are automatically relative to destdir"
                    %path)
            else:
                path = destdir + path
            baseDir = destdir
        else:
            path = defaultDir + os.sep + path
            baseDir = defaultDir
        if isRegexp:
            expPaths.extend(matchRegexp(baseDir, path, item))
        elif braceGlob:
            expPaths.extend(util.braceGlob(path))
        else:
            expPaths.append(path)
    if error:
        notfound = []
        for path in expPaths:
            if not os.path.exists(path):
                notfound.append(path)
        if notfound:
            raise RuntimeError, "No such file(s) '%s'" % "', '".join(notfound)
    return expPaths

class _pathLookupCache(object):
    """Simple cache object for path lookups (singleton-like)"""

    __slots__ = ['_cache']

    def __init__(self):
        self._cache = {}

    def getTrovesByPaths(self, db, paths):
        ret = {}
        for path in paths:
            if path in self._cache:
                ret[path] = self._cache[path]
            else:
                ret[path] = self._cache[path] = [ x.getName()
                                          for x in db.iterTrovesByPath(path) ]
        return ret
