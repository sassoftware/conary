import util
import sys
import string


# build.py and policy.py need some common definitions

def checkUse(use):
    """
    Determines whether to take an action, based on system configuration
    @param use: Flags telling whether to take action
    @type use: None, boolean, or tuple of booleans
    """
    if use == None:
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
                arg % d

    def doAction(self):
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
                self.error(TypeError, "%s.__init__() got an unexpected keyword argument "
                                  "'%s'" % (self.__class__.__name__, key))
        # copy the keywords into our dict, overwriting the defaults
        self.__dict__.update(keywords)

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

    def __init__(self, recipe, *args, **keywords):
        assert(self.__class__ is not RecipeAction)
	self._getLineNum()
	Action.__init__(self, *args, **keywords)
	self.recipe = recipe
	# change self.use to be a simple flag
	self.use = checkUse(self.use)
        
    # virtual method for actually executing the action
    def doAction(self):
	if self.use:
	    self.do()

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

    def error(self, type, msg):
	raise type, "%s:%s: %s: %s" % (self.file, self.linenum,
					   type.__name__, msg)



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


