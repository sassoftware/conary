#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import os
import string
import errno
import sys
import traceback
import pdb
import exceptions
import fixedglob
import shutil
import string
import log
import errno

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


class Action:
    """
    Pure virtual base class for all actions -- classes which are
    instantiated with data, and later asked to take an action based
    on that data.

    @cvar keywords: The keywords and default values accepted by the class
    """

    def _applyDefaults(self):
        """
        Traverse the class hierarchy, picking up default keywords.  We
        ascend to the topmost class and pick up the keywords as we work
        back to our class, to allow proper overriding.
        """
        baselist = []
        bases = list(self.__class__.__bases__)
        while bases:
	    parent = bases.pop()
	    bases.extend(list(parent.__bases__))
            baselist.append(parent)
        baselist.reverse()
        for base in baselist:
            if base.__dict__.has_key('keywords'):
                self.__dict__.update(base.__dict__['keywords'])
        if self.__class__.__dict__.has_key('keywords'):
            self.__dict__.update(self.__class__.keywords)
        
    def __init__(self, *args, **keywords):
        assert(self.__class__ is not Action)
	# keywords will be in the class object, not the instance
	if not hasattr(self.__class__, 'keywords'):
	    self.keywords = {}
        self._applyDefaults()
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

class ShellCommand(Action):
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
    def __init__(self, *args, **keywords):
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
        self.args = string.join(args)
        # pre-fill in the preMake and arguments
        self.command = self.template % self.__dict__


# Simple ease-of-use extensions to python libraries

def mkdirChain(*paths):
    for path in paths:
        if path[0] != "/":
            path = os.getcwd() + "/" + path
            
        paths = path.split('/')
            
        for n in (range(2,len(paths) + 1)):
            p = string.join(paths[0:n], "/")
            if not os.path.exists(p):
                # don't die in case of the race condition where someone
                # made the directory after we stat'ed for it.
                try:
                    os.mkdir(p)
                except OSError, exc:
                    if exc.errno == errno.EEXIST:
                        pass

def _searchVisit(arg, dirname, names):
    file = arg[0]
    path = arg[1]
    testname = '%s/%s' %(dirname, file)
    if file in names:
	path[0] = testname
	del names

def searchPath(file, basepath):
    path = [ None ]
    # XXX replace with os.walk in python 2.3, to cut short properly
    os.path.walk(basepath, _searchVisit, (file, path))
    return path[0]

def searchFile(file, searchdirs, error=None):
    for dir in searchdirs:
        s = "%s/%s" %(dir, file)
        if os.path.exists(s):
            return s
    if error:
        raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT))
    return None

def findFile(file, searchdirs):
    return searchFile(file, searchdirs, error=1)

def excepthook(type, value, tb):
    #if type is exceptions.IOError:
        #sys.stderr.write('%s\n' % value)
        #sys.exit(1)
    sys.excepthook = sys.__excepthook__
    lines = traceback.format_exception(type, value, tb)
    print string.joinfields(lines, "")
    if sys.stdout.isatty() and sys.stdin.isatty():
        pdb.post_mortem(tb)
    else:
        sys.exit(1)

def execute(cmd, destDir=None):
    log.debug(cmd)
    if destDir:
	rc = os.system('cd %s; %s' %(destDir, cmd))
    else:
	rc = os.system(cmd)
    if rc:
	if not os.WIFEXITED(rc):
	    info = 'Shell command "%s" killed with signal %d' \
		    %(cmd, os.WTERMSIG(rc))
	if os.WEXITSTATUS(rc):
	    info = 'Shell command "%s" exited with exit code %d' \
		    %(cmd, os.WEXITSTATUS(rc))
        log.error(info)
	raise RuntimeError, info


# string extensions

def find(s, subs, start=0):
    ret = -1
    found = None
    for sub in subs:
	this = string.find(s, sub, start)
	if this > -1 and ( ret < 0 or this < ret):
	    ret = this
	    found = s[this:this+1]
    return (ret, found)


# shutil module extensions, with {}-expansion and globbing

def braceExpand(path):
    obrace = string.find(path, "{")
    if obrace < 0:
	return [path]

    level=1
    pathlist = []
    h = obrace
    while level:
	(h, it) = find(path, "{}", h)
	if h < 0:
	    raise ValueError, 'path %s has unbalanced {}' %path
	if it == "{":
	    level = level + 1
	    obrace = h
	else:
	    segments = path[obrace+1:h].split(',')
	    start = path[:obrace]
	    end = path[h+1:]
	    for segment in segments:
		newbits = braceExpand(start+segment+end)
		for bit in newbits:
		    if not bit in pathlist:
			pathlist.append(bit)
	    return pathlist
	h = h + 1

def braceGlob(paths):
    pathlist = []
    for path in braceExpand(paths):
	pathlist.extend(fixedglob.glob(path))
    return pathlist

def rmtree(paths, ignore_errors=False, onerror=None):
    for path in braceGlob(paths):
	log.debug('deleting [tree] %s', path)
	shutil.rmtree(path, ignore_errors, onerror)

def remove(paths):
    for path in braceGlob(paths):
	if os.path.exists(path) or os.path.islink(path):
	    log.debug('deleting [file] %s', path)
	    os.remove(path)
	else:
	    log.warning('file %s does not exist when attempting to delete [file]', path)

def copyfile(sources, dest):
    for source in braceGlob(sources):
	log.debug('copying %s to %s', source, dest)
	shutil.copy2(source, dest)

def copyfileobj(source, dest):
    shutil.copyfileobj(source, dest, 128 * 1024)

def rename(sources, dest):
    for source in braceGlob(sources):
	log.debug('renaming %s to %s', source, dest)
	os.rename(source, dest)

def copytree(sources, dest, symlinks=False):
    for source in braceGlob(sources):
	if os.path.isdir(source):
	    dest = '%s/%s' %(dest, os.path.basename(source))
	    log.debug('copying [tree] %s to %s', source, dest)
	    shutil.copytree(source, dest, symlinks)
	else:
	    log.debug('copying [file] %s to %s', source, dest)
	    shutil.copy2(source, dest)

def checkPath(binary):
    """
    Examine $PATH to determine if a binary exists

    @todo: expand ~?
    """
    path = os.environ.get('PATH', '')
    for path in path.split(os.pathsep):
        if os.access(os.path.join(path, binary), os.X_OK):
            return True
    return False
