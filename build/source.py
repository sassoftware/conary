#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Modules used by recipes to find source code, check GPG signatures on
it, unpack it, and patch it in the correct directory.  Each of the
public classes in this module is accessed from a recipe as addI{Name}.
"""

import gzip
import log
import lookaside
import os
import rpmhelper
import util
import action
import string

class _Source(action.RecipeAction):
    keywords = {'rpm': '',
		'dir': '',
		'keyid': None }

	        
    def __init__(self, recipe, *args, **keywords):
	sourcename = args[0]
	action.RecipeAction.__init__(self, recipe, *args, **keywords)
	self.sourcename = sourcename % recipe.macros
	self.rpm = self.rpm % recipe.macros
	self.prep()
	    
    def prep(self):
	if self.keyid:
	    self._addSignature()
	if self.rpm:
	    self._extractFromRPM()

    def doAction(self):
	self.builddir = self.recipe.macros.builddir
	self.dir = self.dir % self.recipe.macros
	action.RecipeAction.doAction(self)

    def _addSignature(self):
        for suffix in ('sig', 'sign', 'asc'):
            self.gpg = '%s.%s' %(self.sourcename, suffix)
            self.localgpgfile = lookaside.searchAll(self.recipe.cfg,
				    self.recipe.laReposCache, self.gpg, 
                                    self.recipe.name, self.recipe.srcdirs)
	    if self.localgpgfile:
		return
	if not self.localgpgfile:
	    log.warning('No GPG signature file found for %s', self.sourcename)
	    del self.localgpgfile

    def _checkSignature(self, filepath):
	if 'localgpgfile' not in self.__dict__:
	    return
        if not util.checkPath("gpg"):
            return
	# FIXME: our own keyring
	if not self._checkKeyID(filepath, self.keyid):
	    # FIXME: only do this if key missing, this is cheap for now
	    os.system("gpg --no-secmem-warning --keyserver pgp.mit.edu --recv-keys 0x%s")
	    if not self._checkKeyID(filepath, self.keyid):
		log.error(self.failedtest)
		raise SourceError, "GPG signature %s failed" %(self.localgpgfile)

    def _checkKeyID(self, filepath, keyid):
	p = util.popen("gpg --logger-fd 1 --no-secmem-warning --verify %s %s"
		      %(self.localgpgfile, filepath))
	result = p.read()
	found = result.find("key ID %s" % keyid)
	if found == -1:
	    self.failedtest = result
	    return False
	return True

    def _extractFromRPM(self):
        """
        Extracts filename from rpm file and creates an entry in the
        source lookaside cache for the extracted file
        """
	# XXX check signature in RPM package?
	r = lookaside.findAll(self.recipe.cfg, self.recipe.laReposCache,
			      self.rpm, self.recipe.name,
			      self.recipe.srcdirs)
	c = lookaside.createCacheName(self.recipe.cfg, self.sourcename,
				      self.recipe.name)
	_extractFilesFromRPM(r, targetfile=c)


    def _findSource(self):
	if self.rpm:
	    # the file was already unpacked from the RPM, and so it
	    # must be in the lookaside cache.
	    return lookaside.searchCache(self.recipe.cfg,self.sourcename,
				         self.recipe.name)
	# otherwise, we use a standard search that looks in the repository
	# first
	return lookaside.findAll(self.recipe.cfg, self.recipe.laReposCache,
	    self.sourcename, self.recipe.name, self.recipe.srcdirs)

    def fetch(self):
	if 'sourcename' not in self.__dict__:
	    return None
	f = lookaside.findAll(self.recipe.cfg, self.recipe.laReposCache,
			      self.sourcename, self.recipe.name,
			      self.recipe.srcdirs)
	self._checkSignature(f)
	return f

    def do(self):
	raise NotImplementedError


class Archive(_Source):
    """
    Called as self.addArchive from a recipe, this class adds an archive
    such as an optionally compressed tarball or zip file, unpacking it
    into the appropriate directory.
    
    If you provide the C{keyid} argument, it will search for a file
    named I{sourcename}C{.{sig,sign,asc}} and make sure that it is
    signed with the appropriate GPG key.  A missing signature is a
    warning; a failed signature check is fatal.

    FIXME: must fix the rules for directories, then explain here.
    """

    def __init__(self, recipe, *args, **keywords):
	"""
	@param recipe: The recipe object currently being built.
	@param sourcename: The name of the archive
	@param rpm: If specified, causes Archive to look in the URL or
	    file specified by C{rpm} for an RPM containing C{sourcename}
	@param dir: FIXME: need to make directory handling more sensible,
	    then describe it
	@param keyid: The 8-digit GPG key ID (no leading C{0x}) for the
	    signature.  Indicates that a signature should be sought and
	    checked.
	@param use: A Use flag or boolean, or a tuple of Use flags and/or
	    booleans, that determine whether the archive is actually
	    unpacked or merely stored in the archive.
	"""
	_Source.__init__(self, recipe, *args, **keywords)

    def do(self):
	f = self._findSource()
	self._checkSignature(f)

	if self.dir:
	    destdir = '%s/%s' % (self.builddir, self.dir)
	    util.mkdirChain(destdir)
	else:
	    destdir = self.builddir

	if f.endswith(".zip"):
	    util.execute("unzip -q -o -d %s %s" % (destdir, f))
	    return

	if f.endswith(".rpm"):
	    _extractFilesFromRPM(r, directory=destdir)
	    return

	if f.endswith(".bz2") or f.endswith(".tbz2"):
	    tarflags = "-jxf"
	elif f.endswith(".gz") or f.endswith(".tgz"):
	    tarflags = "-zxf"
	else:
	    raise SourceError, "unknown archive compression"
	util.execute("tar -C %s %s %s" % (destdir, tarflags, f))


class Patch(_Source):
    """
    Called as self.addPatch from a recipe, this class applies a
    patch.
    
    If you provide the C{keyid} argument, it will search for a file
    named I{sourcename}C{.{sig,sign,asc}} and make sure that it is
    signed with the appropriate GPG key.  A missing signature is a
    warning; a failed signature check is fatal.
    """
    keywords = {'level': '1',
		'backup': '',
		'macros': False,
		'extraArgs': ''}


    def __init__(self, recipe, *args, **keywords):
	"""
	@param recipe: The recipe object currently being built.
	@param sourcename: The name of the patch file
	@param rpm: If specified, causes Archive to look in the URL or
	    file specified by C{rpm} for an RPM containing C{sourcename}
	@param dir: The directory relative to C{%(builddir)s} to which
	    to change before applying the patch.
	@param keyid: The 8-digit GPG key ID (no leading C{0x}) for the
	    signature.  Indicates that a signature should be sought and
	    checked.
	@param use: A Use flag or boolean, or a tuple of Use flags and/or
	    booleans, that determine whether the archive is actually
	    unpacked or merely stored in the archive.
	@param level: The number of initial subdirectory names to strip
	    out when applying the patch; the default is 1.
	@param backup: A backup suffix to use for storing the versions
	    of files before the patch is applied.
	@param macros: If true, interpolate recipe macros in the body
	    of the patch before applying it.  For example, you might
	    have a patch that changes C{CFLAGS = -O2} to
	    C{CFLAGS = %(cflags)s}, which will cause C{%(cflags)s} to
	    be replaced with the current setting of C{recipe.macros.cflags}.
	    Defaults to False.
	@param extraArgs: Arbitrary arguments to pass to the patch program.
	    Use only as a last resort -- and probably also file a bug
	    report suggesting the possibility of direct support.
	"""
	_Source.__init__(self, recipe, *args, **keywords)
	self.applymacros = self.macros

    def do(self):
	destDir = os.sep.join((self.builddir, self.recipe.theMainDir))
	util.mkdirChain(destDir)

	f = self._findSource()
	provides = "cat"
	if self.sourcename.endswith(".gz"):
	    provides = "zcat"
	elif self.sourcename.endswith(".bz2"):
	    provides = "bzcat"
	if self.backup:
	    self.backup = '-b -z %s' % self.backup
	if self.dir:
	    destDir = os.sep.join((destDir, self.dir))
	    util.mkdirChain(destDir)
	if self.applymacros:
	    log.debug('applying macros to patch %s' %f)
	    pin = util.popen("%s '%s'" %(provides, f))
	    log.debug('patch -d %s -p%s %s %s'
		      %(destDir, self.level, self.backup, self.extraArgs))
	    pout = util.popen('patch -d %s -p%s %s %s'
		              %(destDir, self.level, self.backup,
			        self.extraArgs), 'w')
	    pout.write(pin.read()%self.recipe.macros)
	    pin.close()
	    pout.close()
	else:
	    util.execute("%s '%s' | patch -d %s -p%s %s %s"
			 %(provides, f, destDir, self.level, self.backup,
			   self.extraArgs))


class Source(_Source):
    """
    Called as self.addSource from a recipe, this class copies a file
    into the build directory %(builddir)s.
    
    If you provide the C{keyid} argument, it will search for a file
    named I{sourcename}C{.{sig,sign,asc}} and make sure that it is
    signed with the appropriate GPG key.  A missing signature is a
    warning; a failed signature check is fatal.
    """

    keywords = {'apply': '',
		'contents': None,
		'macros': False,
		'dest': None}


    def __init__(self, recipe, *args, **keywords):


	"""
	@param recipe: The recipe object currently being built.
	@param sourcename: The name of the archive
	@param rpm: If specified, causes Archive to look in the URL or
	    file specified by C{rpm} for an RPM containing C{sourcename}
	@param dir: The directory in which to store the file, relative
	    to C{%(builddir)s}.  Defaults to storing directly in the
	    C{%(builddir)s}.
	@param keyid: The 8-digit GPG key ID (no leading C{0x}) for the
	    signature.  Indicates that a signature should be sought and
	    checked.
	@param use: A Use flag or boolean, or a tuple of Use flags and/or
	    booleans, that determine whether the archive is actually
	    unpacked or merely stored in the archive.
	@param apply: A command line to run after storing the file.
	    Macros will be interpolated into this command.
	@param contents: If specified, provides the contents of the
	    file.  The provided contents will be placed in C{sourcename}.
	@param macros: If true, interpolate recipe macros in the body
	    of the patch before applying it.  For example, you might
	    have a patch that changes C{CFLAGS = -O2} to
	    C{CFLAGS = %(cflags)s}, which will cause C{%(cflags)s} to
	    be replaced with the current setting of C{recipe.macros.cflags}.
	    Defaults to False.
	@param dest: If set, provides the name of the file in the build
	    directory.  Do not include any subdirectories; use C{dir}
	    instead for subdirectories.  Useful mainly when fetching
	    the file from an source outside your direct control, such as
	    a URL to a third-party web site, or copying a file out of an
	    RPM package.
	"""
	_Source.__init__(self, recipe, *args, **keywords)
	if self.dest:
	    # make sure that user did not pass subdirectory in
	    self.dest = os.path.basename(self.dest %recipe.macros)
	else:
	    self.dest = os.path.basename(self.sourcename %recipe.macros)
	if self.contents is not None:
	    # Do not look for a file that does not exist...
	    self.sourcename = ''
	if self.macros:
	    self.applymacros = True
	else:
	    self.applymacros = False

    def do(self):
	destDir = os.sep.join((self.builddir, self.recipe.theMainDir))
	util.mkdirChain(destDir)

	f = self._findSource()
	if self.dir:
	    destDir = os.sep.join((destDir, self.dir))
	    util.mkdirChain(destDir)
	if self.contents is not None:
	    pout = file(os.sep.join((destDir, self.dest)), "w")
	    if self.applymacros:
		pout.write(self.contents %self.recipe.macros)
	    else:
		pout.write(self.contents)
	    pout.close()
	else:
	    if self.applymacros:
		log.debug('applying macros to source %s' %f)
		pin = file(f)
		pout = file(os.sep.join((destDir, self.dest)), "w")
		pout.write(pin.read()%self.recipe.macros)
		pin.close()
		pout.close()
	    else:
		util.copyfile(f, os.sep.join((destDir, self.dest)))
	if self.apply:
	    util.execute(self.apply %self.recipe.macros, destDir)


class Action(action.RecipeAction):
    """
    Called as self.addAction from a recipe, this class copies a file
    into the build directory %(builddir)s.
    """

    keywords = {'dir': '' }

    def __init__(self, recipe, *args, **keywords):
	"""
	@param recipe: The recipe object currently being built.
	@param action: A command line to run.
	    Macros will be interpolated into this command.
	@param dir: The directory in which to store the file, relative
	    to C{%(builddir)s}.  Defaults to storing directly in the
	    C{%(builddir)s}.
	@param use: A Use flag or boolean, or a tuple of Use flags and/or
	    booleans, that determine whether the archive is actually
	    unpacked or merely stored in the archive.
	"""
	action.RecipeAction.__init__(self, recipe, *args, **keywords)
	self.action = args[0]

    def do(self):
	self.builddir = self.recipe.macros.builddir
	self.dir = self.dir % self.recipe.macros
	destDir = os.sep.join((self.builddir, self.recipe.theMainDir))
	util.mkdirChain(destDir)
	if self.dir:
	    destDir = os.sep.join((destDir, self.dir))
	    util.mkdirChain(destDir)
	util.execute(self.action %self.recipe.macros, destDir)

    def fetch(self): 
	return None

def _extractFilesFromRPM(rpm, targetfile=None, directory=None):
    assert targetfile or directory
    if not directory:
	directory = os.path.dirname(targetfile)
    cpioArgs = ['/bin/cpio', 'cpio', '-ium', '--quiet']
    if targetfile:
	filename = os.path.basename(targetfile)
	cpioArgs.append(filename)
	errorMessage = 'extracting %s from RPM %s' %(
	    filename, os.path.basename(rpm))
    else:
	errorMessage = 'extracting RPM %s' %os.path.basename(rpm)

    r = file(rpm, 'r')
    rpmhelper.seekToData(r)
    gz = gzip.GzipFile(fileobj=r)
    (rpipe, wpipe) = os.pipe()
    pid = os.fork()
    if not pid:
	os.dup2(rpipe, 0)
	os.chdir(directory)
	os.execl(*cpioArgs)
	os._exit(1)
    while 1:
	buf = gz.read(4096)
	if not buf:
	    break
	os.write(wpipe, buf)
    os.close(wpipe)
    (pid, status) = os.waitpid(pid, 0)
    if not os.WIFEXITED(status):
	raise IOError, 'cpio died %s' %errorMessage
    if os.WEXITSTATUS(status):
	raise IOError, \
	    'cpio returned failure %d %s' %(
		os.WEXITSTATUS(status), errorMessage)
    if targetfile and not os.path.exists(targetfile):
	raise IOError, 'failed to extract source %s from RPM %s' \
		       %(filename, os.path.basename(rpm))



class SourceError(Exception):
    """
    Base class from which source error classes inherit
    """
    def __init__(self, msg, *args):
        self.msg = msg %args

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
