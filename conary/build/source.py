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
Modules used by recipes to find source code, check GPG signatures on
it, unpack it, and patch it in the correct directory.  Each of the
public classes in this module is accessed from a recipe as addI{Name}.
"""

import gzip
import os

from conary.lib import log, magic
from conary.build import lookaside
from conary import rpmhelper
from conary.lib import util
from conary.build import action

class _Source(action.RecipeAction):
    keywords = {'rpm': '',
		'dir': '',
		'keyid': None }

	        
    def __init__(self, recipe, *args, **keywords):
	sourcename = args[0]
	action.RecipeAction.__init__(self, recipe, *args, **keywords)
	self.sourcename = sourcename % recipe.macros
        recipe.sourceMap(self.sourcename)
	self.rpm = self.rpm % recipe.macros
	    
    def doPrep(self):
	if self.keyid:
	    self._addSignature()
	if self.rpm:
	    self._extractFromRPM()

    def doAction(self):
	self.builddir = self.recipe.macros.builddir
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
	    os.system("gpg --no-options --no-secmem-warning --keyserver pgp.mit.edu --recv-keys 0x%s" %self.keyid)
	    if not self._checkKeyID(filepath, self.keyid):
		log.error(self.failedtest)
		raise SourceError, "GPG signature %s failed" %(self.localgpgfile)

    def _checkKeyID(self, filepath, keyid):
	p = util.popen("LANG=C gpg --no-options --logger-fd 1 --no-secmem-warning --verify %s %s"
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
	f = lookaside.searchAll(self.recipe.cfg, self.recipe.laReposCache,
	    self.sourcename, self.recipe.name, self.recipe.srcdirs,
            autoSource=True)
        if f:
            return

        # need to pull from RPM
	r = lookaside.findAll(self.recipe.cfg, self.recipe.laReposCache,
			      self.rpm, self.recipe.name,
			      self.recipe.srcdirs)
	# XXX check signature in RPM package?
	c = lookaside.createCacheName(self.recipe.cfg, self.sourcename,
				      self.recipe.name)
	_extractFilesFromRPM(r, targetfile=c)


    def _findSource(self):
        if self.rpm:
            # the file was pulled at some point from the RPM, and if it
            # has been committed it is in the repository
            return lookaside.findAll(self.recipe.cfg, self.recipe.laReposCache,
                self.sourcename, self.recipe.name, self.recipe.srcdirs,
                autoSource=True)

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
    Called as C{r.addArchive()} from a recipe, this class adds an archive
    such as an optionally compressed tarball or zip file, unpacking it
    into the appropriate directory.
    
    If you provide the C{keyid} argument, it will search for a file
    named I{sourcename}C{.{sig,sign,asc}} and make sure that it is
    signed with the appropriate GPG key.  A missing signature is a
    warning; a failed signature check is fatal.

    By default, C{addArchive} assumes that the archive contains the 
    first directory level in which all the build commands will run,
    called C{%(maindir)s}.  If an archive needs to be unpacked within
    this directory, pass in C{dir=r.macros.maindir} or for a
    subdirectory of maindir, pass in
    C{dir=r.macros.maindir + '/subdir'}
    """

    def __init__(self, recipe, *args, **keywords):
	"""
	@param recipe: The recipe object currently being built.
	    Provided automatically by the PackageRecipe object;
	    do not pass in C{r} from within a recipe.
	@keyword sourcename: The name of the archive
	@keyword rpm: If specified, causes Archive to look in the URL or
	    file specified by C{rpm} for an RPM containing C{sourcename}
        @keyword dir: The directory to change to unpack the sources.
            Relative dirs are relative to C{%(builddir)s}.  Absolute dirs
            are relative to C{%(destdir)s}.
	@keyword keyid: The 8-digit GPG key ID (no leading C{0x}) for the
	    signature.  Indicates that a signature should be sought and
	    checked.
	@keyword use: A Use flag or boolean, or a tuple of Use flags and/or
	    booleans, that determine whether the archive is actually
	    unpacked or merely stored in the archive.
	"""
	_Source.__init__(self, recipe, *args, **keywords)

    def do(self):
	f = self._findSource()
	self._checkSignature(f)
        destDir = action._expandOnePath(self.dir, self.recipe.macros, 
                                        defaultDir=self.builddir)

        guessMainDir = (not self.recipe.explicitMainDir and
                        not self.dir.startswith('/'))

        if guessMainDir:
            before = set(os.listdir(self.builddir))
            if self.recipe.mainDir() in before:
                mainDirPath = '/'.join((self.builddir, self.recipe.mainDir()))
                mainDirBefore = set(os.listdir(mainDirPath))

        util.mkdirChain(destDir)

	if f.endswith(".zip"):
	    util.execute("unzip -q -o -d %s %s" % (destDir, f))

	elif f.endswith(".rpm"):
	    _extractFilesFromRPM(f, directory=destDir)

	else:
            m = magic.magic(f)
            _uncompress = "cat"

            # Question: can magic() ever get these wrong?!
            if isinstance(m, magic.bzip) or f.endswith("bz2"):
                _uncompress = "bzip2 -d -c"
            elif isinstance(m, magic.gzip) or f.endswith("gz") \
                   or f.endswith(".Z"):
                _uncompress = "gzip -d -c"
            
            # There are things we know we know...
            _tarSuffix  = ["tar", "tgz", "tbz2", "taZ",
                           "tar.gz", "tar.bz2", "tar.Z"]
            _cpioSuffix = ["cpio", "cpio.gz", "cpio.bz2"]

            if True in [f.endswith(x) for x in _tarSuffix]:
                _unpack = "tar -C %s -xSf -" % (destDir,)
            elif True in [f.endswith(x) for x in _cpioSuffix]:
                _unpack = "( cd %s ; cpio -iumd --quiet --sparse )" % (destDir,)
            elif _uncompress != 'cat':
                # if we know we've got an archive, we'll default to
                # assuming it's an archive of a tar for now
                # TODO: do something smarter about the contents of the 
                # archive
                _unpack = "tar -C %s -xSf -" % (destDir,)
            else:
                raise SourceError, "unknown archive format: " + f

            util.execute("%s < %s | %s" % (_uncompress, f, _unpack))

        if guessMainDir:
            after = set(os.listdir(self.builddir))
            if self.recipe.mainDir() in before:
                mainDirAfter = set(os.listdir(mainDirPath))
                mainDirDifference = mainDirAfter - mainDirBefore
            else:
                mainDirDifference = set()
            difference = after - before
            oldMainDir = self.recipe.mainDir()
            if len(difference) == 1 and not len(mainDirDifference):
                # Archive produced something outside of mainDir
                # and did not put any contents into mainDir
                candidate = difference.pop()
                if os.path.isdir('%s/%s' %(self.builddir, candidate)):
                    self.recipe.mainDir(candidate)
                else:
                    self.recipe.mainDir(oldMainDir)
            else:
                self.recipe.mainDir(oldMainDir)


class Patch(_Source):
    """
    Called as C{r.addPatch()} from a recipe, this class applies a
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
	    Provided automatically by the PackageRecipe object;
	    do not pass in C{r} from within a recipe.
	@keyword sourcename: The name of the patch file
	@keyword rpm: If specified, causes Archive to look in the URL or
	    file specified by C{rpm} for an RPM containing C{sourcename}
	@keyword dir: The directory to change to before applying the patch.
            Relative dirs are relative to C{%(maindir)s}.  Absolute dirs
            are relative to C{%(destdir)s}.
	@keyword keyid: The 8-digit GPG key ID (no leading C{0x}) for the
	    signature.  Indicates that a signature should be sought and
	    checked.
	@keyword use: A Use flag or boolean, or a tuple of Use flags and/or
	    booleans, that determine whether the archive is actually
	    unpacked or merely stored in the archive.
	@keyword level: The number of initial subdirectory names to strip
	    out when applying the patch; the default is 1.
	@keyword backup: A backup suffix to use for storing the versions
	    of files before the patch is applied.
	@keyword macros: If true, interpolate recipe macros in the body
	    of the patch before applying it.  For example, you might
	    have a patch that changes C{CFLAGS = -O2} to
	    C{CFLAGS = %(cflags)s}, which will cause C{%(cflags)s} to
	    be replaced with the current setting of C{recipe.macros.cflags}.
	    Defaults to False.
	@keyword extraArgs: Arbitrary arguments to pass to the patch program.
	    Use only as a last resort -- and probably also file a bug
	    report suggesting the possibility of direct support.
	"""
	_Source.__init__(self, recipe, *args, **keywords)
	self.applymacros = self.macros

    def do(self):

	f = self._findSource()
	provides = "cat"
	if self.sourcename.endswith(".gz"):
	    provides = "zcat"
	elif self.sourcename.endswith(".bz2"):
	    provides = "bzcat"
	if self.backup:
	    self.backup = '-b -z %s' % self.backup
        defaultDir = os.sep.join((self.builddir, self.recipe.theMainDir))
        destDir = action._expandOnePath(self.dir, self.recipe.macros, 
                                                  defaultDir=defaultDir)
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
    Called as C{r.addSource()} from a recipe, this class copies a file
    into the build directory %(builddir)s or the destination directory
    %(destdir)s.   
    
    If you provide the C{keyid} argument, it will search for a file
    named I{sourcename}C{.{sig,sign,asc}} and make sure that it is
    signed with the appropriate GPG key.  A missing signature is a
    warning; a failed signature check is fatal.
    """

    keywords = {'apply': '',
		'contents': None,
		'macros': False,
		'dest': None,
                'mode': None}


    def __init__(self, recipe, *args, **keywords):


	"""
	@param recipe: The recipe object currently being built.
	    Provided automatically by the PackageRecipe object;
	    do not pass in C{r} from within a recipe.
	@keyword sourcename: The name of the archive
	@keyword rpm: If specified, causes Archive to look in the URL or
	    file specified by C{rpm} for an RPM containing C{sourcename}
	@keyword dir: The directory in which to store the file, relative
	    to C{%(builddir)s}. Absolute directories will be considered
            relative to c{%(destdir)s}. Defaults to storing directly in the
	    C{%(builddir)s}.
	@keyword keyid: The 8-digit GPG key ID (no leading C{0x}) for the
	    signature.  Indicates that a signature should be sought and
	    checked.
	@keyword use: A Use flag or boolean, or a tuple of Use flags and/or
	    booleans, that determine whether the archive is actually
	    unpacked or merely stored in the archive.
	@keyword apply: A command line to run after storing the file.
	    Macros will be interpolated into this command.
	@keyword contents: If specified, provides the contents of the
	    file.  The provided contents will be placed in C{sourcename}.
	@keyword macros: If true, interpolate recipe macros in the body
	    of the patch before applying it.  For example, you might
	    have a patch that changes C{CFLAGS = -O2} to
	    C{CFLAGS = %(cflags)s}, which will cause C{%(cflags)s} to
	    be replaced with the current setting of C{recipe.macros.cflags}.
	    Defaults to False.
        @keyword mode: If set, provides the mode to set on the file.
	@keyword dest: If set, provides the target name of the file in
            the build directory.  A full pathname can be used. Absolute
            directories will be considered relative to c{%(builddir)s},
            but do not specify directory information here as well 
            as in the dir keyword; use one or the other.  Useful mainly 
            when fetching the file from an source outside your direct 
            control, such as a URL to a third-party web site, or copying 
            a file out of an RPM package.
	"""
	_Source.__init__(self, recipe, *args, **keywords)
	if self.dest:
	    # make sure that user did not pass subdirectory in
	    fileName = os.path.basename(self.dest %recipe.macros)
	    if fileName != self.dest:
		if self.dir:
		    self.init_error(RuntimeError, 
				    'do not specify a directory in both dir and'
				    ' dest keywords')
		elif (self.dest % recipe.macros)[-1] == '/':
                    self.dir = self.dest
                    self.dest = os.path.basename(self.sourcename %recipe.macros)
                else:
                    self.dir = os.path.dirname(self.dest % recipe.macros)
                    self.dest = fileName
                    # unfortunately, dir is going to be macro expanded again 
                    # later, make sure any %s in the path name survive
                    self.dir.replace('%', '%%') 
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
	f = self._findSource()

        defaultDir = os.sep.join((self.builddir, self.recipe.theMainDir))
        destDir = action._expandOnePath(self.dir, self.recipe.macros, 
                                                  defaultDir=defaultDir)
        util.mkdirChain(destDir)
        destFile = os.sep.join((destDir, self.dest))
	if self.contents is not None:
	    pout = file(destFile, "w")
	    if self.applymacros:
		pout.write(self.contents %self.recipe.macros)
	    else:
		pout.write(self.contents)
	    pout.close()
	else:
	    if self.applymacros:
		log.debug('applying macros to source %s' %f)
		pin = file(f)
		pout = file(destFile, "w")
                log.debug('copying %s to %s' %(f, destFile))
		pout.write(pin.read()%self.recipe.macros)
		pin.close()
		pout.close()
	    else:
		util.copyfile(f, destFile)
        if self.mode:
            os.chmod(destFile, self.mode)
	if self.apply:
	    util.execute(self.apply %self.recipe.macros, destDir)


class Action(action.RecipeAction):
    """
    Called as C{r.addAction()} from a recipe, this class copies a file
    into the build directory C{%(builddir)s}.
    """

    keywords = {'dir': '' }

    def __init__(self, recipe, *args, **keywords):
	"""
	@param recipe: The recipe object currently being built.
	    Provided automatically by the PackageRecipe object;
	    do not pass in C{r} from within a recipe.
	@keyword action: A command line to run.
	    Macros will be interpolated into this command.
	@keyword dir: The directory in which to store the file, relative
	    to C{%(builddir)s}.  Absolute directories will be considered
            relative to c{%(destdir)s}.  Defaults to storing directly in the
	    C{%(builddir)s}.
	@keyword use: A Use flag or boolean, or a tuple of Use flags and/or
	    booleans, that determine whether the archive is actually
	    unpacked or merely stored in the archive.
	"""
	action.RecipeAction.__init__(self, recipe, *args, **keywords)
	self.action = args[0]

    def do(self):
	builddir = self.recipe.macros.builddir
        defaultDir = os.sep.join((builddir, self.recipe.theMainDir))
        destDir = action._expandOnePath(self.dir, self.recipe.macros, 
                                                  defaultDir)
        util.mkdirChain(destDir)
	util.execute(self.action %self.recipe.macros, destDir)

    def fetch(self): 
	return None

def _extractFilesFromRPM(rpm, targetfile=None, directory=None):
    assert targetfile or directory
    if not directory:
	directory = os.path.dirname(targetfile)
    cpioArgs = ['/bin/cpio', 'cpio', '-iumd', '--quiet']
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
