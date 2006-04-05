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
from conary.build import action, errors

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
    NAME
    ====

    B{C{r.addArchive()}} - Add a source code archive

    SYNOPSIS
    ========

    C{r.addArchive([I{dir},] [I{keyid},] [I{rpm},] [I{sourcename},] [I{use}])}

    DESCRIPTION
    ===========

    The C{r.addArchive()} class adds a source code archive consisting of an
    optionally compressed tarball or zip file, and unpacks it to the proper
    directory.

    KEYWORDS
    ========

    The following keywords are recognized by C{r.addArchive}:

    B{dir} : Instructs C{r.addArchive} to change to the directory
    specified by C{dir} prior to unpacking the source archive.
    An absolute C{dir} value will be considered relative to 
    C{%(destdir)s}, whereas a relative C{dir} value will be
    considered relative to C{%(builddir)s}.

    B{keyid} : Using the C{keyid} keyword indicates the eight-digit
    GNU Privacy Guard (GPG) key ID, without leading C{0x} for the
    source code archive signature should be sought, and checked.
    If you provide the C{keyid} keyword, C{r.addArchive} will
    search for a file named I{sourcenameC{{.sig,sign,asc}}}, and
    ensure it is signed with the appropriate GPG key. A missing signature
    results in a warning; a failed signature check is fatal.

    B{rpm} : If the C{rpm} keyword is used, C{r.addArchive}
    looks in the file or URL specified by C{rpm} for an RPM
    containing C{sourcename}.

    B{sourcename} : The name of the source archive, which may be a
    local filename or a Uniform Resource Locator. (URL)

    B{use} : A Use flag, or boolean, or a tuple of Use flags, and/or
    boolean values which determine whether the source code archive is
    actually unpacked, or merely stored in the archive.

    EXAMPLES
    ========

    The following examples demonstrate invocations of C{r.addArchive}
    from within a recipe:

    C{r.addArchive('initscripts-%(upmajver)s.tar.bz2', rpm=srpm)}

    Demonstrates use with a local source code archive file, and the C{rpm}
    keyword.

    C{r.addArchive('ftp://ftp.visi.com/users/hawkeyd/X/Xaw3d-%(version)s.tar.gz')}

    Demonstrates use with a source code archive accessed via an FTP URL.
    """

    def __init__(self, recipe, *args, **keywords):
	"""
	@param recipe: The recipe object currently being built is provided
        automatically by the C{PackageRecipe} object. Passing in  C{recipe}
        from within a recipe is unnecessary.
    @keyword dir: Instructs C{r.addArchive} to change to the directory
        specified by C{dir} prior to unpacking the source archive. 
        An absolute C{dir} value will be considered relative to 
        C{%(destdir)s}, whereas a relative C{dir} value will be
        considered relative to C{%(builddir)s}.
    @keyword keyid: Using the C{keyid} keyword indicates the eight-digit
        GNU Privacy Guard (GPG) key ID, without leading C{0x} for the
        source code archive signature should be sought, and checked.
        If you provide the C{keyid} keyword, C{r.addArchive} will
        search for a file named I{sourcenameC{{.sig,sign,asc}}} and
        ensure it is signed with the appropriate GPG key. A missing signature
        results in a warning; a failed signature check is fatal.
    @keyword rpm: If the C{rpm} keyword is used, C{r.addArchive} looks in the
        file, or URL specified by C{rpm} for an RPM containing C{sourcename}.
    @keyword sourcename: The name of the source archive, which may be a
        local filename, or a Uniform Resource Locator. (URL)
    @keyword use: A Use flag, or boolean, or a tuple of Use flags, and/or
        boolean values which determine whether the source code archive is
        actually unpacked, or merely stored in the archive.
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
            bd = self.builddir
            join = os.path.join

            before = set(x for x in os.listdir(bd) if os.path.isdir(join(bd, x)))
            if self.recipe.mainDir() in before:
                mainDirPath = '/'.join((bd, self.recipe.mainDir()))
                mainDirBefore = set(os.listdir(mainDirPath))

        util.mkdirChain(destDir)

	if f.endswith(".zip"):
	    util.execute("unzip -q -o -d %s %s" % (destDir, f))

	elif f.endswith(".rpm"):
            log.info("extracting %s into %s" % (f, destDir))
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
                _unpack = "( cd %s ; cpio -iumd --quiet )" % (destDir,)
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
            bd = self.builddir
            after = set(x for x in os.listdir(bd) if os.path.isdir(join(bd, x)))

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
                    os.rmdir('/'.join((self.builddir, oldMainDir)))
                else:
                    self.recipe.mainDir(oldMainDir)
            else:
                self.recipe.mainDir(oldMainDir)


class Patch(_Source):
    """
    NAME
    ====

    B{C{r.addPatch()}} - Add a patch to source code

    SYNOPSIS
    ========

    C{r.addPatch([I{backup},] [I{dir},] [I{extraArgs},] [I{keyid},] [I{level},] [I{macros},] [I{rpm},] [I{sourcename},] [I{use}])}

    DESCRIPTION
    ===========

    The C{r.addPatch()} class adds a patch to be applied to the source code
    during the build phase.

    KEYWORDS
    ========

    The following keywords are recognized by C{r.addPatch}:

    B{backup} : The suffix to use when storing file versions before applying
    the patch.

    B{dir} : Instructs C{r.addPatch} to change to the directory specified by
    C{dir} prior to applying the patch. An absolute C{dir} value will be
    considered relative to C{%(destdir)s}, whereas a relative C{dir} value
    will be considered relative to C{%(builddir)s}.


    B{extraArgs} : As a last resort, arbitrary arguments may be passed to the
    patch program  with the C{extraArgs} keyword. This should not normally be
    required, and is indicative of a possible bug which should be reported
    with the suggestion of direct support for the patch arguments in question.

    B{keyid} : Using the C{keyid} keyword indicates the eight-digit GNU
    Privacy Guard (GPG) key ID, without leading C{0x} for the source code
    archive signature should be sought, and checked. If you provide the
    C{keyid} keyword, {r.addPatch} will search for a file named
    I{sourcenameC{{.sig,sign,asc}}}, and ensure it is signed with the
    appropriate GPG key. A missing signature results in a warning; a failed
    signature check is fatal.

    B{level} : By default, one level of initial subdirectory names is stripped
    out prior to applying the patch. The C{level} keyword allows
    specification of additional initial subdirectory levels to be removed.

    B{macros} : The C{macros} keyword accepts a boolean value, and defaults
    to false. However, if the value of C{macros} is true, recipe macros in the
    body  of the patch will be interpolated before applying the patch. For
    example, a patch which modifies the value C{CFLAGS = -02} using
    C{CFLAGS = %(cflags)s} will update the C{CFLAGS} parameter based upon the
    current setting of C{recipe.macros.cflags}.

    B{rpm} : If the C{rpm} keyword is used, C{Archive}
    looks in the file, or URL specified by C{rpm} for an RPM
    containing C{sourcename}.

    B{sourcename} : The name of the patch file

    B{use} : A Use flag, or boolean, or a tuple of Use flags, and/or
    boolean values which determine whether the source code archive is
    actually unpacked or merely stored in the archive.

    EXAMPLES
    ========

    The following examples demonstrate invocations of C{r.addPatch}
    from within a recipe:

    C{r.addPatch('iptables-1.3.0-no_root.patch')}

    Simple usage of C{r.addPatch} specifying the application of the patch
    C{iptables-1.3.0-no_root.patch}.

    C{r.addPatch('Xaw3d-1.5E-xorg-imake.patch', level=0, dir='lib/Xaw3d')}

    Uses the C{level} keyword specifying that no initial subdirectory names be
    stripped, and a C{dir} keyword, instructing C{r.addPatch} to change to the
    C{lib/Xaw3d} directory prior to applying the patch.
    """
    keywords = {'level': '1',
		'backup': '',
		'macros': False,
		'extraArgs': ''}


    def __init__(self, recipe, *args, **keywords):
	"""
    @param recipe: The recipe object currently being built is provided
        automatically by the PackageRecipe object. Passing in  C{recipe} from
        within a recipe is unnecessary.
    @keyword backup: The suffix to use when storing file versions before
        applying the patch.
    @keyword extraArgs: As a last resort, arbitrary arguments may be passed
        to the patch program  with the C{extraArgs} keyword. This should not
        normally be required, and is indicative of a possible bug which
        should be reported with the suggestion of direct support for the
        patch arguments in question.
    @keyword dir: Instructs C{r.addPatch} to change to the directory
        specified by C{dir} prior to applying the patch. An absolute C{dir}
        value will be considered relative to C{%(destdir)s}, whereas a
        relative C{dir} value will be considered
        relative to C{%(builddir)s}.
    @keyword keyid: Using the C{keyid} keyword indicates the eight-digit GNU
        Privacy Guard (GPG) key ID, without leading C{0x} for the source code
        archive signature should be sought, and checked. If you provide the
        C{keyid} keyword, {r.addPatch} will search for a file named
        I{sourcenameC{{.sig,sign,asc}}}, and ensure it is signed with the
        appropriate GPG key. A missing signature results in a warning; a
        failed signature check is fatal.
    @keyword level: By default, one level of initial subdirectory names is
    stripped out prior to applying the patch.  The C{level} keyword allows
    specification of additional initial subdirectory levels to be removed.
    @keyword macros: The C{macros} keyword accepts a boolean value, and
        defaults to false. However, if the value of C{macros} is true, recipe
        macros in the body  of the patch will be interpolated before applying
        the patch. For example, a patch which modifies the value
        C{CFLAGS = -02} using C{CFLAGS = %(cflags)s} will update the C{CFLAGS}
        parameter based upon the current setting of C{recipe.macros.cflags}.
    @keyword rpm: If the C{rpm} keyword is used, C{Archive} looks in the file,
        or URL specified by C{rpm} for an RPM containing C{sourcename}.
    @keyword sourcename: The name of the patch file
    @keyword use: A Use flag, or boolean, or a tuple of Use flags, and/or
        boolean values which determine whether the source code archive is
        actually unpacked, or merely stored in the archive.
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
	    log.info('applying macros to patch %s' %f)
	    pin = util.popen("%s '%s'" %(provides, f))
	    log.info('patch -d %s -p%s %s %s'
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
    NAME
    ====

    B{C{r.addSource()}} - Copy a file into build or destination directory

    SYNOPSIS
    ========

    C{r.addSource([I{keyid},] [I{rpm},] [I{sourcename},] [I{use}])}

    DESCRIPTION
    ===========

    The C{r.addSource()} class copies a file into the build directory or
    destination directory.

    KEYWORDS
    ========

    The following keywords are recognized by C{r.addSource}:

    B{apply} : A command line to run after storing the file. Macros will be
    interpolated into this command.

    B{dest} : If set, provides the target name of the file in the build
    directory. A full pathname can be used. Use either B{dir}, or B{dest} to
    specify directory information, but not both. Useful mainly  when fetching
    the file from an source outside your direct control, such as a URL to a
    third-party web site, or copying a file out of an RPM package.
    An absolute C{dest} value will be considered relative to C{%(destdir)s},     whereas a relative C{dest} value will be considered relative to
    C{%(builddir)s}.
    
    B{dir} : The directory in which to store the file, relative to the build
    directory. An absolute C{dir} value will be considered relative to 
    C{%(destdir)s}, whereas a relative C{dir} value will be considered
    relative to C{%(builddir)s}. Defaults to storing file directly in the
    build directory.

    B{keyid} : Using the C{keyid} keyword indicates the eight-digit
    GNU Privacy Guard (GPG) key ID, without leading C{0x} for the
    source code archive signature should be sought, and checked.
    If you provide the C{keyid} keyword, C{r.addArchive} will
    search for a file named I{sourcename}C{{.sig,sign,asc}}, and
    ensure it is signed with the appropriate GPG key. A missing signature
    results in a warning; a failed signature check is fatal.

    B{macros} : If True, interpolate recipe macros in the body of a patch
    before applying it.  For example, you might have a patch that changes
    C{CFLAGS = -O2} to C{CFLAGS = %(cflags)s}, which will cause C{%(cflags)s}
    to be replaced with the current setting of C{recipe.macros.cflags}.
    Defaults to False.

    B{mode}: If set, provides the mode to set on the file.

    B{rpm} : If the C{rpm} keyword is used, C{Archive} looks in the file, or
    URL specified by C{rpm} for an RPM containing C{sourcename}.

    B{sourcename} : The name of the file

    B{use} : A Use flag or boolean, or a tuple of Use flags and/or booleans,
    that determine whether the archive is actually unpacked or merely stored
    in the archive.

    EXAMPLES
    ========

    The following examples demonstrate invocations of C{r.addSource}
    from within a recipe:

    C{r.addSource('usbcam.console')}

    The example above is a typical, simple invocation of C{r.addSource()}
    which adds the file C{usbcam.console} directly to the build directory.

    C{r.addSource('pstoraster' , rpm=srpm, dest='pstoraster.new')}

    The above example of C{r.addSource} specifies the file C{pstoraster} is
    to be sought in a source RPM file, and is to be added to the build
    directory as C{pstoraster.new}.
    """

    keywords = {'apply': '',
		'contents': None,
		'macros': False,
		'dest': None,
                'mode': None}


    def __init__(self, recipe, *args, **keywords):


	"""
	@param recipe: The recipe object currently being built is provided
        automatically by the PackageRecipe object. Passing in C{recipe} from
        within a recipe is unnecessary.
    @keyword dest: If set, provides the target name of the file in the build
        directory. A full pathname can be used. Use either B{dir}, or 
        B{dest} to specify directory information, but not both. Useful mainly        when fetching the file from an source outside your direct control,
        such as a URL to a third-party web site, or copying a file out of an         RPM package. An absolute C{dest} value will be considered relative to        C{%(destdir)s}, whereas a relative C{dest} value will be considered
        relative to C{%(builddir)s}.
    @keyword dir: The directory in which to store the file, relative to
        the build directory. An absolute C{dir} value will be considered
        relative to C{%(destdir)s}, whereas a relative C{dir} value will be
        considered relative to C{%(builddir)s}. Defaults to storing file
        directly in the build directory.
    @keyword keyid: Using the C{keyid} keyword indicates the eight-digit GNU
        Privacy Guard (GPG) key ID, without leading C{0x} for the source code
        archive signature should be sought, and checked. If you provide the
        C{keyid} keyword, C{r.addArchive} will search for a file named
        I{sourcename}C{{.sig,sign,asc}}, and ensure it is signed with the
        appropriate GPG key. A missing signature results in a warning; a
        failed signature check is fatal.
    @keyword macros: If True, interpolate recipe macros in the body of a
        patch before applying it.  For example, you might have a patch that
        changes C{CFLAGS = -O2} to C{CFLAGS = %(cflags)s}, which will cause
        C{%(cflags)s} to be replaced with the current setting of
        C{recipe.macros.cflags}. Defaults to False.
    @keyword mode: If set, provides the mode to set on the file.
        use : A Use flag, or boolean, or a tuple of Use flags, and/or boolean
        values which determine whether the source code archive is actually
        unpacked, or merely stored in the archive.
    @keyword rpm: If the C{rpm} keyword is used, C{Archive} looks in the file,
        or URL specified by C{rpm} for an RPM containing C{sourcename}.
    @keyword sourcename: The name of the file
    @keyword use: A Use flag or boolean, or a tuple of Use flags and/or
        booleans, that determine whether the archive is actually unpacked or
        merely stored in the archive.
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
		log.info('applying macros to source %s' %f)
		pin = file(f)
		pout = file(destFile, "w")
                log.info('copying %s to %s' %(f, destFile))
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
    NAME
    ====

    B{C{r.addAction()}} - Performs the specified shell command in the first
    pass of the build (similar to r.Run() but performed during the prep stage).

    SYNOPSIS
    ========

    C{r.addAction([I{action},] [I{dir},] [I{use},])}

    DESCRIPTION
    ===========

    The C{r.addAction()} class copies an arbitrary file into the build
    directory C{%(builddir)s}.

    KEYWORDS
    ========

    The following keywords are recognized by C{r.addAction}:

    B{action} :  A command line which will be executed with macro
    interpolation.

    B{dir} : Specify the directory where the file is to be located, relative
    to C{%(builddir)s}. By default, C{r.addAction} stores the file directly
    in C{%(builddir)s}. If an absolute directory is specified, it will be
    considered relative to C{%(builddir)s}.

    B{use} : A Use flag, or boolean, or a tuple of Use flags, and/or
    boolean values which determine whether the source code archive is
    actually unpacked or merely stored in the archive.

    EXAMPLES
    ========

    The following examples demonstrate invocations of C{r.addAction}
    from within a recipe:

    C{r.addAction('sed -i "s/^SUBLEVEL.*/SUBLEVEL = %(sublevel)s/" Makefile')}

    Demonstrates use of a command line with macro interpolation upon the file
    C{Makefile}.

    C{r.addAction('mv lib/util/shhopt.h lib/util/pbmshhopt.h')}

    Demonstrates renaming a file via the C{mv} command.
    """

    keywords = {'dir': '' }

    def __init__(self, recipe, *args, **keywords):
	"""
	@param recipe: The recipe object currently being built is provided
        automatically by the PackageRecipe object. Passing in  C{recipe} from
        within a recipe is unnecessary.
    @keyword action:  A command line which will be executed with macro
        interpolation.
    @keyword dir: Specify the directory where the file is to be located,
        relative to C{%(builddir)s}. By default, C{r.addAction} stores the
        file directly in C{%(builddir)s}. If an absolute directory is
        specified, it will be considered relative to C{%(builddir)s}.
    @keyword use: A Use flag, or boolean, or a tuple of Use flags, and/or
        boolean values which determine whether the source code archive is
        actually unpacked or merely stored in the archive.
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
    h = rpmhelper.readHeader(r)
    # assume compression is gzip unless specifically tagged as bzip2
    decompressor = lambda fobj: gzip.GzipFile(fileobj=fobj)
    if h.has_key(rpmhelper.PAYLOADCOMPRESSOR):
        compression = h[rpmhelper.PAYLOADCOMPRESSOR]
        if compression == 'bzip2':
            decompressor = lambda fobj: util.BZ2File(fobj)
    # rewind the file to let seekToData do its job
    r.seek(0)
    rpmhelper.seekToData(r)
    uncompressed = decompressor(r)
    (rpipe, wpipe) = os.pipe()
    pid = os.fork()
    if not pid:
        os.close(wpipe)
	os.dup2(rpipe, 0)
	os.chdir(directory)
	os.execl(*cpioArgs)
	os._exit(1)
    os.close(rpipe)
    while 1:
        buf = uncompressed.read(4096)
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



class SourceError(errors.CookError):
    """
    Base class from which source error classes inherit
    """
    def __init__(self, msg, *args):
        self.msg = msg %args

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
