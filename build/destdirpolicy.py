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
Module used by recipes to modify the state of the installed C{%(destdir)s}.
Classes from this module are not used directly; instead, they are accessed
through eponymous interfaces in recipe.

Each policy object is instantiated once per recipe, and each reference to
the policy object's name passes information to the existing instance.
The policy objects do their work in the same order for every recipe.

Not all policy objects are directly used in recipes.  Those that are
provide examples in their summaries.

Most policies can be passed keyword arguments C{exceptions=I{filterexp}}
to remove a file from consideration and C{inclusions=I{filterexp}} to list
a file as explicitly included.  Most policies default to all the files
they would need to apply to, so C{exceptions} is the most common.
"""
import errno
import os
import re
import shutil
import stat

#conary imports
from lib import magic
from lib import util
import macros
import policy

# used in multiple places, should be maintained in one place
# probably needs to migrate to some form of configuration
# need lib and %(lib)s bits for multilib
librarydirs = [
    '%(libdir)s/',
    '%(prefix)s/lib/',
    '%(essentiallibdir)s/',
    '/lib/',
    '%(krbprefix)s/%(lib)s/',
    '%(krbprefix)s/lib/',
    '%(x11prefix)s/%(lib)s/',
    '%(x11prefix)s/lib/',
    '%(prefix)s/local/%(lib)s/',
    '%(prefix)s/local/lib/',
]
# now uniq for non-multilib systems
librarydirs = sorted({}.fromkeys(librarydirs).keys())

class TestSuiteLinks(policy.Policy):
    """
    Indicate extra files to link into the test directory:
    C{r.TestSuiteLinks(I{%(thisdocdir)s/README})} or 
    C{r.TestSuiteLinks(fileMap={I{<builddir_path>:} I{<destdir_path>}})}.

    Files listed in the first filterexp can override standard exclusions;
    currently, document, man, info, and init directories.  
    Alternatively, a fileMap can be given.  A fileMap is a dictionary where 
    each key is a path to a symlink that will be created in the test directory, 
    and the value pointed to by that key is the path of a file installed 
    in the destination directory that the symlink should point to.  

    The C{r.TestSuiteLinks()} command is only useful if you have indicated 
    to conary that you wish to create a test suite.  To create a test suite, 
    you should run the C{r.TestSuite()} command, documented in
    conary.build.build.
    """

    # build is an internal interface, used by r.TestSuite to indicate that 
    # TestSuiteLinks should run
    keywords = { 'build': None,
		 'fileMap' : {}
		} 
    invariantexceptions = [ 
			    '%(mandir)s/',
			    '%(infodir)s/',
			    '%(docdir)s/',
			    '%(initdir)s/', 
			    #test dir itself as well 
			    #as anything below it
			    '%(testdir)s',
			    '%(testdir)s/' ]

    buildTestSuite = None

    def updateArgs(self, *args, **keywords):
	# XXX add fileMap param, to map from builddir -> destdir files
	# could update automatically when we install a file?
	# but then we remove/move a file after installing,
	# we'd have to update map
	build = keywords.pop('build', None)
	if build is not None:
	    self.buildTestSuite = build

	fileMap = keywords.pop('fileMap', None)
	if fileMap is not None:
	    self.fileMap.update(fileMap)
	policy.Policy.updateArgs(self, *args, **keywords)

    def do(self):
	if not self.buildTestSuite:
	    self.recipe.TestSuiteFiles(build=False)
	    return
	
	# expand macros in fileMap
	newFileMap = {}
	for (buildfile, destfile) in self.fileMap.iteritems():
	    newFileMap[util.normpath(buildfile % self.macros)] = destfile % self.macros
	self.fileMap = newFileMap

	self.builddirfiles = {}
	self.builddirlinks = {} 
	builddir = self.macros.builddir
	builddirlen = len(builddir)
	for root, dirs, files in os.walk(builddir): 
	    for file in files:
		realDir = root[builddirlen:]
		realPath = os.path.join(realDir, file)
		if realPath in self.fileMap:
		    continue
		fullpath = os.path.join(root, file)

		if os.path.islink(fullpath):
		    # symlink handling:
		    # change to absolute link and add to symlink list
		    contents = os.readlink(fullpath)
		    if contents[0] != '/':
			contents = util.normpath(os.path.join(root, contents))[builddirlen:]
		    if contents not in self.builddirlinks:
			self.builddirlinks[contents] = []
		    self.builddirlinks[contents].append(os.path.join(realDir, file))
		else:
		    # add to regular file list
		    if file not in self.builddirfiles:
			self.builddirfiles[file] = []
		    self.builddirfiles[file].append(realDir)

	
	if self.buildTestSuite:
	    for (buildfile, destfile) in self.fileMap.iteritems():
		target = destfile
		link = util.normpath('%(destdir)s%(thistestdir)s/' % self.macros + buildfile)
		util.mkdirChain(os.path.dirname(link))
		os.symlink(target, link)

	    self.recipe.TestSuiteFiles(build=True)
	    self.recipe.TestSuiteFiles(builddirlinks=self.builddirlinks)
	    policy.Policy.do(self)
	else:
	    self.recipe.TestSuiteFiles(build=False)
	    return


    def doFile(self, path):
	fullpath = self.macros.destdir + path
        if os.path.islink(fullpath):
            return

	fileName = os.path.basename(path)
	if fileName in self.builddirfiles:
	    dirName = self.findRightFile(fullpath, fileName, 
			 self.builddirfiles[fileName])
	    if dirName is None:
		return

	    # if destdir file eq to symlink in builddir
	    if os.path.islink(self.macros.builddir + dirName + os.sep 
			       + fileName):
	        return 

	    testpath = ('%(destdir)s%(thistestdir)s' + os.sep + dirName 
			 + os.sep + fileName) % self.macros
	    util.mkdirChain(os.path.dirname(testpath))

	    if os.path.islink(testpath):
		buildpath = ''.join((self.macros.builddir, dirName, os.sep, fileName))
		if self.betterLink(self.macros.destdir + path, testpath, buildpath):
		    util.remove(testpath)
		    os.symlink(util.normpath(path), testpath)
		else:
		    return
	    else:
		os.symlink(util.normpath(path), testpath)

	    # we've added a builddir file to the testdir, 
	    # see if there was a symlink in the builddir 
	    # that pointed to it
	    builddirFile = os.path.join(dirName, fileName)
	    if builddirFile in self.builddirlinks:
		for path in self.builddirlinks[builddirFile]:
		    linkpath = '%(destdir)s%(thistestdir)s' % self.macros + os.sep + path
		    if not os.path.islink(linkpath):
			util.mkdirChain(os.path.dirname(linkpath))
			os.symlink('%(thistestdir)s/' % self.macros + dirName 
				    + os.sep + fileName, linkpath)


    def betterLink(self, newpath, testpath, buildpath):
	""" 
	betterLink determines whether the destdir file 
	I{newpath} is a better match than the destdir file pointed
	to by the current symlink at I{testpath} for the builddir
	file I{buildpath}.
	"""
	# sample test: duplicates behavior in findRightfile
	#newsize = os.stat(newpath)[stat.ST_SIZE]
	#buildsize = os.stat(buildpath)[stat.ST_SIZE]
	#if(newsize == buildsize):
	#    return True
	#else:
	#    return False
	return False

    def findRightFile(self, fullpath, fileName, dirList):
	"""
	Search for the best match in buildir for the the destdirfile
	fullPath.  Match I{fullpath} against %(builddir)/dir/fileName for each 
	directory in I{dirList}
	"""
	
	# XXX need to cache size/diff info
	exactMatchPossible = True
	builddir = self.macros.builddir
	builddirlen = len(builddir)
	size = os.stat(fullpath)[stat.ST_SIZE]

	fileList = [ ''.join([builddir, dir, os.sep, fileName]) for dir in dirList ]
	newFileList = [ file for file in fileList if size == os.stat(file)[stat.ST_SIZE] ] 
	if len(newFileList) == 1:
	    return os.path.dirname(newFileList[0])[builddirlen:]
	elif len(newFileList) > 1:
	    # narrow down our search and continue looking
	    fileList = newFileList
	else:
	    exactMatchPossible = False

	# okay, there either at least two entries who passed,
	# or no entries who passed -- next heuristic

	if exactMatchPossible: 
	    # we found at least 2 that were the same size -- 
	    # let's try for the more expensive diff
	    # since this should be a very small set of files
	    for file in fileList:
		fd = util.popen('diff %s %s' % (fullpath, file))
		results = fd.read()
		if results == "":
		    return os.path.dirname(file)[builddirlen:]
	
	# I give up: don't make the link because it's probably wrong
	# XXX other, missing tests: magic, close filenames, close sizes
	# destdirlen = len(self.macros.destdir)
        # self.warn('Could not determine which builddir file %s corresponds to for creating test component', fullpath[destdirlen:])
	return None


#XXX this is a builddir policy
class TestSuiteFiles(policy.Policy):
    """
    Indicate extra files to copy into the test directory; 
    C{r.TestSuiteFiles(I{<filterexp>})} - note that this filterexp is relative
    to the build directory, not the install directory as are the rest of
    the destdir policies.

    Files included in the filterexp will be copied into the test directory,
    with their path relative to the build dir retained.  E.g. a file found
    at %(builddir)s/bin/foo will be copied to %(testdir)s/bin/foo.

    The C{r.TestSuiteFiles()} command is only useful if you have indicated 
    to conary that you wish to create a test suite.  To create a test suite, 
    you should run the C{r.TestSuite()} command, documented in
    conary.build.build.
    """

    rootdir = '%(builddir)s'
    buildTestSuite = None

    invariantexceptions = [ ( '.*', stat.S_IFDIR ), ]
    
    invariantinclusions = [ '.*[tT][eE][sS][tT].*',
			    'Makefile.*',
			    '.*\.exp', #used by dejagnu
			    '.*/config.*',
			    '.*/shconfig',
			    '.*/acconfig.*',
			    '.*/aclocal.*',
			    '.*\.la', ]

    keywords = { 'build': None,
		 'builddirlinks' : None} 
    
    def do(self):
	if self.buildTestSuite is False:
	    return
	policy.Policy.do(self)

    def updateArgs(self, *args, **keywords):
	"""
	call as C{TestSuiteFiles(<inclusions>)}.
	@keyword build: If set to true, will create TestSuiteFiles even if a TestSuite command is not given.  If set to false, will not create TestSuiteFiles even if a TestSuite command is given.  Also turns on/off TestSuiteLinkx
	"""
	build = keywords.pop('build', None)
	builddirlinks = keywords.pop('builddirlinks', None)
	if build is not None:
	    self.buildTestSuite = build
	if builddirlinks is not None:
	    self.builddirlinks = builddirlinks
	policy.Policy.updateArgs(self, *args, **keywords)

    def doFile(self, path):
	fullpath = self.macros.builddir + path 
	testpath = self.macros.destdir + self.macros.thistestdir + path 
	if not (os.path.islink(testpath) or os.path.exists(testpath)):
	    if os.path.islink(fullpath):
		contents = os.readlink(fullpath)
		# only install symlinks by default if they point outside of the builddir
		if contents[0] == '/' and not contents.startswith(self.macros.builddir):
		    util.mkdirChain(os.path.dirname(testpath))
		    os.symlink(contents, testpath)
	    elif os.path.isfile(fullpath):
		util.mkdirChain(os.path.dirname(testpath))
		util.copyfile(fullpath, testpath, verbose=False)
		self.replaceBuildPath(testpath)
		# finally, include any symlinks that point
		# to the file we just copied
		if path in self.builddirlinks:
		    for linkpath in self.builddirlinks[path]:
			linkpath = '%(destdir)s%(thistestdir)s' % self.macros + linkpath
			if not os.path.islink(linkpath):
			    util.mkdirChain(os.path.dirname(linkpath))
			    os.symlink(self.macros.thistestdir + path, linkpath)
    
    def replaceBuildPath(self, path):
	#Now remove references to builddir, but
	# we don't want to mess with binaries
	# XXX probbaly need a better check for binary status
	m = magic.magic(path, basedir='/') 
	extension = path.split('.')[-1]
	if m and m.name != 'ltwrapper':
	    return
	if extension in ('pyo', 'pyc'): # add as needed
	    return
	util.execute(("sed -i -e 's|%%(builddir)s|%%(testdir)s/%%(name)s-%%(version)s|g' '%s'" % path) % self.macros, verbose=False)

	
class FixDirModes(policy.Policy):
    """
    Modifies directory permissions that would otherwise prevent
    Conary from packaging C{%(destdir)s} as non-root; not invoked
    from recipes.

    Any directories that do not have user read/write/execute must be
    fixed up now so that we can traverse the tree in following policy,
    packaging, and removing the tree after building.

    This policy must be run first so that other policies can be
    counted on to search the full directory tree.
    """
    # call doFile for all directories that are not readable, writeable,
    # and executable for the user
    invariantinclusions = [ ('.*', stat.S_IFDIR) ]
    invariantexceptions = [ ('.*', 0700) ]

    def doFile(self, path):
        fullpath = self.macros.destdir + path
	mode = os.lstat(fullpath)[stat.ST_MODE]
	self.recipe.AddModes(mode, path)
	os.chmod(fullpath, mode | 0700)


class AutoDoc(policy.Policy):
    """
    Automatically adds likely documentation not otherwise installed;
    exceptions passed in via C{r.AutoDoc(exceptions=I{filterexpression})}
    are evaluated relative to the C{%(builddir)s}, not the
    C{%(destdir)s}.
    """

    rootdir = '%(builddir)s'
    invariantinclusions = [
        '.*/NEWS$',
        r'.*/(LICENSE|COPY(ING|RIGHT))(\.lib|)$',
        '.*/RELEASE-NOTES$',
        '.*/HACKING$',
        '.*/INSTALL$',
        '.*README.*',
        '.*/CHANGES$',
        '.*/TODO$',
        '.*/FAQ$',
        '.*/Change[lL]og.*',
    ]
    invariantexceptions = [ ('.*', stat.S_IFDIR) ]

    def preProcess(self):
        self.builddir = self.recipe.macros.builddir
        self.destdir = util.joinPaths(
            self.recipe.macros.destdir,
            self.recipe.macros.thisdocdir)

    def doFile(self, filename):
        source = util.joinPaths(self.builddir, filename)
        dest = util.joinPaths(self.destdir, filename)
        if os.path.exists(dest):
            return
        if not util.isregular(source):
            # will not be a directory, but might be a symlink or something
            return
        util.mkdirChain(os.path.dirname(dest))
        shutil.copy2(source, dest)
        # this file should not be counted as making package non-empty
        self.recipe._autoCreatedFileCount += 1


class RemoveNonPackageFiles(policy.Policy):
    """
    Remove classes of files that normally should not be packaged;
    C{r.RemoveNonPackageFiles(exceptions=I{filterexpression})}
    allows one of these files to be included in a package.
    """
    invariantinclusions = [
	r'\.la$',
        # python .a's might have been installed in the wrong place on multilib
	r'%(prefix)s/(lib|%(lib)s)/python.*/site-packages/.*\.a$',
	r'perllocal\.pod$',
	r'\.packlist$',
	r'\.cvsignore$',
	r'\.orig$',
        r'%(sysconfdir)s.*/rc[0-6].d/[KS].*$',
	'~$',
        r'.*/\.#.*',
        '(/var)?/tmp/',
    ]

    def doFile(self, path):
	util.remove(self.macros['destdir']+path)

class FixupMultilibPaths(policy.Policy):
    """
    Fix up (and warn) when programs do not know about C{%(lib)s} and they
    are supposed to be installing to C{lib64} but install to C{lib} instead.
    """
    invariantinclusions = [
	'.*\.(so.*|a)$',
    ]

    def __init__(self, *args, **keywords):
	self.dirmap = {
	    '/lib':            '/%(lib)s',
	    '%(prefix)s/lib':  '%(libdir)s',
	}
	self.invariantsubtrees = self.dirmap.keys()
	policy.Policy.__init__(self, *args, **keywords)

    def test(self):
	if self.macros['lib'] == 'lib':
	    # no need to do anything
	    return False
	for d in self.invariantsubtrees:
	    self.dirmap[d %self.macros] = self.dirmap[d] %self.macros
	return True

    def doFile(self, path):
	destdir = self.macros.destdir
        fullpath = util.joinPaths(destdir, path)
        mode = os.lstat(fullpath)[stat.ST_MODE]
	m = self.recipe.magic[path]
	if stat.S_ISREG(mode) and (
            not m or (m.name != "ELF" and m.name != "ar")):
            self.warn("non-object file with library name %s", path)
	    return
	basename = os.path.basename(path)
        currentsubtree = self.currentsubtree % self.macros
	targetdir = self.dirmap[currentsubtree]
        # we want to append whatever path came after the currentsubtree -
        # e.g. if the original path is /usr/lib/subdir/libfoo.a, 
        # we still need to add the /subdir/
        targetdir += os.path.dirname(path[len(currentsubtree):])
	target = util.joinPaths(targetdir, basename)
        fulltarget = util.joinPaths(destdir, target)
        if os.path.exists(fulltarget):
            tmode = os.lstat(fulltarget)[stat.ST_MODE]
            tm = self.recipe.magic[target]
            if (not stat.S_ISREG(mode) or not stat.S_ISREG(tmode)):
                # one or both might be symlinks, in which case we do
                # not want to touch this
                return
            if ('abi' in m.contents and 'abi' in tm.contents 
                and m.contents['abi'] != tm.contents['abi']):
                # path and target both exist and are of different abis.
                # This means that this is actually a multilib package
                # that properly contains both lib and lib64 items,
                # and we shouldn't try to fix them.
                return
	    raise DestdirPolicyError(
		"Conflicting library files %s and %s installed" %(
		    path, target))
        self.warn('file %s found in wrong directory, attempting to fix...',
                  path)
        util.mkdirChain(destdir + targetdir)
        if stat.S_ISREG(mode):
            util.rename(destdir + path, fulltarget)
        else:
            # we should have a symlink that may need the contents changed
            contents = os.readlink(fullpath)
            if contents.find('/') == -1:
                # simply rename
                util.rename(destdir + path, destdir + target)
            else:
                # need to change the contents of the symlink to point to
                # the new location of the real file
                contentdir = os.path.dirname(contents)
                contenttarget = os.path.basename(contents)
                olddir = os.path.dirname(path)
                if contentdir.startswith('/'):
                    # absolute path
                    if contentdir == olddir:
                        # no need for a path at all, change to local relative
                        os.symlink(contenttarget, destdir + target)
                        os.remove(fullpath)
                        return
                if not contentdir.startswith('.'):
                    raise DestdirPolicyError(
                        'Multilib: cannot fix relative path %s in %s -> %s\n'
                        'Library files should be in %s'
                        %(contentdir, path, contents, targetdir))
                # now deal with ..
                # first, check for relative path that resolves to same dir
                i = contentdir.find(olddir)
                if i != -1:
                    dotlist = contentdir[:i].split('/')
                    dirlist = contentdir[i+1:].split('/')
                    if len(dotlist) == len(dirlist):
                        # no need for a path at all, change to local relative
                        os.symlink(contenttarget, destdir + target)
                        os.remove(fullpath)
                        return
                raise DestdirPolicyError(
                        'Multilib: cannot fix relative path %s in %s -> %s\n'
                        'Library files should be in %s'
                        %(contentdir, path, contents, targetdir))

class ExecutableLibraries(policy.Policy):
    """
    The C{ldconfig} program will complain if libraries do not have have
    executable bits set; this policy changes the mode and warns that
    it has done so.

    Do not invoke C{r.ExecutableLibraries()} from recipes; invoke
    C{r.SharedLibrary(subtrees='/path/to/libraries/')} instead.
    """
    invariantsubtrees = librarydirs
    invariantinclusions = [
	(r'..*\.so\..*', None, stat.S_IFDIR),
    ]
    recursive = False

    # packagepolicy.SharedLibrary will pass in all its arguments,
    # which apply equally to both policies.  They would be one
    # policy if we didn't require this to be in destdirpolicy
    # to touch up permissions, and SharedLibrary to be in
    # packagepolicy to add the "shlib" tag.

    def doFile(self, path):
	fullpath = util.joinPaths(self.macros['destdir'], path)
	if not util.isregular(fullpath):
	    return
	mode = os.lstat(fullpath)[stat.ST_MODE]
	if mode & 0111:
	    # has some executable bit set
	    return
        self.warn('non-executable library %s, changing to mode 0755', path)
	os.chmod(fullpath, 0755)

class ReadableDocs(policy.Policy):
    """
    Documentation should always be world readable
    C{r.ReadableDocs(exceptions=I{filterexp})}
    """
    invariantsubtrees = [
	'%(thisdocdir)s/',
    ]

    def doFile(self, path):
	d = self.macros['destdir']
        fullpath = util.joinPaths(d, path)
	mode = os.lstat(fullpath)[stat.ST_MODE]
	if not mode & 0004:
            mode |= 0044
            isExec = mode & 0111
            if isExec:
                mode |= 0011
            self.warn('non group and world documentation file %s, changing'
                      ' to mode 0%o', path, mode & 07777)
            os.chmod(fullpath, mode)

class Strip(policy.Policy):
    """
    Strips executables and libraries of debugging information.
    May (depending on configuration) save the debugging information
    for future use.
    """
    invariantinclusions = [
	('%(bindir)s/', None, stat.S_IFDIR),
	('%(essentialbindir)s/', None, stat.S_IFDIR),
	('%(sbindir)s/', None, stat.S_IFDIR),
	('%(essentialsbindir)s/', None, stat.S_IFDIR),
	('%(x11prefix)s/bin/', None, stat.S_IFDIR),
	('%(krbprefix)s/bin/', None, stat.S_IFDIR),
	('%(libdir)s/', None, stat.S_IFDIR),
	('%(essentiallibdir)s/', None, stat.S_IFDIR),
        # we need to strip these separately on a multilib system, and
        # on non-multilib systems the multiple listing will be ignored.
	('%(prefix)s/lib/', None, stat.S_IFDIR),
	('/lib/', None, stat.S_IFDIR),
    ]
    invariantexceptions = [
        # let's not recurse...
	'%(debugsrcdir)s/',
	'%(debuglibdir)s/',
    ]

    def __init__(self, *args, **keywords):
	policy.Policy.__init__(self, *args, **keywords)
	self.tryDebuginfo = True

    def updateArgs(self, *args, **keywords):
        self.debuginfo = False
	self.tryDebuginfo = keywords.pop('debuginfo', True)
	policy.Policy.updateArgs(self, *args, **keywords)

    def preProcess(self):
        self.invariantsubtrees = [x[0] for x in self.invariantinclusions]
        # see if we can do debuginfo
        self.debuginfo = False
        # we need this for the debuginfo case
        self.dm = macros.Macros()
        self.dm.update(self.macros)
        # we need to start searching from just below the build directory
        topbuilddir = '/'.join(self.macros.builddir.split('/')[:-1])
        if self.tryDebuginfo and\
           'eu-strip' in self.macros.strip and \
           'debugedit' in self.macros and \
           util.checkPath(self.macros.debugedit) and \
           len(self.macros.debugsrcdir) <= len(topbuilddir):
            self.debuginfo = True
            self.debugfiles = set()
            self.dm.topbuilddir = topbuilddir

    def doFile(self, path):
	m = self.recipe.magic[path]
	if not m:
	    return
	# FIXME: should be:
	#if (m.name == "ELF" or m.name == "ar") and \
	#   m.contents['hasDebug']):
	# but this has to wait until ewt writes debug detection
	# for archives as well as elf files
	if (m.name == "ELF" and m.contents['hasDebug']) or \
	   (m.name == "ar"):
            oldmode = None
            fullpath = self.dm.destdir+path
            mode = os.lstat(fullpath)[stat.ST_MODE]
            if mode & 0600 != 0600:
                # need to be able to read and write the file to strip it
                oldmode = mode
		os.chmod(fullpath, mode|0600)
            if self.debuginfo and m.name == 'ELF' and not path.endswith('.o'):

                dir=os.path.dirname(path)
                b=os.path.basename(path)
                if not b.endswith('.debug'):
                    b += '.debug'

                debuglibdir = '%(destdir)s%(debuglibdir)s' %self.dm +dir
                debuglibpath = util.joinPaths(debuglibdir, b)
                if os.path.exists(debuglibpath):
                    return

                # null-separated AND terminated list, so we need to throw
                # away the last (empty) item before updating self.debugfiles
                self.debugfiles |= set(util.popen(
                    '%(debugedit)s -b %(topbuilddir)s -d %(debugsrcdir)s'
                    ' -l /dev/stdout '%self.dm
                    +fullpath).read().split('\x00')[:-1])
                util.mkdirChain(debuglibdir)
                util.execute('%s -f %s %s' %(
                    self.dm.strip, debuglibpath, fullpath))

            else:
                if m.name == 'ar' or path.endswith('.o'):
                    # just in case strip is eu-strip, which segfaults
                    # whenever it touches an ar archive, and seems to 
                    # break some .o files
                    util.execute('%(strip-archive)s ' %self.dm +fullpath)
                else:
                    util.execute('%(strip)s ' %self.dm +fullpath)

            del self.recipe.magic[path]
            if oldmode is not None:
                os.chmod(fullpath, oldmode)

    def postProcess(self):
        if self.debuginfo:
            for file in sorted(self.debugfiles):
                builddirpath = '%(topbuilddir)s/' % self.dm +file
                dir = os.path.dirname(file)
                util.mkdirChain('%(destdir)s%(debugsrcdir)s/'%self.dm +dir)
                try:
                    shutil.copy2(builddirpath,
                                 '%(destdir)s%(debugsrcdir)s/'%self.dm +file)
                except IOError, msg:
                    if msg.errno == errno.ENOENT:
                        pass

class NormalizeCompression(policy.Policy):
    """
    Compresses files with maximum compression and without data that can
    change from invocation to invocation;
    C{r.NormalizeCompression(exceptions=I{filterexp}} to disable this
    policy for a file.

    Recompresses .gz files with -9 -n, and .bz2 files with -9, to get maximum
    compression and avoid meaningless changes overpopulating the database.
    Ignores man/info pages, as we get them separately while making other
    changes to man/info pages later.
    """
    invariantexceptions = [
	'%(mandir)s/man.*/',
	'%(infodir)s/',
    ]
    invariantinclusions = [
	('.*\.(gz|bz2)', None, stat.S_IFDIR),
    ]
    def doFile(self, path):
	m = self.recipe.magic[path]
	if not m:
	    return
	p = self.macros.destdir+path
	# XXX if foo and foo.gz both occur, this is bad -- fix it!
	if m.name == 'gzip' and \
	   (m.contents['compression'] != '9' or 'name' in m.contents):
	    util.execute('gunzip %s; gzip -f -n -9 %s' %(p, p[:-3]))
            del self.recipe.magic[path]
	if m.name == 'bzip' and m.contents['compression'] != '9':
	    util.execute('bunzip2 %s; bzip2 -9 %s' %(p, p[:-4]))
            del self.recipe.magic[path]

class NormalizeManPages(policy.Policy):
    """
    Make all man pages follow sane system policy; not called from recipes,
    as there are no exceptions, period.
     - Fix all man pages' contents:
       - remove instances of C{/?%(destdir)s} from all man pages
       - C{.so foo.n} becomes a symlink to foo.n
     - (re)compress all man pages with gzip -f -n -9
     - change all symlinks to point to .gz (if they don't already)
     - make all man pages be mode 644
    """
    def _uncompress(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if name.endswith('.gz') and util.isregular(path):
		util.execute('gunzip ' + dirname + os.sep + name)
	    if name.endswith('.bz2') and util.isregular(path):
		util.execute('bunzip2 ' + dirname + os.sep + name)

    def _touchup(self, dirname, names):
	"""
	remove destdir, fix up modes, ensure that it is legal UTF-8
	"""
	mode = os.lstat(dirname)[stat.ST_MODE]
	if mode & 0777 != 0755:
	    os.chmod(dirname, 0755)
	for name in names:
	    path = dirname + os.sep + name
	    mode = os.lstat(path)[stat.ST_MODE]
            # avoid things like symlinks
            if not stat.S_ISREG(mode):
                continue
	    if mode & 0777 != 0644:
		os.chmod(path, 0644)
            f = file(path, 'r+')
            data = f.read()
            write = False
            try:
                data.decode('utf-8')
            except:
                try:
                    data = data.decode('iso-8859-1').encode('utf-8')
                    write = True
                except:
                    self.error('unable to decode %s as utf-8 or iso-8859-1',
                               path)
            if data.find(self.destdir) != -1:
                write = True
                # I think this is cheaper than using a regexp
                data = data.replace('/'+self.destdir, '')
                data = data.replace(self.destdir, '')

            if write:
                f.seek(0)
                f.truncate(0)
                f.write(data)


    def _sosymlink(self, dirname, names):
	section = os.path.basename(dirname)
	for name in names:
	    path = dirname + os.sep + name
	    if util.isregular(path):
		# if only .so, change to symlink
		f = file(path)
		lines = f.readlines(512) # we really don't need the whole file
		f.close()

		# delete comment lines first
		newlines = []
		for line in lines:
		    # newline means len(line) will be at least 1
		    if len(line) > 1 and not self.commentexp.search(line[:-1]):
			newlines.append(line)
		lines = newlines

		# now see if we have only a .so line to replace
		# only replace .so with symlink if the file exists
		# in order to deal with searchpaths
		if len(lines) == 1:
		    line = lines[0]
		    # remove newline and other trailing whitespace if it exists
		    line = line.rstrip() # chop-chop
		    match = self.soexp.search(line)
		    if match:
			matchlist = match.group(1).split('/')
			l = len(matchlist)
			if l == 1 or matchlist[l-2] == section:
			    # no directory specified, or in the same
			    # directory:
			    targetpath = os.sep.join((dirname, matchlist[l-1]))
			    if os.path.exists(targetpath):
                                self.dbg('replacing %s (%s) with symlink %s',
                                         name, match.group(0),
                                         os.path.basename(match.group(1)))
				os.remove(path)
				os.symlink(os.path.basename(match.group(1)),
					   path)
			else:
			    # either the canonical .so manN/foo.N or an
			    # absolute path /usr/share/man/manN/foo.N
			    # .so is relative to %(mandir)s and the other
			    # man page is in a different dir, so add ../
			    target = "../%s/%s" %(matchlist[l-2],
						  matchlist[l-1])
			    targetpath = os.sep.join((dirname, target))
			    if os.path.exists(targetpath):
                                self.dbg('replacing %s (%s) with symlink %s',
                                          name, match.group(0), target)
				os.remove(path)
				os.symlink(target, path)

    def _compress(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if util.isregular(path):
		util.execute('gzip -f -n -9 ' + dirname + os.sep + name)

    def _gzsymlink(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if os.path.islink(path):
		# change symlinks to .gz -> .gz
		contents = os.readlink(path)
		os.remove(path)
		if not contents.endswith('.gz'):
		    contents = contents + '.gz'
		if not path.endswith('.gz'):
		    path = path + '.gz'
		os.symlink(util.normpath(contents), path)

    def __init__(self, *args, **keywords):
	policy.Policy.__init__(self, *args, **keywords)
	self.soexp = re.compile(r'^\.so (.*\...*)$')
	self.commentexp = re.compile(r'^\.\\"')

    def do(self):
	for manpath in (
	    self.macros.mandir,
	    os.sep.join((self.macros.x11prefix, 'man')),
	    os.sep.join((self.macros.krbprefix, 'man')),
	    ):
	    manpath = self.macros.destdir + manpath
	    self.destdir = self.macros['destdir'][1:] # without leading /
	    # uncompress all man pages
	    os.path.walk(manpath, NormalizeManPages._uncompress, self)
	    # remove '/?%(destdir)s' and fix modes
	    os.path.walk(manpath, NormalizeManPages._touchup, self)
	    # .so foo.n becomes a symlink to foo.n
	    os.path.walk(manpath, NormalizeManPages._sosymlink, self)
	    # recompress all man pages
	    os.path.walk(manpath, NormalizeManPages._compress, self)
	    # change all symlinks to point to .gz (if they don't already)
	    os.path.walk(manpath, NormalizeManPages._gzsymlink, self)

class NormalizeInfoPages(policy.Policy):
    """
    Properly compress info files and remove dir file; only recipe invocation is
    C{r.NormalizeInfoPages(r.macros.infodir+'/dir')} in the one recipe that
    should own the info dir file.
    """
    def do(self):
	dir = self.macros['infodir']+'/dir'
	fsdir = self.macros['destdir']+dir
	if os.path.exists(fsdir):
	    if not self.policyException(dir):
		util.remove(fsdir)
	if os.path.isdir('%(destdir)s/%(infodir)s' %self.macros):
	    infofiles = os.listdir('%(destdir)s/%(infodir)s' %self.macros)
	    for file in infofiles:
		syspath = '%(destdir)s/%(infodir)s/' %self.macros + file
		path = '%(infodir)s/' %self.macros + file
		if not self.policyException(path):
		    m = self.recipe.magic[path]
		    if not m:
			# not compressed
			util.execute('gzip -f -n -9 %s' %syspath)
                        del self.recipe.magic[path]
		    elif m.name == 'gzip' and \
		       (m.contents['compression'] != '9' or \
		        'name' in m.contents):
			util.execute('gunzip %s; gzip -f -n -9 %s'
                                     %(syspath, syspath[:-3]))
                        del self.recipe.magic[path]
		    elif m.name == 'bzip':
			# should use gzip instead
			util.execute('bunzip2 %s; gzip -f -n -9 %s'
                                     %(syspath, syspath[:-4]))
                        del self.recipe.magic[path]


class NormalizeInitscriptLocation(policy.Policy):
    """
    Puts initscripts in their place, resolving ambiguity about their
    location.

    Moves all initscripts from /etc/rc.d/init.d/ to their official location
    (if, as is true for the default settings, /etc/rc.d/init.d isn't their
    official location, that is).
    """
    # need both of the next two lines to avoid following /etc/rc.d/init.d
    # if it is a symlink
    invariantsubtrees = [ '/etc/rc.d' ]
    invariantinclusions = [ '/etc/rc.d/init.d/' ]

    def test(self):
	return self.macros['initdir'] != '/etc/rc.d/init.d'

    def doFile(self, path):
	basename = os.path.basename(path)
	target = util.joinPaths(self.macros['initdir'], basename)
	if os.path.exists(self.macros['destdir'] + os.sep + target):
	    raise DestdirPolicyError(
		"Conflicting initscripts %s and %s installed" %(
		    path, target))
	util.mkdirChain(self.macros['destdir'] + os.sep +
			self.macros['initdir'])
	util.rename(self.macros['destdir'] + path,
	            self.macros['destdir'] + target)


class NormalizeAppDefaults(policy.Policy):
    """
    There is some disagreement about where to put X app-defaults
    files; this policy resolves that disagreement, and no exceptions
    are recommended.
    """
    def do(self):
        e = '%(destdir)s/%(sysconfdir)s/X11/app-defaults' % self.macros
        if not os.path.isdir(e):
            return

        x = '%(destdir)s/%(x11prefix)s/lib/X11/app-defaults' % self.macros
        self.warn('app-default files misplaced in'
                  ' %(sysconfdir)s/X11/app-defaults' % self.macros)
        if os.path.islink(x):
            util.remove(x)
        util.mkdirChain(x)
        for file in os.listdir(e):
            util.rename(util.joinPaths(e, file),
                        util.joinPaths(x, file))


class NormalizeInterpreterPaths(policy.Policy):
    """
    Interpreter paths in scripts vary; this policy re-writes the
    paths, in particular changing indirect calls through env to
    direct calls; exceptions to this policy should only be made
    when it is part of the explicit calling convention of a script
    that the location of the final interpreter depend on the
    user's C{PATH}:
    C{r.NormalizeInterpreterPaths(exceptions=I{filterexp})}
    """
    invariantexceptions = [ '%(thisdocdir.literalRegex)s/', ]

    def doFile(self, path):
        destdir = self.recipe.macros.destdir
        d = util.joinPaths(destdir, path)
	mode = os.lstat(d)[stat.ST_MODE]
	if not mode & 0111:
            # we care about interpreter paths only in executable scripts
            return
        m = self.recipe.magic[path]
	if m and m.name == 'script':
            interp = m.contents['interpreter']
            if interp.find('/bin/env') != -1: # finds /usr/bin/env too...
                # rewrite to not have env
                line = m.contents['line']
                # we need to be able to write the file
                os.chmod(d, mode | 0200)
                f = file(d, 'r+')
                l = f.readlines()
                l.pop(0) # we will reconstruct this line, without extra spaces
                wordlist = [ x for x in line.split() ]
                wordlist.pop(0) # get rid of env
                # first look in package
                fullintpath = util.checkPath(wordlist[0], root=destdir)
                if fullintpath == None:
                    # then look on installed system
                    fullintpath = util.checkPath(wordlist[0])
                if fullintpath == None:
		    self.error("Interpreter %s for file %s not found, could not convert from /usr/bin/env syntax", wordlist[0], path)
                    return
                
                wordlist[0] = fullintpath
                l.insert(0, '#!'+" ".join(wordlist)+'\n')
                f.seek(0)
                f.truncate(0) # we may have shrunk the file, avoid garbage
                f.writelines(l)
                f.close()
                # revert any change to mode
                os.chmod(d, mode)
                self.info('changing %s to %s in %s',
                          line, " ".join(wordlist), path)
                del self.recipe.magic[path]


class NormalizePamConfig(policy.Policy):
    """
    Some older PAM configuration files include "/lib/security/$ISA/"
    as part of the module path; there is no need for this prefix
    with modern PAM libraries.

    You should never need to run
    C{r.NormalizePamConfig(exceptions=I{filterexp})}
    """
    invariantsubtrees = [
	'%(sysconfdir)s/pam.d/',
    ]

    def doFile(self, path):
        d = util.joinPaths(self.recipe.macros.destdir, path)
        f = file(d, 'r+')
        l = f.readlines()
        l = [x.replace('/lib/security/$ISA/', '') for x in l]
        f.seek(0)
        f.truncate(0) # we may have shrunk the file, avoid garbage
        f.writelines(l)
        f.close()


class RelativeSymlinks(policy.Policy):
    """
    Makes all symlinks relative; create absolute symlinks in your
    recipes, and this will create minimal relative symlinks from them;
    C{r.RelativeSymlinks(exceptions=I{filterexp})}
    """
    def doFile(self, path):
	fullpath = self.macros['destdir']+path
	if os.path.islink(fullpath):
	    contents = os.readlink(fullpath)
	    if contents.startswith('/'):
		pathlist = util.normpath(path).split('/')
		contentslist = util.normpath(contents).split('/')
		if pathlist == contentslist:
		    raise DestdirPolicyError("Symlink points to itself: %s -> %s" % (path, contents))
		while pathlist[0] == contentslist[0]:
		    pathlist = pathlist[1:]
		    contentslist = contentslist[1:]
		os.remove(fullpath)
		dots = "../"
		dots *= len(pathlist) - 1
		normpath = util.normpath(dots + '/'.join(contentslist))
                # we do not want to give people the idea that they should
                # prefer writing relative symlinks, since we want them to
                # create absolute symlinks and let us make minimal relative
                # symlink from them, so let's not make noise about this.
                #self.dbg('Changing absolute symlink %s -> %s to relative symlink -> %s',
                #          path, contents, normpath)
		os.symlink(normpath, fullpath)


def DefaultPolicy(recipe):
    """
    Returns a list of actions that expresses the default policy.
    """
    return [
	TestSuiteLinks(recipe),
	TestSuiteFiles(recipe),
	FixDirModes(recipe),
        AutoDoc(recipe),
	RemoveNonPackageFiles(recipe),
	FixupMultilibPaths(recipe),
	ExecutableLibraries(recipe),
	ReadableDocs(recipe),
	Strip(recipe),
	NormalizeCompression(recipe),
	NormalizeManPages(recipe),
	NormalizeInfoPages(recipe),
	NormalizeInitscriptLocation(recipe),
        NormalizeAppDefaults(recipe),
        NormalizeInterpreterPaths(recipe),
        NormalizePamConfig(recipe),
	RelativeSymlinks(recipe),
    ]


class DestdirPolicyError(policy.PolicyError):
    pass
