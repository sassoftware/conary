#
# Copyright (c) 2004 Specifix, Inc.
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

import util
import re
import os
import stat
import policy
import log
import magic

# used in multiple places, should be maintained in one place
# probably needs to migrate to some form of configuration
librarydirs = [
    '%(libdir)s/',
    '%(essentiallibdir)s/',
    '%(krbprefix)s/%(lib)s/',
    '%(x11prefix)s/%(lib)s/',
    '%(prefix)s/local/%(lib)s/',
]

class TestSuiteLinks(policy.Policy):
    """
    Create symlinks into a 'test build' directory, which mirrors
    the build directory, except that all executables/configs/etc
    in the 'test build' directory are symlinks to the installed 
    versions where possible.
    """

    keywords = { 'build': None,
		 'fileMap' : {}
		} 
    invariantexceptions = [ 
#			    [ '.*', stat.S_IFDIR ],
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
	"""
	call as C{TestSuiteLinks(<inclusions>, fileMap=<map>, ...)}.
	@keyword fileMap: each key is a path to a builddir file, each value is
	path to a destdir file that is equivalent to the builddir file.  Default: {}
	@type fileMap: Hash builddir path -> destdir path
	@keyword build: If set to true, will create TestSuiteLinks even if a TestSuite command is not given.  If set to false, will not create TestSuiteLinks even if a TestSuite command is given.  Also turns on/off TestSuiteFiles

	"""
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
	# log.warning('Could not determine which builddir file %s corresponds to for creating test component' % fullpath[destdirlen:])
	return None


#XXX this is a builddir policy
class TestSuiteFiles(policy.Policy):
    """
    TestSuiteFiles marks those files which are not installed on 
    the user's system, but are needed for running the testsuite
    included with the package.  TestSuiteFiles are installed 
    in the I{:test} component, in a special I{testdir}. 
    The TestSuiteFiles policy is only activated if a TestSuite
    build command has been given, or it is explictly activated using 
    the keyword I{build}.
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
	fullpath = ('%(builddir)s'+path) %self.macros
	testpath = ('%(destdir)s%(thistestdir)s'+path) % self.macros
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
	fullpath = util.normpath(self.macros['destdir']+os.sep+path)
	mode = os.lstat(fullpath)[stat.ST_MODE]
	self.recipe.AddModes(mode, path)
	os.chmod(fullpath, mode | 0700)

class RemoveNonPackageFiles(policy.Policy):
    """
    Remove classes of files that normally should not be packaged;
    C{r.RemoveNonPackageFiles(exceptions=I{filterexpression})}
    allows one of these files to be included in a package.
    """
    invariantinclusions = [
	r'\.la$',
	r'%(libdir)s/python.*/site-packages/.*\.a$',
	r'perllocal\.pod$',
	r'\.packlist$',
	r'\.cvsignore$',
	r'\.orig$',
	'~$',
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
	m = self.recipe.magic[path]
	if not m or (m.name != "ELF" and m.name != "ar"):
	    log.warning("non-executable object with library name %s", path)
	    return
	basename = os.path.basename(path)
	targetdir = self.dirmap[self.currentsubtree %self.macros]
	target = util.joinPaths(targetdir, basename)
	destdir = self.macros.destdir
	if os.path.exists(util.joinPaths(destdir, target)):
	    raise DestdirPolicyError(
		"Conflicting library files %s and %s installed" %(
		    path, target))
	log.warning('Multilib error: file %s found in wrong directory,'
		    ' attempting to fix...' %path)
	util.mkdirChain(destdir + targetdir)
	util.rename(destdir + path, destdir + target)

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
	log.warning('non-executable library %s, changing to mode 0755' %path)
	os.chmod(fullpath, 0755)

class Strip(policy.Policy):
    """
    Strips executables and libraries of debugging information.
    May (depending on configuration) save the debugging information
    for future use.
    """
    # XXX system policy on whether to create debuginfo packages
    invariantinclusions = [
	('%(bindir)s/', None, stat.S_IFDIR),
	('%(essentialbindir)s/', None, stat.S_IFDIR),
	('%(sbindir)s/', None, stat.S_IFDIR),
	('%(essentialsbindir)s/', None, stat.S_IFDIR),
	('%(libdir)s/', None, stat.S_IFDIR),
	('%(essentiallibdir)s/', None, stat.S_IFDIR),
    ]
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
	    util.execute('%(strip)s -g ' %self.macros +self.macros.destdir+path)


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
	    util.execute('gunzip %s; gzip -n -9 %s' %(p, p[:-3]))
	if m.name == 'bzip' and m.contents['compression'] != '9':
	    util.execute('bunzip2 %s; bzip2 -9 %s' %(p, p[:-4]))

class NormalizeManPages(policy.Policy):
    """
    Make all man pages follow sane system policy; not called from recipes,
    as there are no exceptions, period.
     - Fix all man pages' contents:
       - remove instances of C{/?%(destdir)s} from all man pages
       - C{.so foo.n} becomes a symlink to foo.n
     - (re)compress all man pages with gzip -n -9
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

    def _dedestdir(self, dirname, names):
	"""
	remove destdir, and fix up modes (this is the most convenient
	place to fix up modes without adding an extra scan of the
	directory tree)
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
	    util.execute("sed -i 's,/*%s,,g' %s" %(self.destdir, path))

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
				log.debug('replacing %s (%s) with symlink %s',
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
				log.debug('replacing %s (%s) with symlink %s',
					  name, match.group(0), target)
				os.remove(path)
				os.symlink(target, path)

    def _compress(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if util.isregular(path):
		util.execute('gzip -n -9 ' + dirname + os.sep + name)

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
	    os.path.walk(manpath, NormalizeManPages._dedestdir, self)
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
			util.execute('gzip -n -9 %s' %syspath)
		    elif m.name == 'gzip' and \
		       (m.contents['compression'] != '9' or \
		        'name' in m.contents):
			util.execute('gunzip %s; gzip -n -9 %s'
                                     %(syspath, syspath[:-3]))
		    elif m.name == 'bzip':
			# should use gzip instead
			util.execute('bunzip2 %s; gzip -n -9 %s'
                                     %(syspath, syspath[:-4]))


class NormalizeInitscripts(policy.Policy):
    """
    Puts initscripts in their place, resolving ambiguity about their
    location.

    Moves all initscripts from /etc/rc.d/init.d/ to their official location
    (if, as is true for the default settings, /etc/rc.d/init.d isn't their
    official location, that is).
    """
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
		while pathlist[0] == contentslist[0]:
		    pathlist = pathlist[1:]
		    contentslist = contentslist[1:]
		os.remove(fullpath)
		dots = "../"
		dots *= len(pathlist) - 1
		normpath = util.normpath(dots + '/'.join(contentslist))
		log.debug('Changing absolute symlink %s -> %s to relative symlink -> %s',
                          path, contents, normpath)
		os.symlink(normpath, fullpath)


def DefaultPolicy(recipe):
    """
    Returns a list of actions that expresses the default policy.
    """
    return [
	TestSuiteLinks(recipe),
	TestSuiteFiles(recipe),
	FixDirModes(recipe),
	RemoveNonPackageFiles(recipe),
	FixupMultilibPaths(recipe),
	ExecutableLibraries(recipe),
	Strip(recipe),
	NormalizeCompression(recipe),
	NormalizeManPages(recipe),
	NormalizeInfoPages(recipe),
	NormalizeInitscripts(recipe),
	RelativeSymlinks(recipe),
    ]


class DestdirPolicyError(policy.PolicyError):
    pass
