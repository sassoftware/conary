#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import re
import os
import stat
import policy
import log
import magic

"""
Module used by recipes to modify the state of the installed %(destdir)s
Classes from this module are not used directly; instead, they are used
through eponymous interfaces in recipe.
"""

class FixDirModes(policy.Policy):
    """
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

class SanitizeSonames(policy.Policy):
    """
    make sure that .so -> SONAME -> fullname
    """
    def do(self):
	pass

class RemoveExtraLibs(policy.Policy):
    """
    Kill .la files and any other similar garbage
    """
    invariantinclusions = [
	'\.la$',
	'%(libdir)s/python.*/site-packages/.*.a$'
    ]

    def doFile(self, path):
	util.remove(self.macros['destdir']+path)

class FixupMultilibPaths(policy.Policy):
    """
    Fix up (and warn) when programs do not know about %(lib) and they
    are supposed to be installing to lib64
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
	destdir = self.macros['destdir']
	m = magic.magic(path, destdir)
	if not m or (m.name != "ELF" and m.name != "ar"):
	    log.warning("non-executable object with library name %s", path)
	    return
	basename = os.path.basename(path)
	targetdir = self.dirmap[self.currentsubtree %self.macros]
	target = util.joinPaths(targetdir, basename)
	if os.path.exists(destdir + os.sep + target):
	    raise DestdirPolicyError(
		"Conflicting library files %s and %s installed" %(
		    path, target))
	log.warning('Multilib error: file %s found in wrong directory,'
		    ' attempting to fix...' %path)
	util.mkdirChain(destdir + targetdir)
	util.rename(destdir + path, destdir + target)

class RemoveBackupFiles(policy.Policy):
    """
    Kill editor and patch backup files
    """
    invariantinclusions = [
	'~$',
	'\.orig$',
    ]

    def doFile(self, path):
	util.remove(self.macros['destdir']+path)

class Strip(policy.Policy):
    """
    strip executables
    XXX system policy on whether to create debuginfo packages
    """
    invariantinclusions = [
	('%(bindir)s/', None, stat.S_IFDIR),
	('%(essentialbindir)s/', None, stat.S_IFDIR),
	('%(sbindir)s/', None, stat.S_IFDIR),
	('%(essentialsbindir)s/', None, stat.S_IFDIR),
	('%(libdir)s/', None, stat.S_IFDIR),
	('%(essentiallibdir)s/', None, stat.S_IFDIR),
    ]
    def doFile(self, path):
	if os.path.islink(path):
	    return
	d = self.macros['destdir']
	m = magic.magic(path, d)
	if not m:
	    return
	# FIXME: should be:
	#if (m.name == "ELF" or m.name == "ar") and \
	#   m.contents['hasDebug']):
	# but this has to wait until ewt writes debug detection
	# for archives as well as elf files
	if (m.name == "ELF" and m.contents['hasDebug']) or \
	   (m.name == "ar"):
	    util.execute('%(strip)s -g ' %self.macros +d+path)


class NormalizeCompression(policy.Policy):
    """
    re-gzip .gz files with -9 -n, and .bz2 files with -9, to get maximum
    compression and avoid meaningless changes overpopulating the database.
    Ignore man/info pages, we'll get them separately while fixing
    up other things
    """
    invariantexceptions = [
	'%(mandir)s/man.*/',
	'%(infodir)s/',
    ]
    invariantinclusions = [
	('.*\.(gz|bz2)', None, stat.S_IFDIR),
    ]
    def doFile(self, path):
	if os.path.islink(path):
	    return
	d = self.macros['destdir']
	m = magic.magic(path, d)
	if not m:
	    return
	p = d+path
	if m.name == 'gzip' and \
	   (m.contents['compression'] != '9' or 'name' in m.contents):
	    util.execute('gunzip %s; gzip -n -9 %s' %(p, p[:-3]))
	if m.name == 'bzip' and m.contents['compression'] != '9':
	    util.execute('bunzip2 %s; bzip2 -9 %s' %(p, p[:-4]))

class NormalizeManPages(policy.Policy):
    """
    Make all man pages follow sane system policy
     - Fix all man pages' contents:
       - remove '/?%(destdir)s' from all man pages
       - '.so foo.n' becomes a symlink to foo.n
     - (re)compress all man pages with gzip -n -9
     - change all symlinks to point to .gz (if they don't already)
     - make all man pages be mode 644
    Exceptions to this policy are ill-defined and thus are not
    currently honored.  Any suggestion that this policy should
    honor inclusion/exception needs to include statements of
    precise semantics in that case...
    """
    def _uncompress(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if name.endswith('.gz') and not os.path.islink(path):
		util.execute('gunzip ' + dirname + os.sep + name)
	    if name.endswith('.bz2') and not os.path.islink(path):
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
	    if mode & 0777 != 0644:
		os.chmod(path, 0644)
	    if not os.path.isdir(path) and not os.path.islink(path):
		# no .gz files at this point
		util.execute("sed -i 's,/*%s,,g' %s" %(self.destdir, path))

    def _sosymlink(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if os.path.exists(path) and not os.path.isdir(path) \
	       and not os.path.islink(path):
		# if only .so, change to symlink
		f = file(path)
		lines = f.readlines(512) # we really don't need the whole file
		f.close()

		# delete comment lines first
		newlines = []
		for line in lines:
		    if not self.commentexp.search(line[:-1]):
			newlines.append(line)
		lines = newlines

		# now see if we have only a .so line to replace
		if len(lines) == 1:
		    match = self.soexp.search(lines[0][:-1]) # chop-chop
		    if match:
			# .so is relative to %(mandir)s, so add ../
			log.debug('replacing %s (%s) with symlink ../%s',
                                  name, match.group(0), match.group(1))
			os.remove(path)
			os.symlink(util.normpath('../'+match.group(1)), path)

    def _compress(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if not os.path.isdir(path) and not os.path.islink(path):
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
	self.soexp = re.compile('^\.so (.*\...*)$')
	self.commentexp = re.compile('^\.\\\\"')

    def do(self):
	manpath = self.macros['destdir'] + self.macros['mandir']
	self.destdir = self.macros['destdir'][1:] # we need without leading /
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
    properly compress info files and remove dir file
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
		    m = magic.magic(syspath)
		    if not m:
			# not compressed
			util.execute('gzip -n -9 %s' %syspath)
		    if m.name == 'gzip' and \
		       (m.contents['compression'] != '9' or \
		        'name' in m.contents):
			util.execute('gunzip %s; gzip -n -9 %s',
				     syspath, syspath[:-3])
		    if m.name == 'bzip':
			# should use gzip instead
			util.execute('bunzip2 %s; gzip -n -9 %s',
				     syspath, syspath[:-4])


class NormalizeInitscripts(policy.Policy):
    """
    Move all initscripts from /etc/rc.d/init.d/ to their official location
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
    Make all symlinks relative
    """
    def doFile(self, path):
	fullpath = self.macros['destdir']+path
	if os.path.islink(fullpath):
	    contents = os.readlink(fullpath)
	    if contents.startswith('/'):
		os.remove(fullpath)
		dots = "../"
		dots *= path.count('/') - 1
		normpath = util.normpath(dots + contents)
		# FIXME: make shortest possible relative symlink
		log.debug('Changing absolute symlink %s to relative symlink %s',
                          path, normpath)
		os.symlink(normpath, fullpath)


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	FixDirModes(),
	SanitizeSonames(),
	RemoveExtraLibs(),
	FixupMultilibPaths(),
	RemoveBackupFiles(),
	Strip(),
	NormalizeCompression(),
	NormalizeManPages(),
	NormalizeInfoPages(),
	NormalizeInitscripts(),
	RelativeSymlinks(),
    ]


class DestdirPolicyError(policy.PolicyError):
    pass
