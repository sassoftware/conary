#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import re
import os
import policy
import log

"""
Module used by recipes to modify the state of the installed %(destdir)s
Classes from this module are not used directly; instead, they are used
through eponymous interfaces in recipe.
"""

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
    FIXME: must test when we have a multilib platform!
    """
    invariantinclusions = [
	'*.\.(so.*|a)$',
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
	basename = os.path.basename(path)
	target = util.joinPaths(self.dirmap[self.currentsubtree], basename)
	if os.path.exists(self.macros['destdir'] + os.sep + target):
	    raise DestdirPolicyError(
		"Conflicting library files %s and %s installed" %(
		    path, target))
	log.warning('Multilib error: file %s found in wrong directory,'
		    'attempting to fix...' %path)
	util.mkdirChain(self.macros['destdir'] + os.sep +
			self.macros['initdir'])
	util.rename(self.macros['destdir'] + path,
	              self.macros['destdir'] + target)

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
	'%(bindir)s/',
	'%(essentialbindir)s/',
	'%(sbindir)s/',
	'%(essentialsbindir)s/',
	'%(libdir)s/',
	'%(essentiallibdir)s/',
    ]
    def doFile(self, path):
	if not os.path.islink(path):
	    p = self.macros['destdir']+path
	    # XXX do magic internally instead
	    f = os.popen('file '+p, 'r')
	    filetext = f.read()
	    f.close()
	    if (filetext.find('current ar archive') != -1) or \
	       (filetext.find('ELF') != -1 and
	        filetext.find('not stripped') != -1):
		util.execute('%(strip)s -g ' %self.macros +p)


class NormalizeGzip(policy.Policy):
    """
    re-gzip .gz files with -9 -n to get maximum compression and
    avoid meaningless changes overpopulating the database.
    Ignore man/info pages, we'll get them separately while fixing
    up other things
    """
    invariantexceptions = [
	'%(mandir)s/man.*/',
	'%(infodir)s/',
    ]
    invariantinclusions = [
	'.*\.gz'
    ]
    def doFile(self, path):
	# XXX read in header and check whether needed
	# if (byte[3] & 0xC) == 0x8 or byte[8] != 2: recompress
	util.execute('gunzip %s/%s' %(self.macros['destdir'], path));
	util.execute('gzip -n -9 %s/%s' %(self.macros['destdir'], path[:-3]))

class NormalizeBzip(policy.Policy):
    """
    re-bzip .bz2 files with -9  to get maximum compression.
    Ignore man/info pages, we'll get them separately while fixing
    up other things
    """
    invariantexceptions = [
	'%(mandir)s/man.*/',
	'%(infodir)s/',
    ]
    invariantinclusions = [
	'.*\.bz2'
    ]
    def doFile(self, path):
	# XXX read in header and check whether needed
	# if byte[3] != 9: recompress
	util.execute('bunzip2 %s/%s' %(self.macros['destdir'], path));
	util.execute('bzip2 -9 %s/%s' %(self.macros['destdir'], path[:-3]))

class NormalizeManPages(policy.Policy):
    """
    Make all man pages follow sane system policy
     - Fix all man pages' contents:
       - remove '/?%(destdir)s' from all man pages
       - '.so foo.n' becomes a symlink to foo.n
     - (re)compress all man pages with gzip -n -9
     - change all symlinks to point to .gz (if they don't already)
    Exceptions to this policy are ill-defined and thus are not
    currently honored.  Any suggestion that this policy should
    honor inclusion/exception need to include statements of
    precise semantics in that case...
    """
    def _uncompress(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if name.endswith('.gz') and not os.path.islink(path):
		util.execute('gunzip ' + dirname + os.sep + name)

    def _compress(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if not os.path.isdir(path) and not os.path.islink(path) \
	       and not name.endswith('.gz'):
		util.execute('gzip -n -9 ' + dirname + os.sep + name)

    def _dedestdir(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if not os.path.isdir(path) and not os.path.islink(path) \
	       and not name.endswith('.gz'):
		util.execute("sed -i 's,/?%s,,g' %s"
			     %(self.macros['destdir'][1:], path))

    def _sosymlink(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if os.path.exists(path) and not os.path.isdir(path) \
	       and not os.path.islink(path) \
	       and not name.endswith('.gz'):
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
	# uncompress all man pages
	os.path.walk(manpath, NormalizeManPages._uncompress, self)
	# remove '/?%(destdir)s'
	os.path.walk(manpath, NormalizeManPages._dedestdir, self)
	# .so foo.n becomes a symlink to foo.n
	os.path.walk(manpath, NormalizeManPages._sosymlink, self)
	# recompress all man pages
	os.path.walk(manpath, NormalizeManPages._compress, self)
	# change all symlinks to point to .gz (if they don't already)
	os.path.walk(manpath, NormalizeManPages._gzsymlink, self)

class NormalizeInfoPages(policy.Policy):
    """
    compress info files and remove dir file
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
		    if file.endswith('.gz'):
			util.execute('gunzip %s' %syspath)
			syspath = syspath[:-3]
		    util.execute('gzip -n -9 %s' %syspath)


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
	SanitizeSonames(),
	RemoveExtraLibs(),
	FixupMultilibPaths(),
	RemoveBackupFiles(),
	Strip(),
	NormalizeGzip(),
	NormalizeBzip(),
	NormalizeManPages(),
	NormalizeInfoPages(),
	NormalizeInitscripts(),
	RelativeSymlinks(),
    ]


class DestdirPolicyError(policy.PolicyError):
    pass
