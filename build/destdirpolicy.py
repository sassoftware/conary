#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import re
import os
import policy

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
    invariantinclusions = ['\.la$']

    def doFile(self, path):
	util.remove(self.macros['destdir']+path)

class Strip(policy.Policy):
    """
    strip executables without creating debuginfo subpackage
    """
    def do(self):
	pass

class StripToDebug(policy.Policy):
    """
    move debugging information out of binaries into debuginfo subpackage
    """
    def do(self):
	pass

class NormalizeGzip(policy.Policy):
    """
    re-gzip .gz files with -9 -n to get maximum compression and
    avoid meaningless changes overpopulating the database.
    Ignore man/info pages, we'll get them separately while fixing
    up other things
    """
    invariantexceptions = [
	'%(mandir)s/man.*/.*',
	'%(infodir)s/.*',
    ]
    invariantinclusions = [
	'.*\.gz'
    ]
    def doFile(self, path):
	util.execute('gunzip %s' %path);
	util.execute('gzip -n -9 %s' %path[:-3])

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
			     %(self.macros['destdir'], path))

    def _sosymlink(self, dirname, names):
	for name in names:
	    path = dirname + os.sep + name
	    if os.path.exists and not os.path.isdir(path) \
	       and not os.path.islink(path) \
	       and not name.endswith('.gz'):
		# find .so and change to symlink
		f = file(path)
		lines = f.readlines(512) # we really don't need the whole file
		f.close()
		if len(lines) == 1:
		    match = self.soexp.search(lines[0][:-1]) # chop-chop
		    if match:
			# .so is relative to %(mandir)s, so add ../
			print '+ replacing %s (%s) with symlink ../%s' \
			      %(name, match.group(0), match.group(1))
			os.remove(path)
			os.symlink('../'+match.group(1), path)

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
		os.symlink(contents, path)

    def __init__(self, *args, **keywords):
	policy.Policy.__init__(self, *args, **keywords)
	self.soexp = re.compile('^\.so (.*\...*)$')

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
	    if not policy.policyException(self, dir):
		util.remove(fsdir)
	if os.path.isdir('%(destdir)s/%(infodir)s' %self.macros):
	    infofiles = os.listdir('%(destdir)s/%(infodir)s' %self.macros)
	    for file in infofiles:
		syspath = '%(destdir)s/%(infodir)s/' %self.macros + file
		path = '%(infodir)s/' %self.macros + file
		if not policy.policyException(self, path):
		    if file.endswith('.gz'):
			util.execute('gunzip %s' %syspath)
			syspath = syspath[:-3]
		    util.execute('gzip -n -9 %s' %syspath)


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	SanitizeSonames(),
	RemoveExtraLibs(),
	StripToDebug(),
	NormalizeGzip(),
	NormalizeManPages(),
	NormalizeInfoPages(),
    ]
