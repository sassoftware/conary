#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Modules used by recipes to find source code and unpack and patch it
in the correct directory.
"""

import gzip
import lookaside
import os
import rpmhelper
import util

class _Source:
    def __init__(self, recipe, sourcename, rpm='', dir='', keyid=None, use=None):
	self.recipe = recipe
	self.sourcename = sourcename % recipe.macros
	self.rpm = rpm % recipe.macros
	self.dir = dir % recipe.macros
	self.use = use
	if keyid:
	    self.keyid = keyid
	    self._addSignature()
	if rpm:
	    self._extractFromRPM()

    def _addSignature(self):
        for suffix in ('sig', 'sign', 'asc'):
            self.gpg = '%s.%s' %(self.sourcename, suffix)
            self.localgpgfile = lookaside.searchAll(self.cfg,
				    self.laReposCache, gpg, 
                                    self.name, self.srcdirs)

    def _checkSignature(self, filepath):
	if 'localgpgfile' not in self.__dict__:
	    return
        if not util.checkPath("gpg"):
            return
	# FIXME: our own keyring
	if os.system("gpg --no-secmem-warning --verify %s %s"
		      %(self.localgpgfile, filepath)):
	    # FIXME: only do this if key missing, this is cheap for now
	    os.system("gpg --keyserver pgp.mit.edu --recv-keys 0x %s"
		      %(self.keyid))
	    if os.system("gpg --no-secmem-warning --verify %s %s"
			  %(self.localgpgfile, filepath)):
		raise SourceError, "GPG signature %s failed" %(self.localgpgfile)

    def _extractFromRPM(self):
        """
        Extracts filename from rpm file and creates an entry in the
        source lookaside cache for the extracted file
        """
	# check signature in RPM package?
	f = lookaside.searchAll(self.recipe.cfg, self.recipe.laReposCache, 
                                os.path.basename(self.sourcename),
				self.recipe.name, self.recipe.srcdirs)
	if not f:
	    r = lookaside.findAll(self.recipe.cfg, self.recipe.laReposCache,
				  rpm, self.recipe.name, self.recipe.srcdirs)
	    c = lookaside.createCacheName(self.recipe.cfg, self.sourcename,
					  self.recipe.name)
	    self._extractSourceFromRPM(r, c)

    def _extractSourceFromRPM(self, rpm, targetfile):
	filename = os.path.basename(targetfile)
	directory = os.path.dirname(targetfile)
	r = file(rpm, 'r')
	rpmhelper.seekToData(r)
	gz = gzip.GzipFile(fileobj=r)
	(rpipe, wpipe) = os.pipe()
	pid = os.fork()
	if not pid:
	    os.dup2(rpipe, 0)
	    os.chdir(directory)
	    os.execl('/bin/cpio', 'cpio', '-ium', '--quiet', filename)
	    os._exit(1)
	while 1:
	    buf = gz.read(4096)
	    if not buf:
		break
	    os.write(wpipe, buf)
	os.close(wpipe)
	(pid, status) = os.waitpid(pid, 0)
	if not os.WIFEXITED(status):
	    raise IOError, 'cpio died extracting %s from RPM %s' \
			   %(filename, os.path.basename(rpm))
	if os.WEXITSTATUS(status):
	    raise IOError, \
		'cpio returned failure %d extracting %s from RPM %s' %(
		    os.WEXITSTATUS(status), filename, os.path.basename(rpm))
	if not os.path.exists(targetfile):
	    raise IOError, 'failed to extract source %s from RPM %s' \
			   %(filename, os.path.basename(rpm))

    def _findSource(self):
	return lookaside.findAll(self.recipe.cfg, self.recipe.laReposCache,
	    self.sourcename, self.recipe.name, self.recipe.srcdirs)

    def fetch(self):
	if 'sourcename' not in self.__dict__:
	    return None
	f = lookaside.findAll(recipe.cfg, recipe.laReposCache,
			      self.sourcename, recipe.name, recipe.srcdirs)
	self._checkSignature(f)
	return f

    def unpack(self, builddir):
	if self.use != None:
	    if type(self.use) is not tuple:
		self.use=(self.use,)
	    for usevar in self.use:
		if not usevar:
		    # do not apply this
		    return
	self.builddir = builddir
	self.doUnpack()

    def doUnpack(self):
	raise AttributeError, 'Source class %s does not implement doUnpack' %self.__class__.__name__


class Archive(_Source):
    def __init__(self, recipe, sourcename, rpm='', dir='', keyid=None, use=None):
	_Source.__init__(self, recipe, sourcename, rpm, dir, keyid, use)

    def doUnpack(self):
	f = self._findSource()
	self._checkSignature(f)

	if self.dir:
	    destdir = '%s/%s' % (self.builddir, self.dir)
	    util.mkdirChain(destdir)
	else:
	    destdir = self.builddir

	if f.endswith(".zip"):
	    util.execute("unzip -q -o -d %s %s", destdir, f)
	    return

	if f.endswith(".bz2") or f.endswith(".tbz2"):
	    tarflags = "-jxf"
	elif f.endswith(".gz") or f.endswith(".tgz"):
	    tarflags = "-zxf"
	else:
	    raise SourceError, "unknown archive compression"
	util.execute("tar -C %s %s %s" % (destdir, tarflags, f))


class Patch(_Source):
    def __init__(self, recipe, sourcename, rpm='', dir='', keyid=None,
		 use=None, level='1', backup='', macros=False, extraArgs=''):
	_Source.__init__(self, recipe, sourcename, rpm, dir, keyid, use)
	self.level = level
	self.backup = backup
	self.applymacros = macros
	self.extraArgs = extraArgs

    def doUnpack(self):
	destDir = os.sep.join((self.builddir, self.recipe.theMainDir))
	util.mkdirChain(destDir)

	f = self._findSource()
	provides = "cat"
	if self.sourcename.endswith(".gz"):
	    provides = "zcat"
	elif self.sourcename.endswith(".bz2"):
	    provides = "bzcat"
	if self.backup:
	    backup = '-b -z %s' % self.backup
	if self.dir:
	    destDir = os.sep.join((destDir, self.dir))
	    util.mkdirChain(destDir)
	if self.applymacros:
	    log.debug('applying macros to patch %s' %f)
	    pin = util.popen("%s '%s'" %(provides, f))
	    log.debug('patch -d %s -p%s %s %s'
		      %(destDir, self.level, backup, self.extraArgs))
	    pout = util.popen('patch -d %s -p%s %s %s'
		              %(destDir, self.level, backup, self.extraArgs), 'w')
	    pout.write(pin.read()%self.recipe.macros)
	    pin.close()
	    pout.close()
	else:
	    util.execute("%s '%s' | patch -d %s -p%s %s %s"
			 %(provides, f, destDir, self.level, backup, self.extraArgs))


class Source(_Source):
    def __init__(self, recipe, sourcename, rpm='', dir='', keyid=None,
                  use=None, apply='', macros=False):
	_Source.__init__(self, recipe, sourcename, rpm, dir, keyid, use)
	self.apply = apply % self.recipe.macros
	self.applymacros = macros

    def doUnpack(self):
	destDir = os.sep.join((self.builddir, self.recipe.theMainDir))
	util.mkdirChain(destDir)

	f = self._findSource()
	if self.dir:
	    destDir = os.sep.join((destDir, self.dir))
	    util.mkdirChain(destDir)
	if self.applymacros:
	    log.debug('applying macros to source %s' %f)
	    pin = file(f)
	    pout = file(os.sep.join((destDir,
				     os.path.basename(self.sourcename))), "w")
	    pout.write(pin.read()%self.recipe.macros)
	    pin.close()
	    pout.close()
	else:
	    util.copyfile(f, os.sep.join((destDir,
					  os.path.basename(self.sourcename))))
	if self.apply:
	    util.execute(self.apply, destDir)


class Action(_Source):
    def __init__(self, recipe, action, dir='', use=None):
	_Source.__init__(self, recipe, '', None, dir, None, None)
	self.action = action % self.recipe.macros

    def doUnpack(self):
	destDir = os.sep.join((self.builddir, self.recipe.theMainDir))
	util.mkdirChain(destDir)
	util.execute(self.action, destDir)



class SourceError(Exception):
    """
    Base class from which policy error classes inherit
    """
    def __init__(self, msg, *args):
        self.msg = msg %args

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
