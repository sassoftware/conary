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

from build import fixedglob
import errno
import log
import os
import pdb
import shutil
import stat
import string
import struct
import sys
import traceback
import weakref

# Simple ease-of-use extensions to python libraries

def normpath(path):
    s = os.path.normpath(path)
    if s.startswith(os.sep + os.sep):
	return s[1:]
    return s

def isregular(path):
    return stat.S_ISREG(os.lstat(path)[stat.ST_MODE])

def mkdirChain(*paths):
    for path in paths:
        if path[0] != os.sep:
            path = os.getcwd() + os.sep + path
            
        paths = path.split(os.sep)
            
        for n in (range(2,len(paths) + 1)):
            p = string.join(paths[0:n], os.sep)
            if not os.path.exists(p):
                # don't die in case of the race condition where someone
                # made the directory after we stat'ed for it.
                try:
                    os.mkdir(p)
                except OSError, exc:
                    if exc.errno == errno.EEXIST:
                        s = os.lstat(p)
                        if stat.S_ISDIR(s.st_mode):
                            pass
                        else:
                            raise
                    else:
                        raise

def _searchVisit(arg, dirname, names):
    file = arg[0]
    path = arg[1]
    testname = '%s%s%s' %(dirname, os.sep, file)
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
        s = "%s%s%s" %(dir, os.sep, file)
        if os.path.exists(s):
            return s
    if error:
        raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT))
    return None

def findFile(file, searchdirs):
    return searchFile(file, searchdirs, error=1)

def excepthook(type, value, tb):
    sys.excepthook = sys.__excepthook__
    lines = traceback.format_exception(type, value, tb)
    print string.joinfields(lines, "")
    if sys.stdout.isatty() and sys.stdin.isatty():
        pdb.post_mortem(tb)
    else:
        sys.exit(1)

def _handle_rc(rc, cmd):
    if rc:
	if not os.WIFEXITED(rc):
	    info = 'Shell command "%s" killed with signal %d' \
		    %(cmd, os.WTERMSIG(rc))
	if os.WEXITSTATUS(rc):
	    info = 'Shell command "%s" exited with exit code %d' \
		    %(cmd, os.WEXITSTATUS(rc))
        log.error(info)
	raise RuntimeError, info

def execute(cmd, destDir=None, verbose=True):
    if verbose:
	log.debug(cmd)
    if destDir:
	rc = os.system('cd %s; %s' %(destDir, cmd))
    else:
	rc = os.system(cmd)
    _handle_rc(rc, cmd)

class popen:
    """
    Version of popen() that throws errors on close(), unlike os.popen()
    """
    # unfortunately, can't derive from os.popen.  Add methods as necessary.
    def __init__(self, *args):
	self.p = os.popen(*args)
        self.write = self.p.write
        self.read = self.p.read

    def close(self, *args):
	rc = self.p.close(*args)
	_handle_rc(rc, self.p.name)
        return rc

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
	# act more like rm -rf -- allow files, too
	if (os.path.islink(path) or 
                (os.path.exists(path) and not os.path.isdir(path))):
	    os.remove(path)
	else:
	    os.path.walk(path, _permsVisit, None)
	    shutil.rmtree(path, ignore_errors, onerror)

def _permsVisit(arg, dirname, names):
    for name in names:
	path = dirname + os.sep + name
	mode = os.lstat(path)[stat.ST_MODE]
	# has to be executable to cd, readable to list, writeable to delete
	if stat.S_ISDIR(mode) and (mode & 0700) != 0700:
	    log.warning("working around illegal mode 0%o at %s", mode, path)
	    mode |= 0700
	    os.chmod(path, mode)

def remove(paths):
    for path in braceGlob(paths):
	if os.path.isdir(path):
	    log.warning('Not removing directory %s', path)
	elif os.path.exists(path) or os.path.islink(path):
	    log.debug('deleting [file] %s', path)
	    os.remove(path)
	else:
	    log.warning('file %s does not exist when attempting to delete [file]', path)

def copyfile(sources, dest, verbose=True):
    for source in braceGlob(sources):
	if verbose:
	    log.debug('copying %s to %s', source, dest)
	shutil.copy2(source, dest)

def copyfileobj(source, dest):
    total = 0
    buf = source.read(128 * 1024)
    while buf:
	total += len(buf)
	dest.write(buf)
	buf = source.read(128 * 1024)

    return total

def rename(sources, dest):
    for source in braceGlob(sources):
	log.debug('renaming %s to %s', source, dest)
	os.rename(source, dest)

def _copyVisit(arg, dirname, names):
    sourcelist = arg[0]
    sourcelen = arg[1]
    dest = arg[2]
    filemode = arg[3]
    dirmode = arg[4]
    if dirmode:
	os.chmod(dirname, dirmode)
    for name in names:
	if filemode:
	    os.chmod(dirname+os.sep+name, filemode)
	sourcelist.append(os.path.normpath(
	    dest + os.sep + dirname[sourcelen:] + os.sep + name))

def copytree(sources, dest, symlinks=False, filemode=None, dirmode=None):
    """
    Copies tree(s) from sources to dest, returning a list of
    the filenames that it has written.
    """
    sourcelist = []
    for source in braceGlob(sources):
	if os.path.isdir(source):
	    if source[-1] == '/':
		source = source[:-1]
	    thisdest = '%s%s%s' %(dest, os.sep, os.path.basename(source))
	    log.debug('copying [tree] %s to %s', source, thisdest)
	    shutil.copytree(source, thisdest, symlinks)
	    if dirmode:
		os.chmod(thisdest, dirmode)
	    os.path.walk(source, _copyVisit,
			 (sourcelist, len(source), thisdest, filemode, dirmode))
	else:
	    log.debug('copying [file] %s to %s', source, dest)
	    shutil.copy2(source, dest)
	    if dest.endswith(os.sep):
		thisdest = dest + os.sep + os.path.basename(source)
	    else:
		thisdest = dest
	    if filemode:
		os.chmod(thisdest, filemode)
	    sourcelist.append(thisdest)
    return sourcelist

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

def joinPaths(*args):
    return normpath(os.sep.join(args))

def assertIteratorAtEnd(iter):
    try:
	iter.next()
	raise AssertionError
    except StopIteration:
	return True

class ObjectCache(weakref.WeakKeyDictionary):
    """
    Implements a cache of arbitrary (hashable objects) where an object
    can be looked up and have it's cached value retrieved. This allows
    a single copy of immutable objects to be kept in memory.
    """
    def __setitem__(self, key, value):
	weakref.WeakKeyDictionary.__setitem__(self, key, weakref.ref(value))

    def __getitem__(self, key):
	return weakref.WeakKeyDictionary.__getitem__(self, key)()

def gzipFileSize(f):
    """
    Returns the uncompressed size of the gzipped file referenced by f
    """

    # read in the size of the file
    old = f.tell()
    f.seek(-4, 2)
    size = f.read(4)
    f.seek(old)

    # we need the size to create a file container to pass over
    # the wire for getFileContents()
    size = struct.unpack("<i", size)[0]

    return size

def xzip(l1, l2):
    for x in l1:
	yield (x, l2.next())

def memsize():
    import os
    pfn = "/proc/%d/status" % os.getpid()
    lines = open(pfn).readlines()
    f = lines[10].split()
    return int(f[1])
